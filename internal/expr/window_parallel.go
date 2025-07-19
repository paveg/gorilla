package expr

import (
	"fmt"
	"runtime"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/parallel"
)

// WindowParallelConfig holds configuration for parallel window function execution
type WindowParallelConfig struct {
	// Minimum number of partitions to trigger parallel execution
	MinPartitionsForParallel int
	// Minimum rows per partition to trigger parallel sorting
	MinRowsForParallelSort int
	// Maximum number of workers for window operations
	MaxWorkers int
	// Enable adaptive parallelization based on data characteristics
	AdaptiveParallelization bool
}

// DefaultWindowParallelConfig returns the default configuration for parallel window execution
func DefaultWindowParallelConfig() *WindowParallelConfig {
	const (
		defaultMinPartitions      = 4
		defaultMinRowsForParallel = 1000
	)
	return &WindowParallelConfig{
		MinPartitionsForParallel: defaultMinPartitions,      // Need at least 4 partitions for effective parallelization
		MinRowsForParallelSort:   defaultMinRowsForParallel, // Parallel sort threshold per partition
		MaxWorkers:               runtime.NumCPU(),
		AdaptiveParallelization:  true,
	}
}

// partitionTask represents a partition processing task
type partitionTask struct {
	partitionIndex int
	rowIndices     []int
}

// partitionResult represents the result of processing a partition
type partitionResult struct {
	partitionIndex  int
	results         []interface{}
	originalIndices []int
	err             error
}

// shouldUseParallelExecution determines if parallel execution should be used for window functions
func (e *Evaluator) shouldUseParallelExecution(partitions [][]int, config *WindowParallelConfig) bool {
	if !config.AdaptiveParallelization {
		return len(partitions) >= config.MinPartitionsForParallel
	}

	// Adaptive logic: consider both partition count and total work
	partitionCount := len(partitions)
	totalRows := 0
	for _, partition := range partitions {
		totalRows += len(partition)
	}

	// Use parallel if we have enough partitions OR enough total work
	return partitionCount >= config.MinPartitionsForParallel ||
		(partitionCount >= 2 && totalRows >= config.MinRowsForParallelSort)
}

// evaluateRankParallel implements parallel RANK() window function
func (e *Evaluator) evaluateRankParallel(
	window *WindowSpec,
	columns map[string]arrow.Array,
	dataLength int,
) (arrow.Array, error) {
	return e.evaluateWindowFunctionParallel(
		"RANK",
		window,
		columns,
		dataLength,
		e.processRankPartition,
	)
}

// evaluateDenseRankParallel implements parallel DENSE_RANK() window function
func (e *Evaluator) evaluateDenseRankParallel(
	window *WindowSpec,
	columns map[string]arrow.Array,
	dataLength int,
) (arrow.Array, error) {
	return e.evaluateWindowFunctionParallel(
		"DENSE_RANK",
		window,
		columns,
		dataLength,
		e.processDenseRankPartition,
	)
}

// evaluateWindowFunctionParallel is a generic parallel executor for window functions
func (e *Evaluator) evaluateWindowFunctionParallel(
	funcName string,
	window *WindowSpec,
	columns map[string]arrow.Array,
	dataLength int,
	partitionProcessor func([]int, *WindowSpec, map[string]arrow.Array, memory.Allocator) ([]interface{}, error),
) (arrow.Array, error) {
	// Get partitions
	partitions, err := e.getPartitions(window, columns, dataLength)
	if err != nil {
		return nil, fmt.Errorf("getting partitions: %w", err)
	}

	config := DefaultWindowParallelConfig()

	// Check if parallel execution is beneficial
	if !e.shouldUseParallelExecution(partitions, config) {
		// Fall back to sequential execution for small datasets
		switch funcName {
		case "RANK":
			return e.evaluateRank(window, columns, dataLength)
		case "DENSE_RANK":
			return e.evaluateDenseRank(window, columns, dataLength)
		default:
			return nil, fmt.Errorf("unsupported function for fallback: %s", funcName)
		}
	}

	// Execute partitions in parallel
	results, err := e.executePartitionsParallel(partitions, window, columns, partitionProcessor, config)
	if err != nil {
		return nil, fmt.Errorf("parallel partition execution failed: %w", err)
	}

	// Build the final result array
	return e.buildWindowResult(results, dataLength, typeInt64)
}

// executePartitionsParallel executes window function partitions in parallel
func (e *Evaluator) executePartitionsParallel(
	partitions [][]int,
	window *WindowSpec,
	columns map[string]arrow.Array,
	partitionProcessor func([]int, *WindowSpec, map[string]arrow.Array, memory.Allocator) ([]interface{}, error),
	config *WindowParallelConfig,
) ([]partitionResult, error) {
	// Create worker pool
	workerCount := config.MaxWorkers
	if workerCount > len(partitions) {
		workerCount = len(partitions)
	}

	wp := parallel.NewWorkerPool(workerCount)
	defer wp.Close()

	// Create partition tasks
	tasks := make([]partitionTask, len(partitions))
	for i, partition := range partitions {
		tasks[i] = partitionTask{
			partitionIndex: i,
			rowIndices:     partition,
		}
	}

	// Process partitions in parallel
	results := parallel.ProcessIndexed(wp, tasks,
		func(taskIndex int, task partitionTask) partitionResult {
			// Create independent memory allocator for thread safety
			workerMem := memory.NewGoAllocator()

			// Process the partition
			partitionResults, err := partitionProcessor(
				task.rowIndices,
				window,
				columns,
				workerMem,
			)

			return partitionResult{
				partitionIndex:  task.partitionIndex,
				results:         partitionResults,
				originalIndices: task.rowIndices,
				err:             err,
			}
		})

	// Check for errors
	for _, result := range results {
		if result.err != nil {
			return nil, fmt.Errorf("partition %d failed: %w", result.partitionIndex, result.err)
		}
	}

	return results, nil
}

// processRankPartition processes a single partition for RANK() function
func (e *Evaluator) processRankPartition(
	partition []int,
	window *WindowSpec,
	columns map[string]arrow.Array,
	workerMem memory.Allocator,
) ([]interface{}, error) {
	// Sort partition if ORDER BY is specified
	sortedIndices := partition
	if len(window.orderBy) > 0 {
		config := DefaultWindowParallelConfig()
		if len(partition) >= config.MinRowsForParallelSort {
			// Use parallel sort for large partitions
			sortedIndices = e.sortPartitionParallel(partition, window.orderBy, columns)
		} else {
			// Use sequential sort for small partitions
			sortedIndices = e.sortPartition(partition, window.orderBy, columns)
		}
	}

	// Calculate ranks within partition
	results := make([]interface{}, len(partition))
	currentRank := int64(1)

	for i, idx := range sortedIndices {
		if i > 0 {
			// Check if current row has same values as previous row
			if !e.rowsEqual(sortedIndices[i-1], idx, window.orderBy, columns) {
				currentRank = int64(i + 1)
			}
		}

		// Find the position in original partition order
		originalPos := e.findIndexInSlice(partition, idx)
		if originalPos >= 0 {
			results[originalPos] = currentRank
		}
	}

	return results, nil
}

// processDenseRankPartition processes a single partition for DENSE_RANK() function
func (e *Evaluator) processDenseRankPartition(
	partition []int,
	window *WindowSpec,
	columns map[string]arrow.Array,
	workerMem memory.Allocator,
) ([]interface{}, error) {
	// Sort partition if ORDER BY is specified
	sortedIndices := partition
	if len(window.orderBy) > 0 {
		config := DefaultWindowParallelConfig()
		if len(partition) >= config.MinRowsForParallelSort {
			// Use parallel sort for large partitions
			sortedIndices = e.sortPartitionParallel(partition, window.orderBy, columns)
		} else {
			// Use sequential sort for small partitions
			sortedIndices = e.sortPartition(partition, window.orderBy, columns)
		}
	}

	// Calculate dense ranks within partition (no gaps)
	results := make([]interface{}, len(partition))
	currentRank := int64(1)

	for i, idx := range sortedIndices {
		if i > 0 {
			// Check if current row has same values as previous row
			if !e.rowsEqual(sortedIndices[i-1], idx, window.orderBy, columns) {
				currentRank++
			}
		}

		// Find the position in original partition order
		originalPos := e.findIndexInSlice(partition, idx)
		if originalPos >= 0 {
			results[originalPos] = currentRank
		}
	}

	return results, nil
}

// sortPartitionParallel sorts a partition with adaptive threshold for parallelization
func (e *Evaluator) sortPartitionParallel(
	partition []int,
	orderBy []OrderByExpr,
	columns map[string]arrow.Array,
) []int {
	const minPartitionSizeForParallelSort = 1000
	if len(partition) <= minPartitionSizeForParallelSort {
		// Fall back to sequential sort for small partitions
		return e.sortPartition(partition, orderBy, columns)
	}

	// For very large partitions, attempt parallel sort
	// Currently falls back to sequential sort
	return e.sortPartitionWithFallback(partition, orderBy, columns)
}

// sortPartitionWithFallback performs a sequential sort as a fallback mechanism
func (e *Evaluator) sortPartitionWithFallback(
	partition []int,
	orderBy []OrderByExpr,
	columns map[string]arrow.Array,
) []int {
	// This function currently uses a standard sequential sort.
	// Implementing a parallel merge sort could be considered in the future
	// if benchmarks demonstrate significant performance benefits.
	return e.sortPartition(partition, orderBy, columns)
}

// buildWindowResult builds the final result array from partition results
func (e *Evaluator) buildWindowResult(
	partitionResults []partitionResult,
	dataLength int,
	resultType string,
) (arrow.Array, error) {
	// Create result array to hold all values
	finalResult := make([]interface{}, dataLength)

	// Combine results from all partitions
	for _, pResult := range partitionResults {
		for i, idx := range pResult.originalIndices {
			if i < len(pResult.results) {
				finalResult[idx] = pResult.results[i]
			}
		}
	}

	// Build typed array based on result type
	switch resultType {
	case typeInt64:
		builder := array.NewInt64Builder(e.mem)
		defer builder.Release()
		for i := 0; i < dataLength; i++ {
			if finalResult[i] == nil {
				builder.AppendNull()
			} else {
				builder.Append(finalResult[i].(int64))
			}
		}
		return builder.NewArray(), nil
	default:
		return nil, fmt.Errorf("unsupported result type: %s", resultType)
	}
}

// findIndexInSlice finds the index of a value in a slice
func (e *Evaluator) findIndexInSlice(slice []int, value int) int {
	for i, v := range slice {
		if v == value {
			return i
		}
	}
	return -1
}

// shouldUseWindowParallelExecution determines if parallel execution should be used for a window function
func (e *Evaluator) shouldUseWindowParallelExecution(
	window *WindowSpec,
	columns map[string]arrow.Array,
	dataLength int,
) bool {
	// Get partitions to analyze
	partitions, err := e.getPartitions(window, columns, dataLength)
	if err != nil {
		// If we can't get partitions, fall back to sequential
		return false
	}

	config := e.getWindowParallelConfig()
	return e.shouldUseParallelExecution(partitions, config)
}

// getWindowParallelConfig returns the parallel configuration for window functions
func (e *Evaluator) getWindowParallelConfig() *WindowParallelConfig {
	// Currently, this function returns a default configuration for parallel execution.
	// TODO: Integrate with global configuration system to allow dynamic configuration.
	// Implications: Until integration is complete, any changes to the global configuration
	// will not affect the behavior of this function. Developers should ensure that the
	// default configuration is suitable for their use cases.
	return DefaultWindowParallelConfig()
}
