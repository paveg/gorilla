// Package parallel provides parallel processing infrastructure for DataFrame operations.
//
// This package implements worker pools and parallel execution strategies for
// DataFrame operations that exceed the parallelization threshold. It provides
// both generic parallel processing and specialized order-preserving variants.
//
// Key features:
//   - Adaptive worker pool sizing based on CPU count and data size
//   - Fan-out/fan-in patterns for parallel execution
//   - Thread-safe operations with independent data copies
//   - Memory-efficient chunking for large datasets
//   - Order-preserving parallel operations when needed
//
// The package automatically activates for DataFrames with 1000+ rows and
// uses runtime.NumCPU() as the default worker count, with dynamic adjustment
// based on workload characteristics.
package parallel

import (
	"context"
	"runtime"
	"sync"
)

// WorkerPool manages a pool of goroutines for parallel processing
type WorkerPool struct {
	numWorkers int
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(numWorkers int) *WorkerPool {
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &WorkerPool{
		numWorkers: numWorkers,
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Process executes work items in parallel using fan-out/fan-in pattern
func Process[T, R any](
	wp *WorkerPool,
	items []T,
	worker func(T) R,
) []R {
	if len(items) == 0 {
		return nil
	}

	// Channel for input items
	itemCh := make(chan T, len(items))

	// Channel for results
	resultCh := make(chan indexedResult[R], len(items))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < wp.numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range itemCh {
				select {
				case <-wp.ctx.Done():
					return
				default:
					result := worker(item)
					resultCh <- indexedResult[R]{result: result}
				}
			}
		}()
	}

	// Send items to workers
	go func() {
		defer close(itemCh)
		for _, item := range items {
			select {
			case <-wp.ctx.Done():
				return
			case itemCh <- item:
			}
		}
	}()

	// Close result channel when all workers are done
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Collect results
	results := make([]R, 0, len(items))
	for result := range resultCh {
		results = append(results, result.result)
	}

	return results
}

// ProcessIndexed executes work items in parallel while preserving order
func ProcessIndexed[T, R any](
	wp *WorkerPool,
	items []T,
	worker func(int, T) R,
) []R {
	if len(items) == 0 {
		return nil
	}

	// Channel for input items with index
	itemCh := make(chan indexedItem[T], len(items))

	// Channel for results with index
	resultCh := make(chan indexedResult[R], len(items))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < wp.numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range itemCh {
				select {
				case <-wp.ctx.Done():
					return
				default:
					result := worker(item.index, item.value)
					resultCh <- indexedResult[R]{
						index:  item.index,
						result: result,
					}
				}
			}
		}()
	}

	// Send items to workers
	go func() {
		defer close(itemCh)
		for i, item := range items {
			select {
			case <-wp.ctx.Done():
				return
			case itemCh <- indexedItem[T]{index: i, value: item}:
			}
		}
	}()

	// Close result channel when all workers are done
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Collect results and maintain order
	results := make([]R, len(items))
	for result := range resultCh {
		results[result.index] = result.result
	}

	return results
}

// Close shuts down the worker pool
func (wp *WorkerPool) Close() {
	wp.cancel()
}

// indexedItem holds an item with its index
type indexedItem[T any] struct {
	index int
	value T
}

// indexedResult holds a result with its index
type indexedResult[R any] struct {
	index  int
	result R
}
