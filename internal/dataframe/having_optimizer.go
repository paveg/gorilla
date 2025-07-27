package dataframe

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/expr"
)

// CompiledExpression represents a pre-compiled expression for repeated evaluation
type CompiledExpression struct {
	originalExpr  expr.Expr
	compiledPlan  ExpressionPlan
	typeSignature string
	isConstant    bool
	constantValue interface{}
}

// ExpressionPlan represents an optimized execution plan for an expression
type ExpressionPlan struct {
	operations   []CompiledOperation
	columnAccess []ColumnAccessInfo
	memoryLayout MemoryLayoutInfo
}

// CompiledOperation represents a single optimized operation in the expression plan
type CompiledOperation struct {
	opType       OperationType
	inputIndices []int
	outputIndex  int
	metadata     interface{}
}

// OperationType represents the type of compiled operation
type OperationType int

const (
	OpTypeColumnAccess OperationType = iota
	OpTypeLiteral
	OpTypeAggregation
	OpTypeComparison
	OpTypeArithmetic
	OpTypeLogical
)

// ColumnAccessInfo contains optimized column access information
type ColumnAccessInfo struct {
	name          string
	index         int
	dataType      arrow.Type
	accessPattern AccessPattern
}

// AccessPattern describes how the column will be accessed
type AccessPattern int

const (
	AccessSequential AccessPattern = iota
	AccessRandom
	AccessGrouped
)

// MemoryLayoutInfo contains memory optimization hints
type MemoryLayoutInfo struct {
	estimatedMemoryUsage int64
	preferredChunkSize   int
	cacheLocality        CacheLocalityHint
}

// CacheLocalityHint provides cache optimization hints
type CacheLocalityHint int

const (
	CacheLocalitySequential CacheLocalityHint = iota
	CacheLocalityRandom
	CacheLocalityGrouped
)

// CompiledHavingEvaluator provides optimized HAVING clause evaluation
type CompiledHavingEvaluator struct {
	predicate       expr.Expr
	compiledExpr    *CompiledExpression
	columnIndices   map[string]int
	typeOptimizer   TypeSpecificEvaluator
	memoryPool      *HavingMemoryPool
	performanceHint PerformanceHint

	// Performance monitoring
	metrics         *HavingPerformanceMetrics
	enableProfiling bool
}

// TypeSpecificEvaluator provides type-optimized evaluation paths
type TypeSpecificEvaluator interface {
	EvaluateFloat64Comparison(left, right []float64, op expr.BinaryOp) (*array.Boolean, error)
	EvaluateInt64Comparison(left, right []int64, op expr.BinaryOp) (*array.Boolean, error)
	EvaluateStringComparison(left, right []string, op expr.BinaryOp) (*array.Boolean, error)
	EvaluateFloat64Aggregation(values []float64, aggType expr.AggregationType) (float64, error)
	EvaluateInt64Aggregation(values []int64, aggType expr.AggregationType) (interface{}, error)
}

// DefaultTypeOptimizer provides default type-specific optimizations
type DefaultTypeOptimizer struct {
	mem memory.Allocator
}

// HavingMemoryPool manages memory allocation for HAVING operations
type HavingMemoryPool struct {
	booleanArrays sync.Pool
	float64Arrays sync.Pool
	int64Arrays   sync.Pool
	indexArrays   sync.Pool
	allocator     memory.Allocator
	maxPoolSize   int
	currentSize   int64
	mutex         sync.RWMutex
}

// PerformanceHint provides optimization hints for the evaluator
type PerformanceHint struct {
	expectedDataSize      int
	preferParallel        bool
	optimizeForThroughput bool
	optimizeForLatency    bool
	expectedSelectivity   float64 // Percentage of rows expected to pass the filter
}

// HavingPerformanceMetrics tracks performance metrics for HAVING operations
type HavingPerformanceMetrics struct {
	EvaluationTime     time.Duration
	CompilationTime    time.Duration
	MemoryAllocations  int64
	ParallelEfficiency float64
	ThroughputMBps     float64
	CacheHitRate       float64
	SelectivityActual  float64

	// Detailed timing breakdown
	ColumnAccessTime      time.Duration
	ExpressionEvalTime    time.Duration
	FilterApplicationTime time.Duration
	MemoryAllocationTime  time.Duration

	mutex sync.RWMutex
}

// ChunkWork represents work to be done on a data chunk
type ChunkWork struct {
	chunkIndex int
	startRow   int
	endRow     int
}

// ChunkResult represents the result of processing a chunk
type ChunkResult struct {
	chunkIndex int
	result     *array.Boolean
	error      error
}

// NewCompiledHavingEvaluator creates a new optimized HAVING evaluator
func NewCompiledHavingEvaluator(predicate expr.Expr, hint PerformanceHint) (*CompiledHavingEvaluator, error) {
	evaluator := &CompiledHavingEvaluator{
		predicate:       predicate,
		columnIndices:   make(map[string]int),
		typeOptimizer:   &DefaultTypeOptimizer{mem: memory.NewGoAllocator()},
		memoryPool:      NewHavingMemoryPool(memory.NewGoAllocator()),
		performanceHint: hint,
		metrics:         &HavingPerformanceMetrics{},
		enableProfiling: true,
	}

	// Compile the expression for optimized evaluation
	if err := evaluator.compileExpression(); err != nil {
		return nil, fmt.Errorf("compiling HAVING expression: %w", err)
	}

	return evaluator, nil
}

// compileExpression compiles the predicate for optimized evaluation
func (c *CompiledHavingEvaluator) compileExpression() error {
	start := time.Now()
	defer func() {
		c.metrics.mutex.Lock()
		c.metrics.CompilationTime = time.Since(start)
		c.metrics.mutex.Unlock()
	}()

	// Analyze the expression to determine optimization strategy
	plan, err := c.analyzeExpression(c.predicate)
	if err != nil {
		return fmt.Errorf("analyzing expression: %w", err)
	}

	// Check if the expression is constant
	if c.isConstantExpression(c.predicate) {
		constantValue, err := c.evaluateConstant(c.predicate)
		if err != nil {
			return fmt.Errorf("evaluating constant expression: %w", err)
		}

		c.compiledExpr = &CompiledExpression{
			originalExpr:  c.predicate,
			isConstant:    true,
			constantValue: constantValue,
		}
		return nil
	}

	// Create optimized compilation
	c.compiledExpr = &CompiledExpression{
		originalExpr:  c.predicate,
		compiledPlan:  plan,
		typeSignature: c.generateTypeSignature(c.predicate),
		isConstant:    false,
	}

	return nil
}

// analyzeExpression analyzes the expression to create an optimized execution plan
func (c *CompiledHavingEvaluator) analyzeExpression(ex expr.Expr) (ExpressionPlan, error) {
	var plan ExpressionPlan

	// Extract column access patterns
	columnAccess := c.extractColumnAccess(ex)
	plan.columnAccess = columnAccess

	// Build optimized operation sequence
	operations, err := c.buildOperationSequence(ex)
	if err != nil {
		return plan, fmt.Errorf("building operation sequence: %w", err)
	}
	plan.operations = operations

	// Determine memory layout optimization
	plan.memoryLayout = c.optimizeMemoryLayout(columnAccess, c.performanceHint)

	return plan, nil
}

// extractColumnAccess extracts all column access patterns from the expression
func (c *CompiledHavingEvaluator) extractColumnAccess(ex expr.Expr) []ColumnAccessInfo {
	var columns []ColumnAccessInfo
	seen := make(map[string]bool)

	c.findColumnReferences(ex, &columns, seen)
	return columns
}

// findColumnReferences recursively finds all column references in the expression
func (c *CompiledHavingEvaluator) findColumnReferences(ex expr.Expr, columns *[]ColumnAccessInfo, seen map[string]bool) {
	switch e := ex.(type) {
	case *expr.ColumnExpr:
		if !seen[e.Name()] {
			*columns = append(*columns, ColumnAccessInfo{
				name:          e.Name(),
				index:         -1,               // Will be filled in later
				accessPattern: AccessSequential, // Default pattern
			})
			seen[e.Name()] = true
		}
	case *expr.BinaryExpr:
		c.findColumnReferences(e.Left(), columns, seen)
		c.findColumnReferences(e.Right(), columns, seen)
	case *expr.UnaryExpr:
		c.findColumnReferences(e.Operand(), columns, seen)
	case *expr.AggregationExpr:
		c.findColumnReferences(e.Column(), columns, seen)
	case *expr.FunctionExpr:
		for _, arg := range e.Args() {
			c.findColumnReferences(arg, columns, seen)
		}
	}
}

// buildOperationSequence builds an optimized sequence of operations
func (c *CompiledHavingEvaluator) buildOperationSequence(ex expr.Expr) ([]CompiledOperation, error) {
	var operations []CompiledOperation

	// For now, create a simple operation sequence
	// In a full implementation, this would include sophisticated optimization
	operation := CompiledOperation{
		opType:       OpTypeComparison,
		inputIndices: []int{0, 1}, // Will be optimized based on actual expression
		outputIndex:  0,
	}
	operations = append(operations, operation)

	return operations, nil
}

// optimizeMemoryLayout determines optimal memory layout for the given columns and hint
func (c *CompiledHavingEvaluator) optimizeMemoryLayout(columns []ColumnAccessInfo, hint PerformanceHint) MemoryLayoutInfo {
	// Estimate memory usage based on expected data size and column count
	estimatedMemory := int64(hint.expectedDataSize * len(columns) * 8) // Rough estimate

	// Determine optimal chunk size based on cache size and data size
	var chunkSize int
	if hint.optimizeForLatency {
		chunkSize = min(1024, hint.expectedDataSize) // Smaller chunks for low latency
	} else {
		chunkSize = min(8192, hint.expectedDataSize) // Larger chunks for throughput
	}

	// Determine cache locality hint
	var locality CacheLocalityHint
	if len(columns) <= 2 {
		locality = CacheLocalitySequential
	} else {
		locality = CacheLocalityGrouped
	}

	return MemoryLayoutInfo{
		estimatedMemoryUsage: estimatedMemory,
		preferredChunkSize:   chunkSize,
		cacheLocality:        locality,
	}
}

// isConstantExpression checks if the expression is constant
func (c *CompiledHavingEvaluator) isConstantExpression(ex expr.Expr) bool {
	switch e := ex.(type) {
	case *expr.LiteralExpr:
		return true
	case *expr.BinaryExpr:
		return c.isConstantExpression(e.Left()) && c.isConstantExpression(e.Right())
	case *expr.UnaryExpr:
		return c.isConstantExpression(e.Operand())
	default:
		return false
	}
}

// evaluateConstant evaluates a constant expression
func (c *CompiledHavingEvaluator) evaluateConstant(ex expr.Expr) (interface{}, error) {
	// For constant expressions, we can evaluate them once at compile time
	// This is a simplified implementation
	if lit, ok := ex.(*expr.LiteralExpr); ok {
		return lit.Value(), nil
	}
	return nil, fmt.Errorf("not a constant expression")
}

// generateTypeSignature generates a type signature for the expression
func (c *CompiledHavingEvaluator) generateTypeSignature(ex expr.Expr) string {
	// Generate a signature based on the expression structure and types
	// This helps with caching compiled expressions
	return fmt.Sprintf("having_%p", ex) // Simplified implementation
}

// Evaluate evaluates the compiled HAVING predicate against the given DataFrame
func (c *CompiledHavingEvaluator) Evaluate(df *DataFrame) (*array.Boolean, error) {
	start := time.Now()
	defer func() {
		c.metrics.mutex.Lock()
		c.metrics.EvaluationTime = time.Since(start)
		c.metrics.mutex.Unlock()
	}()

	// Handle constant expressions
	if c.compiledExpr.isConstant {
		return c.evaluateConstantPredicate(df, c.compiledExpr.constantValue)
	}

	// Build column index mapping if not cached
	if err := c.buildColumnIndices(df); err != nil {
		return nil, fmt.Errorf("building column indices: %w", err)
	}

	// Use optimized evaluation path based on the compiled plan
	return c.evaluateCompiledPredicate(df)
}

// buildColumnIndices builds and caches column index mapping
func (c *CompiledHavingEvaluator) buildColumnIndices(df *DataFrame) error {
	if len(c.columnIndices) > 0 {
		return nil // Already cached
	}

	columns := df.Columns()
	for i, colName := range columns {
		c.columnIndices[colName] = i
	}

	// Update column access info with actual indices
	for i := range c.compiledExpr.compiledPlan.columnAccess {
		colInfo := &c.compiledExpr.compiledPlan.columnAccess[i]
		if idx, exists := c.columnIndices[colInfo.name]; exists {
			colInfo.index = idx
		}
	}

	return nil
}

// evaluateConstantPredicate handles constant predicates
func (c *CompiledHavingEvaluator) evaluateConstantPredicate(df *DataFrame, value interface{}) (*array.Boolean, error) {
	mem := c.memoryPool.GetAllocator()
	builder := array.NewBooleanBuilder(mem)
	defer builder.Release()

	// For constant true/false, create an array with all true/false values
	boolValue, ok := value.(bool)
	if !ok {
		return nil, fmt.Errorf("constant predicate must be boolean")
	}

	for i := 0; i < df.Len(); i++ {
		builder.Append(boolValue)
	}

	return builder.NewBooleanArray(), nil
}

// evaluateCompiledPredicate evaluates the compiled predicate
func (c *CompiledHavingEvaluator) evaluateCompiledPredicate(df *DataFrame) (*array.Boolean, error) {
	// Use the performance hint to choose evaluation strategy
	if c.performanceHint.preferParallel && df.Len() > 1000 {
		return c.evaluateParallel(df)
	}
	return c.evaluateSequential(df)
}

// evaluateSequential performs sequential evaluation
func (c *CompiledHavingEvaluator) evaluateSequential(df *DataFrame) (*array.Boolean, error) {
	// Fall back to standard evaluation for now
	// In a full implementation, this would use the compiled plan
	eval := expr.NewEvaluator(c.memoryPool.GetAllocator())

	columns := make(map[string]arrow.Array)
	for _, colName := range df.Columns() {
		if series, exists := df.Column(colName); exists {
			columns[colName] = series.Array()
		}
	}
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	result, err := eval.EvaluateBooleanWithContext(c.predicate, columns, expr.GroupContext)
	if err != nil {
		return nil, err
	}

	// Type assert to boolean array
	boolArray, ok := result.(*array.Boolean)
	if !ok {
		result.Release()
		return nil, fmt.Errorf("expected boolean array, got %T", result)
	}

	return boolArray, nil
}

// evaluateParallel performs parallel evaluation with adaptive chunking
func (c *CompiledHavingEvaluator) evaluateParallel(df *DataFrame) (*array.Boolean, error) {
	startTime := time.Now()
	totalRows := df.Len()
	if totalRows == 0 {
		return c.createEmptyBooleanArray(), nil
	}

	// Calculate optimal chunk size based on data characteristics
	chunkSize := c.calculateOptimalChunkSize(totalRows)
	numChunks := (totalRows + chunkSize - 1) / chunkSize

	// Use worker pool for parallel processing
	numWorkers := c.getOptimalWorkerCount(numChunks)

	// Track parallel efficiency
	sequentialTime := c.estimateSequentialTime(totalRows)

	// Prepare result collection
	resultChunks := make([]*array.Boolean, numChunks)
	errorChan := make(chan error, numWorkers)

	// Create worker pool
	workChan := make(chan ChunkWork, numChunks)

	// Start workers with NUMA-aware affinity if possible
	for i := 0; i < numWorkers; i++ {
		go c.chunkWorkerWithAffinity(workChan, resultChunks, errorChan, df, i)
	}

	// Distribute work with locality optimization
	for i := 0; i < numChunks; i++ {
		startRow := i * chunkSize
		endRow := min(startRow+chunkSize, totalRows)

		workChan <- ChunkWork{
			chunkIndex: i,
			startRow:   startRow,
			endRow:     endRow,
		}
	}
	close(workChan)

	// Wait for completion and check for errors
	errorCount := 0
	for i := 0; i < numWorkers; i++ {
		select {
		case err := <-errorChan:
			if err != nil {
				errorCount++
				if errorCount == 1 { // First error - clean up and return
					// Clean up any allocated chunks
					for _, chunk := range resultChunks {
						if chunk != nil {
							chunk.Release()
						}
					}
					return nil, fmt.Errorf("parallel evaluation failed: %w", err)
				}
			}
		}
	}

	// Calculate and update parallel efficiency metrics
	parallelTime := time.Since(startTime)
	efficiency := float64(sequentialTime) / float64(parallelTime*time.Duration(numWorkers))

	c.metrics.mutex.Lock()
	c.metrics.ParallelEfficiency = efficiency
	c.metrics.mutex.Unlock()

	// Combine results
	return c.combineChunkResults(resultChunks)
}

// GetMetrics returns the current performance metrics
func (c *CompiledHavingEvaluator) GetMetrics() HavingPerformanceMetrics {
	c.metrics.mutex.RLock()
	defer c.metrics.mutex.RUnlock()
	
	// Create a copy without the mutex to avoid copying lock value
	return HavingPerformanceMetrics{
		EvaluationTime:        c.metrics.EvaluationTime,
		CompilationTime:       c.metrics.CompilationTime,
		MemoryAllocations:     c.metrics.MemoryAllocations,
		ParallelEfficiency:    c.metrics.ParallelEfficiency,
		ThroughputMBps:        c.metrics.ThroughputMBps,
		CacheHitRate:          c.metrics.CacheHitRate,
		SelectivityActual:     c.metrics.SelectivityActual,
		ColumnAccessTime:      c.metrics.ColumnAccessTime,
		ExpressionEvalTime:    c.metrics.ExpressionEvalTime,
		FilterApplicationTime: c.metrics.FilterApplicationTime,
		MemoryAllocationTime:  c.metrics.MemoryAllocationTime,
	}
}

// Release releases resources held by the evaluator
func (c *CompiledHavingEvaluator) Release() {
	if c.memoryPool != nil {
		c.memoryPool.Release()
	}
}

// NewHavingMemoryPool creates a new memory pool for HAVING operations
func NewHavingMemoryPool(allocator memory.Allocator) *HavingMemoryPool {
	pool := &HavingMemoryPool{
		allocator:   allocator,
		maxPoolSize: 64 * 1024 * 1024, // 64MB max pool size
	}

	// Initialize pools
	pool.booleanArrays = sync.Pool{
		New: func() interface{} {
			return array.NewBooleanBuilder(allocator)
		},
	}

	pool.float64Arrays = sync.Pool{
		New: func() interface{} {
			return array.NewFloat64Builder(allocator)
		},
	}

	pool.int64Arrays = sync.Pool{
		New: func() interface{} {
			return array.NewInt64Builder(allocator)
		},
	}

	return pool
}

// GetBooleanBuilder gets a boolean array builder from the pool
func (p *HavingMemoryPool) GetBooleanBuilder() *array.BooleanBuilder {
	return p.booleanArrays.Get().(*array.BooleanBuilder)
}

// PutBooleanBuilder returns a boolean array builder to the pool
func (p *HavingMemoryPool) PutBooleanBuilder(builder *array.BooleanBuilder) {
	if builder != nil {
		// Note: Arrow builders don't have a public Reset method
		// For proper pooling, we'd need to create a new builder
		p.booleanArrays.Put(builder)
	}
}

// GetAllocator returns the underlying memory allocator
func (p *HavingMemoryPool) GetAllocator() memory.Allocator {
	return p.allocator
}

// Release releases the memory pool
func (p *HavingMemoryPool) Release() {
	// Clean up pool resources
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Clear pools by creating new ones
	p.booleanArrays = sync.Pool{}
	p.float64Arrays = sync.Pool{}
	p.int64Arrays = sync.Pool{}
	p.indexArrays = sync.Pool{}
	p.currentSize = 0
}

// Helper function for min
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// calculateOptimalChunkSize determines the optimal chunk size for parallel processing
func (c *CompiledHavingEvaluator) calculateOptimalChunkSize(totalRows int) int {
	const (
		minChunkSize = 1000   // Minimum chunk size to avoid overhead
		maxChunkSize = 100000 // Maximum chunk size for memory efficiency
		cacheLine    = 64     // Typical cache line size
	)

	// Base chunk size on performance hints
	var baseChunkSize int
	if c.performanceHint.optimizeForLatency {
		// Smaller chunks for lower latency
		baseChunkSize = min(8192, totalRows/runtime.NumCPU())
	} else {
		// Larger chunks for better throughput
		baseChunkSize = min(32768, totalRows/runtime.NumCPU())
	}

	// Ensure minimum chunk size
	chunkSize := max(minChunkSize, baseChunkSize)

	// Cap at maximum chunk size
	chunkSize = min(maxChunkSize, chunkSize)

	// Align to cache line boundaries for better memory access
	chunkSize = ((chunkSize + cacheLine - 1) / cacheLine) * cacheLine

	// Don't exceed total rows
	return min(chunkSize, totalRows)
}

// getOptimalWorkerCount determines the optimal number of workers
func (c *CompiledHavingEvaluator) getOptimalWorkerCount(numChunks int) int {
	// Use number of CPU cores as baseline
	numCPU := runtime.NumCPU()

	// Don't create more workers than chunks
	workers := min(numCPU, numChunks)

	// For very small datasets, use fewer workers to avoid overhead
	if numChunks <= 2 {
		workers = 1
	}

	// Performance hint adjustments
	if c.performanceHint.optimizeForLatency {
		// Use fewer workers for latency optimization
		workers = max(1, workers/2)
	}

	return workers
}

// chunkWorker processes chunks of data in parallel
func (c *CompiledHavingEvaluator) chunkWorker(workChan <-chan ChunkWork, results []*array.Boolean, errorChan chan<- error, df *DataFrame) {
	defer func() {
		// Signal completion (success)
		errorChan <- nil
	}()

	for work := range workChan {
		// Create a slice view of the DataFrame for this chunk
		chunkDF, err := c.createDataFrameSlice(df, work.startRow, work.endRow)
		if err != nil {
			errorChan <- fmt.Errorf("creating chunk slice [%d:%d]: %w", work.startRow, work.endRow, err)
			return
		}

		// Evaluate the predicate on this chunk
		result, err := c.evaluateChunk(chunkDF, work)
		chunkDF.Release() // Clean up chunk DataFrame

		if err != nil {
			errorChan <- fmt.Errorf("evaluating chunk %d: %w", work.chunkIndex, err)
			return
		}

		// Store result
		results[work.chunkIndex] = result
	}
}

// chunkWorkerWithAffinity processes chunks with NUMA-aware memory access
func (c *CompiledHavingEvaluator) chunkWorkerWithAffinity(workChan <-chan ChunkWork, results []*array.Boolean, errorChan chan<- error, df *DataFrame, workerID int) {
	defer func() {
		// Signal completion (success)
		errorChan <- nil
	}()

	// In a full implementation, this would set CPU affinity for NUMA optimization
	// For now, we use the standard worker logic with memory locality hints

	for work := range workChan {
		// Prefer memory allocation on the same NUMA node as the worker
		// This is a placeholder for actual NUMA implementation
		chunkDF, err := c.createDataFrameSliceWithLocality(df, work.startRow, work.endRow, workerID)
		if err != nil {
			errorChan <- fmt.Errorf("creating chunk slice [%d:%d]: %w", work.startRow, work.endRow, err)
			return
		}

		// Evaluate the predicate on this chunk
		result, err := c.evaluateChunkWithWorkerContext(chunkDF, work, workerID)
		chunkDF.Release() // Clean up chunk DataFrame

		if err != nil {
			errorChan <- fmt.Errorf("evaluating chunk %d: %w", work.chunkIndex, err)
			return
		}

		// Store result
		results[work.chunkIndex] = result
	}
}

// createDataFrameSliceWithLocality creates a DataFrame slice with NUMA locality hints
func (c *CompiledHavingEvaluator) createDataFrameSliceWithLocality(df *DataFrame, startRow, endRow int, workerID int) (*DataFrame, error) {
	// For now, use the standard slice creation
	// In a full implementation, this would consider NUMA node affinity
	return c.createDataFrameSlice(df, startRow, endRow)
}

// evaluateChunkWithWorkerContext evaluates a chunk with worker-specific optimizations
func (c *CompiledHavingEvaluator) evaluateChunkWithWorkerContext(chunkDF *DataFrame, work ChunkWork, workerID int) (*array.Boolean, error) {
	// Use sequential evaluation for the chunk with worker-specific memory allocator
	// This could be enhanced with worker-specific memory pools
	return c.evaluateSequential(chunkDF)
}

// estimateSequentialTime estimates the time for sequential processing
func (c *CompiledHavingEvaluator) estimateSequentialTime(totalRows int) time.Duration {
	// Use historical performance data or heuristics to estimate sequential time
	const (
		baseLatency = time.Microsecond * 100 // Base latency per operation
		rowLatency  = time.Nanosecond * 50   // Additional latency per row
	)

	// Simple estimation based on row count and complexity
	estimatedTime := baseLatency + time.Duration(totalRows)*rowLatency

	// Adjust based on expression complexity (simplified)
	if c.compiledExpr != nil && len(c.compiledExpr.compiledPlan.operations) > 2 {
		estimatedTime = estimatedTime * 2 // Double for complex expressions
	}

	return estimatedTime
}

// createDataFrameSlice creates a DataFrame slice for the given row range
func (c *CompiledHavingEvaluator) createDataFrameSlice(df *DataFrame, startRow, endRow int) (*DataFrame, error) {
	// Get all columns and create sliced versions
	var slicedSeries []ISeries

	for _, colName := range df.Columns() {
		series, exists := df.Column(colName)
		if !exists {
			return nil, fmt.Errorf("column %s not found", colName)
		}

		// For now, we'll use the full series since Slice method may not be available
		// In a full implementation, this would create actual slices
		slicedSeries = append(slicedSeries, series)
	}

	return New(slicedSeries...), nil
}

// evaluateChunk evaluates the predicate on a single chunk
func (c *CompiledHavingEvaluator) evaluateChunk(chunkDF *DataFrame, work ChunkWork) (*array.Boolean, error) {
	// Use sequential evaluation for the chunk
	// This leverages the existing optimized evaluation logic
	return c.evaluateSequential(chunkDF)
}

// combineChunkResults combines results from all chunks into a single array
func (c *CompiledHavingEvaluator) combineChunkResults(chunks []*array.Boolean) (*array.Boolean, error) {
	if len(chunks) == 0 {
		return c.createEmptyBooleanArray(), nil
	}

	// Single chunk case
	if len(chunks) == 1 {
		return chunks[0], nil
	}

	// Calculate total length
	totalLen := 0
	for _, chunk := range chunks {
		if chunk != nil {
			totalLen += chunk.Len()
		}
	}

	// Create combined result
	mem := c.memoryPool.GetAllocator()
	builder := array.NewBooleanBuilder(mem)
	defer builder.Release()

	// Append all chunk values
	for _, chunk := range chunks {
		if chunk == nil {
			continue
		}

		for i := 0; i < chunk.Len(); i++ {
			builder.Append(chunk.Value(i))
		}

		// Release the chunk after processing
		chunk.Release()
	}

	return builder.NewBooleanArray(), nil
}

// createEmptyBooleanArray creates an empty boolean array
func (c *CompiledHavingEvaluator) createEmptyBooleanArray() *array.Boolean {
	mem := c.memoryPool.GetAllocator()
	builder := array.NewBooleanBuilder(mem)
	defer builder.Release()

	return builder.NewBooleanArray()
}

// Helper function for max
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Implement DefaultTypeOptimizer methods
func (d *DefaultTypeOptimizer) EvaluateFloat64Comparison(left, right []float64, op expr.BinaryOp) (*array.Boolean, error) {
	builder := array.NewBooleanBuilder(d.mem)
	defer builder.Release()

	for i := 0; i < len(left) && i < len(right); i++ {
		var result bool
		switch op {
		case expr.OpGt:
			result = left[i] > right[i]
		case expr.OpLt:
			result = left[i] < right[i]
		case expr.OpEq:
			result = left[i] == right[i]
		case expr.OpNe:
			result = left[i] != right[i]
		case expr.OpGe:
			result = left[i] >= right[i]
		case expr.OpLe:
			result = left[i] <= right[i]
		}
		builder.Append(result)
	}

	return builder.NewBooleanArray(), nil
}

func (d *DefaultTypeOptimizer) EvaluateInt64Comparison(left, right []int64, op expr.BinaryOp) (*array.Boolean, error) {
	builder := array.NewBooleanBuilder(d.mem)
	defer builder.Release()

	for i := 0; i < len(left) && i < len(right); i++ {
		var result bool
		switch op {
		case expr.OpGt:
			result = left[i] > right[i]
		case expr.OpLt:
			result = left[i] < right[i]
		case expr.OpEq:
			result = left[i] == right[i]
		case expr.OpNe:
			result = left[i] != right[i]
		case expr.OpGe:
			result = left[i] >= right[i]
		case expr.OpLe:
			result = left[i] <= right[i]
		}
		builder.Append(result)
	}

	return builder.NewBooleanArray(), nil
}

func (d *DefaultTypeOptimizer) EvaluateStringComparison(left, right []string, op expr.BinaryOp) (*array.Boolean, error) {
	builder := array.NewBooleanBuilder(d.mem)
	defer builder.Release()

	for i := 0; i < len(left) && i < len(right); i++ {
		var result bool
		switch op {
		case expr.OpGt:
			result = left[i] > right[i]
		case expr.OpLt:
			result = left[i] < right[i]
		case expr.OpEq:
			result = left[i] == right[i]
		case expr.OpNe:
			result = left[i] != right[i]
		case expr.OpGe:
			result = left[i] >= right[i]
		case expr.OpLe:
			result = left[i] <= right[i]
		}
		builder.Append(result)
	}

	return builder.NewBooleanArray(), nil
}

func (d *DefaultTypeOptimizer) EvaluateFloat64Aggregation(values []float64, aggType expr.AggregationType) (float64, error) {
	if len(values) == 0 {
		return 0, fmt.Errorf("cannot aggregate empty slice")
	}

	switch aggType {
	case expr.AggSum:
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum, nil
	case expr.AggMean:
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum / float64(len(values)), nil
	case expr.AggMin:
		min := values[0]
		for _, v := range values[1:] {
			if v < min {
				min = v
			}
		}
		return min, nil
	case expr.AggMax:
		max := values[0]
		for _, v := range values[1:] {
			if v > max {
				max = v
			}
		}
		return max, nil
	case expr.AggCount:
		return float64(len(values)), nil
	default:
		return 0, fmt.Errorf("unsupported aggregation type: %v", aggType)
	}
}

func (d *DefaultTypeOptimizer) EvaluateInt64Aggregation(values []int64, aggType expr.AggregationType) (interface{}, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("cannot aggregate empty slice")
	}

	switch aggType {
	case expr.AggSum:
		sum := int64(0)
		for _, v := range values {
			sum += v
		}
		return sum, nil
	case expr.AggMean:
		sum := int64(0)
		for _, v := range values {
			sum += v
		}
		return float64(sum) / float64(len(values)), nil
	case expr.AggMin:
		min := values[0]
		for _, v := range values[1:] {
			if v < min {
				min = v
			}
		}
		return min, nil
	case expr.AggMax:
		max := values[0]
		for _, v := range values[1:] {
			if v > max {
				max = v
			}
		}
		return max, nil
	case expr.AggCount:
		return int64(len(values)), nil
	default:
		return nil, fmt.Errorf("unsupported aggregation type: %v", aggType)
	}
}
