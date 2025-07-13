package dataframe

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/parallel"
	"github.com/paveg/gorilla/internal/series"
)

// LazyOperation represents a deferred operation on a DataFrame
type LazyOperation interface {
	Apply(df *DataFrame) (*DataFrame, error)
	String() string
}

// FilterOperation represents a filter operation
type FilterOperation struct {
	predicate expr.Expr
}

func (f *FilterOperation) Apply(df *DataFrame) (*DataFrame, error) {
	// Create expression evaluator
	eval := expr.NewEvaluator(nil)

	// Get column arrays for evaluation
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

	// Evaluate the filter predicate
	mask, err := eval.EvaluateBoolean(f.predicate, columns)
	if err != nil {
		return nil, fmt.Errorf("evaluating filter predicate: %w", err)
	}
	defer mask.Release()

	// Apply the filter mask
	return f.applyFilterMask(df, mask)
}

func (f *FilterOperation) applyFilterMask(df *DataFrame, mask arrow.Array) (*DataFrame, error) {
	boolMask, ok := mask.(*array.Boolean)
	if !ok {
		return nil, fmt.Errorf("filter mask must be boolean array")
	}

	// Count true values to determine result size
	trueCount := 0
	for i := 0; i < boolMask.Len(); i++ {
		if !boolMask.IsNull(i) && boolMask.Value(i) {
			trueCount++
		}
	}

	if trueCount == 0 {
		// Return empty DataFrame with same structure
		return f.createEmptyDataFrame(df), nil
	}

	// Create filtered series for each column
	var filteredSeries []ISeries
	mem := memory.NewGoAllocator()

	for _, colName := range df.Columns() {
		if originalSeries, exists := df.Column(colName); exists {
			filtered, err := f.filterSeries(originalSeries, boolMask, trueCount, mem)
			if err != nil {
				// Clean up any created series
				for _, s := range filteredSeries {
					s.Release()
				}
				return nil, fmt.Errorf("filtering column %s: %w", colName, err)
			}
			filteredSeries = append(filteredSeries, filtered)
		}
	}

	return New(filteredSeries...), nil
}

func (f *FilterOperation) createEmptyDataFrame(df *DataFrame) *DataFrame {
	mem := memory.NewGoAllocator()
	var emptySeries []ISeries

	for _, colName := range df.Columns() {
		if originalSeries, exists := df.Column(colName); exists {
			// Create empty series with same type
			switch originalSeries.DataType().Name() {
			case "utf8":
				emptySeries = append(emptySeries, series.New(colName, []string{}, mem))
			case "int64":
				emptySeries = append(emptySeries, series.New(colName, []int64{}, mem))
			case "float64":
				emptySeries = append(emptySeries, series.New(colName, []float64{}, mem))
			case "bool":
				emptySeries = append(emptySeries, series.New(colName, []bool{}, mem))
			}
		}
	}

	return New(emptySeries...)
}

func (f *FilterOperation) filterSeries(originalSeries ISeries, mask *array.Boolean, resultSize int, mem memory.Allocator) (ISeries, error) {
	name := originalSeries.Name()

	switch originalSeries.DataType().Name() {
	case "utf8":
		return f.filterStringSeries(originalSeries, mask, resultSize, name, mem)
	case "int64":
		return f.filterInt64Series(originalSeries, mask, resultSize, name, mem)
	case "float64":
		return f.filterFloat64Series(originalSeries, mask, resultSize, name, mem)
	case "bool":
		return f.filterBoolSeries(originalSeries, mask, resultSize, name, mem)
	default:
		return nil, fmt.Errorf("unsupported series type for filtering: %s", originalSeries.DataType().Name())
	}
}

func (f *FilterOperation) filterStringSeries(originalSeries ISeries, mask *array.Boolean, resultSize int, name string, mem memory.Allocator) (ISeries, error) {
	originalArray := originalSeries.Array()
	defer originalArray.Release()

	stringArray, ok := originalArray.(*array.String)
	if !ok {
		return nil, fmt.Errorf("expected string array")
	}

	filteredValues := make([]string, 0, resultSize)
	for i := 0; i < mask.Len(); i++ {
		if !mask.IsNull(i) && mask.Value(i) {
			if !stringArray.IsNull(i) {
				filteredValues = append(filteredValues, stringArray.Value(i))
			}
		}
	}

	return series.New(name, filteredValues, mem), nil
}

func (f *FilterOperation) filterInt64Series(originalSeries ISeries, mask *array.Boolean, resultSize int, name string, mem memory.Allocator) (ISeries, error) {
	originalArray := originalSeries.Array()
	defer originalArray.Release()

	intArray, ok := originalArray.(*array.Int64)
	if !ok {
		return nil, fmt.Errorf("expected int64 array")
	}

	filteredValues := make([]int64, 0, resultSize)
	for i := 0; i < mask.Len(); i++ {
		if !mask.IsNull(i) && mask.Value(i) {
			if !intArray.IsNull(i) {
				filteredValues = append(filteredValues, intArray.Value(i))
			}
		}
	}

	return series.New(name, filteredValues, mem), nil
}

func (f *FilterOperation) filterFloat64Series(originalSeries ISeries, mask *array.Boolean, resultSize int, name string, mem memory.Allocator) (ISeries, error) {
	originalArray := originalSeries.Array()
	defer originalArray.Release()

	floatArray, ok := originalArray.(*array.Float64)
	if !ok {
		return nil, fmt.Errorf("expected float64 array")
	}

	filteredValues := make([]float64, 0, resultSize)
	for i := 0; i < mask.Len(); i++ {
		if !mask.IsNull(i) && mask.Value(i) {
			if !floatArray.IsNull(i) {
				filteredValues = append(filteredValues, floatArray.Value(i))
			}
		}
	}

	return series.New(name, filteredValues, mem), nil
}

func (f *FilterOperation) filterBoolSeries(originalSeries ISeries, mask *array.Boolean, resultSize int, name string, mem memory.Allocator) (ISeries, error) {
	originalArray := originalSeries.Array()
	defer originalArray.Release()

	boolArray, ok := originalArray.(*array.Boolean)
	if !ok {
		return nil, fmt.Errorf("expected boolean array")
	}

	filteredValues := make([]bool, 0, resultSize)
	for i := 0; i < mask.Len(); i++ {
		if !mask.IsNull(i) && mask.Value(i) {
			if !boolArray.IsNull(i) {
				filteredValues = append(filteredValues, boolArray.Value(i))
			}
		}
	}

	return series.New(name, filteredValues, mem), nil
}

func (f *FilterOperation) String() string {
	return fmt.Sprintf("filter(%s)", f.predicate.String())
}

// SelectOperation represents a column selection operation
type SelectOperation struct {
	columns []string
}

func (s *SelectOperation) Apply(df *DataFrame) (*DataFrame, error) {
	return df.Select(s.columns...), nil
}

func (s *SelectOperation) String() string {
	return fmt.Sprintf("select(%v)", s.columns)
}

// WithColumnOperation represents adding/modifying a column
type WithColumnOperation struct {
	name string
	expr expr.Expr
}

func (w *WithColumnOperation) Apply(df *DataFrame) (*DataFrame, error) {
	// Create expression evaluator
	eval := expr.NewEvaluator(nil)

	// Get column arrays for evaluation
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

	// Evaluate the expression to create new column
	newColumnArray, err := eval.Evaluate(w.expr, columns)
	if err != nil {
		return nil, fmt.Errorf("evaluating new column expression: %w", err)
	}
	defer newColumnArray.Release()

	// Create new series from the evaluated array
	newSeries, err := w.createSeriesFromArray(w.name, newColumnArray)
	if err != nil {
		return nil, fmt.Errorf("creating series from evaluated array: %w", err)
	}

	// Copy existing series and add/replace the new one
	var allSeries []ISeries
	mem := memory.NewGoAllocator()

	// Copy existing columns
	for _, colName := range df.Columns() {
		if colName == w.name {
			// Skip existing column with same name - we'll replace it
			continue
		}
		if originalSeries, exists := df.Column(colName); exists {
			copied, err := w.copySeries(originalSeries, mem)
			if err != nil {
				// Clean up
				newSeries.Release()
				for _, s := range allSeries {
					s.Release()
				}
				return nil, fmt.Errorf("copying series %s: %w", colName, err)
			}
			allSeries = append(allSeries, copied)
		}
	}

	// Add the new column
	allSeries = append(allSeries, newSeries)

	return New(allSeries...), nil
}

func (w *WithColumnOperation) createSeriesFromArray(name string, arr arrow.Array) (ISeries, error) {
	mem := memory.NewGoAllocator()

	switch typedArr := arr.(type) {
	case *array.String:
		values := make([]string, typedArr.Len())
		for i := 0; i < typedArr.Len(); i++ {
			if !typedArr.IsNull(i) {
				values[i] = typedArr.Value(i)
			}
		}
		return series.New(name, values, mem), nil

	case *array.Int64:
		values := make([]int64, typedArr.Len())
		for i := 0; i < typedArr.Len(); i++ {
			if !typedArr.IsNull(i) {
				values[i] = typedArr.Value(i)
			}
		}
		return series.New(name, values, mem), nil

	case *array.Float64:
		values := make([]float64, typedArr.Len())
		for i := 0; i < typedArr.Len(); i++ {
			if !typedArr.IsNull(i) {
				values[i] = typedArr.Value(i)
			}
		}
		return series.New(name, values, mem), nil

	case *array.Boolean:
		values := make([]bool, typedArr.Len())
		for i := 0; i < typedArr.Len(); i++ {
			if !typedArr.IsNull(i) {
				values[i] = typedArr.Value(i)
			}
		}
		return series.New(name, values, mem), nil

	default:
		return nil, fmt.Errorf("unsupported array type for series creation: %T", arr)
	}
}

func (w *WithColumnOperation) copySeries(originalSeries ISeries, mem memory.Allocator) (ISeries, error) {
	name := originalSeries.Name()
	originalArray := originalSeries.Array()
	defer originalArray.Release()

	switch originalSeries.DataType().Name() {
	case "utf8":
		stringArray := originalArray.(*array.String)
		values := make([]string, stringArray.Len())
		for i := 0; i < stringArray.Len(); i++ {
			if !stringArray.IsNull(i) {
				values[i] = stringArray.Value(i)
			}
		}
		return series.New(name, values, mem), nil

	case "int64":
		intArray := originalArray.(*array.Int64)
		values := make([]int64, intArray.Len())
		for i := 0; i < intArray.Len(); i++ {
			if !intArray.IsNull(i) {
				values[i] = intArray.Value(i)
			}
		}
		return series.New(name, values, mem), nil

	case "float64":
		floatArray := originalArray.(*array.Float64)
		values := make([]float64, floatArray.Len())
		for i := 0; i < floatArray.Len(); i++ {
			if !floatArray.IsNull(i) {
				values[i] = floatArray.Value(i)
			}
		}
		return series.New(name, values, mem), nil

	case "bool":
		boolArray := originalArray.(*array.Boolean)
		values := make([]bool, boolArray.Len())
		for i := 0; i < boolArray.Len(); i++ {
			if !boolArray.IsNull(i) {
				values[i] = boolArray.Value(i)
			}
		}
		return series.New(name, values, mem), nil

	default:
		return nil, fmt.Errorf("unsupported series type for copying: %s", originalSeries.DataType().Name())
	}
}

func (w *WithColumnOperation) String() string {
	return fmt.Sprintf("with_column(%s, %s)", w.name, w.expr.String())
}

// SortOperation represents a sort operation
type SortOperation struct {
	columns   []string
	ascending []bool
}

func (s *SortOperation) Apply(df *DataFrame) (*DataFrame, error) {
	return df.SortBy(s.columns, s.ascending), nil
}

func (s *SortOperation) String() string {
	var directions []string
	for i, asc := range s.ascending {
		if asc {
			directions = append(directions, fmt.Sprintf("%s ASC", s.columns[i]))
		} else {
			directions = append(directions, fmt.Sprintf("%s DESC", s.columns[i]))
		}
	}
	return fmt.Sprintf("sort_by(%s)", strings.Join(directions, ", "))
}

// GroupByOperation represents a group by and aggregation operation
type GroupByOperation struct {
	groupByCols  []string
	aggregations []*expr.AggregationExpr
}

func (g *GroupByOperation) Apply(df *DataFrame) (*DataFrame, error) {
	if len(g.groupByCols) == 0 || len(g.aggregations) == 0 {
		return New(), nil
	}

	// Create GroupBy object
	gb := df.GroupBy(g.groupByCols...)

	// Perform aggregations
	return gb.Agg(g.aggregations...), nil
}

func (g *GroupByOperation) String() string {
	var aggStrs []string
	for _, agg := range g.aggregations {
		aggStrs = append(aggStrs, agg.String())
	}
	return fmt.Sprintf("group_by(%v).agg(%v)", g.groupByCols, aggStrs)
}

// LazyFrame holds a DataFrame and a sequence of deferred operations
type LazyFrame struct {
	source     *DataFrame
	operations []LazyOperation
	pool       *parallel.WorkerPool
}

// Lazy converts a DataFrame to a LazyFrame
func (df *DataFrame) Lazy() *LazyFrame {
	return &LazyFrame{
		source:     df,
		operations: make([]LazyOperation, 0),
		pool:       parallel.NewWorkerPool(0), // Use default number of workers
	}
}

// Filter adds a filter operation to the lazy frame
func (lf *LazyFrame) Filter(predicate expr.Expr) *LazyFrame {
	newOps := append(lf.operations, &FilterOperation{predicate: predicate})
	return &LazyFrame{
		source:     lf.source,
		operations: newOps,
		pool:       lf.pool,
	}
}

// Select adds a column selection operation to the lazy frame
func (lf *LazyFrame) Select(columns ...string) *LazyFrame {
	newOps := append(lf.operations, &SelectOperation{columns: columns})
	return &LazyFrame{
		source:     lf.source,
		operations: newOps,
		pool:       lf.pool,
	}
}

// WithColumn adds a column creation/modification operation to the lazy frame
func (lf *LazyFrame) WithColumn(name string, expr expr.Expr) *LazyFrame {
	newOps := append(lf.operations, &WithColumnOperation{name: name, expr: expr})
	return &LazyFrame{
		source:     lf.source,
		operations: newOps,
		pool:       lf.pool,
	}
}

// Sort adds a sort operation to the lazy frame
func (lf *LazyFrame) Sort(column string, ascending bool) *LazyFrame {
	return lf.SortBy([]string{column}, []bool{ascending})
}

// SortBy adds a multi-column sort operation to the lazy frame
func (lf *LazyFrame) SortBy(columns []string, ascending []bool) *LazyFrame {
	newOps := append(lf.operations, &SortOperation{columns: columns, ascending: ascending})
	return &LazyFrame{
		source:     lf.source,
		operations: newOps,
		pool:       lf.pool,
	}
}

// GroupBy adds a group by and aggregation operation to the lazy frame
func (lf *LazyFrame) GroupBy(columns ...string) *LazyGroupBy {
	return &LazyGroupBy{
		lazyFrame:   lf,
		groupByCols: columns,
	}
}

// LazyGroupBy represents a lazy groupby operation that can be followed by aggregations
type LazyGroupBy struct {
	lazyFrame   *LazyFrame
	groupByCols []string
}

// Agg performs aggregation operations and returns a new LazyFrame
func (lgb *LazyGroupBy) Agg(aggregations ...*expr.AggregationExpr) *LazyFrame {
	newOps := append(lgb.lazyFrame.operations, &GroupByOperation{
		groupByCols:  lgb.groupByCols,
		aggregations: aggregations,
	})
	return &LazyFrame{
		source:     lgb.lazyFrame.source,
		operations: newOps,
		pool:       lgb.lazyFrame.pool,
	}
}

// Sum creates a sum aggregation for the specified column
func (lgb *LazyGroupBy) Sum(column string) *LazyFrame {
	return lgb.Agg(expr.Sum(expr.Col(column)))
}

// Count creates a count aggregation for the specified column
func (lgb *LazyGroupBy) Count(column string) *LazyFrame {
	return lgb.Agg(expr.Count(expr.Col(column)))
}

// Mean creates a mean aggregation for the specified column
func (lgb *LazyGroupBy) Mean(column string) *LazyFrame {
	return lgb.Agg(expr.Mean(expr.Col(column)))
}

// Min creates a min aggregation for the specified column
func (lgb *LazyGroupBy) Min(column string) *LazyFrame {
	return lgb.Agg(expr.Min(expr.Col(column)))
}

// Max creates a max aggregation for the specified column
func (lgb *LazyGroupBy) Max(column string) *LazyFrame {
	return lgb.Agg(expr.Max(expr.Col(column)))
}

// Collect executes all deferred operations and returns the resulting DataFrame
func (lf *LazyFrame) Collect() (*DataFrame, error) {
	if lf.source == nil {
		return New(), nil
	}

	// If no operations, return source as-is
	if len(lf.operations) == 0 {
		return lf.source, nil
	}

	// Create execution plan from operations
	plan := CreateExecutionPlan(lf.source, lf.operations)

	// Apply query optimization
	optimizer := NewQueryOptimizer()
	optimizedPlan := optimizer.Optimize(plan)

	// Use optimized operations (create a temporary LazyFrame copy)
	optimizedOperations := optimizedPlan.operations

	// Use parallel execution for larger datasets
	const minRowsForParallel = 1000
	if lf.source.Len() >= minRowsForParallel && lf.pool != nil {
		return lf.collectParallelWithOps(optimizedOperations)
	}

	// Fall back to sequential execution for small datasets
	return lf.collectSequentialWithOps(optimizedOperations)
}

// collectSequential applies operations sequentially (original implementation)
func (lf *LazyFrame) collectSequential() (*DataFrame, error) {
	return lf.collectSequentialWithOps(lf.operations)
}

// collectSequentialWithOps applies operations sequentially with provided operations
func (lf *LazyFrame) collectSequentialWithOps(operations []LazyOperation) (*DataFrame, error) {
	current := lf.source

	for _, op := range operations {
		result, err := op.Apply(current)
		if err != nil {
			return nil, err
		}
		current = result
	}

	return current, nil
}

// calculateChunkSize determines optimal chunk size for parallel processing
func (lf *LazyFrame) calculateChunkSize() int {
	totalRows := lf.source.Len()
	workerCount := runtime.NumCPU()

	// Base chunk size: aim for 2-4 chunks per worker
	const chunksPerWorker = 3
	baseChunkSize := totalRows / (workerCount * chunksPerWorker)

	// Minimum chunk size to avoid overhead
	const minChunkSize = 500
	if baseChunkSize < minChunkSize {
		baseChunkSize = minChunkSize
	}

	// Maximum chunk size to ensure good parallelism
	const maxChunkSize = 10000
	if baseChunkSize > maxChunkSize {
		baseChunkSize = maxChunkSize
	}

	return baseChunkSize
}

// createIndependentChunk creates a chunk with completely independent data copies
// to ensure thread-safety during parallel processing
func (lf *LazyFrame) createIndependentChunk(start, end int) *DataFrame {
	if start < 0 || end <= start || start >= lf.source.Len() {
		return New() // Return empty DataFrame for invalid range
	}

	// Clamp end to actual length
	totalRows := lf.source.Len()
	if end > totalRows {
		end = totalRows
	}

	// Create independent series for each column with deep data copying
	var independentSeries []ISeries
	mem := memory.NewGoAllocator() // Dedicated allocator for this chunk

	for _, colName := range lf.source.Columns() {
		if originalSeries, exists := lf.source.Column(colName); exists {
			// Create independent copy of series data for this chunk
			independentSeries = append(independentSeries, lf.createIndependentSeries(originalSeries, start, end, mem))
		}
	}

	return New(independentSeries...)
}

// createIndependentSeries creates a completely independent series copy with no shared memory references
func (lf *LazyFrame) createIndependentSeries(s ISeries, start, end int, mem memory.Allocator) ISeries {
	// Get array once and ensure we release it after copying all data
	originalArray := s.Array()
	if originalArray == nil {
		return series.New(s.Name(), []string{}, mem)
	}
	defer originalArray.Release()

	sliceLength := end - start
	if sliceLength <= 0 {
		return series.New(s.Name(), []string{}, mem)
	}

	// Use shared helper to avoid code duplication
	return createSlicedSeriesFromArray(s.Name(), originalArray, start, sliceLength, mem)
}

// collectParallel implements parallel execution with proper memory management
// Key insight: Arrow arrays are thread-safe for reads, but we need independent chunks
// and must avoid aggressive Release() calls that invalidate shared references
func (lf *LazyFrame) collectParallel() (*DataFrame, error) {
	return lf.collectParallelWithOps(lf.operations)
}

// collectParallelWithOps implements parallel execution with provided operations
func (lf *LazyFrame) collectParallelWithOps(operations []LazyOperation) (*DataFrame, error) {
	// Calculate optimal chunk size based on data size and worker count
	chunkSize := lf.calculateChunkSize()
	totalRows := lf.source.Len()

	// Create independent chunks sequentially to avoid concurrent memory access
	var chunks []*DataFrame
	for start := 0; start < totalRows; start += chunkSize {
		end := start + chunkSize
		if end > totalRows {
			end = totalRows
		}

		// Create chunk with independent data copies
		chunk := lf.createIndependentChunk(start, end)
		chunks = append(chunks, chunk)
	}

	// Process chunks in parallel - each chunk now has independent memory
	processedChunks := parallel.Process(lf.pool, chunks, func(chunk *DataFrame) *DataFrame {
		if chunk == nil || chunk.Width() == 0 {
			return New()
		}

		result := chunk
		// Apply all operations to this chunk
		for _, op := range operations {
			nextResult, err := op.Apply(result)
			if err != nil {
				// Return empty DataFrame on error
				return New()
			}

			// Don't aggressively release - this was causing the memory corruption
			// Let Go's garbage collector handle cleanup
			result = nextResult

			// Verify result has valid structure
			if result == nil || result.Width() == 0 {
				return New()
			}
		}
		return result
	})

	// Filter out empty chunks before concatenation
	var nonEmptyChunks []*DataFrame
	for _, chunk := range processedChunks {
		if chunk != nil && chunk.Width() > 0 && chunk.Len() > 0 {
			nonEmptyChunks = append(nonEmptyChunks, chunk)
		}
	}

	if len(nonEmptyChunks) == 0 {
		return New(), nil
	}

	if len(nonEmptyChunks) == 1 {
		return nonEmptyChunks[0], nil
	}

	// Concatenate all non-empty chunks
	result := nonEmptyChunks[0]
	others := nonEmptyChunks[1:]

	return result.Concat(others...), nil
}

// String returns a string representation of the lazy frame and its operations
func (lf *LazyFrame) String() string {
	result := "LazyFrame:\n"
	result += fmt.Sprintf("  source: %s\n", lf.source.String())
	result += "  operations:\n"
	for i, op := range lf.operations {
		result += fmt.Sprintf("    %d. %s\n", i+1, op.String())
	}
	return result
}

// Join adds a join operation to the lazy frame
func (lf *LazyFrame) Join(right *LazyFrame, options *JoinOptions) *LazyFrame {
	op := &JoinOperation{
		right:   right,
		options: options,
	}

	newOps := make([]LazyOperation, len(lf.operations)+1)
	copy(newOps, lf.operations)
	newOps[len(lf.operations)] = op

	return &LazyFrame{
		source:     lf.source,
		operations: newOps,
		pool:       lf.pool,
	}
}

// JoinOperation represents a join operation
type JoinOperation struct {
	right   *LazyFrame
	options *JoinOptions
}

func (j *JoinOperation) Apply(df *DataFrame) (*DataFrame, error) {
	// First collect the right LazyFrame to get the actual DataFrame
	rightDF, err := j.right.Collect()
	if err != nil {
		return nil, fmt.Errorf("collecting right DataFrame for join: %w", err)
	}
	defer rightDF.Release()

	// Perform the join
	return df.Join(rightDF, j.options)
}

func (j *JoinOperation) String() string {
	joinTypeName := ""
	switch j.options.Type {
	case InnerJoin:
		joinTypeName = "INNER"
	case LeftJoin:
		joinTypeName = "LEFT"
	case RightJoin:
		joinTypeName = "RIGHT"
	case FullOuterJoin:
		joinTypeName = "FULL OUTER"
	}

	if j.options.LeftKey != "" && j.options.RightKey != "" {
		return fmt.Sprintf("%s JOIN ON %s = %s", joinTypeName, j.options.LeftKey, j.options.RightKey)
	}

	return fmt.Sprintf("%s JOIN ON %v = %v", joinTypeName, j.options.LeftKeys, j.options.RightKeys)
}

// Release releases resources
func (lf *LazyFrame) Release() {
	if lf.pool != nil {
		lf.pool.Close()
	}
}
