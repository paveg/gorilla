package dataframe

import (
	"fmt"

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

// Collect executes all deferred operations and returns the resulting DataFrame
func (lf *LazyFrame) Collect() (*DataFrame, error) {
	current := lf.source

	// TODO: Implement parallel execution pipeline for LazyFrame.Collect()
	// This is the most critical performance optimization needed:
	// 1. Split DataFrame into chunks based on row ranges
	// 2. Create tasks that apply the full operation pipeline to each chunk
	// 3. Use parallel.Process to execute tasks concurrently across worker pool
	// 4. Concatenate results from all chunks into final DataFrame
	// 5. Implement query optimization (predicate pushdown, projection pushdown)

	// Currently: Apply operations sequentially (functional but not parallel)
	for _, op := range lf.operations {
		result, err := op.Apply(current)
		if err != nil {
			return nil, err
		}
		current = result
	}

	return current, nil
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

// Release releases resources
func (lf *LazyFrame) Release() {
	if lf.pool != nil {
		lf.pool.Close()
	}
}
