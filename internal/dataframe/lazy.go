package dataframe

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/config"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/parallel"
	"github.com/paveg/gorilla/internal/series"
)

// Data type constants for Arrow type name matching.
const (
	dataTypeUTF8      = "utf8"
	dataTypeInt64     = "int64"
	dataTypeFloat64   = "float64"
	dataTypeBool      = "bool"
	dataTypeTimestamp = "timestamp"
)

// FilterMemoryPoolManager manages the global memory pool for filter operations.
type FilterMemoryPoolManager struct {
	pool *parallel.AllocatorPool
	once sync.Once
}

// globalFilterPoolManager is the singleton instance for managing filter memory pools.
var globalFilterPoolManager = &FilterMemoryPoolManager{} //nolint:gochecknoglobals // Required for performance-critical memory pool singleton

// getFilterMemoryPool returns the shared memory pool for filter operations.
func getFilterMemoryPool() *parallel.AllocatorPool {
	globalFilterPoolManager.once.Do(func() {
		globalFilterPoolManager.pool = parallel.NewAllocatorPool(runtime.NumCPU() * allocatorPoolMultiplier)
	})
	return globalFilterPoolManager.pool
}

// LazyOperation represents a deferred operation on a DataFrame.
type LazyOperation interface {
	Apply(df *DataFrame) (*DataFrame, error)
	String() string
}

// FilterMaskOperation represents an operation that can apply boolean filter masks.
type FilterMaskOperation interface {
	filterSeries(originalSeries ISeries, mask *array.Boolean, resultSize int, mem memory.Allocator) (ISeries, error)
	createEmptyDataFrame(df *DataFrame) *DataFrame
}

// applyBooleanFilterMask applies a boolean mask to filter a DataFrame.
// This shared function eliminates duplicate code across FilterOperation, GroupByOperation,
// HavingOperation, and GroupByHavingOperation.
func applyBooleanFilterMask(
	op FilterMaskOperation,
	df *DataFrame,
	mask arrow.Array,
	errorMsg string,
) (*DataFrame, error) {
	boolMask, ok := mask.(*array.Boolean)
	if !ok {
		return nil, errors.New(errorMsg)
	}

	// Count true values to determine result size
	trueCount := 0
	for i := range boolMask.Len() {
		if !boolMask.IsNull(i) && boolMask.Value(i) {
			trueCount++
		}
	}

	if trueCount == 0 {
		// Return empty DataFrame with same structure
		return op.createEmptyDataFrame(df), nil
	}

	// Create filtered series for each column using shared allocator
	var filteredSeries []ISeries
	// Reuse single allocator for all series to reduce memory overhead
	mem := memory.NewGoAllocator()

	for _, colName := range df.Columns() {
		if originalSeries, exists := df.Column(colName); exists {
			filtered, err := op.filterSeries(originalSeries, boolMask, trueCount, mem)
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

// FilterOperation represents a filter operation.
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
	return applyBooleanFilterMask(f, df, mask, "filter mask must be boolean array")
}

func (f *FilterOperation) createEmptyDataFrame(df *DataFrame) *DataFrame {
	mem := memory.NewGoAllocator()
	var emptySeries []ISeries

	for _, colName := range df.Columns() {
		if originalSeries, exists := df.Column(colName); exists {
			// Create empty series with same type
			switch originalSeries.DataType().Name() {
			case dataTypeUTF8:
				emptySeries = append(emptySeries, series.New(colName, []string{}, mem))
			case dataTypeInt64:
				emptySeries = append(emptySeries, series.New(colName, []int64{}, mem))
			case dataTypeFloat64:
				emptySeries = append(emptySeries, series.New(colName, []float64{}, mem))
			case dataTypeBool:
				emptySeries = append(emptySeries, series.New(colName, []bool{}, mem))
			case dataTypeTimestamp:
				emptySeries = append(emptySeries, series.New(colName, []time.Time{}, mem))
			}
		}
	}

	return New(emptySeries...)
}

func (f *FilterOperation) filterSeries(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	mem memory.Allocator,
) (ISeries, error) {
	name := originalSeries.Name()

	switch originalSeries.DataType().Name() {
	case dataTypeUTF8:
		return f.filterStringSeries(originalSeries, mask, resultSize, name, mem)
	case dataTypeInt64:
		return f.filterInt64Series(originalSeries, mask, resultSize, name, mem)
	case dataTypeFloat64:
		return f.filterFloat64Series(originalSeries, mask, resultSize, name, mem)
	case dataTypeBool:
		return f.filterBoolSeries(originalSeries, mask, resultSize, name, mem)
	case dataTypeTimestamp:
		return f.filterTimestampSeries(originalSeries, mask, resultSize, name, mem)
	default:
		return nil, fmt.Errorf("unsupported series type for filtering: %s", originalSeries.DataType().Name())
	}
}

func (f *FilterOperation) filterStringSeries(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	name string,
	mem memory.Allocator,
) (ISeries, error) {
	originalArray := originalSeries.Array()
	defer originalArray.Release()

	stringArray, ok := originalArray.(*array.String)
	if !ok {
		return nil, errors.New("expected string array")
	}

	filteredValues := make([]string, 0, resultSize)
	for i := range mask.Len() {
		if !mask.IsNull(i) && mask.Value(i) {
			if !stringArray.IsNull(i) {
				filteredValues = append(filteredValues, stringArray.Value(i))
			}
		}
	}

	return series.New(name, filteredValues, mem), nil
}

func (f *FilterOperation) filterInt64Series(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	name string,
	mem memory.Allocator,
) (ISeries, error) {
	originalArray := originalSeries.Array()
	defer originalArray.Release()

	intArray, ok := originalArray.(*array.Int64)
	if !ok {
		return nil, errors.New("expected int64 array")
	}

	filteredValues := make([]int64, 0, resultSize)
	for i := range mask.Len() {
		if !mask.IsNull(i) && mask.Value(i) {
			if !intArray.IsNull(i) {
				filteredValues = append(filteredValues, intArray.Value(i))
			}
		}
	}

	return series.New(name, filteredValues, mem), nil
}

func (f *FilterOperation) filterFloat64Series(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	name string,
	mem memory.Allocator,
) (ISeries, error) {
	originalArray := originalSeries.Array()
	defer originalArray.Release()

	floatArray, ok := originalArray.(*array.Float64)
	if !ok {
		return nil, errors.New("expected float64 array")
	}

	filteredValues := make([]float64, 0, resultSize)
	for i := range mask.Len() {
		if !mask.IsNull(i) && mask.Value(i) {
			if !floatArray.IsNull(i) {
				filteredValues = append(filteredValues, floatArray.Value(i))
			}
		}
	}

	return series.New(name, filteredValues, mem), nil
}

func (f *FilterOperation) filterBoolSeries(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	name string,
	mem memory.Allocator,
) (ISeries, error) {
	originalArray := originalSeries.Array()
	defer originalArray.Release()

	boolArray, ok := originalArray.(*array.Boolean)
	if !ok {
		return nil, errors.New("expected boolean array")
	}

	filteredValues := make([]bool, 0, resultSize)
	for i := range mask.Len() {
		if !mask.IsNull(i) && mask.Value(i) {
			if !boolArray.IsNull(i) {
				filteredValues = append(filteredValues, boolArray.Value(i))
			}
		}
	}

	return series.New(name, filteredValues, mem), nil
}

func (f *FilterOperation) filterTimestampSeries(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	name string,
	mem memory.Allocator,
) (ISeries, error) {
	originalArray := originalSeries.Array()
	defer originalArray.Release()

	timestampArray, ok := originalArray.(*array.Timestamp)
	if !ok {
		return nil, errors.New("expected timestamp array")
	}

	filteredValues := make([]time.Time, 0, resultSize)
	for i := range mask.Len() {
		if !mask.IsNull(i) && mask.Value(i) {
			if !timestampArray.IsNull(i) {
				// Convert Arrow timestamp back to time.Time
				timestamp := timestampArray.Value(i)
				nanos := int64(timestamp)
				filteredValues = append(filteredValues, time.Unix(nanos/nanosPerSecond, nanos%nanosPerSecond).UTC())
			}
		}
	}

	return series.New(name, filteredValues, mem), nil
}

func (f *FilterOperation) String() string {
	return fmt.Sprintf("filter(%s)", f.predicate.String())
}

// SelectOperation represents a column selection operation.
type SelectOperation struct {
	columns []string
}

func (s *SelectOperation) Apply(df *DataFrame) (*DataFrame, error) {
	return df.Select(s.columns...), nil
}

func (s *SelectOperation) String() string {
	return fmt.Sprintf("select(%v)", s.columns)
}

// WithColumnOperation represents adding/modifying a column.
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
			copied, copyErr := w.copySeries(originalSeries, mem)
			if copyErr != nil {
				// Clean up
				newSeries.Release()
				for _, s := range allSeries {
					s.Release()
				}
				return nil, fmt.Errorf("copying series %s: %w", colName, copyErr)
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
		return w.createStringSeriesFromArray(name, typedArr, mem)
	case *array.Int64:
		return w.createInt64SeriesFromArray(name, typedArr, mem)
	case *array.Float64:
		return w.createFloat64SeriesFromArray(name, typedArr, mem)
	case *array.Boolean:
		return w.createBoolSeriesFromArray(name, typedArr, mem)
	case *array.Timestamp:
		return w.createTimestampSeriesFromArray(name, typedArr, mem)
	default:
		return nil, fmt.Errorf("unsupported array type for series creation: %T", arr)
	}
}

func (w *WithColumnOperation) copySeries(originalSeries ISeries, mem memory.Allocator) (ISeries, error) {
	name := originalSeries.Name()
	originalArray := originalSeries.Array()
	defer originalArray.Release()

	switch originalSeries.DataType().Name() {
	case dataTypeUTF8:
		return w.copyStringArray(name, originalArray, mem)
	case dataTypeInt64:
		return w.copyInt64Array(name, originalArray, mem)
	case dataTypeFloat64:
		return w.copyFloat64Array(name, originalArray, mem)
	case dataTypeBool:
		return w.copyBoolArray(name, originalArray, mem)
	case dataTypeTimestamp:
		return w.copyTimestampArray(name, originalArray, mem)
	default:
		return nil, fmt.Errorf("unsupported series type for copying: %s", originalSeries.DataType().Name())
	}
}

func (w *WithColumnOperation) copyStringArray(
	name string,
	originalArray arrow.Array,
	mem memory.Allocator,
) (ISeries, error) {
	stringArray, ok := originalArray.(*array.String)
	if !ok {
		return nil, fmt.Errorf("expected string array, got %T", originalArray)
	}
	values := make([]string, stringArray.Len())
	for i := range stringArray.Len() {
		if !stringArray.IsNull(i) {
			values[i] = stringArray.Value(i)
		}
	}
	return series.New(name, values, mem), nil
}

func (w *WithColumnOperation) copyInt64Array(
	name string,
	originalArray arrow.Array,
	mem memory.Allocator,
) (ISeries, error) {
	intArray, ok := originalArray.(*array.Int64)
	if !ok {
		return nil, fmt.Errorf("expected int64 array, got %T", originalArray)
	}
	values := make([]int64, intArray.Len())
	for i := range intArray.Len() {
		if !intArray.IsNull(i) {
			values[i] = intArray.Value(i)
		}
	}
	return series.New(name, values, mem), nil
}

func (w *WithColumnOperation) copyFloat64Array(
	name string,
	originalArray arrow.Array,
	mem memory.Allocator,
) (ISeries, error) {
	floatArray, ok := originalArray.(*array.Float64)
	if !ok {
		return nil, fmt.Errorf("expected float64 array, got %T", originalArray)
	}
	values := make([]float64, floatArray.Len())
	for i := range floatArray.Len() {
		if !floatArray.IsNull(i) {
			values[i] = floatArray.Value(i)
		}
	}
	return series.New(name, values, mem), nil
}

func (w *WithColumnOperation) copyBoolArray(
	name string,
	originalArray arrow.Array,
	mem memory.Allocator,
) (ISeries, error) {
	boolArray, ok := originalArray.(*array.Boolean)
	if !ok {
		return nil, fmt.Errorf("expected boolean array, got %T", originalArray)
	}
	values := make([]bool, boolArray.Len())
	for i := range boolArray.Len() {
		if !boolArray.IsNull(i) {
			values[i] = boolArray.Value(i)
		}
	}
	return series.New(name, values, mem), nil
}

func (w *WithColumnOperation) copyTimestampArray(
	name string,
	originalArray arrow.Array,
	mem memory.Allocator,
) (ISeries, error) {
	timestampArray, ok := originalArray.(*array.Timestamp)
	if !ok {
		return nil, fmt.Errorf("expected timestamp array, got %T", originalArray)
	}
	values := make([]time.Time, timestampArray.Len())
	for i := range timestampArray.Len() {
		if !timestampArray.IsNull(i) {
			// Convert Arrow timestamp back to time.Time
			timestamp := timestampArray.Value(i)
			nanos := int64(timestamp)
			values[i] = time.Unix(nanos/nanosPerSecond, nanos%nanosPerSecond).UTC()
		}
	}
	return series.New(name, values, mem), nil
}

func (w *WithColumnOperation) createStringSeriesFromArray(
	name string,
	arr *array.String,
	mem memory.Allocator,
) (ISeries, error) {
	values := make([]string, arr.Len())
	for i := range arr.Len() {
		if !arr.IsNull(i) {
			values[i] = arr.Value(i)
		}
	}
	return series.New(name, values, mem), nil
}

func (w *WithColumnOperation) createInt64SeriesFromArray(
	name string,
	arr *array.Int64,
	mem memory.Allocator,
) (ISeries, error) {
	values := make([]int64, arr.Len())
	for i := range arr.Len() {
		if !arr.IsNull(i) {
			values[i] = arr.Value(i)
		}
	}
	return series.New(name, values, mem), nil
}

func (w *WithColumnOperation) createFloat64SeriesFromArray(
	name string,
	arr *array.Float64,
	mem memory.Allocator,
) (ISeries, error) {
	values := make([]float64, arr.Len())
	for i := range arr.Len() {
		if !arr.IsNull(i) {
			values[i] = arr.Value(i)
		}
	}
	return series.New(name, values, mem), nil
}

func (w *WithColumnOperation) createBoolSeriesFromArray(
	name string,
	arr *array.Boolean,
	mem memory.Allocator,
) (ISeries, error) {
	values := make([]bool, arr.Len())
	for i := range arr.Len() {
		if !arr.IsNull(i) {
			values[i] = arr.Value(i)
		}
	}
	return series.New(name, values, mem), nil
}

func (w *WithColumnOperation) createTimestampSeriesFromArray(
	name string,
	arr *array.Timestamp,
	mem memory.Allocator,
) (ISeries, error) {
	values := make([]time.Time, arr.Len())
	for i := range arr.Len() {
		if !arr.IsNull(i) {
			// Convert Arrow timestamp back to time.Time
			timestamp := arr.Value(i)
			nanos := int64(timestamp)
			values[i] = time.Unix(nanos/nanosPerSecond, nanos%nanosPerSecond).UTC()
		}
	}
	return series.New(name, values, mem), nil
}

func (w *WithColumnOperation) String() string {
	return fmt.Sprintf("with_column(%s, %s)", w.name, w.expr.String())
}

// SortOperation represents a sort operation.
type SortOperation struct {
	columns   []string
	ascending []bool
}

func (s *SortOperation) Apply(df *DataFrame) (*DataFrame, error) {
	return df.SortBy(s.columns, s.ascending)
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

// GroupByOperation represents a group by and aggregation operation.
type GroupByOperation struct {
	groupByCols     []string
	aggregations    []*expr.AggregationExpr
	havingPredicate expr.Expr // optional HAVING predicate (can be nil)
}

// NewGroupByOperation creates a new GroupByOperation without HAVING predicate (for backward compatibility).
func NewGroupByOperation(groupByCols []string, aggregations []*expr.AggregationExpr) *GroupByOperation {
	return &GroupByOperation{
		groupByCols:     groupByCols,
		aggregations:    aggregations,
		havingPredicate: nil,
	}
}

// NewGroupByOperationWithHaving creates a new GroupByOperation with optional HAVING predicate.
func NewGroupByOperationWithHaving(
	groupByCols []string,
	aggregations []*expr.AggregationExpr,
	havingPredicate expr.Expr,
) *GroupByOperation {
	return &GroupByOperation{
		groupByCols:     groupByCols,
		aggregations:    aggregations,
		havingPredicate: havingPredicate,
	}
}

func (g *GroupByOperation) Apply(df *DataFrame) (*DataFrame, error) {
	if len(g.groupByCols) == 0 || len(g.aggregations) == 0 {
		return New(), nil
	}

	// Create GroupBy object
	gb := df.GroupBy(g.groupByCols...)

	// Perform aggregations
	aggregatedResult := gb.Agg(g.aggregations...)

	// If there's no HAVING predicate, return the aggregated result directly
	if g.havingPredicate == nil {
		return aggregatedResult, nil
	}

	// Apply HAVING filtering to the aggregated result
	defer aggregatedResult.Release()

	// Validate that the predicate contains aggregation functions appropriate for GroupContext
	if err := g.validateHavingPredicate(); err != nil {
		return nil, err
	}

	// Create expression evaluator for GroupContext evaluation
	eval := expr.NewEvaluator(nil)

	// Get column arrays for evaluation against aggregated data
	columns := make(map[string]arrow.Array)
	for _, colName := range aggregatedResult.Columns() {
		if series, exists := aggregatedResult.Column(colName); exists {
			columns[colName] = series.Array()
		}
	}
	// Note: Arrays from series.Array() are managed by the parent series and should not be manually released

	// Evaluate the HAVING predicate in GroupContext
	mask, err := eval.EvaluateBooleanWithContext(g.havingPredicate, columns, expr.GroupContext)
	if err != nil {
		return nil, fmt.Errorf("evaluating HAVING predicate: %w", err)
	}
	defer mask.Release()

	// Apply the filter mask to keep only groups that satisfy the predicate
	return g.applyHavingFilterMask(aggregatedResult, mask)
}

// validateHavingPredicate ensures the predicate is appropriate for GroupContext.
func (g *GroupByOperation) validateHavingPredicate() error {
	if g.havingPredicate == nil {
		return nil
	}

	// Validate that the expression is appropriate for GroupContext
	if err := expr.ValidateExpressionContext(g.havingPredicate, expr.GroupContext); err != nil {
		return fmt.Errorf("HAVING clause must contain aggregation functions: %w", err)
	}
	return nil
}

// applyHavingFilterMask filters the aggregated DataFrame based on the boolean mask.
func (g *GroupByOperation) applyHavingFilterMask(df *DataFrame, mask arrow.Array) (*DataFrame, error) {
	return applyBooleanFilterMask(g, df, mask, "HAVING filter mask must be boolean array")
}

// createEmptyDataFrame creates an empty DataFrame with the same schema.
func (g *GroupByOperation) createEmptyDataFrame(df *DataFrame) *DataFrame {
	mem := memory.NewGoAllocator()
	var emptySeries []ISeries

	for _, colName := range df.Columns() {
		if originalSeries, exists := df.Column(colName); exists {
			// Create empty series with same type
			switch originalSeries.DataType().Name() {
			case dataTypeUTF8:
				emptySeries = append(emptySeries, series.New(colName, []string{}, mem))
			case dataTypeInt64:
				emptySeries = append(emptySeries, series.New(colName, []int64{}, mem))
			case dataTypeFloat64:
				emptySeries = append(emptySeries, series.New(colName, []float64{}, mem))
			case dataTypeBool:
				emptySeries = append(emptySeries, series.New(colName, []bool{}, mem))
			case dataTypeTimestamp:
				emptySeries = append(emptySeries, series.New(colName, []time.Time{}, mem))
			default:
				// For unsupported types, create an empty string series as fallback
				// This ensures the DataFrame structure is preserved even with unexpected types
				emptySeries = append(emptySeries, series.New(colName, []string{}, mem))
			}
		}
	}

	return New(emptySeries...)
}

// filterSeries filters a single series based on the boolean mask.
func (g *GroupByOperation) filterSeries(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	mem memory.Allocator,
) (ISeries, error) {
	name := originalSeries.Name()

	switch originalSeries.DataType().Name() {
	case dataTypeUTF8:
		return g.filterStringSeries(originalSeries, mask, resultSize, name, mem)
	case dataTypeInt64:
		return g.filterInt64Series(originalSeries, mask, resultSize, name, mem)
	case dataTypeFloat64:
		return g.filterFloat64Series(originalSeries, mask, resultSize, name, mem)
	case dataTypeBool:
		return g.filterBoolSeries(originalSeries, mask, resultSize, name, mem)
	case dataTypeTimestamp:
		return g.filterTimestampSeries(originalSeries, mask, resultSize, name, mem)
	default:
		return nil, fmt.Errorf("unsupported series type for HAVING filtering: %s", originalSeries.DataType().Name())
	}
}

// filterStringSeries filters a string series.
func (g *GroupByOperation) filterStringSeries(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	name string,
	mem memory.Allocator,
) (ISeries, error) {
	originalArray := originalSeries.Array()
	// Note: Array from series.Array() is managed by the parent series

	stringArray, ok := originalArray.(*array.String)
	if !ok {
		return nil, errors.New("expected string array")
	}

	filteredValues := make([]string, 0, resultSize)
	for i := range mask.Len() {
		if !mask.IsNull(i) && mask.Value(i) {
			if !stringArray.IsNull(i) {
				filteredValues = append(filteredValues, stringArray.Value(i))
			} else {
				// Handle null values consistently by using empty string as placeholder
				filteredValues = append(filteredValues, "")
			}
		}
	}

	return series.New(name, filteredValues, mem), nil
}

// filterInt64Series filters an int64 series.
func (g *GroupByOperation) filterInt64Series(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	name string,
	mem memory.Allocator,
) (ISeries, error) {
	originalArray := originalSeries.Array()
	// Note: Array from series.Array() is managed by the parent series

	intArray, ok := originalArray.(*array.Int64)
	if !ok {
		return nil, errors.New("expected int64 array")
	}

	filteredValues := make([]int64, 0, resultSize)
	for i := range mask.Len() {
		if !mask.IsNull(i) && mask.Value(i) {
			if !intArray.IsNull(i) {
				filteredValues = append(filteredValues, intArray.Value(i))
			} else {
				// Handle null values consistently by using zero as placeholder
				filteredValues = append(filteredValues, 0)
			}
		}
	}

	return series.New(name, filteredValues, mem), nil
}

// filterFloat64Series filters a float64 series.
func (g *GroupByOperation) filterFloat64Series(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	name string,
	mem memory.Allocator,
) (ISeries, error) {
	originalArray := originalSeries.Array()
	// Note: Array from series.Array() is managed by the parent series

	floatArray, ok := originalArray.(*array.Float64)
	if !ok {
		return nil, errors.New("expected float64 array")
	}

	filteredValues := make([]float64, 0, resultSize)
	for i := range mask.Len() {
		if !mask.IsNull(i) && mask.Value(i) {
			if !floatArray.IsNull(i) {
				filteredValues = append(filteredValues, floatArray.Value(i))
			} else {
				// Handle null values consistently by using zero as placeholder
				filteredValues = append(filteredValues, 0.0)
			}
		}
	}

	return series.New(name, filteredValues, mem), nil
}

// filterBoolSeries filters a boolean series.
func (g *GroupByOperation) filterBoolSeries(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	name string,
	mem memory.Allocator,
) (ISeries, error) {
	originalArray := originalSeries.Array()
	// Note: Array from series.Array() is managed by the parent series

	boolArray, ok := originalArray.(*array.Boolean)
	if !ok {
		return nil, errors.New("expected boolean array")
	}

	filteredValues := make([]bool, 0, resultSize)
	for i := range mask.Len() {
		if !mask.IsNull(i) && mask.Value(i) {
			if !boolArray.IsNull(i) {
				filteredValues = append(filteredValues, boolArray.Value(i))
			} else {
				// Handle null values consistently by using false as placeholder
				filteredValues = append(filteredValues, false)
			}
		}
	}

	return series.New(name, filteredValues, mem), nil
}

func (g *GroupByOperation) filterTimestampSeries(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	name string,
	mem memory.Allocator,
) (ISeries, error) {
	originalArray := originalSeries.Array()
	// Note: Array from series.Array() is managed by the parent series

	timestampArray, ok := originalArray.(*array.Timestamp)
	if !ok {
		return nil, errors.New("expected timestamp array")
	}

	filteredValues := make([]time.Time, 0, resultSize)
	for i := range mask.Len() {
		if !mask.IsNull(i) && mask.Value(i) {
			if !timestampArray.IsNull(i) {
				// Convert Arrow timestamp back to time.Time
				timestamp := timestampArray.Value(i)
				nanos := int64(timestamp)
				filteredValues = append(filteredValues, time.Unix(nanos/nanosPerSecond, nanos%nanosPerSecond).UTC())
			} else {
				// Handle null values consistently by using zero time as placeholder
				filteredValues = append(filteredValues, time.Time{})
			}
		}
	}

	return series.New(name, filteredValues, mem), nil
}

func (g *GroupByOperation) String() string {
	var aggStrs []string
	for _, agg := range g.aggregations {
		aggStrs = append(aggStrs, agg.String())
	}
	result := fmt.Sprintf("group_by(%v).agg(%v)", g.groupByCols, aggStrs)
	if g.havingPredicate != nil {
		result += fmt.Sprintf(".having(%s)", g.havingPredicate.String())
	}
	return result
}

// HavingOperation represents a HAVING clause that filters grouped data based on aggregation predicates.
type HavingOperation struct {
	predicate expr.Expr
}

// NewHavingOperation creates a new HavingOperation with the given predicate.
func NewHavingOperation(predicate expr.Expr) *HavingOperation {
	return &HavingOperation{predicate: predicate}
}

// Apply filters grouped DataFrame based on the aggregation predicate.
func (h *HavingOperation) Apply(df *DataFrame) (*DataFrame, error) {
	// The HAVING operation expects to receive aggregated grouped data
	// It evaluates the predicate against each group's aggregated values

	// Validate that the predicate contains aggregation functions
	if err := h.validatePredicate(); err != nil {
		return nil, err
	}

	// Create expression evaluator with GroupContext
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

	// Evaluate the HAVING predicate in GroupContext
	mask, err := eval.EvaluateBooleanWithContext(h.predicate, columns, expr.GroupContext)
	if err != nil {
		return nil, fmt.Errorf("evaluating HAVING predicate: %w", err)
	}
	defer mask.Release()

	// Apply the filter mask to keep only groups that satisfy the predicate
	return h.applyFilterMask(df, mask)
}

// validatePredicate ensures the predicate contains aggregation functions.
func (h *HavingOperation) validatePredicate() error {
	// Validate that the expression is appropriate for GroupContext
	if err := expr.ValidateExpressionContext(h.predicate, expr.GroupContext); err != nil {
		return fmt.Errorf("HAVING clause must contain aggregation functions: %w", err)
	}
	return nil
}

// applyFilterMask filters the DataFrame based on the boolean mask.
func (h *HavingOperation) applyFilterMask(df *DataFrame, mask arrow.Array) (*DataFrame, error) {
	return applyBooleanFilterMask(h, df, mask, "HAVING filter mask must be boolean array")
}

// createEmptyDataFrame creates an empty DataFrame with the same schema.
func (h *HavingOperation) createEmptyDataFrame(df *DataFrame) *DataFrame {
	mem := memory.NewGoAllocator()
	var emptySeries []ISeries

	for _, colName := range df.Columns() {
		if originalSeries, exists := df.Column(colName); exists {
			// Create empty series with same type
			switch originalSeries.DataType().Name() {
			case dataTypeUTF8:
				emptySeries = append(emptySeries, series.New(colName, []string{}, mem))
			case dataTypeInt64:
				emptySeries = append(emptySeries, series.New(colName, []int64{}, mem))
			case dataTypeFloat64:
				emptySeries = append(emptySeries, series.New(colName, []float64{}, mem))
			case dataTypeBool:
				emptySeries = append(emptySeries, series.New(colName, []bool{}, mem))
			}
		}
	}

	return New(emptySeries...)
}

// filterSeries filters a single series based on the boolean mask.
func (h *HavingOperation) filterSeries(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	mem memory.Allocator,
) (ISeries, error) {
	name := originalSeries.Name()

	switch originalSeries.DataType().Name() {
	case dataTypeUTF8:
		return h.filterStringSeries(originalSeries, mask, resultSize, name, mem)
	case dataTypeInt64:
		return h.filterInt64Series(originalSeries, mask, resultSize, name, mem)
	case dataTypeFloat64:
		return h.filterFloat64Series(originalSeries, mask, resultSize, name, mem)
	case dataTypeBool:
		return h.filterBoolSeries(originalSeries, mask, resultSize, name, mem)
	default:
		return nil, fmt.Errorf("unsupported series type for HAVING filtering: %s", originalSeries.DataType().Name())
	}
}

// filterStringSeries filters a string series.
func (h *HavingOperation) filterStringSeries(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	name string,
	mem memory.Allocator,
) (ISeries, error) {
	originalArray := originalSeries.Array()
	defer originalArray.Release()

	stringArray, ok := originalArray.(*array.String)
	if !ok {
		return nil, errors.New("expected string array")
	}

	filteredValues := make([]string, 0, resultSize)
	for i := range mask.Len() {
		if !mask.IsNull(i) && mask.Value(i) {
			if !stringArray.IsNull(i) {
				filteredValues = append(filteredValues, stringArray.Value(i))
			}
		}
	}

	return series.New(name, filteredValues, mem), nil
}

// filterInt64Series filters an int64 series.
func (h *HavingOperation) filterInt64Series(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	name string,
	mem memory.Allocator,
) (ISeries, error) {
	originalArray := originalSeries.Array()
	defer originalArray.Release()

	intArray, ok := originalArray.(*array.Int64)
	if !ok {
		return nil, errors.New("expected int64 array")
	}

	filteredValues := make([]int64, 0, resultSize)
	for i := range mask.Len() {
		if !mask.IsNull(i) && mask.Value(i) {
			if !intArray.IsNull(i) {
				filteredValues = append(filteredValues, intArray.Value(i))
			}
		}
	}

	return series.New(name, filteredValues, mem), nil
}

// filterFloat64Series filters a float64 series.
func (h *HavingOperation) filterFloat64Series(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	name string,
	mem memory.Allocator,
) (ISeries, error) {
	originalArray := originalSeries.Array()
	defer originalArray.Release()

	floatArray, ok := originalArray.(*array.Float64)
	if !ok {
		return nil, errors.New("expected float64 array")
	}

	filteredValues := make([]float64, 0, resultSize)
	for i := range mask.Len() {
		if !mask.IsNull(i) && mask.Value(i) {
			if !floatArray.IsNull(i) {
				filteredValues = append(filteredValues, floatArray.Value(i))
			}
		}
	}

	return series.New(name, filteredValues, mem), nil
}

// filterBoolSeries filters a boolean series.
func (h *HavingOperation) filterBoolSeries(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	name string,
	mem memory.Allocator,
) (ISeries, error) {
	originalArray := originalSeries.Array()
	defer originalArray.Release()

	boolArray, ok := originalArray.(*array.Boolean)
	if !ok {
		return nil, errors.New("expected boolean array")
	}

	filteredValues := make([]bool, 0, resultSize)
	for i := range mask.Len() {
		if !mask.IsNull(i) && mask.Value(i) {
			if !boolArray.IsNull(i) {
				filteredValues = append(filteredValues, boolArray.Value(i))
			}
		}
	}

	return series.New(name, filteredValues, mem), nil
}

// String returns a string representation of the HAVING operation.
func (h *HavingOperation) String() string {
	return fmt.Sprintf("Having(%s)", h.predicate.String())
}

// Name returns the operation name for debugging.
func (h *HavingOperation) Name() string {
	return "Having"
}

// GroupByHavingOperation combines GroupBy, aggregation, and Having filtering.
type GroupByHavingOperation struct {
	groupByCols []string
	predicate   expr.Expr
	// Cached allocator for reuse to reduce overhead
	cachedAllocator memory.Allocator
	// Cached evaluator for expression reuse
	cachedEvaluator *expr.Evaluator
	// Cached aggregation results to avoid re-evaluation
	cachedAggregations []*expr.AggregationExpr
	// Mutex to protect cached fields from concurrent access
	mu sync.Mutex
}

// Apply performs groupby, extracts aggregations from predicate, performs them, and filters.
func (gh *GroupByHavingOperation) Apply(df *DataFrame) (*DataFrame, error) {
	if len(gh.groupByCols) == 0 {
		return New(), nil
	}

	// Initialize cached allocator if not present (reuse for memory efficiency)
	gh.mu.Lock()
	if gh.cachedAllocator == nil {
		pool := getFilterMemoryPool()
		gh.cachedAllocator = pool.Get()
		if gh.cachedAllocator == nil {
			// Fallback if pool is exhausted
			gh.cachedAllocator = memory.NewGoAllocator()
		}
	}

	// Extract aggregation expressions from the having predicate (cache for reuse)
	if gh.cachedAggregations == nil {
		gh.cachedAggregations = gh.extractAggregations(gh.predicate)
		if len(gh.cachedAggregations) == 0 {
			gh.mu.Unlock()
			return nil, errors.New("HAVING clause must contain aggregation functions")
		}
	}

	// Store cached aggregations for use
	aggregations := gh.cachedAggregations
	gh.mu.Unlock()

	// Create GroupBy object and perform aggregations
	gb := df.GroupBy(gh.groupByCols...)
	aggregatedDF := gb.Agg(aggregations...)
	defer aggregatedDF.Release()

	// Now apply the having filter on the aggregated data
	return gh.applyHavingFilterOptimized(aggregatedDF)
}

// extractAggregations extracts all aggregation expressions from the predicate.
func (gh *GroupByHavingOperation) extractAggregations(ex expr.Expr) []*expr.AggregationExpr {
	var aggregations []*expr.AggregationExpr
	gh.findAggregations(ex, &aggregations)
	return aggregations
}

// findAggregations recursively finds all aggregation expressions.
func (gh *GroupByHavingOperation) findAggregations(ex expr.Expr, aggregations *[]*expr.AggregationExpr) {
	switch e := ex.(type) {
	case *expr.AggregationExpr:
		*aggregations = append(*aggregations, e)
	case *expr.BinaryExpr:
		gh.findAggregations(e.Left(), aggregations)
		gh.findAggregations(e.Right(), aggregations)
	case *expr.UnaryExpr:
		gh.findAggregations(e.Operand(), aggregations)
	case *expr.FunctionExpr:
		for _, arg := range e.Args() {
			gh.findAggregations(arg, aggregations)
		}
	}
}

// applyHavingFilterOptimized applies the having predicate with memory optimizations.
func (gh *GroupByHavingOperation) applyHavingFilterOptimized(df *DataFrame) (*DataFrame, error) {
	// Reuse cached evaluator and allocator to avoid re-initialization overhead
	gh.mu.Lock()
	if gh.cachedEvaluator == nil {
		gh.cachedEvaluator = expr.NewEvaluator(nil)
	}
	evaluator := gh.cachedEvaluator

	// Ensure cached allocator is initialized
	if gh.cachedAllocator == nil {
		pool := getFilterMemoryPool()
		gh.cachedAllocator = pool.Get()
		if gh.cachedAllocator == nil {
			gh.cachedAllocator = memory.NewGoAllocator()
		}
	}
	allocator := gh.cachedAllocator
	gh.mu.Unlock()

	// Get column arrays for evaluation (direct access, no extra copying)
	columns := make(map[string]arrow.Array)
	for _, colName := range df.Columns() {
		if series, exists := df.Column(colName); exists {
			columns[colName] = series.Array()
		}
	}
	// Note: Arrays from series.Array() are managed by the parent series

	// Evaluate the HAVING predicate in GroupContext using cached evaluator
	mask, err := evaluator.EvaluateBooleanWithContext(gh.predicate, columns, expr.GroupContext)
	if err != nil {
		return nil, fmt.Errorf("evaluating HAVING predicate: %w", err)
	}
	defer mask.Release()

	// Apply the filter mask with memory optimizations
	return gh.applyFilterMaskOptimizedWithAllocator(df, mask, allocator)
}

// applyFilterMaskOptimizedWithAllocator applies the boolean mask with memory optimizations using provided allocator.
func (gh *GroupByHavingOperation) applyFilterMaskOptimizedWithAllocator(
	df *DataFrame,
	mask arrow.Array,
	allocator memory.Allocator,
) (*DataFrame, error) {
	boolMask, ok := mask.(*array.Boolean)
	if !ok {
		return nil, errors.New("HAVING filter mask must be boolean array")
	}

	// Count true values to determine result size
	trueCount := 0
	for i := range boolMask.Len() {
		if !boolMask.IsNull(i) && boolMask.Value(i) {
			trueCount++
		}
	}

	if trueCount == 0 {
		// Return empty DataFrame with same structure using provided allocator
		return gh.createEmptyDataFrameWithAllocator(df, allocator), nil
	}

	// Create filtered series for each column using provided allocator
	var filteredSeries []ISeries

	for _, colName := range df.Columns() {
		if originalSeries, exists := df.Column(colName); exists {
			filtered, err := gh.filterSeriesOptimizedWithAllocator(originalSeries, boolMask, trueCount, allocator)
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

// createEmptyDataFrame creates an empty DataFrame with the same schema
//

func (gh *GroupByHavingOperation) createEmptyDataFrame(df *DataFrame) *DataFrame {
	mem := memory.NewGoAllocator()
	var emptySeries []ISeries

	for _, colName := range df.Columns() {
		if originalSeries, exists := df.Column(colName); exists {
			// Create empty series with same type
			switch originalSeries.DataType().Name() {
			case dataTypeUTF8:
				emptySeries = append(emptySeries, series.New(colName, []string{}, mem))
			case dataTypeInt64:
				emptySeries = append(emptySeries, series.New(colName, []int64{}, mem))
			case dataTypeFloat64:
				emptySeries = append(emptySeries, series.New(colName, []float64{}, mem))
			case dataTypeBool:
				emptySeries = append(emptySeries, series.New(colName, []bool{}, mem))
			}
		}
	}

	return New(emptySeries...)
}

// createEmptyDataFrameWithAllocator creates an empty DataFrame using provided allocator.
func (gh *GroupByHavingOperation) createEmptyDataFrameWithAllocator(
	df *DataFrame,
	allocator memory.Allocator,
) *DataFrame {
	var emptySeries []ISeries

	for _, colName := range df.Columns() {
		if originalSeries, exists := df.Column(colName); exists {
			// Create empty series with same type using provided allocator
			switch originalSeries.DataType().Name() {
			case dataTypeUTF8:
				emptySeries = append(emptySeries, series.New(colName, []string{}, allocator))
			case dataTypeInt64:
				emptySeries = append(emptySeries, series.New(colName, []int64{}, allocator))
			case dataTypeFloat64:
				emptySeries = append(emptySeries, series.New(colName, []float64{}, allocator))
			case dataTypeBool:
				emptySeries = append(emptySeries, series.New(colName, []bool{}, allocator))
			}
		}
	}

	return New(emptySeries...)
}

// filterSeries filters a single series based on the boolean mask
//

func (gh *GroupByHavingOperation) filterSeries(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	mem memory.Allocator,
) (ISeries, error) {
	name := originalSeries.Name()

	switch originalSeries.DataType().Name() {
	case dataTypeUTF8:
		return gh.filterStringSeries(originalSeries, mask, resultSize, name, mem)
	case dataTypeInt64:
		return gh.filterInt64Series(originalSeries, mask, resultSize, name, mem)
	case dataTypeFloat64:
		return gh.filterFloat64Series(originalSeries, mask, resultSize, name, mem)
	case dataTypeBool:
		return gh.filterBoolSeries(originalSeries, mask, resultSize, name, mem)
	default:
		return nil, fmt.Errorf("unsupported series type for HAVING filtering: %s", originalSeries.DataType().Name())
	}
}

// filterSeriesOptimizedWithAllocator uses type-specific methods for performance with provided allocator.
func (gh *GroupByHavingOperation) filterSeriesOptimizedWithAllocator(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	allocator memory.Allocator,
) (ISeries, error) {
	name := originalSeries.Name()

	switch originalSeries.DataType().Name() {
	case dataTypeUTF8:
		return gh.filterStringSeriesOptimizedWithAllocator(originalSeries, mask, resultSize, name, allocator)
	case dataTypeInt64:
		return gh.filterInt64SeriesOptimizedWithAllocator(originalSeries, mask, resultSize, name, allocator)
	case dataTypeFloat64:
		return gh.filterFloat64SeriesOptimizedWithAllocator(originalSeries, mask, resultSize, name, allocator)
	case dataTypeBool:
		return gh.filterBoolSeriesOptimizedWithAllocator(originalSeries, mask, resultSize, name, allocator)
	default:
		return nil, fmt.Errorf("unsupported series type for HAVING filtering: %s", originalSeries.DataType().Name())
	}
}

// filterStringSeries filters a string series
//

func (gh *GroupByHavingOperation) filterStringSeries(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	name string,
	mem memory.Allocator,
) (ISeries, error) {
	originalArray := originalSeries.Array()
	defer originalArray.Release()

	stringArray, ok := originalArray.(*array.String)
	if !ok {
		return nil, errors.New("expected string array")
	}

	filteredValues := make([]string, 0, resultSize)
	for i := range mask.Len() {
		if !mask.IsNull(i) && mask.Value(i) {
			if !stringArray.IsNull(i) {
				filteredValues = append(filteredValues, stringArray.Value(i))
			}
		}
	}

	return series.New(name, filteredValues, mem), nil
}

// filterInt64Series filters an int64 series
//

func (gh *GroupByHavingOperation) filterInt64Series(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	name string,
	mem memory.Allocator,
) (ISeries, error) {
	originalArray := originalSeries.Array()
	defer originalArray.Release()

	intArray, ok := originalArray.(*array.Int64)
	if !ok {
		return nil, errors.New("expected int64 array")
	}

	filteredValues := make([]int64, 0, resultSize)
	for i := range mask.Len() {
		if !mask.IsNull(i) && mask.Value(i) {
			if !intArray.IsNull(i) {
				filteredValues = append(filteredValues, intArray.Value(i))
			}
		}
	}

	return series.New(name, filteredValues, mem), nil
}

// filterFloat64Series filters a float64 series
//

func (gh *GroupByHavingOperation) filterFloat64Series(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	name string,
	mem memory.Allocator,
) (ISeries, error) {
	originalArray := originalSeries.Array()
	defer originalArray.Release()

	floatArray, ok := originalArray.(*array.Float64)
	if !ok {
		return nil, errors.New("expected float64 array")
	}

	filteredValues := make([]float64, 0, resultSize)
	for i := range mask.Len() {
		if !mask.IsNull(i) && mask.Value(i) {
			if !floatArray.IsNull(i) {
				filteredValues = append(filteredValues, floatArray.Value(i))
			}
		}
	}

	return series.New(name, filteredValues, mem), nil
}

// filterBoolSeries filters a boolean series
//

func (gh *GroupByHavingOperation) filterBoolSeries(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	name string,
	mem memory.Allocator,
) (ISeries, error) {
	originalArray := originalSeries.Array()
	defer originalArray.Release()

	boolArray, ok := originalArray.(*array.Boolean)
	if !ok {
		return nil, errors.New("expected boolean array")
	}

	filteredValues := make([]bool, 0, resultSize)
	for i := range mask.Len() {
		if !mask.IsNull(i) && mask.Value(i) {
			if !boolArray.IsNull(i) {
				filteredValues = append(filteredValues, boolArray.Value(i))
			}
		}
	}

	return series.New(name, filteredValues, mem), nil
}

// filterStringSeriesOptimizedWithAllocator filters a string series using provided allocator.
func (gh *GroupByHavingOperation) filterStringSeriesOptimizedWithAllocator(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	name string,
	allocator memory.Allocator,
) (ISeries, error) {
	originalArray := originalSeries.Array()
	// Note: Array from series.Array() is managed by the parent series

	stringArray, ok := originalArray.(*array.String)
	if !ok {
		return nil, errors.New("expected string array")
	}

	// Pre-allocate with exact size to reduce memory reallocations
	filteredValues := make([]string, 0, resultSize)
	for i := range mask.Len() {
		if !mask.IsNull(i) && mask.Value(i) {
			if !stringArray.IsNull(i) {
				filteredValues = append(filteredValues, stringArray.Value(i))
			} else {
				// Handle null values consistently by using empty string as placeholder
				filteredValues = append(filteredValues, "")
			}
		}
	}

	return series.New(name, filteredValues, allocator), nil
}

// filterInt64SeriesOptimizedWithAllocator filters an int64 series using provided allocator.
func (gh *GroupByHavingOperation) filterInt64SeriesOptimizedWithAllocator(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	name string,
	allocator memory.Allocator,
) (ISeries, error) {
	originalArray := originalSeries.Array()
	// Note: Array from series.Array() is managed by the parent series

	intArray, ok := originalArray.(*array.Int64)
	if !ok {
		return nil, errors.New("expected int64 array")
	}

	// Pre-allocate with exact size to reduce memory reallocations
	filteredValues := make([]int64, 0, resultSize)
	for i := range mask.Len() {
		if !mask.IsNull(i) && mask.Value(i) {
			if !intArray.IsNull(i) {
				filteredValues = append(filteredValues, intArray.Value(i))
			} else {
				// Handle null values consistently by using zero as placeholder
				filteredValues = append(filteredValues, 0)
			}
		}
	}

	return series.New(name, filteredValues, allocator), nil
}

// filterFloat64SeriesOptimizedWithAllocator filters a float64 series using provided allocator.
func (gh *GroupByHavingOperation) filterFloat64SeriesOptimizedWithAllocator(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	name string,
	allocator memory.Allocator,
) (ISeries, error) {
	originalArray := originalSeries.Array()
	// Note: Array from series.Array() is managed by the parent series

	floatArray, ok := originalArray.(*array.Float64)
	if !ok {
		return nil, errors.New("expected float64 array")
	}

	// Pre-allocate with exact size to reduce memory reallocations
	filteredValues := make([]float64, 0, resultSize)
	for i := range mask.Len() {
		if !mask.IsNull(i) && mask.Value(i) {
			if !floatArray.IsNull(i) {
				filteredValues = append(filteredValues, floatArray.Value(i))
			} else {
				// Handle null values consistently by using zero as placeholder
				filteredValues = append(filteredValues, 0.0)
			}
		}
	}

	return series.New(name, filteredValues, allocator), nil
}

// filterBoolSeriesOptimizedWithAllocator filters a boolean series using provided allocator.
func (gh *GroupByHavingOperation) filterBoolSeriesOptimizedWithAllocator(
	originalSeries ISeries,
	mask *array.Boolean,
	resultSize int,
	name string,
	allocator memory.Allocator,
) (ISeries, error) {
	originalArray := originalSeries.Array()
	// Note: Array from series.Array() is managed by the parent series

	boolArray, ok := originalArray.(*array.Boolean)
	if !ok {
		return nil, errors.New("expected boolean array")
	}

	// Pre-allocate with exact size to reduce memory reallocations
	filteredValues := make([]bool, 0, resultSize)
	for i := range mask.Len() {
		if !mask.IsNull(i) && mask.Value(i) {
			if !boolArray.IsNull(i) {
				filteredValues = append(filteredValues, boolArray.Value(i))
			} else {
				// Handle null values consistently by using false as placeholder
				filteredValues = append(filteredValues, false)
			}
		}
	}

	return series.New(name, filteredValues, allocator), nil
}

// Release returns the cached allocator to the pool for reuse.
func (gh *GroupByHavingOperation) Release() {
	gh.mu.Lock()
	defer gh.mu.Unlock()

	if gh.cachedAllocator != nil {
		pool := getFilterMemoryPool()
		pool.Put(gh.cachedAllocator)
		gh.cachedAllocator = nil
	}
}

// String returns a string representation of the operation.
func (gh *GroupByHavingOperation) String() string {
	return fmt.Sprintf("group_by(%v).having(%s)", gh.groupByCols, gh.predicate.String())
}

// LazyFrame represents a DataFrame with deferred operations for optimized execution.
//
// LazyFrame implements lazy evaluation, building up a query plan of operations
// without executing them immediately. This enables powerful optimizations including:
//   - Query optimization (predicate pushdown, operation fusion)
//   - Automatic parallelization for large datasets
//   - Memory-efficient processing through streaming
//   - Operation reordering for better performance
//
// Operations are only executed when Collect() is called, at which point the
// entire query plan is optimized and executed in the most efficient manner.
//
// Key characteristics:
//   - Zero-cost operation chaining until execution
//   - Automatic parallel execution for datasets > 1000 rows
//   - Query optimization with predicate pushdown
//   - Memory-efficient streaming for large operations
//   - Thread-safe parallel chunk processing
//
// Example usage:
//
//	result, err := df.Lazy().
//	    Filter(expr.Col("age").Gt(expr.Lit(25))).
//	    Select("name", "department").
//	    GroupBy("department").
//	    Agg(expr.Count(expr.Col("*")).As("employee_count")).
//	    Collect()
//
// The above builds a query plan and executes it optimally, potentially
// reordering operations and using parallel processing automatically.
type LazyFrame struct {
	source     *DataFrame
	operations []LazyOperation
	pool       *parallel.WorkerPool
}

// Lazy converts a DataFrame to a LazyFrame for deferred operations.
//
// This method creates a LazyFrame that wraps the current DataFrame, enabling
// lazy evaluation and query optimization. Operations added to the LazyFrame
// are not executed immediately but are instead accumulated in a query plan.
//
// Returns:
//
//	*LazyFrame: A new LazyFrame wrapping this DataFrame with an empty operation queue.
//
// Example:
//
//	// Convert DataFrame to LazyFrame for chained operations
//	lazy := df.Lazy()
//
//	// Chain operations without immediate execution
//	result, err := lazy.
//	    Filter(expr.Col("status").Eq(expr.Lit("active"))).
//	    Select("id", "name").
//	    Collect() // Operations execute here
//
// Performance Benefits:
//   - Operations are optimized before execution
//   - Automatic parallelization for large datasets
//   - Memory-efficient streaming processing
//   - Predicate pushdown reduces data movement
//
// The LazyFrame maintains a reference to the original DataFrame, so both
// objects should be properly released when no longer needed.
func (df *DataFrame) Lazy() *LazyFrame {
	return &LazyFrame{
		source:     df,
		operations: make([]LazyOperation, 0),
		pool:       parallel.NewWorkerPool(0), // Use default number of workers
	}
}

// Filter adds a filter operation to the lazy frame.
func (lf *LazyFrame) Filter(predicate expr.Expr) *LazyFrame {
	operations := make([]LazyOperation, len(lf.operations), len(lf.operations)+1)
	copy(operations, lf.operations)
	operations = append(operations, &FilterOperation{predicate: predicate})
	return &LazyFrame{
		source:     lf.source,
		operations: operations,
		pool:       lf.pool,
	}
}

// Select adds a column selection operation to the lazy frame.
func (lf *LazyFrame) Select(columns ...string) *LazyFrame {
	operations := make([]LazyOperation, len(lf.operations), len(lf.operations)+1)
	copy(operations, lf.operations)
	operations = append(operations, &SelectOperation{columns: columns})
	return &LazyFrame{
		source:     lf.source,
		operations: operations,
		pool:       lf.pool,
	}
}

// WithColumn adds a column creation/modification operation to the lazy frame.
func (lf *LazyFrame) WithColumn(name string, expr expr.Expr) *LazyFrame {
	operations := make([]LazyOperation, len(lf.operations), len(lf.operations)+1)
	copy(operations, lf.operations)
	operations = append(operations, &WithColumnOperation{name: name, expr: expr})
	return &LazyFrame{
		source:     lf.source,
		operations: operations,
		pool:       lf.pool,
	}
}

// Sort adds a sort operation to the lazy frame.
func (lf *LazyFrame) Sort(column string, ascending bool) *LazyFrame {
	return lf.SortBy([]string{column}, []bool{ascending})
}

// SortBy adds a multi-column sort operation to the lazy frame.
func (lf *LazyFrame) SortBy(columns []string, ascending []bool) *LazyFrame {
	operations := make([]LazyOperation, len(lf.operations), len(lf.operations)+1)
	copy(operations, lf.operations)
	operations = append(operations, &SortOperation{columns: columns, ascending: ascending})
	return &LazyFrame{
		source:     lf.source,
		operations: operations,
		pool:       lf.pool,
	}
}

// GroupBy adds a group by and aggregation operation to the lazy frame.
func (lf *LazyFrame) GroupBy(columns ...string) *LazyGroupBy {
	return &LazyGroupBy{
		lazyFrame:   lf,
		groupByCols: columns,
	}
}

// LazyGroupBy represents a lazy groupby operation that can be followed by aggregations.
type LazyGroupBy struct {
	lazyFrame   *LazyFrame
	groupByCols []string
}

// Agg performs aggregation operations and returns a new LazyFrame.
func (lgb *LazyGroupBy) Agg(aggregations ...*expr.AggregationExpr) *LazyFrame {
	operations := make([]LazyOperation, len(lgb.lazyFrame.operations), len(lgb.lazyFrame.operations)+1)
	copy(operations, lgb.lazyFrame.operations)
	operations = append(operations, NewGroupByOperation(lgb.groupByCols, aggregations))
	return &LazyFrame{
		source:     lgb.lazyFrame.source,
		operations: operations,
		pool:       lgb.lazyFrame.pool,
	}
}

// AggWithHaving performs aggregation operations with an optional HAVING predicate and returns a new LazyFrame.
func (lgb *LazyGroupBy) AggWithHaving(havingPredicate expr.Expr, aggregations ...*expr.AggregationExpr) *LazyFrame {
	operations := make([]LazyOperation, len(lgb.lazyFrame.operations), len(lgb.lazyFrame.operations)+1)
	copy(operations, lgb.lazyFrame.operations)
	operations = append(operations, NewGroupByOperationWithHaving(lgb.groupByCols, aggregations, havingPredicate))
	return &LazyFrame{
		source:     lgb.lazyFrame.source,
		operations: operations,
		pool:       lgb.lazyFrame.pool,
	}
}

// Sum creates a sum aggregation for the specified column.
func (lgb *LazyGroupBy) Sum(column string) *LazyFrame {
	return lgb.Agg(expr.Sum(expr.Col(column)))
}

// Count creates a count aggregation for the specified column.
func (lgb *LazyGroupBy) Count(column string) *LazyFrame {
	return lgb.Agg(expr.Count(expr.Col(column)))
}

// Mean creates a mean aggregation for the specified column.
func (lgb *LazyGroupBy) Mean(column string) *LazyFrame {
	return lgb.Agg(expr.Mean(expr.Col(column)))
}

// Min creates a min aggregation for the specified column.
func (lgb *LazyGroupBy) Min(column string) *LazyFrame {
	return lgb.Agg(expr.Min(expr.Col(column)))
}

// Max creates a max aggregation for the specified column.
func (lgb *LazyGroupBy) Max(column string) *LazyFrame {
	return lgb.Agg(expr.Max(expr.Col(column)))
}

// Having adds a HAVING clause to filter grouped data based on aggregation predicates.
func (lgb *LazyGroupBy) Having(predicate expr.Expr) *LazyFrame {
	// For HAVING to work, we need to first perform the GroupBy aggregation
	// and then apply the having filter. We'll create a specialized operation
	// that combines GroupBy + Aggregation + Having

	// Create a GroupByHavingOperation that performs groupby, aggregation, and having together
	operations := make([]LazyOperation, len(lgb.lazyFrame.operations), len(lgb.lazyFrame.operations)+1)
	copy(operations, lgb.lazyFrame.operations)
	operations = append(operations, &GroupByHavingOperation{
		groupByCols: lgb.groupByCols,
		predicate:   predicate,
	})

	return &LazyFrame{
		source:     lgb.lazyFrame.source,
		operations: operations,
		pool:       lgb.lazyFrame.pool,
	}
}

// Collect executes all accumulated operations and returns the final DataFrame.
//
// This method triggers the execution of the entire query plan built up through
// lazy operations. The execution is optimized with:
//   - Query optimization (predicate pushdown, operation fusion)
//   - Automatic parallelization for large datasets (>1000 rows)
//   - Memory-efficient chunk processing
//   - Operation reordering for performance
//
// Parameters:
//
//	ctx: Optional context for cancellation support. If provided, the operation
//	     can be canceled before completion.
//
// Returns:
//
//	*DataFrame: The result of executing all operations in the query plan.
//	error: Any error encountered during execution.
//
// Example:
//
//	result, err := df.Lazy().
//	    Filter(expr.Col("age").Gt(expr.Lit(25))).
//	    GroupBy("department").
//	    Agg(expr.Mean(expr.Col("salary")).As("avg_salary")).
//	    Collect()
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer result.Release()
//
// Performance:
//   - Automatic parallel execution for datasets with >1000 rows
//   - Uses worker pools sized to available CPU cores
//   - Optimized memory allocation through pooling
//   - Query plan optimization reduces unnecessary operations
//
// Memory Management:
// The returned DataFrame is independent and must be released when no longer needed.
// The LazyFrame and its source DataFrame remain valid after Collect().
//
// Cancellation:
// If a context is provided, the operation will check for cancellation and return
// early if the context is canceled. This is useful for long-running operations.
func (lf *LazyFrame) Collect(ctx ...context.Context) (*DataFrame, error) {
	// Handle optional context parameter for backward compatibility
	if len(ctx) > 0 {
		// Check for context cancellation
		select {
		case <-ctx[0].Done():
			return nil, ctx[0].Err()
		default:
		}
	}
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
	parallelThreshold := lf.getParallelThreshold()
	if lf.shouldUseParallelExecution(parallelThreshold) {
		return lf.collectParallelWithOps(optimizedOperations)
	}

	// Fall back to sequential execution for small datasets
	return lf.collectSequentialWithOps(optimizedOperations)
}

// collectSequential applies operations sequentially (original implementation).
func (lf *LazyFrame) collectSequential() (*DataFrame, error) {
	return lf.collectSequentialWithOps(lf.operations)
}

// collectSequentialWithOps applies operations sequentially with provided operations.
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

// calculateChunkSize determines optimal chunk size for parallel processing.
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
// to ensure thread-safety during parallel processing.
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
// FIXED: This method was previously unsafe due to defer originalArray.Release() in parallel contexts.
func (lf *LazyFrame) createIndependentSeries(s ISeries, start, end int, mem memory.Allocator) ISeries {
	// Use the safe implementation that properly handles Arrow memory management
	return lf.createSafeIndependentSeries(s, start, end, mem)
}

// collectParallel implements parallel execution with proper memory management
// Key insight: Arrow arrays are thread-safe for reads, but we need independent chunks
// and must avoid aggressive Release() calls that invalidate shared references.
func (lf *LazyFrame) collectParallel() (*DataFrame, error) {
	return lf.collectParallelWithOps(lf.operations)
}

// collectParallelWithOps implements parallel execution with provided operations.
func (lf *LazyFrame) collectParallelWithOps(operations []LazyOperation) (*DataFrame, error) {
	chunks := lf.createDataChunks()
	processedChunks := lf.processChunksInParallel(chunks, operations)
	return lf.consolidateProcessedChunks(processedChunks)
}

// createDataChunks creates independent data chunks for parallel processing.
func (lf *LazyFrame) createDataChunks() []*DataFrame {
	chunkSize := lf.getChunkSize()
	totalRows := lf.source.Len()

	var chunks []*DataFrame
	for start := 0; start < totalRows; start += chunkSize {
		end := start + chunkSize
		if end > totalRows {
			end = totalRows
		}

		chunk := lf.createIndependentChunk(start, end)
		chunks = append(chunks, chunk)
	}
	return chunks
}

// processChunksInParallel processes data chunks in parallel with operations.
func (lf *LazyFrame) processChunksInParallel(chunks []*DataFrame, operations []LazyOperation) []*DataFrame {
	return parallel.Process(lf.pool, chunks, func(chunk *DataFrame) *DataFrame {
		return lf.applyOperationsToChunk(chunk, operations)
	})
}

// applyOperationsToChunk applies all operations to a single chunk.
func (lf *LazyFrame) applyOperationsToChunk(chunk *DataFrame, operations []LazyOperation) *DataFrame {
	if chunk == nil || chunk.Width() == 0 {
		return New()
	}

	result := chunk
	for _, op := range operations {
		nextResult, err := op.Apply(result)
		if err != nil {
			return New()
		}

		result = nextResult
		if result == nil || result.Width() == 0 {
			return New()
		}
	}
	return result
}

// consolidateProcessedChunks consolidates processed chunks into final result.
func (lf *LazyFrame) consolidateProcessedChunks(processedChunks []*DataFrame) (*DataFrame, error) {
	nonEmptyChunks := lf.filterNonEmptyChunks(processedChunks)

	if len(nonEmptyChunks) == 0 {
		return New(), nil
	}

	if len(nonEmptyChunks) == 1 {
		return nonEmptyChunks[0], nil
	}

	return lf.concatenateChunks(nonEmptyChunks), nil
}

// filterNonEmptyChunks filters out empty chunks before concatenation.
func (lf *LazyFrame) filterNonEmptyChunks(processedChunks []*DataFrame) []*DataFrame {
	var nonEmptyChunks []*DataFrame
	for _, chunk := range processedChunks {
		if chunk != nil && chunk.Width() > 0 && chunk.Len() > 0 {
			nonEmptyChunks = append(nonEmptyChunks, chunk)
		}
	}
	return nonEmptyChunks
}

// concatenateChunks concatenates multiple chunks into a single DataFrame.
func (lf *LazyFrame) concatenateChunks(nonEmptyChunks []*DataFrame) *DataFrame {
	result := nonEmptyChunks[0]
	others := nonEmptyChunks[1:]
	return result.Concat(others...)
}

// String returns a string representation of the lazy frame and its operations.
func (lf *LazyFrame) String() string {
	result := "LazyFrame:\n"
	result += fmt.Sprintf("  source: %s\n", lf.source.String())
	result += "  operations:\n"
	for i, op := range lf.operations {
		result += fmt.Sprintf("    %d. %s\n", i+1, op.String())
	}
	return result
}

// getParallelThreshold returns the parallel threshold based on configuration.
func (lf *LazyFrame) getParallelThreshold() int {
	// Check operation-specific configuration first
	if lf.source.operationConfig != nil {
		// If parallel is forced, use threshold of 1
		if lf.source.operationConfig.ForceParallel {
			return 1
		}
		// If parallel is disabled, use very high threshold
		if lf.source.operationConfig.DisableParallel {
			return int(^uint(0) >> 1) // Max int value
		}
	}

	// Fall back to global configuration
	globalConfig := config.GetGlobalConfig()
	return globalConfig.ParallelThreshold
}

// shouldUseParallelExecution determines if parallel execution should be used.
func (lf *LazyFrame) shouldUseParallelExecution(threshold int) bool {
	if lf.pool == nil {
		return false
	}

	return lf.source.Len() >= threshold
}

// getChunkSize returns the chunk size based on configuration.
func (lf *LazyFrame) getChunkSize() int {
	// Check operation-specific configuration first
	if lf.source.operationConfig != nil && lf.source.operationConfig.CustomChunkSize > 0 {
		return lf.source.operationConfig.CustomChunkSize
	}

	// Fall back to global configuration
	globalConfig := config.GetGlobalConfig()
	if globalConfig.ChunkSize > 0 {
		return globalConfig.ChunkSize
	}

	// Auto-calculate chunk size
	return lf.calculateChunkSize()
}

// Join adds a join operation to the lazy frame.
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

// JoinOperation represents a join operation.
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

// SafeCollectParallel executes all deferred operations using memory-safe parallel processing.
func (lf *LazyFrame) SafeCollectParallel() (*DataFrame, error) {
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

	// Use safe parallel execution
	return lf.safeCollectParallelWithOps(optimizedPlan.operations)
}

// SafeCollectParallelWithMonitoring executes operations with memory monitoring and adaptive parallelism.
func (lf *LazyFrame) SafeCollectParallelWithMonitoring() (*DataFrame, error) {
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

	// Use safe parallel execution with monitoring
	return lf.safeCollectParallelWithMonitoring(optimizedPlan.operations)
}

// safeCollectParallelWithOps implements memory-safe parallel execution.
func (lf *LazyFrame) safeCollectParallelWithOps(operations []LazyOperation) (*DataFrame, error) {
	// Create allocator pool for memory safety
	pool := parallel.NewAllocatorPool(runtime.NumCPU())
	defer pool.Close()

	// Calculate optimal chunk size based on data size and worker count
	chunkSize := lf.getChunkSize()
	totalRows := lf.source.Len()

	// Create safe chunks with independent memory
	var chunks []*DataFrame
	for start := 0; start < totalRows; start += chunkSize {
		end := start + chunkSize
		if end > totalRows {
			end = totalRows
		}

		// Create chunk with safe memory allocation
		chunk := lf.createSafeIndependentChunk(start, end, pool)
		chunks = append(chunks, chunk)
	}

	// Create worker pool for parallel processing
	workerPool := parallel.NewWorkerPool(runtime.NumCPU())
	defer workerPool.Close()

	// Process chunks in parallel using safe infrastructure
	processedChunks := parallel.Process(workerPool, chunks, func(chunk *DataFrame) *DataFrame {
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
			result = nextResult

			// Verify result has valid structure
			if result == nil || result.Width() == 0 {
				return New()
			}
		}
		return result
	})

	// Filter out empty chunks and concatenate
	return lf.concatenateChunks(processedChunks), nil
}

// safeCollectParallelWithMonitoring implements memory-safe parallel execution with monitoring.
func (lf *LazyFrame) safeCollectParallelWithMonitoring(operations []LazyOperation) (*DataFrame, error) {
	monitor, pool := lf.initializeMonitoringComponents()
	defer pool.Close()

	chunks := lf.createMonitoredChunks(monitor, pool)
	processedChunks := lf.processChunksWithMonitoring(chunks, operations, monitor)

	return lf.concatenateChunks(processedChunks), nil
}

// initializeMonitoringComponents creates memory monitor and allocator pool.
func (lf *LazyFrame) initializeMonitoringComponents() (*parallel.MemoryMonitor, *parallel.AllocatorPool) {
	const memoryThresholdMB = 100
	const bytesPerMB = 1024 * 1024

	monitor := parallel.NewMemoryMonitor(memoryThresholdMB*bytesPerMB, runtime.NumCPU())
	pool := parallel.NewAllocatorPool(monitor.AdjustParallelism())
	return monitor, pool
}

// createMonitoredChunks creates chunks with memory monitoring and pressure adjustment.
func (lf *LazyFrame) createMonitoredChunks(monitor *parallel.MemoryMonitor, pool *parallel.AllocatorPool) []*DataFrame {
	chunkSize := lf.calculateAdaptiveChunkSize(monitor)
	totalRows := lf.source.Len()

	var chunks []*DataFrame
	for start := 0; start < totalRows; start += chunkSize {
		end := lf.calculateChunkEnd(start, chunkSize, totalRows, monitor)
		chunk := lf.createSafeIndependentChunk(start, end, pool)
		chunks = append(chunks, chunk)

		// Record memory allocation for monitoring.
		estimatedSize := lf.estimateChunkMemorySize(start, end)
		monitor.RecordAllocation(estimatedSize)
	}
	return chunks
}

// calculateAdaptiveChunkSize determines optimal chunk size based on memory pressure.
func (lf *LazyFrame) calculateAdaptiveChunkSize(monitor *parallel.MemoryMonitor) int {
	baseChunkSize := lf.calculateChunkSize()
	parallelism := monitor.AdjustParallelism()
	adjustedChunkSize := (lf.source.Len() + parallelism - 1) / parallelism

	if adjustedChunkSize > baseChunkSize {
		return baseChunkSize
	}
	return adjustedChunkSize
}

// calculateChunkEnd calculates the end index for a chunk, adjusting for memory pressure.
func (lf *LazyFrame) calculateChunkEnd(start, chunkSize, totalRows int, monitor *parallel.MemoryMonitor) int {
	end := start + chunkSize
	if end > totalRows {
		end = totalRows
	}

	// Check memory pressure and adjust if needed.
	estimatedSize := lf.estimateChunkMemorySize(start, end)
	if !monitor.CanAllocate(estimatedSize) {
		return lf.reduceChunkSizeForMemoryPressure(start, chunkSize)
	}
	return end
}

// reduceChunkSizeForMemoryPressure reduces chunk size when memory pressure is high.
func (lf *LazyFrame) reduceChunkSizeForMemoryPressure(start, chunkSize int) int {
	const chunkSizeReducer = 2
	adjustedEnd := start + chunkSize/chunkSizeReducer
	if adjustedEnd <= start {
		adjustedEnd = start + 1
	}
	return adjustedEnd
}

// estimateChunkMemorySize estimates memory usage for a chunk.
func (lf *LazyFrame) estimateChunkMemorySize(start, end int) int64 {
	const bytesPerValue = 8 // Rough estimate for average value size.
	return int64((end - start) * lf.source.Width() * bytesPerValue)
}

// processChunksWithMonitoring processes chunks in parallel with adaptive worker pool.
func (lf *LazyFrame) processChunksWithMonitoring(
	chunks []*DataFrame,
	operations []LazyOperation,
	monitor *parallel.MemoryMonitor,
) []*DataFrame {
	adaptivePool := parallel.NewWorkerPool(monitor.AdjustParallelism())
	defer adaptivePool.Close()

	return parallel.Process(adaptivePool, chunks, func(chunk *DataFrame) *DataFrame {
		return lf.processChunkOperations(chunk, operations)
	})
}

// processChunkOperations applies all operations to a single chunk.
func (lf *LazyFrame) processChunkOperations(chunk *DataFrame, operations []LazyOperation) *DataFrame {
	if chunk == nil || chunk.Width() == 0 {
		return New()
	}

	result := chunk
	for _, op := range operations {
		nextResult, err := op.Apply(result)
		if err != nil || nextResult == nil || nextResult.Width() == 0 {
			return New()
		}
		result = nextResult
	}
	return result
}

// createSafeIndependentChunk creates a chunk with completely independent data copies using safe allocator pool.
func (lf *LazyFrame) createSafeIndependentChunk(start, end int, pool *parallel.AllocatorPool) *DataFrame {
	if start < 0 || end <= start || start >= lf.source.Len() {
		return New() // Return empty DataFrame for invalid range
	}

	// Clamp end to actual length
	totalRows := lf.source.Len()
	if end > totalRows {
		end = totalRows
	}

	// Create independent series for each column with safe memory allocation
	var independentSeries []ISeries
	processor := parallel.NewChunkProcessor(pool, start) // Use start as chunk ID
	defer processor.Release()

	for _, colName := range lf.source.Columns() {
		if originalSeries, exists := lf.source.Column(colName); exists {
			// Create independent copy of series data for this chunk using safe allocator
			independentSeries = append(
				independentSeries,
				lf.createSafeIndependentSeries(originalSeries, start, end, processor.GetAllocator()),
			)
		}
	}

	return New(independentSeries...)
}

// createSafeIndependentSeries creates a completely independent series copy using safe memory allocation.
func (lf *LazyFrame) createSafeIndependentSeries(s ISeries, start, end int, mem memory.Allocator) ISeries {
	// Get array once and ensure we release it after copying all data
	originalArray := s.Array()
	if originalArray == nil {
		return series.New(s.Name(), []string{}, mem)
	}

	sliceLength := end - start
	if sliceLength <= 0 {
		originalArray.Release()
		return series.New(s.Name(), []string{}, mem)
	}

	// Use shared helper to avoid code duplication
	result := createSlicedSeriesFromArray(s.Name(), originalArray, start, sliceLength, mem)
	originalArray.Release()
	return result
}

// Release releases resources.
func (lf *LazyFrame) Release() {
	if lf.pool != nil {
		lf.pool.Close()
	}
}

// Explain generates an execution plan without executing the operations.
func (lf *LazyFrame) Explain() DebugExecutionPlan {
	return lf.buildExecutionPlan(false)
}

// ExplainAnalyze generates an execution plan and executes it with profiling.
func (lf *LazyFrame) ExplainAnalyze() (DebugExecutionPlan, error) {
	plan := lf.buildExecutionPlan(true)

	// Execute with profiling to get actual statistics
	start := time.Now()
	result, err := lf.collectWithProfiling(&plan)
	plan.Actual.TotalDuration = time.Since(start)

	if err != nil {
		return plan, err
	}
	defer result.Release()

	return plan, nil
}

// buildExecutionPlan builds an execution plan from the operations.
func (lf *LazyFrame) buildExecutionPlan(enableProfiling bool) DebugExecutionPlan {
	plan := DebugExecutionPlan{
		RootNode: &PlanNode{
			ID:          "root",
			Type:        "LazyFrame",
			Description: "Collect operation",
			Cost: PlanCost{
				Estimated: EstimatedCost{
					Rows:   int64(lf.source.Len()),
					Memory: int64(lf.source.Len() * lf.source.Width() * AvgBytesPerCell), // Rough estimate
				},
			},
			Properties: make(map[string]string),
		},
		Estimated: PlanStats{
			TotalRows:   int64(lf.source.Len()),
			TotalMemory: int64(lf.source.Len() * lf.source.Width() * AvgBytesPerCell),
		},
		Metadata: DebugPlanMetadata{
			CreatedAt: time.Now(),
		},
	}

	// Add profiling metadata if enabled
	if enableProfiling {
		plan.RootNode.Properties["profiling"] = "enabled"
		plan.Metadata.OptimizedAt = time.Now()
	}

	// Check if operations warrant parallel execution
	if lf.source.Len() >= ParallelThreshold {
		plan.RootNode.Properties["parallel"] = "true"
		plan.RootNode.Properties["worker_count"] = strconv.Itoa(runtime.NumCPU())
		plan.Estimated.ParallelOps = 1
	}

	// Build child nodes for each operation
	current := plan.RootNode
	estimatedRows := int64(lf.source.Len())

	for i, op := range lf.operations {
		node := &PlanNode{
			ID:          fmt.Sprintf("op_%d", i),
			Type:        lf.getOperationType(op),
			Description: op.String(),
			Cost: PlanCost{
				Estimated: EstimatedCost{
					Rows:   estimatedRows,
					Memory: estimatedRows * int64(lf.source.Width()) * AvgBytesPerCell,
				},
			},
			Properties: make(map[string]string),
		}

		// Estimate selectivity for filters using configurable selectivity
		if _, isFilter := op.(*FilterOperation); isFilter {
			estimatedRows = int64(float64(estimatedRows) * FilterSelectivity)
		}

		current.Children = append(current.Children, node)
		current = node
	}

	// Add scan node
	scanNode := &PlanNode{
		ID:          "scan",
		Type:        "Scan",
		Description: "DataFrame",
		Cost: PlanCost{
			Estimated: EstimatedCost{
				Rows:   int64(lf.source.Len()),
				Memory: int64(lf.source.Len() * lf.source.Width() * AvgBytesPerCell),
			},
		},
		Properties: make(map[string]string),
	}
	current.Children = append(current.Children, scanNode)

	return plan
}

// getOperationType returns the type string for an operation.
func (lf *LazyFrame) getOperationType(op LazyOperation) string {
	switch op.(type) {
	case *FilterOperation:
		return "Filter"
	case *SelectOperation:
		return "Select"
	case *WithColumnOperation:
		return "WithColumn"
	case *GroupByOperation:
		return "GroupBy"
	case *HavingOperation:
		return "Having"
	case *GroupByHavingOperation:
		return "GroupByHaving"
	case *JoinOperation:
		return "Join"
	default:
		return "Unknown"
	}
}

// collectWithProfiling executes the operations with profiling enabled.
func (lf *LazyFrame) collectWithProfiling(plan *DebugExecutionPlan) (*DataFrame, error) {
	// Execute the operations normally and populate actual stats
	result, err := lf.Collect()
	if err != nil {
		return nil, err
	}
	// Fill in actual statistics (simplified for demo)
	plan.Actual.TotalRows = int64(result.Len())
	plan.Actual.TotalMemory = int64(result.Len() * result.Width() * AvgBytesPerCell)
	plan.Metadata.ExecutedAt = time.Now()

	// Update root node with actual statistics
	plan.RootNode.Cost.Actual = ActualCost{
		Rows:   int64(result.Len()),
		Memory: int64(result.Len() * result.Width() * AvgBytesPerCell),
	}

	return result, nil
}
