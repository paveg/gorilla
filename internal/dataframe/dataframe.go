// Package dataframe provides high-performance DataFrame operations
package dataframe

import (
	"cmp"
	"fmt"
	"math"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/config"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/parallel"
	"github.com/paveg/gorilla/internal/series"
	"github.com/paveg/gorilla/internal/validation"
)

const (
	nanosPerSecond            = 1e9
	minChunkSizeForJoin       = 100 // Minimum chunk size to avoid excessive overhead in parallel joins
	groupParallelThresholdDiv = 10  // Divisor for calculating group parallel threshold from row threshold
)

// DataFrame represents a table of data with typed columns
type DataFrame struct {
	columns         map[string]ISeries
	order           []string                // Maintains column order
	operationConfig *config.OperationConfig // Optional per-operation configuration
}

// JoinType represents the type of join operation
type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
	FullOuterJoin
)

// JoinOptions specifies parameters for join operations
type JoinOptions struct {
	Type      JoinType
	LeftKey   string   // Single join key for left DataFrame
	RightKey  string   // Single join key for right DataFrame
	LeftKeys  []string // Multiple join keys for left DataFrame
	RightKeys []string // Multiple join keys for right DataFrame
}

// New creates a new DataFrame from a slice of ISeries
func New(series ...ISeries) *DataFrame {
	columns := make(map[string]ISeries)
	order := make([]string, 0, len(series))

	for _, s := range series {
		name := s.Name()
		columns[name] = s
		order = append(order, name)
	}

	return &DataFrame{
		columns:         columns,
		order:           order,
		operationConfig: nil,
	}
}

// Columns returns the names of all columns in order
func (df *DataFrame) Columns() []string {
	if len(df.order) == 0 {
		return []string{}
	}
	return append([]string(nil), df.order...)
}

// Len returns the number of rows (assumes all columns have same length)
func (df *DataFrame) Len() int {
	if len(df.columns) == 0 {
		return 0
	}

	// Get the first column in order to determine length
	if len(df.order) > 0 {
		if series, exists := df.columns[df.order[0]]; exists {
			return series.Len()
		}
	}

	// Fallback: get any column to determine length
	for _, series := range df.columns {
		return series.Len()
	}
	return 0
}

// NumRows returns the number of rows (alias for Len for compatibility)
func (df *DataFrame) NumRows() int {
	return df.Len()
}

// Width returns the number of columns
func (df *DataFrame) Width() int {
	return len(df.columns)
}

// Column returns the series for the given column name
func (df *DataFrame) Column(name string) (ISeries, bool) {
	series, exists := df.columns[name]
	return series, exists
}

// Select returns a new DataFrame with only the specified columns
func (df *DataFrame) Select(names ...string) *DataFrame {
	newColumns := make(map[string]ISeries)
	newOrder := make([]string, 0, len(names))

	for _, name := range names {
		if series, exists := df.columns[name]; exists {
			newColumns[name] = series
			newOrder = append(newOrder, name)
		}
	}

	return &DataFrame{
		columns: newColumns,
		order:   newOrder,
	}
}

// Drop returns a new DataFrame without the specified columns
func (df *DataFrame) Drop(names ...string) *DataFrame {
	dropSet := make(map[string]bool)
	for _, name := range names {
		dropSet[name] = true
	}

	newColumns := make(map[string]ISeries)
	newOrder := make([]string, 0, len(df.order))

	for _, name := range df.order {
		if !dropSet[name] {
			newColumns[name] = df.columns[name]
			newOrder = append(newOrder, name)
		}
	}

	return &DataFrame{
		columns: newColumns,
		order:   newOrder,
	}
}

// HasColumn checks if a column exists
func (df *DataFrame) HasColumn(name string) bool {
	_, exists := df.columns[name]
	return exists
}

// WithConfig returns a new DataFrame with the specified operation configuration
func (df *DataFrame) WithConfig(opConfig config.OperationConfig) *DataFrame {
	return &DataFrame{
		columns:         df.columns,
		order:           df.order,
		operationConfig: &opConfig,
	}
}

// String returns a string representation of the DataFrame
func (df *DataFrame) String() string {
	if len(df.columns) == 0 {
		return "DataFrame[empty]"
	}

	parts := []string{fmt.Sprintf("DataFrame[%dx%d]", df.Len(), df.Width())}

	for _, name := range df.order {
		series := df.columns[name]
		parts = append(parts, fmt.Sprintf("  %s: %s", name, series.DataType().String()))
	}

	return strings.Join(parts, "\n")
}

// Slice creates a new DataFrame containing rows from start (inclusive) to end (exclusive)
func (df *DataFrame) Slice(start, end int) *DataFrame {
	if start < 0 || end < 0 || start >= end {
		return New() // Return empty DataFrame for invalid range
	}

	length := df.Len()
	if start >= length {
		return New() // Return empty DataFrame if start is beyond data
	}

	// Clamp end to actual length
	if end > length {
		end = length
	}

	var slicedSeries []ISeries
	for _, colName := range df.order {
		if series, exists := df.columns[colName]; exists {
			slicedSeries = append(slicedSeries, df.sliceSeries(series, start, end))
		}
	}

	return New(slicedSeries...)
}

// Sort returns a new DataFrame sorted by the specified column
func (df *DataFrame) Sort(column string, ascending bool) (*DataFrame, error) {
	if err := validation.ValidateColumns(df, "Sort", column); err != nil {
		return nil, err
	}

	return df.SortBy([]string{column}, []bool{ascending})
}

// SortBy returns a new DataFrame sorted by multiple columns
func (df *DataFrame) SortBy(columns []string, ascending []bool) (*DataFrame, error) {
	// Validate input arrays have the same length
	if err := validation.ValidateLength(len(columns), len(ascending), "SortBy",
		"columns and ascending arrays"); err != nil {
		return nil, err
	}

	// Validate all columns exist
	if err := validation.ValidateColumns(df, "SortBy", columns...); err != nil {
		return nil, err
	}

	// Get all row indices
	rowCount := df.Len()
	if rowCount == 0 {
		return New(), nil // Return empty DataFrame
	}

	// Create sorted indices using parallel or sequential approach
	parallelThreshold := config.GetGlobalConfig().ParallelThreshold
	var sortedIndices []int

	if rowCount >= parallelThreshold {
		sortedIndices = df.sortParallel(columns, ascending)
	} else {
		sortedIndices = df.sortSequential(columns, ascending)
	}

	// Build new DataFrame with sorted data
	return df.buildSortedDataFrame(sortedIndices), nil
}

// sliceSeries creates a new series containing elements from start to end
// This method is thread-safe and creates completely independent data copies
func (df *DataFrame) sliceSeries(s ISeries, start, end int) ISeries {
	originalArray := s.Array()
	if originalArray == nil {
		// Return empty series if source array is nil
		mem := memory.NewGoAllocator()
		return series.New(s.Name(), []string{}, mem)
	}
	defer originalArray.Release()

	sliceLength := end - start
	if sliceLength <= 0 {
		// Return empty series for invalid range
		mem := memory.NewGoAllocator()
		return series.New(s.Name(), []string{}, mem)
	}

	// Use dedicated memory allocator to avoid sharing across goroutines
	mem := memory.NewGoAllocator()

	return createSlicedSeriesFromArray(s.Name(), originalArray, start, sliceLength, mem)
}

// createSlicedSeriesFromArray creates a series from an array slice with independent memory
func createSlicedSeriesFromArray(
	name string, originalArray arrow.Array, start, length int, mem memory.Allocator,
) ISeries {
	switch typedArr := originalArray.(type) {
	case *array.String:
		return createSlicedStringSeries(name, typedArr, start, length, mem)
	case *array.Int64:
		return createSlicedInt64Series(name, typedArr, start, length, mem)
	case *array.Int32:
		return createSlicedInt32Series(name, typedArr, start, length, mem)
	case *array.Int16:
		return createSlicedInt16Series(name, typedArr, start, length, mem)
	case *array.Int8:
		return createSlicedInt8Series(name, typedArr, start, length, mem)
	case *array.Uint64:
		return createSlicedUint64Series(name, typedArr, start, length, mem)
	case *array.Uint32:
		return createSlicedUint32Series(name, typedArr, start, length, mem)
	case *array.Uint16:
		return createSlicedUint16Series(name, typedArr, start, length, mem)
	case *array.Uint8:
		return createSlicedUint8Series(name, typedArr, start, length, mem)
	case *array.Float64:
		return createSlicedFloat64Series(name, typedArr, start, length, mem)
	case *array.Float32:
		return createSlicedFloat32Series(name, typedArr, start, length, mem)
	case *array.Boolean:
		return createSlicedBoolSeries(name, typedArr, start, length, mem)
	case *array.Timestamp:
		return createSlicedTimestampSeries(name, typedArr, start, length, mem)
	default:
		// For unsupported types, return empty series
		return series.New(name, []string{}, mem)
	}
}

// ArrowValueArray represents an Arrow array with value access
type ArrowValueArray[T any] interface {
	Len() int
	IsNull(int) bool
	Value(int) T
}

// createSlicedSeries creates a typed series slice using generics
func createSlicedSeries[T any](
	name string, typedArr ArrowValueArray[T], start, length int, mem memory.Allocator,
) ISeries {
	values := make([]T, length)
	for i := 0; i < length; i++ {
		srcIndex := start + i
		if srcIndex < typedArr.Len() && !typedArr.IsNull(srcIndex) {
			values[i] = typedArr.Value(srcIndex)
		}
	}
	return series.New(name, values, mem)
}

// createSlicedStringSeries creates a string series slice
func createSlicedStringSeries(name string, typedArr *array.String, start, length int, mem memory.Allocator) ISeries {
	return createSlicedSeries(name, typedArr, start, length, mem)
}

// createSlicedInt64Series creates an int64 series slice
func createSlicedInt64Series(name string, typedArr *array.Int64, start, length int, mem memory.Allocator) ISeries {
	return createSlicedSeries(name, typedArr, start, length, mem)
}

// createSlicedInt32Series creates an int32 series slice
func createSlicedInt32Series(name string, typedArr *array.Int32, start, length int, mem memory.Allocator) ISeries {
	return createSlicedSeries(name, typedArr, start, length, mem)
}

// createSlicedInt16Series creates an int16 series slice
func createSlicedInt16Series(name string, typedArr *array.Int16, start, length int, mem memory.Allocator) ISeries {
	return createSlicedSeries(name, typedArr, start, length, mem)
}

// createSlicedInt8Series creates an int8 series slice
func createSlicedInt8Series(name string, typedArr *array.Int8, start, length int, mem memory.Allocator) ISeries {
	return createSlicedSeries(name, typedArr, start, length, mem)
}

// createSlicedUint64Series creates a uint64 series slice
func createSlicedUint64Series(name string, typedArr *array.Uint64, start, length int, mem memory.Allocator) ISeries {
	return createSlicedSeries(name, typedArr, start, length, mem)
}

// createSlicedUint32Series creates a uint32 series slice
func createSlicedUint32Series(name string, typedArr *array.Uint32, start, length int, mem memory.Allocator) ISeries {
	return createSlicedSeries(name, typedArr, start, length, mem)
}

// createSlicedUint16Series creates a uint16 series slice
func createSlicedUint16Series(name string, typedArr *array.Uint16, start, length int, mem memory.Allocator) ISeries {
	return createSlicedSeries(name, typedArr, start, length, mem)
}

// createSlicedUint8Series creates a uint8 series slice
func createSlicedUint8Series(name string, typedArr *array.Uint8, start, length int, mem memory.Allocator) ISeries {
	return createSlicedSeries(name, typedArr, start, length, mem)
}

// createSlicedFloat64Series creates a float64 series slice
func createSlicedFloat64Series(name string, typedArr *array.Float64, start, length int, mem memory.Allocator) ISeries {
	return createSlicedSeries(name, typedArr, start, length, mem)
}

// createSlicedFloat32Series creates a float32 series slice
func createSlicedFloat32Series(name string, typedArr *array.Float32, start, length int, mem memory.Allocator) ISeries {
	return createSlicedSeries(name, typedArr, start, length, mem)
}

// createSlicedBoolSeries creates a boolean series slice
func createSlicedBoolSeries(name string, typedArr *array.Boolean, start, length int, mem memory.Allocator) ISeries {
	return createSlicedSeries(name, typedArr, start, length, mem)
}

// createSlicedTimestampSeries creates a timestamp series slice
func createSlicedTimestampSeries(
	name string, typedArr *array.Timestamp, start, length int, mem memory.Allocator,
) ISeries {
	values := make([]time.Time, length)
	for i := 0; i < length; i++ {
		srcIndex := start + i
		if srcIndex < typedArr.Len() && !typedArr.IsNull(srcIndex) {
			// Convert Arrow timestamp back to time.Time
			timestamp := typedArr.Value(srcIndex)
			nanos := int64(timestamp)
			values[i] = time.Unix(nanos/nanosPerSecond, nanos%nanosPerSecond).UTC()
		}
	}
	return series.New(name, values, mem)
}

// Concat concatenates multiple DataFrames vertically (row-wise)
// All DataFrames must have the same column structure
func (df *DataFrame) Concat(others ...*DataFrame) *DataFrame {
	if len(others) == 0 {
		return df // Return copy of current DataFrame
	}

	// Validate column compatibility
	for _, other := range others {
		if !df.hasSameSchema(other) {
			return New() // Return empty DataFrame for incompatible schemas
		}
	}

	var concatenatedSeries []ISeries
	for _, colName := range df.order {
		if series, exists := df.columns[colName]; exists {
			// Collect all series for this column
			allSeries := []ISeries{series}
			for _, other := range others {
				if otherSeries, exists := other.columns[colName]; exists {
					allSeries = append(allSeries, otherSeries)
				}
			}
			// Concatenate series for this column
			concatenatedSeries = append(concatenatedSeries, df.concatSeries(colName, allSeries))
		}
	}

	return New(concatenatedSeries...)
}

// hasSameSchema checks if two DataFrames have the same column structure
func (df *DataFrame) hasSameSchema(other *DataFrame) bool {
	if len(df.order) != len(other.order) {
		return false
	}

	for i, colName := range df.order {
		if i >= len(other.order) || other.order[i] != colName {
			return false
		}

		dfSeries, dfExists := df.columns[colName]
		otherSeries, otherExists := other.columns[colName]
		if !dfExists || !otherExists {
			return false
		}

		// Check data types match (with nil array protection)
		dfType := safeDataType(dfSeries)
		otherType := safeDataType(otherSeries)
		if dfType == nil || otherType == nil || dfType != otherType {
			return false
		}
	}

	return true
}

// concatSeries concatenates multiple series of the same type
func (df *DataFrame) concatSeries(name string, seriesList []ISeries) ISeries {
	if len(seriesList) == 0 {
		return series.New(name, []string{}, memory.NewGoAllocator())
	}

	if len(seriesList) == 1 {
		return df.copySeries(seriesList[0])
	}

	// Determine the total length
	totalLength := 0
	for _, s := range seriesList {
		totalLength += s.Len()
	}

	firstArray := seriesList[0].Array()
	defer firstArray.Release()

	mem := memory.NewGoAllocator()

	// Delegate to type-specific concatenation helpers
	switch firstArray.(type) {
	case *array.String:
		return df.concatStringSeries(name, seriesList, totalLength, mem)
	case *array.Int64:
		return df.concatInt64Series(name, seriesList, totalLength, mem)
	case *array.Float64:
		return df.concatFloat64Series(name, seriesList, totalLength, mem)
	case *array.Boolean:
		return df.concatBoolSeries(name, seriesList, totalLength, mem)
	default:
		return series.New(name, []string{}, mem)
	}
}

// concatStringSeries concatenates string series
func (df *DataFrame) concatStringSeries(
	name string, seriesList []ISeries, totalLength int, mem memory.Allocator,
) ISeries {
	return concatTypedSeries(name, seriesList, totalLength, mem, "", func(arr arrow.Array, i int) string {
		return arr.(*array.String).Value(i)
	})
}

// concatInt64Series concatenates int64 series
func (df *DataFrame) concatInt64Series(
	name string, seriesList []ISeries, totalLength int, mem memory.Allocator,
) ISeries {
	return concatTypedSeries(name, seriesList, totalLength, mem, int64(0), func(arr arrow.Array, i int) int64 {
		return arr.(*array.Int64).Value(i)
	})
}

// concatFloat64Series concatenates float64 series
func (df *DataFrame) concatFloat64Series(
	name string, seriesList []ISeries, totalLength int, mem memory.Allocator,
) ISeries {
	return concatTypedSeries(name, seriesList, totalLength, mem, 0.0, func(arr arrow.Array, i int) float64 {
		return arr.(*array.Float64).Value(i)
	})
}

// concatBoolSeries concatenates boolean series
func (df *DataFrame) concatBoolSeries(
	name string, seriesList []ISeries, totalLength int, mem memory.Allocator,
) ISeries {
	return concatTypedSeries(name, seriesList, totalLength, mem, false, func(arr arrow.Array, i int) bool {
		return arr.(*array.Boolean).Value(i)
	})
}

// concatTypedSeries is a generic helper for concatenating typed series
func concatTypedSeries[T any](
	name string, seriesList []ISeries, totalLength int, mem memory.Allocator,
	defaultValue T, getValue func(arrow.Array, int) T,
) ISeries {
	values := make([]T, 0, totalLength)
	for _, s := range seriesList {
		arr := s.Array()
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsNull(i) {
				values = append(values, getValue(arr, i))
			} else {
				values = append(values, defaultValue)
			}
		}
		arr.Release()
	}
	return series.New(name, values, mem)
}

// copySeries creates a copy of a series
func (df *DataFrame) copySeries(s ISeries) ISeries {
	originalArray := s.Array()
	defer originalArray.Release()

	mem := memory.NewGoAllocator()

	switch typedArr := originalArray.(type) {
	case *array.String:
		values := make([]string, typedArr.Len())
		for i := 0; i < typedArr.Len(); i++ {
			if !typedArr.IsNull(i) {
				values[i] = typedArr.Value(i)
			}
		}
		return series.New(s.Name(), values, mem)

	case *array.Int64:
		values := make([]int64, typedArr.Len())
		for i := 0; i < typedArr.Len(); i++ {
			if !typedArr.IsNull(i) {
				values[i] = typedArr.Value(i)
			}
		}
		return series.New(s.Name(), values, mem)

	case *array.Float64:
		values := make([]float64, typedArr.Len())
		for i := 0; i < typedArr.Len(); i++ {
			if !typedArr.IsNull(i) {
				values[i] = typedArr.Value(i)
			}
		}
		return series.New(s.Name(), values, mem)

	case *array.Boolean:
		values := make([]bool, typedArr.Len())
		for i := 0; i < typedArr.Len(); i++ {
			if !typedArr.IsNull(i) {
				values[i] = typedArr.Value(i)
			}
		}
		return series.New(s.Name(), values, mem)

	default:
		// For unsupported types, return empty series
		return series.New(s.Name(), []string{}, mem)
	}
}

// Release releases all underlying Arrow memory
func (df *DataFrame) Release() {
	for _, series := range df.columns {
		series.Release()
	}
}

// safeDataType safely gets the data type from a series, returning nil if the series has a nil array
func safeDataType(s ISeries) (result arrow.DataType) {
	if s == nil {
		return nil
	}

	// Use the series DataType method directly, but with recovery
	defer func() {
		if r := recover(); r != nil {
			// If there's a panic (e.g. nil pointer), return nil
			result = nil
		}
	}()

	return s.DataType()
}

// GroupBy represents a grouped DataFrame for aggregation operations
type GroupBy struct {
	df          *DataFrame
	groupByCols []string
	groups      map[string][]int // group key -> row indices
}

// GroupBy creates a GroupBy object for the specified columns
func (df *DataFrame) GroupBy(columns ...string) *GroupBy {
	// Validate columns exist
	for _, col := range columns {
		if !df.HasColumn(col) {
			return &GroupBy{
				df:          df,
				groupByCols: columns,
				groups:      make(map[string][]int),
			}
		}
	}

	return &GroupBy{
		df:          df,
		groupByCols: columns,
		groups:      df.buildGroups(columns),
	}
}

// buildGroups creates a hash map of group keys to row indices
func (df *DataFrame) buildGroups(columns []string) map[string][]int {
	groups := make(map[string][]int)
	rowCount := df.Len()

	if rowCount == 0 {
		return groups
	}

	// Get column arrays
	columnArrays := make([]arrow.Array, len(columns))
	for i, col := range columns {
		if series, exists := df.Column(col); exists {
			columnArrays[i] = series.Array()
		}
	}
	defer func() {
		for _, arr := range columnArrays {
			if arr != nil {
				arr.Release()
			}
		}
	}()

	// Build groups
	for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
		groupKey := df.buildGroupKey(columnArrays, rowIdx)
		groups[groupKey] = append(groups[groupKey], rowIdx)
	}

	return groups
}

// buildGroupKey creates a unique string key for a group based on row values
func (df *DataFrame) buildGroupKey(columnArrays []arrow.Array, rowIdx int) string {
	var keyParts []string

	for _, arr := range columnArrays {
		if arr == nil {
			keyParts = append(keyParts, "null")
			continue
		}

		if arr.IsNull(rowIdx) {
			keyParts = append(keyParts, "null")
			continue
		}

		switch typedArr := arr.(type) {
		case *array.String:
			keyParts = append(keyParts, typedArr.Value(rowIdx))
		case *array.Int64:
			keyParts = append(keyParts, fmt.Sprintf("%d", typedArr.Value(rowIdx)))
		case *array.Float64:
			keyParts = append(keyParts, fmt.Sprintf("%f", typedArr.Value(rowIdx)))
		case *array.Boolean:
			keyParts = append(keyParts, fmt.Sprintf("%t", typedArr.Value(rowIdx)))
		default:
			keyParts = append(keyParts, "unknown")
		}
	}

	return strings.Join(keyParts, "|")
}

// Agg performs aggregation operations on the grouped data
func (gb *GroupBy) Agg(aggregations ...*expr.AggregationExpr) *DataFrame {
	if len(aggregations) == 0 || len(gb.groups) == 0 {
		return New()
	}

	// Use parallel processing for large number of groups
	parallelThreshold := config.GetGlobalConfig().ParallelThreshold
	minGroupsForParallel := parallelThreshold / groupParallelThresholdDiv // Use 1/10 of row threshold for groups
	if len(gb.groups) >= minGroupsForParallel {
		return gb.aggParallel(aggregations...)
	}

	return gb.aggSequential(aggregations...)
}

// aggSequential performs aggregation sequentially
func (gb *GroupBy) aggSequential(aggregations ...*expr.AggregationExpr) *DataFrame {
	// Prepare result columns: group columns + aggregation columns
	var resultSeries []ISeries
	mem := memory.NewGoAllocator()

	// Add group columns to result
	for _, groupCol := range gb.groupByCols {
		if originalSeries, exists := gb.df.Column(groupCol); exists {
			groupValues := gb.extractGroupColumnValues(originalSeries)
			resultSeries = append(resultSeries, series.New(groupCol, groupValues, mem))
		}
	}

	// Add aggregation columns to result
	for _, agg := range aggregations {
		columnExpr, ok := agg.Column().(*expr.ColumnExpr)
		if !ok {
			continue // Skip non-column aggregations for now
		}

		columnName := columnExpr.Name()
		if originalSeries, exists := gb.df.Column(columnName); exists {
			aggValues := gb.performAggregation(originalSeries, agg)
			aggColumnName := gb.getAggregationColumnName(agg, columnName)
			resultSeries = append(resultSeries, gb.createAggregationSeries(aggColumnName, aggValues, agg.AggType(), mem))
		}
	}

	return New(resultSeries...)
}

// aggParallel performs aggregation using parallel processing
func (gb *GroupBy) aggParallel(aggregations ...*expr.AggregationExpr) *DataFrame {
	// Prepare result columns: group columns + aggregation columns
	var resultSeries []ISeries
	mem := memory.NewGoAllocator()

	// Add group columns to result (sequential since it's simple)
	for _, groupCol := range gb.groupByCols {
		if originalSeries, exists := gb.df.Column(groupCol); exists {
			groupValues := gb.extractGroupColumnValues(originalSeries)
			resultSeries = append(resultSeries, series.New(groupCol, groupValues, mem))
		}
	}

	// Process aggregations in parallel
	pool := parallel.NewWorkerPool(runtime.NumCPU())
	defer pool.Close()

	// Create work items for each aggregation
	type aggWork struct {
		agg        *expr.AggregationExpr
		columnName string
		series     ISeries
	}

	var workItems []aggWork
	for _, agg := range aggregations {
		columnExpr, ok := agg.Column().(*expr.ColumnExpr)
		if !ok {
			continue
		}

		columnName := columnExpr.Name()
		if originalSeries, exists := gb.df.Column(columnName); exists {
			workItems = append(workItems, aggWork{
				agg:        agg,
				columnName: columnName,
				series:     originalSeries,
			})
		}
	}

	// Process aggregations in parallel
	aggResults := parallel.Process(pool, workItems, func(work aggWork) ISeries {
		aggValues := gb.performAggregation(work.series, work.agg)
		aggColumnName := gb.getAggregationColumnName(work.agg, work.columnName)
		return gb.createAggregationSeries(aggColumnName, aggValues, work.agg.AggType(), memory.NewGoAllocator())
	})

	// Add aggregation results to result series
	resultSeries = append(resultSeries, aggResults...)

	return New(resultSeries...)
}

// extractGroupColumnValues extracts unique values for group columns
func (gb *GroupBy) extractGroupColumnValues(series ISeries) []string {
	var values []string
	originalArray := series.Array()
	defer originalArray.Release()

	// Get sorted group keys to ensure deterministic order
	var groupKeys []string
	for key := range gb.groups {
		groupKeys = append(groupKeys, key)
	}
	sort.Strings(groupKeys)

	// Extract first value from each group (all values in a group are the same)
	for _, key := range groupKeys {
		indices := gb.groups[key]
		if len(indices) > 0 {
			rowIdx := indices[0]
			if rowIdx < originalArray.Len() && !originalArray.IsNull(rowIdx) {
				switch typedArr := originalArray.(type) {
				case *array.String:
					values = append(values, typedArr.Value(rowIdx))
				case *array.Int64:
					values = append(values, fmt.Sprintf("%d", typedArr.Value(rowIdx)))
				case *array.Float64:
					values = append(values, fmt.Sprintf("%f", typedArr.Value(rowIdx)))
				case *array.Boolean:
					values = append(values, fmt.Sprintf("%t", typedArr.Value(rowIdx)))
				default:
					values = append(values, "")
				}
			} else {
				values = append(values, "")
			}
		}
	}

	return values
}

// performAggregation performs the specified aggregation on a series
func (gb *GroupBy) performAggregation(series ISeries, agg *expr.AggregationExpr) []float64 {
	var results []float64
	originalArray := series.Array()
	defer originalArray.Release()

	// Get sorted group keys to ensure deterministic order
	var groupKeys []string
	for key := range gb.groups {
		groupKeys = append(groupKeys, key)
	}
	sort.Strings(groupKeys)

	for _, key := range groupKeys {
		indices := gb.groups[key]
		result := gb.aggregateGroup(originalArray, indices, agg.AggType())
		results = append(results, result)
	}

	return results
}

// aggregateGroup performs aggregation on a single group
func (gb *GroupBy) aggregateGroup(arr arrow.Array, indices []int, aggType expr.AggregationType) float64 {
	if len(indices) == 0 {
		return 0.0
	}

	switch aggType {
	case expr.AggCount:
		return float64(len(indices))

	case expr.AggSum:
		return gb.sumGroup(arr, indices)

	case expr.AggMean:
		sum := gb.sumGroup(arr, indices)
		count := float64(len(indices))
		if count > 0 {
			return sum / count
		}
		return 0.0

	case expr.AggMin:
		return gb.minGroup(arr, indices)

	case expr.AggMax:
		return gb.maxGroup(arr, indices)

	default:
		return 0.0
	}
}

// sumGroup calculates sum for a group
func (gb *GroupBy) sumGroup(arr arrow.Array, indices []int) float64 {
	var sum float64
	for _, idx := range indices {
		if idx < arr.Len() && !arr.IsNull(idx) {
			switch typedArr := arr.(type) {
			case *array.Int64:
				sum += float64(typedArr.Value(idx))
			case *array.Float64:
				sum += typedArr.Value(idx)
			}
		}
	}
	return sum
}

// extractNumericValue extracts a numeric value from an array at the given index
func (gb *GroupBy) extractNumericValue(arr arrow.Array, idx int) (float64, bool) {
	if idx >= arr.Len() || arr.IsNull(idx) {
		return 0, false
	}

	switch typedArr := arr.(type) {
	case *array.Int64:
		return float64(typedArr.Value(idx)), true
	case *array.Float64:
		return typedArr.Value(idx), true
	default:
		return 0, false
	}
}

// minGroup calculates minimum for a group
func (gb *GroupBy) minGroup(arr arrow.Array, indices []int) float64 {
	var minimum float64
	first := true

	for _, idx := range indices {
		if val, ok := gb.extractNumericValue(arr, idx); ok {
			if first || val < minimum {
				minimum = val
				first = false
			}
		}
	}

	return minimum
}

// maxGroup calculates maximum for a group
func (gb *GroupBy) maxGroup(arr arrow.Array, indices []int) float64 {
	var maximum float64
	first := true

	for _, idx := range indices {
		if val, ok := gb.extractNumericValue(arr, idx); ok {
			if first || val > maximum {
				maximum = val
				first = false
			}
		}
	}

	return maximum
}

// getAggregationColumnName generates a name for the aggregation column
func (gb *GroupBy) getAggregationColumnName(agg *expr.AggregationExpr, columnName string) string {
	if agg.Alias() != "" {
		return agg.Alias()
	}

	var aggName string
	switch agg.AggType() {
	case expr.AggSum:
		aggName = "sum"
	case expr.AggCount:
		aggName = "count"
	case expr.AggMean:
		aggName = "mean"
	case expr.AggMin:
		aggName = "min"
	case expr.AggMax:
		aggName = "max"
	}

	return fmt.Sprintf("%s_%s", aggName, columnName)
}

// createAggregationSeries creates a series for aggregation results
func (gb *GroupBy) createAggregationSeries(
	name string, values []float64, aggType expr.AggregationType, mem memory.Allocator,
) ISeries {
	// For count operations, return int64 series
	if aggType == expr.AggCount {
		intValues := make([]int64, len(values))
		for i, v := range values {
			intValues[i] = int64(v)
		}
		return series.New(name, intValues, mem)
	}

	// For other aggregations, return float64 series
	return series.New(name, values, mem)
}

// Join performs a join operation between two DataFrames
func (df *DataFrame) Join(right *DataFrame, options *JoinOptions) (*DataFrame, error) {
	// Determine join keys
	leftKeys, rightKeys := normalizeJoinKeys(options)

	if len(leftKeys) != len(rightKeys) {
		return nil, fmt.Errorf("number of left keys (%d) must match number of right keys (%d)",
			len(leftKeys), len(rightKeys))
	}

	// Validate that join keys exist in both DataFrames
	if err := validateJoinKeys(df, right, leftKeys, rightKeys); err != nil {
		return nil, err
	}

	// Use parallel execution for large datasets
	parallelThreshold := config.GetGlobalConfig().ParallelThreshold
	useParallel := df.Len() >= parallelThreshold || right.Len() >= parallelThreshold

	if useParallel {
		return df.parallelJoin(right, leftKeys, rightKeys, options.Type)
	}

	return df.sequentialJoin(right, leftKeys, rightKeys, options.Type)
}

// normalizeJoinKeys extracts the actual keys to use for joining
func normalizeJoinKeys(options *JoinOptions) ([]string, []string) {
	if len(options.LeftKeys) > 0 && len(options.RightKeys) > 0 {
		return options.LeftKeys, options.RightKeys
	}
	return []string{options.LeftKey}, []string{options.RightKey}
}

// validateJoinKeys ensures all join keys exist in both DataFrames
func validateJoinKeys(left, right *DataFrame, leftKeys, rightKeys []string) error {
	for _, key := range leftKeys {
		if !left.HasColumn(key) {
			return fmt.Errorf("left DataFrame missing join key: %s", key)
		}
	}

	for _, key := range rightKeys {
		if !right.HasColumn(key) {
			return fmt.Errorf("right DataFrame missing join key: %s", key)
		}
	}

	return nil
}

// sequentialJoin performs join operation using sequential hash-based algorithm
func (df *DataFrame) sequentialJoin(
	right *DataFrame, leftKeys, rightKeys []string, joinType JoinType,
) (*DataFrame, error) {
	mem := memory.NewGoAllocator()

	// Build hash map from right DataFrame
	rightHashMap := make(map[string][]int)
	for i := 0; i < right.Len(); i++ {
		key := buildJoinKey(right, rightKeys, i)
		rightHashMap[key] = append(rightHashMap[key], i)
	}

	// Collect join results
	var leftIndices, rightIndices []int

	switch joinType {
	case InnerJoin:
		leftIndices, rightIndices = df.performInnerJoin(rightHashMap, leftKeys)
	case LeftJoin:
		leftIndices, rightIndices = df.performLeftJoin(rightHashMap, leftKeys)
	case RightJoin:
		leftIndices, rightIndices = df.performRightJoin(right, rightHashMap, leftKeys)
	case FullOuterJoin:
		leftIndices, rightIndices = df.performFullOuterJoin(right, rightHashMap, leftKeys)
	default:
		return nil, fmt.Errorf("unsupported join type: %v", joinType)
	}

	// Build result DataFrame
	return df.buildJoinResult(right, leftIndices, rightIndices, mem)
}

// parallelJoin performs join operation using parallel hash-based algorithm
func (df *DataFrame) parallelJoin(
	right *DataFrame, leftKeys, rightKeys []string, joinType JoinType,
) (*DataFrame, error) {
	mem := memory.NewGoAllocator()

	// Use sequential join for small datasets
	parallelThreshold := config.GetGlobalConfig().ParallelThreshold
	if right.Len() < parallelThreshold {
		return df.sequentialJoin(right, leftKeys, rightKeys, joinType)
	}

	// Build hash map from right DataFrame using parallel processing
	rightHashMap := df.buildParallelHashMap(right, rightKeys)

	// Collect join results
	var leftIndices, rightIndices []int

	switch joinType {
	case InnerJoin:
		leftIndices, rightIndices = df.performInnerJoin(rightHashMap, leftKeys)
	case LeftJoin:
		leftIndices, rightIndices = df.performLeftJoin(rightHashMap, leftKeys)
	case RightJoin:
		leftIndices, rightIndices = df.performRightJoin(right, rightHashMap, leftKeys)
	case FullOuterJoin:
		leftIndices, rightIndices = df.performFullOuterJoin(right, rightHashMap, leftKeys)
	default:
		return nil, fmt.Errorf("unsupported join type: %v", joinType)
	}

	// Build result DataFrame
	return df.buildJoinResult(right, leftIndices, rightIndices, mem)
}

// buildParallelHashMap builds a hash map from DataFrame using parallel processing
func (df *DataFrame) buildParallelHashMap(right *DataFrame, rightKeys []string) map[string][]int {
	// Create worker pool
	wp := parallel.NewWorkerPool(0) // Use default CPU count
	defer wp.Close()

	// Determine chunk size based on CPU count and data size
	numWorkers := runtime.NumCPU()
	chunkSize := (right.Len() + numWorkers - 1) / numWorkers
	if chunkSize < minChunkSizeForJoin {
		chunkSize = minChunkSizeForJoin
	}

	// Create chunks of row indices to process
	var chunks [][]int
	for start := 0; start < right.Len(); start += chunkSize {
		end := start + chunkSize
		if end > right.Len() {
			end = right.Len()
		}
		chunk := make([]int, end-start)
		for i := start; i < end; i++ {
			chunk[i-start] = i
		}
		chunks = append(chunks, chunk)
	}

	// Worker function to process a chunk and return partial hash map
	worker := func(chunk []int) map[string][]int {
		partialMap := make(map[string][]int)
		for _, rowIndex := range chunk {
			key := buildJoinKey(right, rightKeys, rowIndex)
			partialMap[key] = append(partialMap[key], rowIndex)
		}
		return partialMap
	}

	// Process chunks in parallel
	partialMaps := parallel.Process(wp, chunks, worker)

	// Merge partial results into final hash map
	finalMap := make(map[string][]int)
	for _, partialMap := range partialMaps {
		for key, indices := range partialMap {
			finalMap[key] = append(finalMap[key], indices...)
		}
	}

	return finalMap
}

// buildJoinKey creates a composite key from multiple columns at given row index
func buildJoinKey(df *DataFrame, keys []string, rowIndex int) string {
	if len(keys) == 1 {
		series, exists := df.Column(keys[0])
		if !exists {
			return ""
		}
		return getStringValue(series, rowIndex)
	}

	var keyParts []string
	for _, key := range keys {
		series, exists := df.Column(key)
		if !exists {
			keyParts = append(keyParts, "")
		} else {
			keyParts = append(keyParts, getStringValue(series, rowIndex))
		}
	}
	return strings.Join(keyParts, "|")
}

// getStringValue extracts string representation of value at given index
func getStringValue(series ISeries, index int) string {
	if series == nil || index >= series.Len() {
		return ""
	}

	arr := series.Array()
	defer arr.Release()

	switch typedArr := arr.(type) {
	case *array.String:
		return typedArr.Value(index)
	case *array.Int64:
		return fmt.Sprintf("%d", typedArr.Value(index))
	case *array.Int32:
		return fmt.Sprintf("%d", typedArr.Value(index))
	case *array.Float64:
		return fmt.Sprintf("%f", typedArr.Value(index))
	case *array.Float32:
		return fmt.Sprintf("%f", typedArr.Value(index))
	case *array.Boolean:
		return fmt.Sprintf("%t", typedArr.Value(index))
	default:
		return ""
	}
}

// performInnerJoin returns matching indices for inner join
func (df *DataFrame) performInnerJoin(rightHashMap map[string][]int, leftKeys []string) ([]int, []int) {
	var leftIndices, rightIndices []int

	for i := 0; i < df.Len(); i++ {
		key := buildJoinKey(df, leftKeys, i)
		if rightRows, exists := rightHashMap[key]; exists {
			for _, rightIdx := range rightRows {
				leftIndices = append(leftIndices, i)
				rightIndices = append(rightIndices, rightIdx)
			}
		}
	}

	return leftIndices, rightIndices
}

// performLeftJoin returns indices for left join (all left rows, matched right rows)
func (df *DataFrame) performLeftJoin(rightHashMap map[string][]int, leftKeys []string) ([]int, []int) {
	var leftIndices, rightIndices []int

	for i := 0; i < df.Len(); i++ {
		key := buildJoinKey(df, leftKeys, i)
		if rightRows, exists := rightHashMap[key]; exists {
			for _, rightIdx := range rightRows {
				leftIndices = append(leftIndices, i)
				rightIndices = append(rightIndices, rightIdx)
			}
		} else {
			leftIndices = append(leftIndices, i)
			rightIndices = append(rightIndices, -1) // -1 indicates null/missing
		}
	}

	return leftIndices, rightIndices
}

// performRightJoin returns indices for right join (matched left rows, all right rows)
func (df *DataFrame) performRightJoin(
	right *DataFrame, rightHashMap map[string][]int, leftKeys []string,
) ([]int, []int) {
	var leftIndices, rightIndices []int
	matched := make(map[int]bool) // Track which right rows were matched

	// First pass: find matches (same as inner join)
	for i := 0; i < df.Len(); i++ {
		key := buildJoinKey(df, leftKeys, i)
		if rightRows, exists := rightHashMap[key]; exists {
			for _, rightIdx := range rightRows {
				leftIndices = append(leftIndices, i)
				rightIndices = append(rightIndices, rightIdx)
				matched[rightIdx] = true
			}
		}
	}

	// Second pass: add unmatched right rows
	for i := 0; i < right.Len(); i++ {
		if !matched[i] {
			leftIndices = append(leftIndices, -1) // -1 indicates null/missing
			rightIndices = append(rightIndices, i)
		}
	}

	return leftIndices, rightIndices
}

// performFullOuterJoin returns indices for full outer join (all rows from both sides)
func (df *DataFrame) performFullOuterJoin(
	right *DataFrame, rightHashMap map[string][]int, leftKeys []string,
) ([]int, []int) {
	var leftIndices, rightIndices []int
	matched := make(map[int]bool) // Track which right rows were matched

	// First pass: process all left rows
	for i := 0; i < df.Len(); i++ {
		key := buildJoinKey(df, leftKeys, i)
		if rightRows, exists := rightHashMap[key]; exists {
			for _, rightIdx := range rightRows {
				leftIndices = append(leftIndices, i)
				rightIndices = append(rightIndices, rightIdx)
				matched[rightIdx] = true
			}
		} else {
			leftIndices = append(leftIndices, i)
			rightIndices = append(rightIndices, -1) // -1 indicates null/missing
		}
	}

	// Second pass: add unmatched right rows
	for i := 0; i < right.Len(); i++ {
		if !matched[i] {
			leftIndices = append(leftIndices, -1) // -1 indicates null/missing
			rightIndices = append(rightIndices, i)
		}
	}

	return leftIndices, rightIndices
}

// buildJoinResult constructs the final DataFrame from join indices
func (df *DataFrame) buildJoinResult(
	right *DataFrame, leftIndices, rightIndices []int, mem memory.Allocator,
) (*DataFrame, error) {
	if len(leftIndices) != len(rightIndices) {
		return nil, fmt.Errorf("left indices length (%d) must match right indices length (%d)",
			len(leftIndices), len(rightIndices))
	}

	resultLength := len(leftIndices)
	var resultSeries []ISeries

	// Add columns from left DataFrame
	for _, colName := range df.Columns() {
		leftCol, exists := df.Column(colName)
		if exists {
			resultCol := df.buildJoinColumn(leftCol, leftIndices, resultLength, mem)
			resultSeries = append(resultSeries, resultCol)
		}
	}

	// Add columns from right DataFrame
	for _, colName := range right.Columns() {
		rightCol, exists := right.Column(colName)
		if exists {
			resultCol := df.buildJoinColumn(rightCol, rightIndices, resultLength, mem)
			resultSeries = append(resultSeries, resultCol)
		}
	}

	return New(resultSeries...), nil
}

// buildJoinColumn creates a new series for join result based on indices
func (df *DataFrame) buildJoinColumn(
	sourceSeries ISeries, indices []int, resultLength int, mem memory.Allocator,
) ISeries {
	if sourceSeries == nil {
		return series.New("", []string{}, mem)
	}

	name := sourceSeries.Name()
	sourceArr := sourceSeries.Array()
	defer sourceArr.Release()

	switch typedArr := sourceArr.(type) {
	case *array.String:
		return df.buildStringJoinColumn(name, typedArr, indices, resultLength, mem)
	case *array.Int64:
		return df.buildInt64JoinColumn(name, typedArr, indices, resultLength, mem)
	case *array.Int32:
		return df.buildInt32JoinColumn(name, typedArr, indices, resultLength, mem)
	case *array.Float64:
		return df.buildFloat64JoinColumn(name, typedArr, indices, resultLength, mem)
	case *array.Float32:
		return df.buildFloat32JoinColumn(name, typedArr, indices, resultLength, mem)
	case *array.Boolean:
		return df.buildBooleanJoinColumn(name, typedArr, indices, resultLength, mem)
	default:
		return df.buildDefaultJoinColumn(name, indices, resultLength, mem)
	}
}

// buildStringJoinColumn creates string series for join result
func (df *DataFrame) buildStringJoinColumn(
	name string, arr *array.String, indices []int, resultLength int, mem memory.Allocator,
) ISeries {
	values := make([]string, resultLength)
	for i, idx := range indices {
		if idx >= 0 && idx < arr.Len() {
			values[i] = arr.Value(idx)
		} else {
			values[i] = "" // Default for null values
		}
	}
	return series.New(name, values, mem)
}

// buildInt64JoinColumn creates int64 series for join result
func (df *DataFrame) buildInt64JoinColumn(
	name string, arr *array.Int64, indices []int, resultLength int, mem memory.Allocator,
) ISeries {
	values := make([]int64, resultLength)
	for i, idx := range indices {
		if idx >= 0 && idx < arr.Len() {
			values[i] = arr.Value(idx)
		} else {
			values[i] = 0 // Default for null values
		}
	}
	return series.New(name, values, mem)
}

// buildInt32JoinColumn creates int32 series for join result
func (df *DataFrame) buildInt32JoinColumn(
	name string, arr *array.Int32, indices []int, resultLength int, mem memory.Allocator,
) ISeries {
	values := make([]int32, resultLength)
	for i, idx := range indices {
		if idx >= 0 && idx < arr.Len() {
			values[i] = arr.Value(idx)
		} else {
			values[i] = 0 // Default for null values
		}
	}
	return series.New(name, values, mem)
}

// buildFloat64JoinColumn creates float64 series for join result
func (df *DataFrame) buildFloat64JoinColumn(
	name string, arr *array.Float64, indices []int, resultLength int, mem memory.Allocator,
) ISeries {
	values := make([]float64, resultLength)
	for i, idx := range indices {
		if idx >= 0 && idx < arr.Len() {
			values[i] = arr.Value(idx)
		} else {
			values[i] = 0.0 // Default for null values
		}
	}
	return series.New(name, values, mem)
}

// buildFloat32JoinColumn creates float32 series for join result
func (df *DataFrame) buildFloat32JoinColumn(
	name string, arr *array.Float32, indices []int, resultLength int, mem memory.Allocator,
) ISeries {
	values := make([]float32, resultLength)
	for i, idx := range indices {
		if idx >= 0 && idx < arr.Len() {
			values[i] = arr.Value(idx)
		} else {
			values[i] = 0.0 // Default for null values
		}
	}
	return series.New(name, values, mem)
}

// buildBooleanJoinColumn creates boolean series for join result
func (df *DataFrame) buildBooleanJoinColumn(
	name string, arr *array.Boolean, indices []int, resultLength int, mem memory.Allocator,
) ISeries {
	values := make([]bool, resultLength)
	for i, idx := range indices {
		if idx >= 0 && idx < arr.Len() {
			values[i] = arr.Value(idx)
		} else {
			values[i] = false // Default for null values
		}
	}
	return series.New(name, values, mem)
}

// buildDefaultJoinColumn creates default string series for join result
func (df *DataFrame) buildDefaultJoinColumn(
	name string, indices []int, resultLength int, mem memory.Allocator,
) ISeries {
	values := make([]string, resultLength)
	for i := range indices {
		values[i] = ""
	}
	return series.New(name, values, mem)
}

// sortSequential performs sequential sorting using standard library sort
func (df *DataFrame) sortSequential(columns []string, ascending []bool) []int {
	rowCount := df.Len()
	indices := make([]int, rowCount)
	for i := range indices {
		indices[i] = i
	}

	// Create comparison function for multi-column sorting
	sort.Slice(indices, func(i, j int) bool {
		return df.compareRows(indices[i], indices[j], columns, ascending)
	})

	return indices
}

// chunkResult represents a sorted chunk with its starting index
type chunkResult struct {
	startIdx int
	indices  []int
}

// sortParallel performs parallel merge sort using worker pool
func (df *DataFrame) sortParallel(columns []string, ascending []bool) []int {
	rowCount := df.Len()

	// Calculate chunk size based on worker count and data size
	numWorkers := runtime.NumCPU()
	minChunkSize := 500
	maxChunkSize := 10000

	chunkSize := rowCount / numWorkers
	if chunkSize < minChunkSize {
		chunkSize = minChunkSize
	} else if chunkSize > maxChunkSize {
		chunkSize = maxChunkSize
	}

	// If chunks would be too small, fall back to sequential sort
	const minParallelChunks = 2
	if rowCount/chunkSize < minParallelChunks {
		return df.sortSequential(columns, ascending)
	}

	// Create chunks of indices for parallel sorting

	chunks := make([]chunkResult, 0)
	for start := 0; start < rowCount; start += chunkSize {
		end := start + chunkSize
		if end > rowCount {
			end = rowCount
		}

		// Create indices for this chunk
		chunkIndices := make([]int, end-start)
		for i := range chunkIndices {
			chunkIndices[i] = start + i
		}

		chunks = append(chunks, chunkResult{
			startIdx: start,
			indices:  chunkIndices,
		})
	}

	// Create a worker pool for parallel processing
	pool := parallel.NewWorkerPool(numWorkers)
	defer pool.Close()

	// Sort each chunk in parallel using the existing ProcessIndexed function
	sortedChunks := parallel.ProcessIndexed(pool, chunks, func(index int, chunk chunkResult) chunkResult {
		// Sort this chunk's indices
		sort.Slice(chunk.indices, func(i, j int) bool {
			return df.compareRows(chunk.indices[i], chunk.indices[j], columns, ascending)
		})
		return chunk
	})

	// Merge the sorted chunks
	return df.mergeSortedChunks(sortedChunks, columns, ascending)
}

// compareRows compares two rows based on multiple columns and sort directions
func (df *DataFrame) compareRows(rowA, rowB int, columns []string, ascending []bool) bool {
	for i, column := range columns {
		series, exists := df.columns[column]
		if !exists {
			continue
		}

		cmp := df.compareSeriesValues(series, rowA, rowB)
		if cmp == 0 {
			continue // Equal, check next column
		}

		if ascending[i] {
			return cmp < 0
		} else {
			return cmp > 0
		}
	}
	return false // All columns equal
}

// compareSeriesValues compares two values in a series
func (df *DataFrame) compareSeriesValues(series ISeries, indexA, indexB int) int {
	arr := series.Array()
	if arr == nil {
		return 0
	}
	defer arr.Release()

	switch typedArr := arr.(type) {
	case *array.String:
		return compareOrderedValues(typedArr.Value(indexA), typedArr.Value(indexB))
	case *array.Int64:
		return compareOrderedValues(typedArr.Value(indexA), typedArr.Value(indexB))
	case *array.Int32:
		return compareOrderedValues(typedArr.Value(indexA), typedArr.Value(indexB))
	case *array.Float64:
		return compareOrderedValues(typedArr.Value(indexA), typedArr.Value(indexB))
	case *array.Float32:
		return compareOrderedValues(typedArr.Value(indexA), typedArr.Value(indexB))
	case *array.Boolean:
		return df.compareBoolValues(typedArr.Value(indexA), typedArr.Value(indexB))
	}

	return 0
}

// compareOrderedValues compares two values of any ordered type
func compareOrderedValues[T cmp.Ordered](valA, valB T) int {
	return cmp.Compare(valA, valB)
}

// compareBoolValues compares two boolean values (false < true)
func (df *DataFrame) compareBoolValues(valA, valB bool) int {
	if !valA && valB {
		return -1
	} else if valA && !valB {
		return 1
	}
	return 0
}

// buildSortedDataFrame creates a new DataFrame with rows in sorted order
func (df *DataFrame) buildSortedDataFrame(sortedIndices []int) *DataFrame {
	mem := memory.NewGoAllocator()
	var sortedSeries []ISeries

	for _, colName := range df.order {
		if series, exists := df.columns[colName]; exists {
			sortedSeries = append(sortedSeries, df.buildSortedSeries(series, sortedIndices, mem))
		}
	}

	return New(sortedSeries...)
}

// buildSortedSeries creates a new series with values in sorted order
func (df *DataFrame) buildSortedSeries(s ISeries, sortedIndices []int, mem memory.Allocator) ISeries {
	arr := s.Array()
	if arr == nil {
		return s
	}
	defer arr.Release()

	switch typedArr := arr.(type) {
	case *array.String:
		values := make([]string, len(sortedIndices))
		for i, idx := range sortedIndices {
			values[i] = typedArr.Value(idx)
		}
		return series.New(s.Name(), values, mem)

	case *array.Int64:
		values := make([]int64, len(sortedIndices))
		for i, idx := range sortedIndices {
			values[i] = typedArr.Value(idx)
		}
		return series.New(s.Name(), values, mem)

	case *array.Int32:
		values := make([]int32, len(sortedIndices))
		for i, idx := range sortedIndices {
			values[i] = typedArr.Value(idx)
		}
		return series.New(s.Name(), values, mem)

	case *array.Float64:
		values := make([]float64, len(sortedIndices))
		for i, idx := range sortedIndices {
			values[i] = typedArr.Value(idx)
		}
		return series.New(s.Name(), values, mem)

	case *array.Float32:
		values := make([]float32, len(sortedIndices))
		for i, idx := range sortedIndices {
			values[i] = typedArr.Value(idx)
		}
		return series.New(s.Name(), values, mem)

	case *array.Boolean:
		values := make([]bool, len(sortedIndices))
		for i, idx := range sortedIndices {
			values[i] = typedArr.Value(idx)
		}
		return series.New(s.Name(), values, mem)
	}

	// Fallback: return original series
	return s
}

// mergeSortedChunks merges multiple sorted chunks into a single sorted array
func (df *DataFrame) mergeSortedChunks(chunks []chunkResult, columns []string, ascending []bool) []int {
	if len(chunks) == 0 {
		return []int{}
	}

	if len(chunks) == 1 {
		return chunks[0].indices
	}

	// Calculate total size
	totalSize := 0
	for _, chunk := range chunks {
		totalSize += len(chunk.indices)
	}

	// Merge chunks using k-way merge algorithm
	result := make([]int, 0, totalSize)

	// Track current position in each chunk
	positions := make([]int, len(chunks))

	// Keep merging until all chunks are exhausted
	for {
		// Find the chunk with the smallest current element
		minChunk := -1
		var minRow int

		for i, chunk := range chunks {
			if positions[i] < len(chunk.indices) {
				currentRow := chunk.indices[positions[i]]

				if minChunk == -1 || df.compareRows(currentRow, minRow, columns, ascending) {
					minChunk = i
					minRow = currentRow
				}
			}
		}

		// If no chunk has remaining elements, we're done
		if minChunk == -1 {
			break
		}

		// Add the minimum element to result and advance the position
		result = append(result, minRow)
		positions[minChunk]++
	}

	return result
}

// SafeCollectParallel performs memory-safe parallel collection using the new infrastructure
func (df *DataFrame) SafeCollectParallel() (*DataFrame, error) {
	// Use the existing lazy frame collection but with safe infrastructure
	return df.Lazy().SafeCollectParallel()
}

// SafeCollectParallelWithMonitoring performs memory-safe parallel collection with memory monitoring
func (df *DataFrame) SafeCollectParallelWithMonitoring() (*DataFrame, error) {
	// Use the existing lazy frame collection but with safe infrastructure and monitoring
	return df.Lazy().SafeCollectParallelWithMonitoring()
}

// Advanced Analytics Functions

// Correlation calculates the Pearson correlation coefficient between two numeric columns
func (df *DataFrame) Correlation(col1, col2 string) (float64, error) {
	// Validate columns exist
	series1, exists1 := df.Column(col1)
	if !exists1 {
		return 0, fmt.Errorf("column %s not found", col1)
	}

	series2, exists2 := df.Column(col2)
	if !exists2 {
		return 0, fmt.Errorf("column %s not found", col2)
	}

	// Extract numeric values
	values1, err := df.extractNumericValues(series1)
	if err != nil {
		return 0, fmt.Errorf("column %s is not numeric: %w", col1, err)
	}

	values2, err := df.extractNumericValues(series2)
	if err != nil {
		return 0, fmt.Errorf("column %s is not numeric: %w", col2, err)
	}

	if len(values1) != len(values2) {
		return 0, fmt.Errorf("columns have different lengths: %d vs %d", len(values1), len(values2))
	}

	if len(values1) == 0 {
		return 0, fmt.Errorf("no data to calculate correlation")
	}

	// Calculate correlation coefficient
	return df.calculateCorrelation(values1, values2), nil
}

// extractNumericValues extracts numeric values from a series
func (df *DataFrame) extractNumericValues(s ISeries) ([]float64, error) {
	arr := s.Array()
	if arr == nil {
		return nil, fmt.Errorf("series has no data")
	}
	defer arr.Release()

	switch typedArr := arr.(type) {
	case *array.Float64:
		return df.extractFloat64Values(typedArr), nil
	case *array.Float32:
		return df.extractFloat32Values(typedArr), nil
	case *array.Int64:
		return df.extractInt64Values(typedArr), nil
	case *array.Int32:
		return df.extractInt32Values(typedArr), nil
	case *array.Int16:
		return df.extractInt16Values(typedArr), nil
	case *array.Int8:
		return df.extractInt8Values(typedArr), nil
	case *array.Uint64:
		return df.extractUint64Values(typedArr), nil
	case *array.Uint32:
		return df.extractUint32Values(typedArr), nil
	case *array.Uint16:
		return df.extractUint16Values(typedArr), nil
	case *array.Uint8:
		return df.extractUint8Values(typedArr), nil
	default:
		return nil, fmt.Errorf("unsupported data type for correlation: %T", arr)
	}
}

// Helper functions for extractNumericValues
func (df *DataFrame) extractFloat64Values(arr *array.Float64) []float64 {
	var values []float64
	for i := 0; i < arr.Len(); i++ {
		if !arr.IsNull(i) {
			values = append(values, arr.Value(i))
		}
	}
	return values
}

func (df *DataFrame) extractFloat32Values(arr *array.Float32) []float64 {
	var values []float64
	for i := 0; i < arr.Len(); i++ {
		if !arr.IsNull(i) {
			values = append(values, float64(arr.Value(i)))
		}
	}
	return values
}

func (df *DataFrame) extractInt64Values(arr *array.Int64) []float64 {
	var values []float64
	for i := 0; i < arr.Len(); i++ {
		if !arr.IsNull(i) {
			values = append(values, float64(arr.Value(i)))
		}
	}
	return values
}

func (df *DataFrame) extractInt32Values(arr *array.Int32) []float64 {
	var values []float64
	for i := 0; i < arr.Len(); i++ {
		if !arr.IsNull(i) {
			values = append(values, float64(arr.Value(i)))
		}
	}
	return values
}

func (df *DataFrame) extractInt16Values(arr *array.Int16) []float64 {
	var values []float64
	for i := 0; i < arr.Len(); i++ {
		if !arr.IsNull(i) {
			values = append(values, float64(arr.Value(i)))
		}
	}
	return values
}

func (df *DataFrame) extractInt8Values(arr *array.Int8) []float64 {
	var values []float64
	for i := 0; i < arr.Len(); i++ {
		if !arr.IsNull(i) {
			values = append(values, float64(arr.Value(i)))
		}
	}
	return values
}

func (df *DataFrame) extractUint64Values(arr *array.Uint64) []float64 {
	var values []float64
	for i := 0; i < arr.Len(); i++ {
		if !arr.IsNull(i) {
			values = append(values, float64(arr.Value(i)))
		}
	}
	return values
}

func (df *DataFrame) extractUint32Values(arr *array.Uint32) []float64 {
	var values []float64
	for i := 0; i < arr.Len(); i++ {
		if !arr.IsNull(i) {
			values = append(values, float64(arr.Value(i)))
		}
	}
	return values
}

func (df *DataFrame) extractUint16Values(arr *array.Uint16) []float64 {
	var values []float64
	for i := 0; i < arr.Len(); i++ {
		if !arr.IsNull(i) {
			values = append(values, float64(arr.Value(i)))
		}
	}
	return values
}

func (df *DataFrame) extractUint8Values(arr *array.Uint8) []float64 {
	var values []float64
	for i := 0; i < arr.Len(); i++ {
		if !arr.IsNull(i) {
			values = append(values, float64(arr.Value(i)))
		}
	}
	return values
}

// calculateCorrelation computes the Pearson correlation coefficient
func (df *DataFrame) calculateCorrelation(x, y []float64) float64 {
	if len(x) != len(y) || len(x) == 0 {
		return 0.0
	}

	n := float64(len(x))

	// Calculate means
	var sumX, sumY float64
	for i := 0; i < len(x); i++ {
		sumX += x[i]
		sumY += y[i]
	}
	meanX := sumX / n
	meanY := sumY / n

	// Calculate correlation coefficient
	var numerator, denomX, denomY float64
	for i := 0; i < len(x); i++ {
		dx := x[i] - meanX
		dy := y[i] - meanY
		numerator += dx * dy
		denomX += dx * dx
		denomY += dy * dy
	}

	if denomX == 0 || denomY == 0 {
		return 0.0
	}

	return numerator / math.Sqrt(denomX*denomY)
}

// RollingWindow applies a rolling window operation to a column
func (df *DataFrame) RollingWindow(column string, windowSize int, operation string) (*DataFrame, error) {
	// Validate column exists
	columnSeries, exists := df.Column(column)
	if !exists {
		return nil, fmt.Errorf("column %s not found", column)
	}

	if windowSize <= 0 {
		return nil, fmt.Errorf("window size must be positive, got %d", windowSize)
	}

	// Extract numeric values
	values, err := df.extractNumericValues(columnSeries)
	if err != nil {
		return nil, fmt.Errorf("column %s is not numeric: %w", column, err)
	}

	// Apply rolling window operation
	results, err := df.applyRollingWindow(values, windowSize, operation)
	if err != nil {
		return nil, err
	}

	// Create result DataFrame
	mem := memory.NewGoAllocator()
	var resultSeries []ISeries

	// Add original columns
	for _, colName := range df.order {
		if originalSeries, exists := df.columns[colName]; exists {
			resultSeries = append(resultSeries, df.copySeries(originalSeries))
		}
	}

	// Add rolling window result column
	resultColumnName := fmt.Sprintf("%s_%s", column, operation)
	resultSeries = append(resultSeries, series.New(resultColumnName, results, mem))

	return New(resultSeries...), nil
}

// applyRollingWindow applies a rolling window operation to values
func (df *DataFrame) applyRollingWindow(values []float64, windowSize int, operation string) ([]float64, error) {
	results := make([]float64, len(values))

	for i := 0; i < len(values); i++ {
		if i < windowSize-1 {
			// Not enough data for window, set to NaN
			results[i] = math.NaN()
			continue
		}

		// Extract window values
		window := values[i-windowSize+1 : i+1]

		// Apply operation
		switch operation {
		case "mean":
			results[i] = df.calculateMean(window)
		case "sum":
			results[i] = df.calculateSum(window)
		case "min":
			results[i] = df.calculateMin(window)
		case "max":
			results[i] = df.calculateMax(window)
		case "std":
			results[i] = df.calculateStd(window)
		default:
			return nil, fmt.Errorf("unsupported rolling window operation: %s", operation)
		}
	}

	return results, nil
}

// calculateMean calculates the mean of a slice of values
func (df *DataFrame) calculateMean(values []float64) float64 {
	if len(values) == 0 {
		return math.NaN()
	}

	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

// calculateSum calculates the sum of a slice of values
func (df *DataFrame) calculateSum(values []float64) float64 {
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum
}

// calculateMin calculates the minimum of a slice of values
func (df *DataFrame) calculateMin(values []float64) float64 {
	if len(values) == 0 {
		return math.NaN()
	}

	minimum := values[0]
	for _, v := range values[1:] {
		if v < minimum {
			minimum = v
		}
	}
	return minimum
}

// calculateMax calculates the maximum of a slice of values
func (df *DataFrame) calculateMax(values []float64) float64 {
	if len(values) == 0 {
		return math.NaN()
	}

	maximum := values[0]
	for _, v := range values[1:] {
		if v > maximum {
			maximum = v
		}
	}
	return maximum
}

// calculateStd calculates the standard deviation of a slice of values
func (df *DataFrame) calculateStd(values []float64) float64 {
	if len(values) == 0 {
		return math.NaN()
	}

	mean := df.calculateMean(values)
	variance := 0.0
	for _, v := range values {
		variance += (v - mean) * (v - mean)
	}
	variance /= float64(len(values))
	return math.Sqrt(variance)
}

// WindowFunction applies a window function to the DataFrame
func (df *DataFrame) WindowFunction(function string, columns ...string) (*DataFrame, error) {
	return df.WindowFunctionWithPartition(function, strings.Join(columns, ","), "")
}

// WindowFunctionWithPartition applies a window function with optional partitioning
func (df *DataFrame) WindowFunctionWithPartition(function, orderBy, partitionBy string) (*DataFrame, error) {
	mem := memory.NewGoAllocator()
	var resultSeries []ISeries

	// Add original columns
	for _, colName := range df.order {
		if originalSeries, exists := df.columns[colName]; exists {
			resultSeries = append(resultSeries, df.copySeries(originalSeries))
		}
	}

	// Apply window function
	var functionResult ISeries
	var err error

	switch function {
	case "row_number":
		functionResult = df.applyRowNumber(partitionBy, mem)
	case "rank":
		if orderBy == "" {
			return nil, fmt.Errorf("rank function requires order by column")
		}
		functionResult, err = df.applyRank(orderBy, partitionBy, mem)
	case "lag":
		if orderBy == "" {
			return nil, fmt.Errorf("lag function requires order by column")
		}
		functionResult, err = df.applyLag(orderBy, 1, mem)
	case "lead":
		if orderBy == "" {
			return nil, fmt.Errorf("lead function requires order by column")
		}
		functionResult, err = df.applyLead(orderBy, 1, mem)
	default:
		return nil, fmt.Errorf("unsupported window function: %s", function)
	}

	if err != nil {
		return nil, err
	}

	resultSeries = append(resultSeries, functionResult)

	return New(resultSeries...), nil
}

// applyRowNumber applies row_number window function
func (df *DataFrame) applyRowNumber(partitionBy string, mem memory.Allocator) ISeries {
	rowCount := df.Len()
	results := make([]int64, rowCount)

	if partitionBy == "" {
		// Simple row numbering
		for i := 0; i < rowCount; i++ {
			results[i] = int64(i + 1)
		}
	} else {
		// Partitioned row numbering
		partitionGroups := df.buildPartitionGroups(partitionBy)
		for _, indices := range partitionGroups {
			for i, idx := range indices {
				results[idx] = int64(i + 1)
			}
		}
	}

	return series.New("row_number", results, mem)
}

// applyRank applies rank window function
func (df *DataFrame) applyRank(orderBy, partitionBy string, mem memory.Allocator) (ISeries, error) {
	orderSeries, exists := df.Column(orderBy)
	if !exists {
		return nil, fmt.Errorf("order by column %s not found", orderBy)
	}

	orderValues, err := df.extractNumericValues(orderSeries)
	if err != nil {
		return nil, fmt.Errorf("order by column %s is not numeric: %w", orderBy, err)
	}

	rowCount := df.Len()
	results := make([]int64, rowCount)

	if partitionBy == "" {
		// Simple ranking
		ranks := df.calculateRanks(orderValues)
		for i, rank := range ranks {
			results[i] = int64(rank)
		}
	} else {
		// Partitioned ranking
		partitionGroups := df.buildPartitionGroups(partitionBy)
		for _, indices := range partitionGroups {
			partitionValues := make([]float64, len(indices))
			for i, idx := range indices {
				partitionValues[i] = orderValues[idx]
			}
			ranks := df.calculateRanks(partitionValues)
			for i, idx := range indices {
				results[idx] = int64(ranks[i])
			}
		}
	}

	return series.New("rank", results, mem), nil
}

// applyLag applies lag window function
func (df *DataFrame) applyLag(column string, offset int, mem memory.Allocator) (ISeries, error) {
	columnSeries, exists := df.Column(column)
	if !exists {
		return nil, fmt.Errorf("column %s not found", column)
	}

	values, err := df.extractNumericValues(columnSeries)
	if err != nil {
		return nil, fmt.Errorf("column %s is not numeric: %w", column, err)
	}

	return df.createLagLeadSeries("lag", values, offset, true, mem)
}

// applyLead applies lead window function
func (df *DataFrame) applyLead(column string, offset int, mem memory.Allocator) (ISeries, error) {
	columnSeries, exists := df.Column(column)
	if !exists {
		return nil, fmt.Errorf("column %s not found", column)
	}

	values, err := df.extractNumericValues(columnSeries)
	if err != nil {
		return nil, fmt.Errorf("column %s is not numeric: %w", column, err)
	}

	return df.createLagLeadSeries("lead", values, offset, false, mem)
}

// createLagLeadSeries creates a series with proper null handling for lag/lead functions
func (df *DataFrame) createLagLeadSeries(
	name string, values []float64, offset int, isLag bool, mem memory.Allocator,
) (ISeries, error) {
	// Create Arrow array builder with proper null handling
	builder := array.NewFloat64Builder(mem)
	defer builder.Release()

	for i := 0; i < len(values); i++ {
		if (isLag && i < offset) || (!isLag && i+offset >= len(values)) {
			builder.AppendNull()
		} else {
			if isLag {
				builder.Append(values[i-offset])
			} else {
				builder.Append(values[i+offset])
			}
		}
	}

	arr := builder.NewArray()

	// Create a series wrapper that properly manages the Arrow array
	return &lagLeadSeries{
		name:  name,
		array: arr,
	}, nil
}

// lagLeadSeries is a simple wrapper for lag/lead series that implements ISeries
type lagLeadSeries struct {
	name  string
	array arrow.Array
}

func (s *lagLeadSeries) Name() string {
	return s.name
}

func (s *lagLeadSeries) Len() int {
	return s.array.Len()
}

func (s *lagLeadSeries) DataType() arrow.DataType {
	return s.array.DataType()
}

func (s *lagLeadSeries) Array() arrow.Array {
	s.array.Retain()
	return s.array
}

func (s *lagLeadSeries) IsNull(index int) bool {
	return s.array.IsNull(index)
}

func (s *lagLeadSeries) String() string {
	return fmt.Sprintf("lagLeadSeries[%s]", s.name)
}

func (s *lagLeadSeries) Release() {
	s.array.Release()
}

func (s *lagLeadSeries) GetAsString(index int) string {
	if index < 0 || index >= s.array.Len() || s.array.IsNull(index) {
		return ""
	}

	switch arr := s.array.(type) {
	case *array.String:
		return arr.Value(index)
	case *array.Int64:
		return fmt.Sprintf("%d", arr.Value(index))
	case *array.Int32:
		return fmt.Sprintf("%d", arr.Value(index))
	case *array.Float64:
		return fmt.Sprintf("%g", arr.Value(index))
	case *array.Float32:
		return fmt.Sprintf("%g", arr.Value(index))
	case *array.Boolean:
		return fmt.Sprintf("%t", arr.Value(index))
	default:
		return fmt.Sprintf("%v", s.array)
	}
}

// buildPartitionGroups builds groups for partitioned window functions
func (df *DataFrame) buildPartitionGroups(partitionBy string) map[string][]int {
	if partitionBy == "" {
		return map[string][]int{}
	}

	groups := make(map[string][]int)
	rowCount := df.Len()

	partitionSeries, exists := df.Column(partitionBy)
	if !exists {
		return groups
	}

	arr := partitionSeries.Array()
	defer arr.Release()

	for i := 0; i < rowCount; i++ {
		key := df.getPartitionKey(arr, i)
		groups[key] = append(groups[key], i)
	}

	return groups
}

// getPartitionKey gets the partition key for a row
func (df *DataFrame) getPartitionKey(arr arrow.Array, rowIdx int) string {
	if arr.IsNull(rowIdx) {
		return "null"
	}

	switch typedArr := arr.(type) {
	case *array.String:
		return typedArr.Value(rowIdx)
	case *array.Int64:
		return fmt.Sprintf("%d", typedArr.Value(rowIdx))
	case *array.Float64:
		return fmt.Sprintf("%f", typedArr.Value(rowIdx))
	case *array.Boolean:
		return fmt.Sprintf("%t", typedArr.Value(rowIdx))
	default:
		return "unknown"
	}
}

// calculateRanks calculates ranks for a slice of values
func (df *DataFrame) calculateRanks(values []float64) []int {
	n := len(values)
	if n == 0 {
		return []int{}
	}

	// Create index-value pairs
	type indexValue struct {
		index int
		value float64
	}

	pairs := make([]indexValue, n)
	for i, v := range values {
		pairs[i] = indexValue{index: i, value: v}
	}

	// Sort by value
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].value < pairs[j].value
	})

	// Assign ranks with ties handled correctly
	ranks := make([]int, n)
	currentRank := 1
	for i, pair := range pairs {
		if i > 0 && pairs[i-1].value == pair.value {
			// Same value as previous, use same rank
			ranks[pair.index] = ranks[pairs[i-1].index]
		} else {
			// New value, use current rank
			ranks[pair.index] = currentRank
		}
		currentRank++
	}

	return ranks
}
