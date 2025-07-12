// Package dataframe provides high-performance DataFrame operations
package dataframe

import (
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/paveg/gorilla/internal/series"
)

// DataFrame represents a table of data with typed columns
type DataFrame struct {
	columns map[string]ISeries
	order   []string // Maintains column order
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
		columns: columns,
		order:   order,
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
	case *array.Float64:
		return createSlicedFloat64Series(name, typedArr, start, length, mem)
	case *array.Boolean:
		return createSlicedBoolSeries(name, typedArr, start, length, mem)
	default:
		// For unsupported types, return empty series
		return series.New(name, []string{}, mem)
	}
}

// createSlicedStringSeries creates a string series slice
func createSlicedStringSeries(name string, typedArr *array.String, start, length int, mem memory.Allocator) ISeries {
	values := make([]string, length)
	for i := 0; i < length; i++ {
		srcIndex := start + i
		if srcIndex < typedArr.Len() && !typedArr.IsNull(srcIndex) {
			values[i] = typedArr.Value(srcIndex)
		}
	}
	return series.New(name, values, mem)
}

// createSlicedInt64Series creates an int64 series slice
func createSlicedInt64Series(name string, typedArr *array.Int64, start, length int, mem memory.Allocator) ISeries {
	values := make([]int64, length)
	for i := 0; i < length; i++ {
		srcIndex := start + i
		if srcIndex < typedArr.Len() && !typedArr.IsNull(srcIndex) {
			values[i] = typedArr.Value(srcIndex)
		}
	}
	return series.New(name, values, mem)
}

// createSlicedFloat64Series creates a float64 series slice
func createSlicedFloat64Series(name string, typedArr *array.Float64, start, length int, mem memory.Allocator) ISeries {
	values := make([]float64, length)
	for i := 0; i < length; i++ {
		srcIndex := start + i
		if srcIndex < typedArr.Len() && !typedArr.IsNull(srcIndex) {
			values[i] = typedArr.Value(srcIndex)
		}
	}
	return series.New(name, values, mem)
}

// createSlicedBoolSeries creates a boolean series slice
func createSlicedBoolSeries(name string, typedArr *array.Boolean, start, length int, mem memory.Allocator) ISeries {
	values := make([]bool, length)
	for i := 0; i < length; i++ {
		srcIndex := start + i
		if srcIndex < typedArr.Len() && !typedArr.IsNull(srcIndex) {
			values[i] = typedArr.Value(srcIndex)
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
