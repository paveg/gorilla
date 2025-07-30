package io

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	multiplier   = 1.5
	alphabetSize = 26
)

func createParquetTestDataFrame(t *testing.T, mem memory.Allocator) *dataframe.DataFrame {
	s1, err := series.NewSafe("id", []int64{1, 2, 3, 4, 5}, mem)
	require.NoError(t, err)

	s2, err := series.NewSafe("name", []string{"Alice", "Bob", "Charlie", "David", "Eve"}, mem)
	require.NoError(t, err)

	s3, err := series.NewSafe("age", []int64{25, 30, 35, 40, 45}, mem)
	require.NoError(t, err)

	return dataframe.New(s1, s2, s3)
}

func createEmptyDataFrame(t *testing.T, mem memory.Allocator) *dataframe.DataFrame {
	s1, err := series.NewSafe("id", []int64{}, mem)
	require.NoError(t, err)

	s2, err := series.NewSafe("name", []string{}, mem)
	require.NoError(t, err)

	return dataframe.New(s1, s2)
}

func createMixedTypeDataFrame(t *testing.T, mem memory.Allocator) *dataframe.DataFrame {
	s1, err := series.NewSafe("int64_col", []int64{1, 2, 3, 4, 5}, mem)
	require.NoError(t, err)

	s2, err := series.NewSafe("int32_col", []int32{10, 20, 30, 40, 50}, mem)
	require.NoError(t, err)

	s3, err := series.NewSafe("float64_col", []float64{1.1, 2.2, 3.3, 4.4, 5.5}, mem)
	require.NoError(t, err)

	s4, err := series.NewSafe("float32_col", []float32{1.5, 2.5, 3.5, 4.5, 5.5}, mem)
	require.NoError(t, err)

	s5, err := series.NewSafe("string_col", []string{"a", "b", "c", "d", "e"}, mem)
	require.NoError(t, err)

	s6, err := series.NewSafe("bool_col", []bool{true, false, true, false, true}, mem)
	require.NoError(t, err)

	return dataframe.New(s1, s2, s3, s4, s5, s6)
}

func createLargeDataFrame(t *testing.T, mem memory.Allocator, size int) *dataframe.DataFrame {
	ids := make([]int64, size)
	values := make([]float64, size)
	names := make([]string, size)

	for i := range size {
		ids[i] = int64(i)
		values[i] = float64(i) * multiplier
		names[i] = string(rune('A' + (i % alphabetSize)))
	}

	s1, err := series.NewSafe("id", ids, mem)
	require.NoError(t, err)

	s2, err := series.NewSafe("value", values, mem)
	require.NoError(t, err)

	s3, err := series.NewSafe("category", names, mem)
	require.NoError(t, err)

	return dataframe.New(s1, s2, s3)
}

func createDataFrameWithNulls(t *testing.T, mem memory.Allocator) *dataframe.DataFrame {
	// Create series with null values and empty strings to simulate nulls
	ids := []int64{10, 20, 30, 40, 50}
	strings := []string{"x", "", "y", "", "z"} // Empty strings simulate nulls
	floats := []float64{1.1, 0, 3.3, 0, 5.5}   // Zero values simulate nulls

	s1, err := series.NewSafe("nullable_int", ids, mem)
	require.NoError(t, err)

	s2, err := series.NewSafe("nullable_string", strings, mem)
	require.NoError(t, err)

	s3, err := series.NewSafe("nullable_float", floats, mem)
	require.NoError(t, err)

	return dataframe.New(s1, s2, s3)
}

func assertDataFramesEqual(t *testing.T, expected, actual *dataframe.DataFrame) {
	assert.Equal(t, expected.Len(), actual.Len(), "number of rows mismatch")
	assert.Len(t, actual.Columns(), len(expected.Columns()), "number of columns mismatch")
	assert.Equal(t, expected.Columns(), actual.Columns(), "column names mismatch")

	for _, colName := range expected.Columns() {
		expectedCol, _ := expected.Column(colName)
		actualCol, _ := actual.Column(colName)

		assert.Equal(t, expectedCol.DataType(), actualCol.DataType(), "column %s type mismatch", colName)
		assert.Equal(t, expectedCol.Len(), actualCol.Len(), "column %s length mismatch", colName)

		// Compare values based on type
		dataTypeName := expectedCol.DataType().Name()
		switch dataTypeName {
		case "int64":
			expectedSeries := expectedCol.(*series.Series[int64])
			actualSeries := actualCol.(*series.Series[int64])
			assert.Equal(t, expectedSeries.Values(), actualSeries.Values(), "column %s values mismatch", colName)
		case "int32":
			expectedSeries := expectedCol.(*series.Series[int32])
			actualSeries := actualCol.(*series.Series[int32])
			assert.Equal(t, expectedSeries.Values(), actualSeries.Values(), "column %s values mismatch", colName)
		case "float64":
			expectedSeries := expectedCol.(*series.Series[float64])
			actualSeries := actualCol.(*series.Series[float64])
			assert.Equal(t, expectedSeries.Values(), actualSeries.Values(), "column %s values mismatch", colName)
		case "float32":
			expectedSeries := expectedCol.(*series.Series[float32])
			actualSeries := actualCol.(*series.Series[float32])
			assert.Equal(t, expectedSeries.Values(), actualSeries.Values(), "column %s values mismatch", colName)
		case "utf8":
			expectedSeries := expectedCol.(*series.Series[string])
			actualSeries := actualCol.(*series.Series[string])
			assert.Equal(t, expectedSeries.Values(), actualSeries.Values(), "column %s values mismatch", colName)
		case "bool":
			expectedSeries := expectedCol.(*series.Series[bool])
			actualSeries := actualCol.(*series.Series[bool])
			assert.Equal(t, expectedSeries.Values(), actualSeries.Values(), "column %s values mismatch", colName)
		}
	}
}
