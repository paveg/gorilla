package io_test

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/io"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCSVReader(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("reads simple CSV with headers", func(t *testing.T) {
		csvData := `name,age,salary
Alice,25,50000
Bob,30,60000
Charlie,35,70000`

		reader := io.NewCSVReader(strings.NewReader(csvData), io.DefaultCSVOptions(), mem)
		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 3, df.Len())
		assert.Equal(t, 3, df.Width())
		assert.Equal(t, []string{"name", "age", "salary"}, df.Columns())

		// Check first row values
		nameCol, exists := df.Column("name")
		require.True(t, exists)
		nameArray := nameCol.Array()
		defer nameArray.Release()
		nameStringArray := nameArray.(*array.String)
		assert.Equal(t, "Alice", nameStringArray.Value(0))

		ageCol, exists := df.Column("age")
		require.True(t, exists)
		ageArray := ageCol.Array()
		defer ageArray.Release()
		ageInt64Array := ageArray.(*array.Int64)
		assert.Equal(t, int64(25), ageInt64Array.Value(0))

		salaryCol, exists := df.Column("salary")
		require.True(t, exists)
		salaryArray := salaryCol.Array()
		defer salaryArray.Release()
		salaryInt64Array := salaryArray.(*array.Int64)
		assert.Equal(t, int64(50000), salaryInt64Array.Value(0))
	})

	t.Run("reads CSV without headers", func(t *testing.T) {
		csvData := `Alice,25,50000
Bob,30,60000
Charlie,35,70000`

		options := io.DefaultCSVOptions()
		options.Header = false

		reader := io.NewCSVReader(strings.NewReader(csvData), options, mem)
		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 3, df.Len())
		assert.Equal(t, 3, df.Width())
		// Should have default column names
		assert.Equal(t, []string{"column_0", "column_1", "column_2"}, df.Columns())
	})

	t.Run("reads CSV with custom delimiter", func(t *testing.T) {
		csvData := `name;age;salary
Alice;25;50000
Bob;30;60000`

		options := io.DefaultCSVOptions()
		options.Delimiter = ';'

		reader := io.NewCSVReader(strings.NewReader(csvData), options, mem)
		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 2, df.Len())
		assert.Equal(t, 3, df.Width())
		assert.Equal(t, []string{"name", "age", "salary"}, df.Columns())
	})

	t.Run("handles empty CSV", func(t *testing.T) {
		csvData := ``

		reader := io.NewCSVReader(strings.NewReader(csvData), io.DefaultCSVOptions(), mem)
		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 0, df.Len())
		assert.Equal(t, 0, df.Width())
	})

	t.Run("handles CSV with only headers", func(t *testing.T) {
		csvData := `name,age,salary`

		reader := io.NewCSVReader(strings.NewReader(csvData), io.DefaultCSVOptions(), mem)
		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 0, df.Len())
		assert.Equal(t, 3, df.Width())
		assert.Equal(t, []string{"name", "age", "salary"}, df.Columns())
	})

	t.Run("handles mixed data types", func(t *testing.T) {
		csvData := `name,age,salary,active
Alice,25,50000.5,true
Bob,30,60000.0,false
Charlie,35,70000.25,true`

		reader := io.NewCSVReader(strings.NewReader(csvData), io.DefaultCSVOptions(), mem)
		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 3, df.Len())
		assert.Equal(t, 4, df.Width())
		assert.Equal(t, []string{"name", "age", "salary", "active"}, df.Columns())

		// Check that types are inferred correctly
		nameCol, _ := df.Column("name")
		nameArray := nameCol.Array()
		defer nameArray.Release()
		nameStringArray := nameArray.(*array.String)
		assert.Equal(t, "Alice", nameStringArray.Value(0))

		ageCol, _ := df.Column("age")
		ageArray := ageCol.Array()
		defer ageArray.Release()
		ageInt64Array := ageArray.(*array.Int64)
		assert.Equal(t, int64(25), ageInt64Array.Value(0))

		salaryCol, _ := df.Column("salary")
		salaryArray := salaryCol.Array()
		defer salaryArray.Release()
		salaryFloat64Array := salaryArray.(*array.Float64)
		assert.InDelta(t, 50000.5, salaryFloat64Array.Value(0), 0.001)

		activeCol, _ := df.Column("active")
		activeArray := activeCol.Array()
		defer activeArray.Release()
		activeBoolArray := activeArray.(*array.Boolean)
		assert.True(t, activeBoolArray.Value(0))
	})
}

func TestCSVWriter(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("writes simple CSV with headers", func(t *testing.T) {
		// Create test DataFrame
		df := createTestDataFrame(mem)
		defer df.Release()

		var output strings.Builder
		writer := io.NewCSVWriter(&output, io.DefaultCSVOptions())
		err := writer.Write(df)
		require.NoError(t, err)

		expectedCSV := `name,age,salary
Alice,25,50000
Bob,30,60000
Charlie,35,70000
`
		assert.Equal(t, expectedCSV, output.String())
	})

	t.Run("writes CSV without headers", func(t *testing.T) {
		// Create test DataFrame
		df := createTestDataFrame(mem)
		defer df.Release()

		options := io.DefaultCSVOptions()
		options.Header = false

		var output strings.Builder
		writer := io.NewCSVWriter(&output, options)
		err := writer.Write(df)
		require.NoError(t, err)

		expectedCSV := `Alice,25,50000
Bob,30,60000
Charlie,35,70000
`
		assert.Equal(t, expectedCSV, output.String())
	})

	t.Run("writes CSV with custom delimiter", func(t *testing.T) {
		// Create test DataFrame
		df := createTestDataFrame(mem)
		defer df.Release()

		options := io.DefaultCSVOptions()
		options.Delimiter = ';'

		var output strings.Builder
		writer := io.NewCSVWriter(&output, options)
		err := writer.Write(df)
		require.NoError(t, err)

		expectedCSV := `name;age;salary
Alice;25;50000
Bob;30;60000
Charlie;35;70000
`
		assert.Equal(t, expectedCSV, output.String())
	})
}

// Helper function to create test DataFrame.
func createTestDataFrame(mem memory.Allocator) *dataframe.DataFrame {
	names := []string{"Alice", "Bob", "Charlie"}
	ages := []int64{25, 30, 35}
	salaries := []int64{50000, 60000, 70000}

	namesSeries, _ := series.NewSafe("name", names, mem)
	agesSeries, _ := series.NewSafe("age", ages, mem)
	salariesSeries, _ := series.NewSafe("salary", salaries, mem)

	return dataframe.New(namesSeries, agesSeries, salariesSeries)
}
