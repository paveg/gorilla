package io

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCSVEdgeCases(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("handles quoted values with commas", func(t *testing.T) {
		csvData := `name,description
"John Doe","A person with, comma in description"
"Jane Smith","Another person, also with comma"`

		reader := NewCSVReader(strings.NewReader(csvData), DefaultCSVOptions(), mem)
		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 2, df.Len())
		assert.Equal(t, 2, df.Width())
	})

	t.Run("handles quoted values with newlines", func(t *testing.T) {
		csvData := `name,description
"John Doe","A person with
newline in description"
"Jane Smith","Normal description"`

		reader := NewCSVReader(strings.NewReader(csvData), DefaultCSVOptions(), mem)
		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 2, df.Len())
		assert.Equal(t, 2, df.Width())
	})

	t.Run("handles escaped quotes", func(t *testing.T) {
		csvData := `name,quote
"John Doe","He said ""Hello"" to me"
"Jane Smith","She said ""Goodbye"""`

		reader := NewCSVReader(strings.NewReader(csvData), DefaultCSVOptions(), mem)
		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 2, df.Len())
		assert.Equal(t, 2, df.Width())
	})

	t.Run("handles empty fields", func(t *testing.T) {
		csvData := `name,age,salary
Alice,,50000
,30,
Bob,25,`

		reader := NewCSVReader(strings.NewReader(csvData), DefaultCSVOptions(), mem)
		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 3, df.Len())
		assert.Equal(t, 3, df.Width())
	})

	t.Run("handles inconsistent column counts", func(t *testing.T) {
		csvData := `name,age,salary
Alice,25,50000
Bob,30
Charlie,35,70000,extra`

		reader := NewCSVReader(strings.NewReader(csvData), DefaultCSVOptions(), mem)
		_, err := reader.Read()
		// The Go CSV parser is strict about field counts, so this should return an error
		assert.Error(t, err)
	})

	t.Run("handles whitespace with skip initial space", func(t *testing.T) {
		csvData := `name, age, salary
Alice, 25, 50000
Bob, 30, 60000`

		options := DefaultCSVOptions()
		options.SkipInitialSpace = true

		reader := NewCSVReader(strings.NewReader(csvData), options, mem)
		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 2, df.Len())
		assert.Equal(t, 3, df.Width())
		assert.Equal(t, []string{"name", "age", "salary"}, df.Columns())
	})

	t.Run("handles comments", func(t *testing.T) {
		csvData := `name,age,salary
# This is a comment
Alice,25,50000
# Another comment
Bob,30,60000`

		options := DefaultCSVOptions()
		options.Comment = '#'

		reader := NewCSVReader(strings.NewReader(csvData), options, mem)
		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 2, df.Len())
		assert.Equal(t, 3, df.Width())
	})

	t.Run("handles large numbers", func(t *testing.T) {
		csvData := `name,big_int,big_float
Alice,9223372036854775807,1.7976931348623157e+308
Bob,-9223372036854775808,2.2250738585072014e-308`

		reader := NewCSVReader(strings.NewReader(csvData), DefaultCSVOptions(), mem)
		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 2, df.Len())
		assert.Equal(t, 3, df.Width())
	})

	t.Run("handles unicode characters", func(t *testing.T) {
		csvData := `name,city,emoji
Alice,Tokyo,😀
Bob,Москва,🚀
Charlie,München,🎉`

		reader := NewCSVReader(strings.NewReader(csvData), DefaultCSVOptions(), mem)
		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 3, df.Len())
		assert.Equal(t, 3, df.Width())
	})

	t.Run("handles malformed CSV gracefully", func(t *testing.T) {
		csvData := `name,age,salary
Alice,25,50000
Bob,30,60000,extra,fields
Charlie,"unclosed quote`

		reader := NewCSVReader(strings.NewReader(csvData), DefaultCSVOptions(), mem)
		_, err := reader.Read()
		assert.Error(t, err) // Should return an error for malformed CSV
	})

	t.Run("handles very long fields", func(t *testing.T) {
		longString := strings.Repeat("a", 10000)
		csvData := `name,description
Alice,` + longString + `
Bob,short`

		reader := NewCSVReader(strings.NewReader(csvData), DefaultCSVOptions(), mem)
		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 2, df.Len())
		assert.Equal(t, 2, df.Width())
	})
}

func TestCSVWriterEdgeCases(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("writes fields with commas correctly", func(t *testing.T) {
		// Create DataFrame with values containing commas
		names := []string{"John, Jr.", "Jane, Sr."}
		descriptions := []string{"A person with, comma", "Another person, also with comma"}

		namesSeries, _ := series.NewSafe("name", names, mem)
		descSeries, _ := series.NewSafe("description", descriptions, mem)
		df := dataframe.New(namesSeries, descSeries)
		defer df.Release()

		var output strings.Builder
		writer := NewCSVWriter(&output, DefaultCSVOptions())
		err := writer.Write(df)
		require.NoError(t, err)

		result := output.String()
		// Should contain quoted values
		assert.Contains(t, result, `"John, Jr."`)
		assert.Contains(t, result, `"Jane, Sr."`)
	})

	t.Run("writes fields with quotes correctly", func(t *testing.T) {
		// Create DataFrame with values containing quotes
		names := []string{`John "Johnny" Doe`, `Jane "Janie" Smith`}

		namesSeries, _ := series.NewSafe("name", names, mem)
		df := dataframe.New(namesSeries)
		defer df.Release()

		var output strings.Builder
		writer := NewCSVWriter(&output, DefaultCSVOptions())
		err := writer.Write(df)
		require.NoError(t, err)

		result := output.String()
		// Should contain escaped quotes
		assert.Contains(t, result, `"John ""Johnny"" Doe"`)
		assert.Contains(t, result, `"Jane ""Janie"" Smith"`)
	})

	t.Run("writes empty DataFrame", func(t *testing.T) {
		df := dataframe.New()
		defer df.Release()

		var output strings.Builder
		writer := NewCSVWriter(&output, DefaultCSVOptions())
		err := writer.Write(df)
		require.NoError(t, err)

		result := output.String()
		// Empty DataFrame with headers enabled should write empty header line
		assert.Equal(t, "\n", result)
	})

	t.Run("writes DataFrame with empty columns", func(t *testing.T) {
		emptySeries, _ := series.NewSafe("empty", []string{}, mem)
		df := dataframe.New(emptySeries)
		defer df.Release()

		var output strings.Builder
		writer := NewCSVWriter(&output, DefaultCSVOptions())
		err := writer.Write(df)
		require.NoError(t, err)

		result := output.String()
		assert.Equal(t, "empty\n", result)
	})
}

func TestCSVTypeInferenceEdgeCases(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("handles mixed valid and invalid integers", func(t *testing.T) {
		csvData := `values
123
abc
456`

		reader := NewCSVReader(strings.NewReader(csvData), DefaultCSVOptions(), mem)
		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 3, df.Len())
		// Should fall back to string type
		valuesCol, _ := df.Column("values")
		assert.Equal(t, "utf8", valuesCol.DataType().Name())
	})

	t.Run("handles scientific notation", func(t *testing.T) {
		csvData := `values
1.23e10
4.56e-5
789`

		reader := NewCSVReader(strings.NewReader(csvData), DefaultCSVOptions(), mem)
		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 3, df.Len())
		// Should be inferred as float
		valuesCol, _ := df.Column("values")
		assert.Equal(t, "float64", valuesCol.DataType().Name())
	})

	t.Run("handles boolean variations", func(t *testing.T) {
		csvData := `values
true
false
True
False`

		reader := NewCSVReader(strings.NewReader(csvData), DefaultCSVOptions(), mem)
		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 4, df.Len())
		// Should be inferred as bool
		valuesCol, _ := df.Column("values")
		assert.Equal(t, "bool", valuesCol.DataType().Name())
	})

	t.Run("handles all empty values", func(t *testing.T) {
		// Test case with all empty values (not empty lines)
		csvData := `values
""
""
""`

		reader := NewCSVReader(strings.NewReader(csvData), DefaultCSVOptions(), mem)
		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 3, df.Len())
		// Should default to string type
		valuesCol, _ := df.Column("values")
		assert.Equal(t, "utf8", valuesCol.DataType().Name())
	})
}
