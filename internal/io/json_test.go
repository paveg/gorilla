package io_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/io"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONReader_ReadArray(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("basic JSON array", func(t *testing.T) {
		jsonData := `[
			{"id": 1, "name": "Alice", "age": 30, "active": true},
			{"id": 2, "name": "Bob", "age": 25, "active": false},
			{"id": 3, "name": "Charlie", "age": 35, "active": true}
		]`

		options := io.DefaultJSONOptions()
		options.Format = io.JSONArray
		reader := io.NewJSONReader(strings.NewReader(jsonData), options, mem)

		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 3, df.Len())
		assert.Len(t, df.Columns(), 4)
		assert.Contains(t, df.Columns(), "id")
		assert.Contains(t, df.Columns(), "name")
		assert.Contains(t, df.Columns(), "age")
		assert.Contains(t, df.Columns(), "active")
	})

	t.Run("empty JSON array", func(t *testing.T) {
		jsonData := `[]`

		options := io.DefaultJSONOptions()
		options.Format = io.JSONArray
		reader := io.NewJSONReader(strings.NewReader(jsonData), options, mem)

		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 0, df.Len())
		assert.Empty(t, df.Columns())
	})

	t.Run("mixed types with type inference", func(t *testing.T) {
		jsonData := `[
			{"mixed": 123, "float_val": 1.5, "bool_val": true, "str_val": "hello"},
			{"mixed": "456", "float_val": 2.7, "bool_val": false, "str_val": "world"},
			{"mixed": 789, "float_val": 3.14, "bool_val": true, "str_val": "test"}
		]`

		options := io.DefaultJSONOptions()
		options.Format = io.JSONArray
		options.TypeInference = true
		reader := io.NewJSONReader(strings.NewReader(jsonData), options, mem)

		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		// Check that mixed column becomes string due to mixed types
		mixedCol, exists := df.Column("mixed")
		require.True(t, exists)
		assert.Equal(t, "utf8", mixedCol.DataType().Name())

		// Check that float column is float64
		floatCol, exists := df.Column("float_val")
		require.True(t, exists)
		assert.Equal(t, "float64", floatCol.DataType().Name())

		// Check that bool column is bool
		boolCol, exists := df.Column("bool_val")
		require.True(t, exists)
		assert.Equal(t, "bool", boolCol.DataType().Name())
	})

	t.Run("with max records limit", func(t *testing.T) {
		jsonData := `[
			{"id": 1, "name": "Alice"},
			{"id": 2, "name": "Bob"},
			{"id": 3, "name": "Charlie"},
			{"id": 4, "name": "David"},
			{"id": 5, "name": "Eve"}
		]`

		options := io.DefaultJSONOptions()
		options.Format = io.JSONArray
		options.MaxRecords = 3
		reader := io.NewJSONReader(strings.NewReader(jsonData), options, mem)

		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 3, df.Len()) // Should only read 3 records
	})

	t.Run("without type inference", func(t *testing.T) {
		jsonData := `[
			{"id": 1, "name": "Alice", "active": true},
			{"id": 2, "name": "Bob", "active": false}
		]`

		options := io.DefaultJSONOptions()
		options.Format = io.JSONArray
		options.TypeInference = false
		reader := io.NewJSONReader(strings.NewReader(jsonData), options, mem)

		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		// All columns should be strings when type inference is disabled
		for _, colName := range df.Columns() {
			col, _ := df.Column(colName)
			assert.Equal(t, "utf8", col.DataType().Name(), "column %s should be string", colName)
		}
	})
}

func TestJSONReader_ReadLines(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("basic JSON Lines", func(t *testing.T) {
		jsonData := `{"id": 1, "name": "Alice", "age": 30}
{"id": 2, "name": "Bob", "age": 25}
{"id": 3, "name": "Charlie", "age": 35}`

		options := io.DefaultJSONOptions()
		options.Format = io.JSONLines
		reader := io.NewJSONReader(strings.NewReader(jsonData), options, mem)

		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 3, df.Len())
		assert.Len(t, df.Columns(), 3)
	})

	t.Run("JSON Lines with empty lines", func(t *testing.T) {
		jsonData := `{"id": 1, "name": "Alice"}

{"id": 2, "name": "Bob"}

{"id": 3, "name": "Charlie"}`

		options := io.DefaultJSONOptions()
		options.Format = io.JSONLines
		reader := io.NewJSONReader(strings.NewReader(jsonData), options, mem)

		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 3, df.Len()) // Empty lines should be skipped
	})

	t.Run("inconsistent schema", func(t *testing.T) {
		jsonData := `{"id": 1, "name": "Alice", "age": 30}
{"id": 2, "name": "Bob"}
{"id": 3, "email": "charlie@example.com"}`

		options := io.DefaultJSONOptions()
		options.Format = io.JSONLines
		reader := io.NewJSONReader(strings.NewReader(jsonData), options, mem)

		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 3, df.Len())
		assert.Len(t, df.Columns(), 4) // id, name, age, email
		assert.Contains(t, df.Columns(), "id")
		assert.Contains(t, df.Columns(), "name")
		assert.Contains(t, df.Columns(), "age")
		assert.Contains(t, df.Columns(), "email")
	})
}

func TestJSONWriter_WriteArray(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("basic JSON array write", func(t *testing.T) {
		// Create test DataFrame
		s1, err := series.NewSafe("id", []int64{1, 2, 3}, mem)
		require.NoError(t, err)
		s2, err := series.NewSafe("name", []string{"Alice", "Bob", "Charlie"}, mem)
		require.NoError(t, err)
		s3, err := series.NewSafe("active", []bool{true, false, true}, mem)
		require.NoError(t, err)

		df := dataframe.New(s1, s2, s3)
		defer df.Release()

		// Write to JSON
		buf := new(bytes.Buffer)
		options := io.DefaultJSONOptions()
		options.Format = io.JSONArray
		writer := io.NewJSONWriter(buf, options)

		err = writer.Write(df)
		require.NoError(t, err)

		// Verify output is valid JSON
		result := buf.String()
		assert.Contains(t, result, `"id":1`)
		assert.Contains(t, result, `"name":"Alice"`)
		assert.Contains(t, result, `"active":true`)

		// Read back and verify
		reader := io.NewJSONReader(strings.NewReader(result), options, mem)
		readDF, err := reader.Read()
		require.NoError(t, err)
		defer readDF.Release()

		assert.Equal(t, df.Len(), readDF.Len())
		assert.Len(t, readDF.Columns(), len(df.Columns()))
	})

	t.Run("empty DataFrame", func(t *testing.T) {
		df := dataframe.New()
		defer df.Release()

		buf := new(bytes.Buffer)
		options := io.DefaultJSONOptions()
		options.Format = io.JSONArray
		writer := io.NewJSONWriter(buf, options)

		err := writer.Write(df)
		require.NoError(t, err)

		assert.Equal(t, "[]", buf.String())
	})
}

func TestJSONWriter_WriteLines(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("basic JSON Lines write", func(t *testing.T) {
		// Create test DataFrame
		s1, err := series.NewSafe("id", []int64{1, 2}, mem)
		require.NoError(t, err)
		s2, err := series.NewSafe("name", []string{"Alice", "Bob"}, mem)
		require.NoError(t, err)

		df := dataframe.New(s1, s2)
		defer df.Release()

		// Write to JSON Lines
		buf := new(bytes.Buffer)
		options := io.DefaultJSONOptions()
		options.Format = io.JSONLines
		writer := io.NewJSONWriter(buf, options)

		err = writer.Write(df)
		require.NoError(t, err)

		// Verify output format
		result := buf.String()
		lines := strings.Split(strings.TrimSpace(result), "\n")
		assert.Len(t, lines, 2)

		// Each line should be valid JSON
		for _, line := range lines {
			assert.True(t, strings.HasPrefix(line, "{"))
			assert.True(t, strings.HasSuffix(line, "}"))
		}

		// Read back and verify
		reader := io.NewJSONReader(strings.NewReader(result), options, mem)
		readDF, err := reader.Read()
		require.NoError(t, err)
		defer readDF.Release()

		assert.Equal(t, df.Len(), readDF.Len())
		assert.Len(t, readDF.Columns(), len(df.Columns()))
	})

	t.Run("empty DataFrame", func(t *testing.T) {
		df := dataframe.New()
		defer df.Release()

		buf := new(bytes.Buffer)
		options := io.DefaultJSONOptions()
		options.Format = io.JSONLines
		writer := io.NewJSONWriter(buf, options)

		err := writer.Write(df)
		require.NoError(t, err)

		assert.Empty(t, buf.String()) // Empty output for JSON Lines
	})
}

func TestJSONRoundTrip(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("JSON array round trip", func(t *testing.T) {
		// Create original DataFrame with mixed types
		s1, err := series.NewSafe("int_col", []int64{1, 2, 3, 4, 5}, mem)
		require.NoError(t, err)
		s2, err := series.NewSafe("float_col", []float64{1.1, 2.2, 3.3, 4.4, 5.5}, mem)
		require.NoError(t, err)
		s3, err := series.NewSafe("string_col", []string{"a", "b", "c", "d", "e"}, mem)
		require.NoError(t, err)
		s4, err := series.NewSafe("bool_col", []bool{true, false, true, false, true}, mem)
		require.NoError(t, err)

		original := dataframe.New(s1, s2, s3, s4)
		defer original.Release()

		// Write to JSON array
		buf := new(bytes.Buffer)
		options := io.DefaultJSONOptions()
		options.Format = io.JSONArray
		writer := io.NewJSONWriter(buf, options)

		err = writer.Write(original)
		require.NoError(t, err)

		// Read back from JSON
		reader := io.NewJSONReader(strings.NewReader(buf.String()), options, mem)
		result, err := reader.Read()
		require.NoError(t, err)
		defer result.Release()

		// Verify basic structure
		assert.Equal(t, original.Len(), result.Len())
		assert.Len(t, result.Columns(), len(original.Columns()))

		// Verify column names are preserved
		for _, colName := range original.Columns() {
			_, exists := result.Column(colName)
			assert.True(t, exists, "column %s should exist", colName)
		}
	})

	t.Run("JSON Lines round trip", func(t *testing.T) {
		// Create test DataFrame
		s1, err := series.NewSafe("x", []int64{10, 20, 30}, mem)
		require.NoError(t, err)
		s2, err := series.NewSafe("y", []string{"foo", "bar", "baz"}, mem)
		require.NoError(t, err)

		original := dataframe.New(s1, s2)
		defer original.Release()

		// Write to JSON Lines
		buf := new(bytes.Buffer)
		options := io.DefaultJSONOptions()
		options.Format = io.JSONLines
		writer := io.NewJSONWriter(buf, options)

		err = writer.Write(original)
		require.NoError(t, err)

		// Read back from JSON Lines
		reader := io.NewJSONReader(strings.NewReader(buf.String()), options, mem)
		result, err := reader.Read()
		require.NoError(t, err)
		defer result.Release()

		// Verify basic structure
		assert.Equal(t, original.Len(), result.Len())
		assert.Len(t, result.Columns(), len(original.Columns()))
	})
}

func TestJSONEdgeCases(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("handles null values", func(t *testing.T) {
		jsonData := `[
			{"id": 1, "name": "Alice", "value": null},
			{"id": 2, "name": null, "value": 42},
			{"id": null, "name": "Charlie", "value": 0}
		]`

		options := io.DefaultJSONOptions()
		options.Format = io.JSONArray
		reader := io.NewJSONReader(strings.NewReader(jsonData), options, mem)

		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 3, df.Len())
		assert.Len(t, df.Columns(), 3)
	})

	t.Run("handles invalid JSON gracefully", func(t *testing.T) {
		jsonData := `{"invalid": json}`

		options := io.DefaultJSONOptions()
		options.Format = io.JSONArray
		reader := io.NewJSONReader(strings.NewReader(jsonData), options, mem)

		_, err := reader.Read()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshaling")
	})

	t.Run("handles very large numbers", func(t *testing.T) {
		jsonData := `[
			{"big_int": 9223372036854775807, "big_float": 1.7976931348623157e+308},
			{"big_int": -9223372036854775808, "big_float": -1.7976931348623157e+308}
		]`

		options := io.DefaultJSONOptions()
		options.Format = io.JSONArray
		reader := io.NewJSONReader(strings.NewReader(jsonData), options, mem)

		df, err := reader.Read()
		require.NoError(t, err)
		defer df.Release()

		assert.Equal(t, 2, df.Len())
	})
}