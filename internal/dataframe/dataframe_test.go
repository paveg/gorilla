package dataframe

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
)

func createTestDataFrame(t *testing.T) *DataFrame {
	mem := memory.NewGoAllocator()

	names := series.New("name", []string{"Alice", "Bob", "Charlie"}, mem)
	ages := series.New("age", []int64{25, 30, 35}, mem)
	salaries := series.New("salary", []float64{50000, 60000, 70000}, mem)

	// DataFrame takes ownership of the series - no need to release them manually
	return New(names, ages, salaries)
}

func TestNewDataFrame(t *testing.T) {
	mem := memory.NewGoAllocator()

	names := series.New("name", []string{"Alice", "Bob"}, mem)
	ages := series.New("age", []int64{25, 30}, mem)
	defer names.Release()
	defer ages.Release()

	df := New(names, ages)
	defer df.Release()

	assert.Equal(t, 2, df.Len())
	assert.Equal(t, 2, df.Width())
	assert.Equal(t, []string{"name", "age"}, df.Columns())
}

func TestDataFrameColumns(t *testing.T) {
	df := createTestDataFrame(t)
	defer df.Release()

	columns := df.Columns()
	expected := []string{"name", "age", "salary"}
	assert.Equal(t, expected, columns)
}

func TestDataFrameColumn(t *testing.T) {
	df := createTestDataFrame(t)
	defer df.Release()

	// Test existing column
	nameSeries, exists := df.Column("name")
	assert.True(t, exists)
	assert.Equal(t, "name", nameSeries.Name())
	assert.Equal(t, 3, nameSeries.Len())

	// Test non-existing column
	_, exists = df.Column("nonexistent")
	assert.False(t, exists)
}

func TestDataFrameSelect(t *testing.T) {
	df := createTestDataFrame(t)
	defer df.Release()

	// Test selecting subset of columns
	selected := df.Select("name", "salary")
	defer selected.Release()

	assert.Equal(t, 3, selected.Len())
	assert.Equal(t, 2, selected.Width())
	assert.Equal(t, []string{"name", "salary"}, selected.Columns())

	// Verify the columns exist
	_, exists := selected.Column("name")
	assert.True(t, exists)
	_, exists = selected.Column("salary")
	assert.True(t, exists)
	_, exists = selected.Column("age")
	assert.False(t, exists)
}

func TestDataFrameSelectNonExistentColumns(t *testing.T) {
	df := createTestDataFrame(t)
	defer df.Release()

	// Test selecting non-existent columns
	selected := df.Select("name", "nonexistent", "salary")
	defer selected.Release()

	// Should only include existing columns
	assert.Equal(t, 2, selected.Width())
	assert.Equal(t, []string{"name", "salary"}, selected.Columns())
}

func TestDataFrameDrop(t *testing.T) {
	df := createTestDataFrame(t)
	defer df.Release()

	// Test dropping one column
	dropped := df.Drop("age")
	defer dropped.Release()

	assert.Equal(t, 3, dropped.Len())
	assert.Equal(t, 2, dropped.Width())
	assert.Equal(t, []string{"name", "salary"}, dropped.Columns())

	// Verify the column was removed
	_, exists := dropped.Column("age")
	assert.False(t, exists)
	_, exists = dropped.Column("name")
	assert.True(t, exists)
}

func TestDataFrameDropMultiple(t *testing.T) {
	df := createTestDataFrame(t)
	defer df.Release()

	// Test dropping multiple columns
	dropped := df.Drop("age", "salary")
	defer dropped.Release()

	assert.Equal(t, 3, dropped.Len())
	assert.Equal(t, 1, dropped.Width())
	assert.Equal(t, []string{"name"}, dropped.Columns())
}

func TestDataFrameDropNonExistent(t *testing.T) {
	df := createTestDataFrame(t)
	defer df.Release()

	// Test dropping non-existent column
	dropped := df.Drop("nonexistent")
	defer dropped.Release()

	// Should be unchanged
	assert.Equal(t, 3, dropped.Width())
	assert.Equal(t, []string{"name", "age", "salary"}, dropped.Columns())
}

func TestDataFrameHasColumn(t *testing.T) {
	df := createTestDataFrame(t)
	defer df.Release()

	assert.True(t, df.HasColumn("name"))
	assert.True(t, df.HasColumn("age"))
	assert.True(t, df.HasColumn("salary"))
	assert.False(t, df.HasColumn("nonexistent"))
}

func TestDataFrameString(t *testing.T) {
	df := createTestDataFrame(t)
	defer df.Release()

	str := df.String()
	assert.Contains(t, str, "DataFrame[3x3]")
	assert.Contains(t, str, "name: utf8")
	assert.Contains(t, str, "age: int64")
	assert.Contains(t, str, "salary: float64")
}

func TestEmptyDataFrame(t *testing.T) {
	df := New()
	defer df.Release()

	assert.Equal(t, 0, df.Len())
	assert.Equal(t, 0, df.Width())
	assert.Equal(t, []string{}, df.Columns())
	assert.Contains(t, df.String(), "DataFrame[empty]")
}

func TestDataFrameMismatchedLength(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create series with different lengths
	names := series.New("name", []string{"Alice", "Bob"}, mem)
	ages := series.New("age", []int64{25, 30, 35}, mem) // Different length
	defer names.Release()
	defer ages.Release()

	df := New(names, ages)
	defer df.Release()

	// DataFrame.Len() should return the length of the first column added
	// This test documents current behavior - in a production system,
	// we might want to validate that all columns have the same length
	// The first series added was "names" with length 2
	assert.Equal(t, 2, df.Len())
}
