package dataframe

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
)

func TestDataFrame_Sort_SingleColumn(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test DataFrame with unsorted data
	namesSeries := series.New("names", []string{"Charlie", "Alice", "Bob"}, mem)
	agesSeries := series.New("ages", []int64{30, 25, 35}, mem)
	df := New(namesSeries, agesSeries)
	defer df.Release()

	// Test ascending sort by names
	result := df.Sort("names", true)
	defer result.Release()

	// Should be sorted: Alice, Bob, Charlie
	namesCol, exists := result.Column("names")
	assert.True(t, exists)
	expectedNames := []string{"Alice", "Bob", "Charlie"}
	actualNames := namesCol.(*series.Series[string]).Values()
	assert.Equal(t, expectedNames, actualNames)

	// Corresponding ages should follow: 25, 35, 30
	agesCol, exists := result.Column("ages")
	assert.True(t, exists)
	expectedAges := []int64{25, 35, 30}
	actualAges := agesCol.(*series.Series[int64]).Values()
	assert.Equal(t, expectedAges, actualAges)
}

func TestDataFrame_Sort_SingleColumn_Descending(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test DataFrame with unsorted numeric data
	valuesSeries := series.New("values", []int64{10, 30, 20}, mem)
	df := New(valuesSeries)
	defer df.Release()

	// Test descending sort
	result := df.Sort("values", false)
	defer result.Release()

	// Should be sorted: 30, 20, 10
	valuesCol, exists := result.Column("values")
	assert.True(t, exists)
	expected := []int64{30, 20, 10}
	actual := valuesCol.(*series.Series[int64]).Values()
	assert.Equal(t, expected, actual)
}

func TestDataFrame_SortBy_MultiColumn(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test DataFrame with multi-column sort scenario
	departmentSeries := series.New("department", []string{"HR", "IT", "HR", "IT"}, mem)
	salarySeries := series.New("salary", []int64{50000, 80000, 60000, 70000}, mem)
	df := New(departmentSeries, salarySeries)
	defer df.Release()

	// Sort by department ASC, then salary DESC
	result := df.SortBy([]string{"department", "salary"}, []bool{true, false})
	defer result.Release()

	// Expected order: HR(60000), HR(50000), IT(80000), IT(70000)
	deptCol, exists := result.Column("department")
	assert.True(t, exists)
	expectedDepts := []string{"HR", "HR", "IT", "IT"}
	actualDepts := deptCol.(*series.Series[string]).Values()
	assert.Equal(t, expectedDepts, actualDepts)

	salaryCol, exists := result.Column("salary")
	assert.True(t, exists)
	expectedSalaries := []int64{60000, 50000, 80000, 70000}
	actualSalaries := salaryCol.(*series.Series[int64]).Values()
	assert.Equal(t, expectedSalaries, actualSalaries)
}

func TestLazyFrame_Sort(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test DataFrame
	valuesSeries := series.New("values", []float64{3.14, 1.41, 2.71}, mem)
	df := New(valuesSeries)
	defer df.Release()

	// Test lazy sort
	lazy := df.Lazy()
	defer lazy.Release()

	sortedLazy := lazy.Sort("values", true)
	defer sortedLazy.Release()

	result, err := sortedLazy.Collect()
	assert.NoError(t, err)
	defer result.Release()

	// Should be sorted: 1.41, 2.71, 3.14
	valuesCol, exists := result.Column("values")
	assert.True(t, exists)
	expected := []float64{1.41, 2.71, 3.14}
	actual := valuesCol.(*series.Series[float64]).Values()
	assert.Equal(t, expected, actual)
}

func TestSort_ErrorCases(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test DataFrame
	valuesSeries := series.New("values", []int64{1, 2, 3}, mem)
	df := New(valuesSeries)
	defer df.Release()

	// Test sorting by non-existent column - should panic/error
	assert.Panics(t, func() {
		result := df.Sort("nonexistent", true)
		defer result.Release()
	})
}

func BenchmarkDataFrame_Sort_Sequential(b *testing.B) {
	mem := memory.NewGoAllocator()

	// Create DataFrame with 500 rows (below parallel threshold)
	values := make([]int64, 500)
	for i := range values {
		values[i] = int64(500 - i) // Reverse order
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		valuesSeries := series.New("values", values, mem)
		df := New(valuesSeries)
		b.StartTimer()

		result := df.Sort("values", true)
		result.Release()
		df.Release()
	}
}

func BenchmarkDataFrame_Sort_Parallel(b *testing.B) {
	mem := memory.NewGoAllocator()

	// Create DataFrame with 5000 rows (above parallel threshold)
	values := make([]int64, 5000)
	for i := range values {
		values[i] = int64(5000 - i) // Reverse order
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		valuesSeries := series.New("values", values, mem)
		df := New(valuesSeries)
		b.StartTimer()

		result := df.Sort("values", true)
		result.Release()
		df.Release()
	}
}
