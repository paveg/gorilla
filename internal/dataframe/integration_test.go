package dataframe

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createIntegrationTestDataFrame(t *testing.T) *DataFrame {
	mem := memory.NewGoAllocator()

	names := series.New("name", []string{"Alice", "Bob", "Charlie", "Diana", "Eve"}, mem)
	ages := series.New("age", []int64{25, 30, 35, 28, 32}, mem)
	salaries := series.New("salary", []float64{50000, 60000, 70000, 55000, 65000}, mem)
	active := series.New("active", []bool{true, true, false, true, true}, mem)

	return New(names, ages, salaries, active)
}

func TestIntegrationFilterOperation(t *testing.T) {
	df := createIntegrationTestDataFrame(t)
	defer df.Release()

	// Test filter operation: age > 28
	lazyDf := df.Lazy().Filter(expr.Col("age").Gt(expr.Lit(int64(28))))
	defer lazyDf.Release()

	result, err := lazyDf.Collect()
	require.NoError(t, err)
	defer result.Release()

	// Should have 3 rows: Bob (30), Charlie (35), Eve (32)
	assert.Equal(t, 3, result.Len())
	assert.Equal(t, 4, result.Width()) // All columns preserved

	// Verify the filtered data
	nameSeries, exists := result.Column("name")
	require.True(t, exists)
	nameArray := nameSeries.Array()
	defer nameArray.Release()

	ageSeries, exists := result.Column("age")
	require.True(t, exists)
	ageArray := ageSeries.Array()
	defer ageArray.Release()

	// Convert to typed arrays for verification
	nameStringArray := nameArray.(*array.String)
	ageInt64Array := ageArray.(*array.Int64)

	expectedNames := []string{"Bob", "Charlie", "Eve"}
	expectedAges := []int64{30, 35, 32}

	for i := 0; i < result.Len(); i++ {
		assert.Equal(t, expectedNames[i], nameStringArray.Value(i))
		assert.Equal(t, expectedAges[i], ageInt64Array.Value(i))
	}
}

func TestIntegrationWithColumnOperation(t *testing.T) {
	df := createIntegrationTestDataFrame(t)
	defer df.Release()

	// Test WithColumn operation: add bonus = salary * 0.1
	lazyDf := df.Lazy().WithColumn("bonus", expr.Col("salary").Mul(expr.Lit(0.1)))
	defer lazyDf.Release()

	result, err := lazyDf.Collect()
	require.NoError(t, err)
	defer result.Release()

	// Should have same number of rows but one additional column
	assert.Equal(t, 5, result.Len())
	assert.Equal(t, 5, result.Width()) // Original 4 + 1 new column

	// Verify bonus column exists and has correct values
	bonusSeries, exists := result.Column("bonus")
	require.True(t, exists)
	bonusArray := bonusSeries.Array()
	defer bonusArray.Release()

	bonusFloat64Array := bonusArray.(*array.Float64)
	expectedBonuses := []float64{5000.0, 6000.0, 7000.0, 5500.0, 6500.0}

	for i := 0; i < result.Len(); i++ {
		assert.InDelta(t, expectedBonuses[i], bonusFloat64Array.Value(i), 0.01)
	}
}

func TestIntegrationComplexChaining(t *testing.T) {
	df := createIntegrationTestDataFrame(t)
	defer df.Release()

	// Test complex chain: filter active users, add bonus, filter by bonus > 5500, select specific columns
	lazyDf := df.Lazy().
		Filter(expr.Col("active").Eq(expr.Lit(true))).
		WithColumn("bonus", expr.Col("salary").Mul(expr.Lit(0.1))).
		Filter(expr.Col("bonus").Gt(expr.Lit(5500.0))).
		Select("name", "salary", "bonus")
	defer lazyDf.Release()

	result, err := lazyDf.Collect()
	require.NoError(t, err)
	defer result.Release()

	// Should have 3 rows: Bob (6000), Eve (6500) - Charlie is filtered out by active=false
	assert.Equal(t, 2, result.Len())
	assert.Equal(t, 3, result.Width()) // Only selected columns

	// Verify columns
	assert.True(t, result.HasColumn("name"))
	assert.True(t, result.HasColumn("salary"))
	assert.True(t, result.HasColumn("bonus"))
	assert.False(t, result.HasColumn("age"))
	assert.False(t, result.HasColumn("active"))

	// Verify data
	nameSeries, _ := result.Column("name")
	nameArray := nameSeries.Array()
	defer nameArray.Release()
	nameStringArray := nameArray.(*array.String)

	expectedNames := []string{"Bob", "Eve"}
	for i := 0; i < result.Len(); i++ {
		assert.Equal(t, expectedNames[i], nameStringArray.Value(i))
	}
}

func TestIntegrationStringOperations(t *testing.T) {
	df := createIntegrationTestDataFrame(t)
	defer df.Release()

	// Test string equality filter
	lazyDf := df.Lazy().Filter(expr.Col("name").Eq(expr.Lit("Alice")))
	defer lazyDf.Release()

	result, err := lazyDf.Collect()
	require.NoError(t, err)
	defer result.Release()

	// Should have 1 row
	assert.Equal(t, 1, result.Len())

	// Verify it's Alice
	nameSeries, _ := result.Column("name")
	nameArray := nameSeries.Array()
	defer nameArray.Release()
	nameStringArray := nameArray.(*array.String)

	assert.Equal(t, "Alice", nameStringArray.Value(0))
}

func TestIntegrationBooleanOperations(t *testing.T) {
	df := createIntegrationTestDataFrame(t)
	defer df.Release()

	// Test logical AND: active=true AND age > 30
	predicate := expr.Col("active").Eq(expr.Lit(true)).And(expr.Col("age").Gt(expr.Lit(int64(30))))
	lazyDf := df.Lazy().Filter(predicate)
	defer lazyDf.Release()

	result, err := lazyDf.Collect()
	require.NoError(t, err)
	defer result.Release()

	// Should have 1 row: Eve (active=true AND age=32 > 30)
	// Bob is age=30 which is not > 30
	// Charlie is age=35 but active=false
	assert.Equal(t, 1, result.Len())

	nameSeries, _ := result.Column("name")
	nameArray := nameSeries.Array()
	defer nameArray.Release()
	nameStringArray := nameArray.(*array.String)

	assert.Equal(t, "Eve", nameStringArray.Value(0))
}

func TestIntegrationEmptyResultFilter(t *testing.T) {
	df := createIntegrationTestDataFrame(t)
	defer df.Release()

	// Test filter that returns no results
	lazyDf := df.Lazy().Filter(expr.Col("age").Gt(expr.Lit(int64(100))))
	defer lazyDf.Release()

	result, err := lazyDf.Collect()
	require.NoError(t, err)
	defer result.Release()

	// Should have 0 rows but same column structure
	assert.Equal(t, 0, result.Len())
	assert.Equal(t, 4, result.Width())

	// Verify all columns exist
	assert.True(t, result.HasColumn("name"))
	assert.True(t, result.HasColumn("age"))
	assert.True(t, result.HasColumn("salary"))
	assert.True(t, result.HasColumn("active"))
}

func TestIntegrationArithmeticOperations(t *testing.T) {
	df := createIntegrationTestDataFrame(t)
	defer df.Release()

	// Test arithmetic in WithColumn: total_comp = salary + (age * 1000)
	ageBonus := expr.Col("age").Mul(expr.Lit(int64(1000)))
	totalComp := expr.Col("salary").Add(ageBonus)

	lazyDf := df.Lazy().WithColumn("total_comp", totalComp)
	defer lazyDf.Release()

	result, err := lazyDf.Collect()
	require.NoError(t, err)
	defer result.Release()

	// Verify total_comp calculation
	totalCompSeries, exists := result.Column("total_comp")
	require.True(t, exists)
	totalCompArray := totalCompSeries.Array()
	defer totalCompArray.Release()
	totalCompFloat64Array := totalCompArray.(*array.Float64)

	// Expected: salary + (age * 1000)
	// Alice: 50000 + (25 * 1000) = 75000
	// Bob: 60000 + (30 * 1000) = 90000
	// etc.
	expectedTotalComp := []float64{75000, 90000, 105000, 83000, 97000}

	for i := 0; i < result.Len(); i++ {
		assert.InDelta(t, expectedTotalComp[i], totalCompFloat64Array.Value(i), 0.01)
	}
}

func TestIntegrationSelectAfterWithColumn(t *testing.T) {
	df := createIntegrationTestDataFrame(t)
	defer df.Release()

	// Test that we can select a computed column
	lazyDf := df.Lazy().
		WithColumn("double_age", expr.Col("age").Mul(expr.Lit(int64(2)))).
		Select("name", "double_age")
	defer lazyDf.Release()

	result, err := lazyDf.Collect()
	require.NoError(t, err)
	defer result.Release()

	assert.Equal(t, 5, result.Len())
	assert.Equal(t, 2, result.Width())

	// Verify we can access the computed column
	doubleAgeSeries, exists := result.Column("double_age")
	require.True(t, exists)
	doubleAgeArray := doubleAgeSeries.Array()
	defer doubleAgeArray.Release()
	doubleAgeInt64Array := doubleAgeArray.(*array.Int64)

	expectedDoubleAges := []int64{50, 60, 70, 56, 64}
	for i := 0; i < result.Len(); i++ {
		assert.Equal(t, expectedDoubleAges[i], doubleAgeInt64Array.Value(i))
	}
}
