package dataframe

import (
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/paveg/gorilla/expr"
	"github.com/paveg/gorilla/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestDataFrameForLazy(t *testing.T) *DataFrame {
	mem := memory.NewGoAllocator()

	names := series.New("name", []string{"Alice", "Bob", "Charlie", "Diana"}, mem)
	ages := series.New("age", []int64{25, 30, 35, 28}, mem)
	salaries := series.New("salary", []float64{50000, 60000, 70000, 55000}, mem)
	active := series.New("active", []bool{true, true, false, true}, mem)

	return New(names, ages, salaries, active)
}

func TestDataFrameLazy(t *testing.T) {
	df := createTestDataFrameForLazy(t)
	defer df.Release()

	lazyDf := df.Lazy()
	defer lazyDf.Release()

	assert.NotNil(t, lazyDf)
	assert.Equal(t, df, lazyDf.source)
	assert.Empty(t, lazyDf.operations)
	assert.NotNil(t, lazyDf.pool)
}

func TestLazyFrameFilter(t *testing.T) {
	df := createTestDataFrameForLazy(t)
	defer df.Release()

	lazyDf := df.Lazy().Filter(expr.Col("age").Gt(expr.Lit(30)))
	defer lazyDf.Release()

	assert.Len(t, lazyDf.operations, 1)

	// Check that the operation is a FilterOperation
	filterOp, ok := lazyDf.operations[0].(*FilterOperation)
	assert.True(t, ok)
	assert.Contains(t, filterOp.String(), "filter")
	assert.Contains(t, filterOp.String(), "age")
	assert.Contains(t, filterOp.String(), "30")
}

func TestLazyFrameSelect(t *testing.T) {
	df := createTestDataFrameForLazy(t)
	defer df.Release()

	lazyDf := df.Lazy().Select("name", "age")
	defer lazyDf.Release()

	assert.Len(t, lazyDf.operations, 1)

	// Check that the operation is a SelectOperation
	selectOp, ok := lazyDf.operations[0].(*SelectOperation)
	assert.True(t, ok)
	assert.Contains(t, selectOp.String(), "select")
	assert.Contains(t, selectOp.String(), "name")
	assert.Contains(t, selectOp.String(), "age")
}

func TestLazyFrameWithColumn(t *testing.T) {
	df := createTestDataFrameForLazy(t)
	defer df.Release()

	lazyDf := df.Lazy().WithColumn("bonus", expr.Col("salary").Mul(expr.Lit(0.1)))
	defer lazyDf.Release()

	assert.Len(t, lazyDf.operations, 1)

	// Check that the operation is a WithColumnOperation
	withColOp, ok := lazyDf.operations[0].(*WithColumnOperation)
	assert.True(t, ok)
	assert.Contains(t, withColOp.String(), "with_column")
	assert.Contains(t, withColOp.String(), "bonus")
	assert.Contains(t, withColOp.String(), "salary")
}

func TestLazyFrameChaining(t *testing.T) {
	df := createTestDataFrameForLazy(t)
	defer df.Release()

	lazyDf := df.Lazy().
		Filter(expr.Col("age").Gt(expr.Lit(25))).
		WithColumn("bonus", expr.Col("salary").Mul(expr.Lit(0.1))).
		Select("name", "age", "bonus")
	defer lazyDf.Release()

	assert.Len(t, lazyDf.operations, 3)

	// Check operation types
	_, ok := lazyDf.operations[0].(*FilterOperation)
	assert.True(t, ok)

	_, ok = lazyDf.operations[1].(*WithColumnOperation)
	assert.True(t, ok)

	_, ok = lazyDf.operations[2].(*SelectOperation)
	assert.True(t, ok)
}

func TestLazyFrameString(t *testing.T) {
	df := createTestDataFrameForLazy(t)
	defer df.Release()

	lazyDf := df.Lazy().
		Filter(expr.Col("age").Gt(expr.Lit(30))).
		Select("name", "age")
	defer lazyDf.Release()

	str := lazyDf.String()

	assert.Contains(t, str, "LazyFrame:")
	assert.Contains(t, str, "source:")
	assert.Contains(t, str, "operations:")
	assert.Contains(t, str, "1. filter")
	assert.Contains(t, str, "2. select")
}

func TestLazyFrameCollect(t *testing.T) {
	df := createTestDataFrameForLazy(t)
	defer df.Release()

	// Test basic collect with Select operation
	lazyDf := df.Lazy().Select("name", "age")
	defer lazyDf.Release()

	result, err := lazyDf.Collect()
	require.NoError(t, err)
	defer result.Release()

	// Select operation should be applied correctly
	assert.Equal(t, df.Len(), result.Len())
	assert.Equal(t, 2, result.Width()) // Select only selected "name", "age"
}

func TestFilterOperationString(t *testing.T) {
	predicate := expr.Col("age").Gt(expr.Lit(25))
	filterOp := &FilterOperation{predicate: predicate}

	str := filterOp.String()
	assert.Contains(t, str, "filter(")
	assert.Contains(t, str, "age")
	assert.Contains(t, str, "25")
}

func TestSelectOperationString(t *testing.T) {
	selectOp := &SelectOperation{columns: []string{"name", "age", "salary"}}

	str := selectOp.String()
	assert.Contains(t, str, "select(")
	assert.Contains(t, str, "name")
	assert.Contains(t, str, "age")
	assert.Contains(t, str, "salary")
}

func TestWithColumnOperationString(t *testing.T) {
	expr := expr.Col("salary").Mul(expr.Lit(0.1))
	withColOp := &WithColumnOperation{name: "bonus", expr: expr}

	str := withColOp.String()
	assert.Contains(t, str, "with_column(")
	assert.Contains(t, str, "bonus")
	assert.Contains(t, str, "salary")
}

func TestSelectOperationApply(t *testing.T) {
	df := createTestDataFrameForLazy(t)
	defer df.Release()

	selectOp := &SelectOperation{columns: []string{"name", "age"}}

	result, err := selectOp.Apply(df)
	require.NoError(t, err)
	defer result.Release()

	// Verify the select operation worked
	assert.Equal(t, df.Len(), result.Len())
	assert.Equal(t, 2, result.Width())
	assert.Equal(t, []string{"name", "age"}, result.Columns())

	// Verify columns exist
	_, exists := result.Column("name")
	assert.True(t, exists)
	_, exists = result.Column("age")
	assert.True(t, exists)
	_, exists = result.Column("salary")
	assert.False(t, exists)
}

func TestLazyFrameMultipleFilters(t *testing.T) {
	df := createTestDataFrameForLazy(t)
	defer df.Release()

	lazyDf := df.Lazy().
		Filter(expr.Col("age").Gt(expr.Lit(25))).
		Filter(expr.Col("salary").Gt(expr.Lit(55000)))
	defer lazyDf.Release()

	assert.Len(t, lazyDf.operations, 2)

	for _, op := range lazyDf.operations {
		_, ok := op.(*FilterOperation)
		assert.True(t, ok)
	}
}

func TestLazyFrameComplexChaining(t *testing.T) {
	df := createTestDataFrameForLazy(t)
	defer df.Release()

	// Test a complex chain of operations
	lazyDf := df.Lazy().
		Filter(expr.Col("active").Eq(expr.Lit(true))).
		WithColumn("age_bonus", expr.Col("age").Mul(expr.Lit(100))).
		WithColumn("total_comp", expr.Col("salary").Add(expr.Col("age_bonus"))).
		Select("name", "age", "total_comp").
		Filter(expr.Col("age").Gt(expr.Lit(27)))
	defer lazyDf.Release()

	assert.Len(t, lazyDf.operations, 5)

	// Verify operation order and types
	assert.IsType(t, &FilterOperation{}, lazyDf.operations[0])
	assert.IsType(t, &WithColumnOperation{}, lazyDf.operations[1])
	assert.IsType(t, &WithColumnOperation{}, lazyDf.operations[2])
	assert.IsType(t, &SelectOperation{}, lazyDf.operations[3])
	assert.IsType(t, &FilterOperation{}, lazyDf.operations[4])

	// Test string representation
	str := lazyDf.String()
	lines := strings.Split(str, "\n")
	// The string representation should contain multiple lines:
	// LazyFrame: + source info (multiple lines) + operations: + 5 operations
	assert.Greater(t, len(lines), 7) // At least LazyFrame: + source lines + operations: + 5 operations
}

// Test parallel execution for large datasets
func TestLazyFrameParallelExecution(t *testing.T) {
	// Create a large dataset to trigger parallel execution
	mem := memory.NewGoAllocator()

	// Create 2000 rows to exceed the parallel threshold (1000)
	size := 2000
	names := make([]string, size)
	ages := make([]int64, size)
	salaries := make([]float64, size)
	active := make([]bool, size)

	for i := 0; i < size; i++ {
		names[i] = fmt.Sprintf("Employee_%d", i)
		ages[i] = int64(25 + (i % 40))       // Ages 25-64
		salaries[i] = float64(40000 + i*100) // Increasing salaries
		active[i] = i%2 == 0                 // Alternating active status
	}

	nameSeries := series.New("name", names, mem)
	ageSeries := series.New("age", ages, mem)
	salarySeries := series.New("salary", salaries, mem)
	activeSeries := series.New("active", active, mem)

	defer nameSeries.Release()
	defer ageSeries.Release()
	defer salarySeries.Release()
	defer activeSeries.Release()

	df := New(nameSeries, ageSeries, salarySeries, activeSeries)
	defer df.Release()

	// Test parallel execution with complex operations
	lazyDf := df.Lazy().
		Filter(expr.Col("active").Eq(expr.Lit(true))).
		WithColumn("bonus", expr.Col("salary").Mul(expr.Lit(0.1))).
		Filter(expr.Col("age").Gt(expr.Lit(int64(30)))).
		WithColumn("total_comp", expr.Col("salary").Add(expr.Col("bonus"))).
		Select("name", "age", "salary", "bonus", "total_comp")
	defer lazyDf.Release()

	// Execute the lazy operations
	result, err := lazyDf.Collect()
	require.NoError(t, err)
	defer result.Release()

	// Verify results
	assert.Greater(t, result.Len(), 0, "Should have filtered results")
	assert.Equal(t, 5, result.Width(), "Should have 5 columns after operations")

	// Verify all required columns exist
	assert.True(t, result.HasColumn("name"))
	assert.True(t, result.HasColumn("age"))
	assert.True(t, result.HasColumn("salary"))
	assert.True(t, result.HasColumn("bonus"))
	assert.True(t, result.HasColumn("total_comp"))

	// Test that results are correct by checking a few values (only if result is not empty)
	if result.Len() > 0 {
		bonusSeries, _ := result.Column("bonus")
		resultSalarySeries, _ := result.Column("salary")
		totalCompSeries, _ := result.Column("total_comp")

		bonusArray := bonusSeries.Array()
		salaryArray := resultSalarySeries.Array()
		totalCompArray := totalCompSeries.Array()

		defer bonusArray.Release()
		defer salaryArray.Release()
		defer totalCompArray.Release()

		bonusFloat := bonusArray.(*array.Float64)
		salaryFloat := salaryArray.(*array.Float64)
		totalCompFloat := totalCompArray.(*array.Float64)

		// Check that bonus = salary * 0.1 and total_comp = salary + bonus
		for i := 0; i < result.Len() && i < 5; i++ {
			salary := salaryFloat.Value(i)
			bonus := bonusFloat.Value(i)
			totalComp := totalCompFloat.Value(i)

			expectedBonus := salary * 0.1
			expectedTotalComp := salary + bonus

			assert.InDelta(t, expectedBonus, bonus, 0.01, "Bonus calculation should be correct")
			assert.InDelta(t, expectedTotalComp, totalComp, 0.01, "Total compensation calculation should be correct")
		}
	}
}

func TestDataFrameSliceAndConcat(t *testing.T) {
	// Test the new Slice and Concat methods that support parallel execution
	df := createTestDataFrameForLazy(t)
	defer df.Release()

	// Test slicing
	slice1 := df.Slice(0, 2)
	defer slice1.Release()
	assert.Equal(t, 2, slice1.Len(), "Slice should have 2 rows")
	assert.Equal(t, df.Width(), slice1.Width(), "Slice should have same number of columns")

	slice2 := df.Slice(2, 4)
	defer slice2.Release()
	assert.Equal(t, 2, slice2.Len(), "Second slice should have 2 rows")

	// Test concatenation
	concatenated := slice1.Concat(slice2)
	defer concatenated.Release()
	assert.Equal(t, 4, concatenated.Len(), "Concatenated DataFrame should have 4 rows")
	assert.Equal(t, df.Width(), concatenated.Width(), "Concatenated DataFrame should have same number of columns")

	// Test edge cases
	emptySlice := df.Slice(10, 20) // Beyond data range
	defer emptySlice.Release()
	assert.Equal(t, 0, emptySlice.Len(), "Slice beyond range should be empty")

	invalidSlice := df.Slice(3, 1) // Invalid range
	defer invalidSlice.Release()
	assert.Equal(t, 0, invalidSlice.Len(), "Invalid slice range should return empty DataFrame")
}
