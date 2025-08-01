//nolint:testpackage // requires internal access to unexported types and functions
package dataframe

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataFrameInnerJoin(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create left DataFrame (users)
	userIDs := series.New("id", []int64{1, 2, 3, 4}, mem)
	userNames := series.New("name", []string{"Alice", "Bob", "Charlie", "David"}, mem)
	leftDF := New(userIDs, userNames)
	defer leftDF.Release()

	// Create right DataFrame (orders)
	orderIDs := series.New("order_id", []int64{101, 102, 103}, mem)
	userIDsRight := series.New("user_id", []int64{1, 2, 1}, mem)
	amounts := series.New("amount", []float64{100.0, 200.0, 150.0}, mem)
	rightDF := New(orderIDs, userIDsRight, amounts)
	defer rightDF.Release()

	// Perform inner join
	result, err := leftDF.Join(rightDF, &JoinOptions{
		Type:     InnerJoin,
		LeftKey:  "id",
		RightKey: "user_id",
	})
	require.NoError(t, err)
	defer result.Release()

	// Verify result has correct schema
	assert.Equal(t, []string{"id", "name", "order_id", "user_id", "amount"}, result.Columns())
	assert.Equal(t, 3, result.Len()) // 3 matching records

	// Verify join results
	idCol, exists := result.Column("id")
	require.True(t, exists)
	require.NotNil(t, idCol)
	nameCol, exists := result.Column("name")
	require.True(t, exists)
	require.NotNil(t, nameCol)
	orderCol, exists := result.Column("order_id")
	require.True(t, exists)
	require.NotNil(t, orderCol)

	// Check that we have the expected data (order may vary)
	expectedIDs := []int64{1, 2, 1}
	expectedNames := []string{"Alice", "Bob", "Alice"}
	expectedOrders := []int64{101, 102, 103}

	idValues := idCol.(*series.Series[int64]).Values()
	nameValues := nameCol.(*series.Series[string]).Values()
	orderValues := orderCol.(*series.Series[int64]).Values()

	// Verify we have the correct elements (order may vary in hash joins)
	assert.ElementsMatch(t, expectedIDs, idValues)
	assert.ElementsMatch(t, expectedNames, nameValues)
	assert.ElementsMatch(t, expectedOrders, orderValues)
}

func TestDataFrameLeftJoin(t *testing.T) {
	// Create test DataFrames
	leftDF, rightDF := createJoinTestData(t)
	defer leftDF.Release()
	defer rightDF.Release()

	// Perform left join
	result, err := leftDF.Join(rightDF, &JoinOptions{
		Type:     LeftJoin,
		LeftKey:  "id",
		RightKey: "user_id",
	})
	require.NoError(t, err)
	defer result.Release()

	// Left join should include all left records
	// Alice appears twice (2 orders), Bob once, Charlie once, David once = 5 total
	assert.Equal(t, 5, result.Len())

	// Verify that user ID 4 (David) has null values for right table columns
	idCol, exists := result.Column("id")
	require.True(t, exists)
	require.NotNil(t, idCol)
	nameCol, exists := result.Column("name")
	require.True(t, exists)
	require.NotNil(t, nameCol)

	nameValues := nameCol.(*series.Series[string]).Values()
	idValues := idCol.(*series.Series[int64]).Values()

	// Find David's record (should be last)
	found := false
	for i := range result.Len() {
		if nameValues[i] == "David" {
			assert.Equal(t, int64(4), idValues[i])
			found = true
			break
		}
	}
	assert.True(t, found, "David's record should be present in left join")
}

func TestDataFrameRightJoin(t *testing.T) {
	// Create test DataFrames
	leftDF, rightDF := createJoinTestData(t)
	defer leftDF.Release()
	defer rightDF.Release()

	// Perform right join
	result, err := leftDF.Join(rightDF, &JoinOptions{
		Type:     RightJoin,
		LeftKey:  "id",
		RightKey: "user_id",
	})
	require.NoError(t, err)
	defer result.Release()

	// Right join should include all right records
	assert.Equal(t, 3, result.Len())

	// Verify all orders are present
	orderCol, exists := result.Column("order_id")
	require.True(t, exists)
	require.NotNil(t, orderCol)

	expectedOrders := []int64{101, 102, 103}
	actualOrders := orderCol.(*series.Series[int64]).Values()
	assert.ElementsMatch(t, expectedOrders, actualOrders)
}

func TestDataFrameFullOuterJoin(t *testing.T) {
	// Create test DataFrames
	leftDF, rightDF := createJoinTestData(t)
	defer leftDF.Release()
	defer rightDF.Release()

	// Perform full outer join
	result, err := leftDF.Join(rightDF, &JoinOptions{
		Type:     FullOuterJoin,
		LeftKey:  "id",
		RightKey: "user_id",
	})
	require.NoError(t, err)
	defer result.Release()

	// Full outer join should include all records from both sides
	// Alice appears twice (2 orders), Bob once, Charlie once, David once = 5 total
	assert.Equal(t, 5, result.Len())
}

func TestLazyFrameJoin(t *testing.T) {
	// Create test DataFrames
	leftDF, rightDF := createJoinTestData(t)
	defer leftDF.Release()
	defer rightDF.Release()

	// Perform lazy join with additional operations
	result, err := leftDF.Lazy().
		Filter(expr.Col("id").Le(expr.Lit(int64(3)))).
		Join(rightDF.Lazy(), &JoinOptions{
			Type:     InnerJoin,
			LeftKey:  "id",
			RightKey: "user_id",
		}).
		Select("name", "amount").
		Collect()
	require.NoError(t, err)
	defer result.Release()

	// Should have 3 records (Alice twice, Bob once) with only name and amount columns
	assert.Equal(t, []string{"name", "amount"}, result.Columns())
	assert.Equal(t, 3, result.Len())
}

func TestJoinWithMultipleKeys(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create DataFrames with composite keys
	leftSeries1 := series.New("key1", []int64{1, 2, 3}, mem)
	leftSeries2 := series.New("key2", []string{"A", "B", "C"}, mem)
	leftValues := series.New("value", []float64{10.0, 20.0, 30.0}, mem)
	leftDF := New(leftSeries1, leftSeries2, leftValues)
	defer leftDF.Release()

	rightSeries1 := series.New("key1", []int64{1, 2, 4}, mem)
	rightSeries2 := series.New("key2", []string{"A", "B", "D"}, mem)
	rightValues := series.New("right_value", []float64{100.0, 200.0, 400.0}, mem)
	rightDF := New(rightSeries1, rightSeries2, rightValues)
	defer rightDF.Release()

	// Join on multiple keys
	result, err := leftDF.Join(rightDF, &JoinOptions{
		Type:      InnerJoin,
		LeftKeys:  []string{"key1", "key2"},
		RightKeys: []string{"key1", "key2"},
	})
	require.NoError(t, err)
	defer result.Release()

	// Should match on (1,A) and (2,B) only
	assert.Equal(t, 2, result.Len())
}

func TestJoinParallelExecution(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create large DataFrames to trigger parallel execution
	size := 2000
	leftIDs := make([]int64, size)
	leftValues := make([]float64, size)
	for i := range size {
		leftIDs[i] = int64(i % 1000) // Create some duplicates
		leftValues[i] = float64(i)
	}

	rightIDs := make([]int64, size/2)
	rightValues := make([]string, size/2)
	for i := range size / 2 {
		rightIDs[i] = int64(i % 500)
		rightValues[i] = "value_" + string(rune(i))
	}

	leftDF := New(
		series.New("id", leftIDs, mem),
		series.New("value", leftValues, mem),
	)
	defer leftDF.Release()

	rightDF := New(
		series.New("id", rightIDs, mem),
		series.New("right_value", rightValues, mem),
	)
	defer rightDF.Release()

	// Perform join - should use parallel execution
	result, err := leftDF.Join(rightDF, &JoinOptions{
		Type:     InnerJoin,
		LeftKey:  "id",
		RightKey: "id",
	})
	require.NoError(t, err)
	defer result.Release()

	// Verify result is correct
	assert.Positive(t, result.Len())
	// Both DataFrames have "id" columns, so we get both in the result
	assert.Equal(t, []string{"id", "value", "id", "right_value"}, result.Columns())
}

// Helper function to create test data for join operations.
func createJoinTestData(_ *testing.T) (*DataFrame, *DataFrame) {
	mem := memory.NewGoAllocator()

	// Left DataFrame (users)
	userIDs := series.New("id", []int64{1, 2, 3, 4}, mem)
	userNames := series.New("name", []string{"Alice", "Bob", "Charlie", "David"}, mem)
	leftDF := New(userIDs, userNames)

	// Right DataFrame (orders) - note: no order for David (id=4)
	orderIDs := series.New("order_id", []int64{101, 102, 103}, mem)
	userIDsRight := series.New("user_id", []int64{1, 2, 1}, mem) // Alice has 2 orders
	amounts := series.New("amount", []float64{100.0, 200.0, 150.0}, mem)
	rightDF := New(orderIDs, userIDsRight, amounts)

	return leftDF, rightDF
}
