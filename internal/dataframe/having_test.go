package dataframe

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHavingOperation_Basic(t *testing.T) {
	t.Run("creates having operation with valid predicate", func(t *testing.T) {
		// Define expected behavior: HavingOperation should accept aggregation predicates
		predicate := expr.Sum(expr.Col("sales")).Gt(expr.Lit(1000.0))

		op := NewHavingOperation(predicate)

		assert.NotNil(t, op)
		assert.Equal(t, "Having", op.Name())
		assert.Equal(t, predicate, op.predicate)
	})

	t.Run("having operation string representation", func(t *testing.T) {
		predicate := expr.Count(expr.Col("id")).Gt(expr.Lit(int64(5)))

		op := NewHavingOperation(predicate)

		expected := "Having((count(col(id)) > lit(5)))"
		assert.Equal(t, expected, op.String())
	})
}

func TestHavingOperation_Apply(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("filters groups based on aggregation predicate", func(t *testing.T) {
		// Create test DataFrame
		categories := series.New("category", []string{"A", "B", "A", "B", "A", "C"}, mem)
		values := series.New("value", []int64{10, 20, 30, 40, 50, 60}, mem)

		df := New(categories, values)
		defer df.Release()

		// Apply HAVING SUM(value) > 70 through lazy evaluation
		// This simulates: SELECT category, SUM(value) FROM df GROUP BY category HAVING SUM(value) > 70
		havingPredicate := expr.Sum(expr.Col("value")).Gt(expr.Lit(int64(70)))

		lazy := df.Lazy().GroupBy("category").Having(havingPredicate)
		result, err := lazy.Collect(context.Background())
		require.NoError(t, err)
		defer result.Release()

		// Group sums: A: 10+30+50 = 90, B: 20+40 = 60, C: 60
		// With predicate SUM(value) > 70, only A (90) should remain
		assert.Equal(t, 1, result.NumRows())

		// Verify only group A remains
		categoryCol, exists := result.Column("category")
		require.True(t, exists)
		assert.Equal(t, "A", categoryCol.GetAsString(0))
	})

	t.Run("multiple aggregation functions in predicate", func(t *testing.T) {
		// Create test DataFrame
		groups := series.New("group", []string{"X", "Y", "X", "Y", "X"}, mem)
		sales := series.New("sales", []float64{100.5, 200.3, 150.2, 50.1, 80.7}, mem)

		df := New(groups, sales)
		defer df.Release()

		// HAVING AVG(sales) > 100 AND COUNT(*) >= 3
		havingPredicate := expr.Mean(expr.Col("sales")).Gt(expr.Lit(100.0)).And(
			expr.Count(expr.Col("sales")).Ge(expr.Lit(int64(3))),
		)

		// Apply through lazy evaluation
		lazy := df.Lazy().GroupBy("group").Having(havingPredicate)
		result, err := lazy.Collect(context.Background())
		require.NoError(t, err)
		defer result.Release()

		// Group X: avg=(100.5+150.2+80.7)/3=110.47, count=3 -> passes both conditions
		// Group Y: avg=(200.3+50.1)/2=125.2, count=2 -> fails count >= 3
		// Only group X should remain
		assert.Equal(t, 1, result.NumRows())

		groupCol, exists := result.Column("group")
		require.True(t, exists)
		assert.Equal(t, "X", groupCol.GetAsString(0))
	})

	t.Run("preserves group structure and order", func(t *testing.T) {
		// Create test DataFrame
		dept := series.New("dept", []string{"HR", "IT", "HR", "IT", "Sales"}, mem)
		salary := series.New("salary", []int64{50000, 80000, 60000, 90000, 70000}, mem)

		df := New(dept, salary)
		defer df.Release()

		// HAVING MAX(salary) >= 90000
		havingPredicate := expr.Max(expr.Col("salary")).Ge(expr.Lit(int64(90000)))

		lazy := df.Lazy().GroupBy("dept").Having(havingPredicate)
		result, err := lazy.Collect(context.Background())
		require.NoError(t, err)
		defer result.Release()

		// HR: max=60000, IT: max=90000, Sales: max=70000
		// Only IT (90000 >= 90000) should remain
		assert.Equal(t, 1, result.NumRows())

		deptCol, exists := result.Column("dept")
		require.True(t, exists)
		assert.Equal(t, "IT", deptCol.GetAsString(0))
	})
}

func TestHavingOperation_ErrorHandling(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("returns error for non-aggregation predicates", func(t *testing.T) {
		col1 := series.New("col1", []int64{1, 2, 3}, mem)
		df := New(col1)
		defer df.Release()

		// Non-aggregation predicate (should fail validation)
		havingPredicate := expr.Col("col1").Gt(expr.Lit(int64(1)))

		// Should fail during lazy evaluation when HAVING is applied without aggregation
		lazy := df.Lazy().GroupBy("col1").Having(havingPredicate)
		_, err := lazy.Collect(context.Background())

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "HAVING clause must contain aggregation functions")
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		group := series.New("group", []string{"A", "B", "A", "B"}, mem)
		value := series.New("value", []int64{1, 2, 3, 4}, mem)

		df := New(group, value)
		defer df.Release()

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		havingPredicate := expr.Sum(expr.Col("value")).Gt(expr.Lit(int64(3)))

		lazy := df.Lazy().GroupBy("group").Having(havingPredicate)
		_, err := lazy.Collect(ctx)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context canceled")
	})

	t.Run("handles invalid column references", func(t *testing.T) {
		group := series.New("group", []string{"A", "B"}, mem)
		value := series.New("value", []int64{1, 2}, mem)

		df := New(group, value)
		defer df.Release()

		// Reference non-existent column
		havingPredicate := expr.Sum(expr.Col("nonexistent")).Gt(expr.Lit(int64(10)))

		lazy := df.Lazy().GroupBy("group").Having(havingPredicate)
		_, err := lazy.Collect(context.Background())

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "sum_nonexistent")
	})
}

func TestHavingOperation_MemoryManagement(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("properly releases memory for filtered groups", func(t *testing.T) {
		// Create a larger dataset to test memory management
		categories := make([]string, 1000)
		values := make([]int64, 1000)
		for i := range 1000 {
			categories[i] = string(rune('A' + (i % 10))) // 10 groups
			values[i] = int64(i)
		}

		categorySeries := series.New("category", categories, mem)
		valueSeries := series.New("value", values, mem)

		df := New(categorySeries, valueSeries)
		defer df.Release()

		// Filter to keep only groups with sum > 20000
		havingPredicate := expr.Sum(expr.Col("value")).Gt(expr.Lit(int64(20000)))

		lazy := df.Lazy().GroupBy("category").Having(havingPredicate)
		result, err := lazy.Collect(context.Background())
		require.NoError(t, err)

		// Verify result is valid before release
		assert.Positive(t, result.NumRows())
		assert.Less(t, result.NumRows(), df.NumRows())

		// Ensure proper cleanup
		result.Release()
	})
}

func TestHavingOperation_ComplexScenarios(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("complex having with multiple conditions", func(t *testing.T) {
		store := series.New("store", []string{"S1", "S2", "S1", "S2", "S3", "S3"}, mem)
		sales := series.New("sales", []float64{100, 200, 150, 250, 300, 350}, mem)
		profit := series.New("profit", []float64{10, 25, 20, 30, 40, 45}, mem)

		df := New(store, sales, profit)
		defer df.Release()

		// HAVING SUM(sales) > 400 AND AVG(profit) > 35
		havingPredicate := expr.Sum(expr.Col("sales")).Gt(expr.Lit(400.0)).And(
			expr.Mean(expr.Col("profit")).Gt(expr.Lit(35.0)),
		)

		lazy := df.Lazy().GroupBy("store").Having(havingPredicate)
		result, err := lazy.Collect(context.Background())
		require.NoError(t, err)
		defer result.Release()

		// S1: sum=250, avg_profit=15 -> fails both conditions
		// S2: sum=450, avg_profit=27.5 -> passes sum but fails avg
		// S3: sum=650, avg_profit=42.5 -> passes both conditions
		// Only S3 should remain
		assert.Equal(t, 1, result.NumRows())

		storeCol, exists := result.Column("store")
		require.True(t, exists)
		assert.Equal(t, "S3", storeCol.GetAsString(0))
	})

	t.Run("having with all aggregation types", func(t *testing.T) {
		typeCol := series.New("type", []string{"A", "B", "A", "B", "A"}, mem)
		valueCol := series.New("value", []float64{10.5, 20.3, 15.2, 25.1, 12.8}, mem)

		df := New(typeCol, valueCol)
		defer df.Release()

		// Test COUNT aggregation: A has 3 values, B has 2 values
		// HAVING COUNT(value) > 2 should keep only A
		havingPredicate := expr.Count(expr.Col("value")).Gt(expr.Lit(int64(2)))

		lazy := df.Lazy().GroupBy("type").Having(havingPredicate)
		result, err := lazy.Collect(context.Background())
		require.NoError(t, err)
		defer result.Release()

		// Only type A should remain (count=3 > 2)
		assert.Equal(t, 1, result.NumRows())

		typeResult, exists := result.Column("type")
		require.True(t, exists)
		assert.Equal(t, "A", typeResult.GetAsString(0))
	})
}

func TestHavingOperation_Integration(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("having in lazy evaluation chain", func(t *testing.T) {
		product := series.New("product", []string{"A", "B", "A", "B", "C", "C"}, mem)
		quantity := series.New("quantity", []int64{10, 5, 15, 8, 20, 25}, mem)
		price := series.New("price", []float64{100, 200, 150, 250, 300, 350}, mem)

		df := New(product, quantity, price)
		defer df.Release()

		// Build operation chain: GroupBy -> Having
		havingPredicate := expr.Sum(expr.Col("quantity")).Gt(expr.Lit(int64(20)))

		lazy := df.Lazy().GroupBy("product").Having(havingPredicate)
		result, err := lazy.Collect(context.Background())
		require.NoError(t, err)
		defer result.Release()

		// Products: A (10+15=25), B (5+8=13), C (20+25=45)
		// Products A (25) and C (45) should remain (both > 20)
		assert.Equal(t, 2, result.NumRows())

		productCol, exists := result.Column("product")
		require.True(t, exists)
		products := make([]string, 0)
		for i := 0; i < productCol.Len(); i++ {
			products = append(products, productCol.GetAsString(i))
		}

		assert.Contains(t, products, "A")
		assert.Contains(t, products, "C")
		assert.NotContains(t, products, "B")
	})

	t.Run("having with subsequent operations", func(t *testing.T) {
		region := series.New("region", []string{"East", "West", "East", "West", "North"}, mem)
		sales := series.New("sales", []float64{1000, 2000, 1500, 2500, 3000}, mem)

		df := New(region, sales)
		defer df.Release()

		// Apply HAVING SUM(sales) > 3000 and then aggregate
		havingPredicate := expr.Sum(expr.Col("sales")).Gt(expr.Lit(3000.0))

		// This would be the expected usage pattern for HAVING with aggregation
		lazy := df.Lazy().GroupBy("region").Having(havingPredicate)
		result, err := lazy.Collect(context.Background())
		require.NoError(t, err)
		defer result.Release()

		// East: 1000+1500=2500, West: 2000+2500=4500, North: 3000
		// Only West (4500) should remain (> 3000)
		assert.Equal(t, 1, result.NumRows())

		regionCol, exists := result.Column("region")
		require.True(t, exists)
		assert.Equal(t, "West", regionCol.GetAsString(0))
	})
}

// Helper functions removed - using testify's Contains instead

func TestHavingOperation_ValidationRules(t *testing.T) {
	t.Run("validates aggregation expressions in predicate", func(t *testing.T) {
		// This is a placeholder test that defines the expected validation behavior
		// The actual validation logic will be implemented in the HavingOperation

		// Valid: contains aggregation
		valid := expr.Sum(expr.Col("value")).Gt(expr.Lit(100))

		// Invalid: no aggregation
		invalid := expr.Col("value").Gt(expr.Lit(100))

		// The HavingOperation should validate that aggregation predicates are present
		// and reject non-aggregation predicates
		assert.NotNil(t, valid)   // Valid predicate structure
		assert.NotNil(t, invalid) // Invalid predicate structure that should be rejected
	})
}

func TestHavingOperation_EdgeCases(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("empty groups after filtering", func(t *testing.T) {
		group := series.New("group", []string{"A", "B", "C"}, mem)
		value := series.New("value", []int64{1, 2, 3}, mem)

		df := New(group, value)
		defer df.Release()

		// Filter that removes all groups (all sums are small)
		havingPredicate := expr.Sum(expr.Col("value")).Gt(expr.Lit(int64(1000)))

		lazy := df.Lazy().GroupBy("group").Having(havingPredicate)
		result, err := lazy.Collect(context.Background())
		require.NoError(t, err)
		defer result.Release()

		// Should return empty DataFrame
		assert.Equal(t, 0, result.NumRows())
	})

	t.Run("single group dataset", func(t *testing.T) {
		group := series.New("group", []string{"A", "A", "A"}, mem)
		value := series.New("value", []int64{10, 20, 30}, mem)

		df := New(group, value)
		defer df.Release()

		havingPredicate := expr.Sum(expr.Col("value")).Eq(expr.Lit(int64(60)))

		lazy := df.Lazy().GroupBy("group").Having(havingPredicate)
		result, err := lazy.Collect(context.Background())
		require.NoError(t, err)
		defer result.Release()

		// Single group should remain (sum=60)
		assert.Equal(t, 1, result.NumRows())

		groupCol, exists := result.Column("group")
		require.True(t, exists)
		assert.Equal(t, "A", groupCol.GetAsString(0))
	})

	t.Run("null values in aggregation", func(t *testing.T) {
		// This test defines expected behavior with null values
		// The actual implementation will need to handle null values properly in aggregations

		group := series.New("group", []string{"A", "A", "B", "B", "B"}, mem)
		// For now, we'll use regular values; null handling will be implemented later
		value := series.New("value", []float64{10.0, 0, 20.0, 0, 30.0}, mem) // 0 represents conceptual nulls

		df := New(group, value)
		defer df.Release()

		// HAVING COUNT(value) > 1 (should count non-null values)
		havingPredicate := expr.Count(expr.Col("value")).Gt(expr.Lit(int64(1)))

		lazy := df.Lazy().GroupBy("group").Having(havingPredicate)
		result, err := lazy.Collect(context.Background())
		require.NoError(t, err)
		defer result.Release()

		// Both groups should remain as they each have counts > 1
		assert.Equal(t, 2, result.NumRows())
	})
}
