//nolint:testpackage // requires internal access to unexported types and functions
package dataframe

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
)

// TestHavingNestedAggregations tests nested aggregation functions in HAVING clauses.
func TestHavingNestedAggregations(t *testing.T) {
	t.Skip("Nested aggregations require aggregation system refactoring - see issue #134")
	mem := memory.NewGoAllocator()

	t.Run("Sum with If condition", func(t *testing.T) {
		// Test: Sum only bonus amounts where type = "bonus"
		departments := series.New("department", []string{"Sales", "Sales", "Sales", "Eng", "Eng", "Eng"}, mem)
		types := series.New("type", []string{"salary", "bonus", "bonus", "salary", "bonus", "salary"}, mem)
		amounts := series.New("amount", []float64{50000, 5000, 3000, 60000, 8000, 55000}, mem)

		df := New(departments, types, amounts)
		defer df.Release()

		// HAVING SUM(IF(type = 'bonus', amount, 0)) > 6000
		// Sales: 5000 + 3000 = 8000 > 6000 ✓
		// Eng: 8000 > 6000 ✓
		result, err := df.Lazy().
			GroupBy("department").
			Having(expr.Sum(expr.If(
				expr.Col("type").Eq(expr.Lit("bonus")),
				expr.Col("amount"),
				expr.Lit(0.0),
			)).Gt(expr.Lit(6000.0))).
			Collect()

		require.NoError(t, err)
		defer result.Release()
		assert.Equal(t, 2, result.Len(), "Both departments should have bonus sum > 6000")
	})

	t.Run("Count with Case expression", func(t *testing.T) {
		// Test: Count active employees using CASE
		teams := series.New("team", []string{"Alpha", "Alpha", "Alpha", "Beta", "Beta", "Beta"}, mem)
		statuses := series.New("status", []string{"active", "inactive", "active", "active", "active", "inactive"}, mem)

		df := New(teams, statuses)
		defer df.Release()

		// HAVING COUNT(CASE WHEN status = 'active' THEN 1 ELSE 0 END) > 1
		// Alpha: 2 active > 1 ✓
		// Beta: 2 active > 1 ✓
		result, err := df.Lazy().
			GroupBy("team").
			Having(expr.Count(expr.Case().
				When(expr.Col("status").Eq(expr.Lit("active")), expr.Lit(1)).
				Else(expr.Lit(0)),
			).Gt(expr.Lit(int64(1)))).
			Collect()

		require.NoError(t, err)
		defer result.Release()
		assert.Equal(t, 2, result.Len(), "Both teams should have more than 1 active member")
	})

	t.Run("Count with Concat and Distinct", func(t *testing.T) {
		// Test: Count distinct full names
		departments := series.New("department", []string{"HR", "HR", "HR", "IT", "IT", "IT"}, mem)
		firstNames := series.New("first_name", []string{"John", "Jane", "John", "Bob", "Alice", "Bob"}, mem)
		lastNames := series.New("last_name", []string{"Doe", "Smith", "Smith", "Jones", "Brown", "Jones"}, mem)

		df := New(departments, firstNames, lastNames)
		defer df.Release()

		// HAVING COUNT(CONCAT(first_name, ' ', last_name)) >= 2
		// Note: Without DISTINCT, this counts all concatenated names (not unique)
		// HR: 3 names >= 2 ✓
		// IT: 2 names >= 2 ✓
		result, err := df.Lazy().
			GroupBy("department").
			Having(expr.Count(expr.Concat(
				expr.Col("first_name"),
				expr.Lit(" "),
				expr.Col("last_name"),
			)).Ge(expr.Lit(int64(2)))).
			Collect()

		require.NoError(t, err)
		defer result.Release()
		assert.Equal(t, 2, result.Len(), "Both departments should have at least 2 distinct full names")
	})

	t.Run("Complex nested aggregations", func(t *testing.T) {
		// Test: Complex business logic with multiple nested conditions
		stores := series.New("store", []string{"A", "A", "A", "B", "B", "B"}, mem)
		productTypes := series.New("product_type", []string{
			"electronics", "clothing", "electronics", "electronics", "food", "clothing",
		}, mem)
		revenues := series.New("revenue", []float64{1000, 500, 1500, 2000, 300, 800}, mem)
		costs := series.New("cost", []float64{800, 400, 1200, 1600, 250, 600}, mem)

		df := New(stores, productTypes, revenues, costs)
		defer df.Release()

		// HAVING SUM(IF(product_type = 'electronics', revenue - cost, 0)) > 500
		// Store A: (1000-800) + (1500-1200) = 200 + 300 = 500, not > 500
		// Store B: (2000-1600) = 400, not > 500
		result, err := df.Lazy().
			GroupBy("store").
			Having(expr.Sum(expr.If(
				expr.Col("product_type").Eq(expr.Lit("electronics")),
				expr.Col("revenue").Sub(expr.Col("cost")),
				expr.Lit(0.0),
			)).Gt(expr.Lit(500.0))).
			Collect()

		require.NoError(t, err)
		defer result.Release()
		assert.Equal(t, 0, result.Len(), "No stores should have electronics profit > 500")
	})

	t.Run("Mean with conditional expression", func(t *testing.T) {
		// Test: Average of only high-value transactions
		branches := series.New("branch", []string{"North", "North", "North", "South", "South", "South"}, mem)
		transactionValues := series.New("value", []float64{100, 5000, 200, 300, 6000, 400}, mem)

		df := New(branches, transactionValues)
		defer df.Release()

		// HAVING AVG(IF(value > 1000, value, NULL)) > 5500
		// North: AVG(5000) = 5000, not > 5500
		// South: AVG(6000) = 6000 > 5500 ✓
		result, err := df.Lazy().
			GroupBy("branch").
			Having(expr.Mean(expr.If(
				expr.Col("value").Gt(expr.Lit(1000.0)),
				expr.Col("value"),
				expr.Lit(nil), // NULL values should be ignored in AVG
			)).Gt(expr.Lit(5500.0))).
			Collect()

		require.NoError(t, err)
		defer result.Release()
		assert.Equal(t, 1, result.Len(), "Only South branch should have high-value avg > 5500")
	})
}

// TestHavingNestedAggregationsEdgeCases tests edge cases for nested aggregations.
func TestHavingNestedAggregationsEdgeCases(t *testing.T) {
	t.Skip("Nested aggregations require aggregation system refactoring - see issue #134")
	mem := memory.NewGoAllocator()

	t.Run("All conditions false in If expression", func(t *testing.T) {
		groups := series.New("group", []string{"X", "X", "Y", "Y"}, mem)
		values := series.New("value", []string{"a", "b", "c", "d"}, mem)

		df := New(groups, values)
		defer df.Release()

		// SUM(IF(value = 'z', 1, 0)) > 0 - no 'z' values
		result, err := df.Lazy().
			GroupBy("group").
			Having(expr.Sum(expr.If(
				expr.Col("value").Eq(expr.Lit("z")),
				expr.Lit(1),
				expr.Lit(0),
			)).Gt(expr.Lit(int64(0)))).
			Collect()

		require.NoError(t, err)
		defer result.Release()
		assert.Equal(t, 0, result.Len(), "No groups should match when all conditions are false")
	})

	t.Run("Empty groups after nested filtering", func(t *testing.T) {
		categories := series.New("category", []string{"A", "B"}, mem)
		values := series.New("value", []float64{10, 20}, mem)

		df := New(categories, values)
		defer df.Release()

		// COUNT(CASE WHEN value > 100 THEN 1 END) > 0 - no values > 100
		result, err := df.Lazy().
			GroupBy("category").
			Having(expr.Count(expr.Case().
				When(expr.Col("value").Gt(expr.Lit(100.0)), expr.Lit(1)).
				Else(expr.Lit(nil)),
			).Gt(expr.Lit(int64(0)))).
			Collect()

		require.NoError(t, err)
		defer result.Release()
		assert.Equal(t, 0, result.Len(), "No groups should have values > 100")
	})

	t.Run("Type coercion in nested expressions", func(t *testing.T) {
		groups := series.New("group", []string{"A", "A", "B", "B"}, mem)
		intValues := series.New("int_val", []int64{10, 20, 30, 40}, mem)
		floatThresholds := series.New("threshold", []float64{15.5, 15.5, 35.5, 35.5}, mem)

		df := New(groups, intValues, floatThresholds)
		defer df.Release()

		// SUM(IF(int_val > threshold, int_val, 0)) > 50
		// Group A: 20 > 50? No
		// Group B: 40 > 50? No
		result, err := df.Lazy().
			GroupBy("group").
			Having(expr.Sum(expr.If(
				expr.Col("int_val").Gt(expr.Col("threshold")),
				expr.Col("int_val"),
				expr.Lit(0),
			)).Gt(expr.Lit(50.0))).
			Collect()

		require.NoError(t, err)
		defer result.Release()
		assert.Equal(t, 0, result.Len(), "No groups should have conditional sum > 50")
	})
}
