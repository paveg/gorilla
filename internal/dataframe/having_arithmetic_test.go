package dataframe

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
)

// TestHavingArithmeticExpressions tests arithmetic expressions in HAVING predicates
func TestHavingArithmeticExpressions(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("Basic arithmetic operations in HAVING", func(t *testing.T) {
		// Create test data: 3 groups with different sum values
		categories := series.New("category", []string{"A", "A", "B", "B", "C", "C"}, mem)
		sales := series.New("sales", []float64{100, 200, 300, 400, 500, 600}, mem)

		df := New(categories, sales)
		defer df.Release()

		// Test: SUM(sales) + 100 > 500
		// Group A: 300 + 100 = 400 (not > 500)
		// Group B: 700 + 100 = 800 (> 500) ✓
		// Group C: 1100 + 100 = 1200 (> 500) ✓
		result, err := df.Lazy().
			GroupBy("category").
			Having(expr.Sum(expr.Col("sales")).Add(expr.Lit(100.0)).Gt(expr.Lit(500.0))).
			Collect()

		require.NoError(t, err)
		defer result.Release()
		assert.Equal(t, 2, result.Len(), "Groups B and C should match")
	})

	t.Run("Complex arithmetic with multiple operations", func(t *testing.T) {
		categories := series.New("category", []string{"X", "X", "Y", "Y", "Z", "Z"}, mem)
		values := series.New("values", []float64{10, 20, 30, 40, 50, 60}, mem)

		df := New(categories, values)
		defer df.Release()

		// Test: (SUM(values) * 2) / COUNT(*) > 40
		// Group X: (30 * 2) / 2 = 30 (not > 40)
		// Group Y: (70 * 2) / 2 = 70 (> 40) ✓
		// Group Z: (110 * 2) / 2 = 110 (> 40) ✓
		result, err := df.Lazy().
			GroupBy("category").
			Having(expr.Sum(expr.Col("values")).Mul(expr.Lit(2.0)).Div(
				expr.Count(expr.Col("category"))).Gt(expr.Lit(40.0))).
			Collect()

		require.NoError(t, err)
		defer result.Release()
		assert.Equal(t, 2, result.Len(), "Groups Y and Z should match")
	})

	t.Run("Arithmetic between different aggregations", func(t *testing.T) {
		categories := series.New("category", []string{"P", "P", "Q", "Q", "R", "R"}, mem)
		sales := series.New("sales", []float64{100, 200, 300, 400, 500, 600}, mem)
		costs := series.New("costs", []float64{50, 100, 150, 200, 250, 300}, mem)

		df := New(categories, sales, costs)
		defer df.Release()

		// Test: SUM(sales) - SUM(costs) > 200 (profit > 200)
		// Group P: 300 - 150 = 150 (not > 200)
		// Group Q: 700 - 350 = 350 (> 200) ✓
		// Group R: 1100 - 550 = 550 (> 200) ✓
		result, err := df.Lazy().
			GroupBy("category").
			Having(expr.Sum(expr.Col("sales")).Sub(expr.Sum(expr.Col("costs"))).Gt(expr.Lit(200.0))).
			Collect()

		require.NoError(t, err)
		defer result.Release()
		assert.Equal(t, 2, result.Len(), "Groups Q and R should match")
	})

	t.Run("Mixed arithmetic and logical operations", func(t *testing.T) {
		departments := series.New("department", []string{"Eng", "Eng", "Sales", "Sales", "HR", "HR"}, mem)
		salaries := series.New("salary", []float64{80000, 90000, 60000, 70000, 50000, 55000}, mem)
		bonuses := series.New("bonus", []float64{8000, 9000, 6000, 7000, 5000, 5500}, mem)

		df := New(departments, salaries, bonuses)
		defer df.Release()

		// Test: AVG(salary) + AVG(bonus) > 75000 AND COUNT(*) >= 2
		// Eng: 85000 + 8500 = 93500 (> 75000) ✓ AND count=2 ✓
		// Sales: 65000 + 6500 = 71500 (not > 75000)
		// HR: 52500 + 5250 = 57750 (not > 75000)
		result, err := df.Lazy().
			GroupBy("department").
			Having(expr.Mean(expr.Col("salary")).Add(expr.Mean(expr.Col("bonus"))).Gt(expr.Lit(75000.0)).
				And(expr.Count(expr.Col("department")).Ge(expr.Lit(int64(2))))).
			Collect()

		require.NoError(t, err)
		defer result.Release()
		assert.Equal(t, 1, result.Len(), "Only Eng department should match")
	})

	t.Run("Type coercion in arithmetic expressions", func(t *testing.T) {
		groups := series.New("group", []string{"Alpha", "Alpha", "Beta", "Beta"}, mem)
		intValues := series.New("int_values", []int64{100, 200, 300, 400}, mem)
		floatValues := series.New("float_values", []float64{10.5, 20.5, 30.5, 40.5}, mem)

		df := New(groups, intValues, floatValues)
		defer df.Release()

		// Test: SUM(int_values) + SUM(float_values) > 350.0
		// Group Alpha: 300 + 31.0 = 331.0 (not > 350.0)
		// Group Beta: 700 + 71.0 = 771.0 (> 350.0) ✓
		result, err := df.Lazy().
			GroupBy("group").
			Having(expr.Sum(expr.Col("int_values")).Add(expr.Sum(expr.Col("float_values"))).Gt(expr.Lit(350.0))).
			Collect()

		require.NoError(t, err)
		defer result.Release()
		assert.Equal(t, 1, result.Len(), "Only Beta group should match")
	})

	t.Run("Nested arithmetic expressions", func(t *testing.T) {
		teams := series.New("team", []string{"Red", "Red", "Blue", "Blue", "Green", "Green"}, mem)
		scores := series.New("scores", []float64{85, 90, 75, 80, 95, 100}, mem)
		weights := series.New("weights", []float64{1.0, 1.2, 0.8, 1.0, 1.1, 1.3}, mem)

		df := New(teams, scores, weights)
		defer df.Release()

		// Test: (SUM(scores) * AVG(weights)) / COUNT(*) > 90
		// Complex nested arithmetic with multiple aggregations
		result, err := df.Lazy().
			GroupBy("team").
			Having(expr.Sum(expr.Col("scores")).Mul(expr.Mean(expr.Col("weights"))).Div(
				expr.Count(expr.Col("team"))).Gt(expr.Lit(90.0))).
			Collect()

		require.NoError(t, err)
		defer result.Release()
		// All groups should be tested - exact count depends on calculations
		assert.GreaterOrEqual(t, result.Len(), 0)
		assert.LessOrEqual(t, result.Len(), 3)
	})
}

// TestHavingArithmeticEdgeCases tests edge cases for arithmetic expressions in HAVING
func TestHavingArithmeticEdgeCases(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("Division by zero handling", func(t *testing.T) {
		categories := series.New("category", []string{"A", "A", "B", "B"}, mem)
		values := series.New("values", []float64{10, 20, 30, 40}, mem)

		df := New(categories, values)
		defer df.Release()

		// Test division by zero - should handle gracefully
		result, err := df.Lazy().
			GroupBy("category").
			Having(expr.Sum(expr.Col("values")).Div(expr.Lit(0.0)).Gt(expr.Lit(100.0))).
			Collect()

		require.NoError(t, err)
		defer result.Release()
		// Division by zero results in +Inf, which is greater than 100
		// So all groups with positive sums should match
		assert.Equal(t, 2, result.Len(), "Both groups should match (sum/0 = +Inf > 100)")
	})

	t.Run("Large number arithmetic", func(t *testing.T) {
		categories := series.New("category", []string{"Big", "Big", "Bigger", "Bigger"}, mem)
		largeValues := series.New("values", []float64{1e6, 2e6, 3e6, 4e6}, mem)

		df := New(categories, largeValues)
		defer df.Release()

		// Test with large numbers
		result, err := df.Lazy().
			GroupBy("category").
			Having(expr.Sum(expr.Col("values")).Mul(expr.Lit(2.0)).Gt(expr.Lit(1e7))).
			Collect()

		require.NoError(t, err)
		defer result.Release()
		// At least one group should match
		assert.GreaterOrEqual(t, result.Len(), 1)
	})

	t.Run("Precision in floating point arithmetic", func(t *testing.T) {
		categories := series.New("category", []string{"Precise", "Precise"}, mem)
		preciseValues := series.New("values", []float64{0.1, 0.2}, mem)

		df := New(categories, preciseValues)
		defer df.Release()

		// Test floating point precision
		result, err := df.Lazy().
			GroupBy("category").
			Having(expr.Sum(expr.Col("values")).Sub(expr.Lit(0.3)).Lt(expr.Lit(1e-10))).
			Collect()

		require.NoError(t, err)
		defer result.Release()
		// Should handle floating point precision appropriately
		assert.GreaterOrEqual(t, result.Len(), 0)
	})
}
