package gorilla

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDirectExpressionAccess tests accessing expression types directly
func TestDirectExpressionAccess(t *testing.T) {
	t.Run("direct column expression access", func(t *testing.T) {
		// Should be able to access ColumnExpr directly
		col := Col("age")

		// Test that we can access the underlying expression
		assert.Equal(t, "age", col.Name())
		assert.Equal(t, expr.ExprColumn, col.Type())
	})

	t.Run("direct literal expression access", func(t *testing.T) {
		// Should be able to access LiteralExpr directly
		lit := Lit(int64(30))

		// Test that we can access the underlying expression
		assert.Equal(t, int64(30), lit.Value())
		assert.Equal(t, expr.ExprLiteral, lit.Type())
	})

	t.Run("expression chaining without wrapper overhead", func(t *testing.T) {
		// Test that expression chaining works without intermediate wrappers
		col := Col("salary")
		expr1 := col.Mul(Lit(1.1))

		// Should be able to chain without type assertions
		assert.Equal(t, expr.ExprBinary, expr1.Type())
		assert.Equal(t, expr.OpMul, expr1.Op())

		// Test that we can use the result in comparisons (only on ColumnExpr)
		expr2 := col.Gt(Lit(50000))
		assert.Equal(t, expr.ExprBinary, expr2.Type())
		assert.Equal(t, expr.OpGt, expr2.Op())
	})
}

// TestOptimizedExpressionPerformance tests performance improvements
func TestOptimizedExpressionPerformance(t *testing.T) {
	t.Run("no type assertions in expression building", func(t *testing.T) {
		// This test verifies that expression building doesn't require type assertions
		col := Col("price")

		// Build expression without type assertions
		mulExpr := col.Mul(Lit(1.1))

		// Should be a BinaryExpr with proper structure
		assert.Equal(t, expr.ExprBinary, mulExpr.Type())
		assert.Equal(t, expr.OpMul, mulExpr.Op())

		// Test comparison separately (since BinaryExpr doesn't have comparison methods)
		gtExpr := col.Gt(Lit(100.0))
		assert.Equal(t, expr.ExprBinary, gtExpr.Type())
		assert.Equal(t, expr.OpGt, gtExpr.Op())
	})

	t.Run("aggregation expressions without wrapper conversion", func(t *testing.T) {
		// Test that aggregation expressions work without conversion overhead
		col := Col("values")
		sumExpr := Sum(col)

		// Should be direct access to AggregationExpr
		assert.Equal(t, expr.ExprAggregation, sumExpr.Type())
		assert.Equal(t, expr.AggSum, sumExpr.AggType())
	})
}

// TestBackwardCompatibility tests that the optimized API is backward compatible
func TestBackwardCompatibility(t *testing.T) {
	t.Run("existing DataFrame operations still work", func(t *testing.T) {
		mem := memory.NewGoAllocator()

		// Create test data
		ages := []int64{25, 30, 35, 40}
		names := []string{"Alice", "Bob", "Charlie", "Diana"}

		ageSeries := series.New("age", ages, mem)
		nameSeries := series.New("name", names, mem)
		defer ageSeries.Release()
		defer nameSeries.Release()

		df := NewDataFrame(ageSeries, nameSeries)
		defer df.Release()

		// Test that lazy operations still work with optimized expressions
		result, err := df.Lazy().
			Filter(Col("age").Gt(Lit(int64(30)))).
			Select("name").
			Collect()

		require.NoError(t, err)
		defer result.Release()

		// Debug: Print result length and content
		t.Logf("Result length: %d", result.Len())
		t.Logf("Result columns: %v", result.Columns())
		if nameSeries, ok := result.Column("name"); ok {
			t.Logf("Name values: %v", nameSeries)
		}

		// Debug: Check original dataframe
		t.Logf("Original dataframe length: %d", df.Len())
		if ageCol, ok := df.Column("age"); ok {
			t.Logf("Age column: %v", ageCol)
		}

		assert.Equal(t, 2, result.Len())
		assert.Equal(t, []string{"name"}, result.Columns())
	})

	t.Run("aggregation operations still work", func(t *testing.T) {
		mem := memory.NewGoAllocator()

		// Create test data
		values := []int64{10, 20, 30, 40, 50}
		categories := []string{"A", "A", "B", "B", "C"}

		valueSeries := series.New("value", values, mem)
		categorySeries := series.New("category", categories, mem)
		defer valueSeries.Release()
		defer categorySeries.Release()

		df := NewDataFrame(valueSeries, categorySeries)
		defer df.Release()

		// Test that aggregation works with optimized expressions
		result, err := df.Lazy().
			GroupBy("category").
			Agg(Sum(Col("value")).As("total")).
			Collect()

		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 3, result.Len())
		assert.Equal(t, []string{"category", "total"}, result.Columns())
	})
}

// TestTypeAliasCompatibility tests that type aliases work where possible
func TestTypeAliasCompatibility(t *testing.T) {
	t.Run("expression interfaces are compatible", func(t *testing.T) {
		// Test that we can work with expr.Expr interface directly
		var expressions []expr.Expr

		col := Col("test")
		lit := Lit(42)

		expressions = append(expressions, col, lit)

		// Should be able to work with them as expr.Expr
		for _, e := range expressions {
			assert.NotNil(t, e)
			assert.NotEmpty(t, e.String())
		}
	})
}

// BenchmarkExpressionPerformance benchmarks the performance improvements
func BenchmarkExpressionPerformance(b *testing.B) {
	b.Run("Direct_Expression_Creation", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			col := Col("test")
			lit := Lit(int64(i))
			_ = col.Gt(lit)
		}
	})

	b.Run("Complex_Expression_Chain", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			col := Col("salary")
			_ = col.Mul(Lit(1.1))
			expr2 := col.Gt(Lit(50000))
			activeExpr := Col("active").Eq(Lit(true))
			_ = expr2.And(activeExpr)
		}
	})

	b.Run("Aggregation_Expression_Creation", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			col := Col("values")
			sum := Sum(col)
			_ = sum.As("total")
		}
	})
}
