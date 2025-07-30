package expr

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewAggregationContext(t *testing.T) {
	t.Run("creates empty context", func(t *testing.T) {
		ctx := NewAggregationContext()

		assert.NotNil(t, ctx)
		assert.Empty(t, ctx.columnMappings)
		assert.Empty(t, ctx.reverseMapping)
		assert.Equal(t, "AggregationContext{empty}", ctx.String())
	})
}

func TestAggregationContext_AddMapping(t *testing.T) {
	t.Run("adds single mapping", func(t *testing.T) {
		ctx := NewAggregationContext()

		ctx.AddMapping("SUM(sales)", "sum_sales")

		columnName, exists := ctx.GetColumnName("SUM(sales)")
		assert.True(t, exists)
		assert.Equal(t, "sum_sales", columnName)

		exprStr, exists := ctx.GetExpression("sum_sales")
		assert.True(t, exists)
		assert.Equal(t, "SUM(sales)", exprStr)
	})

	t.Run("adds multiple mappings", func(t *testing.T) {
		ctx := NewAggregationContext()

		ctx.AddMapping("SUM(sales)", "sum_sales")
		ctx.AddMapping("COUNT(id)", "count_id")
		ctx.AddMapping("AVG(price)", "avg_price")

		// Verify all mappings exist
		mappings := ctx.AllMappings()
		expected := map[string]string{
			"SUM(sales)": "sum_sales",
			"COUNT(id)":  "count_id",
			"AVG(price)": "avg_price",
		}
		assert.Equal(t, expected, mappings)
	})

	t.Run("overwrites existing mapping", func(t *testing.T) {
		ctx := NewAggregationContext()

		ctx.AddMapping("SUM(sales)", "sum_sales")
		ctx.AddMapping("SUM(sales)", "total_sales") // Overwrite

		columnName, exists := ctx.GetColumnName("SUM(sales)")
		assert.True(t, exists)
		assert.Equal(t, "total_sales", columnName)

		// Verify reverse mapping is also updated
		exprStr, exists := ctx.GetExpression("total_sales")
		assert.True(t, exists)
		assert.Equal(t, "SUM(sales)", exprStr)

		// Old reverse mapping should not exist
		_, exists = ctx.GetExpression("sum_sales")
		assert.False(t, exists)
	})
}

func TestAggregationContext_GetColumnName(t *testing.T) {
	t.Run("returns existing mapping", func(t *testing.T) {
		ctx := NewAggregationContext()
		ctx.AddMapping("COUNT(*)", "count_all")

		columnName, exists := ctx.GetColumnName("COUNT(*)")
		assert.True(t, exists)
		assert.Equal(t, "count_all", columnName)
	})

	t.Run("returns false for non-existent mapping", func(t *testing.T) {
		ctx := NewAggregationContext()

		columnName, exists := ctx.GetColumnName("SUM(nonexistent)")
		assert.False(t, exists)
		assert.Empty(t, columnName)
	})
}

func TestAggregationContext_GetExpression(t *testing.T) {
	t.Run("returns existing reverse mapping", func(t *testing.T) {
		ctx := NewAggregationContext()
		ctx.AddMapping("MIN(value)", "min_value")

		exprStr, exists := ctx.GetExpression("min_value")
		assert.True(t, exists)
		assert.Equal(t, "MIN(value)", exprStr)
	})

	t.Run("returns false for non-existent reverse mapping", func(t *testing.T) {
		ctx := NewAggregationContext()

		exprStr, exists := ctx.GetExpression("nonexistent_column")
		assert.False(t, exists)
		assert.Empty(t, exprStr)
	})
}

func TestAggregationContext_HasMapping(t *testing.T) {
	ctx := NewAggregationContext()
	ctx.AddMapping("MAX(score)", "max_score")

	t.Run("returns true for existing mapping", func(t *testing.T) {
		assert.True(t, ctx.HasMapping("MAX(score)"))
	})

	t.Run("returns false for non-existent mapping", func(t *testing.T) {
		assert.False(t, ctx.HasMapping("MIN(score)"))
	})
}

func TestAggregationContext_AllMappings(t *testing.T) {
	t.Run("returns empty map for empty context", func(t *testing.T) {
		ctx := NewAggregationContext()

		mappings := ctx.AllMappings()
		assert.Empty(t, mappings)
	})

	t.Run("returns copy of all mappings", func(t *testing.T) {
		ctx := NewAggregationContext()
		ctx.AddMapping("SUM(a)", "sum_a")
		ctx.AddMapping("COUNT(b)", "count_b")

		mappings := ctx.AllMappings()
		expected := map[string]string{
			"SUM(a)":   "sum_a",
			"COUNT(b)": "count_b",
		}
		assert.Equal(t, expected, mappings)

		// Verify it's a copy (modifying returned map doesn't affect context)
		mappings["SUM(c)"] = "sum_c"
		assert.False(t, ctx.HasMapping("SUM(c)"))
	})
}

func TestAggregationContext_Clear(t *testing.T) {
	t.Run("clears all mappings", func(t *testing.T) {
		ctx := NewAggregationContext()
		ctx.AddMapping("SUM(x)", "sum_x")
		ctx.AddMapping("COUNT(y)", "count_y")

		// Verify mappings exist
		assert.True(t, ctx.HasMapping("SUM(x)"))
		assert.True(t, ctx.HasMapping("COUNT(y)"))

		// Clear all mappings
		ctx.Clear()

		// Verify all mappings are gone
		assert.False(t, ctx.HasMapping("SUM(x)"))
		assert.False(t, ctx.HasMapping("COUNT(y)"))
		assert.Empty(t, ctx.AllMappings())
		assert.Equal(t, "AggregationContext{empty}", ctx.String())
	})
}

func TestAggregationContext_String(t *testing.T) {
	t.Run("empty context", func(t *testing.T) {
		ctx := NewAggregationContext()
		assert.Equal(t, "AggregationContext{empty}", ctx.String())
	})

	t.Run("single mapping", func(t *testing.T) {
		ctx := NewAggregationContext()
		ctx.AddMapping("SUM(sales)", "sum_sales")

		result := ctx.String()
		assert.Contains(t, result, "AggregationContext{")
		assert.Contains(t, result, "SUM(sales)->sum_sales")
		assert.Contains(t, result, "}")
	})

	t.Run("multiple mappings", func(t *testing.T) {
		ctx := NewAggregationContext()
		ctx.AddMapping("SUM(sales)", "sum_sales")
		ctx.AddMapping("COUNT(id)", "count_id")

		result := ctx.String()
		assert.Contains(t, result, "AggregationContext{")
		assert.Contains(t, result, "SUM(sales)->sum_sales")
		assert.Contains(t, result, "COUNT(id)->count_id")
		assert.Contains(t, result, "}")
	})
}

func TestExpressionToColumnName(t *testing.T) {
	t.Run("aggregation expression", func(t *testing.T) {
		expr := Sum(Col("sales"))
		columnName := ExpressionToColumnName(expr)
		assert.Equal(t, "sum(col(sales))", columnName)
	})

	t.Run("column expression", func(t *testing.T) {
		expr := Col("name")
		columnName := ExpressionToColumnName(expr)
		assert.Equal(t, "col(name)", columnName)
	})

	t.Run("literal expression", func(t *testing.T) {
		expr := Lit(42)
		columnName := ExpressionToColumnName(expr)
		assert.Equal(t, "lit(42)", columnName)
	})

	t.Run("binary expression", func(t *testing.T) {
		expr := Col("a").Gt(Lit(10))
		columnName := ExpressionToColumnName(expr)
		assert.Equal(t, "(col(a) > lit(10))", columnName)
	})

	t.Run("function expression", func(t *testing.T) {
		expr := NewFunction("UPPER", Col("name"))
		columnName := ExpressionToColumnName(expr)
		assert.Equal(t, "UPPER(col(name))", columnName)
	})
}

func TestBuildContextFromAggregations(t *testing.T) {
	t.Run("empty aggregations", func(t *testing.T) {
		ctx := BuildContextFromAggregations([]*AggregationExpr{})

		assert.NotNil(t, ctx)
		assert.Empty(t, ctx.AllMappings())
	})

	t.Run("single aggregation", func(t *testing.T) {
		agg := Sum(Col("sales"))
		ctx := BuildContextFromAggregations([]*AggregationExpr{agg})

		exprStr := agg.String()
		columnName := ExpressionToColumnName(agg)

		assert.True(t, ctx.HasMapping(exprStr))
		mappedColumn, exists := ctx.GetColumnName(exprStr)
		assert.True(t, exists)
		assert.Equal(t, columnName, mappedColumn)
	})

	t.Run("multiple aggregations", func(t *testing.T) {
		agg1 := Sum(Col("sales"))
		agg2 := Count(Col("id"))
		agg3 := Mean(Col("price"))

		aggregations := []*AggregationExpr{agg1, agg2, agg3}
		ctx := BuildContextFromAggregations(aggregations)

		// Verify all aggregations are mapped
		for _, agg := range aggregations {
			exprStr := agg.String()
			assert.True(t, ctx.HasMapping(exprStr))

			mappedColumn, exists := ctx.GetColumnName(exprStr)
			assert.True(t, exists)
			assert.Equal(t, ExpressionToColumnName(agg), mappedColumn)
		}
	})

	t.Run("duplicate aggregations", func(t *testing.T) {
		agg1 := Sum(Col("sales"))
		agg2 := Sum(Col("sales")) // Duplicate

		aggregations := []*AggregationExpr{agg1, agg2}
		ctx := BuildContextFromAggregations(aggregations)

		// Should only have one mapping (last one wins)
		exprStr := agg1.String()
		assert.True(t, ctx.HasMapping(exprStr))

		mappings := ctx.AllMappings()
		assert.Len(t, mappings, 1)
	})
}

func TestAggregationContext_Integration(t *testing.T) {
	t.Run("typical HAVING use case", func(t *testing.T) {
		// Simulate typical HAVING scenario:
		// SELECT category, SUM(sales), COUNT(id) FROM table
		// GROUP BY category
		// HAVING SUM(sales) > 1000 AND COUNT(id) > 5

		// Build aggregations used in GROUP BY
		sumSales := Sum(Col("sales"))
		countID := Count(Col("id"))
		aggregations := []*AggregationExpr{sumSales, countID}

		// Create context from aggregations
		ctx := BuildContextFromAggregations(aggregations)

		// Verify context has mappings for both aggregations
		sumExpr := sumSales.String()
		countExpr := countID.String()

		assert.True(t, ctx.HasMapping(sumExpr))
		assert.True(t, ctx.HasMapping(countExpr))

		// Get mapped column names (these would be used in HAVING evaluation)
		sumColumn, exists := ctx.GetColumnName(sumExpr)
		require.True(t, exists)
		assert.Equal(t, "sum(col(sales))", sumColumn)

		countColumn, exists := ctx.GetColumnName(countExpr)
		require.True(t, exists)
		assert.Equal(t, "count(col(id))", countColumn)

		// Verify reverse mapping works
		exprFromSum, exists := ctx.GetExpression(sumColumn)
		require.True(t, exists)
		assert.Equal(t, sumExpr, exprFromSum)

		exprFromCount, exists := ctx.GetExpression(countColumn)
		require.True(t, exists)
		assert.Equal(t, countExpr, exprFromCount)
	})

	t.Run("context modification during evaluation", func(t *testing.T) {
		ctx := NewAggregationContext()

		// Start with basic mapping
		ctx.AddMapping("SUM(a)", "sum_a")
		assert.Len(t, ctx.AllMappings(), 1)

		// Add more mappings during evaluation
		ctx.AddMapping("COUNT(b)", "count_b")
		ctx.AddMapping("AVG(c)", "avg_c")
		assert.Len(t, ctx.AllMappings(), 3)

		// Clear and rebuild (simulate new query)
		ctx.Clear()
		assert.Empty(t, ctx.AllMappings())

		// Rebuild with different aggregations
		ctx.AddMapping("MAX(x)", "max_x")
		ctx.AddMapping("MIN(y)", "min_y")
		assert.Len(t, ctx.AllMappings(), 2)

		// Verify only new mappings exist
		assert.True(t, ctx.HasMapping("MAX(x)"))
		assert.True(t, ctx.HasMapping("MIN(y)"))
		assert.False(t, ctx.HasMapping("SUM(a)"))
	})
}
