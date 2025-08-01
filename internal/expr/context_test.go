package expr_test

import (
	"testing"

	"github.com/paveg/gorilla/internal/expr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvaluationContext(t *testing.T) {
	t.Run("expr.EvaluationContext String representation", func(t *testing.T) {
		assert.Equal(t, "RowContext", expr.RowContext.String())
		assert.Equal(t, "GroupContext", expr.GroupContext.String())

		// Test unknown context
		unknownContext := expr.EvaluationContext(99)
		assert.Equal(t, "UnknownContext", unknownContext.String())
	})

	t.Run("AggregationExpr supports expr.GroupContext only", func(t *testing.T) {
		sum := expr.Sum(expr.Col("amount"))
		assert.True(t, sum.SupportsContext(expr.GroupContext))
		assert.False(t, sum.SupportsContext(expr.RowContext))

		count := expr.Count(expr.Col("id"))
		assert.True(t, count.SupportsContext(expr.GroupContext))
		assert.False(t, count.SupportsContext(expr.RowContext))

		mean := expr.Mean(expr.Col("price"))
		assert.True(t, mean.SupportsContext(expr.GroupContext))
		assert.False(t, mean.SupportsContext(expr.RowContext))

		minExpr := expr.Min(expr.Col("date"))
		assert.True(t, minExpr.SupportsContext(expr.GroupContext))
		assert.False(t, minExpr.SupportsContext(expr.RowContext))

		maxExpr := expr.Max(expr.Col("score"))
		assert.True(t, maxExpr.SupportsContext(expr.GroupContext))
		assert.False(t, maxExpr.SupportsContext(expr.RowContext))
	})

	t.Run("ColumnExpr supports both contexts", func(t *testing.T) {
		col := expr.Col("name")
		assert.True(t, col.SupportsContext(expr.GroupContext))
		assert.True(t, col.SupportsContext(expr.RowContext))

		// Different column names
		col2 := expr.Col("department")
		assert.True(t, col2.SupportsContext(expr.GroupContext))
		assert.True(t, col2.SupportsContext(expr.RowContext))
	})

	t.Run("LiteralExpr supports both contexts", func(t *testing.T) {
		lit := expr.Lit(1000)
		assert.True(t, lit.SupportsContext(expr.GroupContext))
		assert.True(t, lit.SupportsContext(expr.RowContext))

		// Different literal types
		strLit := expr.Lit("test")
		assert.True(t, strLit.SupportsContext(expr.GroupContext))
		assert.True(t, strLit.SupportsContext(expr.RowContext))

		boolLit := expr.Lit(true)
		assert.True(t, boolLit.SupportsContext(expr.GroupContext))
		assert.True(t, boolLit.SupportsContext(expr.RowContext))

		floatLit := expr.Lit(3.14)
		assert.True(t, floatLit.SupportsContext(expr.GroupContext))
		assert.True(t, floatLit.SupportsContext(expr.RowContext))
	})

	t.Run("expr.InvalidExpr supports no context", func(t *testing.T) {
		invalid := expr.Invalid("error message")
		assert.False(t, invalid.SupportsContext(expr.GroupContext))
		assert.False(t, invalid.SupportsContext(expr.RowContext))
	})

	t.Run("BinaryExpr context validation", func(t *testing.T) {
		// Valid in expr.GroupContext: SUM(amount) > 1000
		havingExpr := expr.Sum(expr.Col("amount")).Gt(expr.Lit(1000))
		require.NoError(t, expr.ValidateExpressionContext(havingExpr, expr.GroupContext))
		require.Error(t, expr.ValidateExpressionContext(havingExpr, expr.RowContext))

		// Valid in expr.RowContext: name = 'Alice'
		whereExpr := expr.Col("name").Eq(expr.Lit("Alice"))
		require.NoError(t, expr.ValidateExpressionContext(whereExpr, expr.RowContext))
		require.NoError(t, expr.ValidateExpressionContext(whereExpr, expr.GroupContext))

		// Complex binary expression with aggregation
		complexHaving := expr.Sum(expr.Col("price")).Mul(expr.Lit(1.1)).Gt(expr.Lit(5000))
		require.NoError(t, expr.ValidateExpressionContext(complexHaving, expr.GroupContext))
		require.Error(t, expr.ValidateExpressionContext(complexHaving, expr.RowContext))

		// Binary expression with columns only
		columnComparison := expr.Col("age").Gt(expr.Col("min_age"))
		require.NoError(t, expr.ValidateExpressionContext(columnComparison, expr.RowContext))
		require.NoError(t, expr.ValidateExpressionContext(columnComparison, expr.GroupContext))
	})

	t.Run("UnaryExpr context validation", func(t *testing.T) {
		// Unary NOT on column
		notExpr := expr.Col("active").Not()
		assert.True(t, notExpr.SupportsContext(expr.RowContext))
		assert.True(t, notExpr.SupportsContext(expr.GroupContext))

		// Unary NEG on aggregation
		negSum := expr.Sum(expr.Col("amount")).Neg()
		assert.True(t, negSum.SupportsContext(expr.GroupContext))
		assert.False(t, negSum.SupportsContext(expr.RowContext))

		// Validation
		require.NoError(t, expr.ValidateExpressionContext(notExpr, expr.RowContext))
		require.NoError(t, expr.ValidateExpressionContext(notExpr, expr.GroupContext))
		require.NoError(t, expr.ValidateExpressionContext(negSum, expr.GroupContext))
		require.Error(t, expr.ValidateExpressionContext(negSum, expr.RowContext))
	})

	t.Run("FunctionExpr context validation", func(t *testing.T) {
		// Math function on column
		absExpr := expr.Col("value").Abs()
		assert.True(t, absExpr.SupportsContext(expr.RowContext))
		assert.True(t, absExpr.SupportsContext(expr.GroupContext))

		// String function on column
		upperExpr := expr.Col("name").Upper()
		assert.True(t, upperExpr.SupportsContext(expr.RowContext))
		assert.True(t, upperExpr.SupportsContext(expr.GroupContext))

		// Function on aggregation
		roundSum := expr.Sum(expr.Col("price")).Round()
		assert.True(t, roundSum.SupportsContext(expr.GroupContext))
		assert.False(t, roundSum.SupportsContext(expr.RowContext))

		// Conditional function with aggregation
		ifExpr := expr.If(expr.Sum(expr.Col("amount")).Gt(expr.Lit(1000)), expr.Lit("high"), expr.Lit("low"))
		require.NoError(t, expr.ValidateExpressionContext(ifExpr, expr.GroupContext))
		require.Error(t, expr.ValidateExpressionContext(ifExpr, expr.RowContext))

		// Coalesce with mixed expressions
		coalesceExpr := expr.Coalesce(expr.Col("value"), expr.Lit(0))
		require.NoError(t, expr.ValidateExpressionContext(coalesceExpr, expr.RowContext))
		require.NoError(t, expr.ValidateExpressionContext(coalesceExpr, expr.GroupContext))
	})

	t.Run("CaseExpr context validation", func(t *testing.T) {
		// Simple CASE with columns
		caseExpr := expr.Case().
			When(expr.Col("age").Lt(expr.Lit(18)), expr.Lit("child")).
			When(expr.Col("age").Lt(expr.Lit(65)), expr.Lit("adult")).
			Else(expr.Lit("senior"))

		assert.True(t, caseExpr.SupportsContext(expr.RowContext))
		assert.True(t, caseExpr.SupportsContext(expr.GroupContext))
		require.NoError(t, expr.ValidateExpressionContext(caseExpr, expr.RowContext))
		require.NoError(t, expr.ValidateExpressionContext(caseExpr, expr.GroupContext))

		// CASE with aggregation in condition
		caseWithAgg := expr.Case().
			When(expr.Sum(expr.Col("amount")).Gt(expr.Lit(10000)), expr.Lit("large")).
			When(expr.Sum(expr.Col("amount")).Gt(expr.Lit(1000)), expr.Lit("medium")).
			Else(expr.Lit("small"))

		assert.False(t, caseWithAgg.SupportsContext(expr.RowContext))
		assert.True(t, caseWithAgg.SupportsContext(expr.GroupContext))
		require.Error(t, expr.ValidateExpressionContext(caseWithAgg, expr.RowContext))
		require.NoError(t, expr.ValidateExpressionContext(caseWithAgg, expr.GroupContext))

		// CASE with aggregation in value
		caseWithAggValue := expr.Case().
			When(expr.Col("category").Eq(expr.Lit("A")), expr.Sum(expr.Col("amount"))).
			Else(expr.Lit(0))

		assert.False(t, caseWithAggValue.SupportsContext(expr.RowContext))
		assert.True(t, caseWithAggValue.SupportsContext(expr.GroupContext))
	})

	t.Run("IntervalExpr supports both contexts", func(t *testing.T) {
		dayInterval := expr.Days(7)
		assert.True(t, dayInterval.SupportsContext(expr.RowContext))
		assert.True(t, dayInterval.SupportsContext(expr.GroupContext))

		monthInterval := expr.Months(3)
		assert.True(t, monthInterval.SupportsContext(expr.RowContext))
		assert.True(t, monthInterval.SupportsContext(expr.GroupContext))
	})

	t.Run("WindowExpr supports expr.RowContext only", func(t *testing.T) {
		// ROW_NUMBER window function
		rowNumExpr := expr.RowNumber().Over(expr.NewWindow().PartitionBy("department").OrderBy("salary", false))
		assert.True(t, rowNumExpr.SupportsContext(expr.RowContext))
		assert.False(t, rowNumExpr.SupportsContext(expr.GroupContext))

		// RANK window function
		rankExpr := expr.Rank().Over(expr.NewWindow().OrderBy("score", false))
		assert.True(t, rankExpr.SupportsContext(expr.RowContext))
		assert.False(t, rankExpr.SupportsContext(expr.GroupContext))

		// Additional window functions
		denseRankExpr := expr.DenseRank().Over(expr.NewWindow().OrderBy("score", false))
		assert.True(t, denseRankExpr.SupportsContext(expr.RowContext))
		assert.False(t, denseRankExpr.SupportsContext(expr.GroupContext))

		// Test WindowFunctionExpr as well
		leadExpr := expr.Lead(expr.Col("value"), 1).Over(expr.NewWindow().OrderBy("id", true))
		assert.True(t, leadExpr.SupportsContext(expr.RowContext))
		assert.False(t, leadExpr.SupportsContext(expr.GroupContext))

		lagExpr := expr.Lag(expr.Col("value"), 1).Over(expr.NewWindow().OrderBy("id", true))
		assert.True(t, lagExpr.SupportsContext(expr.RowContext))
		assert.False(t, lagExpr.SupportsContext(expr.GroupContext))

		// Validation
		require.NoError(t, expr.ValidateExpressionContext(rowNumExpr, expr.RowContext))
		require.Error(t, expr.ValidateExpressionContext(rowNumExpr, expr.GroupContext))
		require.NoError(t, expr.ValidateExpressionContext(leadExpr, expr.RowContext))
		require.Error(t, expr.ValidateExpressionContext(leadExpr, expr.GroupContext))
	})

	t.Run("Complex nested expression validation", func(t *testing.T) {
		// Nested expression: (SUM(price) * 1.1) / COUNT(id) > 100
		complexExpr := expr.Sum(expr.Col("price")).
			Mul(expr.Lit(1.1)).
			Div(expr.Count(expr.Col("id"))).
			Gt(expr.Lit(100))

		require.NoError(t, expr.ValidateExpressionContext(complexExpr, expr.GroupContext))
		require.Error(t, expr.ValidateExpressionContext(complexExpr, expr.RowContext))

		// Mixed expression with functions
		mixedExpr := expr.If(
			expr.Col("category").Eq(expr.Lit("premium")),
			expr.Col("price").Mul(expr.Lit(1.2)).Round(),
			expr.Col("price"),
		)
		require.NoError(t, expr.ValidateExpressionContext(mixedExpr, expr.RowContext))
		require.NoError(t, expr.ValidateExpressionContext(mixedExpr, expr.GroupContext))
	})

	t.Run("Edge cases", func(t *testing.T) {
		// Nil expression handling
		var nilExpr expr.Expr
		require.NoError(t, expr.ValidateExpressionContext(nilExpr, expr.RowContext))
		require.NoError(t, expr.ValidateExpressionContext(nilExpr, expr.GroupContext))

		// Expression that doesn't implement expr.ContextualExpr
		// (Currently all expressions implement it, but this tests the fallback logic)
		// The validation should pass for non-contextual expressions
		basicExpr := expr.Col("test")
		require.NoError(t, expr.ValidateExpressionContext(basicExpr, expr.RowContext))
		require.NoError(t, expr.ValidateExpressionContext(basicExpr, expr.GroupContext))

		// Empty expressions and boundary conditions
		emptyColumnExpr := expr.Col("")
		assert.True(t, emptyColumnExpr.SupportsContext(expr.RowContext))
		assert.True(t, emptyColumnExpr.SupportsContext(expr.GroupContext))

		// Complex nested expressions with mixed contexts
		mixedNestedExpr := expr.Case().
			When(expr.Col("category").Eq(expr.Lit("A")), expr.Col("value")).
			When(expr.Col("category").Eq(expr.Lit("B")), expr.Col("value").Mul(expr.Lit(2))).
			Else(expr.Col("default_value"))
		require.NoError(t, expr.ValidateExpressionContext(mixedNestedExpr, expr.RowContext))
		require.NoError(t, expr.ValidateExpressionContext(mixedNestedExpr, expr.GroupContext))

		// Deeply nested arithmetic with valid context
		deeplyNested := expr.Col("a").Add(expr.Col("b")).Mul(expr.Col("c")).Div(expr.Col("d")).Sub(expr.Col("e"))
		require.NoError(t, expr.ValidateExpressionContext(deeplyNested, expr.RowContext))
		require.NoError(t, expr.ValidateExpressionContext(deeplyNested, expr.GroupContext))
	})

	t.Run("Error messages", func(t *testing.T) {
		// Check error message format for aggregation in expr.RowContext
		sumExpr := expr.Sum(expr.Col("amount"))
		err := expr.ValidateExpressionContext(sumExpr, expr.RowContext)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "sum(col(amount))")
		assert.Contains(t, err.Error(), "RowContext")

		// Check error message format for window function in expr.GroupContext
		rowNumExpr := expr.RowNumber().Over(expr.NewWindow().OrderBy("id", true))
		err = expr.ValidateExpressionContext(rowNumExpr, expr.GroupContext)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "GroupContext")

		// Nested error in binary expression
		nestedExpr := expr.Col("value").Add(expr.Sum(expr.Col("total")))
		err = expr.ValidateExpressionContext(nestedExpr, expr.RowContext)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "sum(col(total))")

		// Complex nested error with multiple contexts
		complexExpr := expr.If(
			expr.Sum(expr.Col("amount")).Gt(expr.Lit(1000)),
			expr.RowNumber().Over(expr.NewWindow().OrderBy("id", true)),
			expr.Lit(0),
		)
		// Should fail in both contexts for different reasons
		err = expr.ValidateExpressionContext(complexExpr, expr.RowContext)
		require.Error(t, err)
		err = expr.ValidateExpressionContext(complexExpr, expr.GroupContext)
		require.Error(t, err)
	})
}

func TestContextValidationWithAggregationComparisons(t *testing.T) {
	// Test various aggregation comparison patterns
	t.Run("Aggregation comparison methods", func(t *testing.T) {
		sum := expr.Sum(expr.Col("amount"))

		// All comparison methods should produce expressions valid only in expr.GroupContext
		comparisons := []expr.Expr{
			sum.Gt(expr.Lit(1000)),
			sum.Lt(expr.Lit(500)),
			sum.Eq(expr.Lit(750)),
			sum.Ne(expr.Lit(0)),
			sum.Ge(expr.Lit(100)),
			sum.Le(expr.Lit(10000)),
		}

		for _, e := range comparisons {
			require.NoError(t, expr.ValidateExpressionContext(e, expr.GroupContext))
			require.Error(t, expr.ValidateExpressionContext(e, expr.RowContext))
		}
	})

	t.Run("Logical operations with aggregations", func(t *testing.T) {
		// AND/OR with aggregations
		andExpr := expr.Sum(expr.Col("amount")).Gt(expr.Lit(1000)).And(expr.Count(expr.Col("id")).Lt(expr.Lit(100)))
		require.NoError(t, expr.ValidateExpressionContext(andExpr, expr.GroupContext))
		require.Error(t, expr.ValidateExpressionContext(andExpr, expr.RowContext))

		orExpr := expr.Mean(expr.Col("score")).Ge(expr.Lit(80)).Or(expr.Max(expr.Col("score")).Eq(expr.Lit(100)))
		require.NoError(t, expr.ValidateExpressionContext(orExpr, expr.GroupContext))
		require.Error(t, expr.ValidateExpressionContext(orExpr, expr.RowContext))
	})
}

func TestContextValidationComprehensive(t *testing.T) {
	t.Run("All aggregation types context validation", func(t *testing.T) {
		aggregationExprs := []expr.Expr{
			expr.Sum(expr.Col("amount")),
			expr.Count(expr.Col("id")),
			expr.Mean(expr.Col("price")),
			expr.Min(expr.Col("date")),
			expr.Max(expr.Col("score")),
		}

		for _, aggExpr := range aggregationExprs {
			// All aggregations should support expr.GroupContext only
			assert.True(t, aggExpr.(expr.ContextualExpr).SupportsContext(expr.GroupContext),
				"Aggregation %s should support expr.GroupContext", aggExpr.String())
			assert.False(t, aggExpr.(expr.ContextualExpr).SupportsContext(expr.RowContext),
				"Aggregation %s should not support expr.RowContext", aggExpr.String())

			// Validation should pass in expr.GroupContext and fail in expr.RowContext
			require.NoError(t, expr.ValidateExpressionContext(aggExpr, expr.GroupContext))
			require.Error(t, expr.ValidateExpressionContext(aggExpr, expr.RowContext))
		}
	})

	t.Run("Mixed context expressions with chaining", func(t *testing.T) {
		// Row context: column operations that should work in both contexts
		rowExpr := expr.Col("price").Mul(expr.Lit(1.2)).Round()
		require.NoError(t, expr.ValidateExpressionContext(rowExpr, expr.RowContext))
		require.NoError(t, expr.ValidateExpressionContext(rowExpr, expr.GroupContext))

		// Group context: aggregation operations
		groupExpr := expr.Sum(expr.Col("amount")).Div(expr.Count(expr.Col("id"))).Round()
		require.NoError(t, expr.ValidateExpressionContext(groupExpr, expr.GroupContext))
		require.Error(t, expr.ValidateExpressionContext(groupExpr, expr.RowContext))

		// Mixed invalid: aggregation in row-level comparison
		invalidMixed := expr.Col("name").Eq(expr.Lit("test")).And(expr.Sum(expr.Col("amount")).Gt(expr.Lit(100)))
		require.Error(t, expr.ValidateExpressionContext(invalidMixed, expr.RowContext))
		require.NoError(t, expr.ValidateExpressionContext(invalidMixed, expr.GroupContext))
	})

	t.Run("Boundary conditions and special cases", func(t *testing.T) {
		// Empty function expressions
		upperEmpty := expr.Col("").Upper()
		assert.True(t, upperEmpty.SupportsContext(expr.RowContext))
		assert.True(t, upperEmpty.SupportsContext(expr.GroupContext))

		// Unary operations on different expression types
		negColumn := expr.Col("value").Neg()
		assert.True(t, negColumn.SupportsContext(expr.RowContext))
		assert.True(t, negColumn.SupportsContext(expr.GroupContext))

		negAggregation := expr.Sum(expr.Col("amount")).Neg()
		assert.False(t, negAggregation.SupportsContext(expr.RowContext))
		assert.True(t, negAggregation.SupportsContext(expr.GroupContext))

		notColumn := expr.Col("active").Not()
		assert.True(t, notColumn.SupportsContext(expr.RowContext))
		assert.True(t, notColumn.SupportsContext(expr.GroupContext))

		// Deeply nested conditional with different contexts
		deepConditional := expr.Case().
			When(expr.Col("type").Eq(expr.Lit("A")),
				expr.If(expr.Col("value").Gt(expr.Lit(100)), expr.Lit("high"), expr.Lit("low"))).
			When(expr.Col("type").Eq(expr.Lit("B")),
				expr.Col("category").Upper()).
			Else(expr.Lit("unknown"))

		require.NoError(t, expr.ValidateExpressionContext(deepConditional, expr.RowContext))
		require.NoError(t, expr.ValidateExpressionContext(deepConditional, expr.GroupContext))
	})

	t.Run("Context validation error propagation", func(t *testing.T) {
		// Test that errors bubble up correctly through nested expressions

		// Error should come from Sum in expr.RowContext
		nestedSum := expr.Col("base").Add(expr.Sum(expr.Col("amount")))
		err := expr.ValidateExpressionContext(nestedSum, expr.RowContext)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "sum(col(amount))")

		// Error should come from window function in expr.GroupContext
		nestedWindow := expr.Sum(expr.Col("total")).Add(expr.RowNumber().Over(expr.NewWindow().OrderBy("id", true)))
		err = expr.ValidateExpressionContext(nestedWindow, expr.GroupContext)
		require.Error(t, err)

		// Multiple levels of nesting
		deeplyNested := expr.Case().
			When(expr.Col("condition").Eq(expr.Lit(true)),
				expr.Col("a").Add(expr.Sum(expr.Col("b")))).
			Else(expr.Lit(0))

		err = expr.ValidateExpressionContext(deeplyNested, expr.RowContext)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "sum(col(b))")
	})
}

func TestEvaluationContextStringRepresentation(t *testing.T) {
	t.Run("All context values have proper string representation", func(t *testing.T) {
		assert.Equal(t, "RowContext", expr.RowContext.String())
		assert.Equal(t, "GroupContext", expr.GroupContext.String())

		// Test unknown context handling
		unknownContext := expr.EvaluationContext(999)
		assert.Equal(t, "UnknownContext", unknownContext.String())

		// Test edge case contexts
		negativeContext := expr.EvaluationContext(-1)
		assert.Equal(t, "UnknownContext", negativeContext.String())
	})
}
