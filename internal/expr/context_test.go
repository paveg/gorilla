package expr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEvaluationContext(t *testing.T) {
	t.Run("EvaluationContext String representation", func(t *testing.T) {
		assert.Equal(t, "RowContext", RowContext.String())
		assert.Equal(t, "GroupContext", GroupContext.String())

		// Test unknown context
		unknownContext := EvaluationContext(99)
		assert.Equal(t, "UnknownContext", unknownContext.String())
	})

	t.Run("AggregationExpr supports GroupContext only", func(t *testing.T) {
		sum := Sum(Col("amount"))
		assert.True(t, sum.SupportsContext(GroupContext))
		assert.False(t, sum.SupportsContext(RowContext))

		count := Count(Col("id"))
		assert.True(t, count.SupportsContext(GroupContext))
		assert.False(t, count.SupportsContext(RowContext))

		mean := Mean(Col("price"))
		assert.True(t, mean.SupportsContext(GroupContext))
		assert.False(t, mean.SupportsContext(RowContext))

		minExpr := Min(Col("date"))
		assert.True(t, minExpr.SupportsContext(GroupContext))
		assert.False(t, minExpr.SupportsContext(RowContext))

		maxExpr := Max(Col("score"))
		assert.True(t, maxExpr.SupportsContext(GroupContext))
		assert.False(t, maxExpr.SupportsContext(RowContext))
	})

	t.Run("ColumnExpr supports both contexts", func(t *testing.T) {
		col := Col("name")
		assert.True(t, col.SupportsContext(GroupContext))
		assert.True(t, col.SupportsContext(RowContext))

		// Different column names
		col2 := Col("department")
		assert.True(t, col2.SupportsContext(GroupContext))
		assert.True(t, col2.SupportsContext(RowContext))
	})

	t.Run("LiteralExpr supports both contexts", func(t *testing.T) {
		lit := Lit(1000)
		assert.True(t, lit.SupportsContext(GroupContext))
		assert.True(t, lit.SupportsContext(RowContext))

		// Different literal types
		strLit := Lit("test")
		assert.True(t, strLit.SupportsContext(GroupContext))
		assert.True(t, strLit.SupportsContext(RowContext))

		boolLit := Lit(true)
		assert.True(t, boolLit.SupportsContext(GroupContext))
		assert.True(t, boolLit.SupportsContext(RowContext))

		floatLit := Lit(3.14)
		assert.True(t, floatLit.SupportsContext(GroupContext))
		assert.True(t, floatLit.SupportsContext(RowContext))
	})

	t.Run("InvalidExpr supports no context", func(t *testing.T) {
		invalid := Invalid("error message")
		assert.False(t, invalid.SupportsContext(GroupContext))
		assert.False(t, invalid.SupportsContext(RowContext))
	})

	t.Run("BinaryExpr context validation", func(t *testing.T) {
		// Valid in GroupContext: SUM(amount) > 1000
		havingExpr := Sum(Col("amount")).Gt(Lit(1000))
		assert.NoError(t, ValidateExpressionContext(havingExpr, GroupContext))
		assert.Error(t, ValidateExpressionContext(havingExpr, RowContext))

		// Valid in RowContext: name = 'Alice'
		whereExpr := Col("name").Eq(Lit("Alice"))
		assert.NoError(t, ValidateExpressionContext(whereExpr, RowContext))
		assert.NoError(t, ValidateExpressionContext(whereExpr, GroupContext))

		// Complex binary expression with aggregation
		complexHaving := Sum(Col("price")).Mul(Lit(1.1)).Gt(Lit(5000))
		assert.NoError(t, ValidateExpressionContext(complexHaving, GroupContext))
		assert.Error(t, ValidateExpressionContext(complexHaving, RowContext))

		// Binary expression with columns only
		columnComparison := Col("age").Gt(Col("min_age"))
		assert.NoError(t, ValidateExpressionContext(columnComparison, RowContext))
		assert.NoError(t, ValidateExpressionContext(columnComparison, GroupContext))
	})

	t.Run("UnaryExpr context validation", func(t *testing.T) {
		// Unary NOT on column
		notExpr := Col("active").Not()
		assert.True(t, notExpr.SupportsContext(RowContext))
		assert.True(t, notExpr.SupportsContext(GroupContext))

		// Unary NEG on aggregation
		negSum := Sum(Col("amount")).Neg()
		assert.True(t, negSum.SupportsContext(GroupContext))
		assert.False(t, negSum.SupportsContext(RowContext))

		// Validation
		assert.NoError(t, ValidateExpressionContext(notExpr, RowContext))
		assert.NoError(t, ValidateExpressionContext(notExpr, GroupContext))
		assert.NoError(t, ValidateExpressionContext(negSum, GroupContext))
		assert.Error(t, ValidateExpressionContext(negSum, RowContext))
	})

	t.Run("FunctionExpr context validation", func(t *testing.T) {
		// Math function on column
		absExpr := Col("value").Abs()
		assert.True(t, absExpr.SupportsContext(RowContext))
		assert.True(t, absExpr.SupportsContext(GroupContext))

		// String function on column
		upperExpr := Col("name").Upper()
		assert.True(t, upperExpr.SupportsContext(RowContext))
		assert.True(t, upperExpr.SupportsContext(GroupContext))

		// Function on aggregation
		roundSum := Sum(Col("price")).Round()
		assert.True(t, roundSum.SupportsContext(GroupContext))
		assert.False(t, roundSum.SupportsContext(RowContext))

		// Conditional function with aggregation
		ifExpr := If(Sum(Col("amount")).Gt(Lit(1000)), Lit("high"), Lit("low"))
		assert.NoError(t, ValidateExpressionContext(ifExpr, GroupContext))
		assert.Error(t, ValidateExpressionContext(ifExpr, RowContext))

		// Coalesce with mixed expressions
		coalesceExpr := Coalesce(Col("value"), Lit(0))
		assert.NoError(t, ValidateExpressionContext(coalesceExpr, RowContext))
		assert.NoError(t, ValidateExpressionContext(coalesceExpr, GroupContext))
	})

	t.Run("CaseExpr context validation", func(t *testing.T) {
		// Simple CASE with columns
		caseExpr := Case().
			When(Col("age").Lt(Lit(18)), Lit("child")).
			When(Col("age").Lt(Lit(65)), Lit("adult")).
			Else(Lit("senior"))

		assert.True(t, caseExpr.SupportsContext(RowContext))
		assert.True(t, caseExpr.SupportsContext(GroupContext))
		assert.NoError(t, ValidateExpressionContext(caseExpr, RowContext))
		assert.NoError(t, ValidateExpressionContext(caseExpr, GroupContext))

		// CASE with aggregation in condition
		caseWithAgg := Case().
			When(Sum(Col("amount")).Gt(Lit(10000)), Lit("large")).
			When(Sum(Col("amount")).Gt(Lit(1000)), Lit("medium")).
			Else(Lit("small"))

		assert.False(t, caseWithAgg.SupportsContext(RowContext))
		assert.True(t, caseWithAgg.SupportsContext(GroupContext))
		assert.Error(t, ValidateExpressionContext(caseWithAgg, RowContext))
		assert.NoError(t, ValidateExpressionContext(caseWithAgg, GroupContext))

		// CASE with aggregation in value
		caseWithAggValue := Case().
			When(Col("category").Eq(Lit("A")), Sum(Col("amount"))).
			Else(Lit(0))

		assert.False(t, caseWithAggValue.SupportsContext(RowContext))
		assert.True(t, caseWithAggValue.SupportsContext(GroupContext))
	})

	t.Run("IntervalExpr supports both contexts", func(t *testing.T) {
		dayInterval := Days(7)
		assert.True(t, dayInterval.SupportsContext(RowContext))
		assert.True(t, dayInterval.SupportsContext(GroupContext))

		monthInterval := Months(3)
		assert.True(t, monthInterval.SupportsContext(RowContext))
		assert.True(t, monthInterval.SupportsContext(GroupContext))
	})

	t.Run("WindowExpr supports RowContext only", func(t *testing.T) {
		// ROW_NUMBER window function
		rowNumExpr := RowNumber().Over(NewWindow().PartitionBy("department").OrderBy("salary", false))
		assert.True(t, rowNumExpr.SupportsContext(RowContext))
		assert.False(t, rowNumExpr.SupportsContext(GroupContext))

		// RANK window function
		rankExpr := Rank().Over(NewWindow().OrderBy("score", false))
		assert.True(t, rankExpr.SupportsContext(RowContext))
		assert.False(t, rankExpr.SupportsContext(GroupContext))

		// Validation
		assert.NoError(t, ValidateExpressionContext(rowNumExpr, RowContext))
		assert.Error(t, ValidateExpressionContext(rowNumExpr, GroupContext))
	})

	t.Run("Complex nested expression validation", func(t *testing.T) {
		// Nested expression: (SUM(price) * 1.1) / COUNT(id) > 100
		complexExpr := Sum(Col("price")).
			Mul(Lit(1.1)).
			Div(Count(Col("id"))).
			Gt(Lit(100))

		assert.NoError(t, ValidateExpressionContext(complexExpr, GroupContext))
		assert.Error(t, ValidateExpressionContext(complexExpr, RowContext))

		// Mixed expression with functions
		mixedExpr := If(
			Col("category").Eq(Lit("premium")),
			Col("price").Mul(Lit(1.2)).Round(),
			Col("price"),
		)
		assert.NoError(t, ValidateExpressionContext(mixedExpr, RowContext))
		assert.NoError(t, ValidateExpressionContext(mixedExpr, GroupContext))
	})

	t.Run("Edge cases", func(t *testing.T) {
		// Nil expression handling
		var nilExpr Expr
		assert.NoError(t, ValidateExpressionContext(nilExpr, RowContext))
		assert.NoError(t, ValidateExpressionContext(nilExpr, GroupContext))

		// Expression that doesn't implement ContextualExpr
		// (Currently all expressions implement it, but this tests the fallback logic)
		// The validation should pass for non-contextual expressions
		basicExpr := Col("test")
		assert.NoError(t, ValidateExpressionContext(basicExpr, RowContext))
		assert.NoError(t, ValidateExpressionContext(basicExpr, GroupContext))
	})

	t.Run("Error messages", func(t *testing.T) {
		// Check error message format
		sumExpr := Sum(Col("amount"))
		err := ValidateExpressionContext(sumExpr, RowContext)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "sum(col(amount))")
		assert.Contains(t, err.Error(), "RowContext")

		// Nested error
		nestedExpr := Col("value").Add(Sum(Col("total")))
		err = ValidateExpressionContext(nestedExpr, RowContext)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "sum(col(total))")
	})
}

func TestContextValidationWithAggregationComparisons(t *testing.T) {
	// Test various aggregation comparison patterns
	t.Run("Aggregation comparison methods", func(t *testing.T) {
		sum := Sum(Col("amount"))

		// All comparison methods should produce expressions valid only in GroupContext
		comparisons := []Expr{
			sum.Gt(Lit(1000)),
			sum.Lt(Lit(500)),
			sum.Eq(Lit(750)),
			sum.Ne(Lit(0)),
			sum.Ge(Lit(100)),
			sum.Le(Lit(10000)),
		}

		for _, expr := range comparisons {
			assert.NoError(t, ValidateExpressionContext(expr, GroupContext))
			assert.Error(t, ValidateExpressionContext(expr, RowContext))
		}
	})

	t.Run("Logical operations with aggregations", func(t *testing.T) {
		// AND/OR with aggregations
		andExpr := Sum(Col("amount")).Gt(Lit(1000)).And(Count(Col("id")).Lt(Lit(100)))
		assert.NoError(t, ValidateExpressionContext(andExpr, GroupContext))
		assert.Error(t, ValidateExpressionContext(andExpr, RowContext))

		orExpr := Mean(Col("score")).Ge(Lit(80)).Or(Max(Col("score")).Eq(Lit(100)))
		assert.NoError(t, ValidateExpressionContext(orExpr, GroupContext))
		assert.Error(t, ValidateExpressionContext(orExpr, RowContext))
	})
}
