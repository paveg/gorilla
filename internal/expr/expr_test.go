package expr_test

import (
	"fmt"
	"testing"

	"github.com/paveg/gorilla/internal/expr"
	"github.com/stretchr/testify/assert"
)

func TestColumnExpr(t *testing.T) {
	col := expr.Col("test_column")

	assert.Equal(t, expr.ExprColumn, col.Type())
	assert.Equal(t, "test_column", col.Name())
	assert.Equal(t, "col(test_column)", col.String())
}

func TestLiteralExpr(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{"int literal", 42, "lit(42)"},
		{"string literal", "hello", "lit(hello)"},
		{"float literal", 3.14, "lit(3.14)"},
		{"bool literal", true, "lit(true)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lit := expr.Lit(tt.value)

			assert.Equal(t, expr.ExprLiteral, lit.Type())
			assert.Equal(t, tt.value, lit.Value())
			assert.Equal(t, tt.expected, lit.String())
		})
	}
}

func TestBinaryExpressions(t *testing.T) {
	col := expr.Col("value")
	lit := expr.Lit(10)

	tests := []struct {
		name     string
		expr     *expr.BinaryExpr
		expected string
		op       expr.BinaryOp
	}{
		{"addition", col.Add(lit), "(col(value) + lit(10))", expr.OpAdd},
		{"subtraction", col.Sub(lit), "(col(value) - lit(10))", expr.OpSub},
		{"multiplication", col.Mul(lit), "(col(value) * lit(10))", expr.OpMul},
		{"division", col.Div(lit), "(col(value) / lit(10))", expr.OpDiv},
		{"equality", col.Eq(lit), "(col(value) == lit(10))", expr.OpEq},
		{"not equal", col.Ne(lit), "(col(value) != lit(10))", expr.OpNe},
		{"less than", col.Lt(lit), "(col(value) < lit(10))", expr.OpLt},
		{"less than or equal", col.Le(lit), "(col(value) <= lit(10))", expr.OpLe},
		{"greater than", col.Gt(lit), "(col(value) > lit(10))", expr.OpGt},
		{"greater than or equal", col.Ge(lit), "(col(value) >= lit(10))", expr.OpGe},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, expr.ExprBinary, tt.expr.Type())
			assert.Equal(t, tt.expected, tt.expr.String())
			assert.Equal(t, tt.op, tt.expr.Op())
			assert.Equal(t, col, tt.expr.Left())
			assert.Equal(t, lit, tt.expr.Right())
		})
	}
}

func TestChainedExpressions(t *testing.T) {
	col := expr.Col("value")

	// Test chaining: (value + 10) * 2
	chained := col.Add(expr.Lit(10)).Mul(expr.Lit(2))

	assert.Equal(t, expr.ExprBinary, chained.Type())
	assert.Equal(t, "((col(value) + lit(10)) * lit(2))", chained.String())
	assert.Equal(t, expr.OpMul, chained.Op())

	// The left side should be the addition expression
	leftExpr, ok := chained.Left().(*expr.BinaryExpr)
	assert.True(t, ok)
	assert.Equal(t, expr.OpAdd, leftExpr.Op())
}

func TestLogicalExpressions(t *testing.T) {
	col1 := expr.Col("age")
	col2 := expr.Col("salary")

	// Test: (age > 25) && (salary > 50000)
	expr1 := col1.Gt(expr.Lit(25))
	expr2 := col2.Gt(expr.Lit(50000))
	combined := expr1.And(expr2)

	assert.Equal(t, expr.ExprBinary, combined.Type())
	assert.Equal(t, "((col(age) > lit(25)) && (col(salary) > lit(50000)))", combined.String())
	assert.Equal(t, expr.OpAnd, combined.Op())

	// Test OR operation
	orExpr := expr1.Or(expr2)
	assert.Equal(t, expr.OpOr, orExpr.Op())
	assert.Equal(t, "((col(age) > lit(25)) || (col(salary) > lit(50000)))", orExpr.String())
}

func TestComplexExpressions(t *testing.T) {
	// Test complex nested expression: ((value * 2) + 5) > (threshold - 1)
	value := expr.Col("value")
	threshold := expr.Col("threshold")

	left := value.Mul(expr.Lit(2)).Add(expr.Lit(5))
	right := threshold.Sub(expr.Lit(1))
	complexExpr := left.Gt(right)

	expected := "(((col(value) * lit(2)) + lit(5)) > (col(threshold) - lit(1)))"
	assert.Equal(t, expected, complexExpr.String())
}

func TestBinaryOpString(t *testing.T) {
	// Test that all binary operations have proper string representations
	col := expr.Col("test")
	lit := expr.Lit(1)

	operations := []struct {
		expr     *expr.BinaryExpr
		contains string
	}{
		{col.Add(lit), "+"},
		{col.Sub(lit), "-"},
		{col.Mul(lit), "*"},
		{col.Div(lit), "/"},
		{col.Eq(lit), "=="},
		{col.Ne(lit), "!="},
		{col.Lt(lit), "<"},
		{col.Le(lit), "<="},
		{col.Gt(lit), ">"},
		{col.Ge(lit), ">="},
	}

	for _, op := range operations {
		assert.Contains(t, op.expr.String(), op.contains)
	}
}

func TestExpressionTypes(t *testing.T) {
	col := expr.Col("test")
	lit := expr.Lit(42)
	bin := col.Add(lit)

	// Test type assertions
	assert.Equal(t, expr.ExprColumn, col.Type())
	assert.Equal(t, expr.ExprLiteral, lit.Type())
	assert.Equal(t, expr.ExprBinary, bin.Type())

	// Test that expressions implement the Expr interface
	exprs := []expr.Expr{col, lit, bin}
	for i, expr := range exprs {
		assert.NotNil(t, expr, "Expression %d should not be nil", i)
		assert.NotEmpty(t, expr.String(), "Expression %d should have string representation", i)
	}
}

func TestInvalidExpr(t *testing.T) {
	t.Run("creation and basic properties", func(t *testing.T) {
		message := "test error message"
		invalidExpr := expr.Invalid(message)

		assert.Equal(t, expr.ExprInvalid, invalidExpr.Type())
		assert.Equal(t, message, invalidExpr.Message())
		assert.Equal(t, "invalid(test error message)", invalidExpr.String())
	})

	t.Run("empty message", func(t *testing.T) {
		invalidExpr := expr.Invalid("")

		assert.Equal(t, expr.ExprInvalid, invalidExpr.Type())
		assert.Empty(t, invalidExpr.Message())
		assert.Equal(t, "invalid()", invalidExpr.String())
	})

	t.Run("complex error message", func(t *testing.T) {
		message := "Operation 'Add' not supported on type *InvalidExpr"
		invalidExpr := expr.Invalid(message)

		assert.Equal(t, expr.ExprInvalid, invalidExpr.Type())
		assert.Contains(t, invalidExpr.Message(), "Add")
		assert.Contains(t, invalidExpr.Message(), "not supported")
		assert.Contains(t, invalidExpr.String(), message)
	})

	t.Run("implements Expr interface", func(t *testing.T) {
		var invalidExpr expr.Expr = expr.Invalid("test")

		assert.NotNil(t, invalidExpr)
		assert.Equal(t, expr.ExprInvalid, invalidExpr.Type())
		assert.NotEmpty(t, invalidExpr.String())
	})
}

func TestAggregationExprComparisons(t *testing.T) {
	col := expr.Col("salary")

	t.Run("basic comparison operations with literals", func(t *testing.T) {
		// Test all aggregation types with all comparison operations
		aggregations := []struct {
			name     string
			aggExpr  *expr.AggregationExpr
			expected string
		}{
			{"Sum", expr.Sum(col), "sum(col(salary))"},
			{"Count", expr.Count(col), "count(col(salary))"},
			{"Mean", expr.Mean(col), "mean(col(salary))"},
			{"Min", expr.Min(col), "min(col(salary))"},
			{"Max", expr.Max(col), "max(col(salary))"},
		}

		comparisons := []struct {
			name     string
			method   func(*expr.AggregationExpr, expr.Expr) *expr.BinaryExpr
			op       expr.BinaryOp
			opSymbol string
		}{
			{"Gt", (*expr.AggregationExpr).Gt, expr.OpGt, ">"},
			{"Lt", (*expr.AggregationExpr).Lt, expr.OpLt, "<"},
			{"Eq", (*expr.AggregationExpr).Eq, expr.OpEq, "=="},
			{"Ne", (*expr.AggregationExpr).Ne, expr.OpNe, "!="},
			{"Ge", (*expr.AggregationExpr).Ge, expr.OpGe, ">="},
			{"Le", (*expr.AggregationExpr).Le, expr.OpLe, "<="},
		}

		for _, agg := range aggregations {
			for _, comp := range comparisons {
				t.Run(fmt.Sprintf("%s_%s", agg.name, comp.name), func(t *testing.T) {
					literal := expr.Lit(100)
					result := comp.method(agg.aggExpr, literal)

					// Verify expression properties
					assert.Equal(t, expr.ExprBinary, result.Type())
					assert.Equal(t, comp.op, result.Op())
					assert.Equal(t, agg.aggExpr, result.Left())
					assert.Equal(t, literal, result.Right())

					// Verify string representation
					expected := fmt.Sprintf("(%s %s lit(100))", agg.expected, comp.opSymbol)
					assert.Equal(t, expected, result.String())
				})
			}
		}
	})

	t.Run("comparison with column expressions", func(t *testing.T) {
		sumExpr := expr.Sum(expr.Col("amount"))
		targetCol := expr.Col("target")

		result := sumExpr.Gt(targetCol)

		assert.Equal(t, expr.ExprBinary, result.Type())
		assert.Equal(t, expr.OpGt, result.Op())
		assert.Equal(t, sumExpr, result.Left())
		assert.Equal(t, targetCol, result.Right())
		assert.Equal(t, "(sum(col(amount)) > col(target))", result.String())
	})

	t.Run("logical operations And/Or", func(t *testing.T) {
		sumExpr := expr.Sum(expr.Col("revenue"))
		countExpr := expr.Count(expr.Col("orders"))

		// Test: sum(revenue) > 50000 AND count(orders) > 10
		condition1 := sumExpr.Gt(expr.Lit(50000))
		condition2 := countExpr.Gt(expr.Lit(10))
		andResult := condition1.And(condition2)

		assert.Equal(t, expr.ExprBinary, andResult.Type())
		assert.Equal(t, expr.OpAnd, andResult.Op())
		assert.Equal(t, condition1, andResult.Left())
		assert.Equal(t, condition2, andResult.Right())
		assert.Equal(t, "((sum(col(revenue)) > lit(50000)) && (count(col(orders)) > lit(10)))", andResult.String())

		// Test: sum(revenue) < 1000 OR count(orders) == 0
		condition3 := sumExpr.Lt(expr.Lit(1000))
		condition4 := countExpr.Eq(expr.Lit(0))
		orResult := condition3.Or(condition4)

		assert.Equal(t, expr.ExprBinary, orResult.Type())
		assert.Equal(t, expr.OpOr, orResult.Op())
		assert.Equal(t, condition3, orResult.Left())
		assert.Equal(t, condition4, orResult.Right())
		assert.Equal(t, "((sum(col(revenue)) < lit(1000)) || (count(col(orders)) == lit(0)))", orResult.String())
	})

	t.Run("chaining multiple operations", func(t *testing.T) {
		avgExpr := expr.Mean(expr.Col("rating"))
		minExpr := expr.Min(expr.Col("price"))
		maxExpr := expr.Max(expr.Col("discount"))

		// Complex chain: (mean(rating) >= 4.0) AND (min(price) > 10) AND (max(discount) <= 0.5)
		chain := avgExpr.Ge(expr.Lit(4.0)).And(minExpr.Gt(expr.Lit(10))).And(maxExpr.Le(expr.Lit(0.5)))

		assert.Equal(t, expr.ExprBinary, chain.Type())
		assert.Equal(t, expr.OpAnd, chain.Op())

		// Verify the complex chaining structure
		expected := "(((mean(col(rating)) >= lit(4)) && (min(col(price)) > lit(10))) && (max(col(discount)) <= lit(0.5)))"
		assert.Equal(t, expected, chain.String())
	})

	t.Run("aggregation with alias in comparisons", func(t *testing.T) {
		totalSales := expr.Sum(expr.Col("sales")).As("total_sales")
		result := totalSales.Gt(expr.Lit(100000))

		assert.Equal(t, expr.ExprBinary, result.Type())
		assert.Equal(t, expr.OpGt, result.Op())
		assert.Equal(t, totalSales, result.Left())
		assert.Equal(t, "total_sales", totalSales.Alias())
		assert.Equal(t, "(sum(col(sales)) > lit(100000))", result.String())
	})

	t.Run("different literal types", func(t *testing.T) {
		countExpr := expr.Count(expr.Col("items"))

		tests := []struct {
			name     string
			value    interface{}
			expected string
		}{
			{"integer", 42, "(count(col(items)) == lit(42))"},
			{"float", 3.14, "(count(col(items)) == lit(3.14))"},
			{"string", "test", "(count(col(items)) == lit(test))"},
			{"boolean", true, "(count(col(items)) == lit(true))"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := countExpr.Eq(expr.Lit(tt.value))
				assert.Equal(t, tt.expected, result.String())
			})
		}
	})

	t.Run("nested aggregations comparison", func(t *testing.T) {
		// Test comparison between two aggregations
		avgSalary := expr.Mean(expr.Col("salary"))
		maxSalary := expr.Max(expr.Col("salary"))

		result := avgSalary.Lt(maxSalary)

		assert.Equal(t, expr.ExprBinary, result.Type())
		assert.Equal(t, expr.OpLt, result.Op())
		assert.Equal(t, avgSalary, result.Left())
		assert.Equal(t, maxSalary, result.Right())
		assert.Equal(t, "(mean(col(salary)) < max(col(salary)))", result.String())
	})

	t.Run("integration with existing expression system", func(t *testing.T) {
		// Test that aggregation comparisons work with column expressions and binary expressions
		col1 := expr.Col("amount")
		col2 := expr.Col("threshold")
		sumExpr := expr.Sum(col1)

		// Complex expression: sum(amount) > (threshold * 2)
		threshold := col2.Mul(expr.Lit(2))
		result := sumExpr.Gt(threshold)

		assert.Equal(t, expr.ExprBinary, result.Type())
		assert.Equal(t, expr.OpGt, result.Op())
		assert.Equal(t, sumExpr, result.Left())
		assert.Equal(t, threshold, result.Right())
		assert.Equal(t, "(sum(col(amount)) > (col(threshold) * lit(2)))", result.String())
	})

	t.Run("string representation validation", func(t *testing.T) {
		// Verify that all aggregation types produce correct string representations
		tests := []struct {
			name     string
			aggExpr  *expr.AggregationExpr
			expected string
		}{
			{"Sum string", expr.Sum(expr.Col("revenue")), "sum(col(revenue))"},
			{"Count string", expr.Count(expr.Col("users")), "count(col(users))"},
			{"Mean string", expr.Mean(expr.Col("score")), "mean(col(score))"},
			{"Min string", expr.Min(expr.Col("age")), "min(col(age))"},
			{"Max string", expr.Max(expr.Col("weight")), "max(col(weight))"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				assert.Equal(t, expr.ExprAggregation, tt.aggExpr.Type())
				assert.Equal(t, tt.expected, tt.aggExpr.String())
			})
		}
	})

	t.Run("method chaining from column to aggregation to comparison", func(t *testing.T) {
		// Test the full chain: expr.Col().Sum().Gt()
		result := expr.Col("price").Sum().Gt(expr.Lit(1000))

		assert.Equal(t, expr.ExprBinary, result.Type())
		assert.Equal(t, expr.OpGt, result.Op())
		assert.Equal(t, "(sum(col(price)) > lit(1000))", result.String())

		// Verify left side is aggregation
		leftExpr, ok := result.Left().(*expr.AggregationExpr)
		assert.True(t, ok)
		assert.Equal(t, expr.AggSum, leftExpr.AggType())
		assert.Equal(t, expr.ExprAggregation, leftExpr.Type())
	})
}
