package expr_test

import (
	"testing"

	"github.com/paveg/gorilla/internal/expr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Start with TDD approach - write failing tests first

func TestNewHavingValidator(t *testing.T) {
	t.Run("creates validator with aggregation context", func(t *testing.T) {
		ctx := expr.NewAggregationContext()
		ctx.AddMapping("sum(col(sales))", "sum_sales")

		validator := expr.NewHavingValidator(ctx, []string{"department"})

		assert.NotNil(t, validator)
	})
}

func TestHavingValidator_ValidateExpression_ColumnReferences(t *testing.T) {
	// Setup test context
	ctx := expr.NewAggregationContext()
	ctx.AddMapping("sum(col(sales))", "sum_sales")
	ctx.AddMapping("count(col(id))", "count_id")

	groupByColumns := []string{"department", "region"}
	validator := expr.NewHavingValidator(ctx, groupByColumns)

	t.Run("valid aggregated column reference", func(t *testing.T) {
		// HAVING sum_sales > 1000
		havingExpr := expr.Col("sum_sales").Gt(expr.Lit(1000))

		err := validator.ValidateExpression(havingExpr)
		require.NoError(t, err)
	})

	t.Run("valid GROUP BY column reference", func(t *testing.T) {
		// HAVING department = 'Sales'
		deptExpr := expr.Col("department").Eq(expr.Lit("Sales"))

		err := validator.ValidateExpression(deptExpr)
		require.NoError(t, err)
	})

	t.Run("invalid non-aggregated non-GROUP BY column", func(t *testing.T) {
		// HAVING employee_name = 'John' (not in GROUP BY or aggregated)
		expr := expr.Col("employee_name").Eq(expr.Lit("John"))

		err := validator.ValidateExpression(expr)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "employee_name")
		assert.Contains(t, err.Error(), "not available in HAVING clause")
	})

	t.Run("complex expression with valid references", func(t *testing.T) {
		// HAVING sum_sales > 1000 AND department = 'Sales'
		leftExpr := expr.Col("sum_sales").Gt(expr.Lit(1000))
		rightExpr := expr.Col("department").Eq(expr.Lit("Sales"))
		expr := leftExpr.And(rightExpr)

		err := validator.ValidateExpression(expr)
		require.NoError(t, err)
	})

	t.Run("complex expression with invalid reference", func(t *testing.T) {
		// HAVING sum_sales > 1000 AND employee_name = 'John'
		leftExpr := expr.Col("sum_sales").Gt(expr.Lit(1000))
		rightExpr := expr.Col("employee_name").Eq(expr.Lit("John"))
		expr := leftExpr.And(rightExpr)

		err := validator.ValidateExpression(expr)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "employee_name")
	})
}

func TestHavingValidator_ValidateExpression_AggregationExpressions(t *testing.T) {
	ctx := expr.NewAggregationContext()
	ctx.AddMapping("sum(col(sales))", "sum_sales")

	groupByColumns := []string{"department"}
	validator := expr.NewHavingValidator(ctx, groupByColumns)

	t.Run("aggregation expression in HAVING", func(t *testing.T) {
		// HAVING SUM(sales) > 1000 (direct aggregation reference)
		expr := expr.Sum(expr.Col("sales")).Gt(expr.Lit(1000))

		err := validator.ValidateExpression(expr)
		require.NoError(t, err)
	})

	t.Run("aggregation expression not in context", func(t *testing.T) {
		// HAVING AVG(price) > 100 (not in aggregation context)
		expr := expr.Mean(expr.Col("price")).Gt(expr.Lit(100.0))

		err := validator.ValidateExpression(expr)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "mean(col(price))")
		assert.Contains(t, err.Error(), "not found in aggregation context")
	})
}

func TestHavingValidator_ValidateExpression_NestedExpressions(t *testing.T) {
	ctx := expr.NewAggregationContext()
	ctx.AddMapping("sum(col(sales))", "sum_sales")
	ctx.AddMapping("count(col(id))", "count_id")

	groupByColumns := []string{"department"}
	validator := expr.NewHavingValidator(ctx, groupByColumns)

	t.Run("nested binary expressions", func(t *testing.T) {
		// HAVING (sum_sales > 1000) AND (count_id > 5) AND (department = 'Sales')
		expr1 := expr.Col("sum_sales").Gt(expr.Lit(1000))
		expr2 := expr.Col("count_id").Gt(expr.Lit(5))
		expr3 := expr.Col("department").Eq(expr.Lit("Sales"))
		expr := expr1.And(expr2).And(expr3)

		err := validator.ValidateExpression(expr)
		require.NoError(t, err)
	})

	t.Run("function expressions with valid columns", func(t *testing.T) {
		// HAVING UPPER(department) = 'SALES'
		expr := expr.NewFunction("UPPER", expr.Col("department")).Eq(expr.Lit("SALES"))

		err := validator.ValidateExpression(expr)
		require.NoError(t, err)
	})

	t.Run("function expressions with invalid columns", func(t *testing.T) {
		// HAVING UPPER(employee_name) = 'JOHN'
		expr := expr.NewFunction("UPPER", expr.Col("employee_name")).Eq(expr.Lit("JOHN"))

		err := validator.ValidateExpression(expr)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "employee_name")
	})
}

func TestHavingValidator_ValidateExpression_LiteralExpressions(t *testing.T) {
	ctx := expr.NewAggregationContext()
	validator := expr.NewHavingValidator(ctx, []string{"department"})

	t.Run("literal expressions are always valid", func(t *testing.T) {
		// Literal expressions don't reference columns
		expr := expr.Lit(42)

		err := validator.ValidateExpression(expr)
		require.NoError(t, err)
	})

	t.Run("literal expressions with column comparisons are valid", func(t *testing.T) {
		// HAVING department = 'Sales' (column compared to literal)
		expr := expr.Col("department").Eq(expr.Lit("Sales"))

		err := validator.ValidateExpression(expr)
		require.NoError(t, err)
	})
}

func TestHavingValidator_ErrorMessages(t *testing.T) {
	ctx := expr.NewAggregationContext()
	ctx.AddMapping("sum(col(sales))", "sum_sales")
	ctx.AddMapping("count(col(id))", "count_id")

	groupByColumns := []string{"department", "region"}
	validator := expr.NewHavingValidator(ctx, groupByColumns)

	t.Run("helpful error message for invalid column", func(t *testing.T) {
		expr := expr.Col("employee_name").Eq(expr.Lit("John"))

		err := validator.ValidateExpression(expr)
		require.Error(t, err)

		// Error should be helpful and suggest alternatives
		errMsg := err.Error()
		assert.Contains(t, errMsg, "employee_name")
		assert.Contains(t, errMsg, "not available in HAVING clause")
		assert.Contains(t, errMsg, "Available columns:")
		assert.Contains(t, errMsg, "department")
		assert.Contains(t, errMsg, "region")
		assert.Contains(t, errMsg, "sum_sales")
		assert.Contains(t, errMsg, "count_id")
	})

	t.Run("error message for aggregation not in context", func(t *testing.T) {
		expr := expr.Mean(expr.Col("price")).Gt(expr.Lit(100.0))

		err := validator.ValidateExpression(expr)
		require.Error(t, err)

		errMsg := err.Error()
		assert.Contains(t, errMsg, "mean(col(price))")
		assert.Contains(t, errMsg, "not found in aggregation context")
		assert.Contains(t, errMsg, "Available aggregations:")
		assert.Contains(t, errMsg, "sum(col(sales))")
		assert.Contains(t, errMsg, "count(col(id))")
	})
}

func TestHavingValidator_Integration(t *testing.T) {
	t.Run("realistic HAVING validation scenario", func(t *testing.T) {
		// Simulate: SELECT department, SUM(sales), COUNT(employee_id)
		//          FROM employees
		//          GROUP BY department
		//          HAVING SUM(sales) > 100000 AND COUNT(employee_id) > 10

		// Setup aggregation context
		ctx := expr.NewAggregationContext()
		ctx.AddMapping("sum(col(sales))", "sum_sales")
		ctx.AddMapping("count(col(employee_id))", "count_employee_id")

		// Setup GROUP BY columns
		groupByColumns := []string{"department"}

		validator := expr.NewHavingValidator(ctx, groupByColumns)

		// Valid HAVING expression
		expr1 := expr.Col("sum_sales").Gt(expr.Lit(100000))
		expr2 := expr.Col("count_employee_id").Gt(expr.Lit(10))
		havingExpr := expr1.And(expr2)

		err := validator.ValidateExpression(havingExpr)
		require.NoError(t, err)

		// Invalid HAVING expression (references non-GROUP BY column)
		validPart := expr.Col("sum_sales").Gt(expr.Lit(100000))
		invalidPart := expr.Col("employee_name").Eq(expr.Lit("John")) // Invalid: not in GROUP BY or aggregated
		invalidExpr := validPart.And(invalidPart)

		err = validator.ValidateExpression(invalidExpr)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "employee_name")
	})

	t.Run("validation with empty aggregation context", func(t *testing.T) {
		// GROUP BY without aggregations
		ctx := expr.NewAggregationContext()
		groupByColumns := []string{"department", "region"}

		validator := expr.NewHavingValidator(ctx, groupByColumns)

		// Valid: references GROUP BY column
		validExpr := expr.Col("department").Eq(expr.Lit("Sales"))
		err := validator.ValidateExpression(validExpr)
		require.NoError(t, err)

		// Invalid: references non-GROUP BY column
		invalidExpr := expr.Col("salary").Gt(expr.Lit(50000))
		err = validator.ValidateExpression(invalidExpr)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "salary")
	})
}

func TestHavingValidator_EdgeCases(t *testing.T) {
	t.Run("empty GROUP BY columns and empty aggregation context", func(t *testing.T) {
		ctx := expr.NewAggregationContext()
		validator := expr.NewHavingValidator(ctx, []string{})

		// Any column reference should fail
		expr := expr.Col("any_column").Eq(expr.Lit("value"))
		err := validator.ValidateExpression(expr)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "any_column")
		assert.Contains(t, err.Error(), "No columns available")
	})

	t.Run("nil expressions", func(t *testing.T) {
		ctx := expr.NewAggregationContext()
		validator := expr.NewHavingValidator(ctx, []string{"dept"})

		err := validator.ValidateExpression(nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expression cannot be nil")
	})

	t.Run("case sensitivity in column names", func(t *testing.T) {
		ctx := expr.NewAggregationContext()
		ctx.AddMapping("sum(col(Sales))", "sum_Sales")

		validator := expr.NewHavingValidator(ctx, []string{"Department"})

		// Test exact case match
		validExpr := expr.Col("Department").Eq(expr.Lit("Sales"))
		err := validator.ValidateExpression(validExpr)
		require.NoError(t, err)

		// Test case mismatch
		invalidExpr := expr.Col("department").Eq(expr.Lit("Sales")) // lowercase 'd'
		err = validator.ValidateExpression(invalidExpr)
		require.Error(t, err)
	})
}
