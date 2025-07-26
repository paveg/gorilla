package expr

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewHavingValidatorWithAlias(t *testing.T) {
	t.Run("creates validator with alias resolver", func(t *testing.T) {
		ctx := NewAggregationContext()
		ctx.AddMapping("sum(col(salary))", "sum_salary")

		aliasResolver := NewAliasResolver(false)
		aliasResolver.AddGroupByColumn("department")

		validator := NewHavingValidatorWithAlias(ctx, []string{"department"}, aliasResolver)

		assert.NotNil(t, validator)
		assert.NotNil(t, validator.aliasResolver)
		assert.Equal(t, []string{"department"}, validator.groupByColumns)
	})
}

func TestHavingValidatorWithAlias_ValidateExpression(t *testing.T) {
	// Create test setup
	groupByColumns := []string{"department"}
	aggregations := []*AggregationExpr{
		Sum(Col("salary")).As("total_salary"),
		Count(Col("employee_id")),
		Mean(Col("age")).As("avg_age"),
	}

	validator, err := BuildHavingValidatorWithAlias(groupByColumns, aggregations, false)
	require.NoError(t, err)

	t.Run("validates user-defined aliases", func(t *testing.T) {
		// HAVING total_salary > 100000
		expr := Col("total_salary").Gt(Lit(100000))

		err := validator.ValidateExpression(expr)
		assert.NoError(t, err)
	})

	t.Run("validates default aggregation names", func(t *testing.T) {
		// HAVING count_employee_id > 5
		expr := Col("count_employee_id").Gt(Lit(5))

		err := validator.ValidateExpression(expr)
		assert.NoError(t, err)
	})

	t.Run("validates GROUP BY columns", func(t *testing.T) {
		// HAVING department = 'Sales'
		expr := Col("department").Eq(Lit("Sales"))

		err := validator.ValidateExpression(expr)
		assert.NoError(t, err)
	})

	t.Run("validates both user alias and default name access", func(t *testing.T) {
		// User alias: HAVING total_salary > 100000
		expr1 := Col("total_salary").Gt(Lit(100000))
		err := validator.ValidateExpression(expr1)
		assert.NoError(t, err)

		// Default name: HAVING sum_salary > 100000 (should also work)
		expr2 := Col("sum_salary").Gt(Lit(100000))
		err = validator.ValidateExpression(expr2)
		assert.NoError(t, err)
	})

	t.Run("validates complex expressions with aliases", func(t *testing.T) {
		// HAVING total_salary > 100000 AND avg_age < 40 AND department = 'Engineering'
		expr := Col("total_salary").Gt(Lit(100000)).
			And(Col("avg_age").Lt(Lit(40.0))).
			And(Col("department").Eq(Lit("Engineering")))

		err := validator.ValidateExpression(expr)
		assert.NoError(t, err)
	})

	t.Run("rejects invalid column references with alias suggestions", func(t *testing.T) {
		// HAVING employee_name = 'John' (not in GROUP BY or aggregated)
		expr := Col("employee_name").Eq(Lit("John"))

		err := validator.ValidateExpression(expr)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "employee_name")
		assert.Contains(t, err.Error(), "not found")
		assert.Contains(t, err.Error(), "Available aliases:")
		// Should suggest valid alternatives
		assert.Contains(t, err.Error(), "department")
		assert.Contains(t, err.Error(), "total_salary")
	})

	t.Run("validates direct aggregation expressions", func(t *testing.T) {
		// HAVING SUM(salary) > 100000 (direct aggregation)
		expr := Sum(Col("salary")).Gt(Lit(100000))

		err := validator.ValidateExpression(expr)
		assert.NoError(t, err)
	})
}

func TestHavingValidatorWithAlias_CaseInsensitive(t *testing.T) {
	groupByColumns := []string{"Department"}
	aggregations := []*AggregationExpr{
		Sum(Col("salary")).As("Total_Salary"),
		Count(Col("employee_id")).As("Emp_Count"),
	}

	validator, err := BuildHavingValidatorWithAlias(groupByColumns, aggregations, true)
	require.NoError(t, err)

	testCases := []struct {
		name  string
		alias string
	}{
		{"lowercase department", "department"},
		{"uppercase department", "DEPARTMENT"},
		{"mixed case department", "Department"},
		{"lowercase alias", "total_salary"},
		{"uppercase alias", "TOTAL_SALARY"},
		{"mixed case alias", "Total_Salary"},
		{"default name case variations", "sum_salary"},
		{"default name uppercase", "SUM_SALARY"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			expr := Col(tc.alias).Gt(Lit(0))
			err := validator.ValidateExpression(expr)
			assert.NoError(t, err, "Should validate case-insensitive alias: %s", tc.alias)
		})
	}
}

func TestHavingValidatorWithAlias_BackwardCompatibility(t *testing.T) {
	t.Run("original constructor still works", func(t *testing.T) {
		ctx := NewAggregationContext()
		ctx.AddMapping("sum(col(salary))", "sum_salary")

		validator := NewHavingValidator(ctx, []string{"department"})

		// Should work with original functionality
		err := validator.ValidateExpression(Col("department").Eq(Lit("Sales")))
		assert.NoError(t, err)

		err = validator.ValidateExpression(Col("sum_salary").Gt(Lit(100000)))
		assert.NoError(t, err)

		// Should reject invalid references
		err = validator.ValidateExpression(Col("employee_name").Eq(Lit("John")))
		assert.Error(t, err)
	})
}

func TestHavingValidatorWithAlias_GetAvailableColumns(t *testing.T) {
	groupByColumns := []string{"department", "region"}
	aggregations := []*AggregationExpr{
		Sum(Col("salary")).As("total_salary"),
		Count(Col("employee_id")),
		Mean(Col("age")).As("avg_age"),
	}

	validator, err := BuildHavingValidatorWithAlias(groupByColumns, aggregations, false)
	require.NoError(t, err)

	availableColumns := validator.GetAvailableColumns()

	// Should include all expected aliases
	expectedColumns := []string{
		"department", "region", // GROUP BY columns
		"total_salary", "sum_salary", // User alias + default name
		"count_employee_id",   // Default name only
		"avg_age", "mean_age", // User alias + default name
	}

	for _, expected := range expectedColumns {
		assert.Contains(t, availableColumns, expected, "Missing column: %s", expected)
	}
}

func TestBuildHavingValidatorWithAlias(t *testing.T) {
	t.Run("builds complete validator successfully", func(t *testing.T) {
		groupByColumns := []string{"department", "region"}
		aggregations := []*AggregationExpr{
			Sum(Col("salary")).As("total_salary"),
			Count(Col("employee_id")),
			Mean(Col("age")).As("avg_age"),
		}

		validator, err := BuildHavingValidatorWithAlias(groupByColumns, aggregations, false)
		assert.NoError(t, err)
		assert.NotNil(t, validator)

		// Test that all expected functionality works
		testCases := []string{
			"department", "region",
			"total_salary", "sum_salary",
			"count_employee_id",
			"avg_age", "mean_age",
		}

		for _, alias := range testCases {
			expr := Col(alias).Gt(Lit(0))
			err := validator.ValidateExpression(expr)
			assert.NoError(t, err, "Should validate alias: %s", alias)
		}
	})

	t.Run("returns error for conflicting aliases", func(t *testing.T) {
		groupByColumns := []string{"department"}
		aggregations := []*AggregationExpr{
			Sum(Col("salary")).As("total"),
			Count(Col("employee_id")).As("total"), // Conflict!
		}

		_, err := BuildHavingValidatorWithAlias(groupByColumns, aggregations, false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to build alias resolver")
		assert.Contains(t, err.Error(), "alias conflict")
	})

	t.Run("handles case-insensitive mode", func(t *testing.T) {
		groupByColumns := []string{"Department"}
		aggregations := []*AggregationExpr{
			Sum(Col("salary")).As("Total_Salary"),
		}

		validator, err := BuildHavingValidatorWithAlias(groupByColumns, aggregations, true)
		assert.NoError(t, err)

		// Should work with different case variations
		expr := Col("department").Eq(Lit("Sales"))
		err = validator.ValidateExpression(expr)
		assert.NoError(t, err)

		expr = Col("total_salary").Gt(Lit(100000))
		err = validator.ValidateExpression(expr)
		assert.NoError(t, err)
	})
}

func TestHavingValidatorWithAlias_Integration(t *testing.T) {
	t.Run("comprehensive real-world scenario", func(t *testing.T) {
		// Simulate: SELECT department, region, SUM(salary) as total_salary,
		//                  COUNT(*) as emp_count, AVG(age) as avg_age
		//          FROM employees
		//          WHERE active = true
		//          GROUP BY department, region
		//          HAVING total_salary > 500000
		//             AND emp_count >= 10
		//             AND avg_age BETWEEN 25 AND 50
		//             AND department IN ('Engineering', 'Sales')

		groupByColumns := []string{"department", "region"}
		aggregations := []*AggregationExpr{
			Sum(Col("salary")).As("total_salary"),
			Count(Col("*")).As("emp_count"),
			Mean(Col("age")).As("avg_age"),
		}

		validator, err := BuildHavingValidatorWithAlias(groupByColumns, aggregations, false)
		require.NoError(t, err)

		// Build complex HAVING expression
		havingExpr := Col("total_salary").Gt(Lit(500000)).
			And(Col("emp_count").Ge(Lit(10))).
			And(Col("avg_age").Ge(Lit(25.0)).And(Col("avg_age").Le(Lit(50.0)))).
			And(Col("department").Eq(Lit("Engineering")).Or(Col("department").Eq(Lit("Sales"))))

		err = validator.ValidateExpression(havingExpr)
		assert.NoError(t, err)

		// Test that invalid references are still rejected
		invalidExpr := Col("salary").Gt(Lit(75000)) // Raw column not in GROUP BY
		err = validator.ValidateExpression(invalidExpr)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "salary")
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("mixed alias and default name usage", func(t *testing.T) {
		groupByColumns := []string{"category"}
		aggregations := []*AggregationExpr{
			Sum(Col("revenue")).As("total_revenue"), // User alias
			Count(Col("product_id")),                // Default name: count_product_id
			Mean(Col("price")).As("avg_price"),      // User alias
		}

		validator, err := BuildHavingValidatorWithAlias(groupByColumns, aggregations, false)
		require.NoError(t, err)

		// Mix user aliases and default names in same expression
		mixedExpr := Col("total_revenue").Gt(Lit(1000000)).
			And(Col("count_product_id").Ge(Lit(50))).
			And(Col("avg_price").Ge(Lit(10.0)))

		err = validator.ValidateExpression(mixedExpr)
		assert.NoError(t, err)

		// Also test that default names work alongside user aliases
		defaultExpr := Col("sum_revenue").Gt(Lit(1000000)). // Default name for user-aliased column
									And(Col("mean_price").Ge(Lit(10.0))) // Default name for user-aliased column

		err = validator.ValidateExpression(defaultExpr)
		assert.NoError(t, err)
	})
}
