package expr_test

import (
	"testing"

	"github.com/paveg/gorilla/internal/expr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewHavingValidatorWithAlias(t *testing.T) {
	t.Run("creates validator with alias resolver", func(t *testing.T) {
		ctx := expr.NewAggregationContext()
		ctx.AddMapping("sum(col(salary))", "sum_salary")

		aliasResolver := expr.NewAliasResolver(false)
		aliasResolver.AddGroupByColumn("department")

		validator := expr.NewHavingValidatorWithAlias(ctx, []string{"department"}, aliasResolver)

		assert.NotNil(t, validator)
	})
}

func TestHavingValidatorWithAlias_ValidateExpression(t *testing.T) {
	// Create test setup
	groupByColumns := []string{"department"}
	aggregations := []*expr.AggregationExpr{
		expr.Sum(expr.Col("salary")).As("total_salary"),
		expr.Count(expr.Col("employee_id")),
		expr.Mean(expr.Col("age")).As("avg_age"),
	}

	validator, err := expr.BuildHavingValidatorWithAlias(groupByColumns, aggregations, false)
	require.NoError(t, err)

	t.Run("validates user-defined aliases", func(t *testing.T) {
		// HAVING total_salary > 100000
		expr := expr.Col("total_salary").Gt(expr.Lit(100000))

		validateErr := validator.ValidateExpression(expr)
		require.NoError(t, validateErr)
	})

	t.Run("validates default aggregation names", func(t *testing.T) {
		// HAVING count_employee_id > 5
		expr := expr.Col("count_employee_id").Gt(expr.Lit(5))

		defaultErr := validator.ValidateExpression(expr)
		require.NoError(t, defaultErr)
	})

	t.Run("validates GROUP BY columns", func(t *testing.T) {
		// HAVING department = 'Sales'
		expr := expr.Col("department").Eq(expr.Lit("Sales"))

		groupByErr := validator.ValidateExpression(expr)
		require.NoError(t, groupByErr)
	})

	t.Run("validates both user alias and default name access", func(t *testing.T) {
		// User alias: HAVING total_salary > 100000
		expr1 := expr.Col("total_salary").Gt(expr.Lit(100000))
		aliasErr := validator.ValidateExpression(expr1)
		require.NoError(t, aliasErr)

		// Default name: HAVING sum_salary > 100000 (should also work)
		expr2 := expr.Col("sum_salary").Gt(expr.Lit(100000))
		defaultNameErr := validator.ValidateExpression(expr2)
		require.NoError(t, defaultNameErr)
	})

	t.Run("validates complex expressions with aliases", func(t *testing.T) {
		// HAVING total_salary > 100000 AND avg_age < 40 AND department = 'Engineering'
		expr := expr.Col("total_salary").Gt(expr.Lit(100000)).
			And(expr.Col("avg_age").Lt(expr.Lit(40.0))).
			And(expr.Col("department").Eq(expr.Lit("Engineering")))

		complexErr := validator.ValidateExpression(expr)
		require.NoError(t, complexErr)
	})

	t.Run("rejects invalid column references with alias suggestions", func(t *testing.T) {
		// HAVING employee_name = 'John' (not in GROUP BY or aggregated)
		expr := expr.Col("employee_name").Eq(expr.Lit("John"))

		invalidErr := validator.ValidateExpression(expr)
		require.Error(t, invalidErr)
		assert.Contains(t, invalidErr.Error(), "employee_name")
		assert.Contains(t, invalidErr.Error(), "not found")
		assert.Contains(t, invalidErr.Error(), "Available aliases:")
		// Should suggest valid alternatives
		assert.Contains(t, invalidErr.Error(), "department")
		assert.Contains(t, invalidErr.Error(), "total_salary")
	})

	t.Run("validates direct aggregation expressions", func(t *testing.T) {
		// HAVING SUM(salary) > 100000 (direct aggregation)
		expr := expr.Sum(expr.Col("salary")).Gt(expr.Lit(100000))

		directErr := validator.ValidateExpression(expr)
		require.NoError(t, directErr)
	})
}

func TestHavingValidatorWithAlias_CaseInsensitive(t *testing.T) {
	groupByColumns := []string{"Department"}
	aggregations := []*expr.AggregationExpr{
		expr.Sum(expr.Col("salary")).As("Total_Salary"),
		expr.Count(expr.Col("employee_id")).As("Emp_Count"),
	}

	validator, err := expr.BuildHavingValidatorWithAlias(groupByColumns, aggregations, true)
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
			expr := expr.Col(tc.alias).Gt(expr.Lit(0))
			caseErr := validator.ValidateExpression(expr)
			require.NoError(t, caseErr, "Should validate case-insensitive alias: %s", tc.alias)
		})
	}
}

func TestHavingValidatorWithAlias_BackwardCompatibility(t *testing.T) {
	t.Run("original constructor still works", func(t *testing.T) {
		ctx := expr.NewAggregationContext()
		ctx.AddMapping("sum(col(salary))", "sum_salary")

		validator := expr.NewHavingValidator(ctx, []string{"department"})

		// Should work with original functionality
		err := validator.ValidateExpression(expr.Col("department").Eq(expr.Lit("Sales")))
		require.NoError(t, err)

		err = validator.ValidateExpression(expr.Col("sum_salary").Gt(expr.Lit(100000)))
		require.NoError(t, err)

		// Should reject invalid references
		err = validator.ValidateExpression(expr.Col("employee_name").Eq(expr.Lit("John")))
		require.Error(t, err)
	})
}

func TestHavingValidatorWithAlias_GetAvailableColumns(t *testing.T) {
	groupByColumns := []string{"department", "region"}
	aggregations := []*expr.AggregationExpr{
		expr.Sum(expr.Col("salary")).As("total_salary"),
		expr.Count(expr.Col("employee_id")),
		expr.Mean(expr.Col("age")).As("avg_age"),
	}

	validator, err := expr.BuildHavingValidatorWithAlias(groupByColumns, aggregations, false)
	require.NoError(t, err)

	availableColumns := validator.GetAvailableColumns()

	// Should include all expected aliases
	expectedColumns := []string{
		"department", "region", // GROUP BY columns
		"total_salary", "sum_salary", // User alias + default name
		"count_employee_id", // Default name only
		"avg_age",           // User alias
	}

	for _, expected := range expectedColumns {
		assert.Contains(t, availableColumns, expected, "Missing column: %s", expected)
	}
}

func TestBuildHavingValidatorWithAlias(t *testing.T) {
	t.Run("builds complete validator successfully", func(t *testing.T) {
		groupByColumns := []string{"department", "region"}
		aggregations := []*expr.AggregationExpr{
			expr.Sum(expr.Col("salary")).As("total_salary"),
			expr.Count(expr.Col("employee_id")),
			expr.Mean(expr.Col("age")).As("avg_age"),
		}

		validator, err := expr.BuildHavingValidatorWithAlias(groupByColumns, aggregations, false)
		require.NoError(t, err)
		assert.NotNil(t, validator)

		// Test that all expected functionality works
		testCases := []string{
			"department", "region",
			"total_salary", "sum_salary",
			"count_employee_id",
			"avg_age",
		}

		for _, alias := range testCases {
			expr := expr.Col(alias).Gt(expr.Lit(0))
			backwardErr := validator.ValidateExpression(expr)
			require.NoError(t, backwardErr, "Should validate alias: %s", alias)
		}
	})

	t.Run("returns error for conflicting aliases", func(t *testing.T) {
		groupByColumns := []string{"department"}
		aggregations := []*expr.AggregationExpr{
			expr.Sum(expr.Col("salary")).As("total"),
			expr.Count(expr.Col("employee_id")).As("total"), // Conflict!
		}

		_, err := expr.BuildHavingValidatorWithAlias(groupByColumns, aggregations, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to build alias resolver")
		assert.Contains(t, err.Error(), "alias conflict")
	})

	t.Run("handles case-insensitive mode", func(t *testing.T) {
		groupByColumns := []string{"Department"}
		aggregations := []*expr.AggregationExpr{
			expr.Sum(expr.Col("salary")).As("Total_Salary"),
		}

		validator, err := expr.BuildHavingValidatorWithAlias(groupByColumns, aggregations, true)
		require.NoError(t, err)

		// Should work with different case variations
		deptExpr := expr.Col("department").Eq(expr.Lit("Sales"))
		err = validator.ValidateExpression(deptExpr)
		require.NoError(t, err)

		salaryExpr := expr.Col("total_salary").Gt(expr.Lit(100000))
		err = validator.ValidateExpression(salaryExpr)
		require.NoError(t, err)
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
		aggregations := []*expr.AggregationExpr{
			expr.Sum(expr.Col("salary")).As("total_salary"),
			expr.Count(expr.Col("*")).As("emp_count"),
			expr.Mean(expr.Col("age")).As("avg_age"),
		}

		validator, err := expr.BuildHavingValidatorWithAlias(groupByColumns, aggregations, false)
		require.NoError(t, err)

		// Build complex HAVING expression
		havingExpr := expr.Col("total_salary").Gt(expr.Lit(500000)).
			And(expr.Col("emp_count").Ge(expr.Lit(10))).
			And(expr.Col("avg_age").Ge(expr.Lit(25.0)).And(expr.Col("avg_age").Le(expr.Lit(50.0)))).
			And(expr.Col("department").Eq(expr.Lit("Engineering")).Or(expr.Col("department").Eq(expr.Lit("Sales"))))

		err = validator.ValidateExpression(havingExpr)
		require.NoError(t, err)

		// Test that invalid references are still rejected
		invalidExpr := expr.Col("salary").Gt(expr.Lit(75000)) // Raw column not in GROUP BY
		err = validator.ValidateExpression(invalidExpr)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "salary")
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("mixed alias and default name usage", func(t *testing.T) {
		groupByColumns := []string{"category"}
		aggregations := []*expr.AggregationExpr{
			expr.Sum(expr.Col("revenue")).As("total_revenue"), // User alias
			expr.Count(expr.Col("product_id")),                // Default name: count_product_id
			expr.Mean(expr.Col("price")).As("avg_price"),      // User alias
		}

		validator, err := expr.BuildHavingValidatorWithAlias(groupByColumns, aggregations, false)
		require.NoError(t, err)

		// Mix user aliases and default names in same expression
		mixedExpr := expr.Col("total_revenue").Gt(expr.Lit(1000000)).
			And(expr.Col("count_product_id").Ge(expr.Lit(50))).
			And(expr.Col("avg_price").Ge(expr.Lit(10.0)))

		err = validator.ValidateExpression(mixedExpr)
		require.NoError(t, err)

		// Also test that default names work alongside user aliases
		defaultExpr := expr.Col("sum_revenue").Gt(expr.Lit(1000000)). // Default name for user-aliased column
										And(expr.Col("avg_price").Ge(expr.Lit(10.0)))
			// Default name for user-aliased column

		err = validator.ValidateExpression(defaultExpr)
		require.NoError(t, err)
	})
}
