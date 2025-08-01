package expr_test

import (
	"testing"

	"github.com/paveg/gorilla/internal/expr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewAliasResolver(t *testing.T) {
	t.Run("creates resolver with case-sensitive matching", func(t *testing.T) {
		resolver := expr.NewAliasResolver(false)

		assert.NotNil(t, resolver)
		// Note: Cannot access unexported caseInsensitive field
		assert.Empty(t, resolver.GetAllAvailableAliases())
	})

	t.Run("creates resolver with case-insensitive matching", func(t *testing.T) {
		resolver := expr.NewAliasResolver(true)

		assert.NotNil(t, resolver)
		// Note: Cannot access unexported caseInsensitive field
		assert.Empty(t, resolver.GetAllAvailableAliases())
	})
}

func TestAliasResolver_AddGroupByColumn(t *testing.T) {
	resolver := expr.NewAliasResolver(false)

	t.Run("adds single GROUP BY column", func(t *testing.T) {
		resolver.AddGroupByColumn("department")

		// Should be resolvable by itself
		resolved, exists := resolver.ResolveAlias("department")
		assert.True(t, exists)
		assert.Equal(t, "department", resolved)

		// Should appear in available aliases
		aliases := resolver.GetAllAvailableAliases()
		assert.Contains(t, aliases, "department")
	})

	t.Run("adds multiple GROUP BY columns", func(t *testing.T) {
		multiResolver := expr.NewAliasResolver(false)
		multiResolver.AddGroupByColumn("department")
		multiResolver.AddGroupByColumn("region")
		multiResolver.AddGroupByColumn("category")

		// All should be resolvable
		resolved, exists := multiResolver.ResolveAlias("department")
		assert.True(t, exists)
		assert.Equal(t, "department", resolved)

		resolved, exists = multiResolver.ResolveAlias("region")
		assert.True(t, exists)
		assert.Equal(t, "region", resolved)

		resolved, exists = multiResolver.ResolveAlias("category")
		assert.True(t, exists)
		assert.Equal(t, "category", resolved)

		// All should appear in available aliases
		aliases := multiResolver.GetAllAvailableAliases()
		assert.Contains(t, aliases, "department")
		assert.Contains(t, aliases, "region")
		assert.Contains(t, aliases, "category")
	})
}

func TestAliasResolver_AddAggregation(t *testing.T) {
	resolver := expr.NewAliasResolver(false)

	t.Run("adds aggregation with user-defined alias", func(t *testing.T) {
		sumExpr := expr.Sum(expr.Col("salary")).As("total_salary")

		err := resolver.AddAggregation(sumExpr)
		require.NoError(t, err)

		// Should resolve user alias to the alias name
		resolved, exists := resolver.ResolveAlias("total_salary")
		assert.True(t, exists)
		assert.Equal(t, "total_salary", resolved)

		// Should also resolve default name to the user alias
		resolved, exists = resolver.ResolveAlias("sum_salary")
		assert.True(t, exists)
		assert.Equal(t, "total_salary", resolved)
	})

	t.Run("adds aggregation without user alias (default name)", func(t *testing.T) {
		defaultResolver := expr.NewAliasResolver(false)
		countExpr := expr.Count(expr.Col("employee_id"))

		err := defaultResolver.AddAggregation(countExpr)
		require.NoError(t, err)

		// Should resolve default name
		resolved, exists := defaultResolver.ResolveAlias("count_employee_id")
		assert.True(t, exists)
		assert.Equal(t, "count_employee_id", resolved)
	})

	t.Run("handles different aggregation types", func(t *testing.T) {
		typeResolver := expr.NewAliasResolver(false)

		testCases := []struct {
			expr         *expr.AggregationExpr
			expectedName string
		}{
			{expr.Sum(expr.Col("salary")), "sum_salary"},
			{expr.Count(expr.Col("id")), "count_id"},
			{expr.Mean(expr.Col("age")), "avg_age"},
			{expr.Min(expr.Col("score")), "min_score"},
			{expr.Max(expr.Col("rating")), "max_rating"},
		}

		for _, tc := range testCases {
			err := typeResolver.AddAggregation(tc.expr)
			require.NoError(t, err)

			resolved, exists := typeResolver.ResolveAlias(tc.expectedName)
			assert.True(t, exists)
			assert.Equal(t, tc.expectedName, resolved)
		}
	})

	t.Run("detects alias conflicts", func(t *testing.T) {
		conflictResolver := expr.NewAliasResolver(false)

		// Add first aggregation with alias
		sumExpr1 := expr.Sum(expr.Col("salary")).As("total")
		err := conflictResolver.AddAggregation(sumExpr1)
		require.NoError(t, err)

		// Try to add second aggregation with same alias
		sumExpr2 := expr.Sum(expr.Col("bonus")).As("total")
		err = conflictResolver.AddAggregation(sumExpr2)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "alias conflict")
		assert.Contains(t, err.Error(), "total")
	})
}

func TestAliasResolver_ResolveAlias(t *testing.T) {
	t.Run("resolves various alias types", func(t *testing.T) {
		resolver := expr.NewAliasResolver(false)

		// Add GROUP BY column
		resolver.AddGroupByColumn("department")

		// Add aggregation with user alias
		sumExpr := expr.Sum(expr.Col("salary")).As("total_salary")
		err := resolver.AddAggregation(sumExpr)
		require.NoError(t, err)

		// Add aggregation without alias
		countExpr := expr.Count(expr.Col("employee_id"))
		err = resolver.AddAggregation(countExpr)
		require.NoError(t, err)

		testCases := []struct {
			alias    string
			expected string
			found    bool
		}{
			{"department", "department", true},
			{"total_salary", "total_salary", true},
			{"sum_salary", "total_salary", true}, // Default name maps to user alias
			{"count_employee_id", "count_employee_id", true},
			{"nonexistent", "", false},
		}

		for _, tc := range testCases {
			t.Run("alias_"+tc.alias, func(t *testing.T) {
				resolved, exists := resolver.ResolveAlias(tc.alias)
				assert.Equal(t, tc.found, exists)
				if tc.found {
					assert.Equal(t, tc.expected, resolved)
				}
			})
		}
	})
}

func TestAliasResolver_CaseInsensitive(t *testing.T) {
	t.Run("case-insensitive alias resolution", func(t *testing.T) {
		resolver := expr.NewAliasResolver(true)

		// Add columns with mixed case
		resolver.AddGroupByColumn("Department")
		sumExpr := expr.Sum(expr.Col("salary")).As("Total_Salary")
		err := resolver.AddAggregation(sumExpr)
		require.NoError(t, err)

		testCases := []struct {
			alias    string
			expected string
		}{
			{"department", "Department"},
			{"DEPARTMENT", "Department"},
			{"Department", "Department"},
			{"total_salary", "Total_Salary"},
			{"TOTAL_SALARY", "Total_Salary"},
			{"Total_Salary", "Total_Salary"},
		}

		for _, tc := range testCases {
			t.Run("case_insensitive_"+tc.alias, func(t *testing.T) {
				resolved, exists := resolver.ResolveAlias(tc.alias)
				assert.True(t, exists)
				assert.Equal(t, tc.expected, resolved)
			})
		}
	})

	t.Run("case-sensitive alias resolution", func(t *testing.T) {
		resolver := expr.NewAliasResolver(false)

		resolver.AddGroupByColumn("Department")
		sumExpr := expr.Sum(expr.Col("salary")).As("Total_Salary")
		err := resolver.AddAggregation(sumExpr)
		require.NoError(t, err)

		// Exact case should work
		resolved, exists := resolver.ResolveAlias("Department")
		assert.True(t, exists)
		assert.Equal(t, "Department", resolved)

		resolved, exists = resolver.ResolveAlias("Total_Salary")
		assert.True(t, exists)
		assert.Equal(t, "Total_Salary", resolved)

		// Wrong case should not work
		_, exists = resolver.ResolveAlias("department")
		assert.False(t, exists)

		_, exists = resolver.ResolveAlias("total_salary")
		assert.False(t, exists)
	})
}

func TestAliasResolver_GetAvailableAliases(t *testing.T) {
	resolver := expr.NewAliasResolver(false)

	// Add GROUP BY column
	resolver.AddGroupByColumn("department")

	// Add aggregation with user alias
	sumExpr := expr.Sum(expr.Col("salary")).As("total_salary")
	err := resolver.AddAggregation(sumExpr)
	require.NoError(t, err)

	t.Run("gets aliases for user-defined column", func(t *testing.T) {
		aliases := resolver.GetAvailableAliases("total_salary")

		// Should include both the user alias and default name
		assert.Contains(t, aliases, "total_salary")
		assert.Contains(t, aliases, "sum_salary")
		assert.Len(t, aliases, 2)
	})

	t.Run("gets aliases for GROUP BY column", func(t *testing.T) {
		aliases := resolver.GetAvailableAliases("department")

		// Should include just the original name
		assert.Contains(t, aliases, "department")
		assert.Len(t, aliases, 1)
	})

	t.Run("returns empty for unknown column", func(t *testing.T) {
		aliases := resolver.GetAvailableAliases("unknown")
		assert.Empty(t, aliases)
	})
}

func TestAliasResolver_GetAllAvailableAliases(t *testing.T) {
	resolver := expr.NewAliasResolver(false)

	// Add various types of columns
	resolver.AddGroupByColumn("department")
	resolver.AddGroupByColumn("region")

	sumExpr := expr.Sum(expr.Col("salary")).As("total_salary")
	err := resolver.AddAggregation(sumExpr)
	require.NoError(t, err)

	countExpr := expr.Count(expr.Col("employee_id"))
	err = resolver.AddAggregation(countExpr)
	require.NoError(t, err)

	aliases := resolver.GetAllAvailableAliases()

	// Should include all resolvable aliases
	expectedAliases := []string{
		"department", "region",
		"total_salary", "sum_salary",
		"count_employee_id",
	}

	for _, expected := range expectedAliases {
		assert.Contains(t, aliases, expected, "Missing alias: %s", expected)
	}

	// Should be sorted
	assert.Len(t, aliases, len(expectedAliases))
}

func TestAliasResolver_GetColumnNameFromExpression(t *testing.T) {
	resolver := expr.NewAliasResolver(false)

	// Add aggregations
	sumExpr := expr.Sum(expr.Col("salary")).As("total_salary")
	err := resolver.AddAggregation(sumExpr)
	require.NoError(t, err)

	countExpr := expr.Count(expr.Col("employee_id"))
	err = resolver.AddAggregation(countExpr)
	require.NoError(t, err)

	t.Run("resolves expression with user alias", func(t *testing.T) {
		columnName, exists := resolver.GetColumnNameFromExpression("sum(col(salary))")
		assert.True(t, exists)
		assert.Equal(t, "total_salary", columnName) // Should map to user alias
	})

	t.Run("resolves expression with default name", func(t *testing.T) {
		columnName, exists := resolver.GetColumnNameFromExpression("count(col(employee_id))")
		assert.True(t, exists)
		assert.Equal(t, "count_employee_id", columnName)
	})

	t.Run("returns false for unknown expression", func(t *testing.T) {
		_, exists := resolver.GetColumnNameFromExpression("mean(col(age))")
		assert.False(t, exists)
	})
}

func TestAliasResolver_ValidateAlias(t *testing.T) {
	resolver := expr.NewAliasResolver(false)

	resolver.AddGroupByColumn("department")
	sumExpr := expr.Sum(expr.Col("salary")).As("total_salary")
	err := resolver.AddAggregation(sumExpr)
	require.NoError(t, err)

	t.Run("validates existing aliases", func(t *testing.T) {
		testCases := []string{
			"department",
			"total_salary",
			"sum_salary",
		}

		for _, alias := range testCases {
			validateErr := resolver.ValidateAlias(alias)
			require.NoError(t, validateErr, "Should validate alias: %s", alias)
		}
	})

	t.Run("rejects empty alias", func(t *testing.T) {
		emptyErr := resolver.ValidateAlias("")
		require.Error(t, emptyErr)
		assert.Contains(t, emptyErr.Error(), "cannot be empty")
	})

	t.Run("rejects unknown alias with helpful message", func(t *testing.T) {
		unknownErr := resolver.ValidateAlias("unknown_column")
		require.Error(t, unknownErr)
		assert.Contains(t, unknownErr.Error(), "unknown_column")
		assert.Contains(t, unknownErr.Error(), "not found")
		assert.Contains(t, unknownErr.Error(), "Available aliases:")
		assert.Contains(t, unknownErr.Error(), "department")
		assert.Contains(t, unknownErr.Error(), "total_salary")
	})
}

func TestAliasResolver_DefaultNameGeneration(t *testing.T) {
	resolver := expr.NewAliasResolver(false)

	testCases := []struct {
		name     string
		expr     *expr.AggregationExpr
		expected string
	}{
		{"sum aggregation", expr.Sum(expr.Col("salary")), "sum_salary"},
		{"count aggregation", expr.Count(expr.Col("employee_id")), "count_employee_id"},
		{"mean aggregation", expr.Mean(expr.Col("age")), "avg_age"},
		{"min aggregation", expr.Min(expr.Col("score")), "min_score"},
		{"max aggregation", expr.Max(expr.Col("rating")), "max_rating"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := resolver.AddAggregation(tc.expr)
			require.NoError(t, err)

			resolved, exists := resolver.ResolveAlias(tc.expected)
			assert.True(t, exists)
			assert.Equal(t, tc.expected, resolved)
		})
	}
}

func TestBuildAliasResolver(t *testing.T) {
	t.Run("builds resolver from parameters", func(t *testing.T) {
		groupByColumns := []string{"department", "region"}
		aggregations := []*expr.AggregationExpr{
			expr.Sum(expr.Col("salary")).As("total_salary"),
			expr.Count(expr.Col("employee_id")),
			expr.Mean(expr.Col("age")).As("avg_age"),
		}

		resolver, err := expr.BuildAliasResolver(groupByColumns, aggregations, false)
		require.NoError(t, err)
		assert.NotNil(t, resolver)

		// Test that all expected aliases are available
		testCases := []string{
			"department", "region",
			"total_salary", "sum_salary",
			"count_employee_id",
			"avg_age",
		}

		for _, alias := range testCases {
			_, exists := resolver.ResolveAlias(alias)
			assert.True(t, exists, "Should resolve alias: %s", alias)
		}
	})

	t.Run("returns error for conflicting aliases", func(t *testing.T) {
		groupByColumns := []string{"department"}
		aggregations := []*expr.AggregationExpr{
			expr.Sum(expr.Col("salary")).As("total"),
			expr.Count(expr.Col("employee_id")).As("total"), // Conflict!
		}

		_, err := expr.BuildAliasResolver(groupByColumns, aggregations, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to add aggregation")
		assert.Contains(t, err.Error(), "alias conflict")
	})

	t.Run("handles empty inputs", func(t *testing.T) {
		resolver, err := expr.BuildAliasResolver([]string{}, []*expr.AggregationExpr{}, false)
		require.NoError(t, err)
		assert.NotNil(t, resolver)
		assert.Empty(t, resolver.GetAllAvailableAliases())
	})
}

func TestAliasResolver_Integration(t *testing.T) {
	t.Run("realistic HAVING scenario", func(t *testing.T) {
		// Simulate: SELECT department, SUM(salary) as total_salary, COUNT(*) as emp_count
		//          FROM employees
		//          GROUP BY department
		//          HAVING total_salary > 100000 AND emp_count >= 5

		resolver := expr.NewAliasResolver(false)

		// GROUP BY column
		resolver.AddGroupByColumn("department")

		// Aggregations with user aliases
		sumExpr := expr.Sum(expr.Col("salary")).As("total_salary")
		err := resolver.AddAggregation(sumExpr)
		require.NoError(t, err)

		countExpr := expr.Count(expr.Col("*")).As("emp_count")
		err = resolver.AddAggregation(countExpr)
		require.NoError(t, err)

		// Test that HAVING can reference all expected aliases
		havingAliases := []string{
			"department",   // GROUP BY column
			"total_salary", // User alias
			"sum_salary",   // Default name (also accessible)
			"emp_count",    // User alias
			"count_*",      // Default name (also accessible)
		}

		for _, alias := range havingAliases {
			havingErr := resolver.ValidateAlias(alias)
			require.NoError(t, havingErr, "HAVING should be able to reference: %s", alias)
		}

		// Test that invalid references are rejected
		invalidAliases := []string{
			"employee_name", // Not in GROUP BY or aggregated
			"salary",        // Raw column not in GROUP BY
		}

		for _, alias := range invalidAliases {
			invalidErr := resolver.ValidateAlias(alias)
			require.Error(t, invalidErr, "HAVING should reject invalid reference: %s", alias)
		}
	})
}
