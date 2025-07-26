package expr

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewAliasResolver(t *testing.T) {
	t.Run("creates resolver with case-sensitive matching", func(t *testing.T) {
		resolver := NewAliasResolver(false)

		assert.NotNil(t, resolver)
		assert.False(t, resolver.caseInsensitive)
		assert.Empty(t, resolver.GetAllAvailableAliases())
	})

	t.Run("creates resolver with case-insensitive matching", func(t *testing.T) {
		resolver := NewAliasResolver(true)

		assert.NotNil(t, resolver)
		assert.True(t, resolver.caseInsensitive)
		assert.Empty(t, resolver.GetAllAvailableAliases())
	})
}

func TestAliasResolver_AddGroupByColumn(t *testing.T) {
	resolver := NewAliasResolver(false)

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
		resolver := NewAliasResolver(false)
		resolver.AddGroupByColumn("department")
		resolver.AddGroupByColumn("region")
		resolver.AddGroupByColumn("category")

		// All should be resolvable
		resolved, exists := resolver.ResolveAlias("department")
		assert.True(t, exists)
		assert.Equal(t, "department", resolved)

		resolved, exists = resolver.ResolveAlias("region")
		assert.True(t, exists)
		assert.Equal(t, "region", resolved)

		resolved, exists = resolver.ResolveAlias("category")
		assert.True(t, exists)
		assert.Equal(t, "category", resolved)

		// All should appear in available aliases
		aliases := resolver.GetAllAvailableAliases()
		assert.Contains(t, aliases, "department")
		assert.Contains(t, aliases, "region")
		assert.Contains(t, aliases, "category")
	})
}

func TestAliasResolver_AddAggregation(t *testing.T) {
	resolver := NewAliasResolver(false)

	t.Run("adds aggregation with user-defined alias", func(t *testing.T) {
		sumExpr := Sum(Col("salary")).As("total_salary")

		err := resolver.AddAggregation(sumExpr)
		assert.NoError(t, err)

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
		resolver := NewAliasResolver(false)
		countExpr := Count(Col("employee_id"))

		err := resolver.AddAggregation(countExpr)
		assert.NoError(t, err)

		// Should resolve default name
		resolved, exists := resolver.ResolveAlias("count_employee_id")
		assert.True(t, exists)
		assert.Equal(t, "count_employee_id", resolved)
	})

	t.Run("handles different aggregation types", func(t *testing.T) {
		resolver := NewAliasResolver(false)

		testCases := []struct {
			expr         *AggregationExpr
			expectedName string
		}{
			{Sum(Col("salary")), "sum_salary"},
			{Count(Col("id")), "count_id"},
			{Mean(Col("age")), "mean_age"},
			{Min(Col("score")), "min_score"},
			{Max(Col("rating")), "max_rating"},
		}

		for _, tc := range testCases {
			err := resolver.AddAggregation(tc.expr)
			assert.NoError(t, err)

			resolved, exists := resolver.ResolveAlias(tc.expectedName)
			assert.True(t, exists)
			assert.Equal(t, tc.expectedName, resolved)
		}
	})

	t.Run("detects alias conflicts", func(t *testing.T) {
		resolver := NewAliasResolver(false)

		// Add first aggregation with alias
		sumExpr1 := Sum(Col("salary")).As("total")
		err := resolver.AddAggregation(sumExpr1)
		assert.NoError(t, err)

		// Try to add second aggregation with same alias
		sumExpr2 := Sum(Col("bonus")).As("total")
		err = resolver.AddAggregation(sumExpr2)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "alias conflict")
		assert.Contains(t, err.Error(), "total")
	})
}

func TestAliasResolver_ResolveAlias(t *testing.T) {
	t.Run("resolves various alias types", func(t *testing.T) {
		resolver := NewAliasResolver(false)

		// Add GROUP BY column
		resolver.AddGroupByColumn("department")

		// Add aggregation with user alias
		sumExpr := Sum(Col("salary")).As("total_salary")
		err := resolver.AddAggregation(sumExpr)
		require.NoError(t, err)

		// Add aggregation without alias
		countExpr := Count(Col("employee_id"))
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
		resolver := NewAliasResolver(true)

		// Add columns with mixed case
		resolver.AddGroupByColumn("Department")
		sumExpr := Sum(Col("salary")).As("Total_Salary")
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
		resolver := NewAliasResolver(false)

		resolver.AddGroupByColumn("Department")
		sumExpr := Sum(Col("salary")).As("Total_Salary")
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
	resolver := NewAliasResolver(false)

	// Add GROUP BY column
	resolver.AddGroupByColumn("department")

	// Add aggregation with user alias
	sumExpr := Sum(Col("salary")).As("total_salary")
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
	resolver := NewAliasResolver(false)

	// Add various types of columns
	resolver.AddGroupByColumn("department")
	resolver.AddGroupByColumn("region")

	sumExpr := Sum(Col("salary")).As("total_salary")
	err := resolver.AddAggregation(sumExpr)
	require.NoError(t, err)

	countExpr := Count(Col("employee_id"))
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
	assert.Equal(t, len(expectedAliases), len(aliases))
}

func TestAliasResolver_GetColumnNameFromExpression(t *testing.T) {
	resolver := NewAliasResolver(false)

	// Add aggregations
	sumExpr := Sum(Col("salary")).As("total_salary")
	err := resolver.AddAggregation(sumExpr)
	require.NoError(t, err)

	countExpr := Count(Col("employee_id"))
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
	resolver := NewAliasResolver(false)

	resolver.AddGroupByColumn("department")
	sumExpr := Sum(Col("salary")).As("total_salary")
	err := resolver.AddAggregation(sumExpr)
	require.NoError(t, err)

	t.Run("validates existing aliases", func(t *testing.T) {
		testCases := []string{
			"department",
			"total_salary",
			"sum_salary",
		}

		for _, alias := range testCases {
			err := resolver.ValidateAlias(alias)
			assert.NoError(t, err, "Should validate alias: %s", alias)
		}
	})

	t.Run("rejects empty alias", func(t *testing.T) {
		err := resolver.ValidateAlias("")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be empty")
	})

	t.Run("rejects unknown alias with helpful message", func(t *testing.T) {
		err := resolver.ValidateAlias("unknown_column")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unknown_column")
		assert.Contains(t, err.Error(), "not found")
		assert.Contains(t, err.Error(), "Available aliases:")
		assert.Contains(t, err.Error(), "department")
		assert.Contains(t, err.Error(), "total_salary")
	})
}

func TestAliasResolver_DefaultNameGeneration(t *testing.T) {
	resolver := NewAliasResolver(false)

	testCases := []struct {
		name     string
		expr     *AggregationExpr
		expected string
	}{
		{"sum aggregation", Sum(Col("salary")), "sum_salary"},
		{"count aggregation", Count(Col("employee_id")), "count_employee_id"},
		{"mean aggregation", Mean(Col("age")), "mean_age"},
		{"min aggregation", Min(Col("score")), "min_score"},
		{"max aggregation", Max(Col("rating")), "max_rating"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := resolver.AddAggregation(tc.expr)
			assert.NoError(t, err)

			resolved, exists := resolver.ResolveAlias(tc.expected)
			assert.True(t, exists)
			assert.Equal(t, tc.expected, resolved)
		})
	}
}

func TestBuildAliasResolver(t *testing.T) {
	t.Run("builds resolver from parameters", func(t *testing.T) {
		groupByColumns := []string{"department", "region"}
		aggregations := []*AggregationExpr{
			Sum(Col("salary")).As("total_salary"),
			Count(Col("employee_id")),
			Mean(Col("age")).As("avg_age"),
		}

		resolver, err := BuildAliasResolver(groupByColumns, aggregations, false)
		assert.NoError(t, err)
		assert.NotNil(t, resolver)

		// Test that all expected aliases are available
		testCases := []string{
			"department", "region",
			"total_salary", "sum_salary",
			"count_employee_id",
			"avg_age", "mean_age",
		}

		for _, alias := range testCases {
			_, exists := resolver.ResolveAlias(alias)
			assert.True(t, exists, "Should resolve alias: %s", alias)
		}
	})

	t.Run("returns error for conflicting aliases", func(t *testing.T) {
		groupByColumns := []string{"department"}
		aggregations := []*AggregationExpr{
			Sum(Col("salary")).As("total"),
			Count(Col("employee_id")).As("total"), // Conflict!
		}

		_, err := BuildAliasResolver(groupByColumns, aggregations, false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to add aggregation")
		assert.Contains(t, err.Error(), "alias conflict")
	})

	t.Run("handles empty inputs", func(t *testing.T) {
		resolver, err := BuildAliasResolver([]string{}, []*AggregationExpr{}, false)
		assert.NoError(t, err)
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

		resolver := NewAliasResolver(false)

		// GROUP BY column
		resolver.AddGroupByColumn("department")

		// Aggregations with user aliases
		sumExpr := Sum(Col("salary")).As("total_salary")
		err := resolver.AddAggregation(sumExpr)
		require.NoError(t, err)

		countExpr := Count(Col("*")).As("emp_count")
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
			err := resolver.ValidateAlias(alias)
			assert.NoError(t, err, "HAVING should be able to reference: %s", alias)
		}

		// Test that invalid references are rejected
		invalidAliases := []string{
			"employee_name", // Not in GROUP BY or aggregated
			"salary",        // Raw column not in GROUP BY
		}

		for _, alias := range invalidAliases {
			err := resolver.ValidateAlias(alias)
			assert.Error(t, err, "HAVING should reject invalid reference: %s", alias)
		}
	})
}
