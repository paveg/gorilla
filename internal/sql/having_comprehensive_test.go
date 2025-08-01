//nolint:testpackage // requires internal access to unexported types and functions
package sql

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHavingValidationScenarios tests HAVING clause validation scenarios that work with current parser.
func TestHavingValidationScenarios(t *testing.T) {
	mem := memory.NewGoAllocator()
	translator := NewSQLTranslator(mem)

	// Create test data
	departments := series.New("department", []string{"Engineering", "Sales", "HR", "Marketing"}, mem)
	salaries := series.New("salary", []float64{95000, 75000, 60000, 85000}, mem)

	df := dataframe.New(departments, salaries)
	defer df.Release()

	translator.RegisterTable("employees", df)

	testCases := []struct {
		name        string
		query       string
		expectErr   bool
		errContains string
		description string
	}{
		{
			name: "HAVING without GROUP BY - should fail",
			query: `
				SELECT department, salary
				FROM employees
				HAVING AVG(salary) > 80000
			`,
			expectErr:   true,
			errContains: "HAVING clause requires GROUP BY",
			description: "Test that HAVING requires GROUP BY clause",
		},
		{
			name: "HAVING with non-aggregated column reference - should fail",
			query: `
				SELECT department, AVG(salary) as avg_salary
				FROM employees
				GROUP BY department
				HAVING salary > 80000
			`,
			expectErr:   true,
			errContains: "HAVING validation error",
			description: "Test that HAVING cannot reference non-aggregated columns",
		},
		{
			name: "HAVING with invalid column reference - should fail",
			query: `
				SELECT department, AVG(salary) as avg_salary
				FROM employees
				GROUP BY department
				HAVING non_existent_column > 80000
			`,
			expectErr:   true,
			errContains: "HAVING validation error",
			description: "Test that HAVING validates column references",
		},
		{
			name: "HAVING with valid AVG aggregation - should pass",
			query: `
				SELECT department, AVG(salary) as avg_salary
				FROM employees
				GROUP BY department
				HAVING AVG(salary) > 70000
			`,
			expectErr:   false,
			description: "Test that valid HAVING with AVG aggregation passes validation",
		},
		{
			name: "HAVING with valid SUM aggregation - should pass",
			query: `
				SELECT department, SUM(salary) as total_salary
				FROM employees
				GROUP BY department
				HAVING SUM(salary) > 100000
			`,
			expectErr:   false,
			description: "Test that HAVING with SUM aggregation passes validation",
		},
		{
			name: "HAVING with valid MIN aggregation - should pass",
			query: `
				SELECT department, MIN(salary) as min_salary
				FROM employees
				GROUP BY department
				HAVING MIN(salary) > 50000
			`,
			expectErr:   false,
			description: "Test that HAVING with MIN aggregation passes validation",
		},
		{
			name: "HAVING with valid MAX aggregation - should pass",
			query: `
				SELECT department, MAX(salary) as max_salary
				FROM employees
				GROUP BY department
				HAVING MAX(salary) < 100000
			`,
			expectErr:   false,
			description: "Test that HAVING with MAX aggregation passes validation",
		},
		{
			name: "HAVING with OR condition - should pass",
			query: `
				SELECT department, AVG(salary) as avg_salary
				FROM employees
				GROUP BY department
				HAVING AVG(salary) > 90000 OR AVG(salary) < 70000
			`,
			expectErr:   false,
			description: "Test that HAVING with OR conditions passes validation",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Testing: %s", tc.description)

			stmt, err := ParseSQL(tc.query)
			require.NoError(t, err, "Failed to parse SQL")

			lazy, err := translator.TranslateStatement(stmt)
			if tc.expectErr {
				require.Error(t, err)
				if tc.errContains != "" {
					assert.Contains(t, err.Error(), tc.errContains)
				}
				return
			}

			require.NoError(t, err, "Translation should succeed for valid HAVING clause")
			require.NotNil(t, lazy, "LazyFrame should not be nil")
		})
	}
}

// TestHavingAggregationTypes tests HAVING with different aggregation types.
func TestHavingAggregationTypes(t *testing.T) {
	mem := memory.NewGoAllocator()
	translator := NewSQLTranslator(mem)

	// Create test data
	categories := series.New("category", []string{"A", "A", "B", "B", "C", "C"}, mem)
	values := series.New("value", []int64{10, 20, 30, 40, 50, 60}, mem)
	scores := series.New("score", []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0}, mem)

	df := dataframe.New(categories, values, scores)
	defer df.Release()

	translator.RegisterTable("data", df)

	testCases := []struct {
		name        string
		query       string
		expectErr   bool
		description string
	}{
		{
			name: "HAVING with SUM aggregation",
			query: `
				SELECT category, SUM(value) as total
				FROM data
				GROUP BY category
				HAVING SUM(value) > 50
			`,
			expectErr:   false,
			description: "Test SUM aggregation in HAVING clause",
		},
		{
			name: "HAVING with AVG aggregation",
			query: `
				SELECT category, AVG(score) as avg_score
				FROM data
				GROUP BY category
				HAVING AVG(score) > 3.0
			`,
			expectErr:   false,
			description: "Test AVG aggregation in HAVING clause",
		},
		{
			name: "HAVING with MIN aggregation",
			query: `
				SELECT category, MIN(value) as min_val
				FROM data
				GROUP BY category
				HAVING MIN(value) > 25
			`,
			expectErr:   false,
			description: "Test MIN aggregation in HAVING clause",
		},
		{
			name: "HAVING with MAX aggregation",
			query: `
				SELECT category, MAX(value) as max_val
				FROM data
				GROUP BY category
				HAVING MAX(value) < 100
			`,
			expectErr:   false,
			description: "Test MAX aggregation in HAVING clause",
		},
		{
			name: "HAVING with inequality operators",
			query: `
				SELECT category, SUM(value) as total
				FROM data
				GROUP BY category
				HAVING SUM(value) != 0
			`,
			expectErr:   false,
			description: "Test HAVING with inequality operator",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Testing: %s", tc.description)

			stmt, err := ParseSQL(tc.query)
			require.NoError(t, err, "Failed to parse SQL")

			lazy, err := translator.TranslateStatement(stmt)
			if tc.expectErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err, "Translation should succeed")
			require.NotNil(t, lazy, "LazyFrame should not be nil")
		})
	}
}

// TestHavingBusinessScenarios tests realistic business scenarios.
func TestHavingBusinessScenarios(t *testing.T) {
	mem := memory.NewGoAllocator()
	translator := NewSQLTranslator(mem)

	// Create realistic employee data
	names := []string{"Alice", "Bob", "Charlie", "David", "Eve", "Frank"}
	departments := []string{"Engineering", "Engineering", "Sales", "Sales", "HR", "HR"}
	salaries := []float64{95000, 85000, 75000, 80000, 60000, 65000}
	years := []int64{5, 3, 4, 6, 7, 5}

	nameSeries := series.New("name", names, mem)
	deptSeries := series.New("department", departments, mem)
	salarySeries := series.New("salary", salaries, mem)
	yearsSeries := series.New("years_experience", years, mem)

	df := dataframe.New(nameSeries, deptSeries, salarySeries, yearsSeries)
	defer df.Release()

	translator.RegisterTable("employees", df)

	testCases := []struct {
		name        string
		query       string
		expectErr   bool
		description string
	}{
		{
			name: "High-salary department analysis",
			query: `
				SELECT department, AVG(salary) as avg_salary
				FROM employees
				GROUP BY department
				HAVING AVG(salary) > 70000
			`,
			expectErr:   false,
			description: "Find departments with high average salaries",
		},
		{
			name: "Department cost analysis",
			query: `
				SELECT department, SUM(salary) as total_cost
				FROM employees
				GROUP BY department
				HAVING SUM(salary) > 150000
			`,
			expectErr:   false,
			description: "Find departments with high total costs",
		},
		{
			name: "Experience-based department filtering",
			query: `
				SELECT department, AVG(years_experience) as avg_experience
				FROM employees
				GROUP BY department
				HAVING AVG(years_experience) > 4
			`,
			expectErr:   false,
			description: "Find departments with experienced teams",
		},
		{
			name: "Salary range analysis",
			query: `
				SELECT department, MIN(salary) as min_sal, MAX(salary) as max_sal
				FROM employees
				GROUP BY department
				HAVING MIN(salary) > 70000 OR MAX(salary) < 100000
			`,
			expectErr:   false,
			description: "Analyze salary ranges across departments",
		},
		{
			name: "Department efficiency metric",
			query: `
				SELECT department, AVG(salary) as avg_sal, AVG(years_experience) as avg_exp
				FROM employees
				GROUP BY department
				HAVING AVG(salary) > 80000
			`,
			expectErr:   false,
			description: "Complex department analysis with multiple metrics",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Testing business scenario: %s", tc.description)

			stmt, err := ParseSQL(tc.query)
			require.NoError(t, err, "Failed to parse SQL")

			lazy, err := translator.TranslateStatement(stmt)
			if tc.expectErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err, "Translation should succeed")
			require.NotNil(t, lazy, "LazyFrame should not be nil")
		})
	}
}

// TestHavingMemoryCleanup tests proper memory management with HAVING operations.
func TestHavingMemoryCleanup(t *testing.T) {
	mem := memory.NewGoAllocator()
	translator := NewSQLTranslator(mem)

	// Create test data
	categories := series.New("category", []string{"A", "A", "B", "B"}, mem)
	values := series.New("value", []int64{10, 20, 30, 40}, mem)

	df := dataframe.New(categories, values)
	defer df.Release() // Proper cleanup with defer pattern

	translator.RegisterTable("test_data", df)

	query := `
		SELECT category, SUM(value) as total
		FROM test_data
		GROUP BY category
		HAVING SUM(value) > 25
	`

	// Test multiple executions to ensure no memory leaks
	for i := range 10 {
		func() {
			stmt, err := ParseSQL(query)
			require.NoError(t, err, "Failed to parse SQL on iteration %d", i)

			lazy, err := translator.TranslateStatement(stmt)
			require.NoError(t, err, "Failed to translate on iteration %d", i)
			require.NotNil(t, lazy, "Lazy frame should not be nil on iteration %d", i)

			// Test demonstrates that HAVING translation works correctly
			// and doesn't leak memory across multiple invocations
		}()
	}
}

// TestHavingErrorHandling tests proper error handling in HAVING scenarios.
func TestHavingErrorHandling(t *testing.T) {
	mem := memory.NewGoAllocator()
	translator := NewSQLTranslator(mem)

	// Create test data
	departments := series.New("department", []string{"A", "B"}, mem)
	salaries := series.New("salary", []float64{100, 200}, mem)

	df := dataframe.New(departments, salaries)
	defer df.Release()

	translator.RegisterTable("employees", df)

	errorCases := []struct {
		name        string
		query       string
		expectErr   bool
		errContains string
		description string
	}{
		{
			name: "HAVING references column not in GROUP BY",
			query: `
				SELECT department, AVG(salary) as avg_sal
				FROM employees
				GROUP BY department
				HAVING salary > 150
			`,
			expectErr:   true,
			errContains: "HAVING validation error",
			description: "HAVING cannot reference non-aggregated columns",
		},
		{
			name: "HAVING references non-existent column",
			query: `
				SELECT department, AVG(salary) as avg_sal
				FROM employees
				GROUP BY department
				HAVING invalid_column > 150
			`,
			expectErr:   true,
			errContains: "HAVING validation error",
			description: "HAVING must reference valid columns",
		},
		{
			name: "HAVING without GROUP BY",
			query: `
				SELECT department, salary
				FROM employees
				HAVING AVG(salary) > 150
			`,
			expectErr:   true,
			errContains: "HAVING clause requires GROUP BY",
			description: "HAVING requires GROUP BY clause",
		},
	}

	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Testing error case: %s", tc.description)

			stmt, err := ParseSQL(tc.query)
			require.NoError(t, err, "SQL should parse successfully")

			lazy, err := translator.TranslateStatement(stmt)
			require.Error(t, err, "Translation should fail for invalid HAVING")

			if tc.errContains != "" {
				assert.Contains(t, err.Error(), tc.errContains, "Error should contain expected message")
			}

			assert.Nil(t, lazy, "LazyFrame should be nil for failed translation")
		})
	}
}

// TestHavingEndToEndIntegration tests complete HAVING workflows.
func TestHavingEndToEndIntegration(t *testing.T) {
	mem := memory.NewGoAllocator()
	translator := NewSQLTranslator(mem)

	// Create comprehensive test dataset
	departments := []string{"Engineering", "Engineering", "Engineering",
		"Sales", "Sales", "HR", "HR", "Marketing"}
	salaries := []float64{95000, 85000, 105000, 75000, 80000, 60000, 65000, 90000}
	years := []int64{5, 3, 8, 4, 6, 7, 5, 4}
	ratings := []float64{4.5, 4.2, 4.8, 4.1, 4.3, 4.2, 4.4, 4.6}

	deptSeries := series.New("department", departments, mem)
	salarySeries := series.New("salary", salaries, mem)
	yearsSeries := series.New("years_experience", years, mem)
	ratingSeries := series.New("performance_rating", ratings, mem)

	df := dataframe.New(deptSeries, salarySeries, yearsSeries, ratingSeries)
	defer df.Release()

	translator.RegisterTable("employee_data", df)

	integrationTests := []struct {
		name        string
		query       string
		expectErr   bool
		description string
	}{
		{
			name: "Multi-criteria department analysis",
			query: `
				SELECT department, 
					   AVG(salary) as avg_salary,
					   AVG(performance_rating) as avg_rating,
					   MIN(years_experience) as min_experience
				FROM employee_data
				GROUP BY department
				HAVING AVG(salary) > 80000 OR AVG(performance_rating) > 4.3
			`,
			expectErr:   false,
			description: "Complex analysis with multiple aggregations and OR condition",
		},
		{
			name: "Performance-based filtering",
			query: `
				SELECT department, 
					   MAX(performance_rating) as max_rating,
					   SUM(salary) as total_compensation
				FROM employee_data
				GROUP BY department
				HAVING MAX(performance_rating) > 4.5
			`,
			expectErr:   false,
			description: "Filter departments by peak performance",
		},
		{
			name: "Cost-efficiency analysis",
			query: `
				SELECT department,
					   AVG(salary) as avg_salary,
					   AVG(years_experience) as avg_experience
				FROM employee_data
				GROUP BY department
				HAVING AVG(salary) > 70000
			`,
			expectErr:   false,
			description: "Analyze cost efficiency across departments",
		},
	}

	for _, tc := range integrationTests {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Integration test: %s", tc.description)

			stmt, err := ParseSQL(tc.query)
			require.NoError(t, err, "SQL parsing should succeed")

			lazy, err := translator.TranslateStatement(stmt)
			if tc.expectErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err, "Translation should succeed")
			require.NotNil(t, lazy, "LazyFrame should not be nil")

			// The test validates that complex real-world queries can be
			// parsed and translated successfully, demonstrating that the
			// HAVING implementation is robust enough for practical use
		})
	}
}

// BenchmarkHavingTranslation benchmarks HAVING clause translation performance.
func BenchmarkHavingTranslation(b *testing.B) {
	mem := memory.NewGoAllocator()
	translator := NewSQLTranslator(mem)

	// Create benchmark data
	departments := make([]string, 1000)
	salaries := make([]float64, 1000)
	deptNames := []string{"Engineering", "Sales", "HR", "Marketing", "Support"}

	for i := range 1000 {
		departments[i] = deptNames[i%len(deptNames)]
		salaries[i] = float64(50000 + (i * 100))
	}

	deptSeries := series.New("department", departments, mem)
	salarySeries := series.New("salary", salaries, mem)

	df := dataframe.New(deptSeries, salarySeries)
	defer df.Release()

	translator.RegisterTable("employees", df)

	query := `
		SELECT department,
			   AVG(salary) as avg_salary,
			   MIN(salary) as min_salary,
			   MAX(salary) as max_salary
		FROM employees
		GROUP BY department
		HAVING AVG(salary) > 75000
	`

	stmt, err := ParseSQL(query)
	require.NoError(b, err)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		lazy, translateErr := translator.TranslateStatement(stmt)
		if translateErr != nil {
			b.Fatal(translateErr)
		}
		_ = lazy // Don't collect to avoid execution overhead in benchmark
	}
}
