package sql

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHavingClauseTranslation(t *testing.T) {
	mem := memory.NewGoAllocator()
	translator := NewSQLTranslator(mem)

	// Create test data
	departments := series.New("department",
		[]string{"Engineering", "Sales", "Engineering", "Sales", "Engineering", "HR", "HR", "Sales"},
		mem)
	salaries := series.New("salary", []float64{90000, 75000, 85000, 80000, 95000, 60000, 65000, 70000}, mem)
	employees := series.New("employee",
		[]string{"Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry"}, mem)

	df := dataframe.New(departments, salaries, employees)
	defer df.Release()

	translator.RegisterTable("employees", df)

	testCases := []struct {
		name           string
		query          string
		expectErr      bool
		errContains    string
		validateResult func(*testing.T, *dataframe.DataFrame)
	}{
		{
			name: "HAVING with direct aggregation",
			query: `
				SELECT department, AVG(salary) as avg_salary, COUNT(*) as emp_count
				FROM employees
				GROUP BY department
				HAVING AVG(salary) > 70000
			`,
			expectErr: false,
			validateResult: func(t *testing.T, result *dataframe.DataFrame) {
				// Should have 2 departments: Engineering (avg=90000) and Sales (avg=75000)
				assert.Equal(t, 2, result.Len())

				// Check columns exist
				assert.True(t, result.HasColumn("department"))
				assert.True(t, result.HasColumn("avg_salary"))
				assert.True(t, result.HasColumn("emp_count"))

				// Verify departments
				deptCol, _ := result.Column("department")
				deptArray := deptCol.Array()
				defer deptArray.Release()

				depts := make(map[string]bool)
				for i := 0; i < deptArray.Len(); i++ {
					depts[deptArray.(*array.String).Value(i)] = true
				}

				assert.True(t, depts["Engineering"])
				assert.True(t, depts["Sales"])
				assert.False(t, depts["HR"]) // HR avg salary is 62500, filtered out
			},
		},
		{
			name: "HAVING with alias reference",
			query: `
				SELECT department, SUM(salary) as total_salary
				FROM employees
				GROUP BY department
				HAVING total_salary > 200000
			`,
			expectErr: false,
			validateResult: func(t *testing.T, result *dataframe.DataFrame) {
				// Engineering: 270000, Sales: 225000 (both > 200000)
				// HR: 125000 (filtered out)
				assert.Equal(t, 2, result.Len())

				deptCol, _ := result.Column("department")
				deptArray := deptCol.Array()
				defer deptArray.Release()

				for i := 0; i < deptArray.Len(); i++ {
					dept := deptArray.(*array.String).Value(i)
					assert.True(t, dept == "Engineering" || dept == "Sales")
				}
			},
		},
		{
			name: "HAVING with COUNT aggregation",
			query: `
				SELECT department, COUNT(*) as emp_count
				FROM employees
				GROUP BY department
				HAVING COUNT(*) >= 3
			`,
			expectErr: false,
			validateResult: func(t *testing.T, result *dataframe.DataFrame) {
				// Engineering: 3, Sales: 3 (both >= 3)
				// HR: 2 (filtered out)
				assert.Equal(t, 2, result.Len())
			},
		},
		{
			name: "HAVING with AND condition",
			query: `
				SELECT department, AVG(salary) as avg_salary, COUNT(*) as count
				FROM employees
				GROUP BY department
				HAVING AVG(salary) > 60000 AND COUNT(*) > 2
			`,
			expectErr: false,
			validateResult: func(t *testing.T, result *dataframe.DataFrame) {
				// Engineering: avg=90000, count=3 (passes both)
				// Sales: avg=75000, count=3 (passes both)
				// HR: avg=62500, count=2 (fails count > 2)
				assert.Equal(t, 2, result.Len())
			},
		},
		{
			name: "HAVING with OR condition",
			query: `
				SELECT department, MIN(salary) as min_sal, MAX(salary) as max_sal
				FROM employees
				GROUP BY department
				HAVING MIN(salary) < 65000 OR MAX(salary) > 90000
			`,
			expectErr: false,
			validateResult: func(t *testing.T, result *dataframe.DataFrame) {
				// Engineering: min=85000, max=95000 (passes max > 90000)
				// Sales: min=70000, max=80000 (fails both)
				// HR: min=60000, max=65000 (passes min < 65000)
				assert.Equal(t, 2, result.Len())

				deptCol, _ := result.Column("department")
				deptArray := deptCol.Array()
				defer deptArray.Release()

				depts := make(map[string]bool)
				for i := 0; i < deptArray.Len(); i++ {
					depts[deptArray.(*array.String).Value(i)] = true
				}

				assert.True(t, depts["Engineering"])
				assert.True(t, depts["HR"])
				assert.False(t, depts["Sales"])
			},
		},
		{
			name: "HAVING without GROUP BY - error",
			query: `
				SELECT department
				FROM employees
				HAVING COUNT(*) > 5
			`,
			expectErr:   true,
			errContains: "HAVING clause requires GROUP BY",
		},
		{
			name: "HAVING with non-aggregated column - error",
			query: `
				SELECT department, AVG(salary) as avg_salary
				FROM employees
				GROUP BY department
				HAVING salary > 70000
			`,
			expectErr:   true,
			errContains: "HAVING validation error",
		},
		{
			name: "HAVING with invalid alias - error",
			query: `
				SELECT department, AVG(salary) as avg_salary
				FROM employees
				GROUP BY department
				HAVING total_salary > 200000
			`,
			expectErr:   true,
			errContains: "HAVING validation error",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
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

			require.NoError(t, err)
			require.NotNil(t, lazy)

			// Collect the result
			result, err := lazy.Collect()
			require.NoError(t, err)
			defer result.Release()

			// Validate the result
			if tc.validateResult != nil {
				tc.validateResult(t, result)
			}
		})
	}
}

func TestHavingClauseComplexExpressions(t *testing.T) {
	mem := memory.NewGoAllocator()
	translator := NewSQLTranslator(mem)

	// Create test data
	categories := series.New("category", []string{"A", "B", "A", "B", "C", "C", "A", "B", "C"}, mem)
	values := series.New("value", []int64{10, 20, 30, 40, 50, 60, 70, 80, 90}, mem)
	scores := series.New("score", []float64{1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5}, mem)

	df := dataframe.New(categories, values, scores)
	defer df.Release()

	translator.RegisterTable("data", df)

	testCases := []struct {
		name           string
		query          string
		expectErr      bool
		errContains    string
		validateResult func(*testing.T, *dataframe.DataFrame)
	}{
		{
			name: "HAVING with arithmetic expression",
			query: `
				SELECT category, SUM(value) as total, AVG(score) as avg_score
				FROM data
				GROUP BY category
				HAVING SUM(value) * 2 > 200
			`,
			expectErr: true, // TODO: Arithmetic expressions not yet supported in parser
			errContains: "unsupported expression type",
		},
		{
			name: "HAVING with complex boolean logic",
			query: `
				SELECT category, MIN(value) as min_val, MAX(value) as max_val
				FROM data
				GROUP BY category
				HAVING (MIN(value) < 20 AND MAX(value) > 60) OR (MIN(value) >= 40)
			`,
			expectErr: false,
			validateResult: func(t *testing.T, result *dataframe.DataFrame) {
				// A: min=10, max=70 (passes first condition)
				// B: min=20, max=80 (fails both)
				// C: min=50, max=90 (passes second condition)
				assert.Equal(t, 2, result.Len())

				catCol, _ := result.Column("category")
				catArray := catCol.Array()
				defer catArray.Release()

				cats := make(map[string]bool)
				for i := 0; i < catArray.Len(); i++ {
					cats[catArray.(*array.String).Value(i)] = true
				}

				assert.True(t, cats["A"])
				assert.True(t, cats["C"])
				assert.False(t, cats["B"])
			},
		},
		{
			name: "HAVING with comparison between aggregations",
			query: `
				SELECT category, AVG(value) as avg_val, AVG(score) as avg_score
				FROM data
				GROUP BY category
				HAVING AVG(score) * 10 > AVG(value)
			`,
			expectErr: true, // TODO: Arithmetic expressions with aggregations not yet supported in parser
			errContains: "unsupported expression type",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := ParseSQL(tc.query)
			if tc.expectErr {
				// Check for parsing errors first
				if err != nil {
					require.Error(t, err)
					if tc.errContains != "" {
						assert.Contains(t, err.Error(), tc.errContains)
					}
					return
				}
				// If parsing succeeded, check translation errors
				_, err = translator.TranslateStatement(stmt)
				require.Error(t, err)
				if tc.errContains != "" {
					assert.Contains(t, err.Error(), tc.errContains)
				}
				return
			}

			require.NoError(t, err, "Failed to parse SQL")
			lazy, err := translator.TranslateStatement(stmt)

			require.NoError(t, err)
			require.NotNil(t, lazy)

			// Collect the result
			result, err := lazy.Collect()
			require.NoError(t, err)
			defer result.Release()

			// Validate the result
			if tc.validateResult != nil {
				tc.validateResult(t, result)
			}
		})
	}
}

func BenchmarkHavingClause(b *testing.B) {
	mem := memory.NewGoAllocator()
	translator := NewSQLTranslator(mem)

	// Create larger test data
	size := 10000
	departments := make([]string, size)
	salaries := make([]float64, size)
	deptNames := []string{"Engineering", "Sales", "HR", "Marketing", "Support"}

	for i := 0; i < size; i++ {
		departments[i] = deptNames[i%len(deptNames)]
		salaries[i] = float64(50000 + (i * 1000 % 50000))
	}

	deptSeries := series.New("department", departments, mem)
	salarySeries := series.New("salary", salaries, mem)
	df := dataframe.New(deptSeries, salarySeries)
	defer df.Release()

	translator.RegisterTable("employees", df)

	query := `
		SELECT department, AVG(salary) as avg_salary, COUNT(*) as count
		FROM employees
		GROUP BY department
		HAVING AVG(salary) > 60000 AND COUNT(*) > 100
	`

	stmt, err := ParseSQL(query)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lazy, err := translator.TranslateStatement(stmt)
		if err != nil {
			b.Fatal(err)
		}

		result, err := lazy.Collect()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}
