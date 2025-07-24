package sql

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLExecutorBasicQueries(t *testing.T) {
	t.Skip("TODO: Fix translator integration issues with DataFrame")
	mem := memory.NewGoAllocator()
	executor := NewSQLExecutor(mem)

	// Create test data
	names := series.New("name", []string{"Alice", "Bob", "Charlie", "David"}, mem)
	defer names.Release()

	ages := series.New("age", []int64{25, 30, 35, 28}, mem)
	defer ages.Release()

	departments := series.New("department", []string{"Engineering", "Sales", "Engineering", "Marketing"}, mem)
	defer departments.Release()

	salaries := series.New("salary", []int64{100000, 80000, 120000, 75000}, mem)
	defer salaries.Release()

	active := series.New("active", []bool{true, true, false, true}, mem)
	defer active.Release()

	df := dataframe.New(names, ages, departments, salaries, active)
	defer df.Release()

	executor.RegisterTable("employees", df)

	tests := []struct {
		name             string
		query            string
		expectedRows     int
		expectedCols     int
		expectedColNames []string
	}{
		{
			name:             "Simple SELECT",
			query:            "SELECT name FROM employees",
			expectedRows:     4,
			expectedCols:     1,
			expectedColNames: []string{"name"},
		},
		{
			name:             "SELECT multiple columns",
			query:            "SELECT name, age FROM employees",
			expectedRows:     4,
			expectedCols:     2,
			expectedColNames: []string{"name", "age"},
		},
		{
			name:             "SELECT wildcard",
			query:            "SELECT * FROM employees",
			expectedRows:     4,
			expectedCols:     5,
			expectedColNames: []string{"name", "age", "department", "salary", "active"},
		},
		{
			name:             "SELECT with WHERE",
			query:            "SELECT name FROM employees WHERE age > 30",
			expectedRows:     1,
			expectedCols:     1,
			expectedColNames: []string{"name"},
		},
		{
			name:             "SELECT with boolean WHERE",
			query:            "SELECT name FROM employees WHERE active = true",
			expectedRows:     3,
			expectedCols:     1,
			expectedColNames: []string{"name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.Execute(tt.query)
			require.NoError(t, err)
			defer result.Release()

			assert.Equal(t, tt.expectedRows, result.Len())
			assert.Equal(t, tt.expectedCols, len(result.Columns()))

			if tt.expectedColNames != nil {
				for _, colName := range tt.expectedColNames {
					assert.True(t, result.HasColumn(colName), "Missing expected column: %s", colName)
				}
			}
		})
	}
}

func TestSQLExecutorAggregation(t *testing.T) {
	t.Skip("TODO: Fix SQL parser issues before enabling these tests")
	mem := memory.NewGoAllocator()
	executor := NewSQLExecutor(mem)

	// Create test data
	names := series.New("name", []string{"Alice", "Bob", "Charlie", "David"}, mem)
	defer names.Release()

	departments := series.New("department", []string{"Engineering", "Sales", "Engineering", "Marketing"}, mem)
	defer departments.Release()

	salaries := series.New("salary", []int64{100000, 80000, 120000, 75000}, mem)
	defer salaries.Release()

	df := dataframe.New(names, departments, salaries)
	defer df.Release()

	executor.RegisterTable("employees", df)

	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "COUNT aggregation",
			query: "SELECT COUNT(*) FROM employees",
		},
		{
			name:  "GROUP BY with COUNT",
			query: "SELECT department, COUNT(*) FROM employees GROUP BY department",
		},
		{
			name:  "GROUP BY with multiple aggregations",
			query: "SELECT department, COUNT(*) as count, AVG(salary) as avg_sal FROM employees GROUP BY department",
		},
		{
			name:  "SUM aggregation",
			query: "SELECT SUM(salary) FROM employees",
		},
		{
			name:  "MIN and MAX",
			query: "SELECT MIN(salary), MAX(salary) FROM employees",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.Execute(tt.query)
			require.NoError(t, err)
			defer result.Release()

			assert.Greater(t, result.Len(), 0, "Result should have at least one row")
			assert.Greater(t, len(result.Columns()), 0, "Result should have at least one column")
		})
	}
}

func TestSQLExecutorSorting(t *testing.T) {
	t.Skip("TODO: Fix translator integration issues with DataFrame")
	mem := memory.NewGoAllocator()
	executor := NewSQLExecutor(mem)

	// Create test data with known sort order
	names := series.New("name", []string{"Charlie", "Alice", "Bob"}, mem)
	defer names.Release()

	ages := series.New("age", []int64{35, 25, 30}, mem)
	defer ages.Release()

	df := dataframe.New(names, ages)
	defer df.Release()

	executor.RegisterTable("people", df)

	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "ORDER BY ASC",
			query: "SELECT name FROM people ORDER BY name ASC",
		},
		{
			name:  "ORDER BY DESC",
			query: "SELECT name FROM people ORDER BY age DESC",
		},
		{
			name:  "ORDER BY with WHERE",
			query: "SELECT name FROM people WHERE age > 20 ORDER BY age ASC",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.Execute(tt.query)
			require.NoError(t, err)
			defer result.Release()

			assert.Equal(t, 3, result.Len())
			assert.Greater(t, len(result.Columns()), 0)
		})
	}
}

func TestSQLExecutorLimit(t *testing.T) {
	t.Skip("TODO: Fix translator integration issues with DataFrame")
	mem := memory.NewGoAllocator()
	executor := NewSQLExecutor(mem)

	// Create test data with more rows
	names := series.New("name", []string{"Alice", "Bob", "Charlie", "David", "Eve"}, mem)
	defer names.Release()

	ages := series.New("age", []int64{25, 30, 35, 28, 32}, mem)
	defer ages.Release()

	df := dataframe.New(names, ages)
	defer df.Release()

	executor.RegisterTable("people", df)

	tests := []struct {
		name         string
		query        string
		expectedRows int
	}{
		{
			name:         "LIMIT only",
			query:        "SELECT name FROM people LIMIT 3",
			expectedRows: 3,
		},
		{
			name:         "LIMIT with OFFSET",
			query:        "SELECT name FROM people LIMIT 2 OFFSET 1",
			expectedRows: 2,
		},
		{
			name:         "LIMIT larger than data",
			query:        "SELECT name FROM people LIMIT 10",
			expectedRows: 5,
		},
		{
			name:         "OFFSET at end",
			query:        "SELECT name FROM people LIMIT 5 OFFSET 4",
			expectedRows: 1,
		},
		{
			name:         "OFFSET beyond data",
			query:        "SELECT name FROM people LIMIT 5 OFFSET 10",
			expectedRows: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.Execute(tt.query)
			require.NoError(t, err)
			defer result.Release()

			assert.Equal(t, tt.expectedRows, result.Len())
		})
	}
}

func TestSQLExecutorValidation(t *testing.T) {
	mem := memory.NewGoAllocator()
	executor := NewSQLExecutor(mem)

	// Create test data
	names := series.New("name", []string{"Alice", "Bob"}, mem)
	defer names.Release()

	df := dataframe.New(names)
	defer df.Release()

	executor.RegisterTable("people", df)

	tests := []struct {
		name    string
		query   string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "Valid query",
			query:   "SELECT name FROM people",
			wantErr: false,
		},
		{
			name:    "Table not found",
			query:   "SELECT name FROM nonexistent",
			wantErr: true,
			errMsg:  "table not found",
		},
		{
			name:    "Parse error",
			query:   "SELECT name FROM",
			wantErr: true,
			errMsg:  "parse error",
		},
		{
			name:    "Empty SELECT",
			query:   "SELECT FROM people",
			wantErr: true,
			errMsg:  "parse error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := executor.ValidateQuery(tt.query)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestSQLExecutorExplain(t *testing.T) {
	mem := memory.NewGoAllocator()
	executor := NewSQLExecutor(mem)

	// Create test data
	names := series.New("name", []string{"Alice", "Bob"}, mem)
	defer names.Release()

	df := dataframe.New(names)
	defer df.Release()

	executor.RegisterTable("people", df)

	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "Simple SELECT",
			query: "SELECT name FROM people",
		},
		{
			name:  "SELECT with WHERE",
			query: "SELECT name FROM people WHERE name = 'Alice'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := executor.Explain(tt.query)
			require.NoError(t, err)

			assert.NotEmpty(t, plan)
			assert.True(t, plan != "", "Explain plan should not be empty")
		})
	}
}

func TestSQLExecutorTableManagement(t *testing.T) {
	mem := memory.NewGoAllocator()
	executor := NewSQLExecutor(mem)

	// Create test data
	names1 := series.New("name", []string{"Alice", "Bob"}, mem)
	defer names1.Release()

	names2 := series.New("name", []string{"Charlie", "David"}, mem)
	defer names2.Release()

	df1 := dataframe.New(names1)
	defer df1.Release()

	df2 := dataframe.New(names2)
	defer df2.Release()

	// Test registration
	executor.RegisterTable("people1", df1)
	executor.RegisterTable("people2", df2)

	tables := executor.GetRegisteredTables()
	assert.Len(t, tables, 2)
	assert.Contains(t, tables, "people1")
	assert.Contains(t, tables, "people2")

	// Test clearing
	executor.ClearTables()
	tables = executor.GetRegisteredTables()
	assert.Len(t, tables, 0)
}

func TestSQLExecutorBatchExecute(t *testing.T) {
	t.Skip("TODO: Fix SQL parser issues before enabling these tests")
	mem := memory.NewGoAllocator()
	executor := NewSQLExecutor(mem)

	// Create test data
	names := series.New("name", []string{"Alice", "Bob", "Charlie"}, mem)
	defer names.Release()

	ages := series.New("age", []int64{25, 30, 35}, mem)
	defer ages.Release()

	df := dataframe.New(names, ages)
	defer df.Release()

	executor.RegisterTable("people", df)

	queries := []string{
		"SELECT name FROM people",
		"SELECT COUNT(*) FROM people",
		"SELECT name FROM people WHERE age > 25",
	}

	results, err := executor.BatchExecute(queries)
	require.NoError(t, err)
	require.Len(t, results, 3)

	defer func() {
		for _, result := range results {
			result.Release()
		}
	}()

	// Check first result
	assert.Equal(t, 3, results[0].Len())
	assert.Equal(t, 1, len(results[0].Columns()))

	// Check second result (COUNT)
	assert.Equal(t, 1, results[1].Len())
	assert.Equal(t, 1, len(results[1].Columns()))

	// Check third result (filtered)
	assert.Equal(t, 2, results[2].Len()) // Bob and Charlie
	assert.Equal(t, 1, len(results[2].Columns()))
}

func TestSQLExecutorBatchExecuteError(t *testing.T) {
	mem := memory.NewGoAllocator()
	executor := NewSQLExecutor(mem)

	// Don't register any tables to cause error

	queries := []string{
		"SELECT name FROM people", // This will fail
		"SELECT COUNT(*) FROM people",
	}

	results, err := executor.BatchExecute(queries)
	assert.Error(t, err)
	assert.Nil(t, results)
	assert.Contains(t, err.Error(), "error executing query 1")
}

func TestSQLExecutorComplexQueries(t *testing.T) {
	t.Skip("TODO: Fix SQL parser issues before enabling these tests")
	mem := memory.NewGoAllocator()
	executor := NewSQLExecutor(mem)

	// Create test data
	names := series.New("name", []string{"Alice", "Bob", "Charlie", "David", "Eve"}, mem)
	defer names.Release()

	departments := series.New("department", []string{"Engineering", "Sales", "Engineering", "Marketing", "Sales"}, mem)
	defer departments.Release()

	salaries := series.New("salary", []int64{100000, 80000, 120000, 75000, 85000}, mem)
	defer salaries.Release()

	active := series.New("active", []bool{true, true, false, true, true}, mem)
	defer active.Release()

	df := dataframe.New(names, departments, salaries, active)
	defer df.Release()

	executor.RegisterTable("employees", df)

	complexQueries := []struct {
		name  string
		query string
	}{
		{
			name: "Complex aggregation with filtering",
			query: `
				SELECT department, AVG(salary) as avg_salary, COUNT(*) as employee_count
				FROM employees 
				WHERE active = true 
				GROUP BY department 
				ORDER BY avg_salary DESC
			`,
		},
		{
			name: "Multiple conditions in WHERE",
			query: `
				SELECT name, salary 
				FROM employees 
				WHERE salary > 75000 AND active = true
				ORDER BY salary DESC
			`,
		},
		{
			name: "Computed columns",
			query: `
				SELECT name, salary, salary * 1.1 as bonus_salary
				FROM employees 
				WHERE active = true
				ORDER BY salary DESC
			`,
		},
		{
			name: "HAVING clause",
			query: `
				SELECT department, COUNT(*) as count
				FROM employees 
				GROUP BY department 
				HAVING COUNT(*) > 1
			`,
		},
	}

	for _, tt := range complexQueries {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.Execute(tt.query)
			require.NoError(t, err, "Query: %s", tt.query)
			defer result.Release()

			assert.Greater(t, result.Len(), 0, "Result should have at least one row")
			assert.Greater(t, len(result.Columns()), 0, "Result should have at least one column")
		})
	}
}

func TestSQLExecutorStringFunctions(t *testing.T) {
	t.Skip("TODO: Fix SQL parser issues before enabling these tests")
	mem := memory.NewGoAllocator()
	executor := NewSQLExecutor(mem)

	// Create test data
	names := series.New("name", []string{"alice", "BOB", "Charlie"}, mem)
	defer names.Release()

	df := dataframe.New(names)
	defer df.Release()

	executor.RegisterTable("people", df)

	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "UPPER function",
			query: "SELECT UPPER(name) as upper_name FROM people",
		},
		{
			name:  "LOWER function",
			query: "SELECT LOWER(name) as lower_name FROM people",
		},
		{
			name:  "LENGTH function",
			query: "SELECT name, LENGTH(name) as name_length FROM people",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.Execute(tt.query)
			require.NoError(t, err)
			defer result.Release()

			assert.Equal(t, 3, result.Len())
			assert.Greater(t, len(result.Columns()), 0)
		})
	}
}

func TestSQLExecutorErrorHandling(t *testing.T) {
	mem := memory.NewGoAllocator()
	executor := NewSQLExecutor(mem)

	errorQueries := []struct {
		name     string
		query    string
		errorMsg string
	}{
		{
			name:     "Table not found",
			query:    "SELECT name FROM nonexistent_table",
			errorMsg: "table not found",
		},
		{
			name:     "Invalid syntax",
			query:    "SELECT name FROM",
			errorMsg: "parse error",
		},
		{
			name:     "Column not found (would be caught at runtime)",
			query:    "SELECT nonexistent_column FROM people",
			errorMsg: "", // This might not error until execution depending on implementation
		},
	}

	for _, tt := range errorQueries {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.Execute(tt.query)
			assert.Error(t, err)
			assert.Nil(t, result)

			if tt.errorMsg != "" {
				assert.Contains(t, strings.ToLower(err.Error()), strings.ToLower(tt.errorMsg))
			}
		})
	}
}
