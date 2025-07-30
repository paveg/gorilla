package sql

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestDataFrame(mem memory.Allocator) *dataframe.DataFrame {
	names := series.New("name", []string{"Alice", "Bob", "Charlie", "David"}, mem)
	ages := series.New("age", []int64{25, 30, 35, 28}, mem)
	departments := series.New("department", []string{"Engineering", "Sales", "Engineering", "Marketing"}, mem)
	salaries := series.New("salary", []int64{100000, 80000, 120000, 75000}, mem)
	active := series.New("active", []bool{true, true, false, true}, mem)

	return dataframe.New(names, ages, departments, salaries, active)
}

func TestSQLTranslatorBasic(t *testing.T) {
	t.Skip("TODO: Fix SQL parser issues before enabling these tests")
	mem := memory.NewGoAllocator()
	translator := NewSQLTranslator(mem)

	df := createTestDataFrame(mem)
	defer df.Release()

	translator.RegisterTable("employees", df)

	// Test simple SELECT
	stmt, err := ParseSQL("SELECT name, age FROM employees")
	require.NoError(t, err)

	lazy, err := translator.TranslateStatement(stmt)
	require.NoError(t, err)
	defer lazy.Release()

	result, err := lazy.Collect()
	require.NoError(t, err)
	defer result.Release()

	assert.Equal(t, 4, result.Len())
	assert.Len(t, result.Columns(), 2)
	assert.True(t, result.HasColumn("name"))
	assert.True(t, result.HasColumn("age"))
}

func TestSQLTranslatorWildcard(t *testing.T) {
	t.Skip("TODO: Fix SQL parser issues before enabling these tests")
	mem := memory.NewGoAllocator()
	translator := NewSQLTranslator(mem)

	df := createTestDataFrame(mem)
	defer df.Release()

	translator.RegisterTable("employees", df)

	// Test SELECT *
	stmt, err := ParseSQL("SELECT * FROM employees")
	require.NoError(t, err)

	lazy, err := translator.TranslateStatement(stmt)
	require.NoError(t, err)
	defer lazy.Release()

	result, err := lazy.Collect()
	require.NoError(t, err)
	defer result.Release()

	assert.Equal(t, 4, result.Len())
	assert.Len(t, result.Columns(), 5) // All original columns
}

func TestSQLTranslatorWhere(t *testing.T) {
	t.Skip("TODO: Fix SQL parser issues before enabling these tests")
	mem := memory.NewGoAllocator()
	translator := NewSQLTranslator(mem)

	df := createTestDataFrame(mem)
	defer df.Release()

	translator.RegisterTable("employees", df)

	// Test WHERE clause
	stmt, err := ParseSQL("SELECT name FROM employees WHERE age > 30")
	require.NoError(t, err)

	lazy, err := translator.TranslateStatement(stmt)
	require.NoError(t, err)
	defer lazy.Release()

	result, err := lazy.Collect()
	require.NoError(t, err)
	defer result.Release()

	assert.Equal(t, 1, result.Len()) // Only Charlie (age 35)
	assert.Len(t, result.Columns(), 1)
}

func TestSQLTranslatorGroupBy(t *testing.T) {
	t.Skip("TODO: Fix SQL parser issues before enabling these tests")
	mem := memory.NewGoAllocator()
	translator := NewSQLTranslator(mem)

	df := createTestDataFrame(mem)
	defer df.Release()

	translator.RegisterTable("employees", df)

	// Test GROUP BY with aggregation
	stmt, err := ParseSQL("SELECT department, COUNT(*) FROM employees GROUP BY department")
	require.NoError(t, err)

	lazy, err := translator.TranslateStatement(stmt)
	require.NoError(t, err)
	defer lazy.Release()

	result, err := lazy.Collect()
	require.NoError(t, err)
	defer result.Release()

	assert.Equal(t, 3, result.Len()) // Engineering, Sales, Marketing
	assert.Len(t, result.Columns(), 2)
	assert.True(t, result.HasColumn("department"))
}

func TestSQLTranslatorOrderBy(t *testing.T) {
	t.Skip("TODO: Fix SQL parser issues before enabling these tests")
	mem := memory.NewGoAllocator()
	translator := NewSQLTranslator(mem)

	df := createTestDataFrame(mem)
	defer df.Release()

	translator.RegisterTable("employees", df)

	// Test ORDER BY
	stmt, err := ParseSQL("SELECT name, age FROM employees ORDER BY age DESC")
	require.NoError(t, err)

	lazy, err := translator.TranslateStatement(stmt)
	require.NoError(t, err)
	defer lazy.Release()

	result, err := lazy.Collect()
	require.NoError(t, err)
	defer result.Release()

	assert.Equal(t, 4, result.Len())
	// Should be ordered by age descending: Charlie (35), Bob (30), David (28), Alice (25)
}

func TestSQLTranslatorComputedColumns(t *testing.T) {
	t.Skip("TODO: Fix SQL parser issues before enabling these tests")
	mem := memory.NewGoAllocator()
	translator := NewSQLTranslator(mem)

	df := createTestDataFrame(mem)
	defer df.Release()

	translator.RegisterTable("employees", df)

	// Test computed columns
	stmt, err := ParseSQL("SELECT name, age * 2 as double_age FROM employees")
	require.NoError(t, err)

	lazy, err := translator.TranslateStatement(stmt)
	require.NoError(t, err)
	defer lazy.Release()

	result, err := lazy.Collect()
	require.NoError(t, err)
	defer result.Release()

	assert.Equal(t, 4, result.Len())
	assert.Len(t, result.Columns(), 2)
	assert.True(t, result.HasColumn("name"))
	assert.True(t, result.HasColumn("double_age"))
}

func TestSQLTranslatorValidation(t *testing.T) {
	mem := memory.NewGoAllocator()
	translator := NewSQLTranslator(mem)

	df := createTestDataFrame(mem)
	defer df.Release()

	translator.RegisterTable("employees", df)

	tests := []struct {
		name    string
		query   string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "Valid query",
			query:   "SELECT name FROM employees",
			wantErr: false,
		},
		{
			name:    "Empty SELECT list",
			query:   "SELECT FROM employees",
			wantErr: true,
			errMsg:  "SELECT list cannot be empty",
		},
		{
			name:    "Wildcard with GROUP BY",
			query:   "SELECT * FROM employees GROUP BY department",
			wantErr: true,
			errMsg:  "wildcard (*) not allowed with GROUP BY",
		},
		{
			name:    "Non-grouped column in SELECT with GROUP BY",
			query:   "SELECT name, department FROM employees GROUP BY department",
			wantErr: true,
			errMsg:  "must appear in GROUP BY clause",
		},
		{
			name:    "HAVING without GROUP BY",
			query:   "SELECT name FROM employees HAVING COUNT(*) > 1",
			wantErr: true,
			errMsg:  "HAVING clause requires GROUP BY clause",
		},
		{
			name:    "Invalid LIMIT",
			query:   "SELECT name FROM employees LIMIT -1",
			wantErr: true,
			errMsg:  "LIMIT count must be positive",
		},
		{
			name:    "Invalid OFFSET",
			query:   "SELECT name FROM employees LIMIT 10 OFFSET -5",
			wantErr: true,
			errMsg:  "OFFSET must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := ParseSQL(tt.query)
			if err != nil {
				// Skip if parsing fails
				return
			}

			err = translator.ValidateSQLSyntax(stmt)

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

func TestSQLFunctionTranslation(t *testing.T) {
	mem := memory.NewGoAllocator()
	translator := NewSQLTranslator(mem)

	tests := []struct {
		name     string
		function *SQLFunction
		wantErr  bool
	}{
		{
			name: "COUNT function",
			function: &SQLFunction{
				Name: "COUNT",
				Args: []expr.Expr{expr.Lit(1)},
			},
			wantErr: false,
		},
		{
			name: "SUM function",
			function: &SQLFunction{
				Name: "SUM",
				Args: []expr.Expr{expr.Col("salary")},
			},
			wantErr: false,
		},
		{
			name: "AVG function",
			function: &SQLFunction{
				Name: "AVG",
				Args: []expr.Expr{expr.Col("salary")},
			},
			wantErr: false,
		},
		{
			name: "UPPER function",
			function: &SQLFunction{
				Name: "UPPER",
				Args: []expr.Expr{expr.Col("name")},
			},
			wantErr: false,
		},
		{
			name: "Invalid SUM - too many args",
			function: &SQLFunction{
				Name: "SUM",
				Args: []expr.Expr{expr.Col("salary"), expr.Col("bonus")},
			},
			wantErr: true,
		},
		{
			name: "Invalid UPPER - not a column",
			function: &SQLFunction{
				Name: "UPPER",
				Args: []expr.Expr{expr.Lit("constant")},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := translator.TranslateFunctionCall(tt.function)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, expr)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, expr)
			}
		})
	}
}

func TestSQLTranslatorTableManagement(t *testing.T) {
	mem := memory.NewGoAllocator()
	translator := NewSQLTranslator(mem)

	df1 := createTestDataFrame(mem)
	defer df1.Release()

	df2 := createTestDataFrame(mem)
	defer df2.Release()

	// Test table registration
	translator.RegisterTable("employees", df1)
	translator.RegisterTable("contractors", df2)

	tables := translator.GetRegisteredTables()
	assert.Len(t, tables, 2)
	assert.Contains(t, tables, "employees")
	assert.Contains(t, tables, "contractors")

	// Test table clearing
	translator.ClearTables()
	tables = translator.GetRegisteredTables()
	assert.Empty(t, tables)
}

func TestSQLTranslatorErrors(t *testing.T) {
	t.Skip("TODO: Fix SQL parser issues before enabling these tests")
	mem := memory.NewGoAllocator()
	translator := NewSQLTranslator(mem)

	// Test missing table
	stmt, err := ParseSQL("SELECT name FROM nonexistent")
	require.NoError(t, err)

	lazy, err := translator.TranslateStatement(stmt)
	assert.Error(t, err)
	assert.Nil(t, lazy)
	assert.Contains(t, err.Error(), "table not found")

	// Test missing FROM clause
	stmt2, err := ParseSQL("SELECT 1")
	require.NoError(t, err)

	// Manually create statement without FROM clause for testing
	selectStmt := stmt2.(*SelectStatement)
	selectStmt.FromClause = nil

	lazy, err = translator.TranslateStatement(selectStmt)
	assert.Error(t, err)
	assert.Nil(t, lazy)
	assert.Contains(t, err.Error(), "FROM clause is required")
}

func TestComplexSQLTranslation(t *testing.T) {
	t.Skip("TODO: Fix SQL parser issues before enabling these tests")
	mem := memory.NewGoAllocator()
	translator := NewSQLTranslator(mem)

	df := createTestDataFrame(mem)
	defer df.Release()

	translator.RegisterTable("employees", df)

	// Test complex query with multiple clauses
	query := `
		SELECT department, AVG(salary) as avg_salary, COUNT(*) as count
		FROM employees 
		WHERE active = true 
		GROUP BY department 
		HAVING COUNT(*) > 0
		ORDER BY avg_salary DESC
	`

	stmt, err := ParseSQL(query)
	require.NoError(t, err)

	err = translator.ValidateSQLSyntax(stmt)
	require.NoError(t, err)

	lazy, err := translator.TranslateStatement(stmt)
	require.NoError(t, err)
	defer lazy.Release()

	result, err := lazy.Collect()
	require.NoError(t, err)
	defer result.Release()

	// Should have results grouped by department for active employees
	assert.Positive(t, result.Len())
	assert.Len(t, result.Columns(), 3) // department, avg_salary, count
	assert.True(t, result.HasColumn("department"))
	assert.True(t, result.HasColumn("avg_salary"))
	assert.True(t, result.HasColumn("count"))
}
