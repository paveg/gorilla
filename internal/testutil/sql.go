package testutil

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/series"
	"github.com/paveg/gorilla/internal/sql"
	"github.com/stretchr/testify/require"
)

// SQLTestContext provides a configured SQL translator with test data for SQL tests.
type SQLTestContext struct {
	Translator *sql.SQLTranslator
	TestTable  *dataframe.DataFrame
	Allocator  memory.Allocator
	cleanup    func()
}

// Release cleans up the SQL test context.
func (ctx *SQLTestContext) Release() {
	if ctx.TestTable != nil {
		ctx.TestTable.Release()
	}
	if ctx.cleanup != nil {
		ctx.cleanup()
	}
}

// SetupSQLTest creates a configured SQL translator with a test table for SQL tests.
// This consolidates the common SQL test setup pattern found across SQL test files.
//
// The test table "employees" contains:
// - name, age, department, salary columns with sample employee data
// - Suitable for testing SELECT, WHERE, GROUP BY, HAVING operations
//
// Example usage:
//
//	sqlCtx := testutil.SetupSQLTest(t)
//	defer sqlCtx.Release()
//
//	stmt, err := sql.ParseSQL("SELECT name FROM employees WHERE age > 30")
//	lazy, err := sqlCtx.Translator.TranslateStatement(stmt)
func SetupSQLTest(t *testing.T) *SQLTestContext {
	t.Helper()

	allocator := memory.NewGoAllocator()
	translator := sql.NewSQLTranslator(allocator)

	// Create standard test table with employee data
	testTable := CreateTestDataFrame(allocator, WithActiveColumn())

	// Register the table with a standard name
	translator.RegisterTable("employees", testTable)

	return &SQLTestContext{
		Translator: translator,
		TestTable:  testTable,
		Allocator:  allocator,
		cleanup: func() {
			// Cleanup handled by individual components
		},
	}
}

// SetupSimpleSQLTest creates a minimal SQL test context with simple test data.
// Useful for basic SQL parsing and translation tests.
func SetupSimpleSQLTest(t *testing.T) *SQLTestContext {
	t.Helper()

	allocator := memory.NewGoAllocator()
	translator := sql.NewSQLTranslator(allocator)

	// Create simple test table
	testTable := CreateSimpleTestDataFrame(allocator)
	translator.RegisterTable("test_table", testTable)

	return &SQLTestContext{
		Translator: translator,
		TestTable:  testTable,
		Allocator:  allocator,
		cleanup: func() {
			// Cleanup handled by individual components
		},
	}
}

// ExecuteSQLQuery executes a SQL query and returns the result DataFrame.
// This consolidates the common pattern of parsing -> translating -> collecting SQL queries.
func (ctx *SQLTestContext) ExecuteSQLQuery(t *testing.T, query string) *dataframe.DataFrame {
	t.Helper()

	// Parse SQL
	stmt, err := sql.ParseSQL(query)
	require.NoError(t, err, "SQL parsing should succeed")

	// Translate to lazy frame
	lazy, err := ctx.Translator.TranslateStatement(stmt)
	require.NoError(t, err, "SQL translation should succeed")
	defer lazy.Release()

	// Execute query
	result, err := lazy.Collect()
	require.NoError(t, err, "SQL execution should succeed")

	return result
}

// AssertSQLQueryResult executes a SQL query and validates the result structure.
// This consolidates common SQL test assertion patterns.
func (ctx *SQLTestContext) AssertSQLQueryResult(t *testing.T, query string, expectedRowCount int, expectedColumns []string) *dataframe.DataFrame {
	t.Helper()

	result := ctx.ExecuteSQLQuery(t, query)

	// Validate result structure
	AssertDataFrameNotEmpty(t, result)
	require.Equal(t, expectedRowCount, result.Len(), "result row count should match")
	AssertDataFrameHasColumns(t, result, expectedColumns)

	return result
}

// CreateTestTableWithData creates a DataFrame with specific test data for SQL tests.
// This allows tests to create custom tables beyond the standard employee table.
func CreateTestTableWithData(allocator memory.Allocator, data map[string]interface{}) *dataframe.DataFrame {
	var seriesList []dataframe.ISeries

	for colName, colData := range data {
		switch values := colData.(type) {
		case []string:
			s := series.New(colName, values, allocator)
			seriesList = append(seriesList, s)
		case []int64:
			s := series.New(colName, values, allocator)
			seriesList = append(seriesList, s)
		case []float64:
			s := series.New(colName, values, allocator)
			seriesList = append(seriesList, s)
		case []bool:
			s := series.New(colName, values, allocator)
			seriesList = append(seriesList, s)
		default:
			panic("unsupported data type for test table creation")
		}
	}

	return dataframe.New(seriesList...)
}
