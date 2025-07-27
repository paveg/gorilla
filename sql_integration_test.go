package gorilla

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSQLExecutorIntegration tests that SQL functionality is available through the public API
func TestSQLExecutorIntegration(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data
	names := NewSeries("name", []string{"Alice", "Bob", "Charlie"}, mem)
	ages := NewSeries("age", []int64{25, 30, 35}, mem)
	defer names.Release()
	defer ages.Release()

	df := NewDataFrame(names, ages)
	defer df.Release()

	// Test that NewSQLExecutor function exists and works
	executor := NewSQLExecutor(mem)
	require.NotNil(t, executor, "NewSQLExecutor should return a valid executor")

	// Test that RegisterTable method works
	executor.RegisterTable("employees", df)

	// Test basic SQL query execution
	result, err := executor.Execute("SELECT name FROM employees WHERE age > 30")
	require.NoError(t, err, "SQL query execution should not fail")
	require.NotNil(t, result, "SQL query should return a result")
	defer result.Release()

	// Verify result content
	assert.Equal(t, 1, result.Width(), "Result should have 1 column")
	assert.Equal(t, 1, result.Len(), "Result should have 1 row (Charlie)")

	nameCol, exists := result.Column("name")
	require.True(t, exists, "Result should have 'name' column")
	assert.Equal(t, "Charlie", nameCol.GetAsString(0), "Result should contain Charlie")
}

// TestSQLExecutorValidation tests query validation functionality
func TestSQLExecutorValidation(t *testing.T) {
	mem := memory.NewGoAllocator()
	executor := NewSQLExecutor(mem)

	// Register a test table for validation
	names := NewSeries("name", []string{"Alice"}, mem)
	defer names.Release()
	df := NewDataFrame(names)
	defer df.Release()
	executor.RegisterTable("test_table", df)

	// Test validation of valid query
	err := executor.ValidateQuery("SELECT * FROM test_table")
	assert.NoError(t, err, "Valid SQL query should pass validation")

	// Test validation of invalid query
	err = executor.ValidateQuery("SELECT * FROM")
	assert.Error(t, err, "Invalid SQL query should fail validation")
}

// TestSQLExecutorExplain tests query explanation functionality
func TestSQLExecutorExplain(t *testing.T) {
	mem := memory.NewGoAllocator()
	executor := NewSQLExecutor(mem)

	// Create test data and register table
	names := NewSeries("name", []string{"Alice", "Bob"}, mem)
	defer names.Release()
	df := NewDataFrame(names)
	defer df.Release()
	executor.RegisterTable("test", df)

	// Test explain functionality
	plan, err := executor.Explain("SELECT name FROM test WHERE name = 'Alice'")
	require.NoError(t, err, "EXPLAIN should work for valid queries")
	assert.NotEmpty(t, plan, "EXPLAIN should return a non-empty plan")
}

// TestSQLExecutorTableManagement tests table registration and management
func TestSQLExecutorTableManagement(t *testing.T) {
	mem := memory.NewGoAllocator()
	executor := NewSQLExecutor(mem)

	// Test initial state
	tables := executor.GetRegisteredTables()
	assert.Empty(t, tables, "Executor should start with no registered tables")

	// Register a table
	names := NewSeries("name", []string{"Alice"}, mem)
	defer names.Release()
	df := NewDataFrame(names)
	defer df.Release()
	executor.RegisterTable("test_table", df)

	// Verify table is registered
	tables = executor.GetRegisteredTables()
	assert.Contains(t, tables, "test_table", "Table should be registered")

	// Clear tables
	executor.ClearTables()
	tables = executor.GetRegisteredTables()
	assert.Empty(t, tables, "All tables should be cleared")
}
