package gorilla_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/series"
	"github.com/paveg/gorilla/internal/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSQLExecutorIntegration tests that SQL functionality is available through the public API.
func TestSQLExecutorIntegration(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data
	names, _ := series.NewSafe("name", []string{"Alice", "Bob", "Charlie"}, mem)
	ages, _ := series.NewSafe("age", []int64{25, 30, 35}, mem)
	defer names.Release()
	defer ages.Release()

	df := dataframe.New(names, ages)
	defer df.Release()

	// Test that NewSQLExecutor function exists and works
	executor := sql.NewSQLExecutor(mem)
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
	nameSeries := nameCol.(*series.Series[string])
	assert.Equal(t, "Charlie", nameSeries.Value(0), "Result should contain Charlie")
}

// TestSQLExecutorValidation tests query validation functionality.
func TestSQLExecutorValidation(t *testing.T) {
	mem := memory.NewGoAllocator()
	executor := sql.NewSQLExecutor(mem)

	// Register a test table for validation
	names, _ := series.NewSafe("name", []string{"Alice"}, mem)
	defer names.Release()
	df := dataframe.New(names)
	defer df.Release()
	executor.RegisterTable("test_table", df)

	// Test validation of valid query
	err := executor.ValidateQuery("SELECT * FROM test_table")
	require.NoError(t, err, "Valid SQL query should pass validation")

	// Test validation of invalid query
	err = executor.ValidateQuery("SELECT * FROM")
	require.Error(t, err, "Invalid SQL query should fail validation")
}

// TestSQLExecutorExplain tests query explanation functionality.
func TestSQLExecutorExplain(t *testing.T) {
	mem := memory.NewGoAllocator()
	executor := sql.NewSQLExecutor(mem)

	// Create test data and register table
	names, _ := series.NewSafe("name", []string{"Alice", "Bob"}, mem)
	defer names.Release()
	df := dataframe.New(names)
	defer df.Release()
	executor.RegisterTable("test", df)

	// Test explain functionality
	plan, err := executor.Explain("SELECT name FROM test WHERE name = 'Alice'")
	require.NoError(t, err, "EXPLAIN should work for valid queries")
	assert.NotEmpty(t, plan, "EXPLAIN should return a non-empty plan")
}

// TestSQLExecutorTableManagement tests table registration and management.
func TestSQLExecutorTableManagement(t *testing.T) {
	mem := memory.NewGoAllocator()
	executor := sql.NewSQLExecutor(mem)

	// Test initial state
	tables := executor.GetRegisteredTables()
	assert.Empty(t, tables, "Executor should start with no registered tables")

	// Register a table
	names, _ := series.NewSafe("name", []string{"Alice"}, mem)
	defer names.Release()
	df := dataframe.New(names)
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

// TestSQLLimitClause tests SQL LIMIT functionality.
func TestSQLLimitClause(t *testing.T) {
	mem := memory.NewGoAllocator()
	executor := sql.NewSQLExecutor(mem)

	// Create test data with 5 rows
	names, _ := series.NewSafe("name", []string{"Alice", "Bob", "Charlie", "Diana", "Eve"}, mem)
	ages, _ := series.NewSafe("age", []int64{25, 30, 35, 40, 45}, mem)
	defer names.Release()
	defer ages.Release()

	df := dataframe.New(names, ages)
	defer df.Release()
	executor.RegisterTable("employees", df)

	// Test LIMIT 3 - should return first 3 rows
	result, err := executor.Execute("SELECT * FROM employees LIMIT 3")
	require.NoError(t, err, "LIMIT query should not fail")
	require.NotNil(t, result, "LIMIT query should return a result")
	defer result.Release()

	assert.Equal(t, 3, result.Len(), "LIMIT 3 should return 3 rows")
	assert.Equal(t, 2, result.Width(), "Result should have 2 columns")

	// Verify the first 3 names are correct
	nameCol, exists := result.Column("name")
	require.True(t, exists, "Result should have 'name' column")
	assert.Equal(t, "Alice", nameCol.GetAsString(0))
	assert.Equal(t, "Bob", nameCol.GetAsString(1))
	assert.Equal(t, "Charlie", nameCol.GetAsString(2))
}

// TestSQLOffsetClause tests SQL OFFSET functionality.
func TestSQLOffsetClause(t *testing.T) {
	mem := memory.NewGoAllocator()
	executor := sql.NewSQLExecutor(mem)

	// Create test data with 5 rows
	names, _ := series.NewSafe("name", []string{"Alice", "Bob", "Charlie", "Diana", "Eve"}, mem)
	ages, _ := series.NewSafe("age", []int64{25, 30, 35, 40, 45}, mem)
	defer names.Release()
	defer ages.Release()

	df := dataframe.New(names, ages)
	defer df.Release()
	executor.RegisterTable("employees", df)

	// Test OFFSET 2 - should skip first 2 rows and return remaining 3
	result, err := executor.Execute("SELECT * FROM employees OFFSET 2")
	require.NoError(t, err, "OFFSET query should not fail")
	require.NotNil(t, result, "OFFSET query should return a result")
	defer result.Release()

	assert.Equal(t, 3, result.Len(), "OFFSET 2 should return 3 rows (from 5 total)")
	assert.Equal(t, 2, result.Width(), "Result should have 2 columns")

	// Verify the names start from Charlie (skipping Alice and Bob)
	nameCol, exists := result.Column("name")
	require.True(t, exists, "Result should have 'name' column")
	assert.Equal(t, "Charlie", nameCol.GetAsString(0))
	assert.Equal(t, "Diana", nameCol.GetAsString(1))
	assert.Equal(t, "Eve", nameCol.GetAsString(2))
}

// TestSQLLimitOffset tests SQL LIMIT and OFFSET together.
func TestSQLLimitOffset(t *testing.T) {
	mem := memory.NewGoAllocator()
	executor := sql.NewSQLExecutor(mem)

	// Create test data with 5 rows
	names, _ := series.NewSafe("name", []string{"Alice", "Bob", "Charlie", "Diana", "Eve"}, mem)
	ages, _ := series.NewSafe("age", []int64{25, 30, 35, 40, 45}, mem)
	defer names.Release()
	defer ages.Release()

	df := dataframe.New(names, ages)
	defer df.Release()
	executor.RegisterTable("employees", df)

	// Test LIMIT 2 OFFSET 1 - should skip 1 row and return next 2
	result, err := executor.Execute("SELECT * FROM employees LIMIT 2 OFFSET 1")
	require.NoError(t, err, "LIMIT OFFSET query should not fail")
	require.NotNil(t, result, "LIMIT OFFSET query should return a result")
	defer result.Release()

	assert.Equal(t, 2, result.Len(), "LIMIT 2 OFFSET 1 should return 2 rows")
	assert.Equal(t, 2, result.Width(), "Result should have 2 columns")

	// Verify the names are Bob and Charlie (skipping Alice, taking next 2)
	nameCol, exists := result.Column("name")
	require.True(t, exists, "Result should have 'name' column")
	assert.Equal(t, "Bob", nameCol.GetAsString(0))
	assert.Equal(t, "Charlie", nameCol.GetAsString(1))
}

// TestSQLLimitOffsetEdgeCases tests edge cases for LIMIT and OFFSET.
func TestSQLLimitOffsetEdgeCases(t *testing.T) {
	// Split into sub-tests to isolate any resource management issues

	t.Run("offset_beyond_data", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		executor := sql.NewSQLExecutor(mem)

		// Create test data with 3 rows
		names, _ := series.NewSafe("name", []string{"Alice", "Bob", "Charlie"}, mem)
		defer names.Release()
		df := dataframe.New(names)
		defer df.Release()
		executor.RegisterTable("employees", df)

		// Test OFFSET beyond data size - should return empty result
		result, err := executor.Execute("SELECT * FROM employees OFFSET 10")
		require.NoError(t, err, "OFFSET beyond data should not fail")
		require.NotNil(t, result, "Query should return a result")
		defer result.Release()
		assert.Equal(t, 0, result.Len(), "OFFSET beyond data should return empty result")
	})

	t.Run("limit_zero", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		executor := sql.NewSQLExecutor(mem)

		// Create test data with 3 rows
		names, _ := series.NewSafe("name", []string{"Alice", "Bob", "Charlie"}, mem)
		defer names.Release()
		df := dataframe.New(names)
		defer df.Release()
		executor.RegisterTable("employees", df)

		// Test LIMIT 0 - should return empty result
		result, err := executor.Execute("SELECT * FROM employees LIMIT 0")
		require.NoError(t, err, "LIMIT 0 should not fail")
		require.NotNil(t, result, "Query should return a result")
		defer result.Release()
		assert.Equal(t, 0, result.Len(), "LIMIT 0 should return empty result")
	})

	t.Run("limit_larger_than_data", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		executor := sql.NewSQLExecutor(mem)

		// Create test data with 3 rows
		names, _ := series.NewSafe("name", []string{"Alice", "Bob", "Charlie"}, mem)
		defer names.Release()
		df := dataframe.New(names)
		defer df.Release()
		executor.RegisterTable("employees", df)

		// Test LIMIT larger than data size - should return all data
		result, err := executor.Execute("SELECT * FROM employees LIMIT 100")
		require.NoError(t, err, "LIMIT larger than data should not fail")
		require.NotNil(t, result, "Query should return a result")
		defer result.Release()
		assert.Equal(t, 3, result.Len(), "LIMIT larger than data should return all rows")
	})
}
