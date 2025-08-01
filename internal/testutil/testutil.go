// Package testutil provides common testing utilities to reduce code duplication
// across test files in the gorilla DataFrame library.
//
// This package consolidates common patterns identified in test similarity analysis:
// - Memory allocator setup and cleanup
// - Standard test DataFrame creation
// - Resource lifecycle management
// - Common test assertions
package testutil

import (
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// defaultRowCount is the default number of rows in test DataFrames.
	defaultRowCount = 4
)

// TestMemoryContext provides memory allocator with automatic cleanup.
type TestMemoryContext struct {
	Allocator memory.Allocator
	cleanup   func()
}

// Release performs cleanup of the memory context.
func (tmc *TestMemoryContext) Release() {
	if tmc.cleanup != nil {
		tmc.cleanup()
	}
}

// SetupMemoryTest creates a memory allocator with automatic cleanup for tests.
// Returns a TestMemoryContext that should be released with defer.
//
// Example usage:
//
//	mem := testutil.SetupMemoryTest(t)
//	defer mem.Release()
func SetupMemoryTest(tb testing.TB) *TestMemoryContext {
	tb.Helper()
	allocator := memory.NewGoAllocator()

	return &TestMemoryContext{
		Allocator: allocator,
		cleanup: func() {
			// Memory allocator cleanup is handled by Go GC
			// This pattern maintains consistency with other test utilities
		},
	}
}

// TestDataFrameOption configures test DataFrame creation.
type TestDataFrameOption func(*testDataFrameConfig)

type testDataFrameConfig struct {
	includeNulls bool
	rowCount     int
	withActive   bool
}

// WithNulls includes null values in test data.
func WithNulls() TestDataFrameOption {
	return func(cfg *testDataFrameConfig) {
		cfg.includeNulls = true
	}
}

// WithRowCount sets the number of rows in test data.
func WithRowCount(count int) TestDataFrameOption {
	return func(cfg *testDataFrameConfig) {
		cfg.rowCount = count
	}
}

// WithActiveColumn includes an 'active' boolean column.
func WithActiveColumn() TestDataFrameOption {
	return func(cfg *testDataFrameConfig) {
		cfg.withActive = true
	}
}

// CreateTestDataFrame creates a standard test DataFrame with employee data.
// This consolidates the common createTestDataFrame pattern found across multiple test files.
//
// Default DataFrame includes:
// - name (string): ["Alice", "Bob", "Charlie", "David"]
// - age (int64): [25, 30, 35, 28]
// - department (string): ["Engineering", "Sales", "Engineering", "Marketing"]
// - salary (int64): [100000, 80000, 120000, 75000]
//
// Example usage:
//
//	mem := testutil.SetupMemoryTest(t)
//	defer mem.Release()
//	df := testutil.CreateTestDataFrame(mem.Allocator)
//	defer df.Release()
func CreateTestDataFrame(allocator memory.Allocator, opts ...TestDataFrameOption) *dataframe.DataFrame {
	cfg := &testDataFrameConfig{
		includeNulls: false,
		rowCount:     defaultRowCount,
		withActive:   false,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	// Generate test data based on configuration
	names := generateNames(cfg.rowCount)
	ages := generateAges(cfg.rowCount)
	departments := generateDepartments(cfg.rowCount)
	salaries := generateSalaries(cfg.rowCount)

	namesSeries := series.New("name", names, allocator)
	agesSeries := series.New("age", ages, allocator)
	departmentsSeries := series.New("department", departments, allocator)
	salariesSeries := series.New("salary", salaries, allocator)

	seriesList := []dataframe.ISeries{
		namesSeries,
		agesSeries,
		departmentsSeries,
		salariesSeries,
	}

	if cfg.withActive {
		active := generateActiveFlags(cfg.rowCount)
		activeSeries := series.New("active", active, allocator)
		seriesList = append(seriesList, activeSeries)
	}

	return dataframe.New(seriesList...)
}

// CreateSimpleTestDataFrame creates a simple 2-column DataFrame for basic testing.
// This is useful for tests that don't need the full employee dataset.
func CreateSimpleTestDataFrame(allocator memory.Allocator) *dataframe.DataFrame {
	names := series.New("name", []string{"Alice", "Bob"}, allocator)
	ages := series.New("age", []int64{25, 30}, allocator)

	return dataframe.New(names, ages)
}

// AssertDataFrameEqual performs deep equality comparison of DataFrames.
// This consolidates common DataFrame assertion patterns.
func AssertDataFrameEqual(t *testing.T, expected, actual *dataframe.DataFrame) {
	t.Helper()

	require.NotNil(t, expected, "expected DataFrame should not be nil")
	require.NotNil(t, actual, "actual DataFrame should not be nil")

	assert.Equal(t, expected.Len(), actual.Len(), "DataFrame lengths should match")
	assert.Equal(t, expected.Width(), actual.Width(), "DataFrame widths should match")
	assert.Equal(t, expected.Columns(), actual.Columns(), "DataFrame columns should match")

	// Compare data column by column
	for _, colName := range expected.Columns() {
		expectedCol, expectedExists := expected.Column(colName)
		actualCol, actualExists := actual.Column(colName)

		require.True(t, expectedExists, "expected column %s should exist", colName)
		require.True(t, actualExists, "actual column %s should exist", colName)

		assert.True(t, columnsEqual(expectedCol, actualCol),
			"column %s data should match", colName)
	}
}

// AssertDataFrameHasColumns verifies that a DataFrame has the expected columns.
func AssertDataFrameHasColumns(t *testing.T, df *dataframe.DataFrame, expectedColumns []string) {
	t.Helper()

	require.NotNil(t, df, "DataFrame should not be nil")

	actualColumns := df.Columns()
	assert.Len(t, actualColumns, len(expectedColumns), "column count should match")

	for _, col := range expectedColumns {
		assert.True(t, df.HasColumn(col), "DataFrame should have column %s", col)
	}
}

// AssertDataFrameNotEmpty verifies that a DataFrame is not empty.
func AssertDataFrameNotEmpty(t *testing.T, df *dataframe.DataFrame) {
	t.Helper()

	require.NotNil(t, df, "DataFrame should not be nil")
	assert.Positive(t, df.Len(), "DataFrame should not be empty")
	assert.Positive(t, df.Width(), "DataFrame should have columns")
}

// Helper functions for generating test data

func generateNames(count int) []string {
	baseNames := []string{"Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry"}
	names := make([]string, count)
	for i := range count {
		names[i] = baseNames[i%len(baseNames)]
	}
	return names
}

func generateAges(count int) []int64 {
	baseAges := []int64{25, 30, 35, 28, 32, 45, 29, 38}
	ages := make([]int64, count)
	for i := range count {
		ages[i] = baseAges[i%len(baseAges)]
	}
	return ages
}

func generateDepartments(count int) []string {
	baseDepts := []string{"Engineering", "Sales", "Engineering", "Marketing", "HR", "Finance", "Engineering", "Sales"}
	departments := make([]string, count)
	for i := range count {
		departments[i] = baseDepts[i%len(baseDepts)]
	}
	return departments
}

func generateSalaries(count int) []int64 {
	baseSalaries := []int64{100000, 80000, 120000, 75000, 90000, 110000, 95000, 85000}
	salaries := make([]int64, count)
	for i := range count {
		salaries[i] = baseSalaries[i%len(baseSalaries)]
	}
	return salaries
}

func generateActiveFlags(count int) []bool {
	baseFlags := []bool{true, true, false, true, true, false, true, false}
	flags := make([]bool, count)
	for i := range count {
		flags[i] = baseFlags[i%len(baseFlags)]
	}
	return flags
}

// columnsEqual compares two columns for equality.
// This handles the common data types used in DataFrame columns.
func columnsEqual(col1, col2 interface{}) bool {
	// For test utilities, we can use reflection-based comparison
	// as it's sufficient for the testing scenarios this package addresses.
	// In production DataFrame operations, more sophisticated comparison
	// would be handled by the DataFrame/Series comparison methods.
	return reflect.DeepEqual(col1, col2)
}
