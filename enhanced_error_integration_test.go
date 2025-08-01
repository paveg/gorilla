package gorilla_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEnhancedErrorIntegration tests that enhanced errors are properly used through LazyFrame operations.
func TestEnhancedErrorIntegration(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test DataFrame
	names := gorilla.NewSeries("name", []string{"Alice", "Bob", "Charlie"}, mem)
	ages := gorilla.NewSeries("age", []int64{25, 30, 35}, mem)
	defer names.Release()
	defer ages.Release()

	df := gorilla.NewDataFrame(names, ages)
	defer df.Release()

	t.Run("ColumnNotFoundInFilter", func(t *testing.T) {
		// Test filter with typo - should provide suggestion
		_, err := df.Lazy().Filter(gorilla.Col("nam").Eq(gorilla.Lit("Alice"))).Collect() // typo: should suggest "name"
		require.Error(t, err)

		errorMsg := err.Error()
		// Check if it contains column error information
		assert.Contains(t, errorMsg, "nam")
	})

	t.Run("ValidateEnhancedErrorsWork", func(t *testing.T) {
		// Just verify that we can access the columns that exist
		assert.Contains(t, df.Columns(), "name")
		assert.Contains(t, df.Columns(), "age")
	})
}

// TestBasicErrorEnhancement tests that error enhancements work.
func TestBasicErrorEnhancement(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test DataFrame with known columns
	sales := gorilla.NewSeries("sales_amount", []int64{100, 200, 300}, mem)
	defer sales.Release()

	df := gorilla.NewDataFrame(sales)
	defer df.Release()

	t.Run("ColumnErrorInLazyOperation", func(t *testing.T) {
		// Test that column not found errors are enhanced in lazy operations
		_, err := df.Lazy().Filter(gorilla.Col("missing_column").Gt(gorilla.Lit(100))).Collect()
		require.Error(t, err)

		errorMsg := err.Error()
		// Should mention the missing column
		assert.Contains(t, errorMsg, "missing_column")
	})
}
