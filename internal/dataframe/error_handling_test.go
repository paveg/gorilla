//nolint:testpackage // requires internal access to unexported types and functions
package dataframe

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	dferrors "github.com/paveg/gorilla/internal/errors"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSort_ErrorHandling(t *testing.T) {
	mem := memory.NewGoAllocator()
	// mem.AssertSize not available in this version

	// Create test DataFrame
	idSeries := series.New("id", []int64{3, 1, 2}, mem)
	nameSeries := series.New("name", []string{"Charlie", "Alice", "Bob"}, mem)
	df := New(idSeries, nameSeries)
	defer df.Release()

	t.Run("Valid column", func(t *testing.T) {
		result, err := df.Sort("id", true)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 3, result.Len())
		// Verify sorting worked
		idCol, exists := result.Column("id")
		assert.True(t, exists)
		ids := idCol.(*series.Series[int64]).Values()
		assert.Equal(t, []int64{1, 2, 3}, ids)
	})

	t.Run("Invalid column", func(t *testing.T) {
		result, err := df.Sort("age", true)
		require.Error(t, err)
		assert.Nil(t, result)

		var dfErr *dferrors.DataFrameError
		require.ErrorAs(t, err, &dfErr)
		assert.Equal(t, "Sort", dfErr.Op)
		assert.Equal(t, "age", dfErr.Column)
		assert.Equal(t, "column does not exist", dfErr.Message)
		assert.Equal(t, "Sort operation failed on column 'age': column does not exist", dfErr.Error())
	})

	t.Run("Empty DataFrame with valid column", func(t *testing.T) {
		// Create empty DataFrame with columns but no rows
		emptySeries := series.New("id", []int64{}, mem)
		emptyDf := New(emptySeries)
		defer emptyDf.Release()

		result, err := emptyDf.Sort("id", true)
		require.NoError(t, err) // Empty DataFrame should not error
		defer result.Release()

		assert.Equal(t, 0, result.Len())
	})
}

func TestSortBy_ErrorHandling(t *testing.T) {
	mem := memory.NewGoAllocator()
	// mem.AssertSize not available in this version

	// Create test DataFrame
	idSeries := series.New("id", []int64{3, 1, 2}, mem)
	nameSeries := series.New("name", []string{"Charlie", "Alice", "Bob"}, mem)
	ageSeries := series.New("age", []int64{25, 30, 35}, mem)
	df := New(idSeries, nameSeries, ageSeries)
	defer df.Release()

	t.Run("Valid multi-column sort", func(t *testing.T) {
		result, err := df.SortBy([]string{"name", "age"}, []bool{true, false})
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 3, result.Len())
	})

	t.Run("Length mismatch", func(t *testing.T) {
		result, err := df.SortBy([]string{"name", "age"}, []bool{true})
		require.Error(t, err)
		assert.Nil(t, result)

		var dfErr *dferrors.DataFrameError
		require.ErrorAs(t, err, &dfErr)
		assert.Equal(t, "SortBy", dfErr.Op)
		assert.Contains(t, dfErr.Message, "expected length 2, got 1")
	})

	t.Run("Invalid column in multi-sort", func(t *testing.T) {
		result, err := df.SortBy([]string{"name", "salary"}, []bool{true, false})
		require.Error(t, err)
		assert.Nil(t, result)

		var dfErr *dferrors.DataFrameError
		require.ErrorAs(t, err, &dfErr)
		assert.Equal(t, "SortBy", dfErr.Op)
		assert.Equal(t, "salary", dfErr.Column)
		assert.Equal(t, "column does not exist", dfErr.Message)
	})

	t.Run("Mixed valid and invalid columns", func(t *testing.T) {
		result, err := df.SortBy([]string{"name", "invalid", "age"}, []bool{true, false, true})
		require.Error(t, err)
		assert.Nil(t, result)

		var dfErr *dferrors.DataFrameError
		require.ErrorAs(t, err, &dfErr)
		assert.Equal(t, "invalid", dfErr.Column)
	})

	t.Run("Empty arrays", func(t *testing.T) {
		result, err := df.SortBy([]string{}, []bool{})
		require.NoError(t, err) // Empty sort should not error
		defer result.Release()

		// Should return original DataFrame unchanged
		assert.Equal(t, df.Len(), result.Len())
		assert.Equal(t, df.Width(), result.Width())
	})
}

func TestSort_BackwardCompatibility(t *testing.T) {
	// Test that existing tests still work with the new error-returning signature
	mem := memory.NewGoAllocator()
	// mem.AssertSize not available in this version

	idSeries := series.New("id", []int64{3, 1, 2}, mem)
	df := New(idSeries)
	defer df.Release()

	// This should work the same as before, just with explicit error handling
	result, err := df.Sort("id", true)
	require.NoError(t, err)
	defer result.Release()

	idCol, exists := result.Column("id")
	require.True(t, exists)
	ids := idCol.(*series.Series[int64]).Values()
	assert.Equal(t, []int64{1, 2, 3}, ids)
}

func TestSort_PerformanceWithErrors(t *testing.T) {
	// Ensure error handling doesn't significantly impact performance for valid operations
	mem := memory.NewGoAllocator()
	// mem.AssertSize not available in this version

	// Create larger dataset
	size := 1000
	data := make([]int64, size)
	for i := range size {
		data[i] = int64(size - i) // Reverse order
	}

	idSeries := series.New("id", data, mem)
	df := New(idSeries)
	defer df.Release()

	// This should be fast even with validation
	result, err := df.Sort("id", true)
	require.NoError(t, err)
	defer result.Release()

	// Verify it's actually sorted
	idCol, exists := result.Column("id")
	require.True(t, exists)
	ids := idCol.(*series.Series[int64]).Values()
	assert.Equal(t, int64(1), ids[0])
	assert.Equal(t, int64(size), ids[size-1])
}

func TestLazyOperation_ErrorPropagation(t *testing.T) {
	// Test that errors from Sort operations properly propagate through lazy evaluation
	mem := memory.NewGoAllocator()
	// mem.AssertSize not available in this version

	idSeries := series.New("id", []int64{3, 1, 2}, mem)
	df := New(idSeries)
	defer df.Release()

	t.Run("Valid lazy sort", func(t *testing.T) {
		lazy := df.Lazy()
		defer lazy.Release()

		result, err := lazy.Sort("id", true).Collect()
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 3, result.Len())
	})

	t.Run("Invalid lazy sort propagates error", func(t *testing.T) {
		lazy := df.Lazy()
		defer lazy.Release()

		result, err := lazy.Sort("missing", true).Collect()
		require.Error(t, err)
		assert.Nil(t, result)

		var dfErr *dferrors.DataFrameError
		require.ErrorAs(t, err, &dfErr)
		assert.Equal(t, "SortBy", dfErr.Op) // Note: lazy operations use SortBy internally
		assert.Equal(t, "missing", dfErr.Column)
	})
}
