package dataframe

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDataFrameWithNewIntegerTypes tests DataFrame operations with the new integer types.
func TestDataFrameWithNewIntegerTypes(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("DataFrame creation with int16 series", func(t *testing.T) {
		// Create series with new integer types
		int16Values := []int16{1, 2, 3, 4, 5}
		int16Series, err := series.NewSafe("int16_col", int16Values, mem)
		require.NoError(t, err)
		defer int16Series.Release()

		// Create DataFrame
		df := New(int16Series)
		defer df.Release()

		// Test basic operations
		assert.Equal(t, 5, df.Len())
		assert.Equal(t, 1, df.Width())
		assert.Equal(t, []string{"int16_col"}, df.Columns())
	})

	t.Run("DataFrame creation with uint64 series", func(t *testing.T) {
		uint64Values := []uint64{1, 2, 3, 4, 5}
		uint64Series, err := series.NewSafe("uint64_col", uint64Values, mem)
		require.NoError(t, err)
		defer uint64Series.Release()

		df := New(uint64Series)
		defer df.Release()

		assert.Equal(t, 5, df.Len())
		assert.Equal(t, 1, df.Width())
		assert.Equal(t, []string{"uint64_col"}, df.Columns())
	})

	t.Run("DataFrame creation with multiple new integer types", func(t *testing.T) {
		int8Values := []int8{1, 2, 3}
		uint32Values := []uint32{10, 20, 30}
		uint16Values := []uint16{100, 200, 300}
		uint8Values := []uint8{50, 60, 70}

		int8Series, err := series.NewSafe("int8_col", int8Values, mem)
		require.NoError(t, err)
		defer int8Series.Release()

		uint32Series, err := series.NewSafe("uint32_col", uint32Values, mem)
		require.NoError(t, err)
		defer uint32Series.Release()

		uint16Series, err := series.NewSafe("uint16_col", uint16Values, mem)
		require.NoError(t, err)
		defer uint16Series.Release()

		uint8Series, err := series.NewSafe("uint8_col", uint8Values, mem)
		require.NoError(t, err)
		defer uint8Series.Release()

		df := New(int8Series, uint32Series, uint16Series, uint8Series)
		defer df.Release()

		assert.Equal(t, 3, df.Len())
		assert.Equal(t, 4, df.Width())
		expectedCols := []string{"int8_col", "uint16_col", "uint32_col", "uint8_col"}
		assert.ElementsMatch(t, expectedCols, df.Columns())
	})

	t.Run("DataFrame slicing with new integer types", func(t *testing.T) {
		int16Values := []int16{10, 20, 30, 40, 50}
		int16Series, err := series.NewSafe("values", int16Values, mem)
		require.NoError(t, err)
		defer int16Series.Release()

		df := New(int16Series)
		defer df.Release()

		// Test slicing
		sliced := df.Slice(1, 4)
		defer sliced.Release()

		assert.Equal(t, 3, sliced.Len())

		// Verify sliced values
		valuesSeries, exists := sliced.Column("values")
		require.True(t, exists)
		values := valuesSeries.(*series.Series[int16]).Values()
		expected := []int16{20, 30, 40}
		assert.Equal(t, expected, values)
	})

	t.Run("DataFrame with uint8 series", func(t *testing.T) {
		uint8Values := []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		uint8Series, err := series.NewSafe("values", uint8Values, mem)
		require.NoError(t, err)
		defer uint8Series.Release()

		df := New(uint8Series)
		defer df.Release()

		// Test basic properties
		assert.Equal(t, 10, df.Len())
		assert.Equal(t, 1, df.Width())
		assert.Equal(t, []string{"values"}, df.Columns())

		// Test slicing first 3 elements
		head := df.Slice(0, 3)
		defer head.Release()

		assert.Equal(t, 3, head.Len())
		headSeries, exists := head.Column("values")
		require.True(t, exists)
		headValues := headSeries.(*series.Series[uint8]).Values()
		expectedHead := []uint8{1, 2, 3}
		assert.Equal(t, expectedHead, headValues)

		// Test slicing last 3 elements
		tail := df.Slice(7, 10)
		defer tail.Release()

		assert.Equal(t, 3, tail.Len())
		tailSeries, exists := tail.Column("values")
		require.True(t, exists)
		tailValues := tailSeries.(*series.Series[uint8]).Values()
		expectedTail := []uint8{8, 9, 10}
		assert.Equal(t, expectedTail, tailValues)
	})

	t.Run("DataFrame slicing with int32 series", func(t *testing.T) {
		int32Values := []int32{100, 200, 300, 400, 500}
		int32Series, err := series.NewSafe("values", int32Values, mem)
		require.NoError(t, err)
		defer int32Series.Release()

		df := New(int32Series)
		defer df.Release()

		// Test slicing
		sliced := df.Slice(1, 4)
		defer sliced.Release()

		assert.Equal(t, 3, sliced.Len())

		// Verify sliced values
		valuesSeries, exists := sliced.Column("values")
		require.True(t, exists)
		values := valuesSeries.(*series.Series[int32]).Values()
		expected := []int32{200, 300, 400}
		assert.Equal(t, expected, values)
	})

	t.Run("DataFrame slicing with float32 series", func(t *testing.T) {
		float32Values := []float32{1.1, 2.2, 3.3, 4.4, 5.5}
		float32Series, err := series.NewSafe("values", float32Values, mem)
		require.NoError(t, err)
		defer float32Series.Release()

		df := New(float32Series)
		defer df.Release()

		// Test slicing
		sliced := df.Slice(1, 4)
		defer sliced.Release()

		assert.Equal(t, 3, sliced.Len())

		// Verify sliced values
		valuesSeries, exists := sliced.Column("values")
		require.True(t, exists)
		values := valuesSeries.(*series.Series[float32]).Values()
		expected := []float32{2.2, 3.3, 4.4}
		assert.Equal(t, expected, values)
	})
}
