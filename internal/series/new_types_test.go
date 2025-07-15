package series

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewIntegerTypes tests the additional integer types mentioned in Issue #20
func TestNewIntegerTypes(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("int16 series creation and operations", func(t *testing.T) {
		values := []int16{1, -2, 3, -4, 5}
		series, err := NewSafe("test_int16", values, mem)
		require.NoError(t, err)
		defer series.Release()

		// Test basic properties
		assert.Equal(t, "test_int16", series.Name())
		assert.Equal(t, 5, series.Len())
		assert.Equal(t, arrow.PrimitiveTypes.Int16, series.DataType())

		// Test value retrieval
		retrievedValues, err := series.ValuesSafe()
		require.NoError(t, err)
		assert.Equal(t, values, retrievedValues)

		// Test individual value access
		val := series.Value(0)
		assert.Equal(t, int16(1), val)

		val = series.Value(1)
		assert.Equal(t, int16(-2), val)
	})

	t.Run("int8 series creation and operations", func(t *testing.T) {
		values := []int8{1, -2, 3, -4, 5}
		series, err := NewSafe("test_int8", values, mem)
		require.NoError(t, err)
		defer series.Release()

		// Test basic properties
		assert.Equal(t, "test_int8", series.Name())
		assert.Equal(t, 5, series.Len())
		assert.Equal(t, arrow.PrimitiveTypes.Int8, series.DataType())

		// Test value retrieval
		retrievedValues, err := series.ValuesSafe()
		require.NoError(t, err)
		assert.Equal(t, values, retrievedValues)
	})

	t.Run("uint64 series creation and operations", func(t *testing.T) {
		values := []uint64{1, 2, 3, 4, 5}
		series, err := NewSafe("test_uint64", values, mem)
		require.NoError(t, err)
		defer series.Release()

		// Test basic properties
		assert.Equal(t, "test_uint64", series.Name())
		assert.Equal(t, 5, series.Len())
		assert.Equal(t, arrow.PrimitiveTypes.Uint64, series.DataType())

		// Test value retrieval
		retrievedValues, err := series.ValuesSafe()
		require.NoError(t, err)
		assert.Equal(t, values, retrievedValues)
	})

	t.Run("uint32 series creation and operations", func(t *testing.T) {
		values := []uint32{1, 2, 3, 4, 5}
		series, err := NewSafe("test_uint32", values, mem)
		require.NoError(t, err)
		defer series.Release()

		// Test basic properties
		assert.Equal(t, "test_uint32", series.Name())
		assert.Equal(t, 5, series.Len())
		assert.Equal(t, arrow.PrimitiveTypes.Uint32, series.DataType())

		// Test value retrieval
		retrievedValues, err := series.ValuesSafe()
		require.NoError(t, err)
		assert.Equal(t, values, retrievedValues)
	})

	t.Run("uint16 series creation and operations", func(t *testing.T) {
		values := []uint16{1, 2, 3, 4, 5}
		series, err := NewSafe("test_uint16", values, mem)
		require.NoError(t, err)
		defer series.Release()

		// Test basic properties
		assert.Equal(t, "test_uint16", series.Name())
		assert.Equal(t, 5, series.Len())
		assert.Equal(t, arrow.PrimitiveTypes.Uint16, series.DataType())

		// Test value retrieval
		retrievedValues, err := series.ValuesSafe()
		require.NoError(t, err)
		assert.Equal(t, values, retrievedValues)
	})

	t.Run("uint8 series creation and operations", func(t *testing.T) {
		values := []uint8{1, 2, 3, 4, 5}
		series, err := NewSafe("test_uint8", values, mem)
		require.NoError(t, err)
		defer series.Release()

		// Test basic properties
		assert.Equal(t, "test_uint8", series.Name())
		assert.Equal(t, 5, series.Len())
		assert.Equal(t, arrow.PrimitiveTypes.Uint8, series.DataType())

		// Test value retrieval
		retrievedValues, err := series.ValuesSafe()
		require.NoError(t, err)
		assert.Equal(t, values, retrievedValues)
	})

	t.Run("int32 series creation and operations", func(t *testing.T) {
		values := []int32{100, -200, 300, -400, 500}
		series, err := NewSafe("test_int32", values, mem)
		require.NoError(t, err)
		defer series.Release()

		// Test basic properties
		assert.Equal(t, "test_int32", series.Name())
		assert.Equal(t, 5, series.Len())
		assert.Equal(t, arrow.PrimitiveTypes.Int32, series.DataType())

		// Test value retrieval
		retrievedValues, err := series.ValuesSafe()
		require.NoError(t, err)
		assert.Equal(t, values, retrievedValues)

		// Test individual value access
		val := series.Value(0)
		assert.Equal(t, int32(100), val)

		val = series.Value(1)
		assert.Equal(t, int32(-200), val)
	})

	t.Run("float32 series creation and operations", func(t *testing.T) {
		values := []float32{1.1, -2.2, 3.3, -4.4, 5.5}
		series, err := NewSafe("test_float32", values, mem)
		require.NoError(t, err)
		defer series.Release()

		// Test basic properties
		assert.Equal(t, "test_float32", series.Name())
		assert.Equal(t, 5, series.Len())
		assert.Equal(t, arrow.PrimitiveTypes.Float32, series.DataType())

		// Test value retrieval
		retrievedValues, err := series.ValuesSafe()
		require.NoError(t, err)
		assert.Equal(t, values, retrievedValues)

		// Test individual value access
		val := series.Value(0)
		assert.Equal(t, float32(1.1), val)

		val = series.Value(1)
		assert.Equal(t, float32(-2.2), val)
	})
}
