package common_test

import (
	"math"
	"testing"

	"github.com/paveg/gorilla/internal/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTypeConverter(t *testing.T) {
	converter := common.NewTypeConverter()

	t.Run("SafeInt64ToInt", func(t *testing.T) {
		// Valid conversions
		result, err := converter.SafeInt64ToInt(42)
		require.NoError(t, err)
		assert.Equal(t, 42, result)

		result, err = converter.SafeInt64ToInt(-42)
		require.NoError(t, err)
		assert.Equal(t, -42, result)

		// Overflow cases
		_, err = converter.SafeInt64ToInt(math.MaxInt64)
		require.Error(t, err)

		_, err = converter.SafeInt64ToInt(math.MinInt64)
		require.Error(t, err)
	})

	t.Run("SafeFloat64ToFloat32", func(t *testing.T) {
		// Valid conversions
		result, err := converter.SafeFloat64ToFloat32(3.14)
		require.NoError(t, err)
		assert.InDelta(t, 3.14, result, 0.01)

		// Special values
		result, err = converter.SafeFloat64ToFloat32(math.Inf(1))
		require.NoError(t, err)
		assert.True(t, math.IsInf(float64(result), 1))

		result, err = converter.SafeFloat64ToFloat32(math.NaN())
		require.NoError(t, err)
		assert.True(t, math.IsNaN(float64(result)))

		// Overflow
		_, err = converter.SafeFloat64ToFloat32(math.MaxFloat64)
		require.Error(t, err)
	})

	t.Run("ToInt64", func(t *testing.T) {
		// Integer types
		result, err := converter.ToInt64(int(42))
		require.NoError(t, err)
		assert.Equal(t, int64(42), result)

		result, err = converter.ToInt64(int32(42))
		require.NoError(t, err)
		assert.Equal(t, int64(42), result)

		result, err = converter.ToInt64(uint16(42))
		require.NoError(t, err)
		assert.Equal(t, int64(42), result)

		// Float types
		result, err = converter.ToInt64(float64(42.7))
		require.NoError(t, err)
		assert.Equal(t, int64(42), result)

		// String
		result, err = converter.ToInt64("42")
		require.NoError(t, err)
		assert.Equal(t, int64(42), result)

		// Bool
		result, err = converter.ToInt64(true)
		require.NoError(t, err)
		assert.Equal(t, int64(1), result)

		result, err = converter.ToInt64(false)
		require.NoError(t, err)
		assert.Equal(t, int64(0), result)

		// Invalid string
		_, err = converter.ToInt64("not a number")
		require.Error(t, err)

		// Unsupported type
		_, err = converter.ToInt64(struct{}{})
		require.Error(t, err)
	})

	t.Run("ToFloat64", func(t *testing.T) {
		// Integer types
		result, err := converter.ToFloat64(int(42))
		require.NoError(t, err)
		assert.InEpsilon(t, float64(42), result, 0.001)

		// Float types
		result, err = converter.ToFloat64(float32(3.14))
		require.NoError(t, err)
		assert.InDelta(t, 3.14, result, 0.01)

		// String
		result, err = converter.ToFloat64("3.14")
		require.NoError(t, err)
		assert.InDelta(t, 3.14, result, 0.01)

		// Bool
		result, err = converter.ToFloat64(true)
		require.NoError(t, err)
		assert.InEpsilon(t, 1.0, result, 0.001)

		// Invalid string
		_, err = converter.ToFloat64("not a number")
		require.Error(t, err)
	})

	t.Run("ToString", func(t *testing.T) {
		assert.Equal(t, "hello", converter.ToString("hello"))
		assert.Equal(t, "42", converter.ToString(42))
		assert.Equal(t, "3.14", converter.ToString(3.14))
		assert.Equal(t, "true", converter.ToString(true))
		assert.Equal(t, "false", converter.ToString(false))
	})

	t.Run("ToBool", func(t *testing.T) {
		// Bool
		result, err := converter.ToBool(true)
		require.NoError(t, err)
		assert.True(t, result)

		// Integer types
		result, err = converter.ToBool(1)
		require.NoError(t, err)
		assert.True(t, result)

		result, err = converter.ToBool(0)
		require.NoError(t, err)
		assert.False(t, result)

		// Float types
		result, err = converter.ToBool(1.0)
		require.NoError(t, err)
		assert.True(t, result)

		result, err = converter.ToBool(0.0)
		require.NoError(t, err)
		assert.False(t, result)

		// String
		result, err = converter.ToBool("true")
		require.NoError(t, err)
		assert.True(t, result)

		result, err = converter.ToBool("false")
		require.NoError(t, err)
		assert.False(t, result)

		// Invalid string
		_, err = converter.ToBool("maybe")
		require.Error(t, err)
	})

	t.Run("Type checking methods", func(t *testing.T) {
		assert.True(t, converter.IsNumericType(42))
		assert.True(t, converter.IsNumericType(3.14))
		assert.False(t, converter.IsNumericType("42"))

		assert.True(t, converter.IsIntegerType(42))
		assert.False(t, converter.IsIntegerType(3.14))

		assert.True(t, converter.IsFloatType(3.14))
		assert.False(t, converter.IsFloatType(42))
	})

	t.Run("GetTypeName", func(t *testing.T) {
		assert.Equal(t, "int", converter.GetTypeName(42))
		assert.Equal(t, "int32", converter.GetTypeName(int32(42)))
		assert.Equal(t, "float64", converter.GetTypeName(3.14))
		assert.Equal(t, "string", converter.GetTypeName("hello"))
		assert.Equal(t, "bool", converter.GetTypeName(true))
	})
}

func TestDefaultConverterFunctions(t *testing.T) {
	t.Run("SafeInt64ToInt", func(t *testing.T) {
		result, err := common.SafeInt64ToInt(42)
		require.NoError(t, err)
		assert.Equal(t, 42, result)
	})

	t.Run("ToInt64", func(t *testing.T) {
		result, err := common.ToInt64(42)
		require.NoError(t, err)
		assert.Equal(t, int64(42), result)
	})

	t.Run("ToFloat64", func(t *testing.T) {
		result, err := common.ToFloat64(42)
		require.NoError(t, err)
		assert.InEpsilon(t, float64(42), result, 0.001)
	})

	t.Run("ToString", func(t *testing.T) {
		assert.Equal(t, "42", common.ToString(42))
	})

	t.Run("ToBool", func(t *testing.T) {
		result, err := common.ToBool(1)
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("Type checking", func(t *testing.T) {
		assert.True(t, common.IsNumericType(42))
		assert.True(t, common.IsIntegerType(42))
		assert.True(t, common.IsFloatType(3.14))
	})

	t.Run("GetTypeName", func(t *testing.T) {
		assert.Equal(t, "int", common.GetTypeName(42))
	})
}
