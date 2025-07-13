package expr

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnhancedArithmeticTypeCoercion(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Test int32 + int64 coercion
	t.Run("Int32_Int64_Addition", func(t *testing.T) {
		int32Series := series.New("a", []int32{1, 2, 3}, mem)
		defer int32Series.Release()
		int64Series := series.New("b", []int64{10, 20, 30}, mem)
		defer int64Series.Release()

		columns := map[string]arrow.Array{
			"a": int32Series.Array(),
			"b": int64Series.Array(),
		}

		// int32 + int64 should promote to int64
		expr := Col("a").Add(Col("b"))
		eval := NewEvaluator(nil)
		result, err := eval.Evaluate(expr, columns)
		require.NoError(t, err)
		defer result.Release()

		// Verify result is int64 array with correct values
		assert.Equal(t, "int64", result.DataType().Name())
		int64Result := result.(*array.Int64)
		assert.Equal(t, []int64{11, 22, 33}, int64Result.Int64Values())
	})

	// Test float32 + float64 coercion
	t.Run("Float32_Float64_Addition", func(t *testing.T) {
		float32Series := series.New("a", []float32{1.5, 2.5, 3.5}, mem)
		defer float32Series.Release()
		float64Series := series.New("b", []float64{10.1, 20.2, 30.3}, mem)
		defer float64Series.Release()

		columns := map[string]arrow.Array{
			"a": float32Series.Array(),
			"b": float64Series.Array(),
		}

		// float32 + float64 should promote to float64
		expr := Col("a").Add(Col("b"))
		eval := NewEvaluator(nil)
		result, err := eval.Evaluate(expr, columns)
		require.NoError(t, err)
		defer result.Release()

		// Verify result is float64 array with correct values
		assert.Equal(t, "float64", result.DataType().Name())
		float64Result := result.(*array.Float64)
		expected := []float64{11.6, 22.7, 33.8}
		for i, val := range float64Result.Float64Values() {
			assert.InDelta(t, expected[i], val, 0.0001)
		}
	})

	// Test int32 + float64 coercion
	t.Run("Int32_Float64_Addition", func(t *testing.T) {
		int32Series := series.New("a", []int32{1, 2, 3}, mem)
		defer int32Series.Release()
		float64Series := series.New("b", []float64{10.5, 20.5, 30.5}, mem)
		defer float64Series.Release()

		columns := map[string]arrow.Array{
			"a": int32Series.Array(),
			"b": float64Series.Array(),
		}

		// int32 + float64 should promote to float64
		expr := Col("a").Add(Col("b"))
		eval := NewEvaluator(nil)
		result, err := eval.Evaluate(expr, columns)
		require.NoError(t, err)
		defer result.Release()

		// Verify result is float64 array with correct values
		assert.Equal(t, "float64", result.DataType().Name())
		float64Result := result.(*array.Float64)
		expected := []float64{11.5, 22.5, 33.5}
		for i, val := range float64Result.Float64Values() {
			assert.InDelta(t, expected[i], val, 0.0001)
		}
	})
}

func TestEnhancedComparisonTypeCoercion(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Test int32 vs int64 comparison
	t.Run("Int32_Int64_Comparison", func(t *testing.T) {
		int32Series := series.New("a", []int32{1, 2, 3}, mem)
		defer int32Series.Release()
		int64Series := series.New("b", []int64{2, 2, 2}, mem)
		defer int64Series.Release()

		columns := map[string]arrow.Array{
			"a": int32Series.Array(),
			"b": int64Series.Array(),
		}

		// int32 > int64 should work with type coercion
		expr := Col("a").Gt(Col("b"))
		eval := NewEvaluator(nil)
		result, err := eval.EvaluateBoolean(expr, columns)
		require.NoError(t, err)
		defer result.Release()

		// Verify result: [false, false, true]
		boolResult := result.(*array.Boolean)
		expected := []bool{false, false, true}
		for i := 0; i < boolResult.Len(); i++ {
			assert.Equal(t, expected[i], boolResult.Value(i))
		}
	})

	// Test float32 vs float64 comparison
	t.Run("Float32_Float64_Comparison", func(t *testing.T) {
		float32Series := series.New("a", []float32{1.5, 2.5, 3.5}, mem)
		defer float32Series.Release()
		float64Series := series.New("b", []float64{2.0, 2.0, 2.0}, mem)
		defer float64Series.Release()

		columns := map[string]arrow.Array{
			"a": float32Series.Array(),
			"b": float64Series.Array(),
		}

		// float32 > float64 should work with type coercion
		expr := Col("a").Gt(Col("b"))
		eval := NewEvaluator(nil)
		result, err := eval.EvaluateBoolean(expr, columns)
		require.NoError(t, err)
		defer result.Release()

		// Verify result: [false, true, true]
		boolResult := result.(*array.Boolean)
		expected := []bool{false, true, true}
		for i := 0; i < boolResult.Len(); i++ {
			assert.Equal(t, expected[i], boolResult.Value(i))
		}
	})
}

func TestTypePromotionRules(t *testing.T) {
	// Test the type promotion hierarchy
	tests := []struct {
		name         string
		leftType     string
		rightType    string
		expectedType string
	}{
		{"int32_int64", "int32", "int64", "int64"},
		{"int64_int32", "int64", "int32", "int64"},
		{"float32_float64", "float32", "float64", "float64"},
		{"float64_float32", "float64", "float32", "float64"},
		{"int32_float32", "int32", "float32", "float32"},
		{"int32_float64", "int32", "float64", "float64"},
		{"int64_float32", "int64", "float32", "float64"}, // Promote to float64 for precision
		{"int64_float64", "int64", "float64", "float64"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resultType := getPromotedType(tt.leftType, tt.rightType)
			assert.Equal(t, tt.expectedType, resultType,
				"Type promotion %s + %s should result in %s",
				tt.leftType, tt.rightType, tt.expectedType)
		})
	}
}

// getPromotedType determines the promoted type for mixed arithmetic operations
func getPromotedType(leftType, rightType string) string {
	// Type promotion hierarchy for arithmetic operations
	typeHierarchy := map[string]int{
		"int32":   1,
		"int64":   2,
		"float32": 3,
		"float64": 4,
	}

	leftLevel, leftExists := typeHierarchy[leftType]
	rightLevel, rightExists := typeHierarchy[rightType]

	if !leftExists || !rightExists {
		return "unknown"
	}

	// Special case: int64 + float32 should promote to float64 for precision
	if (leftType == "int64" && rightType == "float32") || (leftType == "float32" && rightType == "int64") {
		return "float64"
	}

	// Return the higher type in the hierarchy
	if leftLevel > rightLevel {
		return leftType
	}
	return rightType
}
