package expr

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntLiteralSupport(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)

	// Create a test int64 column
	builder := array.NewInt64Builder(mem)
	defer builder.Release()

	values := []int64{25, 30, 35, 28}
	for _, v := range values {
		builder.Append(v)
	}

	intArray := builder.NewArray()
	defer intArray.Release()

	columns := map[string]arrow.Array{
		"age": intArray,
	}

	tests := []struct {
		name        string
		literal     interface{}
		expectError bool
		description string
	}{
		{
			name:        "int literal (platform-dependent)",
			literal:     30, // This is of type `int`
			expectError: false,
			description: "Should handle untyped integer literals",
		},
		{
			name:        "int32 literal",
			literal:     int32(30),
			expectError: false,
			description: "Should handle explicit int32 literals",
		},
		{
			name:        "int64 literal",
			literal:     int64(30),
			expectError: false,
			description: "Should handle explicit int64 literals",
		},
		{
			name:        "float64 literal",
			literal:     30.0,
			expectError: false,
			description: "Should handle float64 literals",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create literal expression
			literalExpr := Lit(tt.literal)

			// Create comparison expression: age > literal
			compExpr := Col("age").Gt(literalExpr)

			// Evaluate the boolean expression
			result, err := eval.EvaluateBoolean(compExpr, columns)

			if tt.expectError {
				assert.Error(t, err, tt.description)
				return
			}

			require.NoError(t, err, tt.description)
			defer result.Release()

			// Check that result is a boolean array
			boolArray, ok := result.(*array.Boolean)
			require.True(t, ok, "Result should be a boolean array")

			// Check that the filtering logic works correctly
			// Values: [25, 30, 35, 28], comparing with 30
			// Expected: [false, false, true, false] (only 35 > 30)
			assert.Equal(t, 4, boolArray.Len(), "Should have 4 boolean values")

			expectedResults := []bool{false, false, true, false}
			for i := 0; i < boolArray.Len(); i++ {
				assert.Equal(t, expectedResults[i], boolArray.Value(i),
					"Row %d: expected %v, got %v", i, expectedResults[i], boolArray.Value(i))
			}
		})
	}
}

func TestIntLiteralArithmetic(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)

	// Create a test int64 column
	builder := array.NewInt64Builder(mem)
	defer builder.Release()

	values := []int64{10, 20, 30}
	for _, v := range values {
		builder.Append(v)
	}

	intArray := builder.NewArray()
	defer intArray.Release()

	columns := map[string]arrow.Array{
		"value": intArray,
	}

	// Test arithmetic with int literal: value + 5
	expr := Col("value").Add(Lit(5)) // 5 is of type `int`

	result, err := eval.Evaluate(expr, columns)
	require.NoError(t, err)
	defer result.Release()

	// Should get an int64 array (promoted type)
	int64Array, ok := result.(*array.Int64)
	require.True(t, ok, "Result should be an int64 array")

	// Check results: [10+5, 20+5, 30+5] = [15, 25, 35]
	expectedResults := []int64{15, 25, 35}
	assert.Equal(t, 3, int64Array.Len())

	for i := 0; i < int64Array.Len(); i++ {
		assert.Equal(t, expectedResults[i], int64Array.Value(i),
			"Row %d: expected %v, got %v", i, expectedResults[i], int64Array.Value(i))
	}
}
