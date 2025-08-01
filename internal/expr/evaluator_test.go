package expr_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestColumns(_ *testing.T, mem memory.Allocator) map[string]arrow.Array {
	// Create test data
	intBuilder := array.NewInt64Builder(mem)
	defer intBuilder.Release()
	intBuilder.AppendValues([]int64{10, 20, 30, 40}, nil)
	intArray := intBuilder.NewArray()

	floatBuilder := array.NewFloat64Builder(mem)
	defer floatBuilder.Release()
	floatBuilder.AppendValues([]float64{1.5, 2.5, 3.5, 4.5}, nil)
	floatArray := floatBuilder.NewArray()

	stringBuilder := array.NewStringBuilder(mem)
	defer stringBuilder.Release()
	stringBuilder.AppendValues([]string{"a", "b", "c", "d"}, nil)
	stringArray := stringBuilder.NewArray()

	boolBuilder := array.NewBooleanBuilder(mem)
	defer boolBuilder.Release()
	boolBuilder.AppendValues([]bool{true, false, true, false}, nil)
	boolArray := boolBuilder.NewArray()

	return map[string]arrow.Array{
		"age":    intArray,
		"score":  floatArray,
		"name":   stringArray,
		"active": boolArray,
	}
}

func TestNewEvaluator(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Test with explicit allocator
	eval := expr.NewEvaluator(mem)
	assert.NotNil(t, eval)

	// Test with nil allocator (should create default)
	eval2 := expr.NewEvaluator(nil)
	assert.NotNil(t, eval2)
}

func TestEvaluateColumn(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := expr.NewEvaluator(mem)
	columns := createTestColumns(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	// Test evaluating existing column
	colExpr := expr.Col("age")
	result, err := eval.Evaluate(colExpr, columns)
	require.NoError(t, err)
	defer result.Release()

	intResult, ok := result.(*array.Int64)
	require.True(t, ok)
	assert.Equal(t, 4, intResult.Len())
	assert.Equal(t, int64(10), intResult.Value(0))
	assert.Equal(t, int64(20), intResult.Value(1))

	// Test evaluating non-existent column
	nonExistentExpr := expr.Col("nonexistent")
	_, err = eval.Evaluate(nonExistentExpr, columns)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Column 'nonexistent' does not exist")
}

func TestEvaluateLiteral(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := expr.NewEvaluator(mem)
	columns := createTestColumns(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	tests := getLiteralTestCases()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runLiteralTest(t, eval, columns, tt)
		})
	}
}

// literalTestCase represents a test case for literal evaluation.
type literalTestCase struct {
	name          string
	value         interface{}
	expectedType  string
	expectedValue interface{}
}

// getLiteralTestCases returns all literal test cases.
func getLiteralTestCases() []literalTestCase {
	return []literalTestCase{
		{"int64 literal", int64(42), "int64", int64(42)},
		{"float64 literal", 3.14, "float64", 3.14},
		{"string literal", "hello", "utf8", "hello"},
		{"bool literal", true, "bool", true},
	}
}

// runLiteralTest runs a single literal test case.
func runLiteralTest(t *testing.T, eval *expr.Evaluator, columns map[string]arrow.Array, tc literalTestCase) {
	litExpr := expr.Lit(tc.value)
	result, err := eval.Evaluate(litExpr, columns)
	require.NoError(t, err)
	defer result.Release()

	assert.Equal(t, 4, result.Len()) // Should match column length.
	assert.Equal(t, tc.expectedType, result.DataType().Name())

	verifyLiteralValues(t, result, tc.expectedValue)
}

// verifyLiteralValues verifies that all values in the array match the expected literal value.
func verifyLiteralValues(t *testing.T, result arrow.Array, expectedValue interface{}) {
	switch arr := result.(type) {
	case *array.Int64:
		verifyInt64Values(t, arr, expectedValue)
	case *array.Float64:
		verifyFloat64Values(t, arr, expectedValue)
	case *array.String:
		verifyStringValues(t, arr, expectedValue)
	case *array.Boolean:
		verifyBooleanValues(t, arr, expectedValue)
	}
}

// verifyInt64Values verifies int64 array values.
func verifyInt64Values(t *testing.T, arr *array.Int64, expectedValue interface{}) {
	for i := range arr.Len() {
		assert.Equal(t, expectedValue, arr.Value(i))
	}
}

// verifyFloat64Values verifies float64 array values.
func verifyFloat64Values(t *testing.T, arr *array.Float64, expectedValue interface{}) {
	for i := range arr.Len() {
		assert.InDelta(t, expectedValue, arr.Value(i), 0.001)
	}
}

// verifyStringValues verifies string array values.
func verifyStringValues(t *testing.T, arr *array.String, expectedValue interface{}) {
	for i := range arr.Len() {
		assert.Equal(t, expectedValue, arr.Value(i))
	}
}

// verifyBooleanValues verifies boolean array values.
func verifyBooleanValues(t *testing.T, arr *array.Boolean, expectedValue interface{}) {
	for i := range arr.Len() {
		assert.Equal(t, expectedValue, arr.Value(i))
	}
}

func TestEvaluateArithmetic(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := expr.NewEvaluator(mem)
	columns := createTestColumns(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	tests := []struct {
		name     string
		expr     expr.Expr
		expected []int64
	}{
		{
			name:     "addition",
			expr:     expr.Col("age").Add(expr.Lit(int64(5))),
			expected: []int64{15, 25, 35, 45},
		},
		{
			name:     "subtraction",
			expr:     expr.Col("age").Sub(expr.Lit(int64(5))),
			expected: []int64{5, 15, 25, 35},
		},
		{
			name:     "multiplication",
			expr:     expr.Col("age").Mul(expr.Lit(int64(2))),
			expected: []int64{20, 40, 60, 80},
		},
		{
			name:     "division",
			expr:     expr.Col("age").Div(expr.Lit(int64(2))),
			expected: []int64{5, 10, 15, 20},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := eval.Evaluate(tt.expr, columns)
			require.NoError(t, err)
			defer result.Release()

			intResult, ok := result.(*array.Int64)
			require.True(t, ok)
			assert.Equal(t, len(tt.expected), intResult.Len())

			for i, expected := range tt.expected {
				assert.Equal(t, expected, intResult.Value(i))
			}
		})
	}
}

func TestEvaluateComparison(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := expr.NewEvaluator(mem)
	columns := createTestColumns(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	tests := []struct {
		name     string
		expr     expr.Expr
		expected []bool
	}{
		{
			name:     "greater than",
			expr:     expr.Col("age").Gt(expr.Lit(int64(25))),
			expected: []bool{false, false, true, true},
		},
		{
			name:     "less than",
			expr:     expr.Col("age").Lt(expr.Lit(int64(25))),
			expected: []bool{true, true, false, false},
		},
		{
			name:     "equal",
			expr:     expr.Col("age").Eq(expr.Lit(int64(20))),
			expected: []bool{false, true, false, false},
		},
		{
			name:     "not equal",
			expr:     expr.Col("age").Ne(expr.Lit(int64(20))),
			expected: []bool{true, false, true, true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := eval.EvaluateBoolean(tt.expr, columns)
			require.NoError(t, err)
			defer result.Release()

			boolResult, ok := result.(*array.Boolean)
			require.True(t, ok)
			assert.Equal(t, len(tt.expected), boolResult.Len())

			for i, expected := range tt.expected {
				assert.Equal(t, expected, boolResult.Value(i))
			}
		})
	}
}

func TestEvaluateLogical(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := expr.NewEvaluator(mem)
	columns := createTestColumns(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	// Test logical AND: (age > 15) && active
	expr := expr.Col("age").Gt(expr.Lit(int64(15))).And(expr.Col("active"))
	result, err := eval.EvaluateBoolean(expr, columns)
	require.NoError(t, err)
	defer result.Release()

	boolResult, ok := result.(*array.Boolean)
	require.True(t, ok)

	// Expected: [false, false, true, false]
	// age > 15: [false, true, true, true]
	// active:   [true, false, true, false]
	// AND:      [false, false, true, false]
	expected := []bool{false, false, true, false}
	for i, exp := range expected {
		assert.Equal(t, exp, boolResult.Value(i))
	}
}

func TestEvaluateComplexExpression(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := expr.NewEvaluator(mem)
	columns := createTestColumns(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	// Test complex expression: (age * 2) > 50
	left := expr.Col("age").Mul(expr.Lit(int64(2)))
	complexExpr := left.Gt(expr.Lit(int64(50)))
	result, err := eval.EvaluateBoolean(complexExpr, columns)
	require.NoError(t, err)
	defer result.Release()

	boolResult, ok := result.(*array.Boolean)
	require.True(t, ok)

	// age * 2: [20, 40, 60, 80]
	// > 50:    [false, false, true, true]
	expected := []bool{false, false, true, true}
	for i, exp := range expected {
		assert.Equal(t, exp, boolResult.Value(i))
	}
}

func TestEvaluateStringComparison(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := expr.NewEvaluator(mem)
	columns := createTestColumns(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	// Test string equality
	expr := expr.Col("name").Eq(expr.Lit("b"))
	result, err := eval.EvaluateBoolean(expr, columns)
	require.NoError(t, err)
	defer result.Release()

	boolResult, ok := result.(*array.Boolean)
	require.True(t, ok)

	// name == "b": [false, true, false, false]
	expected := []bool{false, true, false, false}
	for i, exp := range expected {
		assert.Equal(t, exp, boolResult.Value(i))
	}
}

func TestEvaluateErrors(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := expr.NewEvaluator(mem)
	columns := createTestColumns(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	// Test unsupported expression type
	_, err := eval.Evaluate(nil, columns)
	require.Error(t, err)

	// Test column not found
	_, err = eval.Evaluate(expr.Col("nonexistent"), columns)
	require.Error(t, err)

	// Test unsupported literal type
	_, err = eval.Evaluate(expr.Lit(complex(1, 2)), columns)
	require.Error(t, err)
}

func TestEvaluateInvalidExpr(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := expr.NewEvaluator(mem)
	columns := createTestColumns(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	t.Run("evaluate invalid expression", func(t *testing.T) {
		invalidExpr := expr.Invalid("test error message")

		result, err := eval.Evaluate(invalidExpr, columns)

		assert.Nil(t, result)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid expression")
		assert.Contains(t, err.Error(), "test error message")
	})

	t.Run("evaluate boolean invalid expression", func(t *testing.T) {
		invalidExpr := expr.Invalid("boolean operation not supported")

		result, err := eval.EvaluateBoolean(invalidExpr, columns)

		assert.Nil(t, result)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid expression")
		assert.Contains(t, err.Error(), "boolean operation not supported")
	})

	t.Run("empty error message", func(t *testing.T) {
		invalidExpr := expr.Invalid("")

		result, err := eval.Evaluate(invalidExpr, columns)

		assert.Nil(t, result)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid expression")
	})

	t.Run("complex error scenario", func(t *testing.T) {
		complexMessage := "Add operation only supported on column and binary expressions, got *expr.LiteralExpr"
		invalidExpr := expr.Invalid(complexMessage)

		result, err := eval.EvaluateBoolean(invalidExpr, columns)

		assert.Nil(t, result)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid expression")
		assert.Contains(t, err.Error(), complexMessage)
	})
}
