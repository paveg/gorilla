package expr

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestColumns(t *testing.T, mem memory.Allocator) map[string]arrow.Array {
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
	eval := NewEvaluator(mem)
	assert.NotNil(t, eval)
	assert.Equal(t, mem, eval.mem)
	
	// Test with nil allocator (should create default)
	eval2 := NewEvaluator(nil)
	assert.NotNil(t, eval2)
	assert.NotNil(t, eval2.mem)
}

func TestEvaluateColumn(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)
	columns := createTestColumns(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()
	
	// Test evaluating existing column
	colExpr := Col("age")
	result, err := eval.Evaluate(colExpr, columns)
	require.NoError(t, err)
	defer result.Release()
	
	intResult, ok := result.(*array.Int64)
	require.True(t, ok)
	assert.Equal(t, 4, intResult.Len())
	assert.Equal(t, int64(10), intResult.Value(0))
	assert.Equal(t, int64(20), intResult.Value(1))
	
	// Test evaluating non-existent column
	nonExistentExpr := Col("nonexistent")
	_, err = eval.Evaluate(nonExistentExpr, columns)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "column not found")
}

func TestEvaluateLiteral(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)
	columns := createTestColumns(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()
	
	tests := []struct {
		name          string
		value         interface{}
		expectedType  string
		expectedValue interface{}
	}{
		{"int64 literal", int64(42), "int64", int64(42)},
		{"float64 literal", 3.14, "float64", 3.14},
		{"string literal", "hello", "utf8", "hello"},
		{"bool literal", true, "bool", true},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			litExpr := Lit(tt.value)
			result, err := eval.Evaluate(litExpr, columns)
			require.NoError(t, err)
			defer result.Release()
			
			assert.Equal(t, 4, result.Len()) // Should match column length
			assert.Equal(t, tt.expectedType, result.DataType().Name())
			
			// Check that all values are the literal value
			switch arr := result.(type) {
			case *array.Int64:
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, tt.expectedValue, arr.Value(i))
				}
			case *array.Float64:
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, tt.expectedValue, arr.Value(i))
				}
			case *array.String:
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, tt.expectedValue, arr.Value(i))
				}
			case *array.Boolean:
				for i := 0; i < arr.Len(); i++ {
					assert.Equal(t, tt.expectedValue, arr.Value(i))
				}
			}
		})
	}
}

func TestEvaluateArithmetic(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)
	columns := createTestColumns(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()
	
	tests := []struct {
		name     string
		expr     Expr
		expected []int64
	}{
		{
			name:     "addition",
			expr:     Col("age").Add(Lit(int64(5))),
			expected: []int64{15, 25, 35, 45},
		},
		{
			name:     "subtraction",
			expr:     Col("age").Sub(Lit(int64(5))),
			expected: []int64{5, 15, 25, 35},
		},
		{
			name:     "multiplication",
			expr:     Col("age").Mul(Lit(int64(2))),
			expected: []int64{20, 40, 60, 80},
		},
		{
			name:     "division",
			expr:     Col("age").Div(Lit(int64(2))),
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
	eval := NewEvaluator(mem)
	columns := createTestColumns(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()
	
	tests := []struct {
		name     string
		expr     Expr
		expected []bool
	}{
		{
			name:     "greater than",
			expr:     Col("age").Gt(Lit(int64(25))),
			expected: []bool{false, false, true, true},
		},
		{
			name:     "less than",
			expr:     Col("age").Lt(Lit(int64(25))),
			expected: []bool{true, true, false, false},
		},
		{
			name:     "equal",
			expr:     Col("age").Eq(Lit(int64(20))),
			expected: []bool{false, true, false, false},
		},
		{
			name:     "not equal",
			expr:     Col("age").Ne(Lit(int64(20))),
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
	eval := NewEvaluator(mem)
	columns := createTestColumns(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()
	
	// Test logical AND: (age > 15) && active
	expr := Col("age").Gt(Lit(int64(15))).And(Col("active"))
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
	eval := NewEvaluator(mem)
	columns := createTestColumns(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()
	
	// Test complex expression: (age * 2) > 50
	left := Col("age").Mul(Lit(int64(2)))
	expr := &BinaryExpr{left: left, op: OpGt, right: Lit(int64(50))}
	result, err := eval.EvaluateBoolean(expr, columns)
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
	eval := NewEvaluator(mem)
	columns := createTestColumns(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()
	
	// Test string equality
	expr := Col("name").Eq(Lit("b"))
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
	eval := NewEvaluator(mem)
	columns := createTestColumns(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()
	
	// Test unsupported expression type
	_, err := eval.Evaluate(nil, columns)
	assert.Error(t, err)
	
	// Test column not found
	_, err = eval.Evaluate(Col("nonexistent"), columns)
	assert.Error(t, err)
	
	// Test unsupported literal type
	_, err = eval.Evaluate(Lit(complex(1, 2)), columns)
	assert.Error(t, err)
}