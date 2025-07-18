package expr

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestColumnExpr_ArithmeticOperations tests arithmetic operations on ColumnExpr
func TestColumnExpr_ArithmeticOperations(t *testing.T) {
	col := Col("value")
	
	tests := []struct {
		name     string
		expr     *BinaryExpr
		expected BinaryOp
	}{
		{
			name:     "column subtraction",
			expr:     col.Sub(Lit(3)),
			expected: OpSub,
		},
		{
			name:     "column division",
			expr:     col.Div(Lit(4)),
			expected: OpDiv,
		},
		{
			name:     "column multiplication", 
			expr:     col.Mul(Lit(6)),
			expected: OpMul,
		},
		{
			name:     "column addition",
			expr:     col.Add(Lit(5)),
			expected: OpAdd,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotNil(t, tt.expr)
			assert.Equal(t, ExprBinary, tt.expr.Type())
			assert.Equal(t, tt.expected, tt.expr.Op())
		})
	}
}

// TestColumnExpr_ComparisonOperations tests comparison operations on ColumnExpr
func TestColumnExpr_ComparisonOperations(t *testing.T) {
	col := Col("value")
	
	tests := []struct {
		name  string
		expr  *BinaryExpr
		op    BinaryOp
	}{
		{
			name: "column equals",
			expr: col.Eq(Lit(5)),
			op:   OpEq,
		},
		{
			name: "column not equals",
			expr: col.Ne(Lit(3)),
			op:   OpNe,
		},
		{
			name: "column less than",
			expr: col.Lt(Lit(5)),
			op:   OpLt,
		},
		{
			name: "column less than or equal",
			expr: col.Le(Lit(5)),
			op:   OpLe,
		},
		{
			name: "column greater than or equal",
			expr: col.Ge(Lit(3)),
			op:   OpGe,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotNil(t, tt.expr)
			assert.Equal(t, ExprBinary, tt.expr.Type())
			assert.Equal(t, tt.op, tt.expr.Op())
		})
	}
}

// TestColumnExpr_MathFunctions tests math functions on ColumnExpr
func TestColumnExpr_MathFunctions(t *testing.T) {
	col := Col("value")

	tests := []struct {
		name     string
		function *FunctionExpr
		funcName string
	}{
		{"abs", col.Abs(), "abs"},
		{"round", col.Round(), "round"},
		{"round_to", col.RoundTo(Lit(2)), "round"},
		{"floor", col.Floor(), "floor"},
		{"ceil", col.Ceil(), "ceil"},
		{"sqrt", col.Sqrt(), "sqrt"},
		{"log", col.Log(), "log"},
		{"sin", col.Sin(), "sin"},
		{"cos", col.Cos(), "cos"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotNil(t, tt.function)
			assert.Equal(t, ExprFunction, tt.function.Type())
			assert.Equal(t, tt.funcName, tt.function.Name())
		})
	}
}

// TestColumnExpr_StringFunctions tests string functions on ColumnExpr
func TestColumnExpr_StringFunctions(t *testing.T) {
	col := Col("text")

	tests := []struct {
		name     string
		function *FunctionExpr
		funcName string
	}{
		{"upper", col.Upper(), "upper"},
		{"lower", col.Lower(), "lower"},
		{"length", col.Length(), "length"},
		{"trim", col.Trim(), "trim"},
		{"substring", col.Substring(Lit(0), Lit(5)), "substring"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotNil(t, tt.function)
			assert.Equal(t, ExprFunction, tt.function.Type())
			assert.Equal(t, tt.funcName, tt.function.Name())
		})
	}
}

// TestColumnExpr_CastFunctions tests cast functions on ColumnExpr
func TestColumnExpr_CastFunctions(t *testing.T) {
	col := Col("value")

	tests := []struct {
		name     string
		function *FunctionExpr
		funcName string
	}{
		{"cast_to_string", col.CastToString(), "cast_string"},
		{"cast_to_int64", col.CastToInt64(), "cast_int64"},
		{"cast_to_float64", col.CastToFloat64(), "cast_float64"},
		{"cast_to_bool", col.CastToBool(), "cast_bool"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotNil(t, tt.function)
			assert.Equal(t, ExprFunction, tt.function.Type())
			assert.Equal(t, tt.funcName, tt.function.Name())
		})
	}
}

// TestColumnExpr_DateTimeFunctions tests datetime functions on ColumnExpr
func TestColumnExpr_DateTimeFunctions(t *testing.T) {
	col := Col("timestamp")

	tests := []struct {
		name     string
		function *FunctionExpr
		funcName string
	}{
		{"year", col.Year(), "year"},
		{"month", col.Month(), "month"},
		{"day", col.Day(), "day"},
		{"hour", col.Hour(), "hour"},
		{"minute", col.Minute(), "minute"},
		{"second", col.Second(), "second"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotNil(t, tt.function)
			assert.Equal(t, ExprFunction, tt.function.Type())
			assert.Equal(t, tt.funcName, tt.function.Name())
		})
	}
}

// TestEvaluator_BooleanOperations tests boolean evaluation
func TestEvaluator_BooleanOperations(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data
	boolBuilder := array.NewBooleanBuilder(mem)
	defer boolBuilder.Release()
	
	boolBuilder.AppendValues([]bool{true, false, true, false}, nil)
	boolArray := boolBuilder.NewArray()
	defer boolArray.Release()

	data := map[string]arrow.Array{
		"bool_col": boolArray,
	}

	evaluator := NewEvaluator(mem)

	t.Run("evaluate boolean column", func(t *testing.T) {
		result, err := evaluator.EvaluateBoolean(Col("bool_col"), data)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 4, result.Len())
		boolResult := result.(*array.Boolean)
		assert.Equal(t, true, boolResult.Value(0))
		assert.Equal(t, false, boolResult.Value(1))
	})

	t.Run("evaluate boolean literal", func(t *testing.T) {
		result, err := evaluator.EvaluateBoolean(Lit(true), data)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 4, result.Len()) // Literal is broadcast to match data size
		boolResult := result.(*array.Boolean)
		assert.Equal(t, true, boolResult.Value(0))
	})

	t.Run("evaluate boolean comparison", func(t *testing.T) {
		result, err := evaluator.EvaluateBoolean(Col("bool_col").Eq(Lit(true)), data)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 4, result.Len())
		boolResult := result.(*array.Boolean)
		assert.Equal(t, true, boolResult.Value(0))
		assert.Equal(t, false, boolResult.Value(1))
		assert.Equal(t, true, boolResult.Value(2))
		assert.Equal(t, false, boolResult.Value(3))
	})
}

// TestEvaluator_Int32Float32Operations tests int32 and float32 operations
func TestEvaluator_Int32Float32Operations(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create int32 test data
	int32Builder := array.NewInt32Builder(mem)
	defer int32Builder.Release()
	int32Builder.AppendValues([]int32{10, 20, 30, 40}, nil)
	int32Array := int32Builder.NewArray()
	defer int32Array.Release()

	// Create float32 test data
	float32Builder := array.NewFloat32Builder(mem)
	defer float32Builder.Release()
	float32Builder.AppendValues([]float32{1.5, 2.5, 3.5, 4.5}, nil)
	float32Array := float32Builder.NewArray()
	defer float32Array.Release()

	data := map[string]arrow.Array{
		"int32_col":   int32Array,
		"float32_col": float32Array,
	}

	evaluator := NewEvaluator(mem)

	t.Run("int32 arithmetic", func(t *testing.T) {
		// Test addition
		result, err := evaluator.Evaluate(Col("int32_col").Add(Lit(int32(5))), data)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 4, result.Len())
		// Check values
		i32Result := result.(*array.Int32)
		assert.Equal(t, int32(15), i32Result.Value(0))
		assert.Equal(t, int32(25), i32Result.Value(1))
	})

	t.Run("float32 arithmetic", func(t *testing.T) {
		// Test multiplication
		result, err := evaluator.Evaluate(Col("float32_col").Mul(Lit(float32(2.0))), data)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 4, result.Len())
		// Check values
		f32Result := result.(*array.Float32)
		assert.Equal(t, float32(3.0), f32Result.Value(0))
		assert.Equal(t, float32(5.0), f32Result.Value(1))
	})

	t.Run("int32 comparison", func(t *testing.T) {
		result, err := evaluator.EvaluateBoolean(Col("int32_col").Gt(Lit(int32(25))), data)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 4, result.Len())
		boolResult := result.(*array.Boolean)
		assert.Equal(t, false, boolResult.Value(0))
		assert.Equal(t, false, boolResult.Value(1))
		assert.Equal(t, true, boolResult.Value(2))
		assert.Equal(t, true, boolResult.Value(3))
	})

	t.Run("float32 comparison", func(t *testing.T) {
		result, err := evaluator.EvaluateBoolean(Col("float32_col").Le(Lit(float32(3.0))), data)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 4, result.Len())
		boolResult := result.(*array.Boolean)
		assert.Equal(t, true, boolResult.Value(0))
		assert.Equal(t, true, boolResult.Value(1))
		assert.Equal(t, false, boolResult.Value(2))
		assert.Equal(t, false, boolResult.Value(3))
	})
}