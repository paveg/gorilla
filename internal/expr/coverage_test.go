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

// TestColumnExpr_ArithmeticOperations tests arithmetic operations on ColumnExpr.
func TestColumnExpr_ArithmeticOperations(t *testing.T) {
	col := expr.Col("value")

	tests := []struct {
		name     string
		expr     *expr.BinaryExpr
		expected expr.BinaryOp
	}{
		{
			name:     "column subtraction",
			expr:     col.Sub(expr.Lit(3)),
			expected: expr.OpSub,
		},
		{
			name:     "column division",
			expr:     col.Div(expr.Lit(4)),
			expected: expr.OpDiv,
		},
		{
			name:     "column multiplication",
			expr:     col.Mul(expr.Lit(6)),
			expected: expr.OpMul,
		},
		{
			name:     "column addition",
			expr:     col.Add(expr.Lit(5)),
			expected: expr.OpAdd,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotNil(t, tt.expr)
			assert.Equal(t, expr.ExprBinary, tt.expr.Type())
			assert.Equal(t, tt.expected, tt.expr.Op())
		})
	}
}

// TestColumnExpr_ComparisonOperations tests comparison operations on ColumnExpr.
func TestColumnExpr_ComparisonOperations(t *testing.T) {
	col := expr.Col("value")

	tests := []struct {
		name string
		expr *expr.BinaryExpr
		op   expr.BinaryOp
	}{
		{
			name: "column equals",
			expr: col.Eq(expr.Lit(5)),
			op:   expr.OpEq,
		},
		{
			name: "column not equals",
			expr: col.Ne(expr.Lit(3)),
			op:   expr.OpNe,
		},
		{
			name: "column less than",
			expr: col.Lt(expr.Lit(5)),
			op:   expr.OpLt,
		},
		{
			name: "column less than or equal",
			expr: col.Le(expr.Lit(5)),
			op:   expr.OpLe,
		},
		{
			name: "column greater than or equal",
			expr: col.Ge(expr.Lit(3)),
			op:   expr.OpGe,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotNil(t, tt.expr)
			assert.Equal(t, expr.ExprBinary, tt.expr.Type())
			assert.Equal(t, tt.op, tt.expr.Op())
		})
	}
}

// TestColumnExpr_MathFunctions tests math functions on ColumnExpr.
func TestColumnExpr_MathFunctions(t *testing.T) {
	col := expr.Col("value")

	tests := []struct {
		name     string
		function *expr.FunctionExpr
		funcName string
	}{
		{"abs", col.Abs(), "abs"},
		{"round", col.Round(), "round"},
		{"round_to", col.RoundTo(expr.Lit(2)), "round"},
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
			assert.Equal(t, expr.ExprFunction, tt.function.Type())
			assert.Equal(t, tt.funcName, tt.function.Name())
		})
	}
}

// TestColumnExpr_StringFunctions tests string functions on ColumnExpr.
func TestColumnExpr_StringFunctions(t *testing.T) {
	col := expr.Col("text")

	tests := []struct {
		name     string
		function *expr.FunctionExpr
		funcName string
	}{
		{"upper", col.Upper(), "upper"},
		{"lower", col.Lower(), "lower"},
		{"length", col.Length(), "length"},
		{"trim", col.Trim(), "trim"},
		{"substring", col.Substring(expr.Lit(0), expr.Lit(5)), "substring"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotNil(t, tt.function)
			assert.Equal(t, expr.ExprFunction, tt.function.Type())
			assert.Equal(t, tt.funcName, tt.function.Name())
		})
	}
}

// TestColumnExpr_CastFunctions tests cast functions on ColumnExpr.
func TestColumnExpr_CastFunctions(t *testing.T) {
	col := expr.Col("value")

	tests := []struct {
		name     string
		function *expr.FunctionExpr
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
			assert.Equal(t, expr.ExprFunction, tt.function.Type())
			assert.Equal(t, tt.funcName, tt.function.Name())
		})
	}
}

// TestColumnExpr_DateTimeFunctions tests datetime functions on ColumnExpr.
func TestColumnExpr_DateTimeFunctions(t *testing.T) {
	col := expr.Col("timestamp")

	tests := []struct {
		name     string
		function *expr.FunctionExpr
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
			assert.Equal(t, expr.ExprFunction, tt.function.Type())
			assert.Equal(t, tt.funcName, tt.function.Name())
		})
	}
}

// TestEvaluator_BooleanOperations tests boolean evaluation.
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

	evaluator := expr.NewEvaluator(mem)

	t.Run("evaluate boolean column", func(t *testing.T) {
		result, err := evaluator.EvaluateBoolean(expr.Col("bool_col"), data)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 4, result.Len())
		boolResult := result.(*array.Boolean)
		assert.True(t, boolResult.Value(0))
		assert.False(t, boolResult.Value(1))
	})

	t.Run("evaluate boolean literal", func(t *testing.T) {
		result, err := evaluator.EvaluateBoolean(expr.Lit(true), data)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 4, result.Len()) // Literal is broadcast to match data size
		boolResult := result.(*array.Boolean)
		assert.True(t, boolResult.Value(0))
	})

	t.Run("evaluate boolean comparison", func(t *testing.T) {
		result, err := evaluator.EvaluateBoolean(expr.Col("bool_col").Eq(expr.Lit(true)), data)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 4, result.Len())
		boolResult := result.(*array.Boolean)
		assert.True(t, boolResult.Value(0))
		assert.False(t, boolResult.Value(1))
		assert.True(t, boolResult.Value(2))
		assert.False(t, boolResult.Value(3))
	})
}

// TestEvaluator_Int32Float32Operations tests int32 and float32 operations.
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

	evaluator := expr.NewEvaluator(mem)

	t.Run("int32 arithmetic", func(t *testing.T) {
		// Test addition
		result, err := evaluator.Evaluate(expr.Col("int32_col").Add(expr.Lit(int32(5))), data)
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
		result, err := evaluator.Evaluate(expr.Col("float32_col").Mul(expr.Lit(float32(2.0))), data)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 4, result.Len())
		// Check values
		f32Result := result.(*array.Float32)
		assert.InDelta(t, float32(3.0), f32Result.Value(0), 0.001)
		assert.InDelta(t, float32(5.0), f32Result.Value(1), 0.001)
	})

	t.Run("int32 comparison", func(t *testing.T) {
		result, err := evaluator.EvaluateBoolean(expr.Col("int32_col").Gt(expr.Lit(int32(25))), data)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 4, result.Len())
		boolResult := result.(*array.Boolean)
		assert.False(t, boolResult.Value(0))
		assert.False(t, boolResult.Value(1))
		assert.True(t, boolResult.Value(2))
		assert.True(t, boolResult.Value(3))
	})

	t.Run("float32 comparison", func(t *testing.T) {
		result, err := evaluator.EvaluateBoolean(expr.Col("float32_col").Le(expr.Lit(float32(3.0))), data)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 4, result.Len())
		boolResult := result.(*array.Boolean)
		assert.True(t, boolResult.Value(0))
		assert.True(t, boolResult.Value(1))
		assert.False(t, boolResult.Value(2))
		assert.False(t, boolResult.Value(3))
	})
}
