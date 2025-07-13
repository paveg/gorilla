package expr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnaryOperations(t *testing.T) {
	col := Col("test_col")

	t.Run("Negative", func(t *testing.T) {
		negExpr := col.Neg()
		assert.Equal(t, ExprUnary, negExpr.Type())
		assert.Equal(t, "(-col(test_col))", negExpr.String())
		assert.Equal(t, UnaryNeg, negExpr.Op())
		assert.Equal(t, col, negExpr.Operand())
	})

	t.Run("NOT", func(t *testing.T) {
		notExpr := col.Not()
		assert.Equal(t, ExprUnary, notExpr.Type())
		assert.Equal(t, "(!col(test_col))", notExpr.String())
		assert.Equal(t, UnaryNot, notExpr.Op())
		assert.Equal(t, col, notExpr.Operand())
	})

	t.Run("Absolute", func(t *testing.T) {
		absExpr := col.Abs()
		assert.Equal(t, ExprFunction, absExpr.Type())
		assert.Equal(t, "abs(col(test_col))", absExpr.String())
		assert.Equal(t, "abs", absExpr.Name())
		assert.Equal(t, []Expr{col}, absExpr.Args())
	})
}

func TestStringFunctions(t *testing.T) {
	col := Col("text_col")

	t.Run("UPPER", func(t *testing.T) {
		upperExpr := col.Upper()
		assert.Equal(t, ExprFunction, upperExpr.Type())
		assert.Equal(t, "upper(col(text_col))", upperExpr.String())
		assert.Equal(t, "upper", upperExpr.Name())
		assert.Equal(t, []Expr{col}, upperExpr.Args())
	})

	t.Run("LOWER", func(t *testing.T) {
		lowerExpr := col.Lower()
		assert.Equal(t, ExprFunction, lowerExpr.Type())
		assert.Equal(t, "lower(col(text_col))", lowerExpr.String())
		assert.Equal(t, "lower", lowerExpr.Name())
		assert.Equal(t, []Expr{col}, lowerExpr.Args())
	})

	t.Run("LENGTH", func(t *testing.T) {
		lenExpr := col.Length()
		assert.Equal(t, ExprFunction, lenExpr.Type())
		assert.Equal(t, "length(col(text_col))", lenExpr.String())
		assert.Equal(t, "length", lenExpr.Name())
		assert.Equal(t, []Expr{col}, lenExpr.Args())
	})

	t.Run("TRIM", func(t *testing.T) {
		trimExpr := col.Trim()
		assert.Equal(t, ExprFunction, trimExpr.Type())
		assert.Equal(t, "trim(col(text_col))", trimExpr.String())
		assert.Equal(t, "trim", trimExpr.Name())
		assert.Equal(t, []Expr{col}, trimExpr.Args())
	})

	t.Run("SUBSTRING", func(t *testing.T) {
		substringExpr := col.Substring(Lit(1), Lit(5))
		assert.Equal(t, ExprFunction, substringExpr.Type())
		assert.Equal(t, "substring(col(text_col), lit(1), lit(5))", substringExpr.String())
		assert.Equal(t, "substring", substringExpr.Name())
		assert.Equal(t, []Expr{col, Lit(1), Lit(5)}, substringExpr.Args())
	})

	t.Run("CONCAT", func(t *testing.T) {
		concatExpr := Concat(col, Lit("_suffix"))
		assert.Equal(t, ExprFunction, concatExpr.Type())
		assert.Equal(t, "concat(col(text_col), lit(_suffix))", concatExpr.String())
		assert.Equal(t, "concat", concatExpr.Name())
		assert.Equal(t, []Expr{col, Lit("_suffix")}, concatExpr.Args())
	})
}

func TestMathFunctions(t *testing.T) {
	col := Col("num_col")

	t.Run("ROUND", func(t *testing.T) {
		roundExpr := col.Round()
		assert.Equal(t, ExprFunction, roundExpr.Type())
		assert.Equal(t, "round(col(num_col))", roundExpr.String())
		assert.Equal(t, "round", roundExpr.Name())
		assert.Equal(t, []Expr{col}, roundExpr.Args())
	})

	t.Run("ROUND with precision", func(t *testing.T) {
		roundExpr := col.RoundTo(Lit(2))
		assert.Equal(t, ExprFunction, roundExpr.Type())
		assert.Equal(t, "round(col(num_col), lit(2))", roundExpr.String())
		assert.Equal(t, "round", roundExpr.Name())
		assert.Equal(t, []Expr{col, Lit(2)}, roundExpr.Args())
	})

	t.Run("FLOOR", func(t *testing.T) {
		floorExpr := col.Floor()
		assert.Equal(t, ExprFunction, floorExpr.Type())
		assert.Equal(t, "floor(col(num_col))", floorExpr.String())
		assert.Equal(t, "floor", floorExpr.Name())
		assert.Equal(t, []Expr{col}, floorExpr.Args())
	})

	t.Run("CEIL", func(t *testing.T) {
		ceilExpr := col.Ceil()
		assert.Equal(t, ExprFunction, ceilExpr.Type())
		assert.Equal(t, "ceil(col(num_col))", ceilExpr.String())
		assert.Equal(t, "ceil", ceilExpr.Name())
		assert.Equal(t, []Expr{col}, ceilExpr.Args())
	})

	t.Run("SQRT", func(t *testing.T) {
		sqrtExpr := col.Sqrt()
		assert.Equal(t, ExprFunction, sqrtExpr.Type())
		assert.Equal(t, "sqrt(col(num_col))", sqrtExpr.String())
		assert.Equal(t, "sqrt", sqrtExpr.Name())
		assert.Equal(t, []Expr{col}, sqrtExpr.Args())
	})

	t.Run("LOG", func(t *testing.T) {
		logExpr := col.Log()
		assert.Equal(t, ExprFunction, logExpr.Type())
		assert.Equal(t, "log(col(num_col))", logExpr.String())
		assert.Equal(t, "log", logExpr.Name())
		assert.Equal(t, []Expr{col}, logExpr.Args())
	})

	t.Run("SIN", func(t *testing.T) {
		sinExpr := col.Sin()
		assert.Equal(t, ExprFunction, sinExpr.Type())
		assert.Equal(t, "sin(col(num_col))", sinExpr.String())
		assert.Equal(t, "sin", sinExpr.Name())
		assert.Equal(t, []Expr{col}, sinExpr.Args())
	})

	t.Run("COS", func(t *testing.T) {
		cosExpr := col.Cos()
		assert.Equal(t, ExprFunction, cosExpr.Type())
		assert.Equal(t, "cos(col(num_col))", cosExpr.String())
		assert.Equal(t, "cos", cosExpr.Name())
		assert.Equal(t, []Expr{col}, cosExpr.Args())
	})
}

func TestConditionalExpressions(t *testing.T) {
	col := Col("test_col")
	condition := col.Gt(Lit(10))

	t.Run("IF/ELSE", func(t *testing.T) {
		ifExpr := If(condition, Lit("high"), Lit("low"))
		assert.Equal(t, ExprFunction, ifExpr.Type())
		assert.Equal(t, "if((col(test_col) > lit(10)), lit(high), lit(low))", ifExpr.String())
		assert.Equal(t, "if", ifExpr.Name())
		assert.Equal(t, []Expr{condition, Lit("high"), Lit("low")}, ifExpr.Args())
	})

	t.Run("COALESCE", func(t *testing.T) {
		coalesceExpr := Coalesce(col, Lit("default"))
		assert.Equal(t, ExprFunction, coalesceExpr.Type())
		assert.Equal(t, "coalesce(col(test_col), lit(default))", coalesceExpr.String())
		assert.Equal(t, "coalesce", coalesceExpr.Name())
		assert.Equal(t, []Expr{col, Lit("default")}, coalesceExpr.Args())
	})

	t.Run("CASE/WHEN", func(t *testing.T) {
		caseExpr := Case().
			When(col.Lt(Lit(5)), Lit("small")).
			When(col.Lt(Lit(10)), Lit("medium")).
			Else(Lit("large"))

		assert.Equal(t, ExprCase, caseExpr.Type())
		assert.Contains(t, caseExpr.String(), "case")
		assert.Contains(t, caseExpr.String(), "when")
		assert.Contains(t, caseExpr.String(), "else")
	})
}

func TestChainedOperations(t *testing.T) {
	col := Col("value")

	t.Run("Complex expression chain", func(t *testing.T) {
		// ABS(ROUND(value * 2)) > 10
		complexExpr := col.Mul(Lit(2)).Round().Abs().Gt(Lit(10))
		assert.Equal(t, ExprBinary, complexExpr.Type())

		// The left side should be a function expression (abs)
		left := complexExpr.Left()
		assert.Equal(t, ExprFunction, left.Type())
	})

	t.Run("String operations chain", func(t *testing.T) {
		// Test chaining string operations: UPPER(TRIM(text_col))
		stringCol := Col("text_col")
		chainedExpr := stringCol.Trim().Upper()
		assert.Equal(t, ExprFunction, chainedExpr.Type())
		assert.Equal(t, "upper", chainedExpr.Name())

		// The argument should be trim function
		args := chainedExpr.Args()
		assert.Len(t, args, 1)
		assert.Equal(t, ExprFunction, args[0].Type())
		assert.Equal(t, "trim", args[0].(*FunctionExpr).Name())
	})
}

func TestTypeCasting(t *testing.T) {
	col := Col("mixed_col")

	t.Run("Cast to string", func(t *testing.T) {
		castExpr := col.CastToString()
		assert.Equal(t, ExprFunction, castExpr.Type())
		assert.Equal(t, "cast_string(col(mixed_col))", castExpr.String())
		assert.Equal(t, "cast_string", castExpr.Name())
		assert.Equal(t, []Expr{col}, castExpr.Args())
	})

	t.Run("Cast to int64", func(t *testing.T) {
		castExpr := col.CastToInt64()
		assert.Equal(t, ExprFunction, castExpr.Type())
		assert.Equal(t, "cast_int64(col(mixed_col))", castExpr.String())
		assert.Equal(t, "cast_int64", castExpr.Name())
		assert.Equal(t, []Expr{col}, castExpr.Args())
	})

	t.Run("Cast to float64", func(t *testing.T) {
		castExpr := col.CastToFloat64()
		assert.Equal(t, ExprFunction, castExpr.Type())
		assert.Equal(t, "cast_float64(col(mixed_col))", castExpr.String())
		assert.Equal(t, "cast_float64", castExpr.Name())
		assert.Equal(t, []Expr{col}, castExpr.Args())
	})

	t.Run("Cast to bool", func(t *testing.T) {
		castExpr := col.CastToBool()
		assert.Equal(t, ExprFunction, castExpr.Type())
		assert.Equal(t, "cast_bool(col(mixed_col))", castExpr.String())
		assert.Equal(t, "cast_bool", castExpr.Name())
		assert.Equal(t, []Expr{col}, castExpr.Args())
	})
}
