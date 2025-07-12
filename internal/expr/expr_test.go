package expr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColumnExpr(t *testing.T) {
	col := Col("test_column")
	
	assert.Equal(t, ExprColumn, col.Type())
	assert.Equal(t, "test_column", col.Name())
	assert.Equal(t, "col(test_column)", col.String())
}

func TestLiteralExpr(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{"int literal", 42, "lit(42)"},
		{"string literal", "hello", "lit(hello)"},
		{"float literal", 3.14, "lit(3.14)"},
		{"bool literal", true, "lit(true)"},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lit := Lit(tt.value)
			
			assert.Equal(t, ExprLiteral, lit.Type())
			assert.Equal(t, tt.value, lit.Value())
			assert.Equal(t, tt.expected, lit.String())
		})
	}
}

func TestBinaryExpressions(t *testing.T) {
	col := Col("value")
	lit := Lit(10)
	
	tests := []struct {
		name     string
		expr     *BinaryExpr
		expected string
		op       BinaryOp
	}{
		{"addition", col.Add(lit), "(col(value) + lit(10))", OpAdd},
		{"subtraction", col.Sub(lit), "(col(value) - lit(10))", OpSub},
		{"multiplication", col.Mul(lit), "(col(value) * lit(10))", OpMul},
		{"division", col.Div(lit), "(col(value) / lit(10))", OpDiv},
		{"equality", col.Eq(lit), "(col(value) == lit(10))", OpEq},
		{"not equal", col.Ne(lit), "(col(value) != lit(10))", OpNe},
		{"less than", col.Lt(lit), "(col(value) < lit(10))", OpLt},
		{"less than or equal", col.Le(lit), "(col(value) <= lit(10))", OpLe},
		{"greater than", col.Gt(lit), "(col(value) > lit(10))", OpGt},
		{"greater than or equal", col.Ge(lit), "(col(value) >= lit(10))", OpGe},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, ExprBinary, tt.expr.Type())
			assert.Equal(t, tt.expected, tt.expr.String())
			assert.Equal(t, tt.op, tt.expr.Op())
			assert.Equal(t, col, tt.expr.Left())
			assert.Equal(t, lit, tt.expr.Right())
		})
	}
}

func TestChainedExpressions(t *testing.T) {
	col := Col("value")
	
	// Test chaining: (value + 10) * 2
	chained := col.Add(Lit(10)).Mul(Lit(2))
	
	assert.Equal(t, ExprBinary, chained.Type())
	assert.Equal(t, "((col(value) + lit(10)) * lit(2))", chained.String())
	assert.Equal(t, OpMul, chained.Op())
	
	// The left side should be the addition expression
	leftExpr, ok := chained.Left().(*BinaryExpr)
	assert.True(t, ok)
	assert.Equal(t, OpAdd, leftExpr.Op())
}

func TestLogicalExpressions(t *testing.T) {
	col1 := Col("age")
	col2 := Col("salary")
	
	// Test: (age > 25) && (salary > 50000)
	expr1 := col1.Gt(Lit(25))
	expr2 := col2.Gt(Lit(50000))
	combined := expr1.And(expr2)
	
	assert.Equal(t, ExprBinary, combined.Type())
	assert.Equal(t, "((col(age) > lit(25)) && (col(salary) > lit(50000)))", combined.String())
	assert.Equal(t, OpAnd, combined.Op())
	
	// Test OR operation
	orExpr := expr1.Or(expr2)
	assert.Equal(t, OpOr, orExpr.Op())
	assert.Equal(t, "((col(age) > lit(25)) || (col(salary) > lit(50000)))", orExpr.String())
}

func TestComplexExpressions(t *testing.T) {
	// Test complex nested expression: ((value * 2) + 5) > (threshold - 1)
	value := Col("value")
	threshold := Col("threshold")
	
	left := value.Mul(Lit(2)).Add(Lit(5))
	right := threshold.Sub(Lit(1))
	complex := &BinaryExpr{left: left, op: OpGt, right: right}
	
	expected := "(((col(value) * lit(2)) + lit(5)) > (col(threshold) - lit(1)))"
	assert.Equal(t, expected, complex.String())
}

func TestBinaryOpString(t *testing.T) {
	// Test that all binary operations have proper string representations
	col := Col("test")
	lit := Lit(1)
	
	operations := []struct {
		expr     *BinaryExpr
		contains string
	}{
		{col.Add(lit), "+"},
		{col.Sub(lit), "-"},
		{col.Mul(lit), "*"},
		{col.Div(lit), "/"},
		{col.Eq(lit), "=="},
		{col.Ne(lit), "!="},
		{col.Lt(lit), "<"},
		{col.Le(lit), "<="},
		{col.Gt(lit), ">"},
		{col.Ge(lit), ">="},
	}
	
	for _, op := range operations {
		assert.Contains(t, op.expr.String(), op.contains)
	}
}

func TestExpressionTypes(t *testing.T) {
	col := Col("test")
	lit := Lit(42)
	bin := col.Add(lit)
	
	// Test type assertions
	assert.Equal(t, ExprColumn, col.Type())
	assert.Equal(t, ExprLiteral, lit.Type())
	assert.Equal(t, ExprBinary, bin.Type())
	
	// Test that expressions implement the Expr interface
	var exprs []Expr = []Expr{col, lit, bin}
	for i, expr := range exprs {
		assert.NotNil(t, expr, "Expression %d should not be nil", i)
		assert.NotEmpty(t, expr.String(), "Expression %d should have string representation", i)
	}
}