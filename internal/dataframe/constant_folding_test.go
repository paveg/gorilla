package dataframe

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to create binary expressions with literals
func createBinaryLiteral(left interface{}, op expr.BinaryOp, right interface{}) expr.Expr {
	// For testing purposes, we'll create a struct that mimics BinaryExpr
	return &mockBinaryExpr{
		left:  expr.Lit(left),
		op:    op,
		right: expr.Lit(right),
	}
}

// Mock binary expression for testing constant folding
type mockBinaryExpr struct {
	left  expr.Expr
	op    expr.BinaryOp
	right expr.Expr
}

func (m *mockBinaryExpr) Type() expr.ExprType {
	return expr.ExprBinary
}

func (m *mockBinaryExpr) String() string {
	return "mock_binary"
}

func (m *mockBinaryExpr) Left() expr.Expr {
	return m.left
}

func (m *mockBinaryExpr) Op() expr.BinaryOp {
	return m.op
}

func (m *mockBinaryExpr) Right() expr.Expr {
	return m.right
}

// Mock unary expression for testing
type mockUnaryExpr struct {
	op      expr.UnaryOp
	operand expr.Expr
}

func (m *mockUnaryExpr) Type() expr.ExprType {
	return expr.ExprUnary
}

func (m *mockUnaryExpr) String() string {
	return "mock_unary"
}

func (m *mockUnaryExpr) Op() expr.UnaryOp {
	return m.op
}

func (m *mockUnaryExpr) Operand() expr.Expr {
	return m.operand
}

// Mock function expression for testing
type mockFunctionExpr struct {
	name string
	args []expr.Expr
}

func (m *mockFunctionExpr) Type() expr.ExprType {
	return expr.ExprFunction
}

func (m *mockFunctionExpr) String() string {
	return "mock_function"
}

func (m *mockFunctionExpr) Name() string {
	return m.name
}

func (m *mockFunctionExpr) Args() []expr.Expr {
	return m.args
}

func TestConstantFoldingRule_ArithmeticOperations(t *testing.T) {
	rule := &ConstantFoldingRule{}

	tests := []struct {
		name     string
		input    expr.Expr
		expected interface{}
	}{
		{
			name:     "Add two integers",
			input:    createBinaryLiteral(int64(2), expr.OpAdd, int64(3)),
			expected: int64(5),
		},
		{
			name:     "Multiply two integers",
			input:    createBinaryLiteral(int64(2), expr.OpMul, int64(5)),
			expected: int64(10),
		},
		{
			name:     "Subtract two floats",
			input:    createBinaryLiteral(3.5, expr.OpSub, 1.5),
			expected: 2.0,
		},
		{
			name:     "Divide two integers",
			input:    createBinaryLiteral(int64(10), expr.OpDiv, int64(2)),
			expected: int64(5),
		},
		{
			name: "Complex arithmetic expression",
			input: &mockBinaryExpr{
				left:  createBinaryLiteral(int64(2), expr.OpAdd, int64(3)),
				op:    expr.OpMul,
				right: expr.Lit(int64(4)),
			},
			expected: int64(20),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rule.foldConstants(tt.input)

			// Check if result is a literal
			literal, ok := result.(*expr.LiteralExpr)
			require.True(t, ok, "Expected result to be a literal expression")

			assert.Equal(t, tt.expected, literal.Value())
		})
	}
}

func TestConstantFoldingRule_ComparisonOperations(t *testing.T) {
	rule := &ConstantFoldingRule{}

	tests := []struct {
		name     string
		input    expr.Expr
		expected bool
	}{
		{
			name:     "Integer equality true",
			input:    createBinaryLiteral(int64(5), expr.OpEq, int64(5)),
			expected: true,
		},
		{
			name:     "Integer equality false",
			input:    createBinaryLiteral(int64(5), expr.OpEq, int64(3)),
			expected: false,
		},
		{
			name:     "Integer less than true",
			input:    createBinaryLiteral(int64(3), expr.OpLt, int64(5)),
			expected: true,
		},
		{
			name:     "Integer greater than false",
			input:    createBinaryLiteral(int64(3), expr.OpGt, int64(5)),
			expected: false,
		},
		{
			name:     "Float comparison",
			input:    createBinaryLiteral(2.5, expr.OpLe, 3.0),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rule.foldConstants(tt.input)

			// Check if result is a literal
			literal, ok := result.(*expr.LiteralExpr)
			require.True(t, ok, "Expected result to be a literal expression")

			assert.Equal(t, tt.expected, literal.Value())
		})
	}
}

func TestConstantFoldingRule_LogicalOperations(t *testing.T) {
	rule := &ConstantFoldingRule{}

	tests := []struct {
		name     string
		input    expr.Expr
		expected bool
	}{
		{
			name:     "AND operation true",
			input:    createBinaryLiteral(true, expr.OpAnd, true),
			expected: true,
		},
		{
			name:     "AND operation false",
			input:    createBinaryLiteral(true, expr.OpAnd, false),
			expected: false,
		},
		{
			name:     "OR operation true",
			input:    createBinaryLiteral(false, expr.OpOr, true),
			expected: true,
		},
		{
			name:     "OR operation false",
			input:    createBinaryLiteral(false, expr.OpOr, false),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rule.foldConstants(tt.input)

			// Check if result is a literal
			literal, ok := result.(*expr.LiteralExpr)
			require.True(t, ok, "Expected result to be a literal expression")

			assert.Equal(t, tt.expected, literal.Value())
		})
	}
}

func TestConstantFoldingRule_UnaryOperations(t *testing.T) {
	rule := &ConstantFoldingRule{}

	tests := []struct {
		name     string
		input    expr.Expr
		expected interface{}
	}{
		{
			name: "Negate integer",
			input: &mockUnaryExpr{
				op:      expr.UnaryNeg,
				operand: expr.Lit(int64(5)),
			},
			expected: int64(-5),
		},
		{
			name: "Negate float",
			input: &mockUnaryExpr{
				op:      expr.UnaryNeg,
				operand: expr.Lit(3.14),
			},
			expected: -3.14,
		},
		{
			name: "Logical NOT true",
			input: &mockUnaryExpr{
				op:      expr.UnaryNot,
				operand: expr.Lit(true),
			},
			expected: false,
		},
		{
			name: "Logical NOT false",
			input: &mockUnaryExpr{
				op:      expr.UnaryNot,
				operand: expr.Lit(false),
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rule.foldConstants(tt.input)

			// Check if result is a literal
			literal, ok := result.(*expr.LiteralExpr)
			require.True(t, ok, "Expected result to be a literal expression")

			assert.Equal(t, tt.expected, literal.Value())
		})
	}
}

func TestConstantFoldingRule_FunctionOperations(t *testing.T) {
	rule := &ConstantFoldingRule{}

	tests := []struct {
		name     string
		input    expr.Expr
		expected interface{}
	}{
		{
			name: "Absolute value positive",
			input: &mockFunctionExpr{
				name: "abs",
				args: []expr.Expr{expr.Lit(int64(5))},
			},
			expected: int64(5),
		},
		{
			name: "Absolute value negative",
			input: &mockFunctionExpr{
				name: "abs",
				args: []expr.Expr{expr.Lit(int64(-5))},
			},
			expected: int64(5),
		},
		{
			name: "Round float down",
			input: &mockFunctionExpr{
				name: "round",
				args: []expr.Expr{expr.Lit(2.3)},
			},
			expected: 2.0,
		},
		{
			name: "Round float up",
			input: &mockFunctionExpr{
				name: "round",
				args: []expr.Expr{expr.Lit(2.7)},
			},
			expected: 3.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rule.foldConstants(tt.input)

			// Check if result is a literal
			literal, ok := result.(*expr.LiteralExpr)
			require.True(t, ok, "Expected result to be a literal expression")

			assert.Equal(t, tt.expected, literal.Value())
		})
	}
}

func TestConstantFoldingRule_MixedExpressionsNotFolded(t *testing.T) {
	rule := &ConstantFoldingRule{}

	tests := []struct {
		name  string
		input expr.Expr
	}{
		{
			name:  "Column plus literal - should not fold",
			input: expr.Col("age").Add(expr.Lit(int64(10))),
		},
		{
			name: "Literal plus column - should not fold",
			input: &mockBinaryExpr{
				left:  expr.Lit(int64(5)),
				op:    expr.OpAdd,
				right: expr.Col("salary"),
			},
		},
		{
			name:  "Column comparison - should not fold",
			input: expr.Col("age").Gt(expr.Col("min_age")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rule.foldConstants(tt.input)

			// Result should not be a literal (since it involves columns)
			_, ok := result.(*expr.LiteralExpr)
			assert.False(t, ok, "Expected result to NOT be a literal expression")
		})
	}
}

func TestConstantFoldingRule_IntegrationWithLazyFrame(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data
	idSeries := series.New("id", []int64{1, 2, 3, 4, 5}, mem)
	scoreSeries := series.New("score", []int64{10, 20, 30, 40, 50}, mem)
	df := New(idSeries, scoreSeries)
	defer df.Release()

	// Test with constant folding: 2 * 5 should be folded to 10
	lazy := df.Lazy()
	defer lazy.Release()

	// Add a column with constant arithmetic that should be folded
	result, err := lazy.
		WithColumn("constant_result", createBinaryLiteral(int64(2), expr.OpMul, int64(5))).
		// Use a simpler filter that doesn't involve complex mock expressions
		Filter(expr.Col("score").Gt(expr.Lit(int64(15)))).
		Collect()

	require.NoError(t, err)
	defer result.Release()

	// Verify the result
	assert.Equal(t, 4, result.Len()) // Should have 4 rows (score > 15)

	// Check that the constant_result column has the folded value
	constantCol, exists := result.Column("constant_result")
	require.True(t, exists)

	constantValues := constantCol.(*series.Series[int64]).Values()
	expectedValues := []int64{10, 10, 10, 10} // 2 * 5 = 10 for all rows
	assert.Equal(t, expectedValues, constantValues)
}

func TestConstantFoldingRule_EdgeCases(t *testing.T) {
	rule := &ConstantFoldingRule{}

	t.Run("Division by zero integer", func(t *testing.T) {
		input := createBinaryLiteral(int64(10), expr.OpDiv, int64(0))
		result := rule.foldConstants(input)

		literal, ok := result.(*expr.LiteralExpr)
		require.True(t, ok)
		assert.Equal(t, int64(0), literal.Value()) // Should return 0 for division by zero
	})

	t.Run("Division by zero float", func(t *testing.T) {
		input := createBinaryLiteral(10.0, expr.OpDiv, 0.0)
		result := rule.foldConstants(input)

		literal, ok := result.(*expr.LiteralExpr)
		require.True(t, ok)
		// Should return 0.0 for float division by zero
		assert.Equal(t, 0.0, literal.Value()) // Should return 0.0 for division by zero
	})

	t.Run("Type coercion int to float", func(t *testing.T) {
		input := createBinaryLiteral(int64(5), expr.OpAdd, 2.5)
		result := rule.foldConstants(input)

		literal, ok := result.(*expr.LiteralExpr)
		require.True(t, ok)
		assert.Equal(t, 7.5, literal.Value()) // Should be coerced to float
	})
}

func TestConstantFoldingRule_PerformanceBenchmark(t *testing.T) {
	rule := &ConstantFoldingRule{}

	// Test a simple expression first
	simpleExpr := createBinaryLiteral(int64(10), expr.OpMul, int64(5)) // 10 * 5 = 50

	result := rule.foldConstants(simpleExpr)

	literal, ok := result.(*expr.LiteralExpr)
	require.True(t, ok, "Simple constant expression should be folded to literal")
	assert.Equal(t, int64(50), literal.Value()) // 10 * 5 = 50
}
