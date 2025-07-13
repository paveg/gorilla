package gorilla

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublicAPIErrorHandling(t *testing.T) {
	t.Run("invalid operations return errors instead of panics", func(t *testing.T) {
		// Create test DataFrame
		mem := memory.NewGoAllocator()
		s1 := series.New("age", []int64{25, 30, 35}, mem)
		s2 := series.New("name", []string{"Alice", "Bob", "Charlie"}, mem)

		df := NewDataFrame(s1, s2)
		defer df.Release()

		// Test that invalid operations on expressions create InvalidExpr
		// instead of panicking
		lit := Lit(42)

		// These operations should create InvalidExpr, not panic
		invalidEq := lit.Eq(Col("test"))
		invalidAdd := lit.Add(Col("test"))
		invalidAnd := lit.And(Col("test"))

		// Verify these are InvalidExpr instances
		assert.Equal(t, expr.ExprInvalid, invalidEq.expr.Type())
		assert.Equal(t, expr.ExprInvalid, invalidAdd.expr.Type())
		assert.Equal(t, expr.ExprInvalid, invalidAnd.expr.Type())

		// Test that lazy evaluation properly propagates errors
		lazyFrame := df.Lazy().Filter(invalidEq)

		result, err := lazyFrame.Collect()
		assert.Nil(t, result)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid expression")
	})

	t.Run("comparison operations on literals return errors", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		s1 := series.New("value", []int64{1, 2, 3}, mem)
		df := NewDataFrame(s1)
		defer df.Release()

		lit := Lit(10)

		// All comparison operations on literals should return InvalidExpr
		tests := []struct {
			name string
			expr Expression
		}{
			{"Eq on literal", lit.Eq(Col("value"))},
			{"Ne on literal", lit.Ne(Col("value"))},
			{"Lt on literal", lit.Lt(Col("value"))},
			{"Le on literal", lit.Le(Col("value"))},
			{"Gt on literal", lit.Gt(Col("value"))},
			{"Ge on literal", lit.Ge(Col("value"))},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				assert.Equal(t, expr.ExprInvalid, tt.expr.expr.Type())

				// Test in lazy evaluation context
				lazyFrame := df.Lazy().Filter(tt.expr)
				result, err := lazyFrame.Collect()

				assert.Nil(t, result)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "invalid expression")
			})
		}
	})

	t.Run("arithmetic operations on unsupported types return errors", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		s1 := series.New("value", []int64{1, 2, 3}, mem)
		df := NewDataFrame(s1)
		defer df.Release()

		lit := Lit(10)

		// Arithmetic operations on literals should return InvalidExpr
		tests := []struct {
			name string
			expr Expression
		}{
			{"Add on literal", lit.Add(Col("value"))},
			{"Sub on literal", lit.Sub(Col("value"))},
			{"Mul on literal", lit.Mul(Col("value"))},
			{"Div on literal", lit.Div(Col("value"))},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				assert.Equal(t, expr.ExprInvalid, tt.expr.expr.Type())

				// Test in computation context
				lazyFrame := df.Lazy().WithColumn("result", tt.expr)
				result, err := lazyFrame.Collect()

				assert.Nil(t, result)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "invalid expression")
			})
		}
	})

	t.Run("logical operations on non-binary expressions return errors", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		s1 := series.New("value", []bool{true, false, true}, mem)
		df := NewDataFrame(s1)
		defer df.Release()

		col := Col("value")

		// Logical operations on column expressions should return InvalidExpr
		invalidAnd := col.And(Col("other"))
		invalidOr := col.Or(Col("other"))

		assert.Equal(t, expr.ExprInvalid, invalidAnd.expr.Type())
		assert.Equal(t, expr.ExprInvalid, invalidOr.expr.Type())

		// Test in filter context
		lazyFrame := df.Lazy().Filter(invalidAnd)
		result, err := lazyFrame.Collect()

		assert.Nil(t, result)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid expression")
	})
}

func TestErrorPropagationInComplexExpressions(t *testing.T) {
	mem := memory.NewGoAllocator()
	s1 := series.New("age", []int64{25, 30, 35}, mem)
	s2 := series.New("salary", []int64{50000, 60000, 70000}, mem)

	df := NewDataFrame(s1, s2)
	defer df.Release()

	t.Run("nested invalid expressions propagate errors", func(t *testing.T) {
		// Create a complex expression with an invalid operation nested inside
		lit := Lit(42)
		invalidOp := lit.Add(Col("age")) // This should be invalid

		// Use it in a larger expression
		complexExpr := Col("salary").Gt(invalidOp)

		// The complex expression should also be invalid due to the nested invalid operation
		lazyFrame := df.Lazy().Filter(complexExpr)
		result, err := lazyFrame.Collect()

		assert.Nil(t, result)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid expression")
	})

	t.Run("multiple operations in chain with error", func(t *testing.T) {
		// Chain multiple operations where one in the middle is invalid
		lit := Lit("text")
		invalidChain := lit.Eq(Col("age")).And(Col("salary").Gt(Lit(50000)))

		lazyFrame := df.Lazy().Filter(invalidChain)
		result, err := lazyFrame.Collect()

		assert.Nil(t, result)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid expression")
	})
}

func TestInvalidExprErrorMessages(t *testing.T) {
	t.Run("error messages contain helpful information", func(t *testing.T) {
		lit := Lit(42)

		tests := []struct {
			name     string
			expr     Expression
			contains []string
		}{
			{
				"Add on literal",
				lit.Add(Col("test")),
				[]string{"Add", "unsupported"},
			},
			{
				"Eq on literal",
				lit.Eq(Col("test")),
				[]string{"Eq", "only supported on column expressions"},
			},
			{
				"And on column",
				Col("test").And(Col("other")),
				[]string{"And", "unsupported"},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				invalidExpr, ok := tt.expr.expr.(*expr.InvalidExpr)
				require.True(t, ok, "Expression should be InvalidExpr")

				message := invalidExpr.Message()
				for _, substring := range tt.contains {
					assert.Contains(t, message, substring)
				}
			})
		}
	})
}
