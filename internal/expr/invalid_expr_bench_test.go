package expr

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkInvalidExprCreation(b *testing.B) {
	b.Run("create invalid expression", func(b *testing.B) {
		message := "test error message"

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = Invalid(message)
		}
	})

	b.Run("create invalid vs regular expression", func(b *testing.B) {
		b.Run("invalid", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = Invalid("error message")
			}
		})

		b.Run("column", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = Col("test")
			}
		})

		b.Run("literal", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = Lit(42)
			}
		})
	})
}

func BenchmarkInvalidExprEvaluation(b *testing.B) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)
	columns := map[string]arrow.Array{} // Empty columns map for benchmarking

	b.Run("evaluate invalid expression", func(b *testing.B) {
		invalidExpr := Invalid("benchmark error")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = eval.Evaluate(invalidExpr, columns)
		}
	})

	b.Run("evaluate boolean invalid expression", func(b *testing.B) {
		invalidExpr := Invalid("benchmark boolean error")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = eval.EvaluateBoolean(invalidExpr, columns)
		}
	})
}

func BenchmarkErrorPathVsSuccessPath(b *testing.B) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)
	columns := createBenchmarkColumns(b, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	b.Run("success path - column evaluation", func(b *testing.B) {
		colExpr := Col("age")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, _ := eval.Evaluate(colExpr, columns)
			if result != nil {
				result.Release()
			}
		}
	})

	b.Run("error path - invalid expression", func(b *testing.B) {
		invalidExpr := Invalid("benchmark error path")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = eval.Evaluate(invalidExpr, columns)
		}
	})
}

func createBenchmarkColumns(b *testing.B, mem memory.Allocator) map[string]arrow.Array {
	b.Helper()

	// Create minimal test columns for benchmarking
	intBuilder := array.NewInt64Builder(mem)
	defer intBuilder.Release()
	intBuilder.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	intArray := intBuilder.NewArray()

	return map[string]arrow.Array{
		"age": intArray,
	}
}
