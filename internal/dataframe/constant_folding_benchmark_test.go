package dataframe

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
)

// BenchmarkConstantFoldingImpact measures the performance difference between
// queries with constant expressions that can be folded vs those that cannot.
func BenchmarkConstantFoldingImpact(b *testing.B) {
	mem := memory.NewGoAllocator()

	// Create test dataset
	size := 10000
	ids := make([]int64, size)
	values := make([]int64, size)

	for i := range size {
		ids[i] = int64(i)
		values[i] = int64(i % 100)
	}

	idSeries := series.New("id", ids, mem)
	valueSeries := series.New("value", values, mem)
	df := New(idSeries, valueSeries)
	defer df.Release()

	b.Run("WithConstantFolding", func(b *testing.B) {
		// Query with constants that can be folded: 2 * 5 = 10
		constantExpr := createBinaryLiteral(int64(2), expr.OpMul, int64(5))

		b.ResetTimer()
		for range b.N {
			lazy := df.Lazy()
			result, err := lazy.
				WithColumn("constant_result", constantExpr).
				Filter(expr.Col("value").Gt(expr.Lit(int64(50)))).
				Collect()

			if err != nil {
				b.Fatal(err)
			}
			result.Release()
			lazy.Release()
		}
	})

	b.Run("WithoutConstantFolding", func(b *testing.B) {
		// Query with non-constant expression that cannot be folded
		nonConstantExpr := expr.Col("value").Add(expr.Lit(int64(10)))

		b.ResetTimer()
		for range b.N {
			lazy := df.Lazy()
			result, err := lazy.
				WithColumn("computed_result", nonConstantExpr).
				Filter(expr.Col("value").Gt(expr.Lit(int64(50)))).
				Collect()

			if err != nil {
				b.Fatal(err)
			}
			result.Release()
			lazy.Release()
		}
	})
}

// BenchmarkConstantFoldingComplexity measures performance across different
// levels of expression complexity to show constant folding benefits.
func BenchmarkConstantFoldingComplexity(b *testing.B) {
	mem := memory.NewGoAllocator()

	// Create smaller dataset for complex expression benchmarking
	size := 1000
	values := make([]int64, size)
	for i := range size {
		values[i] = int64(i)
	}

	valueSeries := series.New("value", values, mem)
	df := New(valueSeries)
	defer df.Release()

	b.Run("SimpleConstant", func(b *testing.B) {
		// Simple constant: 5
		expr := expr.Lit(int64(5))
		benchmarkConstantExpression(b, df, expr)
	})

	b.Run("ArithmeticConstant", func(b *testing.B) {
		// Arithmetic constant: 2 + 3 = 5
		expr := createBinaryLiteral(int64(2), expr.OpAdd, int64(3))
		benchmarkConstantExpression(b, df, expr)
	})

	b.Run("ComplexConstant", func(b *testing.B) {
		// Complex constant: (2 + 3) * 4 = 20
		expr := &mockBinaryExpr{
			left:  createBinaryLiteral(int64(2), expr.OpAdd, int64(3)),
			op:    expr.OpMul,
			right: expr.Lit(int64(4)),
		}
		benchmarkConstantExpression(b, df, expr)
	})

	b.Run("VeryComplexConstant", func(b *testing.B) {
		// Very complex constant: ((2 + 3) * 4) / 2 = 10
		innerExpr := &mockBinaryExpr{
			left:  createBinaryLiteral(int64(2), expr.OpAdd, int64(3)),
			op:    expr.OpMul,
			right: expr.Lit(int64(4)),
		}
		expr := &mockBinaryExpr{
			left:  innerExpr,
			op:    expr.OpDiv,
			right: expr.Lit(int64(2)),
		}
		benchmarkConstantExpression(b, df, expr)
	})
}

// benchmarkConstantExpression is a helper function for expression benchmarks.
func benchmarkConstantExpression(b *testing.B, df *DataFrame, constantExpr expr.Expr) {
	b.ResetTimer()
	for range b.N {
		lazy := df.Lazy()
		result, err := lazy.
			WithColumn("result", constantExpr).
			Collect()

		if err != nil {
			b.Fatal(err)
		}
		result.Release()
		lazy.Release()
	}
}

// BenchmarkQueryOptimizationEngine measures the overall performance impact
// of the entire query optimization engine including constant folding.
func BenchmarkQueryOptimizationEngine(b *testing.B) {
	mem := memory.NewGoAllocator()

	// Create realistic dataset
	size := 5000
	ids := make([]int64, size)
	ages := make([]int64, size)
	salaries := make([]float64, size)

	for i := range size {
		ids[i] = int64(i)
		ages[i] = int64(20 + (i % 50))
		salaries[i] = float64(30000 + (i%100)*1000)
	}

	idSeries := series.New("id", ids, mem)
	ageSeries := series.New("age", ages, mem)
	salarySeries := series.New("salary", salaries, mem)
	df := New(idSeries, ageSeries, salarySeries)
	defer df.Release()

	b.Run("WithOptimizations", func(b *testing.B) {
		// Query with multiple constant expressions and optimizable operations
		constantMultiplier := createBinaryLiteral(int64(12), expr.OpDiv, int64(10)) // 1.2
		constantBonus := createBinaryLiteral(int64(1000), expr.OpMul, int64(2))     // 2000

		b.ResetTimer()
		for range b.N {
			lazy := df.Lazy()
			result, err := lazy.
				// These constant expressions should be folded during optimization
				WithColumn("bonus", constantBonus).
				WithColumn("multiplier", constantMultiplier).
				// This uses the folded constants
				WithColumn("adjusted_salary", expr.Col("salary").Add(expr.Col("bonus"))).
				// Filter with constants that can be optimized
				Filter(expr.Col("age").Gt(createBinaryLiteral(int64(25), expr.OpAdd, int64(5)))). // age > 30
				Select("id", "adjusted_salary").
				Collect()

			if err != nil {
				b.Fatal(err)
			}
			result.Release()
			lazy.Release()
		}
	})

	b.Run("WithoutConstantFolding", func(b *testing.B) {
		// Similar query but with non-constant expressions
		b.ResetTimer()
		for range b.N {
			lazy := df.Lazy()
			result, err := lazy.
				// These expressions involve columns so cannot be folded
				WithColumn("bonus", expr.Col("id").Mul(expr.Lit(int64(0))).Add(expr.Lit(int64(2000)))).
				WithColumn("multiplier", expr.Col("id").Mul(expr.Lit(float64(0))).Add(expr.Lit(1.2))).
				WithColumn("adjusted_salary", expr.Col("salary").Add(expr.Col("bonus"))).
				Filter(expr.Col("age").Gt(expr.Lit(int64(30)))).
				Select("id", "adjusted_salary").
				Collect()

			if err != nil {
				b.Fatal(err)
			}
			result.Release()
			lazy.Release()
		}
	})
}

// BenchmarkConstantFoldingTypes measures folding performance across different data types.
func BenchmarkConstantFoldingTypes(b *testing.B) {
	mem := memory.NewGoAllocator()

	size := 1000
	values := make([]int64, size)
	for i := range size {
		values[i] = int64(i)
	}

	valueSeries := series.New("value", values, mem)
	df := New(valueSeries)
	defer df.Release()

	b.Run("IntegerConstants", func(b *testing.B) {
		expr := createBinaryLiteral(int64(100), expr.OpAdd, int64(200))
		benchmarkConstantExpression(b, df, expr)
	})

	b.Run("FloatConstants", func(b *testing.B) {
		expr := createBinaryLiteral(10.5, expr.OpMul, 2.0)
		benchmarkConstantExpression(b, df, expr)
	})

	b.Run("BooleanConstants", func(b *testing.B) {
		expr := createBinaryLiteral(true, expr.OpAnd, false)
		benchmarkConstantExpression(b, df, expr)
	})

	b.Run("MixedTypeConstants", func(b *testing.B) {
		// Integer + Float (should be coerced to float)
		expr := createBinaryLiteral(int64(10), expr.OpAdd, 5.5)
		benchmarkConstantExpression(b, df, expr)
	})
}

// BenchmarkOptimizationOverhead measures the overhead of the optimization engine itself.
func BenchmarkOptimizationOverhead(b *testing.B) {
	mem := memory.NewGoAllocator()

	values := make([]int64, 100)
	for i := range 100 {
		values[i] = int64(i)
	}

	valueSeries := series.New("value", values, mem)
	df := New(valueSeries)
	defer df.Release()

	b.Run("OptimizationEnabled", func(b *testing.B) {
		// Query that goes through the optimization engine
		b.ResetTimer()
		for range b.N {
			lazy := df.Lazy()
			result, err := lazy.
				Filter(expr.Col("value").Gt(expr.Lit(int64(50)))).
				Collect()

			if err != nil {
				b.Fatal(err)
			}
			result.Release()
			lazy.Release()
		}
	})

	// Note: We cannot easily benchmark without optimization since the optimizer
	// is integrated into the LazyFrame.Collect() method. This benchmark shows
	// the current performance including optimization overhead.
}
