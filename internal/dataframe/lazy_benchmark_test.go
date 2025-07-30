package dataframe

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
)

// BenchmarkLazyFrameSequentialVsParallel compares performance of sequential vs parallel execution.
func BenchmarkLazyFrameSequentialVsParallel(b *testing.B) {
	// Create test data
	sizes := []int{500, 1000, 2000, 5000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Sequential_%d", size), func(b *testing.B) {
			benchmarkLazyFrameSequential(b, size)
		})

		b.Run(fmt.Sprintf("Parallel_%d", size), func(b *testing.B) {
			benchmarkLazyFrameParallel(b, size)
		})
	}
}

func benchmarkLazyFrameSequential(b *testing.B, size int) {
	df := createBenchmarkDataFrame(size)
	defer df.Release()

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		lazyDf := df.Lazy()

		// Force sequential execution by using small threshold
		result, err := lazyDf.collectSequential()
		if err != nil {
			b.Fatalf("Sequential execution failed: %v", err)
		}

		// Verify results
		if result == nil || result.Width() == 0 {
			b.Fatalf("Expected non-empty result")
		}

		result.Release()
		lazyDf.Release()
	}
}

func benchmarkLazyFrameParallel(b *testing.B, size int) {
	df := createBenchmarkDataFrame(size)
	defer df.Release()

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		lazyDf := df.Lazy()

		// Force parallel execution
		result, err := lazyDf.collectParallel()
		if err != nil {
			b.Fatalf("Parallel execution failed: %v", err)
		}

		// Verify results
		if result == nil || result.Width() == 0 {
			b.Fatalf("Expected non-empty result")
		}

		result.Release()
		lazyDf.Release()
	}
}

func createBenchmarkDataFrame(size int) *DataFrame {
	mem := memory.NewGoAllocator()

	names := make([]string, size)
	ages := make([]int64, size)
	salaries := make([]float64, size)
	active := make([]bool, size)

	for i := range size {
		names[i] = fmt.Sprintf("Employee_%d", i)
		ages[i] = int64(25 + (i % 40))       // Ages 25-64
		salaries[i] = float64(40000 + i*100) // Increasing salaries
		active[i] = i%2 == 0                 // Alternating active status
	}

	nameSeries := series.New("name", names, mem)
	ageSeries := series.New("age", ages, mem)
	salarySeries := series.New("salary", salaries, mem)
	activeSeries := series.New("active", active, mem)

	df := New(nameSeries, ageSeries, salarySeries, activeSeries)

	// Apply complex operations chain to benchmark
	lazyDf := df.Lazy().
		Filter(expr.Col("active").Eq(expr.Lit(true))).
		WithColumn("bonus", expr.Col("salary").Mul(expr.Lit(0.1))).
		Filter(expr.Col("age").Gt(expr.Lit(int64(30)))).
		WithColumn("total_comp", expr.Col("salary").Add(expr.Col("bonus"))).
		Select("name", "age", "salary", "bonus", "total_comp")

	result, err := lazyDf.Collect()
	if err != nil {
		panic(fmt.Sprintf("Failed to create benchmark DataFrame: %v", err))
	}

	lazyDf.Release()
	return result
}

// BenchmarkMemoryManagement benchmarks memory allocation patterns.
func BenchmarkMemoryManagement(b *testing.B) {
	size := 5000
	df := createBenchmarkDataFrame(size)
	defer df.Release()

	b.Run("WithMemoryReuse", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for range b.N {
			lazyDf := df.Lazy()
			result, err := lazyDf.Collect()
			if err != nil {
				b.Fatalf("Execution failed: %v", err)
			}
			result.Release()
			lazyDf.Release()
		}
	})
}
