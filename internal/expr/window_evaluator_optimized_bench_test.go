package expr

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// BenchmarkSortPartitionOptimizedVsOriginal compares optimized vs original implementation
func BenchmarkSortPartitionOptimizedVsOriginal(b *testing.B) {
	partitionSizes := []int{100, 1000, 10000, 50000}

	for _, size := range partitionSizes {
		b.Run(fmt.Sprintf("original_size_%d", size), func(b *testing.B) {
			mem := memory.NewGoAllocator()
			evaluator := NewEvaluator(mem)

			columns := createBenchmarkDataOptimized(b, mem, size)
			defer func() {
				for _, arr := range columns {
					arr.Release()
				}
			}()

			partition := make([]int, size)
			for i := 0; i < size; i++ {
				partition[i] = i
			}

			orderBy := []OrderByExpr{
				{column: "value", ascending: true},
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = evaluator.sortPartition(partition, orderBy, columns)
			}
		})

		b.Run(fmt.Sprintf("optimized_size_%d", size), func(b *testing.B) {
			mem := memory.NewGoAllocator()
			evaluator := NewEvaluator(mem)

			columns := createBenchmarkDataOptimized(b, mem, size)
			defer func() {
				for _, arr := range columns {
					arr.Release()
				}
			}()

			partition := make([]int, size)
			for i := 0; i < size; i++ {
				partition[i] = i
			}

			orderBy := []OrderByExpr{
				{column: "value", ascending: true},
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = evaluator.sortPartitionOptimized(partition, orderBy, columns)
			}
		})
	}
}

// BenchmarkSortPartitionOptimizedMultiColumn tests multi-column sorting performance
func BenchmarkSortPartitionOptimizedMultiColumn(b *testing.B) {
	size := 10000

	b.Run("original_multi", func(b *testing.B) {
		mem := memory.NewGoAllocator()
		evaluator := NewEvaluator(mem)

		columns := createMultiColumnBenchmarkDataOptimized(b, mem, size)
		defer func() {
			for _, arr := range columns {
				arr.Release()
			}
		}()

		partition := make([]int, size)
		for i := 0; i < size; i++ {
			partition[i] = i
		}

		orderBy := []OrderByExpr{
			{column: "category", ascending: true},
			{column: "value", ascending: false},
			{column: "score", ascending: true},
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = evaluator.sortPartition(partition, orderBy, columns)
		}
	})

	b.Run("optimized_multi", func(b *testing.B) {
		mem := memory.NewGoAllocator()
		evaluator := NewEvaluator(mem)

		columns := createMultiColumnBenchmarkDataOptimized(b, mem, size)
		defer func() {
			for _, arr := range columns {
				arr.Release()
			}
		}()

		partition := make([]int, size)
		for i := 0; i < size; i++ {
			partition[i] = i
		}

		orderBy := []OrderByExpr{
			{column: "category", ascending: true},
			{column: "value", ascending: false},
			{column: "score", ascending: true},
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = evaluator.sortPartitionOptimized(partition, orderBy, columns)
		}
	})
}

// BenchmarkCompareRowsOptimized benchmarks the optimized type-specific comparators
func BenchmarkCompareRowsOptimized(b *testing.B) {
	mem := memory.NewGoAllocator()
	evaluator := NewEvaluator(mem)

	size := 10000
	columns := createBenchmarkDataOptimized(b, mem, size)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	orderBy := []OrderByExpr{
		{column: "value", ascending: true},
	}

	b.Run("original_compare", func(b *testing.B) {
		indices := make([][2]int, b.N)
		for i := 0; i < b.N; i++ {
			indices[i] = [2]int{
				rand.Intn(size), //nolint:gosec // Weak random OK for benchmarks
				rand.Intn(size), //nolint:gosec // Weak random OK for benchmarks
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = evaluator.compareRows(indices[i][0], indices[i][1], orderBy, columns)
		}
	})

	b.Run("optimized_compare", func(b *testing.B) {
		comp := createComparator(columns["value"], true)
		indices := make([][2]int, b.N)
		for i := 0; i < b.N; i++ {
			indices[i] = [2]int{
				rand.Intn(size), //nolint:gosec // Weak random OK for benchmarks
				rand.Intn(size), //nolint:gosec // Weak random OK for benchmarks
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = comp.Compare(columns["value"], indices[i][0], indices[i][1])
		}
	})
}

// createBenchmarkDataOptimized creates test data for benchmarking
func createBenchmarkDataOptimized(b *testing.B, mem memory.Allocator, size int) map[string]arrow.Array {
	b.Helper()

	valueBuilder := array.NewInt64Builder(mem)
	defer valueBuilder.Release()

	values := make([]int64, size)
	for i := 0; i < size; i++ {
		values[i] = rand.Int63n(1000000) //nolint:gosec // Weak random OK for benchmarks
	}
	valueBuilder.AppendValues(values, nil)

	return map[string]arrow.Array{
		"value": valueBuilder.NewArray(),
	}
}

// createMultiColumnBenchmarkDataOptimized creates test data with multiple columns
func createMultiColumnBenchmarkDataOptimized(b *testing.B, mem memory.Allocator, size int) map[string]arrow.Array {
	b.Helper()

	categoryBuilder := array.NewStringBuilder(mem)
	defer categoryBuilder.Release()
	categories := []string{"A", "B", "C", "D", "E"}
	for i := 0; i < size; i++ {
		categoryBuilder.Append(categories[rand.Intn(len(categories))]) //nolint:gosec // Weak random OK for benchmarks
	}

	valueBuilder := array.NewInt64Builder(mem)
	defer valueBuilder.Release()
	for i := 0; i < size; i++ {
		valueBuilder.Append(rand.Int63n(1000)) //nolint:gosec // Weak random OK for benchmarks
	}

	scoreBuilder := array.NewFloat64Builder(mem)
	defer scoreBuilder.Release()
	for i := 0; i < size; i++ {
		scoreBuilder.Append(rand.Float64() * 100) //nolint:gosec // Weak random OK for benchmarks
	}

	return map[string]arrow.Array{
		"category": categoryBuilder.NewArray(),
		"value":    valueBuilder.NewArray(),
		"score":    scoreBuilder.NewArray(),
	}
}
