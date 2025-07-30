package expr

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// benchmarkSortPartition benchmarks the sortPartition function with different data sizes.
func BenchmarkSortPartition(b *testing.B) {
	partitionSizes := []int{100, 1000, 10000, 50000}

	for _, size := range partitionSizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			mem := memory.NewGoAllocator()
			evaluator := NewEvaluator(mem)

			// Create test data
			columns := createBenchmarkData(b, mem, size)
			defer func() {
				for _, arr := range columns {
					arr.Release()
				}
			}()

			// Create partition indices
			partition := make([]int, size)
			for i := range size {
				partition[i] = i
			}

			// Create ORDER BY clause
			orderBy := []OrderByExpr{
				{column: "value", ascending: true},
			}

			b.ResetTimer()
			for range b.N {
				_ = evaluator.sortPartition(partition, orderBy, columns)
			}
		})
	}
}

// benchmarkCompareRows benchmarks the compareRows function.
func BenchmarkCompareRows(b *testing.B) {
	mem := memory.NewGoAllocator()
	evaluator := NewEvaluator(mem)

	columns := createBenchmarkData(b, mem, 10000)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	orderBy := []OrderByExpr{
		{column: "value", ascending: true},
	}

	// Generate random indices to compare
	indices := make([][2]int, b.N)
	for i := range b.N {
		indices[i] = [2]int{
			rand.Intn(10000),
			rand.Intn(10000),
		}
	}

	b.ResetTimer()
	for i := range b.N {
		_, _ = evaluator.compareRows(indices[i][0], indices[i][1], orderBy, columns)
	}
}

// BenchmarkSortPartitionMultiColumn benchmarks sorting with multiple columns.
func BenchmarkSortPartitionMultiColumn(b *testing.B) {
	mem := memory.NewGoAllocator()
	evaluator := NewEvaluator(mem)

	size := 10000
	columns := createMultiColumnBenchmarkData(b, mem, size)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	partition := make([]int, size)
	for i := range size {
		partition[i] = i
	}

	orderBy := []OrderByExpr{
		{column: "category", ascending: true},
		{column: "value", ascending: false},
		{column: "score", ascending: true},
	}

	b.ResetTimer()
	for range b.N {
		_ = evaluator.sortPartition(partition, orderBy, columns)
	}
}

// createBenchmarkData creates test data for benchmarking.
func createBenchmarkData(b *testing.B, mem memory.Allocator, size int) map[string]arrow.Array {
	b.Helper()

	// Create random int64 values
	valueBuilder := array.NewInt64Builder(mem)
	defer valueBuilder.Release()

	values := make([]int64, size)
	for i := range size {
		values[i] = rand.Int63n(1000000)
	}
	valueBuilder.AppendValues(values, nil)

	return map[string]arrow.Array{
		"value": valueBuilder.NewArray(),
	}
}

// createMultiColumnBenchmarkData creates test data with multiple columns.
func createMultiColumnBenchmarkData(b *testing.B, mem memory.Allocator, size int) map[string]arrow.Array {
	b.Helper()

	// Create category column (string)
	categoryBuilder := array.NewStringBuilder(mem)
	defer categoryBuilder.Release()
	categories := []string{"A", "B", "C", "D", "E"}
	for range size {
		categoryBuilder.Append(categories[rand.Intn(len(categories))])
	}

	// Create value column (int64)
	valueBuilder := array.NewInt64Builder(mem)
	defer valueBuilder.Release()
	for range size {
		valueBuilder.Append(rand.Int63n(1000))
	}

	// Create score column (float64)
	scoreBuilder := array.NewFloat64Builder(mem)
	defer scoreBuilder.Release()
	for range size {
		scoreBuilder.Append(rand.Float64() * 100)
	}

	return map[string]arrow.Array{
		"category": categoryBuilder.NewArray(),
		"value":    valueBuilder.NewArray(),
		"score":    scoreBuilder.NewArray(),
	}
}
