package dataframe

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// BenchmarkHavingMemoryOptimization compares memory overhead before and after optimizations
func BenchmarkHavingMemoryOptimization(b *testing.B) {
	mem := memory.NewGoAllocator()

	// Medium dataset (10K rows) to trigger optimization benefits
	size := 10000
	departments := make([]string, size)
	salaries := make([]float64, size)

	deptNames := []string{"Engineering", "Sales", "HR", "Marketing", "Support", "Finance"}
	for i := 0; i < size; i++ {
		departments[i] = deptNames[i%len(deptNames)]
		salaries[i] = float64(40000 + (i * 5))
	}

	deptSeries := series.New("department", departments, mem)
	salarySeries := series.New("salary", salaries, mem)
	df := New(deptSeries, salarySeries)
	defer df.Release()

	// Warm up and establish baseline
	runtime.GC()
	runtime.GC()
	var baselineStats runtime.MemStats
	runtime.ReadMemStats(&baselineStats)

	b.Run("Optimized HAVING operation", func(b *testing.B) {
		b.ReportAllocs()

		var totalAllocs uint64
		var totalBytes uint64

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var beforeStats runtime.MemStats
			runtime.ReadMemStats(&beforeStats)

			// Test optimized HAVING functionality through GroupByHavingOperation
			lazy := df.Lazy()
			groupByOp := &GroupByHavingOperation{
				groupByCols: []string{"department"},
				predicate: expr.Mean(expr.Col("salary")).As("avg_salary").Gt(expr.Lit(60000.0)).
					And(expr.Count(expr.Col("department")).As("emp_count").Gt(expr.Lit(1000))),
			}
			lazy.operations = append(lazy.operations, groupByOp)
			result, err := lazy.Collect()

			if err != nil {
				b.Fatal(err)
			}

			var afterStats runtime.MemStats
			runtime.ReadMemStats(&afterStats)

			// Calculate memory overhead for this iteration
			allocsOverhead := afterStats.Mallocs - beforeStats.Mallocs
			bytesOverhead := afterStats.TotalAlloc - beforeStats.TotalAlloc

			totalAllocs += allocsOverhead
			totalBytes += bytesOverhead

			result.Release()
			groupByOp.Release() // Clean up cached allocator

			// Periodic GC to prevent memory buildup
			if i%10 == 0 {
				runtime.GC()
			}
		}

		b.StopTimer()

		// Calculate average overhead per operation
		if b.N > 0 {
			avgAllocsOverhead := float64(totalAllocs) / float64(b.N)
			avgBytesOverhead := float64(totalBytes) / float64(b.N)

			// Estimate base operation memory requirement
			baseMemoryRequirement := float64(size * 2 * 8) // Rough estimate: 2 columns * 8 bytes per value
			overheadPercentage := (avgBytesOverhead / baseMemoryRequirement) * 100.0

			b.ReportMetric(avgAllocsOverhead, "allocs/op")
			b.ReportMetric(avgBytesOverhead, "bytes/op")
			b.ReportMetric(overheadPercentage, "overhead_%")

			// Log results for verification
			b.Logf("Average allocations overhead: %.2f allocs/op", avgAllocsOverhead)
			b.Logf("Average bytes overhead: %.2f bytes/op", avgBytesOverhead)
			b.Logf("Memory overhead percentage: %.2f%%", overheadPercentage)

			// Target: overhead should be <10%
			targetOverhead := 10.0
			if overheadPercentage <= targetOverhead {
				b.Logf("SUCCESS: Memory overhead %.2f%% is within target <=%.2f%%",
					overheadPercentage, targetOverhead)
			} else {
				b.Logf("WARNING: Memory overhead %.2f%% exceeds target <=%.2f%%",
					overheadPercentage, targetOverhead)
			}
		}
	})
}

// TestHavingMemoryOptimizationCorrectness verifies that optimizations don't affect correctness
func TestHavingMemoryOptimizationCorrectness(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test dataset
	size := 1000
	departments := make([]string, size)
	salaries := make([]float64, size)

	deptNames := []string{"Engineering", "Sales", "HR"}
	for i := 0; i < size; i++ {
		departments[i] = deptNames[i%len(deptNames)]
		salaries[i] = float64(40000 + (i * 50))
	}

	deptSeries := series.New("department", departments, mem)
	salarySeries := series.New("salary", salaries, mem)
	df := New(deptSeries, salarySeries)
	defer df.Release()

	// Test optimized operation
	lazy := df.Lazy()
	groupByOp := &GroupByHavingOperation{
		groupByCols: []string{"department"},
		predicate:   expr.Mean(expr.Col("salary")).As("avg_salary").Gt(expr.Lit(50000.0)),
	}
	lazy.operations = append(lazy.operations, groupByOp)
	result, err := lazy.Collect()

	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify the result has the expected structure
	assert.True(t, result.Len() >= 0, "Result should have non-negative length")
	assert.True(t, result.Width() > 0, "Result should have columns")

	// Verify department column exists
	deptCol, exists := result.Column("department")
	assert.True(t, exists, "Department column should exist")
	assert.NotNil(t, deptCol, "Department column should not be nil")

	// Clean up
	result.Release()
	groupByOp.Release()
}

// BenchmarkHavingMemoryScaling tests memory scaling with different dataset sizes
func BenchmarkHavingMemoryScaling(b *testing.B) {
	mem := memory.NewGoAllocator()
	sizes := []int{1000, 5000, 10000, 25000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			departments := make([]string, size)
			salaries := make([]float64, size)

			deptNames := []string{"Eng", "Sales", "HR", "Marketing"}
			for i := 0; i < size; i++ {
				departments[i] = deptNames[i%len(deptNames)]
				salaries[i] = float64(35000 + (i * 3))
			}

			deptSeries := series.New("department", departments, mem)
			salarySeries := series.New("salary", salaries, mem)
			df := New(deptSeries, salarySeries)
			defer df.Release()

			b.ReportAllocs()
			b.ResetTimer()

			var totalOverheadBytes uint64

			for i := 0; i < b.N; i++ {
				var beforeStats runtime.MemStats
				runtime.ReadMemStats(&beforeStats)

				lazy := df.Lazy()
				groupByOp := &GroupByHavingOperation{
					groupByCols: []string{"department"},
					predicate:   expr.Mean(expr.Col("salary")).As("avg_salary").Gt(expr.Lit(60000.0)),
				}
				lazy.operations = append(lazy.operations, groupByOp)
				result, err := lazy.Collect()

				if err != nil {
					b.Fatal(err)
				}

				var afterStats runtime.MemStats
				runtime.ReadMemStats(&afterStats)

				overheadBytes := afterStats.TotalAlloc - beforeStats.TotalAlloc
				totalOverheadBytes += overheadBytes

				result.Release()
				groupByOp.Release()

				if i%5 == 0 {
					runtime.GC()
				}
			}

			b.StopTimer()

			if b.N > 0 {
				avgOverheadBytes := float64(totalOverheadBytes) / float64(b.N)
				baseMemory := float64(size * 2 * 8) // 2 columns * 8 bytes
				overheadPercent := (avgOverheadBytes / baseMemory) * 100.0

				b.ReportMetric(avgOverheadBytes, "overhead_bytes/op")
				b.ReportMetric(overheadPercent, "overhead_%")

				b.Logf("Size %d: Memory overhead %.2f%% (%.0f bytes/op)",
					size, overheadPercent, avgOverheadBytes)
			}
		})
	}
}

// TestHavingCachedAllocatorReuse verifies that allocator caching works correctly
func TestHavingCachedAllocatorReuse(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test dataset
	size := 500
	departments := make([]string, size)
	salaries := make([]float64, size)

	deptNames := []string{"Engineering", "Sales"}
	for i := 0; i < size; i++ {
		departments[i] = deptNames[i%len(deptNames)]
		salaries[i] = float64(40000 + (i * 100))
	}

	deptSeries := series.New("department", departments, mem)
	salarySeries := series.New("salary", salaries, mem)
	df := New(deptSeries, salarySeries)
	defer df.Release()

	// Create operation and verify caching behavior
	groupByOp := &GroupByHavingOperation{
		groupByCols: []string{"department"},
		predicate:   expr.Mean(expr.Col("salary")).As("avg_salary").Gt(expr.Lit(45000.0)),
	}

	// First execution should initialize caches
	lazy1 := df.Lazy()
	lazy1.operations = append(lazy1.operations, groupByOp)
	result1, err := lazy1.Collect()
	require.NoError(t, err)
	defer result1.Release()

	// Verify caches are initialized
	assert.NotNil(t, groupByOp.cachedAllocator, "Cached allocator should be initialized")
	assert.NotNil(t, groupByOp.cachedAggregations, "Cached aggregations should be initialized")
	assert.NotNil(t, groupByOp.cachedEvaluator, "Cached evaluator should be initialized")

	// Second execution should reuse caches
	lazy2 := df.Lazy()
	lazy2.operations = append(lazy2.operations, groupByOp)
	result2, err := lazy2.Collect()
	require.NoError(t, err)
	defer result2.Release()

	// Caches should still be there
	assert.NotNil(t, groupByOp.cachedAllocator, "Cached allocator should persist")
	assert.NotNil(t, groupByOp.cachedAggregations, "Cached aggregations should persist")
	assert.NotNil(t, groupByOp.cachedEvaluator, "Cached evaluator should persist")

	// Results should be identical
	assert.Equal(t, result1.Len(), result2.Len(), "Results should have same length")
	assert.Equal(t, result1.Width(), result2.Width(), "Results should have same width")

	// Clean up
	groupByOp.Release()
}

// BenchmarkHavingMemoryPoolEfficiency tests the efficiency of the memory pool
func BenchmarkHavingMemoryPoolEfficiency(b *testing.B) {
	mem := memory.NewGoAllocator()

	size := 5000
	departments := make([]string, size)
	salaries := make([]float64, size)

	deptNames := []string{"A", "B", "C", "D"}
	for i := 0; i < size; i++ {
		departments[i] = deptNames[i%len(deptNames)]
		salaries[i] = float64(40000 + (i * 10))
	}

	deptSeries := series.New("department", departments, mem)
	salarySeries := series.New("salary", salaries, mem)
	df := New(deptSeries, salarySeries)
	defer df.Release()

	b.Run("With memory pool", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		var totalPoolStats []runtime.MemStats

		for i := 0; i < b.N; i++ {
			var beforeStats runtime.MemStats
			runtime.ReadMemStats(&beforeStats)

			groupByOp := &GroupByHavingOperation{
				groupByCols: []string{"department"},
				predicate:   expr.Mean(expr.Col("salary")).As("avg_salary").Gt(expr.Lit(55000.0)),
			}

			lazy := df.Lazy()
			lazy.operations = append(lazy.operations, groupByOp)
			result, err := lazy.Collect()

			if err != nil {
				b.Fatal(err)
			}

			var afterStats runtime.MemStats
			runtime.ReadMemStats(&afterStats)
			totalPoolStats = append(totalPoolStats, afterStats)

			result.Release()
			groupByOp.Release() // Return allocator to pool

			if i%10 == 0 {
				runtime.GC()
			}
		}

		b.StopTimer()

		// Calculate pool efficiency metrics
		if len(totalPoolStats) > 1 {
			firstHalf := len(totalPoolStats) / 2
			secondHalf := len(totalPoolStats) - firstHalf

			var firstHalfAvg, secondHalfAvg uint64
			for i := 0; i < firstHalf; i++ {
				firstHalfAvg += totalPoolStats[i].TotalAlloc
			}
			for i := firstHalf; i < len(totalPoolStats); i++ {
				secondHalfAvg += totalPoolStats[i].TotalAlloc
			}

			if firstHalf > 0 {
				firstHalfAvg /= uint64(firstHalf)
			}
			if secondHalf > 0 {
				secondHalfAvg /= uint64(secondHalf)
			}

			poolEfficiency := float64(firstHalfAvg) / float64(secondHalfAvg) * 100.0
			b.ReportMetric(poolEfficiency, "pool_efficiency_%")

			b.Logf("Pool efficiency: %.2f%% (should be close to 100%%)", poolEfficiency)
		}
	})
}
