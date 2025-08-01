package gorilla_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/require"
)

// BenchmarkHavingMemoryAllocations profiles memory usage patterns.
func BenchmarkHavingMemoryAllocations(b *testing.B) {
	mem := memory.NewGoAllocator()

	size := 10000
	departments := make([]string, size)
	salaries := make([]float64, size)

	deptNames := []string{"Engineering", "Sales", "HR", "Marketing", "Support"}
	for i := range size {
		departments[i] = deptNames[i%len(deptNames)]
		salaries[i] = float64(40000 + (i * 3))
	}

	deptSeries := series.New("department", departments, mem)
	salarySeries := series.New("salary", salaries, mem)
	df := dataframe.New(deptSeries, salarySeries)
	defer df.Release()

	b.Run("HavingImplementation", func(b *testing.B) {
		var allocs, totalAlloc, sys, heapInUse uint64
		var gcCount uint32

		runtime.GC()
		runtime.GC() // Double GC to stabilize

		var memBefore runtime.MemStats
		runtime.ReadMemStats(&memBefore)

		b.ResetTimer()
		b.ReportAllocs()

		for range b.N {
			result, err := df.Lazy().
				GroupBy("department").
				Having(expr.Mean(expr.Col("salary")).As("avg_salary").Gt(expr.Lit(60000.0))).
				Collect()

			if err != nil {
				b.Fatal(err)
			}
			result.Release()
		}

		b.StopTimer()

		var memAfter runtime.MemStats
		runtime.ReadMemStats(&memAfter)

		allocs = memAfter.Mallocs - memBefore.Mallocs
		totalAlloc = memAfter.TotalAlloc - memBefore.TotalAlloc
		sys = memAfter.Sys - memBefore.Sys
		heapInUse = memAfter.HeapInuse - memBefore.HeapInuse
		gcCount = memAfter.NumGC - memBefore.NumGC

		if b.N > 0 {
			b.Logf("Memory allocations per op: %d", allocs/uint64(b.N))
			b.Logf("Total memory allocated per op: %d bytes", totalAlloc/uint64(b.N))
			b.Logf("System memory per op: %d bytes", sys/uint64(b.N))
			b.Logf("Heap in use per op: %d bytes", heapInUse/uint64(b.N))
		}
		b.Logf("GC count: %d", gcCount)
	})

	b.Run("ManualImplementation", func(b *testing.B) {
		var allocs, totalAlloc, sys, heapInUse uint64
		var gcCount uint32

		runtime.GC()
		runtime.GC() // Double GC to stabilize

		var memBefore runtime.MemStats
		runtime.ReadMemStats(&memBefore)

		b.ResetTimer()
		b.ReportAllocs()

		for range b.N {
			// Manual groupby then filter
			aggregated, err := df.Lazy().
				GroupBy("department").
				Agg(expr.Mean(expr.Col("salary")).As("avg_salary")).
				Collect()
			require.NoError(b, err)

			result, err := aggregated.Lazy().
				Filter(expr.Col("avg_salary").Gt(expr.Lit(60000.0))).
				Collect()

			aggregated.Release()
			require.NoError(b, err)
			result.Release()
		}

		b.StopTimer()

		var memAfter runtime.MemStats
		runtime.ReadMemStats(&memAfter)

		allocs = memAfter.Mallocs - memBefore.Mallocs
		totalAlloc = memAfter.TotalAlloc - memBefore.TotalAlloc
		sys = memAfter.Sys - memBefore.Sys
		heapInUse = memAfter.HeapInuse - memBefore.HeapInuse
		gcCount = memAfter.NumGC - memBefore.NumGC

		if b.N > 0 {
			b.Logf("Memory allocations per op: %d", allocs/uint64(b.N))
			b.Logf("Total memory allocated per op: %d bytes", totalAlloc/uint64(b.N))
			b.Logf("System memory per op: %d bytes", sys/uint64(b.N))
			b.Logf("Heap in use per op: %d bytes", heapInUse/uint64(b.N))
		}
		b.Logf("GC count: %d", gcCount)
	})
}

// TestHavingMemoryLeakDetection specifically tests for memory leaks.
func TestHavingMemoryLeakDetection(t *testing.T) {
	mem := memory.NewGoAllocator()

	size := 5000
	departments := make([]string, size)
	salaries := make([]float64, size)

	deptNames := []string{"Engineering", "Sales", "HR"}
	for i := range size {
		departments[i] = deptNames[i%len(deptNames)]
		salaries[i] = float64(40000 + (i * 3))
	}

	deptSeries := series.New("department", departments, mem)
	salarySeries := series.New("salary", salaries, mem)
	df := dataframe.New(deptSeries, salarySeries)
	defer df.Release()

	// Record initial memory
	runtime.GC()
	runtime.GC()
	var initialMem runtime.MemStats
	runtime.ReadMemStats(&initialMem)

	// Run many operations
	iterations := 100
	for i := range iterations {
		result, err := df.Lazy().
			GroupBy("department").
			Having(expr.Mean(expr.Col("salary")).As("avg_salary").Gt(expr.Lit(55000.0))).
			Collect()

		require.NoError(t, err)
		result.Release()

		// Force GC every 20 iterations
		if i%20 == 0 {
			runtime.GC()
		}
	}

	// Force final GC
	runtime.GC()
	runtime.GC()
	time.Sleep(100 * time.Millisecond) // Allow cleanup
	runtime.GC()

	var finalMem runtime.MemStats
	runtime.ReadMemStats(&finalMem)

	// Check for memory leaks
	memoryGrowth := finalMem.HeapInuse - initialMem.HeapInuse
	allocGrowth := finalMem.Mallocs - initialMem.Mallocs

	t.Logf("Initial HeapInUse: %d bytes", initialMem.HeapInuse)
	t.Logf("Final HeapInUse: %d bytes", finalMem.HeapInuse)
	t.Logf("Memory growth: %d bytes", memoryGrowth)
	t.Logf("Allocation growth: %d", allocGrowth)
	if iterations > 0 {
		t.Logf("Memory growth per iteration: %d bytes", memoryGrowth/uint64(iterations))

		// Warn if memory growth is substantial
		memoryGrowthPerIteration := memoryGrowth / uint64(iterations)
		if memoryGrowthPerIteration > 1024 { // More than 1KB per iteration suggests a leak
			t.Logf("WARNING: Potential memory leak detected. Growth: %d bytes per iteration", memoryGrowthPerIteration)
		}
	}
}
