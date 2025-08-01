//nolint:testpackage // requires internal access to unexported types and functions
package dataframe

import (
	"runtime"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/require"
)

// BenchmarkHavingOverheadAnalysis compares overhead vs base operation.
func BenchmarkHavingOverheadAnalysis(b *testing.B) {
	df := createOverheadTestDataFrame()
	defer df.Release()

	b.Run("GroupBy only (baseline)", func(b *testing.B) {
		benchmarkBaselineGroupBy(b, df)
	})

	b.Run("GroupBy with HAVING (optimized)", func(b *testing.B) {
		benchmarkOptimizedHaving(b, df)
	})

	b.Run("Manual GroupBy + Filter (comparison)", func(b *testing.B) {
		benchmarkManualGroupByFilter(b, df)
	})
}

// createOverheadTestDataFrame creates test data for overhead analysis.
func createOverheadTestDataFrame() *DataFrame {
	mem := memory.NewGoAllocator()
	size := 10000
	departments := make([]string, size)
	salaries := make([]float64, size)

	deptNames := []string{"Engineering", "Sales", "HR", "Marketing", "Support", "Finance"}
	for i := range size {
		departments[i] = deptNames[i%len(deptNames)]
		salaries[i] = float64(40000 + (i * 5))
	}

	deptSeries := series.New("department", departments, mem)
	salarySeries := series.New("salary", salaries, mem)
	return New(deptSeries, salarySeries)
}

// benchmarkBaselineGroupBy runs baseline GroupBy benchmark.
func benchmarkBaselineGroupBy(b *testing.B, df *DataFrame) {
	b.ReportAllocs()
	var totalBytes uint64

	b.ResetTimer()
	for i := range b.N {
		bytes := measureMemoryUsage(func() error {
			result, err := df.Lazy().
				GroupBy("department").
				Agg(expr.Mean(expr.Col("salary")).As("avg_salary"),
					expr.Count(expr.Col("department")).As("emp_count")).
				Collect()
			if err != nil {
				return err
			}
			defer result.Release()
			return nil
		})

		if bytes < 0 {
			b.Fatal("Memory measurement failed")
		}

		totalBytes += uint64(bytes)
		triggerGCPeriodically(i)
	}

	reportAverageMemory(b, totalBytes, "baseline_bytes/op", "baseline")
}

// benchmarkOptimizedHaving runs optimized HAVING benchmark.
func benchmarkOptimizedHaving(b *testing.B, df *DataFrame) {
	b.ReportAllocs()
	var totalBytes uint64

	b.ResetTimer()
	for i := range b.N {
		bytes := measureMemoryUsage(func() error {
			lazy := df.Lazy()
			groupByOp := &GroupByHavingOperation{
				groupByCols: []string{"department"},
				predicate: expr.Mean(expr.Col("salary")).As("avg_salary").Gt(expr.Lit(60000.0)).
					And(expr.Count(expr.Col("department")).As("emp_count").Gt(expr.Lit(1000))),
			}
			lazy.operations = append(lazy.operations, groupByOp)

			result, err := lazy.Collect()
			if err != nil {
				return err
			}
			defer result.Release()
			defer groupByOp.Release()
			return nil
		})

		if bytes < 0 {
			b.Fatal("Memory measurement failed")
		}

		totalBytes += uint64(bytes)
		triggerGCPeriodically(i)
	}

	reportAverageMemory(b, totalBytes, "having_bytes/op", "HAVING")
}

// benchmarkManualGroupByFilter runs manual GroupBy + Filter benchmark.
func benchmarkManualGroupByFilter(b *testing.B, df *DataFrame) {
	b.ReportAllocs()
	var totalBytes uint64

	b.ResetTimer()
	for i := range b.N {
		bytes := measureMemoryUsage(func() error {
			aggregated, err := df.Lazy().
				GroupBy("department").
				Agg(expr.Mean(expr.Col("salary")).As("avg_salary"),
					expr.Count(expr.Col("department")).As("emp_count")).
				Collect()
			if err != nil {
				return err
			}
			defer aggregated.Release()

			result, err := aggregated.Lazy().
				Filter(expr.Col("avg_salary").Gt(expr.Lit(60000.0)).
					And(expr.Col("emp_count").Gt(expr.Lit(1000)))).
				Collect()
			if err != nil {
				return err
			}
			defer result.Release()
			return nil
		})

		if bytes < 0 {
			b.Fatal("Memory measurement failed")
		}

		totalBytes += uint64(bytes)
		triggerGCPeriodically(i)
	}

	reportAverageMemory(b, totalBytes, "manual_bytes/op", "manual")
}

// measureMemoryUsage measures memory usage of a function execution.
func measureMemoryUsage(fn func() error) int64 {
	var beforeStats runtime.MemStats
	runtime.ReadMemStats(&beforeStats)

	if err := fn(); err != nil {
		return -1
	}

	var afterStats runtime.MemStats
	runtime.ReadMemStats(&afterStats)

	return int64(afterStats.TotalAlloc - beforeStats.TotalAlloc)
}

// triggerGCPeriodically triggers GC every 10 iterations.
func triggerGCPeriodically(iteration int) {
	if iteration%10 == 0 {
		runtime.GC()
	}
}

// reportAverageMemory reports average memory usage metrics.
func reportAverageMemory(b *testing.B, totalBytes uint64, metric, label string) {
	b.StopTimer()
	if b.N > 0 {
		avgBytes := float64(totalBytes) / float64(b.N)
		b.ReportMetric(avgBytes, metric)
		b.Logf("Average %s memory: %.0f bytes/op", label, avgBytes)
	}
}

// TestHavingOverheadCalculation calculates actual overhead percentage.
func TestHavingOverheadCalculation(t *testing.T) {
	mem := memory.NewGoAllocator()

	size := 5000
	departments := make([]string, size)
	salaries := make([]float64, size)

	deptNames := []string{"Engineering", "Sales", "HR"}
	for i := range size {
		departments[i] = deptNames[i%len(deptNames)]
		salaries[i] = float64(40000 + (i * 50))
	}

	deptSeries := series.New("department", departments, mem)
	salarySeries := series.New("salary", salaries, mem)
	df := New(deptSeries, salarySeries)
	defer df.Release()

	// Measure baseline (GroupBy only)
	runtime.GC()
	runtime.GC()
	var baselineBefore runtime.MemStats
	runtime.ReadMemStats(&baselineBefore)

	baselineResult, err := df.Lazy().
		GroupBy("department").
		Agg(expr.Mean(expr.Col("salary")).As("avg_salary")).
		Collect()
	require.NoError(t, err)

	var baselineAfter runtime.MemStats
	runtime.ReadMemStats(&baselineAfter)
	baselineMemory := baselineAfter.TotalAlloc - baselineBefore.TotalAlloc
	baselineResult.Release()

	// Measure HAVING operation
	runtime.GC()
	runtime.GC()
	var havingBefore runtime.MemStats
	runtime.ReadMemStats(&havingBefore)

	lazy := df.Lazy()
	groupByOp := &GroupByHavingOperation{
		groupByCols: []string{"department"},
		predicate:   expr.Mean(expr.Col("salary")).As("avg_salary").Gt(expr.Lit(50000.0)),
	}
	lazy.operations = append(lazy.operations, groupByOp)
	havingResult, err := lazy.Collect()
	require.NoError(t, err)

	var havingAfter runtime.MemStats
	runtime.ReadMemStats(&havingAfter)
	havingMemory := havingAfter.TotalAlloc - havingBefore.TotalAlloc

	havingResult.Release()
	groupByOp.Release()

	// Calculate overhead
	overhead := havingMemory - baselineMemory
	overheadPercentage := (float64(overhead) / float64(baselineMemory)) * 100.0

	t.Logf("Baseline memory (GroupBy only): %d bytes", baselineMemory)
	t.Logf("HAVING memory (GroupBy + filter): %d bytes", havingMemory)
	t.Logf("Actual overhead: %d bytes", overhead)
	t.Logf("Overhead percentage: %.2f%%", overheadPercentage)

	// Target: overhead should be <10%
	targetOverhead := 10.0
	if overheadPercentage <= targetOverhead {
		t.Logf("SUCCESS: Memory overhead %.2f%% is within target <=%.2f%%",
			overheadPercentage, targetOverhead)
	} else {
		t.Logf("INFO: Memory overhead %.2f%% exceeds target <=%.2f%% - may need further optimization",
			overheadPercentage, targetOverhead)
	}
}

// BenchmarkHavingFilterEfficiency tests different filtering approaches.
func BenchmarkHavingFilterEfficiency(b *testing.B) {
	mem := memory.NewGoAllocator()

	size := 8000
	departments := make([]string, size)
	salaries := make([]float64, size)

	deptNames := []string{"A", "B", "C", "D", "E"}
	for i := range size {
		departments[i] = deptNames[i%len(deptNames)]
		salaries[i] = float64(35000 + (i * 8))
	}

	deptSeries := series.New("department", departments, mem)
	salarySeries := series.New("salary", salaries, mem)
	df := New(deptSeries, salarySeries)
	defer df.Release()

	b.Run("Optimized filter (pooled allocators)", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			lazy := df.Lazy()
			groupByOp := &GroupByHavingOperation{
				groupByCols: []string{"department"},
				predicate:   expr.Mean(expr.Col("salary")).As("avg_salary").Gt(expr.Lit(55000.0)),
			}
			lazy.operations = append(lazy.operations, groupByOp)
			result, err := lazy.Collect()

			if err != nil {
				b.Fatal(err)
			}
			result.Release()
			groupByOp.Release() // Return allocator to pool
		}
	})

	b.Run("Traditional filter (new allocators)", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			// Use separate operations to simulate non-optimized approach
			aggregated, err := df.Lazy().
				GroupBy("department").
				Agg(expr.Mean(expr.Col("salary")).As("avg_salary")).
				Collect()

			if err != nil {
				b.Fatal(err)
			}

			result, err := aggregated.Lazy().
				Filter(expr.Col("avg_salary").Gt(expr.Lit(55000.0))).
				Collect()

			aggregated.Release()
			if err != nil {
				b.Fatal(err)
			}
			result.Release()
		}
	})
}
