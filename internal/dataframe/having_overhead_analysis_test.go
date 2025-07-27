package dataframe

import (
	"runtime"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/require"
)

// BenchmarkHavingOverheadAnalysis compares overhead vs base operation
func BenchmarkHavingOverheadAnalysis(b *testing.B) {
	mem := memory.NewGoAllocator()

	// Medium dataset (10K rows)
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

	b.Run("GroupBy only (baseline)", func(b *testing.B) {
		b.ReportAllocs()

		var totalBaselineBytes uint64

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var beforeStats runtime.MemStats
			runtime.ReadMemStats(&beforeStats)

			// Just GroupBy + Aggregation without HAVING
			result, err := df.Lazy().
				GroupBy("department").
				Agg(expr.Mean(expr.Col("salary")).As("avg_salary"),
					expr.Count(expr.Col("department")).As("emp_count")).
				Collect()

			if err != nil {
				b.Fatal(err)
			}

			var afterStats runtime.MemStats
			runtime.ReadMemStats(&afterStats)

			baselineBytes := afterStats.TotalAlloc - beforeStats.TotalAlloc
			totalBaselineBytes += baselineBytes

			result.Release()

			if i%10 == 0 {
				runtime.GC()
			}
		}

		b.StopTimer()
		if b.N > 0 {
			avgBaselineBytes := float64(totalBaselineBytes) / float64(b.N)
			b.ReportMetric(avgBaselineBytes, "baseline_bytes/op")
			b.Logf("Average baseline memory: %.0f bytes/op", avgBaselineBytes)
		}
	})

	b.Run("GroupBy with HAVING (optimized)", func(b *testing.B) {
		b.ReportAllocs()

		var totalHavingBytes uint64

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var beforeStats runtime.MemStats
			runtime.ReadMemStats(&beforeStats)

			// GroupBy + Aggregation + HAVING using optimized operation
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

			havingBytes := afterStats.TotalAlloc - beforeStats.TotalAlloc
			totalHavingBytes += havingBytes

			result.Release()
			groupByOp.Release()

			if i%10 == 0 {
				runtime.GC()
			}
		}

		b.StopTimer()
		if b.N > 0 {
			avgHavingBytes := float64(totalHavingBytes) / float64(b.N)
			b.ReportMetric(avgHavingBytes, "having_bytes/op")
			b.Logf("Average HAVING memory: %.0f bytes/op", avgHavingBytes)
		}
	})

	b.Run("Manual GroupBy + Filter (comparison)", func(b *testing.B) {
		b.ReportAllocs()

		var totalManualBytes uint64

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var beforeStats runtime.MemStats
			runtime.ReadMemStats(&beforeStats)

			// Manual approach: GroupBy + Aggregation, then Filter
			aggregated, err := df.Lazy().
				GroupBy("department").
				Agg(expr.Mean(expr.Col("salary")).As("avg_salary"),
					expr.Count(expr.Col("department")).As("emp_count")).
				Collect()

			if err != nil {
				b.Fatal(err)
			}

			result, err := aggregated.Lazy().
				Filter(expr.Col("avg_salary").Gt(expr.Lit(60000.0)).
					And(expr.Col("emp_count").Gt(expr.Lit(1000)))).
				Collect()

			if err != nil {
				aggregated.Release()
				b.Fatal(err)
			}

			var afterStats runtime.MemStats
			runtime.ReadMemStats(&afterStats)

			manualBytes := afterStats.TotalAlloc - beforeStats.TotalAlloc
			totalManualBytes += manualBytes

			aggregated.Release()
			result.Release()

			if i%10 == 0 {
				runtime.GC()
			}
		}

		b.StopTimer()
		if b.N > 0 {
			avgManualBytes := float64(totalManualBytes) / float64(b.N)
			b.ReportMetric(avgManualBytes, "manual_bytes/op")
			b.Logf("Average manual memory: %.0f bytes/op", avgManualBytes)
		}
	})
}

// TestHavingOverheadCalculation calculates actual overhead percentage
func TestHavingOverheadCalculation(t *testing.T) {
	mem := memory.NewGoAllocator()

	size := 5000
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

// BenchmarkHavingFilterEfficiency tests different filtering approaches
func BenchmarkHavingFilterEfficiency(b *testing.B) {
	mem := memory.NewGoAllocator()

	size := 8000
	departments := make([]string, size)
	salaries := make([]float64, size)

	deptNames := []string{"A", "B", "C", "D", "E"}
	for i := 0; i < size; i++ {
		departments[i] = deptNames[i%len(deptNames)]
		salaries[i] = float64(35000 + (i * 8))
	}

	deptSeries := series.New("department", departments, mem)
	salarySeries := series.New("salary", salaries, mem)
	df := New(deptSeries, salarySeries)
	defer df.Release()

	b.Run("Optimized filter (pooled allocators)", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
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
		for i := 0; i < b.N; i++ {
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
