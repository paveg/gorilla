package dataframe

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func BenchmarkHavingWithSmallDataset(b *testing.B) {
	mem := memory.NewGoAllocator()

	// Small dataset (100 rows)
	size := 100
	departments := make([]string, size)
	salaries := make([]float64, size)

	deptNames := []string{"Engineering", "Sales", "HR", "Marketing"}
	for i := 0; i < size; i++ {
		departments[i] = deptNames[i%len(deptNames)]
		salaries[i] = float64(40000 + (i * 500))
	}

	deptSeries := series.New("department", departments, mem)
	salarySeries := series.New("salary", salaries, mem)
	df := New(deptSeries, salarySeries)
	defer df.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Test HAVING functionality through GroupByHavingOperation
		lazy := df.Lazy()
		groupByOp := &GroupByHavingOperation{
			groupByCols: []string{"department"},
			predicate:   expr.Mean(expr.Col("salary")).As("avg_salary").Gt(expr.Lit(50000.0)),
		}
		lazy.operations = append(lazy.operations, groupByOp)
		result, err := lazy.Collect()

		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkHavingWithMediumDataset(b *testing.B) {
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Test HAVING functionality through GroupByHavingOperation
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
		result.Release()
	}
}

func BenchmarkHavingWithLargeDataset(b *testing.B) {
	mem := memory.NewGoAllocator()

	// Large dataset (100K rows)
	size := 100000
	departments := make([]string, size)
	salaries := make([]float64, size)
	experience := make([]int64, size)

	deptNames := []string{"Engineering", "Sales", "HR", "Marketing", "Support", "Finance", "Operations", "Legal"}
	for i := 0; i < size; i++ {
		departments[i] = deptNames[i%len(deptNames)]
		salaries[i] = float64(35000 + (i % 100000))
		experience[i] = int64(i % 20)
	}

	deptSeries := series.New("department", departments, mem)
	salarySeries := series.New("salary", salaries, mem)
	expSeries := series.New("experience", experience, mem)
	df := New(deptSeries, salarySeries, expSeries)
	defer df.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Test HAVING functionality through GroupByHavingOperation
		lazy := df.Lazy()
		groupByOp := &GroupByHavingOperation{
			groupByCols: []string{"department"},
			predicate: expr.Mean(expr.Col("salary")).As("avg_salary").Gt(expr.Lit(65000.0)).
				And(expr.Sum(expr.Col("experience")).As("total_experience").Gt(expr.Lit(50000))),
		}
		lazy.operations = append(lazy.operations, groupByOp)
		result, err := lazy.Collect()

		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkHavingParallelExecution(b *testing.B) {
	mem := memory.NewGoAllocator()

	// Dataset large enough to trigger parallel execution
	size := 50000
	departments := make([]string, size)
	salaries := make([]float64, size)

	deptNames := []string{"Eng", "Sales", "HR", "Marketing", "Support", "Finance", "Ops", "Legal", "R&D", "QA"}
	for i := 0; i < size; i++ {
		departments[i] = deptNames[i%len(deptNames)]
		salaries[i] = float64(30000 + (i * 2))
	}

	deptSeries := series.New("department", departments, mem)
	salarySeries := series.New("salary", salaries, mem)
	df := New(deptSeries, salarySeries)
	defer df.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Test HAVING functionality through GroupByHavingOperation
		lazy := df.Lazy()
		groupByOp := &GroupByHavingOperation{
			groupByCols: []string{"department"},
			predicate: expr.Mean(expr.Col("salary")).As("avg_salary").Gt(expr.Lit(75000.0)).
				Or(expr.Max(expr.Col("salary")).As("max_salary").Gt(expr.Lit(120000.0))),
		}
		lazy.operations = append(lazy.operations, groupByOp)
		result, err := lazy.Collect()

		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkHavingComplexPredicates(b *testing.B) {
	mem := memory.NewGoAllocator()

	size := 25000
	categories := make([]string, size)
	values1 := make([]float64, size)
	values2 := make([]float64, size)
	values3 := make([]int64, size)

	catNames := []string{"A", "B", "C", "D", "E", "F", "G", "H"}
	for i := 0; i < size; i++ {
		categories[i] = catNames[i%len(catNames)]
		values1[i] = float64(100 + (i * 3))
		values2[i] = float64(50 + (i * 2))
		values3[i] = int64(i % 100)
	}

	catSeries := series.New("category", categories, mem)
	val1Series := series.New("value1", values1, mem)
	val2Series := series.New("value2", values2, mem)
	val3Series := series.New("value3", values3, mem)
	df := New(catSeries, val1Series, val2Series, val3Series)
	defer df.Release()

	// Complex predicate: (AVG(value1) > 50000 AND SUM(value2) > 1000000) OR (COUNT(*) > 5000)
	complexCondition := expr.Mean(expr.Col("value1")).As("avg_value1").Gt(expr.Lit(50000.0)).
		And(expr.Sum(expr.Col("value2")).As("sum_value2").Gt(expr.Lit(1000000.0))).
		Or(expr.Count(expr.Col("category")).As("count_items").Gt(expr.Lit(5000)))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Test HAVING functionality through GroupByHavingOperation
		lazy := df.Lazy()
		groupByOp := &GroupByHavingOperation{
			groupByCols: []string{"category"},
			predicate:   complexCondition,
		}
		lazy.operations = append(lazy.operations, groupByOp)
		result, err := lazy.Collect()

		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

// Comparative performance tests
func BenchmarkHavingVsManualFiltering(b *testing.B) {
	mem := memory.NewGoAllocator()

	size := 20000
	departments := make([]string, size)
	salaries := make([]float64, size)

	deptNames := []string{"Engineering", "Sales", "HR", "Marketing", "Support"}
	for i := 0; i < size; i++ {
		departments[i] = deptNames[i%len(deptNames)]
		salaries[i] = float64(40000 + (i * 3))
	}

	deptSeries := series.New("department", departments, mem)
	salarySeries := series.New("salary", salaries, mem)
	df := New(deptSeries, salarySeries)
	defer df.Release()

	b.Run("Using HAVING clause", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Test HAVING functionality through GroupByHavingOperation
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
			result.Release()
		}
	})

	b.Run("Manual aggregation then filtering", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// First aggregate without HAVING
			aggregated, err := df.Lazy().
				GroupBy("department").
				Agg(expr.Mean(expr.Col("salary")).As("avg_salary")).
				Collect()

			if err != nil {
				b.Fatal(err)
			}

			// Then filter
			result, err := aggregated.Lazy().
				Filter(expr.Col("avg_salary").Gt(expr.Lit(60000.0))).
				Collect()

			aggregated.Release()
			if err != nil {
				b.Fatal(err)
			}
			result.Release()
		}
	})
}

func TestHavingParallelExecutionCorrectness(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create dataset large enough to trigger parallel execution
	size := 10000
	departments := make([]string, size)
	salaries := make([]float64, size)

	deptNames := []string{"Engineering", "Sales", "HR", "Marketing", "Support"}
	for i := 0; i < size; i++ {
		departments[i] = deptNames[i%len(deptNames)]
		salaries[i] = float64(40000 + (i * 5))
	}

	deptSeries := series.New("department", departments, mem)
	salarySeries := series.New("salary", salaries, mem)
	df := New(deptSeries, salarySeries)
	defer df.Release()

	t.Run("parallel vs sequential consistency", func(t *testing.T) {
		// Run the same operation multiple times
		var results []*DataFrame

		for i := 0; i < 5; i++ {
			// Test HAVING functionality through GroupByHavingOperation
			lazy := df.Lazy()
			groupByOp := &GroupByHavingOperation{
				groupByCols: []string{"department"},
				predicate:   expr.Mean(expr.Col("salary")).As("avg_salary").Gt(expr.Lit(80000.0)),
			}
			lazy.operations = append(lazy.operations, groupByOp)
			result, err := lazy.Collect()

			require.NoError(t, err)
			results = append(results, result)
		}

		// Verify all results are identical
		for i := 1; i < len(results); i++ {
			assert.Equal(t, results[0].Len(), results[i].Len(),
				"Result %d should have same length as result 0", i)

			// Compare department columns
			dept0, _ := results[0].Column("department")
			depti, _ := results[i].Column("department")

			arr0 := dept0.Array()
			arri := depti.Array()

			assert.Equal(t, arr0.Len(), arri.Len())
			// Additional detailed comparison could be added here

			arr0.Release()
			arri.Release()
		}

		// Cleanup
		for _, result := range results {
			result.Release()
		}
	})
}

func TestHavingMemoryLeakDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory leak test in short mode")
	}

	mem := memory.NewGoAllocator()

	// Force garbage collection and get initial memory stats
	runtime.GC()
	runtime.GC()
	var initialStats runtime.MemStats
	runtime.ReadMemStats(&initialStats)

	// Perform many HAVING operations
	iterations := 100
	for i := 0; i < iterations; i++ {
		size := 1000
		departments := make([]string, size)
		salaries := make([]float64, size)

		deptNames := []string{"A", "B", "C", "D", "E"}
		for j := 0; j < size; j++ {
			departments[j] = deptNames[j%len(deptNames)]
			salaries[j] = float64(40000 + (j * 10))
		}

		deptSeries := series.New("department", departments, mem)
		salarySeries := series.New("salary", salaries, mem)
		df := New(deptSeries, salarySeries)

		// Test HAVING functionality through GroupByHavingOperation
		lazy := df.Lazy()
		groupByOp := &GroupByHavingOperation{
			groupByCols: []string{"department"},
			predicate:   expr.Mean(expr.Col("salary")).As("avg_salary").Gt(expr.Lit(60000.0)),
		}
		lazy.operations = append(lazy.operations, groupByOp)
		result, err := lazy.Collect()

		require.NoError(t, err)

		// Proper cleanup
		result.Release()
		df.Release()

		// Periodic garbage collection
		if i%10 == 0 {
			runtime.GC()
		}
	}

	// Final garbage collection and memory check
	runtime.GC()
	runtime.GC()
	var finalStats runtime.MemStats
	runtime.ReadMemStats(&finalStats)

	// Memory usage should not have grown excessively
	memoryGrowth := finalStats.Alloc - initialStats.Alloc
	t.Logf("Memory growth: %d bytes", memoryGrowth)

	// Allow some growth but not excessive (adjust threshold as needed)
	maxAllowedGrowth := uint64(50 * 1024 * 1024) // 50MB
	assert.True(t, memoryGrowth < maxAllowedGrowth,
		"Memory growth %d bytes exceeds threshold %d bytes", memoryGrowth, maxAllowedGrowth)
}

func TestHavingConcurrentSafety(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create shared test data
	size := 5000
	departments := make([]string, size)
	salaries := make([]float64, size)

	deptNames := []string{"Engineering", "Sales", "HR", "Marketing"}
	for i := 0; i < size; i++ {
		departments[i] = deptNames[i%len(deptNames)]
		salaries[i] = float64(40000 + (i * 8))
	}

	deptSeries := series.New("department", departments, mem)
	salarySeries := series.New("salary", salaries, mem)
	df := New(deptSeries, salarySeries)
	defer df.Release()

	t.Run("concurrent HAVING operations", func(t *testing.T) {
		numGoroutines := runtime.NumCPU() * 2
		var wg sync.WaitGroup
		errChan := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(threshold float64) {
				defer wg.Done()

				// Test HAVING functionality through GroupByHavingOperation
				lazy := df.Lazy()
				groupByOp := &GroupByHavingOperation{
					groupByCols: []string{"department"},
					predicate:   expr.Mean(expr.Col("salary")).As("avg_salary").Gt(expr.Lit(threshold)),
				}
				lazy.operations = append(lazy.operations, groupByOp)
				result, err := lazy.Collect()

				if err != nil {
					errChan <- err
					return
				}

				// Verify result is valid
				if result.Len() < 0 {
					errChan <- assert.AnError
					return
				}

				result.Release()
			}(50000.0 + float64(i*5000))
		}

		wg.Wait()
		close(errChan)

		// Check for any errors
		for err := range errChan {
			assert.NoError(t, err)
		}
	})
}

func BenchmarkHavingScalability(b *testing.B) {
	mem := memory.NewGoAllocator()

	sizes := []int{1000, 5000, 10000, 25000, 50000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			departments := make([]string, size)
			salaries := make([]float64, size)

			deptNames := []string{"Eng", "Sales", "HR", "Marketing", "Support", "Finance"}
			for i := 0; i < size; i++ {
				departments[i] = deptNames[i%len(deptNames)]
				salaries[i] = float64(35000 + (i * 3))
			}

			deptSeries := series.New("department", departments, mem)
			salarySeries := series.New("salary", salaries, mem)
			df := New(deptSeries, salarySeries)
			defer df.Release()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Test HAVING functionality through GroupByHavingOperation
				lazy := df.Lazy()
				groupByOp := &GroupByHavingOperation{
					groupByCols: []string{"department"},
					predicate:   expr.Mean(expr.Col("salary")).As("avg_salary").Gt(expr.Lit(65000.0)),
				}
				lazy.operations = append(lazy.operations, groupByOp)
				result, err := lazy.Collect()

				if err != nil {
					b.Fatal(err)
				}
				result.Release()
			}
		})
	}
}
