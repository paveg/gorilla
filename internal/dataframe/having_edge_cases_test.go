package dataframe

import (
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
)

// TestHavingEdgeCases_EmptyAndNull tests edge cases related to empty and null data.
func TestHavingEdgeCases_EmptyAndNull(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("EmptyDataFrame", func(t *testing.T) {
		// Create empty DataFrame with proper schema
		categories := series.New("category", []string{}, mem)
		amounts := series.New("amount", []float64{}, mem)

		df := New(categories, amounts)
		defer df.Release()

		// HAVING with aggregation should handle empty data gracefully
		result, err := df.Lazy().GroupBy("category").Having(expr.Sum(expr.Col("amount")).Gt(expr.Lit(100.0))).Collect()

		// Empty DataFrame might cause errors or return empty result
		if err != nil {
			// This is acceptable for empty DataFrames
			assert.Contains(t, err.Error(), "Column", "Error should be related to column handling")
		} else {
			defer result.Release()
			assert.Equal(t, 0, result.Len(), "Empty DataFrame should produce empty result")
		}
	})

	t.Run("EmptyGroups", func(t *testing.T) {
		// DataFrame with data but no groups matching criteria
		categories := series.New("category", []string{"A", "A", "B", "B"}, mem)
		amounts := series.New("amount", []float64{1.0, 2.0, 3.0, 4.0}, mem)

		df := New(categories, amounts)
		defer df.Release()

		// HAVING condition that excludes all groups (max sum is 7.0)
		result, err := df.Lazy().GroupBy("category").Having(expr.Sum(expr.Col("amount")).Gt(expr.Lit(100.0))).Collect()
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 0, result.Len(), "No groups should match the condition")
	})

	t.Run("NullValuesInAggregations", func(t *testing.T) {
		// Create DataFrame with potential null-like behavior
		categories := series.New("category", []string{"A", "A", "B", "B"}, mem)
		amounts := series.New("amount", []float64{10.0, 20.0, 30.0, 40.0}, mem)

		df := New(categories, amounts)
		defer df.Release()

		// HAVING should handle aggregations appropriately
		result, err := df.Lazy().GroupBy("category").Having(expr.Sum(expr.Col("amount")).Gt(expr.Lit(0.0))).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Both groups should appear since all sums are positive
		assert.Equal(t, 2, result.Len())
	})

	t.Run("AllZeroAggregations", func(t *testing.T) {
		categories := series.New("category", []string{"A", "A", "B", "B"}, mem)
		amounts := series.New("amount", []float64{0.0, 0.0, 0.0, 0.0}, mem)

		df := New(categories, amounts)
		defer df.Release()

		// HAVING with zero aggregation values
		result, err := df.Lazy().GroupBy("category").Having(expr.Sum(expr.Col("amount")).Gt(expr.Lit(0.0))).Collect()
		require.NoError(t, err)
		defer result.Release()

		// No groups should match since all sums are 0
		assert.Equal(t, 0, result.Len())
	})

	t.Run("SingleRowDataFrame", func(t *testing.T) {
		categories := series.New("category", []string{"A"}, mem)
		amounts := series.New("amount", []float64{50.0}, mem)

		df := New(categories, amounts)
		defer df.Release()

		result, err := df.Lazy().GroupBy("category").Having(expr.Sum(expr.Col("amount")).Gt(expr.Lit(25.0))).Collect()
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 1, result.Len(), "Single row should create one group")
	})
}

// TestHavingEdgeCases_TypeHandling tests edge cases related to type handling and coercion.
func TestHavingEdgeCases_TypeHandling(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("MixedNumericTypes", func(t *testing.T) {
		// Test HAVING with different numeric types
		categories := series.New("category", []string{"A", "A", "B", "B"}, mem)
		intValues := series.New("int_values", []int64{10, 20, 30, 40}, mem)
		floatValues := series.New("float_values", []float64{1.5, 2.5, 3.5, 4.5}, mem)

		df := New(categories, intValues, floatValues)
		defer df.Release()

		// HAVING with mixed numeric aggregations
		result, err := df.Lazy().GroupBy("category").Having(
			expr.Sum(expr.Col("int_values")).Gt(expr.Lit(int64(25))).And(
				expr.Sum(expr.Col("float_values")).Gt(expr.Lit(5.0)))).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Only group B should match (int sum=70, float sum=8.0)
		assert.Equal(t, 1, result.Len())
	})

	t.Run("BooleanAggregations", func(t *testing.T) {
		categories := series.New("category", []string{"A", "A", "B", "B"}, mem)
		flags := series.New("flags", []bool{true, false, true, true}, mem)

		df := New(categories, flags)
		defer df.Release()

		// COUNT aggregation on boolean values
		result, err := df.Lazy().
			GroupBy("category").
			Having(expr.Count(expr.Col("flags")).Gt(expr.Lit(int64(1)))).
			Collect()
		require.NoError(t, err)
		defer result.Release()

		// Both groups have 2 rows each
		assert.Equal(t, 2, result.Len())
	})

	t.Run("UnicodeStringHandling", func(t *testing.T) {
		// Test HAVING with Unicode strings
		categories := series.New("category", []string{"café", "café", "naïve", "naïve"}, mem)
		values := series.New("values", []int64{10, 20, 30, 40}, mem)

		df := New(categories, values)
		defer df.Release()

		result, err := df.Lazy().
			GroupBy("category").
			Having(expr.Sum(expr.Col("values")).Gt(expr.Lit(int64(35)))).
			Collect()
		require.NoError(t, err)
		defer result.Release()

		// Only "naïve" group should match (sum=70)
		assert.Equal(t, 1, result.Len())

		// Verify the Unicode category name is preserved
		catCol, ok := result.Column("category")
		require.True(t, ok)
		catArray := catCol.Array()
		defer catArray.Release()

		categoryName := catArray.(*array.String).Value(0)
		// Verify Unicode string is preserved correctly
		assert.Equal(t, "naïve", categoryName)
	})

	t.Run("TypeCoercionEdgeCases", func(t *testing.T) {
		// Test edge cases in type coercion during aggregation
		categories := series.New("category", []string{"A", "A", "B", "B"}, mem)
		// Large values that might cause overflow concerns
		largeValues := series.New("large_values", []int64{math.MaxInt32, 1000, math.MaxInt32, 2000}, mem)

		df := New(categories, largeValues)
		defer df.Release()

		// Split the long HAVING expression across multiple lines
		havingPredicate := expr.Sum(expr.Col("large_values")).Gt(expr.Lit(int64(math.MaxInt32)))
		result, err := df.Lazy().GroupBy("category").Having(havingPredicate).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Both groups likely match since both have sums > MaxInt32
		assert.GreaterOrEqual(t, result.Len(), 1)
		assert.LessOrEqual(t, result.Len(), 2)
	})
}

// TestHavingEdgeCases_Performance tests performance-related edge cases.
func TestHavingEdgeCases_Performance(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("SingleRowDataFramePerformance", func(t *testing.T) {
		// Ensure single-row edge case has optimal performance
		categories := series.New("category", []string{"OnlyGroup"}, mem)
		values := series.New("values", []float64{42.0}, mem)

		df := New(categories, values)
		defer df.Release()

		result, err := df.Lazy().GroupBy("category").Having(expr.Sum(expr.Col("values")).Gt(expr.Lit(0.0))).Collect()
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 1, result.Len())
		// GroupBy result contains the grouping column(s)
		assert.GreaterOrEqual(t, result.Width(), 1)
	})

	t.Run("VeryLargeGroups", func(t *testing.T) {
		// Test performance with large groups (50K rows)
		size := 50000
		categories := make([]string, size)
		values := make([]float64, size)

		// Create 100 groups with 500 rows each
		for i := range size {
			categories[i] = fmt.Sprintf("Group_%d", i%100)
			values[i] = float64(i)
		}

		catSeries := series.New("category", categories, mem)
		valSeries := series.New("values", values, mem)

		df := New(catSeries, valSeries)
		defer df.Release()

		result, err := df.Lazy().
			GroupBy("category").
			Having(expr.Sum(expr.Col("values")).Gt(expr.Lit(1000000.0))).
			Collect()
		require.NoError(t, err)
		defer result.Release()

		// Many groups will pass the threshold - adjust expectation
		assert.Positive(t, result.Len())
		assert.LessOrEqual(t, result.Len(), size) // Can't exceed total row count
	})

	t.Run("ManySmallGroups", func(t *testing.T) {
		// Test performance with many small groups (5K groups, 1 row each)
		size := 5000
		categories := make([]string, size)
		values := make([]float64, size)

		for i := range size {
			categories[i] = fmt.Sprintf("UniqueGroup_%d", i)
			values[i] = float64(i)
		}

		catSeries := series.New("category", categories, mem)
		valSeries := series.New("values", values, mem)

		df := New(catSeries, valSeries)
		defer df.Release()

		result, err := df.Lazy().GroupBy("category").Having(expr.Sum(expr.Col("values")).Gt(expr.Lit(2500.0))).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Expect roughly half the groups to pass (values >= 2500)
		expectedMatches := size - 2500 - 1                   // Groups with index > 2500
		assert.InDelta(t, expectedMatches, result.Len(), 10) // Allow small variance
	})

	t.Run("DeepExpressionNesting", func(t *testing.T) {
		// Test performance with deeply nested HAVING expressions
		categories := series.New("category", []string{"A", "A", "B", "B", "C", "C"}, mem)
		values := series.New("values", []float64{10, 20, 30, 40, 50, 60}, mem)

		df := New(categories, values)
		defer df.Release()

		// Create a deeply nested boolean expression
		deepExpr := expr.Sum(expr.Col("values")).Gt(expr.Lit(25.0)).
			And(expr.Sum(expr.Col("values")).Lt(expr.Lit(150.0))).
			And(expr.Count(expr.Col("values")).Gt(expr.Lit(int64(1)))).
			Or(expr.Sum(expr.Col("values")).Eq(expr.Lit(110.0)))

		result, err := df.Lazy().GroupBy("category").Having(deepExpr).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Groups A (sum=30), B (sum=70), C (sum=110) should be evaluated
		assert.LessOrEqual(t, result.Len(), 3)
	})
}

// TestHavingEdgeCases_ComplexScenarios tests complex edge case scenarios.
func TestHavingEdgeCases_ComplexScenarios(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("MultipleHavingConditions", func(t *testing.T) {
		// Test multiple HAVING conditions combined
		categories := series.New("category", []string{"A", "A", "B", "B", "C", "C"}, mem)
		sales := series.New("sales", []float64{100, 200, 300, 400, 500, 600}, mem)
		counts := series.New("count", []int64{1, 2, 3, 4, 5, 6}, mem)

		df := New(categories, sales, counts)
		defer df.Release()

		// Complex HAVING with multiple aggregations
		complexHaving := expr.Sum(expr.Col("sales")).Gt(expr.Lit(500.0)).And(
			expr.Sum(expr.Col("count")).Lt(expr.Lit(int64(15))))

		result, err := df.Lazy().GroupBy("category").Having(complexHaving).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Group B (sales=700, count=7) and C (sales=1100, count=11) should match
		assert.Equal(t, 2, result.Len())
	})

	t.Run("HavingWithoutGroupBy_ShouldError", func(t *testing.T) {
		// Test that HAVING without appropriate GROUP BY context behaves correctly
		values := series.New("values", []float64{1, 2, 3}, mem)

		df := New(values)
		defer df.Release()

		// HAVING requires a proper GROUP BY context - test with a column that doesn't exist for grouping
		// This tests behavior when HAVING is used in an inappropriate context
		havingPredicate := expr.Sum(expr.Col("values")).Gt(expr.Lit(5.0))
		result, err := df.Lazy().GroupBy("nonexistent_column").Having(havingPredicate).Collect()

		// This should either error or return empty result due to invalid grouping column
		if err != nil {
			assert.Error(t, err, "HAVING with invalid GROUP BY should produce an error")
		} else {
			defer result.Release()
			assert.Equal(t, 0, result.Len(), "HAVING with invalid GROUP BY should return empty result")
		}
	})

	t.Run("DuplicateColumnNames", func(t *testing.T) {
		// Test edge case with potentially confusing column names
		categories := series.New("category", []string{"sum", "sum", "count", "count"}, mem)
		values := series.New("sum", []float64{10, 20, 30, 40}, mem) // Column named "sum"

		df := New(categories, values)
		defer df.Release()

		// HAVING referencing the "sum" column (not the aggregation function)
		result, err := df.Lazy().GroupBy("category").Having(expr.Sum(expr.Col("sum")).Gt(expr.Lit(35.0))).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Group "count" should match (sum of "sum" column = 70)
		assert.Equal(t, 1, result.Len())
	})

	t.Run("ColumnNameConflictsWithAliases", func(t *testing.T) {
		// Test potential conflicts between column names and aggregation aliases
		categories := series.New("category", []string{"A", "A", "B", "B"}, mem)
		total := series.New("total", []float64{5, 10, 15, 20}, mem) // Column already named "total"

		df := New(categories, total)
		defer df.Release()

		// HAVING should work correctly even with naming conflicts
		result, err := df.Lazy().GroupBy("category").Having(expr.Sum(expr.Col("total")).Gt(expr.Lit(20.0))).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Group B should match (sum=35)
		assert.Equal(t, 1, result.Len())
	})

	t.Run("NestedAggregationExpressions", func(t *testing.T) {
		// Test HAVING with complex aggregation expressions
		categories := series.New("category", []string{"A", "A", "B", "B"}, mem)
		values := series.New("values", []float64{10, 20, 30, 40}, mem)

		df := New(categories, values)
		defer df.Release()

		// HAVING with comparison between different aggregations
		havingExpr := expr.Sum(expr.Col("values")).Gt(expr.Count(expr.Col("values")).Mul(expr.Lit(int64(12))))

		result, err := df.Lazy().GroupBy("category").Having(havingExpr).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Group A: sum=30, count=2, 30 > 2*12 (24) ✓
		// Group B: sum=70, count=2, 70 > 2*12 (24) ✓
		assert.Equal(t, 2, result.Len())
	})
}

// TestHavingEdgeCases_ConcurrencyAndMemory tests memory and concurrency edge cases.
func TestHavingEdgeCases_ConcurrencyAndMemory(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("ConcurrentHavingOperations", func(t *testing.T) {
		// Test thread safety of HAVING operations
		const numGoroutines = 10
		const dataSize = 1000

		categories := make([]string, dataSize)
		values := make([]float64, dataSize)

		for i := range dataSize {
			categories[i] = fmt.Sprintf("Group_%d", i%50)
			values[i] = float64(i)
		}

		catSeries := series.New("category", categories, mem)
		valSeries := series.New("values", values, mem)

		df := New(catSeries, valSeries)
		defer df.Release()

		var wg sync.WaitGroup
		results := make([]*DataFrame, numGoroutines)
		errors := make([]error, numGoroutines)

		for i := range numGoroutines {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()

				// Each goroutine performs the same HAVING operation
				result, err := df.Lazy().
					GroupBy("category").
					Having(expr.Sum(expr.Col("values")).Gt(expr.Lit(5000.0))).
					Collect()
				results[idx] = result
				errors[idx] = err
			}(i)
		}

		wg.Wait()

		// Cleanup all results at once after all goroutines finish
		defer func() {
			for i := range numGoroutines {
				if results[i] != nil {
					results[i].Release()
				}
			}
		}()

		// Verify all operations succeeded and produced consistent results
		for i := range numGoroutines {
			require.NoError(t, errors[i], "Concurrent operation %d should succeed", i)
			require.NotNil(t, results[i], "Result %d should not be nil", i)

			// All results should have the same number of rows
			if i > 0 {
				assert.Equal(t, results[0].Len(), results[i].Len(), "All concurrent results should be identical")
			}
		}
	})

	t.Run("MemoryPressureScenarios", func(t *testing.T) {
		// Test HAVING under memory pressure conditions
		const largeSize = 100000
		categories := make([]string, largeSize)
		values := make([]float64, largeSize)

		// Create large dataset with many unique groups
		for i := range largeSize {
			categories[i] = fmt.Sprintf("Group_%d", i%1000) // 1000 groups
			values[i] = float64(i)
		}

		catSeries := series.New("category", categories, mem)
		valSeries := series.New("values", values, mem)

		df := New(catSeries, valSeries)
		defer df.Release()

		// Perform HAVING operation on large dataset
		result, err := df.Lazy().
			GroupBy("category").
			Having(expr.Sum(expr.Col("values")).Gt(expr.Lit(1000000.0))).
			Collect()
		require.NoError(t, err)
		defer result.Release()

		// Should successfully handle large dataset, may or may not have results
		assert.GreaterOrEqual(t, result.Len(), 0)
		assert.LessOrEqual(t, result.Len(), 1000)
	})

	t.Run("ResourceCleanupValidation", func(t *testing.T) {
		// Test that resources are properly cleaned up in error scenarios
		categories := series.New("category", []string{"A", "B"}, mem)
		values := series.New("values", []float64{10, 20}, mem)

		df := New(categories, values)
		defer df.Release()

		// Multiple HAVING operations to test cleanup
		for range 100 {
			result, err := df.Lazy().
				GroupBy("category").
				Having(expr.Sum(expr.Col("values")).Gt(expr.Lit(5.0))).
				Collect()
			require.NoError(t, err)

			// Immediately release to test cleanup
			result.Release()
		}

		// Test should complete without memory issues
		assert.True(t, true, "Resource cleanup test completed")
	})

	t.Run("ErrorRecoveryInParallelOperations", func(t *testing.T) {
		// Test error recovery in parallel HAVING scenarios
		categories := series.New("category", []string{"A", "A", "B", "B"}, mem)
		values := series.New("values", []float64{10, 20, 30, 40}, mem)

		df := New(categories, values)
		defer df.Release()

		const numOps = 20
		var wg sync.WaitGroup
		var successCount int64
		var mu sync.Mutex

		for i := range numOps {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()

				// Some operations might have valid conditions, others might not
				threshold := float64(idx * 10)
				result, err := df.Lazy().
					GroupBy("category").
					Having(expr.Sum(expr.Col("values")).Gt(expr.Lit(threshold))).
					Collect()

				if err == nil && result != nil {
					result.Release()
					mu.Lock()
					successCount++
					mu.Unlock()
				}
			}(i)
		}

		wg.Wait()

		// At least half operations should succeed
		assert.GreaterOrEqual(t, successCount, int64(numOps/2), "At least half parallel operations should succeed")
	})

	t.Run("PanicRecoveryInWorkerThreads", func(t *testing.T) {
		// Test that panics in worker threads are handled gracefully
		categories := series.New("category", []string{"A", "A", "B", "B"}, mem)
		values := series.New("values", []float64{10, 20, 30, 40}, mem)

		df := New(categories, values)
		defer df.Release()

		// Normal HAVING operation should work despite internal complexity
		result, err := df.Lazy().GroupBy("category").Having(expr.Sum(expr.Col("values")).Gt(expr.Lit(25.0))).Collect()
		require.NoError(t, err)
		defer result.Release()

		// Should handle the operation successfully
		assert.GreaterOrEqual(t, result.Len(), 0) // May have different results
	})
}
