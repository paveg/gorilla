package expr

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWindowParallelConfig(t *testing.T) {
	config := DefaultWindowParallelConfig()

	assert.Equal(t, 4, config.MinPartitionsForParallel)
	assert.Equal(t, 1000, config.MinRowsForParallelSort)
	assert.True(t, config.AdaptiveParallelization)
	assert.Greater(t, config.MaxWorkers, 0)
}

func TestShouldUseParallelExecution(t *testing.T) {
	mem := memory.NewGoAllocator()

	evaluator := NewEvaluator(mem)
	config := DefaultWindowParallelConfig()

	tests := []struct {
		name        string
		partitions  [][]int
		expected    bool
		description string
	}{
		{
			name:        "small partition count",
			partitions:  [][]int{{0, 1}, {2, 3}},
			expected:    false,
			description: "2 partitions < MinPartitionsForParallel (4)",
		},
		{
			name:        "adequate partition count",
			partitions:  [][]int{{0, 1}, {2, 3}, {4, 5}, {6, 7}},
			expected:    true,
			description: "4 partitions = MinPartitionsForParallel",
		},
		{
			name:        "large partition count",
			partitions:  [][]int{{0}, {1}, {2}, {3}, {4}, {5}},
			expected:    true,
			description: "6 partitions > MinPartitionsForParallel",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := evaluator.shouldUseParallelExecution(tt.partitions, config)
			assert.Equal(t, tt.expected, result, tt.description)
		})
	}
}

func TestParallelRankFunction(t *testing.T) {
	mem := memory.NewGoAllocator()

	evaluator := NewEvaluator(mem)

	// Create test data with multiple partitions to trigger parallel execution
	departments := []string{
		"Eng", "Sales", "Marketing", "HR", // 4 different departments = 4 partitions
		"Eng", "Sales", "Marketing", "HR",
		"Eng", "Sales", "Marketing", "HR",
		"Eng", "Sales", "Marketing", "HR",
		"Eng", "Sales", "Marketing", "HR", // 20 total rows, 5 per partition
	}

	salaries := []int64{
		100, 80, 90, 70,
		120, 75, 95, 75,
		110, 85, 85, 80,
		130, 90, 100, 85,
		105, 70, 80, 65,
	}

	// Build Arrow arrays
	deptBuilder := array.NewStringBuilder(mem)
	defer deptBuilder.Release()
	for _, dept := range departments {
		deptBuilder.Append(dept)
	}
	deptArray := deptBuilder.NewArray()
	defer deptArray.Release()

	salaryBuilder := array.NewInt64Builder(mem)
	defer salaryBuilder.Release()
	for _, salary := range salaries {
		salaryBuilder.Append(salary)
	}
	salaryArray := salaryBuilder.NewArray()
	defer salaryArray.Release()

	columns := map[string]arrow.Array{
		"department": deptArray,
		"salary":     salaryArray,
	}

	// Create window spec with PARTITION BY and ORDER BY
	window := &WindowSpec{
		partitionBy: []string{"department"},
		orderBy: []OrderByExpr{
			{column: "salary", ascending: false}, // Order by salary descending
		},
	}

	t.Run("parallel RANK vs sequential RANK", func(t *testing.T) {
		// Test parallel execution
		parallelResult, err := evaluator.evaluateRankParallel(window, columns, len(departments))
		require.NoError(t, err)
		defer parallelResult.Release()

		// Test sequential execution for comparison
		sequentialResult, err := evaluator.evaluateRank(window, columns, len(departments))
		require.NoError(t, err)
		defer sequentialResult.Release()

		// Results should be identical
		assert.Equal(t, sequentialResult.Len(), parallelResult.Len())

		seqArray := sequentialResult.(*array.Int64)
		parArray := parallelResult.(*array.Int64)

		for i := 0; i < seqArray.Len(); i++ {
			assert.Equal(t, seqArray.Value(i), parArray.Value(i),
				"Rank mismatch at index %d (dept: %s, salary: %d)",
				i, departments[i], salaries[i])
		}
	})

	t.Run("parallel DENSE_RANK vs sequential DENSE_RANK", func(t *testing.T) {
		// Test parallel execution
		parallelResult, err := evaluator.evaluateDenseRankParallel(window, columns, len(departments))
		require.NoError(t, err)
		defer parallelResult.Release()

		// Test sequential execution for comparison
		sequentialResult, err := evaluator.evaluateDenseRank(window, columns, len(departments))
		require.NoError(t, err)
		defer sequentialResult.Release()

		// Results should be identical
		assert.Equal(t, sequentialResult.Len(), parallelResult.Len())

		seqArray := sequentialResult.(*array.Int64)
		parArray := parallelResult.(*array.Int64)

		for i := 0; i < seqArray.Len(); i++ {
			assert.Equal(t, seqArray.Value(i), parArray.Value(i),
				"Dense rank mismatch at index %d (dept: %s, salary: %d)",
				i, departments[i], salaries[i])
		}
	})
}

func TestParallelExecutionFallback(t *testing.T) {
	mem := memory.NewGoAllocator()

	evaluator := NewEvaluator(mem)

	// Create small dataset that should fall back to sequential execution
	departments := []string{"Eng", "Sales"} // Only 2 partitions
	salaries := []int64{100, 80}

	// Build Arrow arrays
	deptBuilder := array.NewStringBuilder(mem)
	defer deptBuilder.Release()
	for _, dept := range departments {
		deptBuilder.Append(dept)
	}
	deptArray := deptBuilder.NewArray()
	defer deptArray.Release()

	salaryBuilder := array.NewInt64Builder(mem)
	defer salaryBuilder.Release()
	for _, salary := range salaries {
		salaryBuilder.Append(salary)
	}
	salaryArray := salaryBuilder.NewArray()
	defer salaryArray.Release()

	columns := map[string]arrow.Array{
		"department": deptArray,
		"salary":     salaryArray,
	}

	window := &WindowSpec{
		partitionBy: []string{"department"},
		orderBy: []OrderByExpr{
			{column: "salary", ascending: false},
		},
	}

	t.Run("should_use_parallel_execution returns false for small dataset", func(t *testing.T) {
		shouldUseParallel := evaluator.shouldUseWindowParallelExecution(window, columns, len(departments))
		assert.False(t, shouldUseParallel, "Small dataset should not use parallel execution")
	})

	t.Run("parallel functions work with small datasets", func(t *testing.T) {
		// Even though it falls back to sequential, the parallel functions should still work
		result, err := evaluator.evaluateRankParallel(window, columns, len(departments))
		require.NoError(t, err)
		defer result.Release()

		rankArray := result.(*array.Int64)
		assert.Equal(t, 2, rankArray.Len())

		// Each department should have rank 1 (only one row per partition)
		assert.Equal(t, int64(1), rankArray.Value(0))
		assert.Equal(t, int64(1), rankArray.Value(1))
	})
}

func TestParallelProcessingCorrectness(t *testing.T) {
	mem := memory.NewGoAllocator()

	evaluator := NewEvaluator(mem)

	// Create a larger, more complex dataset to thoroughly test parallel processing
	const numDepartments = 5
	const rowsPerDepartment = 20
	const totalRows = numDepartments * rowsPerDepartment

	departments := make([]string, totalRows)
	salaries := make([]int64, totalRows)

	deptNames := []string{"Engineering", "Sales", "Marketing", "HR", "Finance"}

	// Create data with known rankings
	for deptIdx := 0; deptIdx < numDepartments; deptIdx++ {
		for rowIdx := 0; rowIdx < rowsPerDepartment; rowIdx++ {
			globalIdx := deptIdx*rowsPerDepartment + rowIdx
			departments[globalIdx] = deptNames[deptIdx]
			// Create salaries in descending order within each department
			salaries[globalIdx] = int64(1000 - rowIdx*10 + deptIdx*5) // Add dept variation
		}
	}

	// Build Arrow arrays
	deptBuilder := array.NewStringBuilder(mem)
	defer deptBuilder.Release()
	for _, dept := range departments {
		deptBuilder.Append(dept)
	}
	deptArray := deptBuilder.NewArray()
	defer deptArray.Release()

	salaryBuilder := array.NewInt64Builder(mem)
	defer salaryBuilder.Release()
	for _, salary := range salaries {
		salaryBuilder.Append(salary)
	}
	salaryArray := salaryBuilder.NewArray()
	defer salaryArray.Release()

	columns := map[string]arrow.Array{
		"department": deptArray,
		"salary":     salaryArray,
	}

	window := &WindowSpec{
		partitionBy: []string{"department"},
		orderBy: []OrderByExpr{
			{column: "salary", ascending: false},
		},
	}

	t.Run("parallel execution produces correct ranks", func(t *testing.T) {
		result, err := evaluator.evaluateRankParallel(window, columns, totalRows)
		require.NoError(t, err)
		defer result.Release()

		rankArray := result.(*array.Int64)
		assert.Equal(t, totalRows, rankArray.Len())

		// Verify that within each department, ranks are correct
		for deptIdx := 0; deptIdx < numDepartments; deptIdx++ {
			for rowIdx := 0; rowIdx < rowsPerDepartment; rowIdx++ {
				globalIdx := deptIdx*rowsPerDepartment + rowIdx
				expectedRank := int64(rowIdx + 1) // Since salaries are in descending order
				actualRank := rankArray.Value(globalIdx)

				assert.Equal(t, expectedRank, actualRank,
					"Incorrect rank at dept %s, global index %d (expected %d, got %d)",
					deptNames[deptIdx], globalIdx, expectedRank, actualRank)
			}
		}
	})

	t.Run("parallel vs sequential consistency across multiple runs", func(t *testing.T) {
		// Run multiple times to check for race conditions
		for run := 0; run < 5; run++ {
			t.Run(fmt.Sprintf("run_%d", run), func(t *testing.T) {
				parallelResult, err := evaluator.evaluateRankParallel(window, columns, totalRows)
				require.NoError(t, err)
				defer parallelResult.Release()

				sequentialResult, err := evaluator.evaluateRank(window, columns, totalRows)
				require.NoError(t, err)
				defer sequentialResult.Release()

				// Results should be identical every time
				seqArray := sequentialResult.(*array.Int64)
				parArray := parallelResult.(*array.Int64)

				for i := 0; i < seqArray.Len(); i++ {
					assert.Equal(t, seqArray.Value(i), parArray.Value(i),
						"Run %d: Rank mismatch at index %d", run, i)
				}
			})
		}
	})
}

func TestParallelSortingThreshold(t *testing.T) {
	mem := memory.NewGoAllocator()

	evaluator := NewEvaluator(mem)

	// Test the parallel sorting threshold logic
	smallPartition := make([]int, 500)
	largePartition := make([]int, 2000)

	for i := range smallPartition {
		smallPartition[i] = i
	}
	for i := range largePartition {
		largePartition[i] = i
	}

	// Create simple test columns for sorting
	salaryBuilder := array.NewInt64Builder(mem)
	defer salaryBuilder.Release()
	for i := 0; i < 2000; i++ {
		salaryBuilder.Append(int64(i))
	}
	salaryArray := salaryBuilder.NewArray()
	defer salaryArray.Release()

	columns := map[string]arrow.Array{
		"salary": salaryArray,
	}

	orderBy := []OrderByExpr{
		{column: "salary", ascending: true},
	}

	t.Run("small partition uses sequential sort", func(t *testing.T) {
		// This should use sequential sort (we can't directly test this, but we can verify it works)
		result := evaluator.sortPartitionParallel(smallPartition, orderBy, columns)
		assert.Equal(t, len(smallPartition), len(result))

		// Verify it's sorted
		for i := 1; i < len(result); i++ {
			assert.LessOrEqual(t, result[i-1], result[i], "Small partition should be sorted")
		}
	})

	t.Run("large partition uses parallel sort", func(t *testing.T) {
		// This should trigger parallel sort consideration (though it may fall back)
		result := evaluator.sortPartitionParallel(largePartition, orderBy, columns)
		assert.Equal(t, len(largePartition), len(result))

		// Verify it's sorted
		for i := 1; i < len(result); i++ {
			assert.LessOrEqual(t, result[i-1], result[i], "Large partition should be sorted")
		}
	})
}

func BenchmarkParallelVsSequentialRank(b *testing.B) {
	mem := memory.NewGoAllocator()

	evaluator := NewEvaluator(mem)

	// Create large dataset for meaningful benchmark
	const numDepartments = 10
	const rowsPerDepartment = 1000
	const totalRows = numDepartments * rowsPerDepartment

	departments := make([]string, totalRows)
	salaries := make([]int64, totalRows)

	for i := 0; i < totalRows; i++ {
		departments[i] = fmt.Sprintf("Dept_%d", i%numDepartments)
		salaries[i] = int64(i * 3 % 10000) // Semi-random salaries
	}

	// Build Arrow arrays
	deptBuilder := array.NewStringBuilder(mem)
	defer deptBuilder.Release()
	for _, dept := range departments {
		deptBuilder.Append(dept)
	}
	deptArray := deptBuilder.NewArray()
	defer deptArray.Release()

	salaryBuilder := array.NewInt64Builder(mem)
	defer salaryBuilder.Release()
	for _, salary := range salaries {
		salaryBuilder.Append(salary)
	}
	salaryArray := salaryBuilder.NewArray()
	defer salaryArray.Release()

	columns := map[string]arrow.Array{
		"department": deptArray,
		"salary":     salaryArray,
	}

	window := &WindowSpec{
		partitionBy: []string{"department"},
		orderBy: []OrderByExpr{
			{column: "salary", ascending: false},
		},
	}

	b.Run("Sequential", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := evaluator.evaluateRank(window, columns, totalRows)
			if err != nil {
				b.Fatal(err)
			}
			result.Release()
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := evaluator.evaluateRankParallel(window, columns, totalRows)
			if err != nil {
				b.Fatal(err)
			}
			result.Release()
		}
	})
}
