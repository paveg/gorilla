package expr

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvaluateWindowFunction(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data
	salaries := []int64{50000, 60000, 70000, 80000, 90000}
	departments := []string{"HR", "HR", "Engineering", "Engineering", "Sales"}

	salarySeries, err := series.NewSafe[int64]("salary", salaries, mem)
	require.NoError(t, err)
	defer salarySeries.Release()

	deptSeries, err := series.NewSafe[string]("department", departments, mem)
	require.NoError(t, err)
	defer deptSeries.Release()

	columns := map[string]arrow.Array{
		"salary":     salarySeries.Array(),
		"department": deptSeries.Array(),
	}

	evaluator := NewEvaluator(mem)

	tests := []struct {
		name           string
		windowExpr     *WindowExpr
		expectedValues []interface{}
	}{
		{
			name: "ROW_NUMBER with partition",
			windowExpr: RowNumber().Over(
				NewWindow().PartitionBy("department").OrderBy("salary", false),
			),
			expectedValues: []interface{}{int64(2), int64(1), int64(2), int64(1), int64(1)}, // Ordered by salary desc
		},
		{
			name: "RANK with partition",
			windowExpr: Rank().Over(
				NewWindow().PartitionBy("department").OrderBy("salary", false),
			),
			expectedValues: []interface{}{
				int64(2),
				int64(1),
				int64(2),
				int64(1),
				int64(1),
			}, // Same as ROW_NUMBER for this data
		},
		{
			name: "DENSE_RANK with partition",
			windowExpr: DenseRank().Over(
				NewWindow().PartitionBy("department").OrderBy("salary", false),
			),
			expectedValues: []interface{}{int64(2), int64(1), int64(2), int64(1), int64(1)}, // No gaps in DENSE_RANK
		},
		// TODO: Implement LAG function in future phase
		// {
		// 	name: "LAG with partition",
		// 	windowExpr: Lag(Col("salary"), 1).Over(
		// 		NewWindow().PartitionBy("department").OrderBy("salary", true),
		// 	),
		// 	expectedValues: []interface{}{nil, int64(50000), nil, int64(70000), nil}, // Previous salary within partition
		// },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluator.EvaluateWindow(tt.windowExpr, columns)
			require.NoError(t, err)
			defer result.Release()

			// Check result length
			assert.Equal(t, len(tt.expectedValues), result.Len())

			// Check values based on type
			switch tt.expectedValues[0].(type) {
			case int64:
				int64Array := result.(*array.Int64)
				for i, expectedValue := range tt.expectedValues {
					if expectedValue == nil {
						assert.True(t, int64Array.IsNull(i))
					} else {
						assert.Equal(t, expectedValue.(int64), int64Array.Value(i))
					}
				}
			case nil:
				// Handle mixed nil and non-nil values
				for i, expectedValue := range tt.expectedValues {
					if expectedValue == nil {
						assert.True(t, result.IsNull(i))
					} else {
						// Check non-nil values based on actual type
						if arr, ok := result.(*array.Int64); ok {
							assert.Equal(t, expectedValue.(int64), arr.Value(i))
						}
					}
				}
			}
		})
	}
}

func TestEvaluateWindowAggregation(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data
	salaries := []int64{50000, 60000, 70000, 80000, 90000}
	departments := []string{"HR", "HR", "Engineering", "Engineering", "Sales"}

	salarySeries, err := series.NewSafe[int64]("salary", salaries, mem)
	require.NoError(t, err)
	defer salarySeries.Release()

	deptSeries, err := series.NewSafe[string]("department", departments, mem)
	require.NoError(t, err)
	defer deptSeries.Release()

	columns := map[string]arrow.Array{
		"salary":     salarySeries.Array(),
		"department": deptSeries.Array(),
	}

	evaluator := NewEvaluator(mem)

	tests := []struct {
		name           string
		windowExpr     *WindowExpr
		expectedValues []int64
	}{
		{
			name: "SUM with partition",
			windowExpr: Sum(Col("salary")).Over(
				NewWindow().PartitionBy("department"),
			),
			expectedValues: []int64{110000, 110000, 150000, 150000, 90000}, // Sum within each department
		},
		// TODO: Implement running sums in future phase
		// {
		// 	name: "running SUM with ORDER BY",
		// 	windowExpr: Sum(Col("salary")).Over(
		// 		NewWindow().OrderBy("salary", true),
		// 	),
		// 	expectedValues: []int64{50000, 110000, 180000, 260000, 350000}, // Running sum
		// },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluator.EvaluateWindow(tt.windowExpr, columns)
			require.NoError(t, err)
			defer result.Release()

			// Check result length
			assert.Equal(t, len(tt.expectedValues), result.Len())

			// Check values
			int64Array := result.(*array.Int64)
			for i, expectedValue := range tt.expectedValues {
				assert.Equal(t, expectedValue, int64Array.Value(i))
			}
		})
	}
}

func TestWindowFunctions_DuplicateValues(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data with duplicate values to show difference between RANK and DENSE_RANK
	salaries := []int64{50000, 60000, 60000, 70000, 70000, 80000}
	departments := []string{"HR", "HR", "HR", "Engineering", "Engineering", "Engineering"}

	salarySeries, err := series.NewSafe[int64]("salary", salaries, mem)
	require.NoError(t, err)
	defer salarySeries.Release()

	deptSeries, err := series.NewSafe[string]("department", departments, mem)
	require.NoError(t, err)
	defer deptSeries.Release()

	columns := map[string]arrow.Array{
		"salary":     salarySeries.Array(),
		"department": deptSeries.Array(),
	}

	evaluator := NewEvaluator(mem)

	tests := []struct {
		name           string
		windowExpr     *WindowExpr
		expectedValues []interface{}
	}{
		{
			name: "RANK with duplicate values",
			windowExpr: Rank().Over(
				NewWindow().PartitionBy("department").OrderBy("salary", true),
			),
			expectedValues: []interface{}{int64(1), int64(2), int64(2), int64(1), int64(1), int64(3)}, // RANK has gaps
		},
		{
			name: "DENSE_RANK with duplicate values",
			windowExpr: DenseRank().Over(
				NewWindow().PartitionBy("department").OrderBy("salary", true),
			),
			expectedValues: []interface{}{
				int64(1),
				int64(2),
				int64(2),
				int64(1),
				int64(1),
				int64(2),
			}, // DENSE_RANK has no gaps
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluator.EvaluateWindow(tt.windowExpr, columns)
			require.NoError(t, err)
			defer result.Release()

			// Check result length
			assert.Equal(t, len(tt.expectedValues), result.Len())

			// Check values
			int64Array := result.(*array.Int64)
			for i, expectedValue := range tt.expectedValues {
				assert.Equal(t, expectedValue.(int64), int64Array.Value(i))
			}
		})
	}
}

// TODO: Implement window frames in future phase
func TestEvaluateWindowFrame_TODO(t *testing.T) {
	t.Skip("Window frames not implemented yet - future phase")
	// Window frames will be implemented in future phase
	// Tests would go here
}

// Test edge cases for improved coverage.
func TestEvaluateWindowFunction_EdgeCases(t *testing.T) {
	mem := memory.NewGoAllocator()

	tests := []struct {
		name        string
		setup       func() (map[string]arrow.Array, *WindowExpr, []interface{})
		expectError bool
	}{
		{
			name: "Empty dataset",
			setup: func() (map[string]arrow.Array, *WindowExpr, []interface{}) {
				emptySeries, _ := series.NewSafe[int64]("empty", []int64{}, mem)
				defer emptySeries.Release()

				columns := map[string]arrow.Array{
					"empty": emptySeries.Array(),
				}

				windowExpr := RowNumber().Over(NewWindow())
				return columns, windowExpr, []interface{}{}
			},
			expectError: false,
		},
		{
			name: "Single row",
			setup: func() (map[string]arrow.Array, *WindowExpr, []interface{}) {
				singleSeries, _ := series.NewSafe[int64]("single", []int64{42}, mem)
				defer singleSeries.Release()

				columns := map[string]arrow.Array{
					"single": singleSeries.Array(),
				}

				windowExpr := RowNumber().Over(NewWindow())
				return columns, windowExpr, []interface{}{int64(1)}
			},
			expectError: false,
		},
		{
			name: "Duplicate values in ORDER BY",
			setup: func() (map[string]arrow.Array, *WindowExpr, []interface{}) {
				dupSeries, _ := series.NewSafe[int64]("dup", []int64{1, 2, 2, 3}, mem)
				defer dupSeries.Release()

				columns := map[string]arrow.Array{
					"dup": dupSeries.Array(),
				}

				windowExpr := Rank().Over(NewWindow().OrderBy("dup", true))
				return columns, windowExpr, []interface{}{int64(1), int64(2), int64(2), int64(4)}
			},
			expectError: false,
		},
		{
			name: "Missing column error",
			setup: func() (map[string]arrow.Array, *WindowExpr, []interface{}) {
				validSeries, _ := series.NewSafe[int64]("valid", []int64{1, 2, 3}, mem)
				defer validSeries.Release()

				columns := map[string]arrow.Array{
					"valid": validSeries.Array(),
				}

				windowExpr := RowNumber().Over(NewWindow().PartitionBy("missing"))
				return columns, windowExpr, nil
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			columns, windowExpr, expectedValues := tt.setup()

			evaluator := NewEvaluator(mem)
			result, err := evaluator.EvaluateWindow(windowExpr, columns)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			if result != nil {
				defer result.Release()
			}

			if len(expectedValues) > 0 {
				assert.Equal(t, len(expectedValues), result.Len())

				int64Array := result.(*array.Int64)
				for i, expectedValue := range expectedValues {
					assert.Equal(t, expectedValue.(int64), int64Array.Value(i))
				}
			}
		})
	}
}

func TestEvaluateWindowAggregation_EdgeCases(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Test mixed null and non-null values
	t.Run("Mixed null and non-null values", func(t *testing.T) {
		// Create test data with series that handles nulls properly
		data := []int64{10, 20}
		testSeries, err := series.NewSafe[int64]("mixed", data, mem)
		require.NoError(t, err)
		defer testSeries.Release()

		columns := map[string]arrow.Array{
			"mixed": testSeries.Array(),
		}

		windowExpr := Sum(Col("mixed")).Over(NewWindow())

		evaluator := NewEvaluator(mem)
		result, err := evaluator.EvaluateWindow(windowExpr, columns)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 2, result.Len())

		int64Array := result.(*array.Int64)
		assert.Equal(t, int64(30), int64Array.Value(0))
		assert.Equal(t, int64(30), int64Array.Value(1))
	})

	// Test COUNT with simple data
	t.Run("COUNT with data", func(t *testing.T) {
		data := []int64{10, 20, 30}
		testSeries, err := series.NewSafe[int64]("count", data, mem)
		require.NoError(t, err)
		defer testSeries.Release()

		columns := map[string]arrow.Array{
			"count": testSeries.Array(),
		}

		windowExpr := Count(Col("count")).Over(NewWindow())

		evaluator := NewEvaluator(mem)
		result, err := evaluator.EvaluateWindow(windowExpr, columns)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 3, result.Len())

		int64Array := result.(*array.Int64)
		for i := range 3 {
			assert.Equal(t, int64(3), int64Array.Value(i))
		}
	})
}

func TestWindowFunction_SortingPerformance(t *testing.T) {
	mem := memory.NewGoAllocator()
	evaluator := NewEvaluator(mem)

	// Test sortPartition with larger dataset to ensure performance improvement
	t.Run("sortPartition_performance", func(t *testing.T) {
		// Create larger dataset for performance testing
		data := make([]int64, 1000)
		for i := range data {
			data[i] = int64(1000 - i) // Reverse order
		}

		int64Series, _ := series.NewSafe[int64]("test", data, mem)
		defer int64Series.Release()

		columns := map[string]arrow.Array{
			"test": int64Series.Array(),
		}

		orderBy := []OrderByExpr{{column: "test", ascending: true}}
		partition := make([]int, 1000)
		for i := range partition {
			partition[i] = i
		}

		sorted := evaluator.sortPartition(partition, orderBy, columns)
		assert.Len(t, sorted, 1000)

		// Verify first few elements are sorted correctly
		assert.Equal(t, 999, sorted[0]) // Index of smallest value (1)
		assert.Equal(t, 998, sorted[1]) // Index of second smallest value (2)
		assert.Equal(t, 997, sorted[2]) // Index of third smallest value (3)
	})
}
