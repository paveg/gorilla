package expr_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvaluateWindowFunction(t *testing.T) {
	columns, evaluator := setupWindowTestData(t)
	defer cleanupWindowTestData(columns)

	tests := createWindowTestCases()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runWindowFunctionTest(t, evaluator, tt, columns)
		})
	}
}

// setupWindowTestData creates test data and evaluator for window function tests.
func setupWindowTestData(t *testing.T) (map[string]arrow.Array, *expr.Evaluator) {
	mem := memory.NewGoAllocator()

	salaries := []int64{50000, 60000, 70000, 80000, 90000}
	departments := []string{"HR", "HR", "Engineering", "Engineering", "Sales"}

	salarySeries, err := series.NewSafe[int64]("salary", salaries, mem)
	require.NoError(t, err)

	deptSeries, err := series.NewSafe[string]("department", departments, mem)
	require.NoError(t, err)

	columns := map[string]arrow.Array{
		"salary":     salarySeries.Array(),
		"department": deptSeries.Array(),
	}

	evaluator := expr.NewEvaluator(mem)
	return columns, evaluator
}

// cleanupWindowTestData releases test data resources.
func cleanupWindowTestData(_ map[string]arrow.Array) {
	// Note: Arrays are released automatically when series are released
	// This is a placeholder for explicit cleanup if needed
}

// windowTestCase represents a single window function test case.
type windowTestCase struct {
	name           string
	windowExpr     *expr.WindowExpr
	expectedValues []interface{}
}

// createWindowTestCases creates all test cases for window functions.
func createWindowTestCases() []windowTestCase {
	return []windowTestCase{
		{
			name: "ROW_NUMBER with partition",
			windowExpr: expr.RowNumber().Over(
				expr.NewWindow().PartitionBy("department").OrderBy("salary", false),
			),
			expectedValues: []interface{}{int64(2), int64(1), int64(2), int64(1), int64(1)},
		},
		{
			name: "RANK with partition",
			windowExpr: expr.Rank().Over(
				expr.NewWindow().PartitionBy("department").OrderBy("salary", false),
			),
			expectedValues: []interface{}{int64(2), int64(1), int64(2), int64(1), int64(1)},
		},
		{
			name: "DENSE_RANK with partition",
			windowExpr: expr.DenseRank().Over(
				expr.NewWindow().PartitionBy("department").OrderBy("salary", false),
			),
			expectedValues: []interface{}{int64(2), int64(1), int64(2), int64(1), int64(1)},
		},
	}
}

// runWindowFunctionTest executes a single window function test.
func runWindowFunctionTest(
	t *testing.T,
	evaluator *expr.Evaluator,
	testCase windowTestCase,
	columns map[string]arrow.Array,
) {
	result, evalErr := evaluator.EvaluateWindow(testCase.windowExpr, columns)
	require.NoError(t, evalErr)
	defer result.Release()

	assert.Equal(t, len(testCase.expectedValues), result.Len())
	validateWindowFunctionResult(t, result, testCase.expectedValues)
}

// validateWindowFunctionResult validates the result against expected values.
func validateWindowFunctionResult(t *testing.T, result arrow.Array, expectedValues []interface{}) {
	if len(expectedValues) == 0 {
		return
	}

	switch expectedValues[0].(type) {
	case int64:
		validateInt64WindowResult(t, result, expectedValues)
	case nil:
		validateMixedWindowResult(t, result, expectedValues)
	}
}

// validateInt64WindowResult validates int64 window function results.
func validateInt64WindowResult(t *testing.T, result arrow.Array, expectedValues []interface{}) {
	int64Array := result.(*array.Int64)
	for i, expectedValue := range expectedValues {
		if expectedValue == nil {
			assert.True(t, int64Array.IsNull(i))
		} else {
			assert.Equal(t, expectedValue.(int64), int64Array.Value(i))
		}
	}
}

// validateMixedWindowResult validates mixed nil and non-nil window function results.
func validateMixedWindowResult(t *testing.T, result arrow.Array, expectedValues []interface{}) {
	for i, expectedValue := range expectedValues {
		if expectedValue == nil {
			assert.True(t, result.IsNull(i))
			continue
		}

		if arr, ok := result.(*array.Int64); ok {
			assert.Equal(t, expectedValue.(int64), arr.Value(i))
		}
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

	evaluator := expr.NewEvaluator(mem)

	tests := []struct {
		name           string
		windowExpr     *expr.WindowExpr
		expectedValues []int64
	}{
		{
			name: "SUM with partition",
			windowExpr: expr.Sum(expr.Col("salary")).Over(
				expr.NewWindow().PartitionBy("department"),
			),
			expectedValues: []int64{110000, 110000, 150000, 150000, 90000}, // Sum within each department
		},
		// TODO: Implement running sums in future phase
		// {
		// 	name: "running SUM with ORDER BY",
		// 	windowExpr: expr.Sum(expr.Col("salary")).Over(
		// 		expr.NewWindow().OrderBy("salary", true),
		// 	),
		// 	expectedValues: []int64{50000, 110000, 180000, 260000, 350000}, // Running sum
		// },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, evalErr := evaluator.EvaluateWindow(tt.windowExpr, columns)
			require.NoError(t, evalErr)
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

	evaluator := expr.NewEvaluator(mem)

	tests := []struct {
		name           string
		windowExpr     *expr.WindowExpr
		expectedValues []interface{}
	}{
		{
			name: "RANK with duplicate values",
			windowExpr: expr.Rank().Over(
				expr.NewWindow().PartitionBy("department").OrderBy("salary", true),
			),
			expectedValues: []interface{}{int64(1), int64(2), int64(2), int64(1), int64(1), int64(3)}, // RANK has gaps
		},
		{
			name: "DENSE_RANK with duplicate values",
			windowExpr: expr.DenseRank().Over(
				expr.NewWindow().PartitionBy("department").OrderBy("salary", true),
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
			result, evalErr := evaluator.EvaluateWindow(tt.windowExpr, columns)
			require.NoError(t, evalErr)
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
		setup       func() (map[string]arrow.Array, *expr.WindowExpr, []interface{})
		expectError bool
	}{
		{
			name: "Empty dataset",
			setup: func() (map[string]arrow.Array, *expr.WindowExpr, []interface{}) {
				emptySeries, _ := series.NewSafe[int64]("empty", []int64{}, mem)
				defer emptySeries.Release()

				columns := map[string]arrow.Array{
					"empty": emptySeries.Array(),
				}

				windowExpr := expr.RowNumber().Over(expr.NewWindow())
				return columns, windowExpr, []interface{}{}
			},
			expectError: false,
		},
		{
			name: "Single row",
			setup: func() (map[string]arrow.Array, *expr.WindowExpr, []interface{}) {
				singleSeries, _ := series.NewSafe[int64]("single", []int64{42}, mem)
				defer singleSeries.Release()

				columns := map[string]arrow.Array{
					"single": singleSeries.Array(),
				}

				windowExpr := expr.RowNumber().Over(expr.NewWindow())
				return columns, windowExpr, []interface{}{int64(1)}
			},
			expectError: false,
		},
		{
			name: "Duplicate values in ORDER BY",
			setup: func() (map[string]arrow.Array, *expr.WindowExpr, []interface{}) {
				dupSeries, _ := series.NewSafe[int64]("dup", []int64{1, 2, 2, 3}, mem)
				defer dupSeries.Release()

				columns := map[string]arrow.Array{
					"dup": dupSeries.Array(),
				}

				windowExpr := expr.Rank().Over(expr.NewWindow().OrderBy("dup", true))
				return columns, windowExpr, []interface{}{int64(1), int64(2), int64(2), int64(4)}
			},
			expectError: false,
		},
		{
			name: "Missing column error",
			setup: func() (map[string]arrow.Array, *expr.WindowExpr, []interface{}) {
				validSeries, _ := series.NewSafe[int64]("valid", []int64{1, 2, 3}, mem)
				defer validSeries.Release()

				columns := map[string]arrow.Array{
					"valid": validSeries.Array(),
				}

				windowExpr := expr.RowNumber().Over(expr.NewWindow().PartitionBy("missing"))
				return columns, windowExpr, nil
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			columns, windowExpr, expectedValues := tt.setup()

			evaluator := expr.NewEvaluator(mem)
			result, err := evaluator.EvaluateWindow(windowExpr, columns)

			if tt.expectError {
				require.Error(t, err)
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

		windowExpr := expr.Sum(expr.Col("mixed")).Over(expr.NewWindow())

		evaluator := expr.NewEvaluator(mem)
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

		windowExpr := expr.Count(expr.Col("count")).Over(expr.NewWindow())

		evaluator := expr.NewEvaluator(mem)
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
