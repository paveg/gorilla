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
			expectedValues: []interface{}{int64(2), int64(1), int64(2), int64(1), int64(1)}, // Within each department, ordered by salary desc
		},
		{
			name: "RANK with partition",
			windowExpr: Rank().Over(
				NewWindow().PartitionBy("department").OrderBy("salary", false),
			),
			expectedValues: []interface{}{int64(2), int64(1), int64(2), int64(1), int64(1)}, // Same as ROW_NUMBER for this data
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
						switch arr := result.(type) {
						case *array.Int64:
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

// TODO: Implement window frames in future phase
func TestEvaluateWindowFrame_TODO(t *testing.T) {
	t.Skip("Window frames not implemented yet - future phase")
	// Window frames will be implemented in future phase
	// Tests would go here
}
