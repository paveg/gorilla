package expr_test

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDateTimeFunctions(t *testing.T) {
	mem := memory.NewGoAllocator()
	evaluator := expr.NewEvaluator(mem)

	// Create test timestamps
	testTimes := []time.Time{
		time.Date(2023, 1, 15, 14, 30, 45, 0, time.UTC),
		time.Date(2023, 12, 31, 23, 59, 59, 0, time.UTC),
		time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC),
	}

	// Create timestamp array
	timestampType := &arrow.TimestampType{Unit: arrow.Nanosecond}
	timestampBuilder := array.NewTimestampBuilder(mem, timestampType)
	defer timestampBuilder.Release()

	for _, t := range testTimes {
		timestampBuilder.Append(arrow.Timestamp(t.UnixNano()))
	}
	timestampArray := timestampBuilder.NewArray()
	defer timestampArray.Release()

	columns := map[string]arrow.Array{
		"timestamp_col": timestampArray,
	}

	tests := []struct {
		name     string
		expr     expr.Expr
		expected []int64
	}{
		{
			name:     "Year extraction",
			expr:     expr.Col("timestamp_col").Year(),
			expected: []int64{2023, 2023, 2024},
		},
		{
			name:     "Month extraction",
			expr:     expr.Col("timestamp_col").Month(),
			expected: []int64{1, 12, 6},
		},
		{
			name:     "Day extraction",
			expr:     expr.Col("timestamp_col").Day(),
			expected: []int64{15, 31, 15},
		},
		{
			name:     "Hour extraction",
			expr:     expr.Col("timestamp_col").Hour(),
			expected: []int64{14, 23, 12},
		},
		{
			name:     "Minute extraction",
			expr:     expr.Col("timestamp_col").Minute(),
			expected: []int64{30, 59, 0},
		},
		{
			name:     "Second extraction",
			expr:     expr.Col("timestamp_col").Second(),
			expected: []int64{45, 59, 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluator.Evaluate(tt.expr, columns)
			require.NoError(t, err)
			defer result.Release()

			int64Array, ok := result.(*array.Int64)
			require.True(t, ok, "expected int64 array")

			assert.Equal(t, len(tt.expected), int64Array.Len())
			for i, expected := range tt.expected {
				assert.Equal(t, expected, int64Array.Value(i))
			}
		})
	}
}

func TestDateTimeFunctionConstructors(t *testing.T) {
	mem := memory.NewGoAllocator()
	evaluator := expr.NewEvaluator(mem)

	// Create test timestamp
	testTime := time.Date(2023, 5, 10, 16, 45, 30, 0, time.UTC)
	timestampType := &arrow.TimestampType{Unit: arrow.Nanosecond}
	timestampBuilder := array.NewTimestampBuilder(mem, timestampType)
	defer timestampBuilder.Release()

	timestampBuilder.Append(arrow.Timestamp(testTime.UnixNano()))
	timestampArray := timestampBuilder.NewArray()
	defer timestampArray.Release()

	columns := map[string]arrow.Array{
		"ts": timestampArray,
	}

	tests := []struct {
		name     string
		expr     expr.Expr
		expected int64
	}{
		{
			name:     "Year constructor function",
			expr:     expr.Year(expr.Col("ts")),
			expected: 2023,
		},
		{
			name:     "Month constructor function",
			expr:     expr.Month(expr.Col("ts")),
			expected: 5,
		},
		{
			name:     "Day constructor function",
			expr:     expr.Day(expr.Col("ts")),
			expected: 10,
		},
		{
			name:     "Hour constructor function",
			expr:     expr.Hour(expr.Col("ts")),
			expected: 16,
		},
		{
			name:     "Minute constructor function",
			expr:     expr.Minute(expr.Col("ts")),
			expected: 45,
		},
		{
			name:     "Second constructor function",
			expr:     expr.Second(expr.Col("ts")),
			expected: 30,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluator.Evaluate(tt.expr, columns)
			require.NoError(t, err)
			defer result.Release()

			int64Array, ok := result.(*array.Int64)
			require.True(t, ok, "expected int64 array")

			assert.Equal(t, 1, int64Array.Len())
			assert.Equal(t, tt.expected, int64Array.Value(0))
		})
	}
}

func TestDateTimeLiterals(t *testing.T) {
	mem := memory.NewGoAllocator()
	evaluator := expr.NewEvaluator(mem)

	// Create a dummy column to determine array length
	stringBuilder := array.NewStringBuilder(mem)
	defer stringBuilder.Release()
	stringBuilder.Append("dummy")
	stringArray := stringBuilder.NewArray()
	defer stringArray.Release()

	columns := map[string]arrow.Array{
		"dummy": stringArray,
	}

	testTime := time.Date(2023, 7, 4, 12, 30, 45, 0, time.UTC)
	literal := expr.Lit(testTime)

	result, err := evaluator.Evaluate(literal, columns)
	require.NoError(t, err)
	defer result.Release()

	timestampArray, ok := result.(*array.Timestamp)
	require.True(t, ok, "expected timestamp array")

	assert.Equal(t, 1, timestampArray.Len())

	// Convert back to time.Time and compare
	tsValue := timestampArray.Value(0)
	nanos := int64(tsValue)
	const nanosPerSecond = 1e9
	resultTime := time.Unix(nanos/nanosPerSecond, nanos%nanosPerSecond).UTC()
	assert.Equal(t, testTime, resultTime)
}

func TestDateTimeFunctionExpressions(t *testing.T) {
	mem := memory.NewGoAllocator()
	evaluator := expr.NewEvaluator(mem)

	// Create test data
	testTime := time.Date(2023, 8, 15, 10, 25, 30, 0, time.UTC)
	timestampType := &arrow.TimestampType{Unit: arrow.Nanosecond}
	timestampBuilder := array.NewTimestampBuilder(mem, timestampType)
	defer timestampBuilder.Release()

	timestampBuilder.Append(arrow.Timestamp(testTime.UnixNano()))
	timestampArray := timestampBuilder.NewArray()
	defer timestampArray.Release()

	columns := map[string]arrow.Array{
		"timestamp_col": timestampArray,
	}

	// Test chaining with other functions
	t.Run("chained functions", func(t *testing.T) {
		// Test expr.Year().Add(expr.Lit(1))
		yearExpr := expr.Col("timestamp_col").Year()
		addExpr := yearExpr.Add(expr.Lit(1))

		result, err := evaluator.Evaluate(addExpr, columns)
		require.NoError(t, err)
		defer result.Release()

		int64Array, ok := result.(*array.Int64)
		require.True(t, ok, "expected int64 array")

		assert.Equal(t, 1, int64Array.Len())
		assert.Equal(t, int64(2024), int64Array.Value(0)) // 2023 + 1
	})

	// Test comparisons
	t.Run("date comparison", func(t *testing.T) {
		monthExpr := expr.Col("timestamp_col").Month()
		comparisonExpr := monthExpr.Gt(expr.Lit(6))

		result, err := evaluator.EvaluateBoolean(comparisonExpr, columns)
		require.NoError(t, err)
		defer result.Release()

		boolArray, ok := result.(*array.Boolean)
		require.True(t, ok, "expected boolean array")

		assert.Equal(t, 1, boolArray.Len())
		assert.True(t, boolArray.Value(0)) // Month 8 > 6
	})
}

func TestDateTimeFunctionWithNulls(t *testing.T) {
	mem := memory.NewGoAllocator()
	evaluator := expr.NewEvaluator(mem)

	// Create timestamp array with null values
	timestampType := &arrow.TimestampType{Unit: arrow.Nanosecond}
	timestampBuilder := array.NewTimestampBuilder(mem, timestampType)
	defer timestampBuilder.Release()

	testTime := time.Date(2023, 3, 20, 8, 15, 45, 0, time.UTC)
	timestampBuilder.Append(arrow.Timestamp(testTime.UnixNano()))
	timestampBuilder.AppendNull()
	timestampBuilder.Append(arrow.Timestamp(testTime.UnixNano()))

	timestampArray := timestampBuilder.NewArray()
	defer timestampArray.Release()

	columns := map[string]arrow.Array{
		"timestamp_col": timestampArray,
	}

	yearExpr := expr.Col("timestamp_col").Year()
	result, err := evaluator.Evaluate(yearExpr, columns)
	require.NoError(t, err)
	defer result.Release()

	int64Array, ok := result.(*array.Int64)
	require.True(t, ok, "expected int64 array")

	assert.Equal(t, 3, int64Array.Len())
	assert.Equal(t, int64(2023), int64Array.Value(0))
	assert.True(t, int64Array.IsNull(1))
	assert.Equal(t, int64(2023), int64Array.Value(2))
}

func TestDateTimeFunctionErrors(t *testing.T) {
	mem := memory.NewGoAllocator()
	evaluator := expr.NewEvaluator(mem)

	// Create a non-timestamp column
	stringBuilder := array.NewStringBuilder(mem)
	defer stringBuilder.Release()
	stringBuilder.Append("not a timestamp")
	stringArray := stringBuilder.NewArray()
	defer stringArray.Release()

	columns := map[string]arrow.Array{
		"string_col": stringArray,
	}

	tests := []struct {
		name string
		expr expr.Expr
	}{
		{
			name: "Year on non-timestamp column",
			expr: expr.Col("string_col").Year(),
		},
		{
			name: "Month on non-timestamp column",
			expr: expr.Col("string_col").Month(),
		},
		{
			name: "Day on non-timestamp column",
			expr: expr.Col("string_col").Day(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := evaluator.Evaluate(tt.expr, columns)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "requires a timestamp argument")
		})
	}
}

func TestDateTimeFunctionStringRepresentation(t *testing.T) {
	tests := []struct {
		name     string
		expr     expr.Expr
		expected string
	}{
		{
			name:     "Year column expression",
			expr:     expr.Col("timestamp_col").Year(),
			expected: "year(col(timestamp_col))",
		},
		{
			name:     "Month constructor",
			expr:     expr.Month(expr.Col("ts")),
			expected: "month(col(ts))",
		},
		{
			name:     "Day with literal",
			expr:     expr.Day(expr.Lit(time.Date(2023, 5, 10, 0, 0, 0, 0, time.UTC))),
			expected: "day(lit(2023-05-10 00:00:00 +0000 UTC))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.expr.String())
		})
	}
}
