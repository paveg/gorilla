package expr

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntervalExpr(t *testing.T) {
	tests := []struct {
		name     string
		interval *IntervalExpr
		expected string
	}{
		{
			name:     "Days interval",
			interval: Days(7),
			expected: "interval(7 days)",
		},
		{
			name:     "Hours interval",
			interval: Hours(24),
			expected: "interval(24 hours)",
		},
		{
			name:     "Minutes interval",
			interval: Minutes(30),
			expected: "interval(30 minutes)",
		},
		{
			name:     "Months interval",
			interval: Months(3),
			expected: "interval(3 months)",
		},
		{
			name:     "Years interval",
			interval: Years(1),
			expected: "interval(1 years)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, ExprLiteral, tt.interval.Type())
			assert.Equal(t, tt.expected, tt.interval.String())
		})
	}
}

func TestDateAdd(t *testing.T) {
	mem := memory.NewGoAllocator()
	evaluator := NewEvaluator(mem)

	// Create test timestamps (2024-01-15 12:30:00 UTC and 2024-02-20 18:45:00 UTC)
	baseTime1 := time.Date(2024, 1, 15, 12, 30, 0, 0, time.UTC)
	baseTime2 := time.Date(2024, 2, 20, 18, 45, 0, 0, time.UTC)

	builder := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Nanosecond})
	defer builder.Release()

	builder.Append(arrow.Timestamp(baseTime1.UnixNano()))
	builder.Append(arrow.Timestamp(baseTime2.UnixNano()))
	timestampArray := builder.NewArray()
	defer timestampArray.Release()

	columns := map[string]arrow.Array{
		"timestamp_col": timestampArray,
	}

	tests := []struct {
		name           string
		expr           *FunctionExpr
		expectedTimes  []time.Time
		expectedLength int
	}{
		{
			name:           "Add 7 days",
			expr:           DateAdd(Col("timestamp_col"), Days(7)),
			expectedTimes:  []time.Time{baseTime1.AddDate(0, 0, 7), baseTime2.AddDate(0, 0, 7)},
			expectedLength: 2,
		},
		{
			name:           "Add 3 hours",
			expr:           DateAdd(Col("timestamp_col"), Hours(3)),
			expectedTimes:  []time.Time{baseTime1.Add(3 * time.Hour), baseTime2.Add(3 * time.Hour)},
			expectedLength: 2,
		},
		{
			name:           "Add 45 minutes",
			expr:           DateAdd(Col("timestamp_col"), Minutes(45)),
			expectedTimes:  []time.Time{baseTime1.Add(45 * time.Minute), baseTime2.Add(45 * time.Minute)},
			expectedLength: 2,
		},
		{
			name:           "Add 2 months",
			expr:           DateAdd(Col("timestamp_col"), Months(2)),
			expectedTimes:  []time.Time{baseTime1.AddDate(0, 2, 0), baseTime2.AddDate(0, 2, 0)},
			expectedLength: 2,
		},
		{
			name:           "Add 1 year",
			expr:           DateAdd(Col("timestamp_col"), Years(1)),
			expectedTimes:  []time.Time{baseTime1.AddDate(1, 0, 0), baseTime2.AddDate(1, 0, 0)},
			expectedLength: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluator.Evaluate(tt.expr, columns)
			require.NoError(t, err)
			defer result.Release()

			timestampResult, ok := result.(*array.Timestamp)
			require.True(t, ok)
			assert.Equal(t, tt.expectedLength, timestampResult.Len())

			for i := 0; i < tt.expectedLength; i++ {
				tsValue := int64(timestampResult.Value(i))
				resultTime := time.Unix(tsValue/nanosPerSecond, tsValue%nanosPerSecond).UTC()
				assert.True(t, tt.expectedTimes[i].Equal(resultTime), "Expected %v, got %v", tt.expectedTimes[i], resultTime)
			}
		})
	}
}

func TestDateSub(t *testing.T) {
	mem := memory.NewGoAllocator()
	evaluator := NewEvaluator(mem)

	// Create test timestamps (2024-01-15 12:30:00 UTC and 2024-02-20 18:45:00 UTC)
	baseTime1 := time.Date(2024, 1, 15, 12, 30, 0, 0, time.UTC)
	baseTime2 := time.Date(2024, 2, 20, 18, 45, 0, 0, time.UTC)

	builder := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Nanosecond})
	defer builder.Release()

	builder.Append(arrow.Timestamp(baseTime1.UnixNano()))
	builder.Append(arrow.Timestamp(baseTime2.UnixNano()))
	timestampArray := builder.NewArray()
	defer timestampArray.Release()

	columns := map[string]arrow.Array{
		"timestamp_col": timestampArray,
	}

	tests := []struct {
		name           string
		expr           *FunctionExpr
		expectedTimes  []time.Time
		expectedLength int
	}{
		{
			name:           "Subtract 7 days",
			expr:           DateSub(Col("timestamp_col"), Days(7)),
			expectedTimes:  []time.Time{baseTime1.AddDate(0, 0, -7), baseTime2.AddDate(0, 0, -7)},
			expectedLength: 2,
		},
		{
			name:           "Subtract 3 hours",
			expr:           DateSub(Col("timestamp_col"), Hours(3)),
			expectedTimes:  []time.Time{baseTime1.Add(-3 * time.Hour), baseTime2.Add(-3 * time.Hour)},
			expectedLength: 2,
		},
		{
			name:           "Subtract 45 minutes",
			expr:           DateSub(Col("timestamp_col"), Minutes(45)),
			expectedTimes:  []time.Time{baseTime1.Add(-45 * time.Minute), baseTime2.Add(-45 * time.Minute)},
			expectedLength: 2,
		},
		{
			name:           "Subtract 2 months",
			expr:           DateSub(Col("timestamp_col"), Months(2)),
			expectedTimes:  []time.Time{baseTime1.AddDate(0, -2, 0), baseTime2.AddDate(0, -2, 0)},
			expectedLength: 2,
		},
		{
			name:           "Subtract 1 year",
			expr:           DateSub(Col("timestamp_col"), Years(1)),
			expectedTimes:  []time.Time{baseTime1.AddDate(-1, 0, 0), baseTime2.AddDate(-1, 0, 0)},
			expectedLength: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluator.Evaluate(tt.expr, columns)
			require.NoError(t, err)
			defer result.Release()

			timestampResult, ok := result.(*array.Timestamp)
			require.True(t, ok)
			assert.Equal(t, tt.expectedLength, timestampResult.Len())

			for i := 0; i < tt.expectedLength; i++ {
				tsValue := int64(timestampResult.Value(i))
				resultTime := time.Unix(tsValue/nanosPerSecond, tsValue%nanosPerSecond).UTC()
				assert.True(t, tt.expectedTimes[i].Equal(resultTime), "Expected %v, got %v", tt.expectedTimes[i], resultTime)
			}
		})
	}
}

func TestDateDiff(t *testing.T) {
	mem := memory.NewGoAllocator()
	evaluator := NewEvaluator(mem)

	// Create test timestamps
	startTime1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime1 := time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC) // 7 days later

	startTime2 := time.Date(2024, 2, 15, 12, 0, 0, 0, time.UTC)
	endTime2 := time.Date(2024, 2, 15, 15, 30, 0, 0, time.UTC) // 3.5 hours later

	// Build start times array
	startBuilder := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Nanosecond})
	defer startBuilder.Release()
	startBuilder.Append(arrow.Timestamp(startTime1.UnixNano()))
	startBuilder.Append(arrow.Timestamp(startTime2.UnixNano()))
	startArray := startBuilder.NewArray()
	defer startArray.Release()

	// Build end times array
	endBuilder := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Nanosecond})
	defer endBuilder.Release()
	endBuilder.Append(arrow.Timestamp(endTime1.UnixNano()))
	endBuilder.Append(arrow.Timestamp(endTime2.UnixNano()))
	endArray := endBuilder.NewArray()
	defer endArray.Release()

	columns := map[string]arrow.Array{
		"start_col": startArray,
		"end_col":   endArray,
	}

	tests := []struct {
		name           string
		expr           *FunctionExpr
		expectedValues []int64
		expectedLength int
	}{
		{
			name:           "Diff in days",
			expr:           DateDiff(Col("start_col"), Col("end_col"), "days"),
			expectedValues: []int64{7, 0}, // 7 days, 0 days
			expectedLength: 2,
		},
		{
			name:           "Diff in hours",
			expr:           DateDiff(Col("start_col"), Col("end_col"), "hours"),
			expectedValues: []int64{168, 3}, // 7*24 = 168 hours, 3 hours
			expectedLength: 2,
		},
		{
			name:           "Diff in minutes",
			expr:           DateDiff(Col("start_col"), Col("end_col"), "minutes"),
			expectedValues: []int64{10080, 210}, // 7*24*60 = 10080 minutes, 3.5*60 = 210 minutes
			expectedLength: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluator.Evaluate(tt.expr, columns)
			require.NoError(t, err)
			defer result.Release()

			int64Result, ok := result.(*array.Int64)
			require.True(t, ok)
			assert.Equal(t, tt.expectedLength, int64Result.Len())

			for i := 0; i < tt.expectedLength; i++ {
				assert.Equal(t, tt.expectedValues[i], int64Result.Value(i))
			}
		})
	}
}

func TestDateDiffMonthsAndYears(t *testing.T) {
	mem := memory.NewGoAllocator()
	evaluator := NewEvaluator(mem)

	// Create test timestamps for month/year calculations
	startTime := time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2024, 3, 10, 0, 0, 0, 0, time.UTC) // 1 year, 1 month, but 10 < 15 day

	// Build arrays
	startBuilder := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Nanosecond})
	defer startBuilder.Release()
	startBuilder.Append(arrow.Timestamp(startTime.UnixNano()))
	startArray := startBuilder.NewArray()
	defer startArray.Release()

	endBuilder := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Nanosecond})
	defer endBuilder.Release()
	endBuilder.Append(arrow.Timestamp(endTime.UnixNano()))
	endArray := endBuilder.NewArray()
	defer endArray.Release()

	columns := map[string]arrow.Array{
		"start_col": startArray,
		"end_col":   endArray,
	}

	tests := []struct {
		name          string
		expr          *FunctionExpr
		expectedValue int64
	}{
		{
			name:          "Diff in months (adjust for day)",
			expr:          DateDiff(Col("start_col"), Col("end_col"), "months"),
			expectedValue: 13, // 14 months minus 1 for day adjustment
		},
		{
			name:          "Diff in years",
			expr:          DateDiff(Col("start_col"), Col("end_col"), "years"),
			expectedValue: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluator.Evaluate(tt.expr, columns)
			require.NoError(t, err)
			defer result.Release()

			int64Result, ok := result.(*array.Int64)
			require.True(t, ok)
			assert.Equal(t, 1, int64Result.Len())
			assert.Equal(t, tt.expectedValue, int64Result.Value(0))
		})
	}
}

func TestDateArithmeticWithNulls(t *testing.T) {
	mem := memory.NewGoAllocator()
	evaluator := NewEvaluator(mem)

	// Create array with null values
	builder := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Nanosecond})
	defer builder.Release()

	baseTime := time.Date(2024, 1, 15, 12, 30, 0, 0, time.UTC)
	builder.Append(arrow.Timestamp(baseTime.UnixNano()))
	builder.AppendNull()
	builder.Append(arrow.Timestamp(baseTime.UnixNano()))
	timestampArray := builder.NewArray()
	defer timestampArray.Release()

	columns := map[string]arrow.Array{
		"timestamp_col": timestampArray,
	}

	tests := []struct {
		name string
		expr *FunctionExpr
	}{
		{
			name: "DateAdd with nulls",
			expr: DateAdd(Col("timestamp_col"), Days(7)),
		},
		{
			name: "DateSub with nulls",
			expr: DateSub(Col("timestamp_col"), Hours(3)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluator.Evaluate(tt.expr, columns)
			require.NoError(t, err)
			defer result.Release()

			timestampResult, ok := result.(*array.Timestamp)
			require.True(t, ok)
			assert.Equal(t, 3, timestampResult.Len())

			// Check that null values are preserved
			assert.False(t, timestampResult.IsNull(0))
			assert.True(t, timestampResult.IsNull(1))
			assert.False(t, timestampResult.IsNull(2))
		})
	}
}

func TestDateArithmeticChaining(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data
	baseTime := time.Date(2024, 1, 15, 12, 30, 0, 0, time.UTC)
	builder := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Nanosecond})
	defer builder.Release()
	builder.Append(arrow.Timestamp(baseTime.UnixNano()))
	timestampArray := builder.NewArray()
	defer timestampArray.Release()

	tests := []struct {
		name string
		expr Expr
	}{
		{
			name: "Column DateAdd method",
			expr: Col("timestamp_col").DateAdd(Days(7)),
		},
		{
			name: "Column DateSub method",
			expr: Col("timestamp_col").DateSub(Hours(3)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			funcExpr, ok := tt.expr.(*FunctionExpr)
			require.True(t, ok)
			assert.Contains(t, []string{"date_add", "date_sub"}, funcExpr.name)
			assert.Len(t, funcExpr.args, 2)
		})
	}
}

func TestDateArithmeticErrors(t *testing.T) {
	mem := memory.NewGoAllocator()
	evaluator := NewEvaluator(mem)

	// Create non-timestamp array for error testing
	int64Builder := array.NewInt64Builder(mem)
	defer int64Builder.Release()
	int64Builder.Append(123)
	int64Array := int64Builder.NewArray()
	defer int64Array.Release()

	columns := map[string]arrow.Array{
		"int_col": int64Array,
	}

	tests := []struct {
		name        string
		expr        *FunctionExpr
		expectedErr string
	}{
		{
			name:        "DateAdd with non-timestamp",
			expr:        DateAdd(Col("int_col"), Days(7)),
			expectedErr: "date_add function requires a timestamp argument",
		},
		{
			name:        "DateSub with non-timestamp",
			expr:        DateSub(Col("int_col"), Hours(3)),
			expectedErr: "date_sub function requires a timestamp argument",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := evaluator.Evaluate(tt.expr, columns)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}

func TestUnsupportedDateDiffUnit(t *testing.T) {
	mem := memory.NewGoAllocator()
	evaluator := NewEvaluator(mem)

	// Create test timestamps
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	builder := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Nanosecond})
	defer builder.Release()
	builder.Append(arrow.Timestamp(baseTime.UnixNano()))
	timestampArray := builder.NewArray()
	defer timestampArray.Release()

	columns := map[string]arrow.Array{
		"start_col": timestampArray,
		"end_col":   timestampArray,
	}

	// Test unsupported unit - should now return an error for better error visibility
	expr := DateDiff(Col("start_col"), Col("end_col"), "invalid_unit")
	_, err := evaluator.Evaluate(expr, columns)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "date_diff function unsupported unit: invalid_unit")
}
