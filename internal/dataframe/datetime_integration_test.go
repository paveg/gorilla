package dataframe_test

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper functions to extract values from ISeries.
func getStringValues(series dataframe.ISeries) []string {
	arr := series.Array()
	stringArray, ok := arr.(*array.String)
	if !ok {
		return nil
	}
	values := make([]string, stringArray.Len())
	for i := range stringArray.Len() {
		if !stringArray.IsNull(i) {
			values[i] = stringArray.Value(i)
		}
	}
	return values
}

func getInt64Values(series dataframe.ISeries) []int64 {
	arr := series.Array()
	intArray, ok := arr.(*array.Int64)
	if !ok {
		return nil
	}
	values := make([]int64, intArray.Len())
	for i := range intArray.Len() {
		if !intArray.IsNull(i) {
			values[i] = intArray.Value(i)
		}
	}
	return values
}

func getFloat64Values(series dataframe.ISeries) []float64 {
	arr := series.Array()
	floatArray, ok := arr.(*array.Float64)
	if !ok {
		return nil
	}
	values := make([]float64, floatArray.Len())
	for i := range floatArray.Len() {
		if !floatArray.IsNull(i) {
			values[i] = floatArray.Value(i)
		}
	}
	return values
}

func getTimestampValues(series dataframe.ISeries) []time.Time {
	arr := series.Array()
	timestampArray, ok := arr.(*array.Timestamp)
	if !ok {
		return nil
	}
	values := make([]time.Time, timestampArray.Len())
	for i := range timestampArray.Len() {
		if !timestampArray.IsNull(i) {
			nanos := int64(timestampArray.Value(i))
			values[i] = time.Unix(nanos/1e9, nanos%1e9).UTC()
		}
	}
	return values
}

func TestDataFrameDateTimeArithmetic(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data with timestamps
	timestamps := []time.Time{
		time.Date(2023, 1, 15, 9, 0, 0, 0, time.UTC),
		time.Date(2023, 6, 30, 14, 30, 0, 0, time.UTC),
		time.Date(2023, 12, 25, 18, 45, 0, 0, time.UTC),
	}

	names := []string{"Alice", "Bob", "Charlie"}
	ids := []int64{1, 2, 3}

	// Create series
	timestampSeries := series.New("created_at", timestamps, mem)
	nameSeries := series.New("name", names, mem)
	idSeries := series.New("id", ids, mem)

	defer timestampSeries.Release()
	defer nameSeries.Release()
	defer idSeries.Release()

	// Create DataFrame
	df := dataframe.New(timestampSeries, nameSeries, idSeries)
	defer df.Release()

	t.Run("DateAdd in LazyFrame", func(t *testing.T) {
		// Add 30 days to created_at
		result, err := df.Lazy().
			WithColumn("expiry_date", expr.Col("created_at").DateAdd(expr.Days(30))).
			Select("name", "created_at", "expiry_date").
			Collect()
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 3, result.Len())
		assert.Equal(t, 3, result.Width())
		assert.Equal(t, []string{"name", "created_at", "expiry_date"}, result.Columns())

		// Verify the expiry dates are 30 days after created_at
		createdAtSeries, exists := result.Column("created_at")
		require.True(t, exists)
		createdAtArray := createdAtSeries.Array()
		createdAtTimestamp, ok := createdAtArray.(*array.Timestamp)
		require.True(t, ok)

		expirySeries, exists := result.Column("expiry_date")
		require.True(t, exists)
		expiryArray := expirySeries.Array()
		expiryTimestamp, ok := expiryArray.(*array.Timestamp)
		require.True(t, ok)

		for i := range timestamps {
			expected := timestamps[i].AddDate(0, 0, 30)

			// Convert timestamp values back to time.Time
			expiryNanos := int64(expiryTimestamp.Value(i))
			expiryTime := time.Unix(expiryNanos/1e9, expiryNanos%1e9).UTC()

			createdNanos := int64(createdAtTimestamp.Value(i))
			createdTime := time.Unix(createdNanos/1e9, createdNanos%1e9).UTC()

			assert.True(t, expected.Equal(expiryTime), "expiry date %d should be 30 days after created_at", i)
			assert.True(t, timestamps[i].Equal(createdTime), "created_at should be preserved")
		}
	})

	t.Run("DateSub with Filter", func(t *testing.T) {
		// Subtract 1 hour and filter for afternoon times
		result, err := df.Lazy().
			WithColumn("adjusted_time", expr.Col("created_at").DateSub(expr.Hours(1))).
			Filter(expr.Col("adjusted_time").Hour().Gt(expr.Lit(int64(12)))).
			Select("name", "adjusted_time").
			Collect()
		require.NoError(t, err)
		defer result.Release()

		// Should include Bob (13:30) and Charlie (17:45) after subtracting 1 hour
		assert.Equal(t, 2, result.Len())
		nameSeries, exists := result.Column("name")
		require.True(t, exists)
		nameValues := getStringValues(nameSeries)
		assert.Contains(t, nameValues, "Bob")
		assert.Contains(t, nameValues, "Charlie")
	})

	t.Run("Multiple DateTime Operations", func(t *testing.T) {
		// Complex datetime manipulation
		result, err := df.Lazy().
			WithColumn("next_month", expr.Col("created_at").DateAdd(expr.Months(1))).
			WithColumn("year", expr.Col("created_at").Year()).
			WithColumn("month", expr.Col("created_at").Month()).
			Select("name", "year", "month", "next_month").
			Collect()
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 3, result.Len())
		assert.Equal(t, 4, result.Width())

		// Verify years and months
		yearSeries, exists := result.Column("year")
		require.True(t, exists)
		years := getInt64Values(yearSeries)

		monthSeries, exists := result.Column("month")
		require.True(t, exists)
		months := getInt64Values(monthSeries)

		nextMonthSeries, exists := result.Column("next_month")
		require.True(t, exists)
		nextMonths := getTimestampValues(nextMonthSeries)

		expectedYears := []int64{2023, 2023, 2023}
		expectedMonths := []int64{1, 6, 12}

		assert.Equal(t, expectedYears, years)
		assert.Equal(t, expectedMonths, months)

		// Verify next month calculations
		for i, timestamp := range timestamps {
			expected := timestamp.AddDate(0, 1, 0)
			assert.Equal(t, expected, nextMonths[i], "next month %d should be correct", i)
		}
	})
}

func TestDataFrameDateDiff(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data with start and end timestamps
	startTimes := []time.Time{
		time.Date(2023, 1, 1, 9, 0, 0, 0, time.UTC),
		time.Date(2023, 6, 15, 10, 0, 0, 0, time.UTC),
		time.Date(2023, 10, 1, 8, 0, 0, 0, time.UTC),
	}

	endTimes := []time.Time{
		time.Date(2023, 1, 8, 17, 0, 0, 0, time.UTC),   // 7 days 8 hours later
		time.Date(2023, 6, 15, 14, 30, 0, 0, time.UTC), // 4.5 hours later
		time.Date(2023, 12, 1, 8, 0, 0, 0, time.UTC),   // 2 months later
	}

	projects := []string{"Project A", "Project B", "Project C"}

	// Create series
	startSeries := series.New("start_date", startTimes, mem)
	endSeries := series.New("end_date", endTimes, mem)
	projectSeries := series.New("project", projects, mem)

	defer startSeries.Release()
	defer endSeries.Release()
	defer projectSeries.Release()

	// Create DataFrame
	df := dataframe.New(startSeries, endSeries, projectSeries)
	defer df.Release()

	t.Run("DateDiff in Days", func(t *testing.T) {
		result, err := df.Lazy().
			WithColumn("duration_days", expr.DateDiff(expr.Col("start_date"), expr.Col("end_date"), "days")).
			Select("project", "duration_days").
			Collect()
		require.NoError(t, err)
		defer result.Release()

		durationSeries, exists := result.Column("duration_days")
		require.True(t, exists)
		durations := getInt64Values(durationSeries)
		expected := []int64{7, 0, 61} // 7 days, same day, ~61 days (Oct 1 to Dec 1)

		assert.Equal(t, expected, durations)
	})

	t.Run("DateDiff in Hours with Filter", func(t *testing.T) {
		result, err := df.Lazy().
			WithColumn("duration_hours", expr.DateDiff(expr.Col("start_date"), expr.Col("end_date"), "hours")).
			Filter(expr.Col("duration_hours").Lt(expr.Lit(int64(24)))).
			Select("project", "duration_hours").
			Collect()
		require.NoError(t, err)
		defer result.Release()

		// Should only include Project B (4 hours)
		assert.Equal(t, 1, result.Len())
		projectSeries, exists := result.Column("project")
		require.True(t, exists)
		projectValues := getStringValues(projectSeries)

		durationSeries, exists := result.Column("duration_hours")
		require.True(t, exists)
		durations := getInt64Values(durationSeries)

		assert.Equal(t, []string{"Project B"}, projectValues)
		assert.Equal(t, []int64{4}, durations)
	})
}

func TestDataFrameDateTimeGroupBy(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data with timestamps across different months
	timestamps := []time.Time{
		time.Date(2023, 1, 15, 9, 0, 0, 0, time.UTC),
		time.Date(2023, 1, 20, 14, 0, 0, 0, time.UTC),
		time.Date(2023, 2, 10, 11, 0, 0, 0, time.UTC),
		time.Date(2023, 2, 25, 16, 0, 0, 0, time.UTC),
		time.Date(2023, 3, 5, 13, 0, 0, 0, time.UTC),
	}

	amounts := []float64{100.0, 150.0, 200.0, 300.0, 250.0}
	types := []string{"sale", "sale", "refund", "sale", "sale"}

	// Create series
	timestampSeries := series.New("transaction_date", timestamps, mem)
	amountSeries := series.New("amount", amounts, mem)
	typeSeries := series.New("type", types, mem)

	defer timestampSeries.Release()
	defer amountSeries.Release()
	defer typeSeries.Release()

	// Create DataFrame
	df := dataframe.New(timestampSeries, amountSeries, typeSeries)
	defer df.Release()

	t.Run("GroupBy Month with DateTime Extraction", func(t *testing.T) {
		result, err := df.Lazy().
			WithColumn("month", expr.Col("transaction_date").Month()).
			GroupBy("month").
			Agg(
				expr.Sum(expr.Col("amount")).As("total_amount"),
				expr.Count(expr.Col("*")).As("transaction_count"),
			).
			Collect()
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 3, result.Len()) // 3 months
		assert.Equal(t, 3, result.Width())

		monthSeries, exists := result.Column("month")
		require.True(t, exists)
		months := getInt64Values(monthSeries)

		totalSeries, exists := result.Column("total_amount")
		require.True(t, exists)
		totals := getFloat64Values(totalSeries)

		countSeries, exists := result.Column("transaction_count")
		require.True(t, exists)
		counts := getInt64Values(countSeries)

		// Verify the data (exact order may vary due to grouping)
		expectedTotals := map[int64]float64{1: 250.0, 2: 500.0, 3: 250.0} // Jan: 250, Feb: 500, Mar: 250
		expectedCounts := map[int64]int64{1: 2, 2: 2, 3: 1}               // Jan: 2, Feb: 2, Mar: 1

		for i := range months {
			month := months[i]
			assert.InDelta(t, expectedTotals[month], totals[i], 0.01, "total for month %d", month)
			assert.Equal(t, expectedCounts[month], counts[i], "count for month %d", month)
		}
	})

	t.Run("Filter by Year and DateAdd", func(t *testing.T) {
		result, err := df.Lazy().
			Filter(expr.Col("transaction_date").Year().Eq(expr.Lit(int64(2023)))).
			WithColumn("due_date", expr.Col("transaction_date").DateAdd(expr.Days(30))).
			WithColumn("due_month", expr.Col("due_date").Month()).
			Select("type", "amount", "due_month").
			Collect()
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 5, result.Len()) // All transactions are from 2023

		dueMonthSeries, exists := result.Column("due_month")
		require.True(t, exists)
		dueMonths := getInt64Values(dueMonthSeries)
		// Due dates should be 30 days after transaction dates
		// Jan 15 + 30 = Feb 14 (month 2), Jan 20 + 30 = Feb 19 (month 2)
		// Feb 10 + 30 = Mar 12 (month 3), Feb 25 + 30 = Mar 27 (month 3)
		// Mar 5 + 30 = Apr 4 (month 4)
		expectedDueMonths := []int64{2, 2, 3, 3, 4}
		assert.Equal(t, expectedDueMonths, dueMonths)
	})
}

func TestDataFrameDateTimeComplexQuery(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create a more complex dataset with employee records
	employees := []string{"Alice", "Bob", "Charlie", "Diana", "Eve"}
	hireDates := []time.Time{
		time.Date(2020, 3, 15, 0, 0, 0, 0, time.UTC),
		time.Date(2019, 7, 22, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 1, 10, 0, 0, 0, 0, time.UTC),
		time.Date(2018, 11, 5, 0, 0, 0, 0, time.UTC),
		time.Date(2022, 9, 1, 0, 0, 0, 0, time.UTC),
	}
	departments := []string{"Engineering", "Sales", "Engineering", "HR", "Sales"}
	salaries := []float64{75000, 65000, 80000, 55000, 60000}

	// Create series
	employeeSeries := series.New("employee", employees, mem)
	hireDateSeries := series.New("hire_date", hireDates, mem)
	deptSeries := series.New("department", departments, mem)
	salarySeries := series.New("salary", salaries, mem)

	defer employeeSeries.Release()
	defer hireDateSeries.Release()
	defer deptSeries.Release()
	defer salarySeries.Release()

	// Create DataFrame
	df := dataframe.New(employeeSeries, hireDateSeries, deptSeries, salarySeries)
	defer df.Release()

	t.Run("Complex DateTime Query with Service Length", func(t *testing.T) {
		// Calculate years of service and analyze by department
		currentDate := time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC)
		currentDateExpr := expr.Lit(currentDate)

		result, err := df.Lazy().
			WithColumn("years_of_service", expr.DateDiff(expr.Col("hire_date"), currentDateExpr, "years")).
			WithColumn("hire_year", expr.Col("hire_date").Year()).
			Filter(expr.Col("years_of_service").Ge(expr.Lit(int64(2)))).
			GroupBy("department").
			Agg(
				expr.Count(expr.Col("*")).As("employee_count"),
				expr.Mean(expr.Col("years_of_service")).As("avg_years_service"),
				expr.Sum(expr.Col("salary")).As("total_salary"),
			).
			Collect()
		require.NoError(t, err)
		defer result.Release()

		// Should include departments with employees having 2+ years service
		// Alice: 3 years (Engineering), Bob: 3+ years (Sales), Charlie: 2 years (Engineering), Diana: 4+ years (HR)

		deptSeries, exists := result.Column("department")
		require.True(t, exists)
		depts := getStringValues(deptSeries)

		countSeries, exists := result.Column("employee_count")
		require.True(t, exists)
		counts := getInt64Values(countSeries)

		// Should have multiple departments
		assert.GreaterOrEqual(t, result.Len(), 1)

		// Verify all counts are positive
		for i, count := range counts {
			assert.Positive(t, count, "employee count for department %s should be positive", depts[i])
		}
	})

	t.Run("Quarterly Hire Analysis", func(t *testing.T) {
		result, err := df.Lazy().
			WithColumn("hire_quarter", expr.Col("hire_date").Month().Sub(expr.Lit(int64(1))).Div(expr.Lit(int64(3))).Add(expr.Lit(int64(1)))).
			WithColumn("hire_year", expr.Col("hire_date").Year()).
			GroupBy("hire_year", "hire_quarter").
			Agg(
				expr.Count(expr.Col("*")).As("hires"),
				expr.Mean(expr.Col("salary")).As("avg_salary"),
			).
			Collect()
		require.NoError(t, err)
		defer result.Release()

		// Verify that we have the expected number of year-quarter combinations
		assert.LessOrEqual(t, result.Len(), 5) // At most 5 different quarters (one per employee)

		yearSeries, exists := result.Column("hire_year")
		require.True(t, exists)
		years := getInt64Values(yearSeries)

		quarterSeries, exists := result.Column("hire_quarter")
		require.True(t, exists)
		quarters := getInt64Values(quarterSeries)

		// All quarters should be valid (1-4)
		for i, quarter := range quarters {
			assert.True(t, quarter >= 1 && quarter <= 4, "Quarter %d should be between 1-4 for year %d", quarter, years[i])
		}
	})
}
