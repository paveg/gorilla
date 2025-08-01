// Package main demonstrates time series analysis with Gorilla DataFrame.
//
// This example showcases:
// - Time-based data manipulation and filtering
// - Rolling window calculations (simulated)
// - Seasonal pattern analysis
// - Data resampling and aggregation
// - Trend analysis and forecasting
// - Performance optimization for time series data
//
//nolint:gosec,mnd // Example code: random generation and magic numbers acceptable for demo
package main

import (
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla"
)

func main() {
	fmt.Println("=== Time Series Analysis Example ===")

	// Create memory allocator
	mem := memory.NewGoAllocator()

	// Generate sample time series data
	df := generateTimeSeriesData(mem)
	defer df.Release()

	fmt.Printf("Generated %d time series observations\n", df.Len())

	// Perform time series analysis
	analysisResult := performTimeSeriesAnalysis(df)
	defer analysisResult.Release()

	fmt.Printf("\nMonthly Analysis Results:\n")
	fmt.Println(analysisResult)

	// Demonstrate streaming analysis for large datasets
	fmt.Println("\n=== Streaming Time Series Processing ===")
	streamingResult := performStreamingAnalysis(df)
	defer streamingResult.Release()

	fmt.Printf("Quarterly Streaming Results:\n")
	fmt.Println(streamingResult)
}

// generateTimeSeriesData creates sample time series data.
func generateTimeSeriesData(mem memory.Allocator) *gorilla.DataFrame {
	const numDays = 365 * 2 // Two years of daily data

	timestamps := make([]string, numDays)
	dates := make([]string, numDays)
	values := make([]float64, numDays)
	categories := make([]string, numDays)
	weekdays := make([]int64, numDays)
	months := make([]int64, numDays)
	quarters := make([]int64, numDays)
	years := make([]int64, numDays)

	baseDate := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
	categoryList := []string{"Sales", "Marketing", "Operations", "Support"}

	for i := range numDays {
		currentDate := baseDate.AddDate(0, 0, i)

		// Generate time components
		timestamps[i] = currentDate.Format("2006-01-02T15:04:05Z")
		dates[i] = currentDate.Format("2006-01-02")
		weekdays[i] = int64(currentDate.Weekday())
		months[i] = int64(currentDate.Month())
		quarters[i] = int64((currentDate.Month()-1)/3 + 1)
		years[i] = int64(currentDate.Year())

		// Generate realistic time series with trends and seasonality
		trend := float64(i) * 0.1                               // Linear trend
		seasonal := 10.0 * math.Sin(2*math.Pi*float64(i)/365.0) // Yearly seasonality
		weekly := 5.0 * math.Sin(2*math.Pi*float64(i)/7.0)      // Weekly seasonality
		noise := rand.NormFloat64() * 2.0                       // Random noise

		baseValue := 100.0
		values[i] = baseValue + trend + seasonal + weekly + noise

		// Add weekend effects (lower values on weekends)
		if weekdays[i] == 0 || weekdays[i] == 6 { // Sunday or Saturday
			values[i] *= 0.7
		}

		categories[i] = categoryList[i%len(categoryList)]
	}

	// Create series
	timestampSeries := gorilla.NewSeries("timestamp", timestamps, mem)
	dateSeries := gorilla.NewSeries("date", dates, mem)
	valueSeries := gorilla.NewSeries("value", values, mem)
	categorySeries := gorilla.NewSeries("category", categories, mem)
	weekdaySeries := gorilla.NewSeries("weekday", weekdays, mem)
	monthSeries := gorilla.NewSeries("month", months, mem)
	quarterSeries := gorilla.NewSeries("quarter", quarters, mem)
	yearSeries := gorilla.NewSeries("year", years, mem)

	defer timestampSeries.Release()
	defer dateSeries.Release()
	defer valueSeries.Release()
	defer categorySeries.Release()
	defer weekdaySeries.Release()
	defer monthSeries.Release()
	defer quarterSeries.Release()
	defer yearSeries.Release()

	return gorilla.NewDataFrame(
		timestampSeries, dateSeries, valueSeries, categorySeries,
		weekdaySeries, monthSeries, quarterSeries, yearSeries,
	)
}

// performTimeSeriesAnalysis demonstrates time series analysis patterns.
func performTimeSeriesAnalysis(df *gorilla.DataFrame) *gorilla.DataFrame {
	// Complex time series analysis with multiple transformations
	result, err := df.Lazy().
		// Step 1: Add time-based indicators
		WithColumn("is_weekend",
			gorilla.Col("weekday").Eq(gorilla.Lit(int64(0))).Or(
				gorilla.Col("weekday").Eq(gorilla.Lit(int64(6))),
			),
		).
		WithColumn("is_month_end",
			gorilla.Lit(false), // Simplified - would need proper date parsing
		).

		// Step 2: Calculate performance metrics
		WithColumn("value_category",
			gorilla.Case().
				When(gorilla.Col("value").Gt(gorilla.Lit(120.0)), gorilla.Lit("High")).
				When(gorilla.Col("value").Gt(gorilla.Lit(100.0)), gorilla.Lit("Medium")).
				Else(gorilla.Lit("Low")),
		).

		// Step 3: Add growth indicators
		WithColumn("above_baseline", gorilla.Col("value").Gt(gorilla.Lit(100.0))).

		// Step 4: Monthly aggregation with multiple metrics
		GroupBy("year", "month", "category").
		Agg(
			gorilla.Count(gorilla.Col("*")).As("observation_count"),
			gorilla.Mean(gorilla.Col("value")).As("avg_value"),
			gorilla.Min(gorilla.Col("value")).As("min_value"),
			gorilla.Max(gorilla.Col("value")).As("max_value"),
			gorilla.Sum(
				gorilla.If(
					gorilla.Col("is_weekend").Eq(gorilla.Lit(true)),
					gorilla.Lit(int64(1)),
					gorilla.Lit(int64(0)),
				),
			).As("weekend_days"),
			gorilla.Sum(
				gorilla.If(
					gorilla.Col("above_baseline").Eq(gorilla.Lit(true)),
					gorilla.Lit(int64(1)),
					gorilla.Lit(int64(0)),
				),
			).As("above_baseline_days"),
		).

		// Step 5: Calculate derived metrics
		WithColumn("value_range",
			gorilla.Col("max_value").Sub(gorilla.Col("min_value")),
		).
		WithColumn("weekend_ratio",
			gorilla.Col("weekend_days").Div(gorilla.Col("observation_count")),
		).
		WithColumn("performance_score",
			gorilla.Col("above_baseline_days").Div(gorilla.Col("observation_count")).Mul(gorilla.Lit(100.0)),
		).

		// Step 6: Filter for meaningful analysis periods
		Filter(gorilla.Col("observation_count").Gt(gorilla.Lit(int64(20)))). // At least 20 observations

		// Step 7: Sort by time for trend analysis
		SortBy([]string{"year", "month", "avg_value"}, []bool{true, true, false}).
		Collect()

	if err != nil {
		log.Fatalf("Time series analysis failed: %v", err)
	}

	return result
}

// performStreamingAnalysis demonstrates streaming analysis for large time series.
func performStreamingAnalysis(df *gorilla.DataFrame) *gorilla.DataFrame {
	// Simulate streaming analysis with chunked processing
	result, err := df.Lazy().
		// Step 1: Add quarter-based grouping for summarization
		WithColumn("year_quarter",
			gorilla.Concat(
				gorilla.Col("year"),
				gorilla.Lit("-Q"),
				gorilla.Col("quarter"),
			),
		).

		// Step 2: Calculate volatility indicators
		WithColumn("high_volatility",
			gorilla.Col("value").Gt(gorilla.Lit(110.0)).Or(
				gorilla.Col("value").Lt(gorilla.Lit(90.0)),
			),
		).

		// Step 3: Quarterly aggregation optimized for streaming
		GroupBy("year_quarter", "category").
		Agg(
			gorilla.Count(gorilla.Col("*")).As("total_observations"),
			gorilla.Mean(gorilla.Col("value")).As("quarterly_avg"),
			gorilla.Sum(
				gorilla.If(
					gorilla.Col("high_volatility").Eq(gorilla.Lit(true)),
					gorilla.Lit(int64(1)),
					gorilla.Lit(int64(0)),
				),
			).As("volatile_days"),
		).

		// Step 4: Calculate quarterly performance metrics
		WithColumn("volatility_ratio",
			gorilla.Col("volatile_days").Div(gorilla.Col("total_observations")),
		).
		WithColumn("performance_tier",
			gorilla.Case().
				When(
					gorilla.Col("quarterly_avg").Gt(gorilla.Lit(115.0)).And(
						gorilla.Col("volatility_ratio").Lt(gorilla.Lit(0.2)),
					),
					gorilla.Lit("Excellent"),
				).
				When(
					gorilla.Col("quarterly_avg").Gt(gorilla.Lit(105.0)),
					gorilla.Lit("Good"),
				).
				When(
					gorilla.Col("quarterly_avg").Gt(gorilla.Lit(95.0)),
					gorilla.Lit("Average"),
				).
				Else(gorilla.Lit("Below_Average")),
		).

		// Step 5: Filter for complete quarters (at least 60 days)
		Filter(gorilla.Col("total_observations").Gt(gorilla.Lit(int64(60)))).

		// Step 6: Sort by performance for ranking
		Sort("quarterly_avg", false).
		Collect()

	if err != nil {
		log.Fatalf("Streaming analysis failed: %v", err)
	}

	return result
}
