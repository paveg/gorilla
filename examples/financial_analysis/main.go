// Package main demonstrates complex financial data analysis with Gorilla DataFrame.
//
// This example shows advanced patterns including:
// - Multi-step financial calculations
// - Rolling window operations
// - Conditional logic for trading signals
// - Multi-level grouping and aggregation
// - Complex filtering with HAVING clauses
// - Performance optimization techniques
//
//nolint:gosec,mnd // Example code: random generation and magic numbers acceptable for demo
package main

import (
	"fmt"
	"log"
	"math/rand/v2"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla"
)

func main() {
	fmt.Println("=== Financial Analysis Example ===")

	// Create memory allocator
	mem := memory.NewGoAllocator()

	// Generate sample financial data
	df := generateFinancialData(mem)
	defer df.Release()

	fmt.Printf("Loaded %d financial records across %d columns\n", df.Len(), df.Width())

	// Perform complex multi-step analysis
	result := performFinancialAnalysis(df)
	defer result.Release()

	fmt.Printf("\nAnalysis Results (%d sectors analyzed):\n", result.Len())
	fmt.Println(result)
}

// generateFinancialData creates sample financial data for analysis.
func generateFinancialData(mem memory.Allocator) *gorilla.DataFrame {
	const numRecords = 1000
	sectors := []string{"Technology", "Finance", "Healthcare", "Energy", "Consumer"}

	// Generate sample data
	dates := make([]string, numRecords)
	symbols := make([]string, numRecords)
	sectorData := make([]string, numRecords)
	prices := make([]float64, numRecords)
	volumes := make([]int64, numRecords)
	returns := make([]float64, numRecords)

	baseDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	for i := range numRecords {
		// Generate dates (daily data)
		date := baseDate.AddDate(0, 0, i%252) // One year of trading days
		dates[i] = date.Format("2006-01-02")

		// Generate symbols and sectors
		sectorIdx := i % len(sectors)
		sectorData[i] = sectors[sectorIdx]
		symbols[i] = fmt.Sprintf("%s%d", sectors[sectorIdx][:3], (i%10)+1)

		// Generate realistic financial data
		basePrice := 50.0 + float64(sectorIdx)*20.0
		volatility := 0.02 + rand.Float64()*0.03
		prices[i] = basePrice * (1.0 + (rand.Float64()-0.5)*volatility)

		volumes[i] = int64(100000 + rand.IntN(900000))
		returns[i] = (rand.Float64() - 0.5) * 0.1 // -5% to +5% returns
	}

	// Create series
	dateSeries := gorilla.NewSeries("date", dates, mem)
	symbolSeries := gorilla.NewSeries("symbol", symbols, mem)
	sectorSeries := gorilla.NewSeries("sector", sectorData, mem)
	priceSeries := gorilla.NewSeries("price", prices, mem)
	volumeSeries := gorilla.NewSeries("volume", volumes, mem)
	returnSeries := gorilla.NewSeries("return", returns, mem)

	defer dateSeries.Release()
	defer symbolSeries.Release()
	defer sectorSeries.Release()
	defer priceSeries.Release()
	defer volumeSeries.Release()
	defer returnSeries.Release()

	return gorilla.NewDataFrame(dateSeries, symbolSeries, sectorSeries, priceSeries, volumeSeries, returnSeries)
}

// performFinancialAnalysis demonstrates complex financial analysis patterns.
func performFinancialAnalysis(df *gorilla.DataFrame) *gorilla.DataFrame {
	// Complex multi-step analysis using lazy evaluation
	result, err := df.Lazy().
		// Step 1: Calculate technical indicators
		// Note: This is a simplified example - real moving averages would require window functions
		WithColumn("price_ma_proxy", gorilla.Col("price").Mul(gorilla.Lit(0.98))). // Simplified MA proxy
		WithColumn("high_volume", gorilla.Col("volume").Gt(gorilla.Lit(int64(500000)))).

		// Step 2: Generate trading signals using conditional logic
		WithColumn("signal",
			gorilla.Case().
				When(
					gorilla.Col("return").Gt(gorilla.Lit(0.03)).And(
						gorilla.Col("high_volume").Eq(gorilla.Lit(true)),
					),
					gorilla.Lit("STRONG_BUY"),
				).
				When(
					gorilla.Col("return").Gt(gorilla.Lit(0.01)),
					gorilla.Lit("BUY"),
				).
				When(
					gorilla.Col("return").Lt(gorilla.Lit(-0.03)).And(
						gorilla.Col("high_volume").Eq(gorilla.Lit(true)),
					),
					gorilla.Lit("STRONG_SELL"),
				).
				When(
					gorilla.Col("return").Lt(gorilla.Lit(-0.01)),
					gorilla.Lit("SELL"),
				).
				Else(gorilla.Lit("HOLD")),
		).

		// Step 3: Calculate position sizing based on volatility
		WithColumn("position_size",
			gorilla.If(
				gorilla.Col("return").Gt(gorilla.Lit(0.0)),
				gorilla.Col("volume").Mul(gorilla.Lit(0.1)),  // 10% of volume for positive returns
				gorilla.Col("volume").Mul(gorilla.Lit(0.05)), // 5% of volume for negative returns
			),
		).

		// Step 4: Multi-level grouping and aggregation
		GroupBy("sector", "signal").
		Agg(
			gorilla.Count(gorilla.Col("*")).As("trade_count"),
			gorilla.Sum(gorilla.Col("volume")).As("total_volume"),
			gorilla.Mean(gorilla.Col("return")).As("avg_return"),
			gorilla.Sum(gorilla.Col("position_size")).As("total_position"),
			gorilla.Min(gorilla.Col("price")).As("min_price"),
			gorilla.Max(gorilla.Col("price")).As("max_price"),
		).

		// Step 5: Calculate risk metrics
		WithColumn("risk_reward_ratio",
			gorilla.If(
				gorilla.Col("avg_return").Lt(gorilla.Lit(0.0)),
				gorilla.Lit(-1.0), // Negative ratio for losses
				gorilla.Col("avg_return").Div(gorilla.Lit(0.02)), // Risk-adjusted return
			),
		).

		// Step 6: Filter for significant trading opportunities
		// Use HAVING clause to filter aggregated results
		Filter(gorilla.Col("trade_count").Gt(gorilla.Lit(int64(5)))).        // At least 5 trades
		Filter(gorilla.Col("total_volume").Gt(gorilla.Lit(int64(1000000)))). // Minimum volume threshold

		// Step 7: Rank by performance
		Sort("avg_return", false). // Sort by average return descending

		// Execute the entire analysis plan
		Collect()

	if err != nil {
		log.Fatalf("Financial analysis failed: %v", err)
	}

	return result
}
