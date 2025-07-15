package main

import (
	"fmt"
	"log"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
)

func main() {
	fmt.Println("=== Debug Mode and Execution Plan Visualization Demo ===")
	fmt.Println()

	// Create memory allocator
	mem := memory.NewGoAllocator()

	// Create sample data
	names := []string{"Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry"}
	ages := []int64{25, 30, 35, 40, 45, 50, 55, 60}
	salaries := []float64{50000, 60000, 70000, 80000, 90000, 100000, 110000, 120000}

	// Create series
	namesSeries, err := series.NewSafe("name", names, mem)
	if err != nil {
		log.Fatal(err)
	}
	defer namesSeries.Release()

	agesSeries, err := series.NewSafe("age", ages, mem)
	if err != nil {
		log.Fatal(err)
	}
	defer agesSeries.Release()

	salariesSeries, err := series.NewSafe("salary", salaries, mem)
	if err != nil {
		log.Fatal(err)
	}
	defer salariesSeries.Release()

	// Create DataFrame
	df := dataframe.New(namesSeries, agesSeries, salariesSeries)
	defer df.Release()

	fmt.Printf("Created DataFrame with %d rows and %d columns\n", df.Len(), df.Width())
	fmt.Println("Columns:", df.Columns())
	fmt.Println()

	// Demonstrate debug mode
	fmt.Println("=== Debug Mode Features ===")
	fmt.Println()

	// 1. Basic execution plan
	fmt.Println("1. Basic Execution Plan:")
	lazyFrame := df.Lazy().
		Filter(expr.Col("age").Gt(expr.Lit(30))).
		Select("name", "salary").
		Filter(expr.Col("salary").Lt(expr.Lit(100000)))

	plan := lazyFrame.Explain()
	fmt.Print(plan.RenderText())
	fmt.Println()

	// 2. Execution plan with analysis
	fmt.Println("2. Execution Plan with Analysis:")
	analyzedPlan, err := lazyFrame.ExplainAnalyze()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(analyzedPlan.RenderText())
	fmt.Println()

	// 3. JSON representation
	fmt.Println("3. JSON Representation:")
	jsonPlan, err := plan.RenderJSON()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(jsonPlan))
	fmt.Println()

	// 4. Query analyzer with operation tracing
	fmt.Println("4. Query Analyzer with Operation Tracing:")
	config := dataframe.DebugConfig{
		Enabled:           true,
		LogLevel:          dataframe.LogLevelDebug,
		TrackMemory:       true,
		ProfileOperations: true,
		OutputFormat:      dataframe.OutputFormatText,
	}

	analyzer := dataframe.NewQueryAnalyzer(config)

	// Trace multiple operations
	fmt.Println("Tracing filter operation...")
	result1, err := analyzer.TraceOperation("filter_age", df, func() (*dataframe.DataFrame, error) {
		return df.Lazy().Filter(expr.Col("age").Gt(expr.Lit(35))).Collect()
	})
	if err != nil {
		log.Fatal(err)
	}
	defer result1.Release()

	fmt.Println("Tracing select operation...")
	result2, err := analyzer.TraceOperation("select_columns", result1, func() (*dataframe.DataFrame, error) {
		return result1.Select("name", "salary"), nil
	})
	if err != nil {
		log.Fatal(err)
	}
	defer result2.Release()

	fmt.Println("Tracing sort operation...")
	result3, err := analyzer.TraceOperation("sort_by_salary", result2, func() (*dataframe.DataFrame, error) {
		return result2.Sort("salary", false) // Sort descending
	})
	if err != nil {
		log.Fatal(err)
	}
	defer result3.Release()

	// Generate analysis report
	report := analyzer.GenerateReport()

	fmt.Println("=== Analysis Report ===")
	fmt.Printf("Total Operations: %d\n", report.Summary.TotalOperations)
	fmt.Printf("Total Duration: %v\n", report.Summary.TotalDuration)
	fmt.Printf("Total Memory Delta: %d bytes\n", report.Summary.TotalMemory)
	fmt.Printf("Parallel Operations: %d\n", report.Summary.ParallelOps)
	fmt.Println()

	if len(report.Bottlenecks) > 0 {
		fmt.Println("Bottlenecks identified:")
		for _, bottleneck := range report.Bottlenecks {
			fmt.Printf("- %s: %v (%s)\n", bottleneck.Operation, bottleneck.Duration, bottleneck.Reason)
		}
		fmt.Println()
	}

	if len(report.Suggestions) > 0 {
		fmt.Println("Optimization suggestions:")
		for _, suggestion := range report.Suggestions {
			fmt.Printf("- %s\n", suggestion)
		}
		fmt.Println()
	}

	// Show final result
	fmt.Println("=== Final Result ===")
	fmt.Printf("Final DataFrame has %d rows and %d columns\n", result3.Len(), result3.Width())
	fmt.Println("Result columns:", result3.Columns())

	// Demonstrate debug mode on DataFrame
	fmt.Println()
	fmt.Println("5. DataFrame Debug Mode:")
	debugDF := df.Debug()
	fmt.Printf("Debug DataFrame created with %d rows and %d columns\n", debugDF.Len(), debugDF.Width())

	fmt.Println()
	fmt.Println("=== Demo Complete ===")
}
