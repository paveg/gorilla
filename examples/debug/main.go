// +build ignore

package main

import (
	"fmt"
	"log"
	"os"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
)

func main() {
	fmt.Println("=== Debug Mode and Execution Plan Visualization Demo ===")
	fmt.Println()

	df := createSampleDataFrame()
	defer df.Release()

	fmt.Printf("Created DataFrame with %d rows and %d columns\n", df.Len(), df.Width())
	fmt.Println("Columns:", df.Columns())
	fmt.Println()

	demonstrateDebugFeatures(df)
	fmt.Println("=== Demo Complete ===")
}

func createSampleDataFrame() *dataframe.DataFrame {
	mem := memory.NewGoAllocator()

	names := []string{"Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry"}
	ages := []int64{25, 30, 35, 40, 45, 50, 55, 60}
	salaries := []float64{50000, 60000, 70000, 80000, 90000, 100000, 110000, 120000}

	namesSeries, err := series.NewSafe("name", names, mem)
	if err != nil {
		log.Printf("Error creating name series: %v", err)
		os.Exit(1)
	}

	agesSeries, err := series.NewSafe("age", ages, mem)
	if err != nil {
		namesSeries.Release()
		log.Printf("Error creating age series: %v", err)
		os.Exit(1)
	}

	salariesSeries, err := series.NewSafe("salary", salaries, mem)
	if err != nil {
		namesSeries.Release()
		agesSeries.Release()
		log.Printf("Error creating salary series: %v", err)
		os.Exit(1)
	}

	defer namesSeries.Release()
	defer agesSeries.Release()
	defer salariesSeries.Release()

	return dataframe.New(namesSeries, agesSeries, salariesSeries)
}

func demonstrateDebugFeatures(df *dataframe.DataFrame) {
	fmt.Println("=== Debug Mode Features ===")
	fmt.Println()

	showBasicExecutionPlan(df)
	showAnalyzedExecutionPlan(df)
	showJSONRepresentation(df)
	demonstrateQueryTracing(df)
}

func showBasicExecutionPlan(df *dataframe.DataFrame) {
	fmt.Println("1. Basic Execution Plan:")
	lazyFrame := createLazyFrame(df)
	plan := lazyFrame.Explain()
	fmt.Print(plan.RenderText())
	fmt.Println()
}

func showAnalyzedExecutionPlan(df *dataframe.DataFrame) {
	fmt.Println("2. Execution Plan with Analysis:")
	lazyFrame := createLazyFrame(df)
	analyzedPlan, err := lazyFrame.ExplainAnalyze()
	if err != nil {
		log.Printf("Error analyzing execution plan: %v", err)
		os.Exit(1)
	}
	fmt.Print(analyzedPlan.RenderText())
	fmt.Println()
}

func showJSONRepresentation(df *dataframe.DataFrame) {
	fmt.Println("3. JSON Representation:")
	lazyFrame := createLazyFrame(df)
	plan := lazyFrame.Explain()
	jsonPlan, err := plan.RenderJSON()
	if err != nil {
		log.Printf("Error rendering JSON plan: %v", err)
		os.Exit(1)
	}
	fmt.Println(string(jsonPlan))
	fmt.Println()
}

func createLazyFrame(df *dataframe.DataFrame) *dataframe.LazyFrame {
	const (
		ageThreshold    = 30
		salaryThreshold = 100000
	)
	return df.Lazy().
		Filter(expr.Col("age").Gt(expr.Lit(ageThreshold))).
		Select("name", "salary").
		Filter(expr.Col("salary").Lt(expr.Lit(salaryThreshold)))
}

func demonstrateQueryTracing(df *dataframe.DataFrame) {
	fmt.Println("4. Query Analyzer with Operation Tracing:")
	config := dataframe.DebugConfig{
		Enabled:           true,
		LogLevel:          dataframe.LogLevelDebug,
		TrackMemory:       true,
		ProfileOperations: true,
		OutputFormat:      dataframe.OutputFormatText,
	}

	analyzer := dataframe.NewQueryAnalyzer(config)
	result3 := executeTracedOperations(analyzer, df)
	defer result3.Release()

	showAnalysisReport(analyzer)
	showFinalResults(result3, df)
}

func executeTracedOperations(analyzer *dataframe.QueryAnalyzer, df *dataframe.DataFrame) *dataframe.DataFrame {
	const ageFilter = 35

	fmt.Println("Tracing filter operation...")
	result1, err := analyzer.TraceOperation("filter_age", df, func() (*dataframe.DataFrame, error) {
		return df.Lazy().Filter(expr.Col("age").Gt(expr.Lit(ageFilter))).Collect()
	})
	if err != nil {
		log.Printf("Error tracing filter operation: %v", err)
		os.Exit(1)
	}

	fmt.Println("Tracing select operation...")
	result2, err := analyzer.TraceOperation("select_columns", result1, func() (*dataframe.DataFrame, error) {
		return result1.Select("name", "salary"), nil
	})
	if err != nil {
		result1.Release()
		log.Printf("Error tracing select operation: %v", err)
		os.Exit(1)
	}

	fmt.Println("Tracing sort operation...")
	result3, err := analyzer.TraceOperation("sort_by_salary", result2, func() (*dataframe.DataFrame, error) {
		return result2.Sort("salary", false) // Sort descending
	})
	if err != nil {
		result1.Release()
		result2.Release()
		log.Printf("Error tracing sort operation: %v", err)
		os.Exit(1)
	}

	defer result1.Release()
	defer result2.Release()

	return result3
}

func showAnalysisReport(analyzer *dataframe.QueryAnalyzer) {
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
}

func showFinalResults(result3 *dataframe.DataFrame, df *dataframe.DataFrame) {
	fmt.Println("=== Final Result ===")
	fmt.Printf("Final DataFrame has %d rows and %d columns\n", result3.Len(), result3.Width())
	fmt.Println("Result columns:", result3.Columns())

	fmt.Println()
	fmt.Println("5. DataFrame Debug Mode:")
	debugDF := df.Debug()
	fmt.Printf("Debug DataFrame created with %d rows and %d columns\n", debugDF.Len(), debugDF.Width())
	fmt.Println()
}
