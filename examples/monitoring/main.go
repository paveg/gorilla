// Package main demonstrates the performance monitoring and metrics functionality.
package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/monitoring"
	"github.com/paveg/gorilla/internal/series"
)

const (
	headerSeparatorLength = 50
	baseAge               = 25
	ageRange              = 40
	baseSalary            = 50000.0
	salaryVariance        = 100
	salaryMultiplier      = 1000.0
	highSalaryThreshold   = 75000.0
	ageFilterThreshold    = 30
	highSalaryCategory    = 80000.0
	filterThreshold       = 500
	monitoringPort        = 8080
	serverStartDelay      = 100
	dashSeparatorLength   = 30
	bytesToMB             = 1024 * 1024
)

func main() {
	fmt.Println("Gorilla DataFrame Performance Monitoring Example")
	fmt.Println(strings.Repeat("=", headerSeparatorLength))

	// Enable global monitoring
	monitoring.EnableGlobalMonitoring()
	defer monitoring.DisableGlobalMonitoring()

	// Create a sample DataFrame
	mem := memory.NewGoAllocator()

	// Generate sample data
	size := 10000
	names := make([]string, size)
	ages := make([]int64, size)
	salaries := make([]float64, size)

	for i := range size {
		names[i] = fmt.Sprintf("Employee_%d", i)
		ages[i] = int64(baseAge + (i % ageRange))                             // Ages 25-64
		salaries[i] = baseSalary + float64(i%salaryVariance)*salaryMultiplier // Salaries 50k-149k
	}

	nameSeries := series.New("name", names, mem)
	ageSeries := series.New("age", ages, mem)
	salarySeries := series.New("salary", salaries, mem)

	df := dataframe.New(nameSeries, ageSeries, salarySeries)
	defer df.Release()

	fmt.Printf("Created DataFrame with %d rows and %d columns\n", df.Len(), df.Width())

	// Demonstrate various operations with monitoring
	runDataFrameOperations(df)

	// Create and run benchmark suite
	runBenchmarkSuite(mem)

	// Start monitoring server (optional)
	startMonitoringServer()

	// Display collected metrics
	displayMetrics()
}

func runDataFrameOperations(df *dataframe.DataFrame) {
	fmt.Println("\nRunning DataFrame Operations with Monitoring...")

	// Operation 1: Filter
	err := monitoring.RecordGlobalOperation("filter_high_salary", func() error {
		result, err := df.Lazy().
			Filter(expr.Col("salary").Gt(expr.Lit(highSalaryThreshold))).
			Collect()
		if err != nil {
			return err
		}
		defer result.Release()
		fmt.Printf("  - Filtered to %d high-salary employees\n", result.Len())
		return nil
	})
	if err != nil {
		log.Printf("Filter operation failed: %v", err)
	}

	// Operation 2: Group By and Aggregation
	err = monitoring.RecordGlobalOperation("groupby_age", func() error {
		result, groupbyErr := df.Lazy().
			GroupBy("age").
			Agg(
				expr.Count(expr.Col("name")).As("count"),
				expr.Mean(expr.Col("salary")).As("avg_salary"),
			).
			Collect()
		if groupbyErr != nil {
			return groupbyErr
		}
		defer result.Release()
		fmt.Printf("  - Grouped by age: %d age groups\n", result.Len())
		return nil
	})
	if err != nil {
		log.Printf("GroupBy operation failed: %v", err)
	}

	// Operation 3: Sort
	err = monitoring.RecordGlobalOperation("sort_by_salary", func() error {
		result, sortErr := df.Sort("salary", false) // Descending order
		if sortErr != nil {
			return sortErr
		}
		defer result.Release()
		fmt.Printf("  - Sorted %d employees by salary\n", result.Len())
		return nil
	})
	if err != nil {
		log.Printf("Sort operation failed: %v", err)
	}

	// Operation 4: Complex Query
	err = monitoring.RecordGlobalOperation("complex_query", func() error {
		result, queryErr := df.Lazy().
			Filter(expr.Col("age").Gt(expr.Lit(int64(ageFilterThreshold)))).
			WithColumn("salary_category",
				expr.If(
					expr.Col("salary").Gt(expr.Lit(highSalaryCategory)),
					expr.Lit("High"),
					expr.Lit("Standard"),
				),
			).
			GroupBy("salary_category").
			Agg(
				expr.Count(expr.Col("name")).As("count"),
				expr.Mean(expr.Col("salary")).As("avg_salary"),
				expr.Max(expr.Col("age")).As("max_age"),
			).
			Collect()
		if queryErr != nil {
			return queryErr
		}
		defer result.Release()
		fmt.Printf("  - Complex query result: %d categories\n", result.Len())
		return nil
	})
	if err != nil {
		log.Printf("Complex query failed: %v", err)
	}
}

func runBenchmarkSuite(mem memory.Allocator) {
	fmt.Println("\nRunning Benchmark Suite...")

	suite := monitoring.NewBenchmarkSuite()

	// Benchmark 1: DataFrame creation
	suite.AddQuickScenario("create_dataframe", "Create DataFrame with 1000 rows", func() error {
		size := 1000
		data := make([]int64, size)
		for i := range size {
			data[i] = int64(i)
		}

		testSeries := series.New("test", data, mem)
		testDF := dataframe.New(testSeries)
		defer testDF.Release()

		return nil
	})

	// Benchmark 2: Filter operation
	suite.AddQuickScenario("filter_operation", "Filter DataFrame operation", func() error {
		size := 1000
		data := make([]int64, size)
		for i := range size {
			data[i] = int64(i)
		}

		testSeries := series.New("values", data, mem)
		testDF := dataframe.New(testSeries)
		defer testDF.Release()

		result, err := testDF.Lazy().
			Filter(expr.Col("values").Gt(expr.Lit(int64(filterThreshold)))).
			Collect()
		if err != nil {
			return err
		}
		defer result.Release()

		return nil
	})

	// Benchmark 3: Sort operation
	suite.AddQuickScenario("sort_operation", "Sort DataFrame operation", func() error {
		size := 1000
		data := make([]int64, size)
		for i := range size {
			data[i] = int64(size - i) // Reverse order
		}

		testSeries := series.New("values", data, mem)
		testDF := dataframe.New(testSeries)
		defer testDF.Release()

		result, err := testDF.Sort("values", true)
		if err != nil {
			return err
		}
		defer result.Release()

		return nil
	})

	// Run benchmarks
	results := suite.Run()

	fmt.Printf("  - Completed %d benchmark scenarios\n", len(results))

	// Generate and display report
	report := suite.GenerateReport()
	fmt.Println("\nBenchmark Report:")
	fmt.Println(report)
}

func startMonitoringServer() {
	fmt.Println("\nStarting Monitoring Server...")

	collector := monitoring.GetGlobalCollector()
	if collector == nil {
		fmt.Println("  - No global collector available")
		return
	}

	server := monitoring.NewMonitoringServer(collector, monitoringPort)

	// Start server in background
	go func() {
		if err := server.Start(); err != nil {
			log.Printf("Monitoring server error: %v", err)
		}
	}()

	fmt.Println("  - Monitoring server started on http://localhost:8080")
	fmt.Println("  - Available endpoints:")
	fmt.Println("    - http://localhost:8080/metrics (JSON metrics)")
	fmt.Println("    - http://localhost:8080/health (Health check)")
	fmt.Println("    - http://localhost:8080/dashboard (HTML dashboard)")

	// Give server time to start
	time.Sleep(serverStartDelay * time.Millisecond)
}

func displayMetrics() {
	fmt.Println("\nCollected Metrics Summary:")
	fmt.Println(strings.Repeat("-", dashSeparatorLength))

	summary := monitoring.GetGlobalSummary()

	fmt.Printf("Total Operations: %d\n", summary.TotalOperations)
	fmt.Printf("Total Duration: %v\n", summary.TotalDuration)
	fmt.Printf("Average Duration: %v\n", summary.AverageDuration)
	fmt.Printf("Total Memory Used: %d bytes (%.2f MB)\n",
		summary.TotalMemory, float64(summary.TotalMemory)/bytesToMB)
	fmt.Printf("Total Rows Processed: %d\n", summary.TotalRows)

	if len(summary.OperationCounts) > 0 {
		fmt.Println("\nOperations by Type:")
		for operation, count := range summary.OperationCounts {
			fmt.Printf("  - %s: %d operations\n", operation, count)
		}
	}

	// Display individual metrics
	metrics := monitoring.GetGlobalMetrics()
	if len(metrics) > 0 {
		fmt.Println("\nDetailed Operations:")
		for i, metric := range metrics {
			fmt.Printf("  %d. %s: %v (Memory: %d bytes)\n",
				i+1, metric.Operation, metric.Duration, metric.MemoryUsed)
		}
	}
}
