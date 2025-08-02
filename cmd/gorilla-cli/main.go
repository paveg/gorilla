package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla"
	"github.com/paveg/gorilla/internal/version"
)

func customUsage() {
	fmt.Fprintf(os.Stderr, "Gorilla DataFrame Library CLI (version %s)\n\n", version.Version)
	fmt.Fprintf(os.Stderr, "Usage: gorilla-cli [options]\n\n")
	fmt.Fprintf(os.Stderr, "Options:\n")
	fmt.Fprintf(os.Stderr, "  --demo\n\t\tRun basic demo\n")
	fmt.Fprintf(os.Stderr, "  --benchmark\n\t\tRun benchmark tests\n")
	fmt.Fprintf(os.Stderr, "  --rows N\n\t\tNumber of rows to use (default: 1000 for demo, 1000000 for benchmark)\n")
	fmt.Fprintf(os.Stderr, "  -v, --version\n\t\tPrint version information and exit\n")
	fmt.Fprintf(os.Stderr, "  -h, --help\n\t\tShow this help message and exit\n")
}

// Remove hardcoded version - use version package instead

func main() {
	// Define flags
	versionFlag := flag.Bool("v", false, "Print version and exit")
	flag.BoolVar(versionFlag, "version", false, "Print version and exit") // alias
	demoFlag := flag.Bool("demo", false, "Run basic demo")
	benchmarkFlag := flag.Bool("benchmark", false, "Run benchmark tests")
	rowsFlag := flag.Int("rows", 0, "Number of rows to use (default: 1000 for demo, 1000000 for benchmark)")

	// Customize usage message for -h, --help
	//nolint:reassign // Standard Go pattern for customizing flag usage message
	flag.Usage = customUsage

	flag.Parse()

	// Handle version flag
	if *versionFlag {
		fmt.Print(version.Info().String())
		return
	}

	// Handle other flags
	switch {
	case *demoFlag:
		runDemo(*rowsFlag)
	case *benchmarkFlag:
		runBenchmark(*rowsFlag)
	default:
		// If no flags are provided, print usage and exit.
		flag.Usage()
		os.Exit(1)
	}
}

func runDemo(rows int) {
	fmt.Println("🦍 Gorilla DataFrame Library Demo")
	fmt.Println("=================================")

	mem := memory.NewGoAllocator()

	// Create larger sample dataset
	fmt.Println("Creating sample dataset...")

	// Set default if not specified
	if rows == 0 {
		rows = 1000
	}
	sampleSize := rows

	const (
		baseAge            = 25
		ageRange           = 40
		baseSalary         = 40000
		salaryIncrement    = 1000
		salaryRange        = 60
		ageFilterThreshold = 35  // filter for employees older than this age
		bonusPercentage    = 0.1 // bonus as 10% of salary
	)
	names := make([]string, sampleSize)
	ages := make([]int64, sampleSize)
	salaries := make([]float64, sampleSize)
	departments := make([]string, sampleSize)

	depts := []string{"Engineering", "Sales", "Marketing", "HR", "Finance"}

	for i := range sampleSize {
		names[i] = fmt.Sprintf("Employee_%d", i+1)
		ages[i] = int64(baseAge + (i % ageRange))                           // Ages 25-64
		salaries[i] = float64(baseSalary + (i%salaryRange)*salaryIncrement) // Salaries 40k-99k
		departments[i] = depts[i%len(depts)]
	}

	// Create Series
	nameSeries := gorilla.NewSeries("name", names, mem)
	ageSeries := gorilla.NewSeries("age", ages, mem)
	salarySeries := gorilla.NewSeries("salary", salaries, mem)
	deptSeries := gorilla.NewSeries("department", departments, mem)

	defer nameSeries.Release()
	defer ageSeries.Release()
	defer salarySeries.Release()
	defer deptSeries.Release()

	// Create DataFrame
	df := gorilla.NewDataFrame(nameSeries, ageSeries, salarySeries, deptSeries)
	defer df.Release()

	fmt.Printf("Created DataFrame with %d rows and %d columns\n", df.Len(), df.Width())
	fmt.Println("Columns:", df.Columns())
	fmt.Println()

	// Demonstrate lazy operations
	fmt.Println("Applying lazy operations:")
	fmt.Println("1. Filter employees older than 35")
	fmt.Println("2. Add bonus column (10% of salary)")
	fmt.Println("3. Select specific columns")

	lazyDf := df.Lazy().
		Filter(gorilla.Col("age").Gt(gorilla.Lit(int64(ageFilterThreshold)))).
		WithColumn("bonus", gorilla.Col("salary").Mul(gorilla.Lit(bonusPercentage))).
		Select("name", "age", "salary", "bonus", "department")

	fmt.Println("\nLazy operations defined:")
	fmt.Println(lazyDf)

	// Execute operations
	fmt.Println("Executing lazy operations...")
	result, err := lazyDf.Collect()
	if err != nil {
		log.Printf("Error executing lazy operations: %v", err)
		return
	}
	defer result.Release()
	defer lazyDf.Release()

	fmt.Printf("Result: %d rows, %d columns\n", result.Len(), result.Width())
	fmt.Println("Demo completed successfully! 🎉")
}

//nolint:funlen // Benchmark function requires extensive setup and measurement code
func runBenchmark(rows int) {
	fmt.Println("🚀 Gorilla DataFrame Library Benchmark")
	fmt.Println("=====================================")

	// Set default if not specified
	if rows == 0 {
		rows = 1_000_000 // 1 million rows for benchmarking
	}
	numRows := rows

	const (
		baseAge            = 25
		ageRange           = 40
		baseSalary         = 40000
		salaryIncrement    = 1000
		salaryRange        = 60
		ageFilterThreshold = 35  // filter for employees older than this age
		bonusPercentage    = 0.1 // bonus as 10% of salary
	)
	mem := memory.NewGoAllocator()

	// --- Benchmark: Series Creation ---
	fmt.Printf("\nBenchmarking Series creation for %d rows...\n", numRows)
	start := time.Now()
	names := make([]string, numRows)
	ages := make([]int64, numRows)
	salaries := make([]float64, numRows)
	departments := make([]string, numRows)
	depts := []string{"Engineering", "Sales", "Marketing", "HR", "Finance"}

	for i := range numRows {
		names[i] = fmt.Sprintf("Employee_%d", i+1)
		ages[i] = int64(baseAge + (i % ageRange))
		salaries[i] = float64(baseSalary + (i%salaryRange)*salaryIncrement)
		departments[i] = depts[i%len(depts)]
	}

	nameSeries := gorilla.NewSeries("name", names, mem)
	ageSeries := gorilla.NewSeries("age", ages, mem)
	salarySeries := gorilla.NewSeries("salary", salaries, mem)
	deptSeries := gorilla.NewSeries("department", departments, mem)
	seriesCreationTime := time.Since(start)
	fmt.Printf("Series Creation Time: %s\n", seriesCreationTime)

	// --- Benchmark: DataFrame Creation ---
	fmt.Printf("\nBenchmarking DataFrame creation for %d rows...\n", numRows)
	start = time.Now()
	df := gorilla.NewDataFrame(nameSeries, ageSeries, salarySeries, deptSeries)
	dfCreationTime := time.Since(start)
	fmt.Printf("DataFrame Creation Time: %s\n", dfCreationTime)

	// --- Benchmark: Lazy Evaluation (Filter, WithColumn, Select, Collect) ---
	fmt.Printf("\nBenchmarking Lazy Evaluation (Filter, WithColumn, Select, Collect) for %d rows...\n", numRows)
	start = time.Now()
	lazyDf := df.Lazy().
		Filter(gorilla.Col("age").Gt(gorilla.Lit(int64(ageFilterThreshold)))).
		WithColumn("bonus", gorilla.Col("salary").Mul(gorilla.Lit(bonusPercentage))).
		Select("name", "age", "salary", "bonus", "department")

	result, err := lazyDf.Collect()
	if err != nil {
		// Clean up all resources before exit
		lazyDf.Release()
		df.Release()
		nameSeries.Release()
		ageSeries.Release()
		salarySeries.Release()
		deptSeries.Release()
		log.Printf("Error during lazy evaluation benchmark: %v", err)
		os.Exit(1)
	}

	// Ensure Series and DataFrame are released after use
	defer nameSeries.Release()
	defer ageSeries.Release()
	defer salarySeries.Release()
	defer deptSeries.Release()
	defer df.Release()
	defer result.Release()
	defer lazyDf.Release()

	lazyEvalTime := time.Since(start)
	fmt.Printf("Lazy Evaluation Time: %s\n", lazyEvalTime)

	fmt.Println("\nBenchmark suite completed successfully! 🎉")
}
