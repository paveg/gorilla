package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
)

func main() {
	var (
		demo      = flag.Bool("demo", false, "Run basic demo")
		benchmark = flag.Bool("benchmark", false, "Run benchmark tests")
	)
	flag.Parse()

	switch {
	case *demo:
		runDemo()
	case *benchmark:
		runBenchmark()
	default:
		fmt.Println("Gorilla DataFrame Library CLI")
		fmt.Println("Usage:")
		fmt.Println("  -demo       Run basic demonstration")
		fmt.Println("  -benchmark  Run benchmark tests")
		os.Exit(1)
	}
}

func runDemo() {
	fmt.Println("🦍 Gorilla DataFrame Library Demo")
	fmt.Println("================================")

	mem := memory.NewGoAllocator()

	// Create larger sample dataset
	fmt.Println("Creating sample dataset...")

	names := make([]string, 1000)
	ages := make([]int64, 1000)
	salaries := make([]float64, 1000)
	departments := make([]string, 1000)

	depts := []string{"Engineering", "Sales", "Marketing", "HR", "Finance"}

	for i := 0; i < 1000; i++ {
		names[i] = fmt.Sprintf("Employee_%d", i+1)
		ages[i] = int64(25 + (i % 40))             // Ages 25-64
		salaries[i] = float64(40000 + (i%60)*1000) // Salaries 40k-99k
		departments[i] = depts[i%len(depts)]
	}

	// Create Series
	nameSeries := series.New("name", names, mem)
	ageSeries := series.New("age", ages, mem)
	salarySeries := series.New("salary", salaries, mem)
	deptSeries := series.New("department", departments, mem)

	defer nameSeries.Release()
	defer ageSeries.Release()
	defer salarySeries.Release()
	defer deptSeries.Release()

	// Create DataFrame
	df := dataframe.New(nameSeries, ageSeries, salarySeries, deptSeries)
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
		Filter(expr.Col("age").Gt(expr.Lit(int64(35)))).
		WithColumn("bonus", expr.Col("salary").Mul(expr.Lit(0.1))).
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

func runBenchmark() {
	fmt.Println("🚀 Gorilla DataFrame Library Benchmark")
	fmt.Println("=====================================")

	// TODO: Implement comprehensive benchmark suite
	// Planned benchmarks:
	fmt.Println("TODO: Implement benchmark suite - would measure:")
	fmt.Println("- Series creation performance")
	fmt.Println("- DataFrame operations throughput")
	fmt.Println("- Lazy evaluation optimization")
	fmt.Println("- Parallel processing efficiency")
	fmt.Println("- Memory usage patterns")

	fmt.Println("\nBenchmark suite would run here...")
}
