package main

import (
	"fmt"
	"log"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/paveg/gorilla/dataframe"
	"github.com/paveg/gorilla/expr"
	"github.com/paveg/gorilla/series"
)

func main() {
	// Initialize memory allocator
	mem := memory.NewGoAllocator()

	// Create sample data
	names := []string{"Alice", "Bob", "Charlie", "Diana", "Eve"}
	ages := []int64{25, 30, 35, 28, 32}
	salaries := []float64{50000.0, 60000.0, 75000.0, 55000.0, 65000.0}

	// Create Series
	nameSeries := series.New("name", names, mem)
	ageSeries := series.New("age", ages, mem)
	salarySeries := series.New("salary", salaries, mem)

	// Ensure proper cleanup
	defer nameSeries.Release()
	defer ageSeries.Release()
	defer salarySeries.Release()

	// Create DataFrame
	df := dataframe.New(nameSeries, ageSeries, salarySeries)
	defer df.Release()

	fmt.Println("Original DataFrame:")
	fmt.Println(df)
	fmt.Println()

	// Demonstrate basic operations
	fmt.Println("Column names:", df.Columns())
	fmt.Println("DataFrame dimensions:", df.Len(), "x", df.Width())
	fmt.Println()

	// Select specific columns
	selected := df.Select("name", "salary")
	defer selected.Release()

	fmt.Println("Selected columns (name, salary):")
	fmt.Println(selected)
	fmt.Println()

	// Demonstrate lazy evaluation
	fmt.Println("Creating lazy frame with operations...")
	lazyDf := df.Lazy().
		Filter(expr.Col("age").Gt(expr.Lit(int64(30)))).
		WithColumn("bonus", expr.Col("salary").Mul(expr.Lit(0.1))).
		Select("name", "age", "salary", "bonus")

	fmt.Println("Lazy frame (before execution):")
	fmt.Println(lazyDf)
	fmt.Println()

	// Execute lazy operations
	fmt.Println("Collecting results...")
	result, err := lazyDf.Collect()
	if err != nil {
		log.Fatal(err)
	}
	defer result.Release()
	defer lazyDf.Release()

	fmt.Println("Result after lazy evaluation:")
	fmt.Println(result)
	fmt.Println()

	// Demonstrate Series operations
	fmt.Println("Series examples:")
	fmt.Printf("Name series: %s\n", nameSeries)
	fmt.Printf("First name: %v\n", nameSeries.Value(0))
	fmt.Printf("Age series length: %d\n", ageSeries.Len())
	fmt.Printf("Salary values: %v\n", salarySeries.Values())

	fmt.Println("\nGorilla DataFrame library basic usage complete!")
}
