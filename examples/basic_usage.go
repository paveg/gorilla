package main

import (
	"fmt"
	"log"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
)

func main() {
	// Gorilla is built on Apache Arrow, which requires an allocator for memory management.
	// It's good practice to create one at the start of your application.
	mem := memory.NewGoAllocator()

	// Create a new DataFrame.
	df := createDataFrame(mem)
	// Use defer to ensure Release() is called to free the memory.
	defer df.Release()

	fmt.Println("--- Original DataFrame ---")
	fmt.Println(df)

	// --- Basic Operations ---
	demonstrateBasics(df)

	// --- Lazy Evaluation Example ---
	demonstrateLazyEvaluation(df)

	// --- Chaining Multiple Operations ---
	demonstrateChaining(df)

	fmt.Println("\nGorilla DataFrame library basic usage complete!")
}

// createDataFrame builds a new DataFrame from scratch.
func createDataFrame(mem memory.Allocator) *dataframe.DataFrame {
	// A DataFrame is composed of one or more Series.
	// A Series is a single column of data with a name and a type.
	nameSeries := series.New("name", []string{"Alice", "Bob", "Charlie", "Diana", "Eve"}, mem)
	ageSeries := series.New("age", []int64{25, 30, 35, 28, 32}, mem)
	salarySeries := series.New("salary", []float64{50000.0, 60000.0, 75000.0, 55000.0, 65000.0}, mem)

	// IMPORTANT: Series and DataFrames hold memory managed by Arrow.
	// You must call Release() when you are done with them to avoid memory leaks.
	// Using defer in the calling function is a good way to manage this.

	return dataframe.New(nameSeries, ageSeries, salarySeries)
}

// demonstrateBasics shows some of the fundamental DataFrame operations.
func demonstrateBasics(df *dataframe.DataFrame) {
	fmt.Println("\n--- Basic DataFrame Info ---")
	fmt.Println("Column names:", df.Columns())
	fmt.Println("Dimensions (rows x cols):", df.Len(), "x", df.Width())

	// Select specific columns to create a new view of the data.
	selected := df.Select("name", "salary")
	defer selected.Release()

	fmt.Println("\n--- Selected Columns (name, salary) ---")
	fmt.Println(selected)
}

// demonstrateLazyEvaluation shows the power of lazy evaluation.
// Operations are built up into a plan, which is only executed when .Collect() is called.
func demonstrateLazyEvaluation(df *dataframe.DataFrame) {
	fmt.Println("\n--- Lazy Evaluation: Filter and Add a Column ---")

	// 1. Create a lazy frame.
	lazyDf := df.Lazy().
		// 2. Add a filter operation to the plan.
		Filter(expr.Col("age").Gt(expr.Lit(int64(30)))).
		// 3. Add a new column to the plan.
		WithColumn("bonus", expr.Col("salary").Mul(expr.Lit(0.1)))

	// 4. The plan is only executed when we call Collect().
	result, err := lazyDf.Collect()
	if err != nil {
		log.Fatal(err)
	}
	defer result.Release()

	fmt.Println("Result after filtering for age > 30 and adding a bonus column:")
	fmt.Println(result)
}

// demonstrateChaining shows how to chain multiple operations together fluently.
func demonstrateChaining(df *dataframe.DataFrame) {
	fmt.Println("\n--- Chaining: Filter and Select ---")

	// Find all employees younger than 35 and select their name and salary.
	lazyDf := df.Lazy().
		Filter(expr.Col("age").Lt(expr.Lit(int64(35)))).
		Select("name", "salary")

	result, err := lazyDf.Collect()
	if err != nil {
		log.Fatal(err)
	}
	defer result.Release()

	fmt.Println("Result of finding employees < 35:")
	fmt.Println(result)
}
