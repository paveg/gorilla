package main

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla"
)

// Constants for demo filtering.
const (
	highValuePriceThreshold = 100.0 // price threshold for high-value products
)

// SalesData represents a sales record.
type SalesData struct {
	region   string
	product  string
	quantity int64
	price    float64
}

// createSampleData generates sample sales data for the examples.
func createSampleData() []SalesData {
	return []SalesData{
		{"North", "Laptop", 10, 1200.0},
		{"South", "Laptop", 8, 1200.0},
		{"North", "Mouse", 50, 25.0},
		{"South", "Mouse", 30, 25.0},
		{"North", "Keyboard", 20, 75.0},
		{"South", "Keyboard", 15, 75.0},
		{"North", "Laptop", 5, 1200.0},
		{"South", "Monitor", 12, 300.0},
		{"North", "Monitor", 18, 300.0},
	}
}

// createDataFrame creates a DataFrame from the sample sales data.
func createDataFrame(salesData []SalesData, mem memory.Allocator) *gorilla.DataFrame {
	// Extract data into slices
	regions := make([]string, len(salesData))
	products := make([]string, len(salesData))
	quantities := make([]int64, len(salesData))
	prices := make([]float64, len(salesData))

	for i, sale := range salesData {
		regions[i] = sale.region
		products[i] = sale.product
		quantities[i] = sale.quantity
		prices[i] = sale.price
	}

	// Create DataFrame
	return gorilla.NewDataFrame(
		gorilla.NewSeries("region", regions, mem),
		gorilla.NewSeries("product", products, mem),
		gorilla.NewSeries("quantity", quantities, mem),
		gorilla.NewSeries("price", prices, mem),
	)
}

// runExample1 demonstrates grouping by region and summing quantities.
func runExample1(df *gorilla.DataFrame) *gorilla.DataFrame {
	fmt.Println("=== Example 1: Sales by Region ===")
	regionSales := df.GroupBy("region").Agg(
		gorilla.Sum(gorilla.Col("quantity")).As("total_quantity"),
		gorilla.Count(gorilla.Col("product")).As("num_products"),
	)
	fmt.Println(regionSales.String())
	fmt.Println()
	return regionSales
}

// runExample2 demonstrates grouping by product and calculating statistics.
func runExample2(df *gorilla.DataFrame) *gorilla.DataFrame {
	fmt.Println("=== Example 2: Product Statistics ===")
	productStats := df.GroupBy("product").Agg(
		gorilla.Sum(gorilla.Col("quantity")).As("total_sold"),
		gorilla.Mean(gorilla.Col("quantity")).As("avg_quantity"),
		gorilla.Count(gorilla.Col("region")).As("regions_sold"),
	)
	fmt.Println(productStats.String())
	fmt.Println()
	return productStats
}

// runExample3 demonstrates grouping by multiple columns.
func runExample3(df *gorilla.DataFrame) *gorilla.DataFrame {
	fmt.Println("=== Example 3: Region-Product Breakdown ===")
	regionProductStats := df.GroupBy("region", "product").Agg(
		gorilla.Sum(gorilla.Col("quantity")).As("total_quantity"),
		gorilla.Max(gorilla.Col("price")).As("unit_price"),
	)
	fmt.Println(regionProductStats.String())
	fmt.Println()
	return regionProductStats
}

// runExample4 demonstrates lazy evaluation with groupby and filtering.
func runExample4(df *gorilla.DataFrame) (*gorilla.DataFrame, error) {
	fmt.Println("=== Example 4: Lazy GroupBy with Filtering ===")
	// Filter high-value products (price > 100) then group by region
	highValueSales, err := df.Lazy().
		Filter(gorilla.Col("price").Gt(gorilla.Lit(highValuePriceThreshold))).
		GroupBy("region").
		Agg(
			gorilla.Sum(gorilla.Col("quantity")).As("high_value_quantity"),
			gorilla.Mean(gorilla.Col("price")).As("avg_price"),
		).
		Collect()

	if err != nil {
		return nil, err
	}

	fmt.Println(highValueSales.String())
	fmt.Println()
	return highValueSales, nil
}

// runExample5 demonstrates using convenience methods.
func runExample5(df *gorilla.DataFrame) (*gorilla.DataFrame, error) {
	fmt.Println("=== Example 5: Using Convenience Methods ===")
	simpleSums, err := df.Lazy().
		GroupBy("region").
		Sum("quantity").
		Collect()

	if err != nil {
		return nil, err
	}

	fmt.Println(simpleSums.String())
	return simpleSums, nil
}

func main() {
	mem := memory.NewGoAllocator()

	// Create sample data and DataFrame
	salesData := createSampleData()
	df := createDataFrame(salesData, mem)
	defer df.Release()

	fmt.Println("Original Sales Data:")
	fmt.Println(df.String())
	fmt.Println()

	// Run examples
	regionSales := runExample1(df)
	defer regionSales.Release()

	productStats := runExample2(df)
	defer productStats.Release()

	regionProductStats := runExample3(df)
	defer regionProductStats.Release()

	highValueSales, err := runExample4(df)
	if err != nil {
		fmt.Printf("Error in Example 4: %v\n", err)
		return
	}
	defer highValueSales.Release()

	simpleSums, err := runExample5(df)
	if err != nil {
		fmt.Printf("Error in Example 5: %v\n", err)
		return
	}
	defer simpleSums.Release()
}
