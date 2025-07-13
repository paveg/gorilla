package main

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla"
)

func main() {
	mem := memory.NewGoAllocator()

	// Create sample sales data
	salesData := []struct {
		region   string
		product  string
		quantity int64
		price    float64
	}{
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
	df := gorilla.NewDataFrame(
		gorilla.NewSeries("region", regions, mem),
		gorilla.NewSeries("product", products, mem),
		gorilla.NewSeries("quantity", quantities, mem),
		gorilla.NewSeries("price", prices, mem),
	)

	fmt.Println("Original Sales Data:")
	fmt.Println(df.String())
	fmt.Println()

	// Example 1: Group by region and sum quantities
	fmt.Println("=== Example 1: Sales by Region ===")
	regionSales := df.GroupBy("region").Agg(
		gorilla.Sum(gorilla.Col("quantity")).As("total_quantity"),
		gorilla.Count(gorilla.Col("product")).As("num_products"),
	)
	fmt.Println(regionSales.String())
	fmt.Println()

	// Example 2: Group by product and calculate statistics
	fmt.Println("=== Example 2: Product Statistics ===")
	productStats := df.GroupBy("product").Agg(
		gorilla.Sum(gorilla.Col("quantity")).As("total_sold"),
		gorilla.Mean(gorilla.Col("quantity")).As("avg_quantity"),
		gorilla.Count(gorilla.Col("region")).As("regions_sold"),
	)
	fmt.Println(productStats.String())
	fmt.Println()

	// Example 3: Group by multiple columns
	fmt.Println("=== Example 3: Region-Product Breakdown ===")
	regionProductStats := df.GroupBy("region", "product").Agg(
		gorilla.Sum(gorilla.Col("quantity")).As("total_quantity"),
		gorilla.Max(gorilla.Col("price")).As("unit_price"),
	)
	fmt.Println(regionProductStats.String())
	fmt.Println()

	// Example 4: Using lazy evaluation with groupby
	fmt.Println("=== Example 4: Lazy GroupBy with Filtering ===")
	// Filter high-value products (price > 100) then group by region
	highValueSales, err := df.Lazy().
		Filter(gorilla.Col("price").Gt(gorilla.Lit(100.0))).
		GroupBy("region").
		Agg(
			gorilla.Sum(gorilla.Col("quantity")).As("high_value_quantity"),
			gorilla.Mean(gorilla.Col("price")).As("avg_price"),
		).
		Collect()

	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println(highValueSales.String())
	fmt.Println()

	// Example 5: Using convenience methods
	fmt.Println("=== Example 5: Using Convenience Methods ===")
	simpleSums, err := df.Lazy().
		GroupBy("region").
		Sum("quantity").
		Collect()

	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println(simpleSums.String())

	// Clean up memory
	df.Release()
	regionSales.Release()
	productStats.Release()
	regionProductStats.Release()
	highValueSales.Release()
	simpleSums.Release()
}
