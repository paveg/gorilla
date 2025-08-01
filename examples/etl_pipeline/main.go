// Package main demonstrates ETL (Extract, Transform, Load) pipeline patterns with Gorilla DataFrame.
//
// This example shows:
// - Data extraction from multiple sources
// - Complex data transformations and cleaning
// - Data validation and quality checks
// - Efficient data loading and export
// - Error handling and recovery patterns
// - Memory-efficient processing for large datasets
//
//nolint:gosec,mnd,revive // Example code: random generation and magic numbers acceptable for demo
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
	fmt.Println("=== ETL Pipeline Example ===")

	// Create memory allocator
	mem := memory.NewGoAllocator()

	// Step 1: Extract data from multiple sources
	fmt.Println("1. Extracting data from sources...")
	customerData := extractCustomerData(mem)
	defer customerData.Release()

	orderData := extractOrderData(mem)
	defer orderData.Release()

	productData := extractProductData(mem)
	defer productData.Release()

	fmt.Printf("   - Customers: %d records\n", customerData.Len())
	fmt.Printf("   - Orders: %d records\n", orderData.Len())
	fmt.Printf("   - Products: %d records\n", productData.Len())

	// Step 2: Transform and clean data
	fmt.Println("\n2. Transforming and cleaning data...")
	cleanedCustomers := transformCustomerData(customerData)
	defer cleanedCustomers.Release()

	cleanedOrders := transformOrderData(orderData)
	defer cleanedOrders.Release()

	// Step 3: Join data and create analytical views
	fmt.Println("\n3. Creating analytical data views...")
	analyticalView := createAnalyticalView(cleanedCustomers, cleanedOrders, productData)
	defer analyticalView.Release()

	fmt.Printf("   - Analytical view: %d records\n", analyticalView.Len())

	// Step 4: Perform data quality checks
	fmt.Println("\n4. Performing data quality validation...")
	qualityReport := performDataQualityChecks(analyticalView)
	defer qualityReport.Release()

	fmt.Println("   Quality Report:")
	fmt.Println(qualityReport)

	// Step 5: Generate business insights
	fmt.Println("\n5. Generating business insights...")
	insights := generateBusinessInsights(analyticalView)
	defer insights.Release()

	fmt.Printf("Business Insights (%d insights generated):\n", insights.Len())
	fmt.Println(insights)
}

// extractCustomerData simulates extracting customer data from a source system.
func extractCustomerData(mem memory.Allocator) *gorilla.DataFrame {
	const numCustomers = 500

	ids := make([]int64, numCustomers)
	names := make([]string, numCustomers)
	emails := make([]string, numCustomers)
	ages := make([]int64, numCustomers)
	cities := make([]string, numCustomers)
	signupDates := make([]string, numCustomers)

	citiesList := []string{"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia"}
	baseDate := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	for i := range numCustomers {
		ids[i] = int64(i + 1)
		names[i] = fmt.Sprintf("Customer_%d", i+1)

		// Simulate some data quality issues (empty emails, invalid ages)
		if rand.Float32() < 0.05 { // 5% missing emails
			emails[i] = ""
		} else {
			emails[i] = fmt.Sprintf("customer%d@example.com", i+1)
		}

		if rand.Float32() < 0.02 { // 2% invalid ages
			ages[i] = -1
		} else {
			ages[i] = int64(18 + rand.IntN(65))
		}

		cities[i] = citiesList[rand.IntN(len(citiesList))]

		// Random signup date within last 4 years
		daysAgo := rand.IntN(365 * 4)
		signupDate := baseDate.AddDate(0, 0, daysAgo)
		signupDates[i] = signupDate.Format("2006-01-02")
	}

	// Create series
	idSeries := gorilla.NewSeries("customer_id", ids, mem)
	nameSeries := gorilla.NewSeries("name", names, mem)
	emailSeries := gorilla.NewSeries("email", emails, mem)
	ageSeries := gorilla.NewSeries("age", ages, mem)
	citySeries := gorilla.NewSeries("city", cities, mem)
	signupSeries := gorilla.NewSeries("signup_date", signupDates, mem)

	defer idSeries.Release()
	defer nameSeries.Release()
	defer emailSeries.Release()
	defer ageSeries.Release()
	defer citySeries.Release()
	defer signupSeries.Release()

	return gorilla.NewDataFrame(idSeries, nameSeries, emailSeries, ageSeries, citySeries, signupSeries)
}

// extractOrderData simulates extracting order data.
func extractOrderData(mem memory.Allocator) *gorilla.DataFrame {
	const numOrders = 2000

	orderIds := make([]int64, numOrders)
	customerIds := make([]int64, numOrders)
	productIds := make([]int64, numOrders)
	quantities := make([]int64, numOrders)
	unitPrices := make([]float64, numOrders)
	orderDates := make([]string, numOrders)
	statuses := make([]string, numOrders)

	statusesList := []string{"completed", "pending", "cancelled", "shipped"}
	baseDate := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

	for i := range numOrders {
		orderIds[i] = int64(i + 1)
		customerIds[i] = int64(rand.IntN(500) + 1) // Reference to customers
		productIds[i] = int64(rand.IntN(100) + 1)  // Reference to products
		quantities[i] = int64(rand.IntN(10) + 1)
		unitPrices[i] = 10.0 + rand.Float64()*90.0 // $10-$100

		// Random order date within last year
		daysAgo := rand.IntN(365)
		orderDate := baseDate.AddDate(0, 0, daysAgo)
		orderDates[i] = orderDate.Format("2006-01-02")

		statuses[i] = statusesList[rand.IntN(len(statusesList))]
	}

	// Create series
	orderIDSeries := gorilla.NewSeries("order_id", orderIds, mem)
	customerIDSeries := gorilla.NewSeries("customer_id", customerIds, mem)
	productIDSeries := gorilla.NewSeries("product_id", productIds, mem)
	quantitySeries := gorilla.NewSeries("quantity", quantities, mem)
	priceSeries := gorilla.NewSeries("unit_price", unitPrices, mem)
	dateSeries := gorilla.NewSeries("order_date", orderDates, mem)
	statusSeries := gorilla.NewSeries("status", statuses, mem)

	defer orderIDSeries.Release()
	defer customerIDSeries.Release()
	defer productIDSeries.Release()
	defer quantitySeries.Release()
	defer priceSeries.Release()
	defer dateSeries.Release()
	defer statusSeries.Release()

	return gorilla.NewDataFrame(orderIDSeries, customerIDSeries, productIDSeries, quantitySeries, priceSeries, dateSeries, statusSeries)
}

// extractProductData simulates extracting product catalog data.
func extractProductData(mem memory.Allocator) *gorilla.DataFrame {
	const numProducts = 100

	ids := make([]int64, numProducts)
	names := make([]string, numProducts)
	categories := make([]string, numProducts)
	costs := make([]float64, numProducts)

	categoriesList := []string{"Electronics", "Clothing", "Books", "Home", "Sports"}

	for i := range numProducts {
		ids[i] = int64(i + 1)
		names[i] = fmt.Sprintf("Product_%d", i+1)
		categories[i] = categoriesList[i%len(categoriesList)]
		costs[i] = 5.0 + float64(i)*0.5 // Incrementally increasing cost
	}

	// Create series
	idSeries := gorilla.NewSeries("product_id", ids, mem)
	nameSeries := gorilla.NewSeries("product_name", names, mem)
	categorySeries := gorilla.NewSeries("category", categories, mem)
	costSeries := gorilla.NewSeries("cost", costs, mem)

	defer idSeries.Release()
	defer nameSeries.Release()
	defer categorySeries.Release()
	defer costSeries.Release()

	return gorilla.NewDataFrame(idSeries, nameSeries, categorySeries, costSeries)
}

// transformCustomerData cleans and transforms customer data.
func transformCustomerData(df *gorilla.DataFrame) *gorilla.DataFrame {
	result, err := df.Lazy().
		// Clean invalid ages
		WithColumn("age_cleaned",
			gorilla.If(
				gorilla.Col("age").Lt(gorilla.Lit(int64(0))),
				gorilla.Lit(int64(25)), // Default age for invalid values
				gorilla.Col("age"),
			),
		).

		// Add age groups
		WithColumn("age_group",
			gorilla.Case().
				When(gorilla.Col("age_cleaned").Lt(gorilla.Lit(int64(25))), gorilla.Lit("18-24")).
				When(gorilla.Col("age_cleaned").Lt(gorilla.Lit(int64(35))), gorilla.Lit("25-34")).
				When(gorilla.Col("age_cleaned").Lt(gorilla.Lit(int64(45))), gorilla.Lit("35-44")).
				When(gorilla.Col("age_cleaned").Lt(gorilla.Lit(int64(55))), gorilla.Lit("45-54")).
				Else(gorilla.Lit("55+")),
		).

		// Add email validity flag
		WithColumn("has_valid_email",
			gorilla.Col("email").Ne(gorilla.Lit("")),
		).

		// Calculate customer tenure (days since signup)
		WithColumn("signup_year", gorilla.Col("signup_date").Substring(gorilla.Lit(0), gorilla.Lit(4))).

		// Filter out test/invalid customers - simplified without LIKE
		Filter(gorilla.Col("name").Ne(gorilla.Lit("Test"))).

		// Select and rename columns
		Select("customer_id", "name", "email", "age_cleaned", "age_group", "city", "signup_date", "has_valid_email").
		Collect()

	if err != nil {
		log.Fatalf("Customer data transformation failed: %v", err)
	}

	return result
}

// transformOrderData cleans and enriches order data.
func transformOrderData(df *gorilla.DataFrame) *gorilla.DataFrame {
	result, err := df.Lazy().
		// Calculate total order value
		WithColumn("total_value",
			gorilla.Col("quantity").Mul(gorilla.Col("unit_price")),
		).

		// Add order size categories
		WithColumn("order_size",
			gorilla.Case().
				When(gorilla.Col("total_value").Gt(gorilla.Lit(500.0)), gorilla.Lit("Large")).
				When(gorilla.Col("total_value").Gt(gorilla.Lit(100.0)), gorilla.Lit("Medium")).
				Else(gorilla.Lit("Small")),
		).

		// Add month/year for time-based analysis
		WithColumn("order_month", gorilla.Col("order_date").Substring(gorilla.Lit(5), gorilla.Lit(2))).
		WithColumn("order_year", gorilla.Col("order_date").Substring(gorilla.Lit(0), gorilla.Lit(4))).

		// Filter for completed or shipped orders only
		Filter(
			gorilla.Col("status").Eq(gorilla.Lit("completed")).Or(
				gorilla.Col("status").Eq(gorilla.Lit("shipped")),
			),
		).

		// Filter out invalid quantities
		Filter(gorilla.Col("quantity").Gt(gorilla.Lit(int64(0)))).
		Filter(gorilla.Col("unit_price").Gt(gorilla.Lit(0.0))).
		Collect()

	if err != nil {
		log.Fatalf("Order data transformation failed: %v", err)
	}

	return result
}

// createAnalyticalView joins all data sources into an analytical view.
func createAnalyticalView(customers, orders, products *gorilla.DataFrame) *gorilla.DataFrame {
	// First join customers with orders
	customerOrders, err := customers.Join(orders, &gorilla.JoinOptions{
		Type:     gorilla.InnerJoin,
		LeftKey:  "customer_id",
		RightKey: "customer_id",
	})
	if err != nil {
		log.Fatalf("Customer-Order join failed: %v", err)
	}
	defer customerOrders.Release()

	// Then join with products
	fullView, err := customerOrders.Join(products, &gorilla.JoinOptions{
		Type:     gorilla.InnerJoin,
		LeftKey:  "product_id",
		RightKey: "product_id",
	})
	if err != nil {
		//nolint:gocritic // Example code: exitAfterDefer acceptable for demo error handling
		log.Fatalf("Product join failed: %v", err)
	}

	// Add calculated fields for analysis
	result, err := fullView.Lazy().
		// Calculate profit margin
		WithColumn("profit_margin",
			gorilla.Col("unit_price").Sub(gorilla.Col("cost")).Div(gorilla.Col("unit_price")),
		).

		// Calculate total profit
		WithColumn("total_profit",
			gorilla.Col("quantity").Mul(
				gorilla.Col("unit_price").Sub(gorilla.Col("cost")),
			),
		).

		// Add customer value tier
		WithColumn("customer_value_tier",
			gorilla.Case().
				When(gorilla.Col("total_value").Gt(gorilla.Lit(300.0)), gorilla.Lit("High_Value")).
				When(gorilla.Col("total_value").Gt(gorilla.Lit(100.0)), gorilla.Lit("Medium_Value")).
				Else(gorilla.Lit("Low_Value")),
		).
		Collect()

	if err != nil {
		log.Fatalf("Analytical view creation failed: %v", err)
	}

	return result
}

// performDataQualityChecks validates data quality and generates a report.
func performDataQualityChecks(df *gorilla.DataFrame) *gorilla.DataFrame {
	result, err := df.Lazy().
		// Group by different dimensions to check data quality
		GroupBy("category", "age_group").
		Agg(
			gorilla.Count(gorilla.Col("*")).As("total_records"),
			gorilla.Sum(
				gorilla.If(
					gorilla.Col("has_valid_email").Eq(gorilla.Lit(true)),
					gorilla.Lit(int64(1)),
					gorilla.Lit(int64(0)),
				),
			).As("valid_emails"),
			gorilla.Sum(
				gorilla.If(
					gorilla.Col("total_value").Gt(gorilla.Lit(0.0)),
					gorilla.Lit(int64(1)),
					gorilla.Lit(int64(0)),
				),
			).As("valid_orders"),
			gorilla.Mean(gorilla.Col("profit_margin")).As("avg_profit_margin"),
		).

		// Calculate quality metrics
		WithColumn("email_quality_pct",
			gorilla.Col("valid_emails").Div(gorilla.Col("total_records")).Mul(gorilla.Lit(100.0)),
		).
		WithColumn("order_quality_pct",
			gorilla.Col("valid_orders").Div(gorilla.Col("total_records")).Mul(gorilla.Lit(100.0)),
		).

		// Filter for significant segments
		Filter(gorilla.Col("total_records").Gt(gorilla.Lit(int64(5)))).

		// Sort by data quality issues
		Sort("email_quality_pct", true).
		Collect()

	if err != nil {
		log.Fatalf("Data quality check failed: %v", err)
	}

	return result
}

// generateBusinessInsights creates business intelligence from the analytical view.
func generateBusinessInsights(df *gorilla.DataFrame) *gorilla.DataFrame {
	result, err := df.Lazy().
		// Group by key business dimensions
		GroupBy("category", "city", "customer_value_tier").
		Agg(
			gorilla.Count(gorilla.Col("*")).As("transaction_count"),
			gorilla.Sum(gorilla.Col("total_value")).As("total_revenue"),
			gorilla.Sum(gorilla.Col("total_profit")).As("total_profit"),
			gorilla.Mean(gorilla.Col("profit_margin")).As("avg_margin"),
			gorilla.Count(gorilla.Col("customer_id")).As("unique_customers"), // Simplified without DISTINCT
		).

		// Calculate key business metrics
		WithColumn("avg_transaction_value",
			gorilla.Col("total_revenue").Div(gorilla.Col("transaction_count")),
		).
		WithColumn("profit_per_customer",
			gorilla.Col("total_profit").Div(gorilla.Col("unique_customers")),
		).
		WithColumn("margin_category",
			gorilla.Case().
				When(gorilla.Col("avg_margin").Gt(gorilla.Lit(0.3)), gorilla.Lit("High_Margin")).
				When(gorilla.Col("avg_margin").Gt(gorilla.Lit(0.15)), gorilla.Lit("Medium_Margin")).
				Else(gorilla.Lit("Low_Margin")),
		).

		// Filter for significant business segments
		Filter(gorilla.Col("transaction_count").Gt(gorilla.Lit(int64(10)))).
		Filter(gorilla.Col("total_revenue").Gt(gorilla.Lit(1000.0))).

		// Sort by business impact
		Sort("total_profit", false).
		Collect()

	if err != nil {
		log.Fatalf("Business insights generation failed: %v", err)
	}

	return result
}
