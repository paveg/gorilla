// Package dataframe provides practical integration tests for AggregationExpr comparison methods.
//
// This file demonstrates the new comparison methods (Gt, Ge, Lt, Le, Eq, Ne, And, Or)
// available on AggregationExpr types, which enable HAVING clause-like functionality
// for filtering aggregated results in DataFrame operations.
//
// Issue #106: Add comparison methods to AggregationExpr for HAVING clause support
//
// The tests showcase realistic business scenarios where these comparison methods
// would be used to filter grouped aggregations, effectively implementing the
// functionality needed for SQL HAVING clauses in Gorilla DataFrames.
//
//nolint:testpackage // requires internal access to unexported types and functions
package dataframe

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"

	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
)

// TestAggregationExprComparison_SalesDataScenario demonstrates a realistic sales analysis
// scenario where we need to filter groups based on aggregated values (future HAVING clause).
func TestAggregationExprComparison_SalesDataScenario(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create realistic sales data
	// Company sales data with multiple regions and products
	regions := series.New("region", []string{
		"North", "South", "North", "West", "South", "East", "North", "West", "East", "South",
	}, mem)

	products := series.New("product", []string{
		"Laptop", "Phone", "Tablet", "Laptop", "Phone", "Tablet", "Phone", "Laptop", "Tablet", "Phone",
	}, mem)

	salesAmount := series.New("sales_amount", []float64{
		1500.0, 800.0, 600.0, 1200.0, 750.0, 550.0, 900.0, 1800.0, 650.0, 850.0,
	}, mem)

	quantity := series.New("quantity", []int64{
		3, 4, 2, 2, 3, 2, 4, 3, 2, 4,
	}, mem)

	df := New(regions, products, salesAmount, quantity)
	defer df.Release()

	t.Run("Total Sales by Region with Filtering", func(t *testing.T) {
		// Group by region and calculate total sales
		result := df.GroupBy("region").Agg(
			expr.Sum(expr.Col("sales_amount")).As("total_sales"),
			expr.Count(expr.Col("product")).As("order_count"),
		)
		defer result.Release()

		// Verify aggregation results
		assert.Equal(t, 4, result.Len()) // 4 regions: North, South, West, East
		assert.True(t, result.HasColumn("region"))
		assert.True(t, result.HasColumn("total_sales"))
		assert.True(t, result.HasColumn("order_count"))

		// Test aggregation expression comparisons (future HAVING clause scenarios)
		totalSalesExpr := expr.Sum(expr.Col("sales_amount")).As("total_sales")

		// Test Gt (greater than) - regions with total sales > 2000
		havingCondition1 := totalSalesExpr.Gt(expr.Lit(2000.0))
		assert.Equal(t, expr.ExprBinary, havingCondition1.Type())
		assert.Contains(t, havingCondition1.String(), "sum(col(sales_amount))")
		assert.Contains(t, havingCondition1.String(), ">")
		assert.Contains(t, havingCondition1.String(), "2000")

		// Test Ge (greater than or equal) - regions with total sales >= 1500
		havingCondition2 := totalSalesExpr.Ge(expr.Lit(1500.0))
		assert.Equal(t, expr.ExprBinary, havingCondition2.Type())
		assert.Contains(t, havingCondition2.String(), ">=")

		// Test Lt (less than) - regions with total sales < 1000
		havingCondition3 := totalSalesExpr.Lt(expr.Lit(1000.0))
		assert.Equal(t, expr.ExprBinary, havingCondition3.Type())
		assert.Contains(t, havingCondition3.String(), "<")

		// Test combined conditions with And/Or
		combinedCondition := totalSalesExpr.Gt(expr.Lit(2000.0)).And(
			expr.Count(expr.Col("product")).Ge(expr.Lit(2)),
		)
		assert.Equal(t, expr.ExprBinary, combinedCondition.Type())
		assert.Contains(t, combinedCondition.String(), "&&")
	})

	t.Run("Average Sales per Product with Complex Conditions", func(t *testing.T) {
		// Group by product and calculate average sales and total quantity
		result := df.GroupBy("product").Agg(
			expr.Mean(expr.Col("sales_amount")).As("avg_sales"),
			expr.Sum(expr.Col("quantity")).As("total_quantity"),
			expr.Count(expr.Col("region")).As("region_count"),
		)
		defer result.Release()

		// Test multiple aggregation comparisons
		avgSalesExpr := expr.Mean(expr.Col("sales_amount")).As("avg_sales")
		totalQuantityExpr := expr.Sum(expr.Col("quantity")).As("total_quantity")

		// Complex HAVING-like conditions
		// Products with average sales > 1000 AND total quantity >= 8
		complexCondition1 := avgSalesExpr.Gt(expr.Lit(1000.0)).And(
			totalQuantityExpr.Ge(expr.Lit(8)),
		)

		// Products with average sales between 800 and 1200 (using Ge and Le)
		rangeCondition := avgSalesExpr.Ge(expr.Lit(800.0)).And(
			avgSalesExpr.Le(expr.Lit(1200.0)),
		)

		// Products with either high average sales OR high quantity
		orCondition := avgSalesExpr.Gt(expr.Lit(1500.0)).Or(
			totalQuantityExpr.Gt(expr.Lit(10)),
		)

		// Verify condition structure
		assert.Equal(t, expr.ExprBinary, complexCondition1.Type())
		assert.Equal(t, expr.ExprBinary, rangeCondition.Type())
		assert.Equal(t, expr.ExprBinary, orCondition.Type())

		// Verify condition contains expected operators
		assert.Contains(t, complexCondition1.String(), "&&")
		assert.Contains(t, rangeCondition.String(), ">=")
		assert.Contains(t, rangeCondition.String(), "<=")
		assert.Contains(t, orCondition.String(), "||")
	})
}

// TestAggregationExprComparison_EmployeeAnalysisScenario demonstrates HR analytics
// use case with department-based aggregations and filtering.
func TestAggregationExprComparison_EmployeeAnalysisScenario(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create employee data
	departments := series.New("department", []string{
		"Engineering", "Sales", "Marketing", "Engineering", "Sales", "HR",
		"Engineering", "Marketing", "Sales", "HR", "Engineering", "Marketing",
	}, mem)

	salaries := series.New("salary", []float64{
		95000, 65000, 55000, 105000, 70000, 60000,
		85000, 58000, 72000, 62000, 98000, 60000,
	}, mem)

	experience := series.New("years_experience", []int64{
		5, 3, 2, 8, 4, 6, 3, 3, 5, 7, 6, 4,
	}, mem)

	df := New(departments, salaries, experience)
	defer df.Release()

	t.Run("Department Salary Analysis with HAVING-like Filtering", func(t *testing.T) {
		// Group by department and calculate salary statistics
		result := df.GroupBy("department").Agg(
			expr.Mean(expr.Col("salary")).As("avg_salary"),
			expr.Max(expr.Col("salary")).As("max_salary"),
			expr.Min(expr.Col("salary")).As("min_salary"),
			expr.Count(expr.Col("salary")).As("employee_count"),
		)
		defer result.Release()

		// Test different aggregation comparison scenarios
		avgSalaryExpr := expr.Mean(expr.Col("salary")).As("avg_salary")
		maxSalaryExpr := expr.Max(expr.Col("salary")).As("max_salary")
		minSalaryExpr := expr.Min(expr.Col("salary")).As("min_salary")
		countExpr := expr.Count(expr.Col("salary")).As("employee_count")

		// Scenario 1: Departments with high average salary (HAVING avg_salary > 80000)
		highAvgSalaryCondition := avgSalaryExpr.Gt(expr.Lit(80000.0))
		assert.Equal(t, expr.ExprBinary, highAvgSalaryCondition.Type())
		assert.Contains(t, highAvgSalaryCondition.String(), "mean(col(salary))")

		// Scenario 2: Large departments with good pay (HAVING count >= 3 AND avg_salary >= 70000)
		largeDeptCondition := countExpr.Ge(expr.Lit(3)).And(
			avgSalaryExpr.Ge(expr.Lit(70000.0)),
		)
		assert.Contains(t, largeDeptCondition.String(), "count(col(salary))")
		assert.Contains(t, largeDeptCondition.String(), "&&")

		// Scenario 3: Departments with salary inequality (HAVING max_salary > 100000 AND min_salary < 70000)
		// Testing high max and low min separately (arithmetic between aggregations is a future enhancement)
		highMaxCondition := maxSalaryExpr.Gt(expr.Lit(100000.0))
		lowMinCondition := minSalaryExpr.Lt(expr.Lit(70000.0))
		salaryInequalityCondition := highMaxCondition.And(lowMinCondition)
		assert.Equal(t, expr.ExprBinary, salaryInequalityCondition.Type())

		// Scenario 4: Departments needing attention (HAVING avg_salary < 65000 OR employee_count = 1)
		attentionNeededCondition := avgSalaryExpr.Lt(expr.Lit(65000.0)).Or(
			countExpr.Eq(expr.Lit(1)),
		)
		assert.Contains(t, attentionNeededCondition.String(), "||")
	})
}

// TestAggregationExprComparison_InventoryManagementScenario demonstrates inventory
// management with stock level analysis and supplier performance.
func TestAggregationExprComparison_InventoryManagementScenario(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create inventory data
	suppliers := series.New("supplier", []string{
		"SupplierA", "SupplierB", "SupplierA", "SupplierC", "SupplierB",
		"SupplierA", "SupplierC", "SupplierB", "SupplierA", "SupplierC",
	}, mem)

	categories := series.New("category", []string{
		"Electronics", "Clothing", "Electronics", "Food", "Clothing",
		"Food", "Electronics", "Food", "Clothing", "Electronics",
	}, mem)

	stockLevel := series.New("stock_level", []int64{
		150, 80, 200, 50, 120, 30, 180, 75, 90, 160,
	}, mem)

	unitCost := series.New("unit_cost", []float64{
		25.0, 15.0, 30.0, 5.0, 18.0, 8.0, 28.0, 6.0, 20.0, 32.0,
	}, mem)

	df := New(suppliers, categories, stockLevel, unitCost)
	defer df.Release()

	t.Run("Supplier Performance Analysis", func(t *testing.T) {
		// Group by supplier and analyze their performance
		result := df.GroupBy("supplier").Agg(
			expr.Sum(expr.Col("stock_level")).As("total_stock"),
			expr.Mean(expr.Col("unit_cost")).As("avg_cost"),
			expr.Count(expr.Col("category")).As("product_variety"),
			expr.Max(expr.Col("stock_level")).As("max_stock_item"),
		)
		defer result.Release()

		totalStockExpr := expr.Sum(expr.Col("stock_level")).As("total_stock")
		avgCostExpr := expr.Mean(expr.Col("unit_cost")).As("avg_cost")
		varietyExpr := expr.Count(expr.Col("category")).As("product_variety")

		// High-volume suppliers (HAVING total_stock > 400)
		highVolumeCondition := totalStockExpr.Gt(expr.Lit(400))
		assert.Equal(t, expr.ExprBinary, highVolumeCondition.Type())

		// Cost-effective suppliers (HAVING avg_cost <= 20.0 AND product_variety >= 3)
		costEffectiveCondition := avgCostExpr.Le(expr.Lit(20.0)).And(
			varietyExpr.Ge(expr.Lit(3)),
		)

		// Premium suppliers (HAVING avg_cost > 25.0)
		premiumCondition := avgCostExpr.Gt(expr.Lit(25.0))

		// Verify all conditions are properly structured
		assert.Equal(t, expr.ExprBinary, highVolumeCondition.Type())
		assert.Equal(t, expr.ExprBinary, costEffectiveCondition.Type())
		assert.Equal(t, expr.ExprBinary, premiumCondition.Type())

		// Test inequality comparisons
		notEqualCondition := varietyExpr.Ne(expr.Lit(2))
		assert.Contains(t, notEqualCondition.String(), "!=")
	})

	t.Run("Category-based Stock Analysis", func(t *testing.T) {
		// Multi-dimensional grouping for detailed analysis
		result := df.GroupBy("category", "supplier").Agg(
			expr.Sum(expr.Col("stock_level")).As("category_supplier_stock"),
			expr.Mean(expr.Col("unit_cost")).As("avg_category_cost"),
		)
		defer result.Release()

		categoryStockExpr := expr.Sum(expr.Col("stock_level")).As("category_supplier_stock")
		avgCategoryCostExpr := expr.Mean(expr.Col("unit_cost")).As("avg_category_cost")

		// Low stock alerts (HAVING category_supplier_stock < 100)
		lowStockCondition := categoryStockExpr.Lt(expr.Lit(100))

		// High-value categories (HAVING avg_category_cost >= 25.0)
		highValueCondition := avgCategoryCostExpr.Ge(expr.Lit(25.0))

		// Combined conditions for strategic decision making
		strategicCondition := lowStockCondition.And(highValueCondition)

		assert.Equal(t, expr.ExprBinary, strategicCondition.Type())
		assert.Contains(t, strategicCondition.String(), "&&")
	})
}

// TestAggregationExprComparison_ChainedComparisons tests complex chained comparison scenarios.
func TestAggregationExprComparison_ChainedComparisons(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Simple test data for complex condition testing
	groups := series.New("group", []string{"A", "B", "A", "C", "B", "A", "C"}, mem)
	values := series.New("value", []int64{10, 20, 30, 15, 25, 40, 35}, mem)
	scores := series.New("score", []float64{1.5, 2.0, 3.5, 2.5, 3.0, 4.0, 3.8}, mem)

	df := New(groups, values, scores)
	defer df.Release()

	t.Run("Complex Chained Aggregation Conditions", func(t *testing.T) {
		result := df.GroupBy("group").Agg(
			expr.Sum(expr.Col("value")).As("total_value"),
			expr.Mean(expr.Col("score")).As("avg_score"),
			expr.Count(expr.Col("value")).As("count"),
			expr.Max(expr.Col("value")).As("max_value"),
			expr.Min(expr.Col("score")).As("min_score"),
		)
		defer result.Release()

		sumExpr := expr.Sum(expr.Col("value")).As("total_value")
		meanExpr := expr.Mean(expr.Col("score")).As("avg_score")
		countExpr := expr.Count(expr.Col("value")).As("count")
		maxExpr := expr.Max(expr.Col("value")).As("max_value")
		minExpr := expr.Min(expr.Col("score")).As("min_score")

		// Test all comparison operators on aggregations

		// Equality and inequality
		equalityCondition := countExpr.Eq(expr.Lit(3))
		inequalityCondition := countExpr.Ne(expr.Lit(1))

		// Range conditions
		rangeCondition := sumExpr.Ge(expr.Lit(50.0)).And(sumExpr.Le(expr.Lit(100.0)))

		// Multiple aggregation comparisons
		multiAggCondition := sumExpr.Gt(expr.Lit(60.0)).And(
			meanExpr.Ge(expr.Lit(3.0))).And(
			maxExpr.Le(expr.Lit(50.0))).Or(
			minExpr.Eq(expr.Lit(1.5)))

		// Verify condition structures
		assert.Equal(t, expr.ExprBinary, equalityCondition.Type())
		assert.Equal(t, expr.ExprBinary, inequalityCondition.Type())
		assert.Equal(t, expr.ExprBinary, rangeCondition.Type())
		assert.Equal(t, expr.ExprBinary, multiAggCondition.Type())

		// Verify string representations contain expected operators
		assert.Contains(t, equalityCondition.String(), "==")
		assert.Contains(t, inequalityCondition.String(), "!=")
		assert.Contains(t, rangeCondition.String(), ">=")
		assert.Contains(t, rangeCondition.String(), "<=")
		assert.Contains(t, rangeCondition.String(), "&&")
		assert.Contains(t, multiAggCondition.String(), "||")

		// Test that aggregation expressions can be used in logical operations
		logicalCondition := sumExpr.Gt(expr.Lit(70.0)).Or(
			meanExpr.Lt(expr.Lit(2.0)).And(countExpr.Ge(expr.Lit(2))),
		)
		assert.Equal(t, expr.ExprBinary, logicalCondition.Type())
	})
}

// TestAggregationExprComparison_ErrorHandling tests edge cases and error conditions.
func TestAggregationExprComparison_ErrorHandling(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Edge case data
	groups := series.New("group", []string{"A", "B"}, mem)
	values := series.New("value", []int64{1, 2}, mem)

	df := New(groups, values)
	defer df.Release()

	t.Run("Edge Cases with Small Data", func(t *testing.T) {
		result := df.GroupBy("group").Agg(
			expr.Sum(expr.Col("value")).As("sum_val"),
			expr.Count(expr.Col("value")).As("count_val"),
		)
		defer result.Release()

		sumExpr := expr.Sum(expr.Col("value")).As("sum_val")
		countExpr := expr.Count(expr.Col("value")).As("count_val")

		// Test boundary conditions
		zeroCondition := countExpr.Eq(expr.Lit(0))
		oneCondition := countExpr.Eq(expr.Lit(1))

		// Test with different literal types
		intComparison := sumExpr.Gt(expr.Lit(0))
		floatComparison := sumExpr.Gt(expr.Lit(0.0))

		// All should create valid binary expressions
		assert.Equal(t, expr.ExprBinary, zeroCondition.Type())
		assert.Equal(t, expr.ExprBinary, oneCondition.Type())
		assert.Equal(t, expr.ExprBinary, intComparison.Type())
		assert.Equal(t, expr.ExprBinary, floatComparison.Type())
	})

	t.Run("Empty DataFrame Handling", func(t *testing.T) {
		emptyDf := New()
		defer emptyDf.Release()

		// Even with empty data, expressions should be constructible
		sumExpr := expr.Sum(expr.Col("nonexistent")).As("sum_val")
		condition := sumExpr.Gt(expr.Lit(0))

		assert.Equal(t, expr.ExprBinary, condition.Type())
		assert.NotEmpty(t, condition.String())
	})
}

// TestAggregationExprComparison_TypeConsistency verifies that comparison operations
// maintain type consistency and produce valid expression trees.
func TestAggregationExprComparison_TypeConsistency(t *testing.T) {
	t.Run("All Aggregation Types Support All Comparisons", func(t *testing.T) {
		// Test that all aggregation types support all comparison operations
		col := expr.Col("test_column")

		aggregations := []*expr.AggregationExpr{
			expr.Sum(col),
			expr.Count(col),
			expr.Mean(col),
			expr.Min(col),
			expr.Max(col),
		}

		literal := expr.Lit(100)

		for _, agg := range aggregations {
			// Test all comparison methods exist and return BinaryExpr
			comparisons := []*expr.BinaryExpr{
				agg.Gt(literal),
				agg.Ge(literal),
				agg.Lt(literal),
				agg.Le(literal),
				agg.Eq(literal),
				agg.Ne(literal),
			}

			for _, comp := range comparisons {
				assert.Equal(t, expr.ExprBinary, comp.Type())
				assert.NotEmpty(t, comp.String())

				// Verify that the aggregation is the left operand
				assert.Equal(t, agg, comp.Left())
				assert.Equal(t, literal, comp.Right())
			}

			// Test logical operations
			logicalOps := []*expr.BinaryExpr{
				agg.And(literal),
				agg.Or(literal),
			}

			for _, op := range logicalOps {
				assert.Equal(t, expr.ExprBinary, op.Type())
				assert.NotEmpty(t, op.String())
			}
		}
	})

	t.Run("Aggregation Comparisons Can Be Combined", func(t *testing.T) {
		sumExpr := expr.Sum(expr.Col("sales"))
		countExpr := expr.Count(expr.Col("items"))

		// Test that aggregation comparisons can be combined with other expressions
		combined1 := sumExpr.Gt(expr.Lit(1000)).And(countExpr.Ge(expr.Lit(5)))
		combined2 := sumExpr.Le(expr.Lit(500)).Or(countExpr.Eq(expr.Lit(1)))

		assert.Equal(t, expr.ExprBinary, combined1.Type())
		assert.Equal(t, expr.ExprBinary, combined2.Type())

		// Test nested combinations
		nested := combined1.Or(combined2)
		assert.Equal(t, expr.ExprBinary, nested.Type())
		assert.Contains(t, nested.String(), "||")
	})
}

// TestAggregationExprComparison_DocumentedUsageExamples provides clear examples
// of how these comparison methods will be used in practice.
func TestAggregationExprComparison_DocumentedUsageExamples(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Example data mimicking the documentation examples
	regions := series.New("region", []string{
		"North", "South", "East", "West", "North", "South", "East", "West",
	}, mem)
	sales := series.New("sales", []float64{
		1000, 1500, 800, 1200, 1100, 1300, 900, 1400,
	}, mem)

	df := New(regions, sales)
	defer df.Release()

	t.Run("SQL HAVING Clause Equivalents", func(t *testing.T) {
		// This test documents how the new comparison methods enable SQL HAVING-like functionality

		// SQL: SELECT region, SUM(sales) as total_sales FROM table GROUP BY region HAVING SUM(sales) > 2000
		// Gorilla equivalent preparation:
		totalSalesAgg := expr.Sum(expr.Col("sales")).As("total_sales")
		havingCondition := totalSalesAgg.Gt(expr.Lit(2000.0))

		// SQL: SELECT region, AVG(sales) as avg_sales FROM table GROUP BY region HAVING AVG(sales) BETWEEN 1000 AND 1500
		// Gorilla equivalent:
		avgSalesAgg := expr.Mean(expr.Col("sales")).As("avg_sales")
		betweenCondition := avgSalesAgg.Ge(expr.Lit(1000.0)).And(avgSalesAgg.Le(expr.Lit(1500.0)))

		// SQL: SELECT region, COUNT(*) as count FROM table GROUP BY region HAVING COUNT(*) >= 2 AND SUM(sales) > 1500
		// Gorilla equivalent:
		countAgg := expr.Count(expr.Col("sales"))
		sumAgg := expr.Sum(expr.Col("sales"))
		complexHaving := countAgg.Ge(expr.Lit(2)).And(sumAgg.Gt(expr.Lit(1500.0)))

		// Verify these create valid expression trees for future HAVING implementation
		assert.Equal(t, expr.ExprBinary, havingCondition.Type())
		assert.Equal(t, expr.ExprBinary, betweenCondition.Type())
		assert.Equal(t, expr.ExprBinary, complexHaving.Type())

		// Verify string representations for debugging
		assert.Contains(t, havingCondition.String(), "sum(col(sales)) > lit(2000)")
		assert.Contains(t, betweenCondition.String(), "mean(col(sales))")
		assert.Contains(t, betweenCondition.String(), "&&")
		assert.Contains(t, complexHaving.String(), "count(col(sales))")
	})

	t.Run("Business Intelligence Query Patterns", func(t *testing.T) {
		// Common BI patterns that will be enabled by these comparisons

		// Pattern 1: High-performing regions
		highPerformance := expr.Sum(expr.Col("sales")).Gt(expr.Lit(2500.0))

		// Pattern 2: Consistent regions (low variance indicated by min/max similarity)
		maxSales := expr.Max(expr.Col("sales"))
		minSales := expr.Min(expr.Col("sales"))
		// Note: This would require arithmetic between aggregations (future enhancement)
		// For now, we test individual comparisons
		highMin := minSales.Ge(expr.Lit(1000.0))
		reasonableMax := maxSales.Le(expr.Lit(1600.0))
		consistent := highMin.And(reasonableMax)

		// Pattern 3: Market coverage (regions with good diversity)
		goodCoverage := expr.Count(expr.Col("sales")).Ge(expr.Lit(2))

		assert.Equal(t, expr.ExprBinary, highPerformance.Type())
		assert.Equal(t, expr.ExprBinary, consistent.Type())
		assert.Equal(t, expr.ExprBinary, goodCoverage.Type())
	})
}
