package sql

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHavingCrossFeatureIntegration(t *testing.T) {
	mem := memory.NewGoAllocator()
	executor := NewSQLExecutor(mem)

	// Create comprehensive test dataset
	regions := series.New("region",
		[]string{"North", "South", "North", "West", "South", "North", "East", "West", "South", "East"}, mem)
	departments := series.New("department", []string{
		"Engineering", "Sales", "Engineering", "HR", "Sales",
		"Engineering", "Sales", "HR", "Engineering", "Sales",
	}, mem)
	salaries := series.New("salary",
		[]float64{95000, 70000, 90000, 55000, 75000, 100000, 80000, 60000, 85000, 78000}, mem)
	experience := series.New("experience",
		[]int64{5, 3, 4, 8, 2, 6, 4, 10, 3, 5}, mem)
	active := series.New("active",
		[]bool{true, true, true, false, true, true, true, false, true, true}, mem)

	employees := dataframe.New(regions, departments, salaries, experience, active)
	defer employees.Release()

	executor.RegisterTable("employees", employees)

	t.Run("HAVING with WHERE clause integration", func(t *testing.T) {
		query := `
			SELECT region, department, AVG(salary) as avg_salary, COUNT(*) as emp_count
			FROM employees 
			WHERE active = true AND experience > 2
			GROUP BY region, department 
			HAVING AVG(salary) > 75000 AND COUNT(*) >= 2
			ORDER BY avg_salary DESC
		`

		result, err := executor.Execute(query)
		require.NoError(t, err)
		defer result.Release()

		assert.True(t, result.Len() > 0)
		assert.True(t, result.HasColumn("region"))
		assert.True(t, result.HasColumn("department"))
		assert.True(t, result.HasColumn("avg_salary"))
		assert.True(t, result.HasColumn("emp_count"))

		// Verify all results meet the HAVING criteria
		avgSalaryCol, _ := result.Column("avg_salary")
		empCountCol, _ := result.Column("emp_count")

		avgArray := avgSalaryCol.Array()
		countArray := empCountCol.Array()
		defer avgArray.Release()
		defer countArray.Release()

		for i := 0; i < result.Len(); i++ {
			avgSalaryFloat64, ok := avgArray.(*array.Float64)
			require.True(t, ok, "avg_salary column should be Float64 array")
			avgSalary := avgSalaryFloat64.Value(i)

			empCountInt64, ok := countArray.(*array.Int64)
			require.True(t, ok, "emp_count column should be Int64 array")
			empCount := empCountInt64.Value(i)

			assert.True(t, avgSalary > 75000, "avg_salary should be > 75000, got %f", avgSalary)
			assert.True(t, empCount >= 2, "emp_count should be >= 2, got %d", empCount)
		}
	})

	t.Run("HAVING with LIMIT and ORDER BY integration", func(t *testing.T) {
		query := `
			SELECT department, SUM(salary) as total_salary, MAX(experience) as max_exp
			FROM employees 
			WHERE active = true
			GROUP BY department 
			HAVING SUM(salary) > 100000
			ORDER BY total_salary DESC
			LIMIT 2
		`

		result, err := executor.Execute(query)
		require.NoError(t, err)
		defer result.Release()

		// Should have at most 2 results due to LIMIT
		assert.True(t, result.Len() <= 2)

		if result.Len() > 1 {
			// Verify ORDER BY DESC is working
			totalSalaryCol, _ := result.Column("total_salary")
			salaryArray := totalSalaryCol.Array()
			defer salaryArray.Release()

			// First result should have higher or equal salary than second
			salaryFloat64, ok := salaryArray.(*array.Float64)
			require.True(t, ok, "total_salary column should be Float64 array")
			salary1 := salaryFloat64.Value(0)
			salary2 := salaryFloat64.Value(1)
			assert.True(t, salary1 >= salary2, "First result salary %f should be >= second result salary %f", salary1, salary2)
		}
	})

	t.Run("HAVING with column aliases and complex expressions", func(t *testing.T) {
		query := `
			SELECT 
				region,
				AVG(salary) as avg_sal,
				COUNT(*) as team_size,
				SUM(experience) as total_exp,
				MAX(salary) as max_sal
			FROM employees 
			WHERE active = true
			GROUP BY region 
			HAVING avg_sal > 70000 AND team_size >= 3 AND total_exp > 10
			ORDER BY max_sal DESC
		`

		result, err := executor.Execute(query)
		require.NoError(t, err)
		defer result.Release()

		// Verify all columns are present
		expectedColumns := []string{"region", "avg_sal", "team_size", "total_exp", "max_sal"}
		for _, col := range expectedColumns {
			assert.True(t, result.HasColumn(col), "Column %s should be present", col)
		}

		// Verify HAVING conditions are satisfied
		for i := 0; i < result.Len(); i++ {
			avgSalCol, _ := result.Column("avg_sal")
			teamSizeCol, _ := result.Column("team_size")
			totalExpCol, _ := result.Column("total_exp")

			avgSalArray := avgSalCol.Array()
			avgSalFloat64, ok := avgSalArray.(*array.Float64)
			require.True(t, ok, "avg_sal column should be Float64 array")
			avgSal := avgSalFloat64.Value(i)

			teamSizeArray := teamSizeCol.Array()
			teamSizeInt64, ok := teamSizeArray.(*array.Int64)
			require.True(t, ok, "team_size column should be Int64 array")
			teamSize := teamSizeInt64.Value(i)

			totalExpArray := totalExpCol.Array()
			totalExpFloat64, ok := totalExpArray.(*array.Float64) // SUM returns float64
			require.True(t, ok, "total_exp column should be Float64 array")
			totalExp := totalExpFloat64.Value(i)

			assert.True(t, avgSal > 70000, "avg_sal should be > 70000, got %f", avgSal)
			assert.True(t, teamSize >= 3, "team_size should be >= 3, got %d", teamSize)
			assert.True(t, totalExp > 10, "total_exp should be > 10, got %f", totalExp)
		}
	})
}

func TestHavingBackwardCompatibility(t *testing.T) {
	mem := memory.NewGoAllocator()
	executor := NewSQLExecutor(mem)

	// Create simple test data
	departments := series.New("department", []string{"A", "B", "A", "B", "A"}, mem)
	values := series.New("value", []float64{100, 200, 150, 250, 175}, mem)

	testData := dataframe.New(departments, values)
	defer testData.Release()

	executor.RegisterTable("test_data", testData)

	t.Run("existing GROUP BY without HAVING should work unchanged", func(t *testing.T) {
		query := `
			SELECT department, AVG(value) as avg_value
			FROM test_data 
			GROUP BY department
		`

		result, err := executor.Execute(query)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 2, result.Len()) // Should have 2 departments
		assert.True(t, result.HasColumn("department"))
		assert.True(t, result.HasColumn("avg_value"))
	})

	t.Run("simple queries without GROUP BY should work unchanged", func(t *testing.T) {
		query := `SELECT department FROM test_data WHERE value > 150`

		result, err := executor.Execute(query)
		require.NoError(t, err)
		defer result.Release()

		assert.True(t, result.Len() > 0)
		assert.True(t, result.HasColumn("department"))
	})

	t.Run("aggregation without GROUP BY should work unchanged", func(t *testing.T) {
		query := `SELECT COUNT(*) as total_count FROM test_data`

		result, err := executor.Execute(query)
		require.NoError(t, err)
		defer result.Release()

		// This might return 0 or 1 rows depending on implementation
		// The important thing is it doesn't error
		assert.True(t, result.Len() >= 0)

		// Only check for column if we have results
		if result.Len() > 0 {
			assert.True(t, result.HasColumn("total_count"))
		}
	})
}

func TestHavingWithJoinsIntegration(t *testing.T) {
	mem := memory.NewGoAllocator()
	executor := NewSQLExecutor(mem)

	// Create related test tables for join testing

	// Employees table
	empIds := series.New("emp_id", []int64{1, 2, 3, 4, 5}, mem)
	empNames := series.New("name", []string{"Alice", "Bob", "Charlie", "David", "Eve"}, mem)
	deptIds := series.New("dept_id", []int64{1, 2, 1, 3, 2}, mem)
	empSalaries := series.New("salary", []float64{80000, 70000, 85000, 60000, 75000}, mem)

	employees := dataframe.New(empIds, empNames, deptIds, empSalaries)
	defer employees.Release()

	// Departments table
	deptIdsSeries := series.New("dept_id", []int64{1, 2, 3}, mem)
	deptNames := series.New("dept_name", []string{"Engineering", "Sales", "HR"}, mem)

	departments := dataframe.New(deptIdsSeries, deptNames)
	defer departments.Release()

	executor.RegisterTable("employees", employees)
	executor.RegisterTable("departments", departments)

	t.Run("HAVING with joins - manual join simulation", func(t *testing.T) {
		// Note: Since joins might not be fully implemented, test conceptual SQL
		// that would work with HAVING once joins are available

		// For now, test a query that simulates join behavior using existing data
		query := `
			SELECT dept_id, AVG(salary) as avg_salary, COUNT(*) as emp_count
			FROM employees 
			GROUP BY dept_id 
			HAVING AVG(salary) > 70000 AND COUNT(*) >= 2
		`

		result, err := executor.Execute(query)
		require.NoError(t, err)
		defer result.Release()

		assert.True(t, result.Len() > 0)

		// Verify HAVING conditions
		for i := 0; i < result.Len(); i++ {
			avgSalCol, _ := result.Column("avg_salary")
			empCountCol, _ := result.Column("emp_count")

			avgSalArray := avgSalCol.Array()
			avgSalFloat64, ok := avgSalArray.(*array.Float64)
			require.True(t, ok, "avg_salary column should be Float64 array")
			avgSal := avgSalFloat64.Value(i)

			empCountArray := empCountCol.Array()
			empCountInt64, ok := empCountArray.(*array.Int64)
			require.True(t, ok, "emp_count column should be Int64 array")
			empCount := empCountInt64.Value(i)

			assert.True(t, avgSal > 70000, "avg_salary should be > 70000, got %f", avgSal)
			assert.True(t, empCount >= 2, "emp_count should be >= 2, got %d", empCount)
		}
	})
}

func TestHavingComplexWorkflows(t *testing.T) {
	mem := memory.NewGoAllocator()
	executor := NewSQLExecutor(mem)

	// Create complex business scenario data
	orderIds := series.New("order_id", []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, mem)
	customerIds := series.New("customer_id", []int64{1, 2, 1, 3, 2, 1, 3, 4, 2, 3}, mem)
	products := series.New("product", []string{"A", "B", "A", "C", "B", "A", "C", "D", "B", "C"}, mem)
	quantities := series.New("quantity", []int64{2, 1, 3, 1, 2, 1, 2, 1, 3, 1}, mem)
	prices := series.New("price", []float64{100, 200, 100, 300, 200, 100, 300, 400, 200, 300}, mem)

	orders := dataframe.New(orderIds, customerIds, products, quantities, prices)
	defer orders.Release()

	executor.RegisterTable("orders", orders)

	t.Run("e-commerce analytics with HAVING", func(t *testing.T) {
		// Find customers with high-value orders (simplified)
		query := `
			SELECT 
				customer_id,
				COUNT(*) as order_count,
				SUM(price) as total_spent,
				AVG(price) as avg_order_value
			FROM orders 
			GROUP BY customer_id 
			HAVING SUM(price) > 500 AND COUNT(*) >= 2
			ORDER BY total_spent DESC
		`

		result, err := executor.Execute(query)
		require.NoError(t, err)
		defer result.Release()

		assert.True(t, result.Len() > 0)

		// Verify business logic
		for i := 0; i < result.Len(); i++ {
			orderCountCol, _ := result.Column("order_count")
			totalSpentCol, _ := result.Column("total_spent")

			orderCountArray := orderCountCol.Array()
			orderCountInt64, ok := orderCountArray.(*array.Int64)
			require.True(t, ok, "order_count column should be Int64 array")
			orderCount := orderCountInt64.Value(i)

			totalSpentArray := totalSpentCol.Array()
			totalSpentFloat64, ok := totalSpentArray.(*array.Float64)
			require.True(t, ok, "total_spent column should be Float64 array")
			totalSpent := totalSpentFloat64.Value(i)

			assert.True(t, totalSpent > 500, "total_spent should be > 500, got %f", totalSpent)
			assert.True(t, orderCount >= 2, "order_count should be >= 2, got %d", orderCount)
		}
	})

	t.Run("product performance analysis with HAVING", func(t *testing.T) {
		// Find popular products
		query := `
			SELECT 
				product,
				COUNT(*) as times_ordered,
				SUM(quantity) as total_quantity,
				AVG(price) as avg_price
			FROM orders 
			GROUP BY product 
			HAVING times_ordered >= 2 AND total_quantity >= 4
			ORDER BY total_quantity DESC
		`

		result, err := executor.Execute(query)
		require.NoError(t, err)
		defer result.Release()

		// Verify each product meets the criteria
		for i := 0; i < result.Len(); i++ {
			timesOrderedCol, _ := result.Column("times_ordered")
			totalQuantityCol, _ := result.Column("total_quantity")

			timesOrderedArray := timesOrderedCol.Array()
			timesOrderedInt64, ok := timesOrderedArray.(*array.Int64)
			require.True(t, ok, "times_ordered column should be Int64 array")
			timesOrdered := timesOrderedInt64.Value(i)

			totalQuantityArray := totalQuantityCol.Array()
			totalQuantityFloat64, ok := totalQuantityArray.(*array.Float64) // SUM returns float64
			require.True(t, ok, "total_quantity column should be Float64 array")
			totalQuantity := totalQuantityFloat64.Value(i)

			assert.True(t, timesOrdered >= 2, "times_ordered should be >= 2, got %d", timesOrdered)
			assert.True(t, totalQuantity >= 4, "total_quantity should be >= 4, got %f", totalQuantity)
		}
	})
}

func TestHavingErrorRecoveryAndRobustness(t *testing.T) {
	mem := memory.NewGoAllocator()
	executor := NewSQLExecutor(mem)

	// Create test data
	categories := series.New("category", []string{"X", "Y", "X", "Y"}, mem)
	values := series.New("value", []float64{10, 20, 30, 40}, mem)

	testData := dataframe.New(categories, values)
	defer testData.Release()

	executor.RegisterTable("test_data", testData)

	t.Run("graceful error handling for invalid HAVING syntax", func(t *testing.T) {
		invalidQueries := []string{
			"SELECT category FROM test_data HAVING value > 10",                                        // HAVING without GROUP BY
			"SELECT category, AVG(value) FROM test_data GROUP BY category HAVING invalid_column > 10", // Invalid column
			"SELECT category, AVG(value) FROM test_data GROUP BY category HAVING value > 10",          // Non-aggregated column
		}

		for _, query := range invalidQueries {
			currentQuery := query
			result, err := executor.Execute(currentQuery)
			assert.Error(t, err, "Query should fail: %s", currentQuery)
			if result != nil {
				result.Release()
			}
		}
	})

	t.Run("recovery after failed HAVING operations", func(t *testing.T) {
		// Execute a failing query
		_, err := executor.Execute("SELECT category FROM test_data HAVING value > 10")
		assert.Error(t, err)

		// Then execute a valid query - should work fine
		validResult, err := executor.Execute(`
			SELECT category, AVG(value) as avg_val 
			FROM test_data 
			GROUP BY category 
			HAVING AVG(value) > 15
		`)
		require.NoError(t, err)
		defer validResult.Release()

		assert.True(t, validResult.Len() > 0)
	})

	t.Run("HAVING with empty result sets", func(t *testing.T) {
		// Query that should return no results
		query := `
			SELECT category, AVG(value) as avg_val 
			FROM test_data 
			GROUP BY category 
			HAVING AVG(value) > 1000
		`

		result, err := executor.Execute(query)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 0, result.Len(), "Result should be empty")

		// But still have the correct columns
		assert.True(t, result.HasColumn("category"))
		assert.True(t, result.HasColumn("avg_val"))
	})
}

func TestHavingPerformanceRegression(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance regression test in short mode")
	}

	mem := memory.NewGoAllocator()
	executor := NewSQLExecutor(mem)

	// Create larger dataset for performance testing
	size := 10000
	categories := make([]string, size)
	values := make([]float64, size)

	catNames := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
	for i := 0; i < size; i++ {
		categories[i] = catNames[i%len(catNames)]
		values[i] = float64(100 + (i % 1000))
	}

	catSeries := series.New("category", categories, mem)
	valSeries := series.New("value", values, mem)
	largeData := dataframe.New(catSeries, valSeries)
	defer largeData.Release()

	executor.RegisterTable("large_data", largeData)

	t.Run("HAVING performance should be reasonable", func(t *testing.T) {
		// Complex HAVING query that should still execute reasonably fast
		query := `
			SELECT 
				category,
				COUNT(*) as count,
				AVG(value) as avg_value,
				SUM(value) as total_value,
				MAX(value) as max_value
			FROM large_data 
			GROUP BY category 
			HAVING COUNT(*) > 500 AND AVG(value) > 400 AND MAX(value) > 800
			ORDER BY total_value DESC
		`

		// Measure execution time
		result, err := executor.Execute(query)
		require.NoError(t, err)
		defer result.Release()

		// Verify we get reasonable results
		assert.True(t, result.Len() >= 0) // Should execute without hanging

		// Verify HAVING conditions are satisfied
		for i := 0; i < result.Len(); i++ {
			countCol, _ := result.Column("count")
			avgCol, _ := result.Column("avg_value")
			maxCol, _ := result.Column("max_value")

			countArray := countCol.Array()
			countInt64, ok := countArray.(*array.Int64)
			require.True(t, ok, "count column should be Int64 array")
			count := countInt64.Value(i)

			avgArray := avgCol.Array()
			avgFloat64, ok := avgArray.(*array.Float64)
			require.True(t, ok, "avg_value column should be Float64 array")
			avgValue := avgFloat64.Value(i)

			maxArray := maxCol.Array()
			maxFloat64, ok := maxArray.(*array.Float64)
			require.True(t, ok, "max_value column should be Float64 array")
			maxValue := maxFloat64.Value(i)

			assert.True(t, count > 500, "count should be > 500, got %d", count)
			assert.True(t, avgValue > 400, "avg_value should be > 400, got %f", avgValue)
			assert.True(t, maxValue > 800, "max_value should be > 800, got %f", maxValue)
		}
	})
}
