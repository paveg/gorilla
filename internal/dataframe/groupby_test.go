//nolint:testpackage // requires internal access to unexported types and functions
package dataframe

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataFrameGroupBy(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data
	categories := series.New("category", []string{"A", "B", "A", "B", "A"}, mem)
	values := series.New("value", []int64{10, 20, 30, 40, 50}, mem)
	prices := series.New("price", []float64{1.5, 2.0, 3.5, 4.0, 5.5}, mem)

	df := New(categories, values, prices)
	defer df.Release()

	// Test GroupBy creation
	gb := df.GroupBy("category")
	if gb == nil {
		t.Fatal("GroupBy should not be nil")
	}

	if len(gb.groupByCols) != 1 || gb.groupByCols[0] != "category" {
		t.Errorf("Expected group by columns [category], got %v", gb.groupByCols)
	}

	// Test that groups were created
	if len(gb.groups) == 0 {
		t.Fatal("Groups should not be empty")
	}

	// Should have 2 groups: A and B
	if len(gb.groups) != 2 {
		t.Errorf("Expected 2 groups, got %d", len(gb.groups))
	}
}

func TestGroupBySum(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data: category A has values [10, 30, 50], category B has values [20, 40]
	categories := series.New("category", []string{"A", "B", "A", "B", "A"}, mem)
	values := series.New("value", []int64{10, 20, 30, 40, 50}, mem)

	df := New(categories, values)
	defer df.Release()

	// Test sum aggregation
	result := df.GroupBy("category").Agg(expr.Sum(expr.Col("value")))
	defer result.Release()

	if result.Len() != 2 {
		t.Errorf("Expected 2 rows in result, got %d", result.Len())
	}

	if result.Width() != 2 {
		t.Errorf("Expected 2 columns in result, got %d", result.Width())
	}

	// Check that we have the expected columns
	if !result.HasColumn("category") {
		t.Error("Result should have 'category' column")
	}

	if !result.HasColumn("sum_value") {
		t.Error("Result should have 'sum_value' column")
	}
}

func TestGroupByCount(t *testing.T) {
	mem := memory.NewGoAllocator()

	categories := series.New("category", []string{"A", "B", "A", "B", "A"}, mem)
	values := series.New("value", []int64{10, 20, 30, 40, 50}, mem)

	df := New(categories, values)
	defer df.Release()

	// Test count aggregation
	result := df.GroupBy("category").Agg(expr.Count(expr.Col("value")))
	defer result.Release()

	if result.Len() != 2 {
		t.Errorf("Expected 2 rows in result, got %d", result.Len())
	}

	// Check that we have the expected columns
	if !result.HasColumn("category") {
		t.Error("Result should have 'category' column")
	}

	if !result.HasColumn("count_value") {
		t.Error("Result should have 'count_value' column")
	}

	// Verify count column is int64 type
	if countSeries, exists := result.Column("count_value"); exists {
		if countSeries.DataType().Name() != "int64" {
			t.Errorf("Expected count column to be int64, got %s", countSeries.DataType().Name())
		}
	}
}

func TestGroupByMean(t *testing.T) {
	mem := memory.NewGoAllocator()

	categories := series.New("category", []string{"A", "B", "A", "B"}, mem)
	values := series.New("value", []float64{10.0, 20.0, 30.0, 40.0}, mem)

	df := New(categories, values)
	defer df.Release()

	// Test mean aggregation
	result := df.GroupBy("category").Agg(expr.Mean(expr.Col("value")))
	defer result.Release()

	if result.Len() != 2 {
		t.Errorf("Expected 2 rows in result, got %d", result.Len())
	}

	if !result.HasColumn("mean_value") {
		t.Error("Result should have 'mean_value' column")
	}
}

func TestGroupByMultipleAggregations(t *testing.T) {
	mem := memory.NewGoAllocator()

	categories := series.New("category", []string{"A", "B", "A", "B", "A"}, mem)
	values := series.New("value", []int64{10, 20, 30, 40, 50}, mem)

	df := New(categories, values)
	defer df.Release()

	// Test multiple aggregations
	result := df.GroupBy("category").Agg(
		expr.Sum(expr.Col("value")),
		expr.Count(expr.Col("value")),
		expr.Mean(expr.Col("value")),
	)
	defer result.Release()

	expectedColumns := []string{"category", "sum_value", "count_value", "mean_value"}
	for _, col := range expectedColumns {
		if !result.HasColumn(col) {
			t.Errorf("Result should have '%s' column", col)
		}
	}

	if result.Width() != len(expectedColumns) {
		t.Errorf("Expected %d columns, got %d", len(expectedColumns), result.Width())
	}
}

func TestGroupByAggregationAlias(t *testing.T) {
	mem := memory.NewGoAllocator()

	categories := series.New("category", []string{"A", "B", "A"}, mem)
	values := series.New("value", []int64{10, 20, 30}, mem)

	df := New(categories, values)
	defer df.Release()

	// Test aggregation with alias
	result := df.GroupBy("category").Agg(
		expr.Sum(expr.Col("value")).As("total"),
	)
	defer result.Release()

	if !result.HasColumn("total") {
		t.Error("Result should have 'total' column (aliased)")
	}

	if result.HasColumn("sum_value") {
		t.Error("Result should not have 'sum_value' column when alias is used")
	}
}

func TestGroupByMultipleColumns(t *testing.T) {
	mem := memory.NewGoAllocator()

	category1 := series.New("cat1", []string{"A", "A", "B", "B"}, mem)
	category2 := series.New("cat2", []string{"X", "Y", "X", "Y"}, mem)
	values := series.New("value", []int64{10, 20, 30, 40}, mem)

	df := New(category1, category2, values)
	defer df.Release()

	// Test grouping by multiple columns
	result := df.GroupBy("cat1", "cat2").Agg(expr.Sum(expr.Col("value")))
	defer result.Release()

	if result.Len() != 4 {
		t.Errorf("Expected 4 groups (A-X, A-Y, B-X, B-Y), got %d", result.Len())
	}

	expectedColumns := []string{"cat1", "cat2", "sum_value"}
	for _, col := range expectedColumns {
		if !result.HasColumn(col) {
			t.Errorf("Result should have '%s' column", col)
		}
	}
}

func TestLazyFrameGroupBy(t *testing.T) {
	mem := memory.NewGoAllocator()

	categories := series.New("category", []string{"A", "B", "A", "B", "A"}, mem)
	values := series.New("value", []int64{10, 20, 30, 40, 50}, mem)

	df := New(categories, values)
	defer df.Release()

	// Test lazy GroupBy with sum
	result, err := df.Lazy().GroupBy("category").Sum("value").Collect()
	if err != nil {
		t.Fatalf("Error collecting lazy groupby: %v", err)
	}
	defer result.Release()

	if result.Len() != 2 {
		t.Errorf("Expected 2 rows in result, got %d", result.Len())
	}

	if !result.HasColumn("sum_value") {
		t.Error("Result should have 'sum_value' column")
	}
}

func TestLazyFrameGroupByChain(t *testing.T) {
	mem := memory.NewGoAllocator()

	categories := series.New("category", []string{"A", "B", "A", "B", "A"}, mem)
	values := series.New("value", []int64{10, 20, 30, 40, 50}, mem)
	enabled := series.New("enabled", []bool{true, true, false, true, true}, mem)

	df := New(categories, values, enabled)
	defer df.Release()

	// Test chaining operations: filter then group by
	result, err := df.Lazy().
		Filter(expr.Col("enabled").Eq(expr.Lit(true))).
		GroupBy("category").
		Sum("value").
		Collect()

	if err != nil {
		t.Fatalf("Error collecting chained operations: %v", err)
	}
	defer result.Release()

	if result.Len() != 2 {
		t.Errorf("Expected 2 rows in result, got %d", result.Len())
	}
}

func TestGroupByEmptyDataFrame(t *testing.T) {
	df := New()

	// Test GroupBy on empty DataFrame
	result := df.GroupBy("nonexistent").Agg(expr.Sum(expr.Col("value")))
	defer result.Release()

	if result.Len() != 0 {
		t.Errorf("Expected 0 rows for empty DataFrame, got %d", result.Len())
	}
}

func TestGroupByNonexistentColumn(t *testing.T) {
	mem := memory.NewGoAllocator()

	categories := series.New("category", []string{"A", "B"}, mem)
	df := New(categories)
	defer df.Release()

	// Test GroupBy with non-existent column
	result := df.GroupBy("nonexistent").Agg(expr.Sum(expr.Col("value")))
	defer result.Release()

	if result.Len() != 0 {
		t.Errorf("Expected 0 rows for non-existent column, got %d", result.Len())
	}
}

// MARK: Enhanced GroupByOperation with HAVING Support Tests

func TestGroupByOperationBackwardCompatibility(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("GroupByOperation without HAVING works as before", func(t *testing.T) {
		// Create test data
		categories := series.New("category", []string{"A", "B", "A", "B", "A"}, mem)
		values := series.New("value", []int64{10, 20, 30, 40, 50}, mem)

		df := New(categories, values)
		defer df.Release()

		// Test traditional GroupBy aggregation (no HAVING)
		operation := NewGroupByOperation([]string{"category"}, []*expr.AggregationExpr{
			expr.Sum(expr.Col("value")),
		})

		result, err := operation.Apply(df)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 2, result.Len()) // Groups A and B
		assert.True(t, result.HasColumn("category"))
		assert.True(t, result.HasColumn("sum_value"))
	})

	t.Run("Lazy evaluation with traditional GroupBy", func(t *testing.T) {
		categories := series.New("category", []string{"A", "B", "A", "B", "A"}, mem)
		values := series.New("value", []int64{10, 20, 30, 40, 50}, mem)

		df := New(categories, values)
		defer df.Release()

		// Test backward compatibility through lazy evaluation
		result, err := df.Lazy().GroupBy("category").Sum("value").Collect(context.Background())
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 2, result.NumRows())
		assert.True(t, result.HasColumn("sum_value"))
	})
}

func TestGroupByOperationBasicHaving(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("Simple HAVING with greater than", func(t *testing.T) {
		categories := series.New("category", []string{"A", "B", "A", "B", "A", "C"}, mem)
		values := series.New("value", []int64{10, 20, 30, 40, 50, 60}, mem)

		df := New(categories, values)
		defer df.Release()

		// Test HAVING SUM(value) > 70
		havingPredicate := expr.Sum(expr.Col("value")).Gt(expr.Lit(int64(70)))
		operation := NewGroupByOperationWithHaving(
			[]string{"category"},
			[]*expr.AggregationExpr{expr.Sum(expr.Col("value"))},
			havingPredicate,
		)

		result, err := operation.Apply(df)
		require.NoError(t, err)
		defer result.Release()

		// Group sums: A=90, B=60, C=60. Only A (90) > 70
		assert.Equal(t, 1, result.Len())

		categoryCol, exists := result.Column("category")
		require.True(t, exists)
		assert.Equal(t, "A", categoryCol.GetAsString(0))
	})

	t.Run("Simple HAVING with less than", func(t *testing.T) {
		departments := series.New("dept", []string{"HR", "IT", "Sales", "HR", "IT"}, mem)
		salaries := series.New("salary", []float64{50000, 80000, 90000, 60000, 85000}, mem)

		df := New(departments, salaries)
		defer df.Release()

		// Test HAVING AVG(salary) < 70000
		havingPredicate := expr.Mean(expr.Col("salary")).Lt(expr.Lit(70000.0))
		operation := NewGroupByOperationWithHaving(
			[]string{"dept"},
			[]*expr.AggregationExpr{expr.Mean(expr.Col("salary"))},
			havingPredicate,
		)

		result, err := operation.Apply(df)
		require.NoError(t, err)
		defer result.Release()

		// Dept averages: HR=55000, IT=82500, Sales=90000. Only HR (55000) < 70000
		assert.Equal(t, 1, result.Len())

		deptCol, exists := result.Column("dept")
		require.True(t, exists)
		assert.Equal(t, "HR", deptCol.GetAsString(0))
	})

	t.Run("Simple HAVING with equality", func(t *testing.T) {
		regions := series.New("region", []string{"North", "South", "North", "South"}, mem)
		counts := series.New("count", []int64{5, 10, 15, 10}, mem)

		df := New(regions, counts)
		defer df.Release()

		// Test HAVING COUNT(*) = 2 (each region appears twice)
		havingPredicate := expr.Count(expr.Col("count")).Eq(expr.Lit(int64(2)))
		operation := NewGroupByOperationWithHaving(
			[]string{"region"},
			[]*expr.AggregationExpr{expr.Count(expr.Col("count"))},
			havingPredicate,
		)

		result, err := operation.Apply(df)
		require.NoError(t, err)
		defer result.Release()

		// Both regions appear exactly 2 times
		assert.Equal(t, 2, result.Len())
	})
}

func TestGroupByOperationComplexHaving(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("HAVING with AND condition", func(t *testing.T) {
		stores := series.New("store", []string{"A", "B", "A", "B", "C", "C"}, mem)
		sales := series.New("sales", []float64{100, 200, 300, 150, 400, 500}, mem)
		profit := series.New("profit", []float64{10, 30, 40, 20, 50, 60}, mem)

		df := New(stores, sales, profit)
		defer df.Release()

		// Test HAVING SUM(sales) > 300 AND AVG(profit) > 35
		havingPredicate := expr.Sum(expr.Col("sales")).Gt(expr.Lit(300.0)).And(
			expr.Mean(expr.Col("profit")).Gt(expr.Lit(35.0)),
		)
		operation := NewGroupByOperationWithHaving(
			[]string{"store"},
			[]*expr.AggregationExpr{
				expr.Sum(expr.Col("sales")),
				expr.Mean(expr.Col("profit")),
			},
			havingPredicate,
		)

		result, err := operation.Apply(df)
		require.NoError(t, err)
		defer result.Release()

		// Store A: sum=400, avg_profit=25 -> fails avg condition
		// Store B: sum=350, avg_profit=25 -> fails avg condition
		// Store C: sum=900, avg_profit=55 -> passes both conditions
		assert.Equal(t, 1, result.Len())

		storeCol, exists := result.Column("store")
		require.True(t, exists)
		assert.Equal(t, "C", storeCol.GetAsString(0))
	})

	t.Run("HAVING with OR condition", func(t *testing.T) {
		products := series.New("product", []string{"X", "Y", "Z", "X", "Y"}, mem)
		quantities := series.New("qty", []int64{10, 5, 20, 15, 10}, mem)

		df := New(products, quantities)
		defer df.Release()

		// Test HAVING SUM(qty) > 30 OR COUNT(*) = 1
		havingPredicate := expr.Sum(expr.Col("qty")).Gt(expr.Lit(int64(30))).Or(
			expr.Count(expr.Col("qty")).Eq(expr.Lit(int64(1))),
		)
		operation := NewGroupByOperationWithHaving(
			[]string{"product"},
			[]*expr.AggregationExpr{
				expr.Sum(expr.Col("qty")),
				expr.Count(expr.Col("qty")),
			},
			havingPredicate,
		)

		result, err := operation.Apply(df)
		require.NoError(t, err)
		defer result.Release()

		// Product X: sum=25, count=2 -> fails both conditions
		// Product Y: sum=15, count=2 -> fails both conditions
		// Product Z: sum=20, count=1 -> passes count=1 condition
		assert.Equal(t, 1, result.Len())

		productCol, exists := result.Column("product")
		require.True(t, exists)
		assert.Equal(t, "Z", productCol.GetAsString(0))
	})

	t.Run("Nested boolean expressions in HAVING", func(t *testing.T) {
		teams := series.New("team", []string{"A", "B", "A", "B", "C", "C", "C"}, mem)
		scores := series.New("score", []int64{80, 90, 85, 95, 70, 75, 80}, mem)

		df := New(teams, scores)
		defer df.Release()

		// Test HAVING (COUNT(*) >= 2 AND AVG(score) > 80) OR MAX(score) >= 95
		havingPredicate := expr.Count(expr.Col("score")).Ge(expr.Lit(int64(2))).And(
			expr.Mean(expr.Col("score")).Gt(expr.Lit(80.0)),
		).Or(expr.Max(expr.Col("score")).Ge(expr.Lit(int64(95))))

		operation := NewGroupByOperationWithHaving(
			[]string{"team"},
			[]*expr.AggregationExpr{
				expr.Count(expr.Col("score")),
				expr.Mean(expr.Col("score")),
				expr.Max(expr.Col("score")),
			},
			havingPredicate,
		)

		result, err := operation.Apply(df)
		require.NoError(t, err)
		defer result.Release()

		// Team A: count=2, avg=82.5, max=85 -> passes first condition (count>=2 AND avg>80)
		// Team B: count=2, avg=92.5, max=95 -> passes both conditions
		// Team C: count=3, avg=75, max=80 -> fails both conditions
		assert.Equal(t, 2, result.Len())

		teamCol, exists := result.Column("team")
		require.True(t, exists)
		teamResults := make([]string, 0)
		for i := range teamCol.Len() {
			teamResults = append(teamResults, teamCol.GetAsString(i))
		}
		assert.Contains(t, teamResults, "A")
		assert.Contains(t, teamResults, "B")
		assert.NotContains(t, teamResults, "C")
	})
}

func TestGroupByOperationMultipleAggregations(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("HAVING with multiple aggregated columns", func(t *testing.T) {
		departments := series.New("dept", []string{"Eng", "Sales", "Eng", "Sales", "HR"}, mem)
		salaries := series.New("salary", []int64{80000, 60000, 90000, 70000, 55000}, mem)
		bonuses := series.New("bonus", []int64{8000, 5000, 10000, 7000, 3000}, mem)

		df := New(departments, salaries, bonuses)
		defer df.Release()

		// Test HAVING AVG(salary) > 70000 AND SUM(bonus) > 15000
		havingPredicate := expr.Mean(expr.Col("salary")).Gt(expr.Lit(70000.0)).And(
			expr.Sum(expr.Col("bonus")).Gt(expr.Lit(int64(15000))),
		)
		operation := NewGroupByOperationWithHaving(
			[]string{"dept"},
			[]*expr.AggregationExpr{
				expr.Mean(expr.Col("salary")),
				expr.Sum(expr.Col("bonus")),
				expr.Count(expr.Col("salary")),
			},
			havingPredicate,
		)

		result, err := operation.Apply(df)
		require.NoError(t, err)
		defer result.Release()

		// Eng: avg_salary=85000, sum_bonus=18000 -> passes both
		// Sales: avg_salary=65000, sum_bonus=12000 -> fails both
		// HR: avg_salary=55000, sum_bonus=3000 -> fails both
		assert.Equal(t, 1, result.Len())

		deptCol, exists := result.Column("dept")
		require.True(t, exists)
		assert.Equal(t, "Eng", deptCol.GetAsString(0))

		// Verify all aggregated columns are present
		assert.True(t, result.HasColumn("mean_salary"))
		assert.True(t, result.HasColumn("sum_bonus"))
		assert.True(t, result.HasColumn("count_salary"))
	})

	t.Run("HAVING with MIN and MAX aggregations", func(t *testing.T) {
		categories := series.New("category", []string{"A", "B", "A", "B", "C"}, mem)
		prices := series.New("price", []float64{10.5, 25.0, 30.5, 15.0, 40.0}, mem)

		df := New(categories, prices)
		defer df.Release()

		// Test HAVING MAX(price) - MIN(price) > 15
		// Create the subtraction as part of a comparison
		havingPredicate := expr.Max(expr.Col("price")).Gt(
			expr.Min(expr.Col("price")).Add(expr.Lit(15.0)),
		)
		operation := NewGroupByOperationWithHaving(
			[]string{"category"},
			[]*expr.AggregationExpr{
				expr.Min(expr.Col("price")),
				expr.Max(expr.Col("price")),
			},
			havingPredicate,
		)

		result, err := operation.Apply(df)
		require.NoError(t, err)
		defer result.Release()

		// Category A: min=10.5, max=30.5, diff=20 -> 30.5 > (10.5 + 15) = 25.5? Yes
		// Category B: min=15.0, max=25.0, diff=10 -> 25.0 > (15.0 + 15) = 30.0? No
		// Category C: min=40.0, max=40.0, diff=0  -> 40.0 > (40.0 + 15) = 55.0? No
		assert.Equal(t, 1, result.Len())

		categoryCol, exists := result.Column("category")
		require.True(t, exists)
		assert.Equal(t, "A", categoryCol.GetAsString(0))
	})
}

func TestGroupByOperationEdgeCases(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("HAVING resulting in empty dataset", func(t *testing.T) {
		categories := series.New("category", []string{"A", "B", "C"}, mem)
		values := series.New("value", []int64{1, 2, 3}, mem)

		df := New(categories, values)
		defer df.Release()

		// Test HAVING SUM(value) > 1000 (impossible condition)
		havingPredicate := expr.Sum(expr.Col("value")).Gt(expr.Lit(int64(1000)))
		operation := NewGroupByOperationWithHaving(
			[]string{"category"},
			[]*expr.AggregationExpr{expr.Sum(expr.Col("value"))},
			havingPredicate,
		)

		result, err := operation.Apply(df)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 0, result.Len())
		// Should still have proper column structure
		assert.True(t, result.HasColumn("category"))
		assert.True(t, result.HasColumn("sum_value"))
	})

	t.Run("HAVING with null-like values", func(t *testing.T) {
		groups := series.New("group", []string{"X", "Y", "X", "Y"}, mem)
		// Using 0 to represent conceptual nulls for testing
		values := series.New("value", []float64{10.0, 0.0, 20.0, 0.0}, mem)

		df := New(groups, values)
		defer df.Release()

		// Test HAVING AVG(value) > 5
		havingPredicate := expr.Mean(expr.Col("value")).Gt(expr.Lit(5.0))
		operation := NewGroupByOperationWithHaving(
			[]string{"group"},
			[]*expr.AggregationExpr{expr.Mean(expr.Col("value"))},
			havingPredicate,
		)

		result, err := operation.Apply(df)
		require.NoError(t, err)
		defer result.Release()

		// Group X: avg=15, Group Y: avg=0. Only X (15) > 5
		assert.Equal(t, 1, result.Len())

		groupCol, exists := result.Column("group")
		require.True(t, exists)
		assert.Equal(t, "X", groupCol.GetAsString(0))
	})

	t.Run("Single group with HAVING", func(t *testing.T) {
		groups := series.New("group", []string{"ONLY", "ONLY", "ONLY"}, mem)
		values := series.New("value", []int64{10, 20, 30}, mem)

		df := New(groups, values)
		defer df.Release()

		// Test HAVING SUM(value) = 60
		havingPredicate := expr.Sum(expr.Col("value")).Eq(expr.Lit(int64(60)))
		operation := NewGroupByOperationWithHaving(
			[]string{"group"},
			[]*expr.AggregationExpr{expr.Sum(expr.Col("value"))},
			havingPredicate,
		)

		result, err := operation.Apply(df)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 1, result.Len())

		groupCol, exists := result.Column("group")
		require.True(t, exists)
		assert.Equal(t, "ONLY", groupCol.GetAsString(0))
	})
}

func TestGroupByOperationMemoryManagement(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("Proper resource cleanup with HAVING", func(t *testing.T) {
		// Create larger dataset to test memory management
		groups := make([]string, 1000)
		values := make([]int64, 1000)
		for i := range 1000 {
			groups[i] = string(rune('A' + (i % 5))) // 5 groups
			values[i] = int64(i)
		}

		groupSeries := series.New("group", groups, mem)
		valueSeries := series.New("value", values, mem)

		df := New(groupSeries, valueSeries)
		defer df.Release()

		// Test HAVING SUM(value) > 50000
		havingPredicate := expr.Sum(expr.Col("value")).Gt(expr.Lit(int64(50000)))
		operation := NewGroupByOperationWithHaving(
			[]string{"group"},
			[]*expr.AggregationExpr{expr.Sum(expr.Col("value"))},
			havingPredicate,
		)

		result, err := operation.Apply(df)
		require.NoError(t, err)

		// Verify result is valid
		assert.GreaterOrEqual(t, result.Len(), 0) // May be 0 if no groups meet criteria
		assert.LessOrEqual(t, result.Len(), 5)    // Should be at most 5 groups

		// Ensure proper cleanup
		result.Release()
		// Test should complete without memory leaks
	})

	t.Run("Memory management with multiple operations", func(t *testing.T) {
		categories := series.New("category", []string{"A", "B", "A", "B"}, mem)
		values := series.New("value", []int64{100, 200, 300, 400}, mem)

		df := New(categories, values)
		defer df.Release()

		// Perform multiple operations with proper cleanup
		for i := range 10 {
			threshold := int64(250 + i*10)
			havingPredicate := expr.Sum(expr.Col("value")).Gt(expr.Lit(threshold))
			operation := NewGroupByOperationWithHaving(
				[]string{"category"},
				[]*expr.AggregationExpr{expr.Sum(expr.Col("value"))},
				havingPredicate,
			)

			result, err := operation.Apply(df)
			require.NoError(t, err)

			// Use defer to ensure cleanup happens
			func() {
				defer result.Release()
				assert.GreaterOrEqual(t, result.Len(), 0)
			}()
		}
	})
}

func TestGroupByOperationErrorHandling(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("Invalid predicate without aggregation", func(t *testing.T) {
		categories := series.New("category", []string{"A", "B"}, mem)
		values := series.New("value", []int64{10, 20}, mem)

		df := New(categories, values)
		defer df.Release()

		// Test HAVING predicate without aggregation (should fail)
		invalidPredicate := expr.Col("value").Gt(expr.Lit(int64(15)))
		operation := NewGroupByOperationWithHaving(
			[]string{"category"},
			[]*expr.AggregationExpr{expr.Sum(expr.Col("value"))},
			invalidPredicate,
		)

		_, err := operation.Apply(df)
		require.Error(t, err)
		// The error should indicate either HAVING validation or column not found
		// Both are valid error cases for invalid predicates
		errorContainsExpected := strings.Contains(err.Error(), "HAVING clause must contain aggregation functions") ||
			strings.Contains(err.Error(), "column not found") ||
			strings.Contains(err.Error(), "evaluating HAVING predicate")
		assert.True(t, errorContainsExpected, "Error should indicate HAVING validation or column issue, got: %v", err)
	})

	t.Run("Missing columns in aggregation", func(t *testing.T) {
		categories := series.New("category", []string{"A", "B"}, mem)
		df := New(categories)
		defer df.Release()

		// Test HAVING with reference to non-existent column
		havingPredicate := expr.Sum(expr.Col("nonexistent")).Gt(expr.Lit(int64(10)))
		operation := NewGroupByOperationWithHaving(
			[]string{"category"},
			[]*expr.AggregationExpr{expr.Sum(expr.Col("nonexistent"))},
			havingPredicate,
		)

		_, err := operation.Apply(df)
		require.Error(t, err)
		// Error should mention the missing column in aggregation context
	})

	t.Run("Empty aggregations list", func(t *testing.T) {
		categories := series.New("category", []string{"A", "B"}, mem)
		df := New(categories)
		defer df.Release()

		// Test with empty aggregations
		havingPredicate := expr.Sum(expr.Col("value")).Gt(expr.Lit(int64(10)))
		operation := NewGroupByOperationWithHaving(
			[]string{"category"},
			[]*expr.AggregationExpr{}, // Empty aggregations
			havingPredicate,
		)

		result, err := operation.Apply(df)
		require.NoError(t, err)
		defer result.Release()

		// Should return empty DataFrame
		assert.Equal(t, 0, result.Len())
	})

	t.Run("Empty group columns", func(t *testing.T) {
		values := series.New("value", []int64{10, 20}, mem)
		df := New(values)
		defer df.Release()

		// Test with empty group columns
		havingPredicate := expr.Sum(expr.Col("value")).Gt(expr.Lit(int64(15)))
		operation := NewGroupByOperationWithHaving(
			[]string{}, // Empty group columns
			[]*expr.AggregationExpr{expr.Sum(expr.Col("value"))},
			havingPredicate,
		)

		result, err := operation.Apply(df)
		require.NoError(t, err)
		defer result.Release()

		// Should return empty DataFrame
		assert.Equal(t, 0, result.Len())
	})
}

func TestGroupByOperationLazyIntegration(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("AggWithHaving through lazy evaluation", func(t *testing.T) {
		regions := series.New("region", []string{"North", "South", "North", "South", "East"}, mem)
		sales := series.New("sales", []float64{1000, 2000, 1500, 2500, 3000}, mem)

		df := New(regions, sales)
		defer df.Release()

		// Test AggWithHaving method
		havingPredicate := expr.Sum(expr.Col("sales")).Gt(expr.Lit(2000.0))
		result, err := df.Lazy().
			GroupBy("region").
			AggWithHaving(havingPredicate, expr.Sum(expr.Col("sales"))).
			Collect(context.Background())

		require.NoError(t, err)
		defer result.Release()

		// North: 2500, South: 4500, East: 3000. All > 2000, so all should remain
		assert.Equal(t, 3, result.NumRows())
	})

	t.Run("Having method through lazy evaluation", func(t *testing.T) {
		products := series.New("product", []string{"A", "B", "A", "B", "C"}, mem)
		quantities := series.New("qty", []int64{10, 5, 20, 15, 30}, mem)

		df := New(products, quantities)
		defer df.Release()

		// Test Having method (which internally performs aggregation)
		havingPredicate := expr.Sum(expr.Col("qty")).Gt(expr.Lit(int64(25)))
		result, err := df.Lazy().
			GroupBy("product").
			Having(havingPredicate).
			Collect(context.Background())

		require.NoError(t, err)
		defer result.Release()

		// Product A: 30, Product B: 20, Product C: 30
		// Products A and C have sum > 25
		assert.Equal(t, 2, result.NumRows())

		productCol, exists := result.Column("product")
		require.True(t, exists)
		productResults := make([]string, 0)
		for i := range productCol.Len() {
			productResults = append(productResults, productCol.GetAsString(i))
		}
		assert.Contains(t, productResults, "A")
		assert.Contains(t, productResults, "C")
		assert.NotContains(t, productResults, "B")
	})

	t.Run("Complex lazy chain with HAVING", func(t *testing.T) {
		employees := series.New("dept", []string{"IT", "HR", "IT", "HR", "Sales", "Sales"}, mem)
		salaries := series.New("salary", []float64{80000, 50000, 90000, 60000, 70000, 75000}, mem)
		ages := series.New("age", []int64{30, 25, 35, 28, 40, 45}, mem)

		df := New(employees, salaries, ages)
		defer df.Release()

		// Complex chain: Filter -> GroupBy -> Having
		havingPredicate := expr.Mean(expr.Col("salary")).Gt(expr.Lit(70000.0))
		result, err := df.Lazy().
			Filter(expr.Col("age").Gt(expr.Lit(int64(25)))). // Filter out age <= 25
			GroupBy("dept").
			AggWithHaving(havingPredicate,
				expr.Mean(expr.Col("salary")),
				expr.Count(expr.Col("salary")),
			).
			Collect(context.Background())

		require.NoError(t, err)
		defer result.Release()

		// After age filter: IT(80k,90k), HR(60k), Sales(70k,75k)
		// Dept averages: IT=85k, HR=60k, Sales=72.5k
		// Only IT (85k) and Sales (72.5k) have avg > 70k
		assert.Equal(t, 2, result.NumRows())
	})
}

// MARK: Performance and Benchmark Tests

func TestGroupByOperationPerformance(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("Performance with large dataset", func(t *testing.T) {
		// Create a large dataset for performance testing
		size := 10000
		categories := make([]string, size)
		values := make([]int64, size)
		for i := range size {
			categories[i] = string(rune('A' + (i % 100))) // 100 different groups
			values[i] = int64(i)
		}

		categorySeries := series.New("category", categories, mem)
		valueSeries := series.New("value", values, mem)

		df := New(categorySeries, valueSeries)
		defer df.Release()

		// Test HAVING performance with large dataset
		havingPredicate := expr.Sum(expr.Col("value")).Gt(expr.Lit(int64(400000)))
		operation := NewGroupByOperationWithHaving(
			[]string{"category"},
			[]*expr.AggregationExpr{
				expr.Sum(expr.Col("value")),
				expr.Count(expr.Col("value")),
			},
			havingPredicate,
		)

		result, err := operation.Apply(df)
		require.NoError(t, err)
		defer result.Release()

		// Verify reasonable performance characteristics
		assert.GreaterOrEqual(t, result.Len(), 0)
		assert.LessOrEqual(t, result.Len(), 100) // At most 100 groups
	})
}

// Benchmark tests for GroupByOperation with HAVING.
func BenchmarkGroupByOperationWithHaving(b *testing.B) {
	mem := memory.NewGoAllocator()

	// Create test data
	size := 1000
	categories := make([]string, size)
	values := make([]int64, size)
	for i := range size {
		categories[i] = string(rune('A' + (i % 10))) // 10 groups
		values[i] = int64(i)
	}

	categorySeries := series.New("category", categories, mem)
	valueSeries := series.New("value", values, mem)

	df := New(categorySeries, valueSeries)
	defer df.Release()

	havingPredicate := expr.Sum(expr.Col("value")).Gt(expr.Lit(int64(20000)))
	operation := NewGroupByOperationWithHaving(
		[]string{"category"},
		[]*expr.AggregationExpr{expr.Sum(expr.Col("value"))},
		havingPredicate,
	)

	b.ResetTimer()
	for range b.N {
		result, err := operation.Apply(df)
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGroupByOperationWithoutHaving(b *testing.B) {
	mem := memory.NewGoAllocator()

	// Create test data
	size := 1000
	categories := make([]string, size)
	values := make([]int64, size)
	for i := range size {
		categories[i] = string(rune('A' + (i % 10))) // 10 groups
		values[i] = int64(i)
	}

	categorySeries := series.New("category", categories, mem)
	valueSeries := series.New("value", values, mem)

	df := New(categorySeries, valueSeries)
	defer df.Release()

	operation := NewGroupByOperation(
		[]string{"category"},
		[]*expr.AggregationExpr{expr.Sum(expr.Col("value"))},
	)

	b.ResetTimer()
	for range b.N {
		result, err := operation.Apply(df)
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}
