package dataframe

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/paveg/gorilla/expr"
	"github.com/paveg/gorilla/series"
)

func TestDataFrameGroupBy(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data
	categories := series.New("category", []string{"A", "B", "A", "B", "A"}, mem)
	values := series.New("value", []int64{10, 20, 30, 40, 50}, mem)
	prices := series.New("price", []float64{1.5, 2.0, 3.5, 4.0, 5.5}, mem)

	df := New(categories, values, prices)

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

	// Test sum aggregation
	result := df.GroupBy("category").Agg(expr.Sum(expr.Col("value")))

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

	// Test count aggregation
	result := df.GroupBy("category").Agg(expr.Count(expr.Col("value")))

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

	// Test mean aggregation
	result := df.GroupBy("category").Agg(expr.Mean(expr.Col("value")))

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

	// Test multiple aggregations
	result := df.GroupBy("category").Agg(
		expr.Sum(expr.Col("value")),
		expr.Count(expr.Col("value")),
		expr.Mean(expr.Col("value")),
	)

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

	// Test aggregation with alias
	result := df.GroupBy("category").Agg(
		expr.Sum(expr.Col("value")).As("total"),
	)

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

	// Test grouping by multiple columns
	result := df.GroupBy("cat1", "cat2").Agg(expr.Sum(expr.Col("value")))

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

	// Test lazy GroupBy with sum
	result, err := df.Lazy().GroupBy("category").Sum("value").Collect()
	if err != nil {
		t.Fatalf("Error collecting lazy groupby: %v", err)
	}

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

	// Test chaining operations: filter then group by
	result, err := df.Lazy().
		Filter(expr.Col("enabled").Eq(expr.Lit(true))).
		GroupBy("category").
		Sum("value").
		Collect()

	if err != nil {
		t.Fatalf("Error collecting chained operations: %v", err)
	}

	if result.Len() != 2 {
		t.Errorf("Expected 2 rows in result, got %d", result.Len())
	}
}

func TestGroupByEmptyDataFrame(t *testing.T) {
	df := New()

	// Test GroupBy on empty DataFrame
	result := df.GroupBy("nonexistent").Agg(expr.Sum(expr.Col("value")))

	if result.Len() != 0 {
		t.Errorf("Expected 0 rows for empty DataFrame, got %d", result.Len())
	}
}

func TestGroupByNonexistentColumn(t *testing.T) {
	mem := memory.NewGoAllocator()

	categories := series.New("category", []string{"A", "B"}, mem)
	df := New(categories)

	// Test GroupBy with non-existent column
	result := df.GroupBy("nonexistent").Agg(expr.Sum(expr.Col("value")))

	if result.Len() != 0 {
		t.Errorf("Expected 0 rows for non-existent column, got %d", result.Len())
	}
}
