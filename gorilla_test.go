package gorilla_test

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla"
)

func TestDataFrame_Select(t *testing.T) {
	mem := memory.NewGoAllocator()

	names := gorilla.NewSeries("name", []string{"Alice", "Bob", "Charlie"}, mem)
	ages := gorilla.NewSeries("age", []int64{25, 30, 35}, mem)
	defer names.Release()
	defer ages.Release()

	df := gorilla.NewDataFrame(names, ages)
	defer df.Release()

	selected := df.Select("age")
	defer selected.Release()

	if selected.Width() != 1 {
		t.Errorf("Expected 1 column, got %d", selected.Width())
	}

	if selected.Columns()[0] != "age" {
		t.Errorf("Expected column 'age', got '%s'", selected.Columns()[0])
	}
}

func TestDataFrame_Drop(t *testing.T) {
	mem := memory.NewGoAllocator()

	names := gorilla.NewSeries("name", []string{"Alice", "Bob", "Charlie"}, mem)
	ages := gorilla.NewSeries("age", []int64{25, 30, 35}, mem)
	defer names.Release()
	defer ages.Release()

	df := gorilla.NewDataFrame(names, ages)
	defer df.Release()

	dropped := df.Drop("name")
	defer dropped.Release()

	if dropped.Width() != 1 {
		t.Errorf("Expected 1 column, got %d", dropped.Width())
	}

	if dropped.Columns()[0] != "age" {
		t.Errorf("Expected column 'age', got '%s'", dropped.Columns()[0])
	}
}

func TestDataFrame_HasColumn(t *testing.T) {
	mem := memory.NewGoAllocator()

	names := gorilla.NewSeries("name", []string{"Alice", "Bob", "Charlie"}, mem)
	defer names.Release()

	df := gorilla.NewDataFrame(names)
	defer df.Release()

	if !df.HasColumn("name") {
		t.Error("Expected to have column 'name'")
	}

	if df.HasColumn("age") {
		t.Error("Expected to not have column 'age'")
	}
}

func TestDataFrame_String(t *testing.T) {
	mem := memory.NewGoAllocator()

	names := gorilla.NewSeries("name", []string{"Alice", "Bob"}, mem)
	ages := gorilla.NewSeries("age", []int64{25, 30}, mem)
	defer names.Release()
	defer ages.Release()

	df := gorilla.NewDataFrame(names, ages)
	defer df.Release()

	expected := `DataFrame[2x2]
  name: utf8
  age: int64`
	if !strings.Contains(df.String(), expected) {
		t.Errorf("Expected\n%s\ngot\n%s", expected, df.String())
	}
}

func TestDataFrame_Slice(t *testing.T) {
	mem := memory.NewGoAllocator()

	names := gorilla.NewSeries("name", []string{"Alice", "Bob", "Charlie"}, mem)
	defer names.Release()

	df := gorilla.NewDataFrame(names)
	defer df.Release()

	sliced := df.Slice(1, 3)
	defer sliced.Release()

	if sliced.Len() != 2 {
		t.Errorf("Expected 2 rows, got %d", sliced.Len())
	}
}

func TestDataFrame_Sort(t *testing.T) {
	mem := memory.NewGoAllocator()

	names := gorilla.NewSeries("name", []string{"Charlie", "Alice", "Bob"}, mem)
	ages := gorilla.NewSeries("age", []int64{35, 25, 30}, mem)
	defer names.Release()
	defer ages.Release()

	df := gorilla.NewDataFrame(names, ages)
	defer df.Release()

	sorted, err := df.Sort("name", true)
	if err != nil {
		t.Fatal(err)
	}
	defer sorted.Release()

	expected := "Alice"
	series, _ := sorted.Column("name")
	arr := series.Array().(*array.String)
	val := arr.Value(0)
	if val != expected {
		t.Errorf("Expected '%s', got '%s'", expected, val)
	}
}

func TestDataFrame_SortBy(t *testing.T) {
	mem := memory.NewGoAllocator()

	names := gorilla.NewSeries("name", []string{"Charlie", "Alice", "Bob"}, mem)
	ages := gorilla.NewSeries("age", []int64{30, 25, 30}, mem)
	defer names.Release()
	defer ages.Release()

	df := gorilla.NewDataFrame(names, ages)
	defer df.Release()

	sorted, err := df.SortBy([]string{"age", "name"}, []bool{true, true})
	if err != nil {
		t.Fatal(err)
	}
	defer sorted.Release()

	expected := "Alice"
	series, _ := sorted.Column("name")
	arr := series.Array().(*array.String)
	val := arr.Value(0)
	if val != expected {
		t.Errorf("Expected '%s', got '%s'", expected, val)
	}
}

func TestDataFrame_Concat(t *testing.T) {
	mem := memory.NewGoAllocator()

	names1 := gorilla.NewSeries("name", []string{"Alice"}, mem)
	df1 := gorilla.NewDataFrame(names1)
	defer names1.Release()
	defer df1.Release()

	names2 := gorilla.NewSeries("name", []string{"Bob"}, mem)
	df2 := gorilla.NewDataFrame(names2)
	defer names2.Release()
	defer df2.Release()

	concatenated := df1.Concat(df2)
	defer concatenated.Release()

	if concatenated.Len() != 2 {
		t.Errorf("Expected 2 rows, got %d", concatenated.Len())
	}
}

func TestDataFrame_GroupBy(t *testing.T) {
	mem := memory.NewGoAllocator()

	departments := gorilla.NewSeries("department", []string{"Eng", "Sales", "Eng"}, mem)
	salaries := gorilla.NewSeries("salary", []int64{100, 80, 120}, mem)
	defer departments.Release()
	defer salaries.Release()

	df := gorilla.NewDataFrame(departments, salaries)
	defer df.Release()

	result := df.GroupBy("department").Agg(gorilla.Sum(gorilla.Col("salary")).As("total_salary"))
	defer result.Release()

	if result.Len() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.Len())
	}
}

func TestDataFrame_Join(t *testing.T) {
	mem := memory.NewGoAllocator()

	empIds := gorilla.NewSeries("id", []int64{1, 2, 3}, mem)
	empNames := gorilla.NewSeries("name", []string{"Alice", "Bob", "Charlie"}, mem)
	defer empIds.Release()
	defer empNames.Release()
	employees := gorilla.NewDataFrame(empIds, empNames)
	defer employees.Release()

	deptIds := gorilla.NewSeries("id", []int64{1, 2, 3}, mem)
	deptNames := gorilla.NewSeries("department", []string{"Eng", "Sales", "Marketing"}, mem)
	defer deptIds.Release()
	defer deptNames.Release()
	departments := gorilla.NewDataFrame(deptIds, deptNames)
	defer departments.Release()

	result, err := employees.Join(departments, &gorilla.JoinOptions{
		Type:     gorilla.InnerJoin,
		LeftKey:  "id",
		RightKey: "id",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()

	if result.Width() != 3 {
		t.Errorf("Expected 3 columns, got %d", result.Width())
	}
}

func TestLazyFrame_WithColumn(t *testing.T) {
	mem := memory.NewGoAllocator()

	salaries := gorilla.NewSeries("salary", []int64{100}, mem)
	defer salaries.Release()
	df := gorilla.NewDataFrame(salaries)
	defer df.Release()

	result, err := df.Lazy().WithColumn("bonus", gorilla.Col("salary").Mul(gorilla.Lit(0.1))).Collect()
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()

	if !result.HasColumn("bonus") {
		t.Error("Expected to have column 'bonus'")
	}
}

func TestLazyGroupBy_Sum(t *testing.T) {
	mem := memory.NewGoAllocator()

	departments := gorilla.NewSeries("department", []string{"Eng", "Sales", "Eng"}, mem)
	salaries := gorilla.NewSeries("salary", []int64{100, 80, 120}, mem)
	defer departments.Release()
	defer salaries.Release()

	df := gorilla.NewDataFrame(departments, salaries)
	defer df.Release()

	result, err := df.Lazy().GroupBy("department").Sum("salary").Collect()
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()

	if result.Len() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.Len())
	}
}

func TestLazyGroupBy_Count(t *testing.T) {
	mem := memory.NewGoAllocator()

	departments := gorilla.NewSeries("department", []string{"Eng", "Sales", "Eng"}, mem)
	salaries := gorilla.NewSeries("salary", []int64{100, 80, 120}, mem)
	defer departments.Release()
	defer salaries.Release()

	df := gorilla.NewDataFrame(departments, salaries)
	defer df.Release()

	result, err := df.Lazy().GroupBy("department").Count("salary").Collect()
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()

	if result.Len() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.Len())
	}
}

func TestLazyGroupBy_Mean(t *testing.T) {
	mem := memory.NewGoAllocator()

	departments := gorilla.NewSeries("department", []string{"Eng", "Sales", "Eng"}, mem)
	salaries := gorilla.NewSeries("salary", []int64{100, 80, 120}, mem)
	defer departments.Release()
	defer salaries.Release()

	df := gorilla.NewDataFrame(departments, salaries)
	defer df.Release()

	result, err := df.Lazy().GroupBy("department").Mean("salary").Collect()
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()

	if result.Len() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.Len())
	}
}

func TestLazyGroupBy_Min(t *testing.T) {
	mem := memory.NewGoAllocator()

	departments := gorilla.NewSeries("department", []string{"Eng", "Sales", "Eng"}, mem)
	salaries := gorilla.NewSeries("salary", []int64{100, 80, 120}, mem)
	defer departments.Release()
	defer salaries.Release()

	df := gorilla.NewDataFrame(departments, salaries)
	defer df.Release()

	result, err := df.Lazy().GroupBy("department").Min("salary").Collect()
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()

	if result.Len() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.Len())
	}
}

func TestLazyGroupBy_Max(t *testing.T) {
	mem := memory.NewGoAllocator()

	departments := gorilla.NewSeries("department", []string{"Eng", "Sales", "Eng"}, mem)
	salaries := gorilla.NewSeries("salary", []int64{100, 80, 120}, mem)
	defer departments.Release()
	defer salaries.Release()

	df := gorilla.NewDataFrame(departments, salaries)
	defer df.Release()

	result, err := df.Lazy().GroupBy("department").Max("salary").Collect()
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()

	if result.Len() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.Len())
	}
}

func TestVersion(t *testing.T) {
	if gorilla.Version() == "" {
		t.Error("Expected version to be non-empty")
	}
}

func TestBuildInfo(t *testing.T) {
	info := gorilla.BuildInfo()
	if info.Version == "" {
		t.Error("Expected version to be non-empty")
	}
}

func TestExample(t *testing.T) {
	// This test just runs the example to ensure it doesn't panic.
	gorilla.Example()
}

func TestExampleDataFrameGroupBy(t *testing.T) {
	// This test just runs the example to ensure it doesn't panic.
	gorilla.ExampleDataFrameGroupBy()
}

func TestExampleDataFrameJoin(t *testing.T) {
	// This test just runs the example to ensure it doesn't panic.
	gorilla.ExampleDataFrameJoin()
}

func TestLazyFrame_String(t *testing.T) {
	mem := memory.NewGoAllocator()

	series := gorilla.NewSeries("a", []int64{1, 2, 3}, mem)
	defer series.Release()

	df := gorilla.NewDataFrame(series)
	defer df.Release()

	lf := df.Lazy().Filter(gorilla.Col("a").Gt(gorilla.Lit(1)))
	if !strings.Contains(strings.ToLower(lf.String()), "filter") {
		t.Errorf("Expected string representation to contain 'filter', got %s", lf.String())
	}
}

func TestLazyFrame_Release(t *testing.T) {
	mem := memory.NewGoAllocator()

	df := gorilla.NewDataFrame(
		gorilla.NewSeries("a", []int64{1, 2, 3}, mem),
	)
	defer df.Release()

	lf := df.Lazy()
	lf.Release() // Should not panic
}

func TestGroupBy_Agg(t *testing.T) {
	mem := memory.NewGoAllocator()

	df := gorilla.NewDataFrame(
		gorilla.NewSeries("key", []string{"a", "a", "b"}, mem),
		gorilla.NewSeries("val", []int64{1, 2, 3}, mem),
	)
	defer df.Release()

	result := df.GroupBy("key").Agg(gorilla.Sum(gorilla.Col("val")))
	defer result.Release()

	if result.Len() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.Len())
	}
}

func TestLazyGroupBy_Agg(t *testing.T) {
	mem := memory.NewGoAllocator()

	df := gorilla.NewDataFrame(
		gorilla.NewSeries("key", []string{"a", "a", "b"}, mem),
		gorilla.NewSeries("val", []int64{1, 2, 3}, mem),
	)
	defer df.Release()

	result, err := df.Lazy().GroupBy("key").Agg(gorilla.Sum(gorilla.Col("val"))).Collect()
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()

	if result.Len() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.Len())
	}
}
