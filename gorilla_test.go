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

	keySeries := gorilla.NewSeries("key", []string{"a", "a", "b"}, mem)
	defer keySeries.Release()
	valSeries := gorilla.NewSeries("val", []int64{1, 2, 3}, mem)
	defer valSeries.Release()

	df := gorilla.NewDataFrame(
		keySeries,
		valSeries,
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

func TestLazyFrame_Sort(t *testing.T) {
	mem := memory.NewGoAllocator()

	names := gorilla.NewSeries("name", []string{"Charlie", "Alice", "Bob"}, mem)
	ages := gorilla.NewSeries("age", []int64{35, 25, 30}, mem)
	defer names.Release()
	defer ages.Release()

	df := gorilla.NewDataFrame(names, ages)
	defer df.Release()

	result, err := df.Lazy().Sort("name", true).Collect()
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()

	expected := "Alice"
	series, _ := result.Column("name")
	arr := series.Array().(*array.String)
	val := arr.Value(0)
	if val != expected {
		t.Errorf("Expected '%s', got '%s'", expected, val)
	}
}

func TestLazyFrame_SortBy(t *testing.T) {
	mem := memory.NewGoAllocator()

	names := gorilla.NewSeries("name", []string{"Charlie", "Alice", "Bob"}, mem)
	ages := gorilla.NewSeries("age", []int64{30, 25, 30}, mem)
	defer names.Release()
	defer ages.Release()

	df := gorilla.NewDataFrame(names, ages)
	defer df.Release()

	result, err := df.Lazy().SortBy([]string{"age", "name"}, []bool{true, true}).Collect()
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()

	expected := "Alice"
	series, _ := result.Column("name")
	arr := series.Array().(*array.String)
	val := arr.Value(0)
	if val != expected {
		t.Errorf("Expected '%s', got '%s'", expected, val)
	}
}

func TestLazyFrame_Join(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Left DataFrame
	leftIds := gorilla.NewSeries("id", []int64{1, 2, 3}, mem)
	leftNames := gorilla.NewSeries("name", []string{"Alice", "Bob", "Charlie"}, mem)
	defer leftIds.Release()
	defer leftNames.Release()
	leftDf := gorilla.NewDataFrame(leftIds, leftNames)
	defer leftDf.Release()

	// Right DataFrame
	rightIds := gorilla.NewSeries("id", []int64{1, 2, 4}, mem)
	rightDepts := gorilla.NewSeries("dept", []string{"Eng", "Sales", "Marketing"}, mem)
	defer rightIds.Release()
	defer rightDepts.Release()
	rightDf := gorilla.NewDataFrame(rightIds, rightDepts)
	defer rightDf.Release()

	// Test inner join
	result, err := leftDf.Lazy().Join(rightDf.Lazy(), &gorilla.JoinOptions{
		Type:     gorilla.InnerJoin,
		LeftKey:  "id",
		RightKey: "id",
	}).Collect()
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()

	if result.Len() != 2 {
		t.Errorf("Expected 2 rows for inner join, got %d", result.Len())
	}
}

func TestExpression_Mean(t *testing.T) {
	mem := memory.NewGoAllocator()

	departments := gorilla.NewSeries("dept", []string{"Eng", "Sales", "Eng", "Sales"}, mem)
	salaries := gorilla.NewSeries("salary", []int64{100, 80, 120, 90}, mem)
	defer departments.Release()
	defer salaries.Release()

	df := gorilla.NewDataFrame(departments, salaries)
	defer df.Release()

	result := df.GroupBy("dept").Agg(
		gorilla.Mean(gorilla.Col("salary")).As("avg_salary"),
	)
	defer result.Release()

	if result.Len() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.Len())
	}

	// Check averages for both departments
	deptCol, _ := result.Column("dept")
	avgCol, _ := result.Column("avg_salary")
	deptArr := deptCol.Array().(*array.String)
	avgArr := avgCol.Array().(*array.Float64)

	expectedAvgs := map[string]float64{
		"Eng":   110.0, // (100 + 120) / 2
		"Sales": 85.0,  // (80 + 90) / 2
	}

	foundDepts := make(map[string]bool)
	for i := 0; i < result.Len(); i++ {
		dept := deptArr.Value(i)
		avg := avgArr.Value(i)

		if expectedAvg, exists := expectedAvgs[dept]; exists {
			foundDepts[dept] = true
			if avg != expectedAvg {
				t.Errorf("Expected avg salary for %s to be %f, got %f", dept, expectedAvg, avg)
			}
		} else {
			t.Errorf("Unexpected department: %s", dept)
		}
	}

	// Ensure both departments were found
	for dept := range expectedAvgs {
		if !foundDepts[dept] {
			t.Errorf("Department %s not found in results", dept)
		}
	}
}

func TestExpression_Min(t *testing.T) {
	mem := memory.NewGoAllocator()

	departments := gorilla.NewSeries("dept", []string{"Eng", "Sales", "Eng", "Sales"}, mem)
	salaries := gorilla.NewSeries("salary", []int64{100, 80, 120, 90}, mem)
	defer departments.Release()
	defer salaries.Release()

	df := gorilla.NewDataFrame(departments, salaries)
	defer df.Release()

	result := df.GroupBy("dept").Agg(
		gorilla.Min(gorilla.Col("salary")).As("min_salary"),
	)
	defer result.Release()

	if result.Len() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.Len())
	}

	// Check minimum for Sales department (should be 80)
	deptCol, _ := result.Column("dept")
	minCol, _ := result.Column("min_salary")
	deptArr := deptCol.Array().(*array.String)
	minArr := minCol.Array().(*array.Float64)

	for i := 0; i < result.Len(); i++ {
		if deptArr.Value(i) == "Sales" {
			if minArr.Value(i) != 80.0 {
				t.Errorf("Expected min salary for Sales to be 80, got %f", minArr.Value(i))
			}
		}
	}
}

func TestExpression_Max(t *testing.T) {
	mem := memory.NewGoAllocator()

	departments := gorilla.NewSeries("dept", []string{"Eng", "Sales", "Eng", "Sales"}, mem)
	salaries := gorilla.NewSeries("salary", []int64{100, 80, 120, 90}, mem)
	defer departments.Release()
	defer salaries.Release()

	df := gorilla.NewDataFrame(departments, salaries)
	defer df.Release()

	result := df.GroupBy("dept").Agg(
		gorilla.Max(gorilla.Col("salary")).As("max_salary"),
	)
	defer result.Release()

	if result.Len() != 2 {
		t.Errorf("Expected 2 rows, got %d", result.Len())
	}

	// Check maximum for Eng department (should be 120)
	deptCol, _ := result.Column("dept")
	maxCol, _ := result.Column("max_salary")
	deptArr := deptCol.Array().(*array.String)
	maxArr := maxCol.Array().(*array.Float64)

	for i := 0; i < result.Len(); i++ {
		if deptArr.Value(i) == "Eng" {
			if maxArr.Value(i) != 120.0 {
				t.Errorf("Expected max salary for Eng to be 120, got %f", maxArr.Value(i))
			}
		}
	}
}

func TestExpression_If(t *testing.T) {
	t.Skip("If expression not fully implemented yet")
	mem := memory.NewGoAllocator()

	salaries := gorilla.NewSeries("salary", []int64{100, 80, 120}, mem)
	defer salaries.Release()

	df := gorilla.NewDataFrame(salaries)
	defer df.Release()

	// Add a column with If expression: if salary > 100 then "high" else "low"
	result, err := df.Lazy().WithColumn("level",
		gorilla.If(
			gorilla.Col("salary").Gt(gorilla.Lit(100)),
			gorilla.Lit("high"),
			gorilla.Lit("low"),
		),
	).Collect()
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()

	levelCol, _ := result.Column("level")
	levelArr := levelCol.Array().(*array.String)

	expected := []string{"low", "low", "high"}
	for i := 0; i < result.Len(); i++ {
		if levelArr.Value(i) != expected[i] {
			t.Errorf("At index %d: expected '%s', got '%s'", i, expected[i], levelArr.Value(i))
		}
	}
}

func TestExpression_Coalesce(t *testing.T) {
	t.Skip("Coalesce expression needs nullable series support")
	mem := memory.NewGoAllocator()

	// Create series with null values
	values := gorilla.NewSeries("value", []interface{}{100, nil, 300}, mem)
	defaults := gorilla.NewSeries("default", []int64{50, 50, 50}, mem)
	defer values.Release()
	defer defaults.Release()

	df := gorilla.NewDataFrame(values, defaults)
	defer df.Release()

	// Use Coalesce to get value or default
	result, err := df.Lazy().WithColumn("final",
		gorilla.Coalesce(
			gorilla.Col("value"),
			gorilla.Col("default"),
		),
	).Collect()
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()

	finalCol, _ := result.Column("final")
	finalArr := finalCol.Array().(*array.Int64)

	expected := []int64{100, 50, 300}
	for i := 0; i < result.Len(); i++ {
		if finalArr.Value(i) != expected[i] {
			t.Errorf("At index %d: expected %d, got %d", i, expected[i], finalArr.Value(i))
		}
	}
}

func TestExpression_Concat(t *testing.T) {
	t.Skip("Concat expression not fully implemented yet")
	mem := memory.NewGoAllocator()

	firstNames := gorilla.NewSeries("first", []string{"John", "Jane", "Bob"}, mem)
	lastNames := gorilla.NewSeries("last", []string{"Doe", "Smith", "Johnson"}, mem)
	defer firstNames.Release()
	defer lastNames.Release()

	df := gorilla.NewDataFrame(firstNames, lastNames)
	defer df.Release()

	// Concatenate first and last names with a space
	result, err := df.Lazy().WithColumn("full_name",
		gorilla.Concat(
			gorilla.Col("first"),
			gorilla.Lit(" "),
			gorilla.Col("last"),
		),
	).Collect()
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()

	fullNameCol, _ := result.Column("full_name")
	fullNameArr := fullNameCol.Array().(*array.String)

	expected := []string{"John Doe", "Jane Smith", "Bob Johnson"}
	for i := 0; i < result.Len(); i++ {
		if fullNameArr.Value(i) != expected[i] {
			t.Errorf("At index %d: expected '%s', got '%s'", i, expected[i], fullNameArr.Value(i))
		}
	}
}

func TestExpression_Case(t *testing.T) {
	t.Skip("Case expression not fully implemented yet")
	mem := memory.NewGoAllocator()

	scores := gorilla.NewSeries("score", []int64{95, 75, 85, 60}, mem)
	defer scores.Release()

	df := gorilla.NewDataFrame(scores)
	defer df.Release()

	// Use Case expression to assign grades
	result, err := df.Lazy().WithColumn("grade",
		gorilla.Case().
			When(gorilla.Col("score").Ge(gorilla.Lit(90)), gorilla.Lit("A")).
			When(gorilla.Col("score").Ge(gorilla.Lit(80)), gorilla.Lit("B")).
			When(gorilla.Col("score").Ge(gorilla.Lit(70)), gorilla.Lit("C")).
			Else(gorilla.Lit("F")),
	).Collect()
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()

	gradeCol, _ := result.Column("grade")
	gradeArr := gradeCol.Array().(*array.String)

	expected := []string{"A", "C", "B", "F"}
	for i := 0; i < result.Len(); i++ {
		if gradeArr.Value(i) != expected[i] {
			t.Errorf("At index %d: expected '%s', got '%s'", i, expected[i], gradeArr.Value(i))
		}
	}
}

func TestDataFrame_Sort_Error(t *testing.T) {
	mem := memory.NewGoAllocator()

	names := gorilla.NewSeries("name", []string{"Alice", "Bob"}, mem)
	defer names.Release()

	df := gorilla.NewDataFrame(names)
	defer df.Release()

	// Try to sort by non-existent column
	_, err := df.Sort("nonexistent", true)
	if err == nil {
		t.Error("Expected error when sorting by non-existent column")
	}
}

func TestDataFrame_SortBy_Error(t *testing.T) {
	mem := memory.NewGoAllocator()

	names := gorilla.NewSeries("name", []string{"Alice", "Bob"}, mem)
	defer names.Release()

	df := gorilla.NewDataFrame(names)
	defer df.Release()

	// Try to sort by non-existent column
	_, err := df.SortBy([]string{"nonexistent"}, []bool{true})
	if err == nil {
		t.Error("Expected error when sorting by non-existent column")
	}

	// Mismatched lengths
	_, err = df.SortBy([]string{"name"}, []bool{true, false})
	if err == nil {
		t.Error("Expected error when columns and ascending have different lengths")
	}
}

func TestDataFrame_Join_Error(t *testing.T) {
	mem := memory.NewGoAllocator()

	leftIds := gorilla.NewSeries("id", []int64{1, 2}, mem)
	leftNames := gorilla.NewSeries("name", []string{"Alice", "Bob"}, mem)
	defer leftIds.Release()
	defer leftNames.Release()
	leftDf := gorilla.NewDataFrame(leftIds, leftNames)
	defer leftDf.Release()

	rightIds := gorilla.NewSeries("id", []int64{1, 2}, mem)
	rightDepts := gorilla.NewSeries("dept", []string{"Eng", "Sales"}, mem)
	defer rightIds.Release()
	defer rightDepts.Release()
	rightDf := gorilla.NewDataFrame(rightIds, rightDepts)
	defer rightDf.Release()

	// Try to join with non-existent key
	_, err := leftDf.Join(rightDf, &gorilla.JoinOptions{
		Type:     gorilla.InnerJoin,
		LeftKey:  "nonexistent",
		RightKey: "id",
	})
	if err == nil {
		t.Error("Expected error when joining with non-existent left key")
	}

	// Try to join with non-existent right key
	_, err = leftDf.Join(rightDf, &gorilla.JoinOptions{
		Type:     gorilla.InnerJoin,
		LeftKey:  "id",
		RightKey: "nonexistent",
	})
	if err == nil {
		t.Error("Expected error when joining with non-existent right key")
	}
}

func TestLazyFrame_Collect_Error(t *testing.T) {
	mem := memory.NewGoAllocator()

	values := gorilla.NewSeries("value", []int64{1, 2, 3}, mem)
	defer values.Release()

	df := gorilla.NewDataFrame(values)
	defer df.Release()

	// Try to filter with invalid expression - this should cause an error
	_, err := df.Lazy().Filter(gorilla.Col("nonexistent").Gt(gorilla.Lit(1))).Collect()
	if err == nil {
		t.Error("Expected error when filtering with non-existent column")
	}
}
