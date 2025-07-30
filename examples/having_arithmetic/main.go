package main

import (
	"fmt"
	"log"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
)

// Example demonstrating arithmetic expressions in HAVING predicates.
func main() {
	mem := memory.NewGoAllocator()

	// Sample data: employees in different departments
	departments := series.New("department", []string{
		"Engineering", "Engineering", "Engineering",
		"Sales", "Sales", "Sales",
		"Marketing", "Marketing",
		"HR", "HR",
	}, mem)

	salaries := series.New("salary", []float64{
		95000, 105000, 85000, // Engineering
		75000, 80000, 70000, // Sales
		85000, 90000, // Marketing
		60000, 65000, // HR
	}, mem)

	bonuses := series.New("bonus", []float64{
		9500, 10500, 8500, // Engineering
		7500, 8000, 7000, // Sales
		8500, 9000, // Marketing
		6000, 6500, // HR
	}, mem)

	df := dataframe.New(departments, salaries, bonuses)
	defer df.Release()

	fmt.Println("=== Arithmetic Expressions in HAVING Predicates ===")
	fmt.Println()

	// Example 1: Basic arithmetic - find departments where average total compensation > $100k
	fmt.Println("1. Departments with average total compensation > $100,000:")
	fmt.Println("   HAVING AVG(salary) + AVG(bonus) > 100000")

	result1, err := df.Lazy().
		GroupBy("department").
		Having(expr.Mean(expr.Col("salary")).Add(expr.Mean(expr.Col("bonus"))).Gt(expr.Lit(100000.0))).
		Collect()

	if err != nil {
		log.Fatal(err)
	}
	defer result1.Release()

	printDepartmentResults(result1)

	// Example 2: Complex arithmetic - departments where salary budget * 1.1 exceeds $300k
	fmt.Println("\n2. Departments where salary budget * 1.1 > $300,000:")
	fmt.Println("   HAVING SUM(salary) * 1.1 > 300000")

	result2, err := df.Lazy().
		GroupBy("department").
		Having(expr.Sum(expr.Col("salary")).Mul(expr.Lit(1.1)).Gt(expr.Lit(300000.0))).
		Collect()

	if err != nil {
		log.Fatal(err)
	}
	defer result2.Release()

	printDepartmentResults(result2)

	// Example 3: Arithmetic with division - departments with per-person budget > $90k
	fmt.Println("\n3. Departments with per-person salary budget > $90,000:")
	fmt.Println("   HAVING SUM(salary) / COUNT(*) > 90000")

	result3, err := df.Lazy().
		GroupBy("department").
		Having(expr.Sum(expr.Col("salary")).Div(expr.Count(expr.Col("department"))).Gt(expr.Lit(90000.0))).
		Collect()

	if err != nil {
		log.Fatal(err)
	}
	defer result3.Release()

	printDepartmentResults(result3)

	// Example 4: Complex nested arithmetic with multiple aggregations
	fmt.Println("\n4. Departments where (total_compensation / team_size) * bonus_ratio > $95,000:")
	fmt.Println("   HAVING ((SUM(salary) + SUM(bonus)) / COUNT(*)) * (AVG(bonus) / AVG(salary)) > 95000")

	result4, err := df.Lazy().
		GroupBy("department").
		Having(
			expr.Sum(expr.Col("salary")).Add(expr.Sum(expr.Col("bonus"))).
				Div(expr.Count(expr.Col("department"))).
				Mul(expr.Mean(expr.Col("bonus")).Div(expr.Mean(expr.Col("salary")))).
				Gt(expr.Lit(95000.0)),
		).
		Collect()

	if err != nil {
		log.Fatal(err)
	}
	defer result4.Release()

	printDepartmentResults(result4)

	// Example 5: Arithmetic with logical operators
	fmt.Println("\n5. Departments with high average salary AND reasonable team size:")
	fmt.Println("   HAVING AVG(salary) > 80000 AND COUNT(*) >= 3")

	result5, err := df.Lazy().
		GroupBy("department").
		Having(
			expr.Mean(expr.Col("salary")).Gt(expr.Lit(80000.0)).
				And(expr.Count(expr.Col("department")).Ge(expr.Lit(int64(3)))),
		).
		Collect()

	if err != nil {
		log.Fatal(err)
	}
	defer result5.Release()

	printDepartmentResults(result5)
}

func printDepartmentResults(df *dataframe.DataFrame) {
	if df.Len() == 0 {
		fmt.Println("   No departments match the criteria.")
		return
	}

	deptCol, ok := df.Column("department")
	if !ok {
		fmt.Println("   Error: department column not found")
		return
	}

	fmt.Printf("   Matching departments (%d):\n", df.Len())
	for i := 0; i < df.Len(); i++ {
		fmt.Printf("   - %s\n", deptCol.GetAsString(i))
	}
}
