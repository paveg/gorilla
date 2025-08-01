package main

import (
	"fmt"
	"log"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
)

const (
	engineeringSalary1     = 95000.0
	engineeringSalary2     = 105000.0
	engineeringSalary3     = 85000.0
	salesSalary1           = 75000.0
	salesSalary2           = 80000.0
	salesSalary3           = 70000.0
	marketingSalary1       = 85000.0
	marketingSalary2       = 90000.0
	hrSalary1              = 60000.0
	hrSalary2              = 65000.0
	engineeringBonus1      = 9500.0
	engineeringBonus2      = 10500.0
	engineeringBonus3      = 8500.0
	salesBonus1            = 7500.0
	salesBonus2            = 8000.0
	salesBonus3            = 7000.0
	marketingBonus1        = 8500.0
	marketingBonus2        = 9000.0
	hrBonus1               = 6000.0
	hrBonus2               = 6500.0
	compensationThreshold  = 100000.0
	budgetMultiplier       = 1.1
	budgetThreshold        = 300000.0
	perPersonThreshold     = 90000.0
	complexThreshold       = 95000.0
	averageSalaryThreshold = 80000.0
	minTeamSize            = 3
)

func createSampleData(mem memory.Allocator) *dataframe.DataFrame {
	departments := series.New("department", []string{
		"Engineering", "Engineering", "Engineering",
		"Sales", "Sales", "Sales",
		"Marketing", "Marketing",
		"HR", "HR",
	}, mem)

	salaries := series.New("salary", []float64{
		engineeringSalary1, engineeringSalary2, engineeringSalary3, // Engineering
		salesSalary1, salesSalary2, salesSalary3, // Sales
		marketingSalary1, marketingSalary2, // Marketing
		hrSalary1, hrSalary2, // HR
	}, mem)

	bonuses := series.New("bonus", []float64{
		engineeringBonus1, engineeringBonus2, engineeringBonus3, // Engineering
		salesBonus1, salesBonus2, salesBonus3, // Sales
		marketingBonus1, marketingBonus2, // Marketing
		hrBonus1, hrBonus2, // HR
	}, mem)

	return dataframe.New(departments, salaries, bonuses)
}

func runExample1(df *dataframe.DataFrame) error {
	fmt.Println("1. Departments with average total compensation > $100,000:")
	fmt.Println("   HAVING AVG(salary) + AVG(bonus) > 100000")

	result1, err := df.Lazy().
		GroupBy("department").
		Having(expr.Mean(expr.Col("salary")).Add(expr.Mean(expr.Col("bonus"))).Gt(expr.Lit(compensationThreshold))).
		Collect()

	if err != nil {
		return err
	}
	defer result1.Release()

	printDepartmentResults(result1)
	return nil
}

func runExample2(df *dataframe.DataFrame) error {
	fmt.Println("\n2. Departments where salary budget * 1.1 > $300,000:")
	fmt.Println("   HAVING SUM(salary) * 1.1 > 300000")

	result2, err := df.Lazy().
		GroupBy("department").
		Having(expr.Sum(expr.Col("salary")).Mul(expr.Lit(budgetMultiplier)).Gt(expr.Lit(budgetThreshold))).
		Collect()

	if err != nil {
		return err
	}
	defer result2.Release()

	printDepartmentResults(result2)
	return nil
}

func runExample3(df *dataframe.DataFrame) error {
	fmt.Println("\n3. Departments with per-person salary budget > $90,000:")
	fmt.Println("   HAVING SUM(salary) / COUNT(*) > 90000")

	result3, err := df.Lazy().
		GroupBy("department").
		Having(expr.Sum(expr.Col("salary")).Div(expr.Count(expr.Col("department"))).Gt(expr.Lit(perPersonThreshold))).
		Collect()

	if err != nil {
		return err
	}
	defer result3.Release()

	printDepartmentResults(result3)
	return nil
}

func runExample4(df *dataframe.DataFrame) error {
	fmt.Println("\n4. Departments where (total_compensation / team_size) * bonus_ratio > $95,000:")
	fmt.Println("   HAVING ((SUM(salary) + SUM(bonus)) / COUNT(*)) * (AVG(bonus) / AVG(salary)) > 95000")

	result4, err := df.Lazy().
		GroupBy("department").
		Having(
			expr.Sum(expr.Col("salary")).Add(expr.Sum(expr.Col("bonus"))).
				Div(expr.Count(expr.Col("department"))).
				Mul(expr.Mean(expr.Col("bonus")).Div(expr.Mean(expr.Col("salary")))).
				Gt(expr.Lit(complexThreshold)),
		).
		Collect()

	if err != nil {
		return err
	}
	defer result4.Release()

	printDepartmentResults(result4)
	return nil
}

func runExample5(df *dataframe.DataFrame) error {
	fmt.Println("\n5. Departments with high average salary AND reasonable team size:")
	fmt.Println("   HAVING AVG(salary) > 80000 AND COUNT(*) >= 3")

	result5, err := df.Lazy().
		GroupBy("department").
		Having(
			expr.Mean(expr.Col("salary")).Gt(expr.Lit(averageSalaryThreshold)).
				And(expr.Count(expr.Col("department")).Ge(expr.Lit(int64(minTeamSize)))),
		).
		Collect()

	if err != nil {
		return err
	}
	defer result5.Release()

	printDepartmentResults(result5)
	return nil
}

// Example demonstrating arithmetic expressions in HAVING predicates.
func main() {
	mem := memory.NewGoAllocator()
	df := createSampleData(mem)
	defer df.Release()

	fmt.Println("=== Arithmetic Expressions in HAVING Predicates ===")
	fmt.Println()

	if err := runExample1(df); err != nil {
		log.Printf("Error in example 1: %v", err)
		return
	}

	if err := runExample2(df); err != nil {
		log.Printf("Error in example 2: %v", err)
		return
	}

	if err := runExample3(df); err != nil {
		log.Printf("Error in example 3: %v", err)
		return
	}

	if err := runExample4(df); err != nil {
		log.Printf("Error in example 4: %v", err)
		return
	}

	if err := runExample5(df); err != nil {
		log.Printf("Error in example 5: %v", err)
		return
	}
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
	for i := range df.Len() {
		fmt.Printf("   - %s\n", deptCol.GetAsString(i))
	}
}
