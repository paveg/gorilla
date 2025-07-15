package main

import (
	"bytes"
	"fmt"
	"log"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/io"
	"github.com/paveg/gorilla/internal/series"
)

func main() {
	fmt.Println("=== Gorilla I/O Operations Demo ===")
	fmt.Println()

	// Create memory allocator
	mem := memory.NewGoAllocator()

	// Demo 1: Reading CSV data
	fmt.Println("1. Reading CSV Data:")
	csvData := `name,age,salary,active
Alice,25,50000.5,true
Bob,30,60000.0,false
Charlie,35,70000.25,true
Diana,28,55000.0,true`

	reader := io.NewCSVReader(strings.NewReader(csvData), io.DefaultCSVOptions(), mem)
	df, err := reader.Read()
	if err != nil {
		log.Fatal(err)
	}
	defer df.Release()

	fmt.Printf("Loaded DataFrame: %d rows, %d columns\n", df.Len(), df.Width())
	fmt.Printf("Columns: %v\n", df.Columns())
	fmt.Println()

	// Demo 2: Writing CSV data
	fmt.Println("2. Writing CSV Data:")
	var output bytes.Buffer
	writer := io.NewCSVWriter(&output, io.DefaultCSVOptions())
	err = writer.Write(df)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Generated CSV:")
	fmt.Print(output.String())
	fmt.Println()

	// Demo 3: Custom CSV options
	fmt.Println("3. Custom CSV Options (semicolon delimiter, no headers):")
	options := io.DefaultCSVOptions()
	options.Delimiter = ';'
	options.Header = false

	var customOutput bytes.Buffer
	customWriter := io.NewCSVWriter(&customOutput, options)
	err = customWriter.Write(df)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Generated CSV with custom options:")
	fmt.Print(customOutput.String())
	fmt.Println()

	// Demo 4: Creating DataFrame and exporting to CSV
	fmt.Println("4. Creating DataFrame and Exporting to CSV:")

	// Create sample data
	names := []string{"Eve", "Frank", "Grace", "Henry"}
	ages := []int64{32, 45, 38, 29}
	salaries := []float64{75000.0, 85000.0, 68000.0, 52000.0}
	active := []bool{true, false, true, true}

	// Create series
	namesSeries, err := series.NewSafe("employee_name", names, mem)
	if err != nil {
		log.Fatal(err)
	}
	defer namesSeries.Release()

	agesSeries, err := series.NewSafe("employee_age", ages, mem)
	if err != nil {
		log.Fatal(err)
	}
	defer agesSeries.Release()

	salariesSeries, err := series.NewSafe("employee_salary", salaries, mem)
	if err != nil {
		log.Fatal(err)
	}
	defer salariesSeries.Release()

	activeSeries, err := series.NewSafe("is_active", active, mem)
	if err != nil {
		log.Fatal(err)
	}
	defer activeSeries.Release()

	// Create DataFrame
	employeeDF := dataframe.New(namesSeries, agesSeries, salariesSeries, activeSeries)
	defer employeeDF.Release()

	// Export to CSV
	var employeeOutput bytes.Buffer
	employeeWriter := io.NewCSVWriter(&employeeOutput, io.DefaultCSVOptions())
	err = employeeWriter.Write(employeeDF)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Employee DataFrame: %d rows, %d columns\n", employeeDF.Len(), employeeDF.Width())
	fmt.Println("Exported CSV:")
	fmt.Print(employeeOutput.String())
	fmt.Println()

	// Demo 5: Round-trip (CSV → DataFrame → CSV)
	fmt.Println("5. Round-trip Processing:")

	originalCSV := `product,price,quantity
Laptop,999.99,10
Mouse,25.50,100
Keyboard,75.00,50`

	// Read CSV
	roundTripReader := io.NewCSVReader(strings.NewReader(originalCSV), io.DefaultCSVOptions(), mem)
	productDF, err := roundTripReader.Read()
	if err != nil {
		log.Fatal(err)
	}
	defer productDF.Release()

	// Write back to CSV
	var roundTripOutput bytes.Buffer
	roundTripWriter := io.NewCSVWriter(&roundTripOutput, io.DefaultCSVOptions())
	err = roundTripWriter.Write(productDF)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Original CSV:")
	fmt.Print(originalCSV)
	fmt.Println()
	fmt.Println("Round-trip result:")
	fmt.Print(roundTripOutput.String())
	fmt.Println()

	// Demo 6: Type inference demonstration
	fmt.Println("6. Type Inference:")
	mixedData := `column,value
string_col,hello
int_col,42
float_col,3.14
bool_col,true`

	mixedReader := io.NewCSVReader(strings.NewReader(mixedData), io.DefaultCSVOptions(), mem)
	mixedDF, err := mixedReader.Read()
	if err != nil {
		log.Fatal(err)
	}
	defer mixedDF.Release()

	fmt.Printf("Mixed DataFrame: %d rows, %d columns\n", mixedDF.Len(), mixedDF.Width())
	for _, colName := range mixedDF.Columns() {
		if col, exists := mixedDF.Column(colName); exists {
			fmt.Printf("Column '%s': %s\n", colName, col.DataType().Name())
		}
	}

	fmt.Println()
	fmt.Println("=== Demo Complete ===")
}
