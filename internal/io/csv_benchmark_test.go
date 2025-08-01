package io_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/io"
	"github.com/paveg/gorilla/internal/series"
)

// BenchmarkCSVReader benchmarks CSV reading performance.
func BenchmarkCSVReader(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("ReadCSV_%d_rows", size), func(b *testing.B) {
			// Create test CSV data
			csvData := generateCSVData(size)
			mem := memory.NewGoAllocator()

			b.ResetTimer()
			for range b.N {
				reader := io.NewCSVReader(strings.NewReader(csvData), io.DefaultCSVOptions(), mem)
				df, err := reader.Read()
				if err != nil {
					b.Fatal(err)
				}
				df.Release()
			}
		})
	}
}

// BenchmarkCSVWriter benchmarks CSV writing performance.
func BenchmarkCSVWriter(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("WriteCSV_%d_rows", size), func(b *testing.B) {
			// Create test DataFrame
			df := generateDataFrame(size)
			defer df.Release()

			b.ResetTimer()
			for range b.N {
				var buf bytes.Buffer
				writer := io.NewCSVWriter(&buf, io.DefaultCSVOptions())
				err := writer.Write(df)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkCSVRoundTrip benchmarks full read-write cycle.
func BenchmarkCSVRoundTrip(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("RoundTrip_%d_rows", size), func(b *testing.B) {
			// Create test CSV data
			csvData := generateCSVData(size)
			mem := memory.NewGoAllocator()

			b.ResetTimer()
			for range b.N {
				// Read CSV
				reader := io.NewCSVReader(strings.NewReader(csvData), io.DefaultCSVOptions(), mem)
				df, err := reader.Read()
				if err != nil {
					b.Fatal(err)
				}

				// Write CSV
				var buf bytes.Buffer
				writer := io.NewCSVWriter(&buf, io.DefaultCSVOptions())
				err = writer.Write(df)
				if err != nil {
					b.Fatal(err)
				}

				df.Release()
			}
		})
	}
}

// generateCSVData creates test CSV data with the specified number of rows.
func generateCSVData(rows int) string {
	var sb strings.Builder
	sb.WriteString("id,name,age,salary,active\n")

	for i := range rows {
		sb.WriteString(fmt.Sprintf("%d,Person_%d,%d,%.2f,%t\n",
			i, i, 25+(i%40), 30000.0+(float64(i)*100.0), i%2 == 0))
	}

	return sb.String()
}

// generateDataFrame creates a test DataFrame with the specified number of rows.
func generateDataFrame(rows int) *dataframe.DataFrame {
	mem := memory.NewGoAllocator()

	ids := make([]int64, rows)
	names := make([]string, rows)
	ages := make([]int64, rows)
	salaries := make([]float64, rows)
	active := make([]bool, rows)

	for i := range rows {
		ids[i] = int64(i)
		names[i] = fmt.Sprintf("Person_%d", i)
		ages[i] = int64(25 + (i % 40))
		salaries[i] = 30000.0 + (float64(i) * 100.0)
		active[i] = i%2 == 0
	}

	idSeries, _ := series.NewSafe("id", ids, mem)
	nameSeries, _ := series.NewSafe("name", names, mem)
	ageSeries, _ := series.NewSafe("age", ages, mem)
	salarySeries, _ := series.NewSafe("salary", salaries, mem)
	activeSeries, _ := series.NewSafe("active", active, mem)

	return dataframe.New(idSeries, nameSeries, ageSeries, salarySeries, activeSeries)
}

// BenchmarkCSVTypeInference benchmarks type inference performance.
func BenchmarkCSVTypeInference(b *testing.B) {
	mem := memory.NewGoAllocator()

	// Test different data types
	testCases := []struct {
		name    string
		csvData string
	}{
		{"int_data", "values\n1\n2\n3\n4\n5"},
		{"float_data", "values\n1.5\n2.5\n3.5\n4.5\n5.5"},
		{"bool_data", "values\ntrue\nfalse\ntrue\nfalse\ntrue"},
		{"string_data", "values\nhello\nworld\nfoo\nbar\nbaz"},
		{"mixed_data", "values\n1\nhello\n3.5\ntrue\nworld"},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			for range b.N {
				reader := io.NewCSVReader(strings.NewReader(tc.csvData), io.DefaultCSVOptions(), mem)
				df, err := reader.Read()
				if err != nil {
					b.Fatal(err)
				}
				df.Release()
			}
		})
	}
}

// BenchmarkCSVMemoryUsage benchmarks memory usage patterns.
func BenchmarkCSVMemoryUsage(b *testing.B) {
	sizes := []int{1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("MemoryUsage_%d_rows", size), func(b *testing.B) {
			csvData := generateCSVData(size)

			b.ResetTimer()
			for range b.N {
				mem := memory.NewGoAllocator()
				reader := io.NewCSVReader(strings.NewReader(csvData), io.DefaultCSVOptions(), mem)
				df, err := reader.Read()
				if err != nil {
					b.Fatal(err)
				}

				// Force memory cleanup
				df.Release()
			}
		})
	}
}
