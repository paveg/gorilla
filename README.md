# Gorilla 🦍

[![Go CI](https://github.com/paveg/gorilla/actions/workflows/ci.yml/badge.svg)](https://github.com/paveg/gorilla/actions/workflows/ci.yml)

Gorilla is a high-performance, in-memory DataFrame library for Go. Inspired
by libraries like Pandas and Polars, it is designed for fast and efficient data
manipulation.

At its core, Gorilla is built on [Apache Arrow](https://arrow.apache.org/).
This allows it to provide excellent performance and memory efficiency by
leveraging Arrow's columnar data format.

## ✨ Key Features

- **Built on Apache Arrow:** Utilizes the Arrow columnar memory format for
  zero-copy data access and efficient computation.
- **Lazy Evaluation:** Operations aren't executed immediately. Gorilla builds an
  optimized query plan and only executes it when you request the results.
- **Expressive API:** A clear, intuitive, and chainable API for filtering,
  selecting, transforming, and manipulating data.
- **Core Data Structures:** Provides two fundamental data structures:
  - `Series`: A 1-dimensional array of data, representing a single column.
  - `DataFrame`: A 2-dimensional table-like structure, composed of multiple
    `Series`.

## 🚀 Getting Started

### Installation

To add Gorilla to your project, use `go get`:

```sh
go get github.com/paveg/gorilla
```

### Memory Management

Gorilla is built on Apache Arrow, which requires explicit memory management. **We strongly recommend using the `defer` pattern** for most use cases as it provides better readability and prevents memory leaks.

#### ✅ Recommended: Defer Pattern

```go
// Create resources and immediately defer their cleanup
mem := memory.NewGoAllocator()
df := gorilla.NewDataFrame(series1, series2)
defer df.Release() // ← Clear resource lifecycle

result, err := df.Lazy().Filter(...).Collect()
defer result.Release() // ← Always clean up results
```

#### 📋 When to Use MemoryManager

For complex scenarios with many short-lived resources, you can use `MemoryManager`:

```go
err := gorilla.WithMemoryManager(mem, func(manager *gorilla.MemoryManager) error {
    // Create multiple temporary resources
    for i := 0; i < 100; i++ {
        temp := createTempDataFrame(i)
        manager.Track(temp) // Bulk cleanup at end
    }
    return processData()
})
// All tracked resources automatically released
```

### Quick Example

Here is a quick example to demonstrate the basic usage of Gorilla.

```go
package main

import (
	"fmt"
	"log"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla"
)

func main() {
	// Gorilla uses Apache Arrow for memory management.
	// Always start by creating an allocator.
	mem := memory.NewGoAllocator()

	// 1. Create some Series (columns)
	names := []string{"Alice", "Bob", "Charlie", "Diana", "Eve"}
	ages := []int64{25, 30, 35, 28, 32}
	salaries := []float64{50000.0, 60000.0, 75000.0, 55000.0, 65000.0}
	nameSeries := gorilla.NewSeries("name", names, mem)
	ageSeries := gorilla.NewSeries("age", ages, mem)
	salarySeries := gorilla.NewSeries("salary", salaries, mem)

	// Remember to release the memory when you're done.
	defer nameSeries.Release()
	defer ageSeries.Release()
	defer salarySeries.Release()

	// 2. Create a DataFrame from the Series
	df := gorilla.NewDataFrame(nameSeries, ageSeries, salarySeries)
	defer df.Release()

	fmt.Println("Original DataFrame:")
	fmt.Println(df)

	// 3. Use Lazy Evaluation for powerful transformations
	lazyDf := df.Lazy().
		// Filter for rows where age is greater than 30
		Filter(gorilla.Col("age").Gt(gorilla.Lit(int64(30)))).
		// Add a new column 'bonus'
		WithColumn("bonus", gorilla.Col("salary").Mul(gorilla.Lit(0.1))).
		// Select the columns we want in the final result
		Select("name", "age", "bonus")

	// 4. Execute the plan and collect the results
	result, err := lazyDf.Collect()
	if err != nil {
		log.Fatal(err)
	}
	defer result.Release()
	defer lazyDf.Release()

	fmt.Println("Result after lazy evaluation:")
	fmt.Println(result)
}
```

## 🤝 Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull
request.

To run the test suite, use the standard Go command:

```sh
go test ./...
```

## 📄 License

This project is licensed under the MIT License.
