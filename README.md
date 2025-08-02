# Gorilla 🦍

[![Go CI](https://github.com/paveg/gorilla/actions/workflows/ci.yml/badge.svg)](https://github.com/paveg/gorilla/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/paveg/gorilla/graph/badge.svg?token=0V6SRFOKHO)](https://codecov.io/gh/paveg/gorilla)

Gorilla is a high-performance, in-memory DataFrame library for Go. Inspired
by libraries like Pandas and Polars, it is designed for fast and efficient data
manipulation.

At its core, Gorilla is built on [Apache Arrow](https://arrow.apache.org/).
This allows it to provide excellent performance and memory efficiency by
leveraging Arrow's columnar data format.

## ✨ Key Features

- **Built on Apache Arrow:** Utilizes the Arrow columnar memory format for
  zero-copy data access and efficient computation.
- **Lazy Evaluation & Query Optimization:** Operations aren't executed immediately. Gorilla builds an
  optimized query plan with predicate pushdown, filter fusion, and join optimization.
- **Automatic Parallelization:** Intelligent parallel processing for datasets >1000 rows with
  adaptive worker pools and thread-safe operations.
- **Comprehensive I/O Operations:** Native CSV read/write with automatic type inference,
  configurable delimiters, and robust error handling.
- **Advanced Join Operations:** Inner, left, right, and full outer joins with multi-key
  support and automatic optimization strategies.
- **GroupBy & Aggregations:** Efficient grouping with Sum, Count, Mean, Min, Max aggregations
  and parallel execution for large datasets.
- **HAVING Clause Support:** Full SQL-compatible HAVING clause implementation for filtering
  grouped data after aggregation with high-performance optimization.
- **Streaming & Large Dataset Processing:** Handle datasets larger than memory with
  chunk-based processing and automatic disk spilling.
- **Debug & Profiling:** Built-in execution plan visualization, performance profiling,
  and bottleneck identification with `.Debug()` mode.
- **Flexible Configuration:** Multi-format config support (JSON/YAML/env) with performance
  tuning, memory management, and optimization controls.
- **Rich Data Types:** Support for string, bool, int64, int32, int16, int8, uint64, uint32, 
  uint16, uint8, float64, float32 with automatic type coercion.
- **Expressive API:** Chainable operations for filtering, selecting, transforming, sorting,
  and complex data manipulation.
- **Core Data Structures:** 
  - `Series`: A 1-dimensional array of data, representing a single column.
  - `DataFrame`: A 2-dimensional table-like structure, composed of multiple `Series`.
- **CLI Tool:** `gorilla-cli` for benchmarking, demos, and performance testing.

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

## 📊 Advanced Features

### I/O Operations

Gorilla provides comprehensive CSV I/O capabilities with automatic type inference:

```go
import "github.com/paveg/gorilla/internal/io"

// Reading CSV with automatic type inference
mem := memory.NewGoAllocator()
csvData := `name,age,salary,active
Alice,25,50000.5,true
Bob,30,60000.0,false`

reader := io.NewCSVReader(strings.NewReader(csvData), io.DefaultCSVOptions(), mem)
df, err := reader.Read()
defer df.Release()

// Writing DataFrame to CSV
var output strings.Builder
writer := io.NewCSVWriter(&output, io.DefaultCSVOptions())
err = writer.Write(df)
```

### Join Operations

Perform SQL-like joins between DataFrames:

```go
// Inner join on multiple keys
result, err := left.Join(right, gorilla.JoinOptions{
    Type: gorilla.InnerJoin,
    LeftKeys: []string{"id", "dept"},
    RightKeys: []string{"user_id", "department"},
})
defer result.Release()
```

### GroupBy and Aggregations

Efficient grouping and aggregation operations:

```go
// Group by department and calculate aggregations
result, err := df.Lazy().
    GroupBy("department").
    Agg(
        gorilla.Sum(gorilla.Col("salary")).As("total_salary"),
        gorilla.Count(gorilla.Col("*")).As("employee_count"),
        gorilla.Mean(gorilla.Col("age")).As("avg_age"),
    ).
    Collect()
defer result.Release()
```

### HAVING Clause Support

Filter grouped data after aggregation with full SQL-compatible HAVING clause support:

```go
// HAVING with aggregation functions
result, err := df.Lazy().
    GroupBy("department").
    Agg(
        gorilla.Sum(gorilla.Col("salary")).As("total_salary"),
        gorilla.Count(gorilla.Col("*")).As("employee_count"),
    ).
    Having(gorilla.Sum(gorilla.Col("salary")).Gt(gorilla.Lit(100000))).
    Collect()
defer result.Release()

// HAVING with alias references
result, err := df.Lazy().
    GroupBy("department").
    Agg(gorilla.Count(gorilla.Col("*")).As("emp_count")).
    Having(gorilla.Col("emp_count").Gt(gorilla.Lit(5))).
    Collect()
defer result.Release()

// Complex HAVING conditions
result, err := df.Lazy().
    GroupBy("department").
    Agg(
        gorilla.Mean(gorilla.Col("salary")).As("avg_salary"),
        gorilla.Count(gorilla.Col("*")).As("count"),
    ).
    Having(
        gorilla.Mean(gorilla.Col("salary")).Gt(gorilla.Lit(75000)).
        And(gorilla.Col("count").Gte(gorilla.Lit(3))),
    ).
    Collect()
defer result.Release()
```

### Debug and Profiling

Monitor performance and analyze query execution:

```go
// Enable debug mode for detailed execution analysis
debugDf := df.Debug()
result, err := debugDf.Lazy().
    Filter(gorilla.Col("salary").Gt(gorilla.Lit(50000))).
    Sort("name", true).
    Collect()

// Get execution plan and performance metrics
plan := debugDf.GetExecutionPlan()
fmt.Println("Execution plan:", plan)
```

### Configuration

Customize performance and behavior with flexible configuration:

```go
// Load configuration from file or environment
config, err := gorilla.LoadConfig("gorilla.yaml")
df := gorilla.NewDataFrame(series...).WithConfig(config.Operations)

// Or configure programmatically
config := gorilla.OperationConfig{
    ParallelThreshold: 2000,
    WorkerPoolSize: 8,
    EnableQueryOptimization: true,
    TrackMemory: true,
}
```

Example configuration file (`gorilla.yaml`):
```yaml
# Parallel Processing
parallel_threshold: 2000
worker_pool_size: 8
chunk_size: 1000

# Memory Management  
memory_threshold: 1073741824  # 1GB
gc_pressure_threshold: 0.75

# Query Optimization
filter_fusion: true
predicate_pushdown: true
join_optimization: true

# Debugging
enable_profiling: false
verbose_logging: false
metrics_collection: true
```

### Streaming Large Datasets

Process datasets larger than memory:

```go
processor := gorilla.NewStreamingProcessor(gorilla.StreamingConfig{
    ChunkSize: 10000,
    MaxMemoryMB: 1024,
})

err := processor.ProcessLargeCSV("large_dataset.csv", func(chunk *gorilla.DataFrame) error {
    // Process each chunk
    result := chunk.Filter(gorilla.Col("score").Gt(gorilla.Lit(0.8)))
    return saveResults(result)
})
```

## 🔧 CLI Tool

Gorilla includes a command-line tool for benchmarking and demonstrations:

```sh
# Build the CLI
make build

# Run interactive demo
./bin/gorilla-cli demo

# Run benchmarks
./bin/gorilla-cli benchmark
```

## ⚡ Performance

Gorilla is designed for high-performance data processing with several optimization features:

### Benchmarks

Recent performance benchmarks show excellent performance characteristics:

- **CSV I/O**: ~52μs for 100 rows, ~5.3ms for 10,000 rows
- **Parallel Processing**: Automatic scaling with adaptive worker pools
- **Memory Efficiency**: Zero-copy operations with Apache Arrow columnar format
- **Query Optimization**: 20-50% performance improvement with predicate pushdown and filter fusion

### Performance Features

- **Lazy Evaluation**: Build optimized execution plans before processing
- **Automatic Parallelization**: Intelligent parallel processing for large datasets
- **Memory Management**: Efficient Arrow-based columnar storage with GC pressure monitoring
- **Query Optimization**: Predicate pushdown, filter fusion, and join optimization
- **Streaming**: Process datasets larger than memory with configurable chunk sizes

Run benchmarks locally:
```sh
# Build and run performance tests
make build
./bin/gorilla-cli benchmark

# Run specific package benchmarks
go test -bench=. ./internal/dataframe
go test -bench=. ./internal/io
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
