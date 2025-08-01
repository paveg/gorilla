# Gorilla DataFrame Performance Optimization Guide

This guide provides comprehensive strategies for optimizing performance when working with the Gorilla DataFrame library. It covers memory management, query optimization, parallel processing, and best practices for different use cases.

## Table of Contents

1. [Memory Management Best Practices](#memory-management-best-practices)
2. [Query Optimization](#query-optimization)
3. [Parallel Processing](#parallel-processing)
4. [Data Type Optimization](#data-type-optimization)
5. [I/O Performance](#io-performance)
6. [Profiling and Monitoring](#profiling-and-monitoring)
7. [Common Performance Anti-Patterns](#common-performance-anti-patterns)

## Memory Management Best Practices

### 1. Use Defer Pattern for Resource Cleanup

Always use the `defer` pattern immediately after creating resources to prevent memory leaks:

```go
// ✅ GOOD: Immediate defer cleanup
func processData() error {
    mem := memory.NewGoAllocator()
    
    df := gorilla.NewDataFrame(series1, series2)
    defer df.Release() // Clean ownership and lifecycle
    
    result, err := df.Lazy().Filter(...).Collect()
    if err != nil {
        return err
    }
    defer result.Release() // Always clean up results
    
    // Use result...
    return nil
}

// ❌ BAD: Manual cleanup prone to leaks
func processDataBad() error {
    df := gorilla.NewDataFrame(series1, series2)
    result, err := df.Lazy().Filter(...).Collect()
    if err != nil {
        df.Release() // Easy to forget in error paths
        return err
    }
    
    // Use result...
    df.Release()
    result.Release()
    return nil
}
```

### 2. Chunk Large Operations

For datasets larger than 1GB, process data in chunks to manage memory usage:

```go
func processLargeDataset(df *gorilla.DataFrame) error {
    const chunkSize = 100000
    totalRows := df.Len()
    
    for start := 0; start < totalRows; start += chunkSize {
        end := start + chunkSize
        if end > totalRows {
            end = totalRows
        }
        
        // Process chunk with automatic cleanup
        chunk := df.Slice(start, end)
        defer chunk.Release()
        
        result, err := chunk.Lazy().
            Filter(gorilla.Col("active").Eq(gorilla.Lit(true))).
            Collect()
        if err != nil {
            return err
        }
        defer result.Release()
        
        // Process result...
    }
    
    return nil
}
```

### 3. Memory-Aware Configuration

Configure memory usage for large dataset processing:

```go
func configureMemoryProcessing(df *gorilla.DataFrame) *gorilla.DataFrame {
    config := config.OperationConfig{
        MaxMemoryUsage:  512 * 1024 * 1024, // 512MB limit
        CustomChunkSize: 50000,              // Smaller chunks for memory pressure
        ForceParallel:   false,              // Disable for memory-constrained environments
    }
    
    return df.WithConfig(config)
}
```

### 4. Streaming Processing for Very Large Data

Use streaming patterns for datasets that don't fit in memory:

```go
func streamingProcessing() error {
    mem := memory.NewGoAllocator()
    
    processor := gorilla.NewStreamingProcessor(gorilla.StreamingConfig{
        ChunkSize:   100000,
        MaxMemoryMB: 512,
        SpillToDisk: true,
    })
    
    return processor.ProcessChunks(func(chunk *gorilla.DataFrame) error {
        defer chunk.Release()
        
        // Process each chunk
        result, err := chunk.Lazy().
            Filter(gorilla.Col("valid").Eq(gorilla.Lit(true))).
            Collect()
        if err != nil {
            return err
        }
        defer result.Release()
        
        // Write or accumulate results
        return nil
    })
}
```

## Query Optimization

### 1. Filter Early and Often

Place filters as early as possible in your query pipeline:

```go
// ✅ GOOD: Filter early to reduce data size
func optimizedQuery(df *gorilla.DataFrame) *gorilla.DataFrame {
    result, err := df.Lazy().
        // Apply filters first
        Filter(gorilla.Col("date").Gt(gorilla.Lit("2023-01-01"))).
        Filter(gorilla.Col("status").Eq(gorilla.Lit("active"))).
        Filter(gorilla.Col("amount").Gt(gorilla.Lit(100.0))).
        
        // Then select needed columns
        Select("customer_id", "amount", "date").
        
        // Finally perform expensive operations
        GroupBy("customer_id").
        Agg(gorilla.Sum(gorilla.Col("amount")).As("total")).
        Collect()
    
    return result
}

// ❌ BAD: Filter after expensive operations
func unoptimizedQuery(df *gorilla.DataFrame) *gorilla.DataFrame {
    result, err := df.Lazy().
        // Expensive operations on full dataset
        GroupBy("customer_id", "product_id", "date").
        Agg(gorilla.Sum(gorilla.Col("amount")).As("total")).
        
        // Late filtering requires processing all data first
        Filter(gorilla.Col("total").Gt(gorilla.Lit(1000.0))).
        Collect()
    
    return result
}
```

### 2. Column Selection Optimization

Select only the columns you need before expensive operations:

```go
func columnOptimization(df *gorilla.DataFrame) *gorilla.DataFrame {
    // Select early to reduce memory and processing overhead
    result, err := df.Lazy().
        Select("id", "name", "amount", "category"). // Only needed columns
        Filter(gorilla.Col("amount").Gt(gorilla.Lit(0.0))).
        GroupBy("category").
        Agg(
            gorilla.Sum(gorilla.Col("amount")).As("total"),
            gorilla.Count(gorilla.Col("*")).As("count"),
        ).
        Collect()
    
    return result
}
```

### 3. Aggregation Optimization

Use efficient aggregation patterns:

```go
func aggregationOptimization(df *gorilla.DataFrame) *gorilla.DataFrame {
    // Use HAVING clauses for post-aggregation filtering
    result, err := df.Lazy().
        GroupBy("department", "level").
        Agg(
            gorilla.Sum(gorilla.Col("salary")).As("total_salary"),
            gorilla.Count(gorilla.Col("*")).As("employee_count"),
            gorilla.Mean(gorilla.Col("performance")).As("avg_performance"),
        ).
        // HAVING clause is more efficient than filtering after collection
        Having(gorilla.Col("employee_count").Gt(gorilla.Lit(int64(5)))).
        Having(gorilla.Col("total_salary").Gt(gorilla.Lit(500000.0))).
        Collect()
    
    return result
}
```

### 4. Join Optimization

Choose the right join strategy and order:

```go
func joinOptimization() *gorilla.DataFrame {
    // Order joins from smallest to largest tables
    // Join small dimension tables first
    result, err := factTable.Lazy().
        // Small dimension table first (automatic broadcast join)
        Join(smallDimension.Lazy(), &gorilla.JoinOptions{
            Type:     gorilla.InnerJoin,
            LeftKey:  "dim1_id",
            RightKey: "id",
        }).
        // Larger dimension table second
        Join(mediumDimension.Lazy(), &gorilla.JoinOptions{
            Type:     gorilla.InnerJoin,
            LeftKey:  "dim2_id",
            RightKey: "id",
        }).
        // Filter after joins to reduce result set
        Filter(gorilla.Col("active").Eq(gorilla.Lit(true))).
        Collect()
    
    return result
}
```

## Parallel Processing

### 1. Automatic Parallelization

Gorilla automatically parallelizes operations for datasets with 1000+ rows. You can tune this behavior:

```go
func parallelizationConfig(df *gorilla.DataFrame) *gorilla.DataFrame {
    config := config.OperationConfig{
        ParallelThreshold: 5000,                     // Parallel for 5000+ rows
        WorkerPoolSize:    runtime.NumCPU() * 2,     // 2x CPU cores
        ForceParallel:     true,                     // Force parallel even for small data
        CustomChunkSize:   10000,                    // Optimize chunk size
    }
    
    configuredDF := df.WithConfig(config)
    defer configuredDF.Release()
    
    // Operations will use optimal parallel processing
    result, err := configuredDF.Lazy().
        Filter(gorilla.Col("amount").Gt(gorilla.Lit(100.0))).
        GroupBy("category").
        Agg(gorilla.Sum(gorilla.Col("amount")).As("total")).
        Collect()
    
    return result
}
```

### 2. Memory-Safe Parallel Processing

For memory-intensive operations, use memory-safe parallel patterns:

```go
func memorySafeParallel(df *gorilla.DataFrame) error {
    return parallel.ProcessMemorySafe(df, parallel.MemorySafeConfig{
        MaxConcurrency: runtime.NumCPU(),
        MemoryLimitMB:  256,
        AllocatorPool:  parallel.NewAllocatorPool(4),
    }, func(chunk *gorilla.DataFrame, allocator memory.Allocator) error {
        defer chunk.Release()
        
        // Process chunk with dedicated allocator
        result, err := chunk.Lazy().
            Filter(gorilla.Col("valid").Eq(gorilla.Lit(true))).
            Collect()
        if err != nil {
            return err
        }
        defer result.Release()
        
        // Handle result...
        return nil
    })
}
```

### 3. CPU-Intensive Operations

Optimize for CPU-bound workloads:

```go
func cpuIntensiveOptimization(df *gorilla.DataFrame) *gorilla.DataFrame {
    // Configure for CPU-intensive processing
    config := config.OperationConfig{
        WorkerPoolSize:   runtime.NumCPU(),
        ForceParallel:    true,
        CustomChunkSize:  1000, // Smaller chunks for better load distribution
        CPUIntensive:     true,
    }
    
    result, err := df.WithConfig(config).Lazy().
        // CPU-intensive calculations
        WithColumn("complex_calc",
            gorilla.Col("value1").Mul(gorilla.Col("value2")).
                Add(gorilla.Col("value3").Div(gorilla.Col("value4"))),
        ).
        WithColumn("score",
            gorilla.Case().
                When(gorilla.Col("complex_calc").Gt(gorilla.Lit(100.0)), gorilla.Lit("High")).
                When(gorilla.Col("complex_calc").Gt(gorilla.Lit(50.0)), gorilla.Lit("Medium")).
                Otherwise(gorilla.Lit("Low")),
        ).
        Collect()
    
    return result
}
```

## Data Type Optimization

### 1. Choose Appropriate Data Types

Use the most efficient data types for your use case:

```go
func dataTypeOptimization(mem memory.Allocator) *gorilla.DataFrame {
    // Use smaller integer types when possible
    ids := gorilla.NewSeries("id", []int32{1, 2, 3}, mem)          // int32 vs int64
    ages := gorilla.NewSeries("age", []int8{25, 30, 35}, mem)      // int8 for ages
    scores := gorilla.NewSeries("score", []float32{95.5, 87.2}, mem) // float32 vs float64
    
    defer ids.Release()
    defer ages.Release()
    defer scores.Release()
    
    return gorilla.NewDataFrame(ids, ages, scores)
}
```

### 2. String Optimization

Optimize string handling for performance:

```go
func stringOptimization(df *gorilla.DataFrame) *gorilla.DataFrame {
    result, err := df.Lazy().
        // Use categorical data for repeated strings
        WithColumn("category_code", 
            gorilla.Case().
                When(gorilla.Col("category").Eq(gorilla.Lit("A")), gorilla.Lit(int32(1))).
                When(gorilla.Col("category").Eq(gorilla.Lit("B")), gorilla.Lit(int32(2))).
                Otherwise(gorilla.Lit(int32(0))),
        ).
        
        // Avoid string operations in tight loops
        Filter(gorilla.Col("category_code").Gt(gorilla.Lit(int32(0)))).
        Collect()
    
    return result
}
```

## I/O Performance

### 1. Efficient CSV Reading

Optimize CSV reading for large files:

```go
func optimizedCSVReading(filename string) (*gorilla.DataFrame, error) {
    mem := memory.NewGoAllocator()
    
    // Configure CSV reader for performance
    config := io.CSVConfig{
        ChunkSize:    100000,   // Read in chunks
        ParallelRead: true,     // Parallel parsing
        SkipRows:     1,        // Skip header
        BufferSize:   64 * 1024, // 64KB buffer
    }
    
    df, err := io.ReadCSVWithConfig(filename, mem, config)
    if err != nil {
        return nil, err
    }
    
    return df, nil
}
```

### 2. Parquet Optimization

Use Parquet for better I/O performance:

```go
func parquetOptimization(df *gorilla.DataFrame, filename string) error {
    // Write with compression for storage efficiency
    config := io.ParquetWriteConfig{
        Compression: "SNAPPY",  // Fast compression
        ChunkSize:   100000,    // Optimal chunk size
        Parallel:    true,      // Parallel writing
    }
    
    return io.WriteParquetWithConfig(df, filename, config)
}

func readParquetOptimized(filename string, mem memory.Allocator) (*gorilla.DataFrame, error) {
    config := io.ParquetReadConfig{
        Columns:     []string{"id", "name", "amount"}, // Column pruning
        BatchSize:   50000,                            // Optimal batch size
        Parallel:    true,                            // Parallel reading
    }
    
    return io.ReadParquetWithConfig(filename, mem, config)
}
```

## Profiling and Monitoring

### 1. Memory Profiling

Monitor memory usage during processing:

```go
func profileMemoryUsage(df *gorilla.DataFrame) {
    // Enable memory monitoring
    monitor := memory.NewUsageMonitor()
    defer monitor.Stop()
    
    monitor.SetThreshold(512 * 1024 * 1024) // 512MB threshold
    monitor.OnThresholdExceeded(func(usage int64) {
        log.Printf("Memory usage exceeded threshold: %d MB", usage/(1024*1024))
    })
    
    // Process data with monitoring
    result, err := df.Lazy().
        Filter(gorilla.Col("active").Eq(gorilla.Lit(true))).
        GroupBy("category").
        Agg(gorilla.Sum(gorilla.Col("amount")).As("total")).
        Collect()
    if err != nil {
        log.Fatal(err)
    }
    defer result.Release()
    
    // Print memory statistics
    stats := monitor.GetStats()
    log.Printf("Peak memory usage: %d MB", stats.PeakUsage/(1024*1024))
    log.Printf("Total allocations: %d", stats.TotalAllocations)
}
```

### 2. Performance Benchmarking

Benchmark critical operations:

```go
func BenchmarkAggregation(b *testing.B) {
    mem := memory.NewGoAllocator()
    df := createLargeTestDataFrame(mem, 100000) // 100K rows
    defer df.Release()
    
    b.ResetTimer()
    b.ReportAllocs()
    
    for i := 0; i < b.N; i++ {
        result, err := df.Lazy().
            GroupBy("category").
            Agg(gorilla.Sum(gorilla.Col("amount")).As("total")).
            Collect()
        if err != nil {
            b.Fatal(err)
        }
        result.Release()
    }
}
```

### 3. Query Plan Analysis

Analyze query execution plans:

```go
func analyzeQueryPlan(df *gorilla.DataFrame) {
    // Enable debug mode for query plan visualization
    debugConfig := config.OperationConfig{
        DebugMode: true,
        LogLevel:  "DEBUG",
    }
    
    debugDF := df.WithConfig(debugConfig)
    defer debugDF.Release()
    
    result, err := debugDF.Lazy().
        Filter(gorilla.Col("amount").Gt(gorilla.Lit(100.0))).
        GroupBy("category").
        Agg(gorilla.Sum(gorilla.Col("amount")).As("total")).
        Collect()
    if err != nil {
        log.Fatal(err)
    }
    defer result.Release()
    
    // Query plan and execution statistics are logged
}
```

## Common Performance Anti-Patterns

### 1. ❌ Late Filtering

```go
// BAD: Filter after expensive operations
result := df.Lazy().
    GroupBy("col1", "col2", "col3").
    Agg(gorilla.Sum(gorilla.Col("amount")).As("total")).
    Filter(gorilla.Col("total").Gt(gorilla.Lit(1000.0))). // Too late!
    Collect()
```

### 2. ❌ Unnecessary Column Selection

```go
// BAD: Process all columns when only few are needed
result := df.Lazy().
    Filter(gorilla.Col("active").Eq(gorilla.Lit(true))).
    GroupBy("category").
    Agg(gorilla.Sum(gorilla.Col("amount")).As("total")).
    Select("category", "total"). // Should select earlier
    Collect()
```

### 3. ❌ Memory Leaks

```go
// BAD: Forgetting to release resources
func memoryLeak() *gorilla.DataFrame {
    df := gorilla.NewDataFrame(series1, series2)
    // Missing: defer df.Release()
    
    result, _ := df.Lazy().Filter(...).Collect()
    // Missing: defer result.Release()
    
    return result // Memory leak!
}
```

### 4. ❌ Inefficient Joins

```go
// BAD: Join large tables first
result := largeTable1.Join(largeTable2, opts).
    Join(smallDimension, opts). // Should join small table first
    Filter(...).
    Collect()
```

### 5. ❌ Wrong Data Types

```go
// BAD: Using int64 for small values
ages := gorilla.NewSeries("age", []int64{25, 30, 35}, mem) // Wasteful

// GOOD: Use appropriate size
ages := gorilla.NewSeries("age", []int8{25, 30, 35}, mem) // Efficient
```

## Performance Checklist

- [ ] Use `defer` for all resource cleanup
- [ ] Filter early in query pipeline
- [ ] Select only needed columns before expensive operations
- [ ] Use appropriate data types for your data
- [ ] Configure parallel processing for large datasets
- [ ] Use chunking for very large datasets
- [ ] Profile memory usage for memory-intensive operations
- [ ] Benchmark critical code paths
- [ ] Use Parquet for better I/O performance
- [ ] Avoid late filtering and unnecessary column processing
- [ ] Monitor memory usage in production

Following these guidelines will help you achieve optimal performance with the Gorilla DataFrame library while maintaining code clarity and reliability.
