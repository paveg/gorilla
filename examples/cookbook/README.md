# Gorilla DataFrame Cookbook

This cookbook provides practical examples and patterns for common DataFrame operations and use cases. Each section includes working code examples that demonstrate best practices for the Gorilla DataFrame library.

## Table of Contents

1. [Basic Operations](#basic-operations)
2. [Data Cleaning and Transformation](#data-cleaning-and-transformation)  
3. [Aggregation Patterns](#aggregation-patterns)
4. [Join Operations](#join-operations)
5. [Conditional Logic](#conditional-logic)
6. [Memory Management](#memory-management)
7. [Performance Optimization](#performance-optimization)
8. [Error Handling](#error-handling)

## Basic Operations

### Creating DataFrames from Different Data Types

```go
package main

import (
    "github.com/apache/arrow-go/v18/arrow/memory"
    "github.com/paveg/gorilla"
)

func createDataFrameExamples() {
    mem := memory.NewGoAllocator()
    
    // From slices
    names := gorilla.NewSeries("name", []string{"Alice", "Bob", "Charlie"}, mem)
    ages := gorilla.NewSeries("age", []int64{25, 30, 35}, mem)
    scores := gorilla.NewSeries("score", []float64{95.5, 87.2, 92.1}, mem)
    active := gorilla.NewSeries("active", []bool{true, false, true}, mem)
    defer names.Release()
    defer ages.Release()
    defer scores.Release()
    defer active.Release()
    
    df := gorilla.NewDataFrame(names, ages, scores, active)
    defer df.Release()
    
    // Basic operations
    fmt.Printf("Shape: %d rows x %d columns\n", df.Len(), df.Width())
    fmt.Printf("Columns: %v\n", df.Columns())
    
    // Check if column exists
    if df.HasColumn("age") {
        column, _ := df.Column("age")
        fmt.Printf("Age column length: %d\n", column.Len())
    }
}
```

### Selecting and Filtering Data

```go
func selectionExamples(df *gorilla.DataFrame) {
    // Select specific columns
    subset := df.Select("name", "age")
    defer subset.Release()
    
    // Drop columns
    reduced := df.Drop("score")
    defer reduced.Release()
    
    // Filter rows with conditions
    adults, err := df.Lazy().
        Filter(gorilla.Col("age").Gt(gorilla.Lit(int64(25)))).
        Collect()
    if err != nil {
        log.Fatal(err)
    }
    defer adults.Release()
    
    // Complex filtering with multiple conditions
    qualified, err := df.Lazy().
        Filter(
            gorilla.Col("age").Gt(gorilla.Lit(int64(20))).And(
                gorilla.Col("score").Gt(gorilla.Lit(90.0)),
            ).And(
                gorilla.Col("active").Eq(gorilla.Lit(true)),
            ),
        ).
        Collect()
    if err != nil {
        log.Fatal(err)
    }
    defer qualified.Release()
}
```

## Data Cleaning and Transformation

### Handling Missing and Invalid Data

```go
func dataCleaningExamples(df *gorilla.DataFrame) {
    cleaned, err := df.Lazy().
        // Replace negative ages with default value
        WithColumn("age_clean",
            gorilla.If(
                gorilla.Col("age").Lt(gorilla.Lit(int64(0))),
                gorilla.Lit(int64(25)), // Default age
                gorilla.Col("age"),
            ),
        ).
        
        // Clean string data
        WithColumn("name_clean",
            gorilla.If(
                gorilla.Col("name").Eq(gorilla.Lit("")),
                gorilla.Lit("Unknown"),
                gorilla.Col("name"),
            ),
        ).
        
        // Create data quality flags
        WithColumn("is_valid",
            gorilla.Col("age").Gt(gorilla.Lit(int64(0))).And(
                gorilla.Col("name").Ne(gorilla.Lit("")),
            ),
        ).
        
        // Filter out invalid records
        Filter(gorilla.Col("is_valid").Eq(gorilla.Lit(true))).
        
        Collect()
    
    if err != nil {
        log.Fatal(err)
    }
    defer cleaned.Release()
}
```

### Creating Derived Columns

```go
func transformationExamples(df *gorilla.DataFrame) {
    transformed, err := df.Lazy().
        // Mathematical transformations
        WithColumn("age_squared", gorilla.Col("age").Mul(gorilla.Col("age"))).
        WithColumn("score_normalized", gorilla.Col("score").Div(gorilla.Lit(100.0))).
        
        // String transformations
        WithColumn("name_upper", gorilla.Col("name").Upper()).
        WithColumn("name_length", gorilla.Col("name").Len()).
        
        // Categorical transformations
        WithColumn("age_group",
            gorilla.Case().
                When(gorilla.Col("age").Lt(gorilla.Lit(int64(25))), gorilla.Lit("Young")).
                When(gorilla.Col("age").Lt(gorilla.Lit(int64(40))), gorilla.Lit("Middle")).
                Else(gorilla.Lit("Senior")),
        ).
        
        // Performance tiers
        WithColumn("performance",
            gorilla.Case().
                When(gorilla.Col("score").Gt(gorilla.Lit(95.0)), gorilla.Lit("Excellent")).
                When(gorilla.Col("score").Gt(gorilla.Lit(85.0)), gorilla.Lit("Good")).
                When(gorilla.Col("score").Gt(gorilla.Lit(70.0)), gorilla.Lit("Average")).
                Else(gorilla.Lit("Below Average")),
        ).
        
        Collect()
    
    if err != nil {
        log.Fatal(err)
    }
    defer transformed.Release()
}
```

## Aggregation Patterns

### Basic Aggregations

```go
func basicAggregationExamples(df *gorilla.DataFrame) {
    // Single column aggregations
    summary, err := df.Lazy().
        GroupBy("age_group").
        Agg(
            gorilla.Count(gorilla.Col("*")).As("count"),
            gorilla.Mean(gorilla.Col("score")).As("avg_score"),
            gorilla.Min(gorilla.Col("score")).As("min_score"),
            gorilla.Max(gorilla.Col("score")).As("max_score"),
            gorilla.Sum(gorilla.Col("age")).As("total_age"),
        ).
        Collect()
    
    if err != nil {
        log.Fatal(err)
    }
    defer summary.Release()
}
```

### Conditional Aggregations

```go
func conditionalAggregationExamples(df *gorilla.DataFrame) {
    analysis, err := df.Lazy().
        GroupBy("department").
        Agg(
            // Count active employees only
            gorilla.Sum(
                gorilla.If(
                    gorilla.Col("active").Eq(gorilla.Lit(true)),
                    gorilla.Lit(int64(1)),
                    gorilla.Lit(int64(0)),
                ),
            ).As("active_count"),
            
            // Sum salary only for senior employees
            gorilla.Sum(
                gorilla.If(
                    gorilla.Col("level").Eq(gorilla.Lit("Senior")),
                    gorilla.Col("salary"),
                    gorilla.Lit(int64(0)),
                ),
            ).As("senior_salary_total"),
            
            // Average score for high performers only  
            gorilla.Mean(
                gorilla.If(
                    gorilla.Col("performance_rating").Gt(gorilla.Lit(4.0)),
                    gorilla.Col("score"),
                    gorilla.Lit(0.0), // This approach may skew results
                ),
            ).As("high_performer_avg_score"),
        ).
        Collect()
    
    if err != nil {
        log.Fatal(err)
    }
    defer analysis.Release()
}
```

### Multi-Level Grouping

```go
func multiLevelGroupingExamples(df *gorilla.DataFrame) {
    hierarchical, err := df.Lazy().
        GroupBy("year", "quarter", "department", "level").
        Agg(
            gorilla.Count(gorilla.Col("*")).As("employee_count"),
            gorilla.Sum(gorilla.Col("salary")).As("total_salary"),
            gorilla.Mean(gorilla.Col("performance_rating")).As("avg_rating"),
        ).
        
        // Add calculated fields after grouping
        WithColumn("avg_salary_per_employee",
            gorilla.Col("total_salary").Div(gorilla.Col("employee_count")),
        ).
        
        // Filter for significant groups
        Filter(gorilla.Col("employee_count").Gt(gorilla.Lit(int64(5)))).
        
        // Sort by hierarchy
        SortBy(
            []string{"year", "quarter", "total_salary"}, 
            []bool{true, true, false},
        ).
        
        Collect()
    
    if err != nil {
        log.Fatal(err)
    }
    defer hierarchical.Release()
}
```

## Join Operations

### Basic Joins

```go
func joinExamples() {
    mem := memory.NewGoAllocator()
    
    // Create employee data
    empIds := gorilla.NewSeries("id", []int64{1, 2, 3, 4}, mem)
    empNames := gorilla.NewSeries("name", []string{"Alice", "Bob", "Charlie", "Diana"}, mem)
    deptIds := gorilla.NewSeries("dept_id", []int64{10, 20, 10, 30}, mem)
    defer empIds.Release()
    defer empNames.Release() 
    defer deptIds.Release()
    
    employees := gorilla.NewDataFrame(empIds, empNames, deptIds)
    defer employees.Release()
    
    // Create department data
    dIds := gorilla.NewSeries("id", []int64{10, 20, 30}, mem)
    dNames := gorilla.NewSeries("department", []string{"Engineering", "Sales", "Marketing"}, mem)
    defer dIds.Release()
    defer dNames.Release()
    
    departments := gorilla.NewDataFrame(dIds, dNames)
    defer departments.Release()
    
    // Inner join
    innerResult, err := employees.Join(departments, &gorilla.JoinOptions{
        Type:     gorilla.InnerJoin,
        LeftKey:  "dept_id",
        RightKey: "id",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer innerResult.Release()
    
    // Left join to keep all employees
    leftResult, err := employees.Join(departments, &gorilla.JoinOptions{
        Type:     gorilla.LeftJoin,
        LeftKey:  "dept_id", 
        RightKey: "id",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer leftResult.Release()
}
```

### Multi-Key Joins

```go
func multiKeyJoinExamples() {
    mem := memory.NewGoAllocator()
    
    // Create sales data with composite keys
    regions := gorilla.NewSeries("region", []string{"North", "South", "North", "South"}, mem)
    products := gorilla.NewSeries("product", []string{"A", "A", "B", "B"}, mem)
    sales := gorilla.NewSeries("sales", []int64{100, 150, 200, 180}, mem)
    defer regions.Release()
    defer products.Release()
    defer sales.Release()
    
    salesData := gorilla.NewDataFrame(regions, products, sales)
    defer salesData.Release()
    
    // Create target data with same composite keys
    tRegions := gorilla.NewSeries("region", []string{"North", "South", "North", "South"}, mem)
    tProducts := gorilla.NewSeries("product", []string{"A", "A", "B", "B"}, mem)  
    targets := gorilla.NewSeries("target", []int64{120, 140, 180, 200}, mem)
    defer tRegions.Release()
    defer tProducts.Release()
    defer targets.Release()
    
    targetData := gorilla.NewDataFrame(tRegions, tProducts, targets)
    defer targetData.Release()
    
    // Multi-key join
    comparison, err := salesData.Join(targetData, &gorilla.JoinOptions{
        Type:      gorilla.InnerJoin,
        LeftKeys:  []string{"region", "product"},
        RightKeys: []string{"region", "product"},
    })
    if err != nil {
        log.Fatal(err)
    }
    defer comparison.Release()
    
    // Add performance analysis
    analysis, err := comparison.Lazy().
        WithColumn("performance_pct",
            gorilla.Col("sales").Div(gorilla.Col("target")).Mul(gorilla.Lit(100.0)),
        ).
        WithColumn("variance",
            gorilla.Col("sales").Sub(gorilla.Col("target")),
        ).
        WithColumn("status",
            gorilla.Case().
                When(gorilla.Col("performance_pct").Gt(gorilla.Lit(100.0)), gorilla.Lit("Above Target")).
                When(gorilla.Col("performance_pct").Gt(gorilla.Lit(90.0)), gorilla.Lit("Near Target")).
                Else(gorilla.Lit("Below Target")),
        ).
        Collect()
    
    if err != nil {
        log.Fatal(err)
    }
    defer analysis.Release()
}
```

## Conditional Logic

### Complex Case Expressions

```go
func conditionalLogicExamples(df *gorilla.DataFrame) {
    complex, err := df.Lazy().
        // Multi-criteria scoring
        WithColumn("risk_score",
            gorilla.Case().
                When(
                    gorilla.Col("age").Lt(gorilla.Lit(int64(25))).And(
                        gorilla.Col("income").Lt(gorilla.Lit(30000.0)),
                    ),
                    gorilla.Lit("High Risk"),
                ).
                When(
                    gorilla.Col("age").Gt(gorilla.Lit(int64(65))).Or(
                        gorilla.Col("credit_score").Lt(gorilla.Lit(600.0)),
                    ),
                    gorilla.Lit("Medium Risk"),
                ).
                When(
                    gorilla.Col("income").Gt(gorilla.Lit(100000.0)).And(
                        gorilla.Col("credit_score").Gt(gorilla.Lit(750.0)),
                    ),
                    gorilla.Lit("Low Risk"),
                ).
                Else(gorilla.Lit("Standard Risk")),
        ).
        
        // Nested conditional logic
        WithColumn("loan_eligibility",
            gorilla.If(
                gorilla.Col("risk_score").Eq(gorilla.Lit("High Risk")),
                gorilla.Lit("Not Eligible"),
                gorilla.If(
                    gorilla.Col("income").Gt(
                        gorilla.Col("requested_amount").Mul(gorilla.Lit(3.0)),
                    ),
                    gorilla.Lit("Eligible"),
                    gorilla.Lit("Manual Review"),
                ),
            ),
        ).
        
        Collect()
    
    if err != nil {
        log.Fatal(err)
    }
    defer complex.Release()
}
```

### Coalesce and Null Handling

```go
func nullHandlingExamples(df *gorilla.DataFrame) {
    cleaned, err := df.Lazy().
        // Handle null values with coalesce
        WithColumn("phone_clean",
            gorilla.Coalesce(
                gorilla.Col("mobile_phone"),
                gorilla.Col("home_phone"), 
                gorilla.Col("work_phone"),
                gorilla.Lit("No Phone"),
            ),
        ).
        
        // Default values for missing data
        WithColumn("email_clean",
            gorilla.Coalesce(
                gorilla.Col("primary_email"),
                gorilla.Col("secondary_email"),
                gorilla.Concat(
                    gorilla.Col("username"),
                    gorilla.Lit("@company.com"),
                ),
            ),
        ).
        
        Collect()
    
    if err != nil {
        log.Fatal(err)
    }
    defer cleaned.Release()
}
```

## Memory Management

### Proper Resource Cleanup

```go
func memoryManagementExamples() {
    mem := memory.NewGoAllocator()
    
    // Best practice: defer cleanup immediately after creation
    processLargeDataset := func() error {
        // Create large dataset
        data := generateLargeDataset(mem) // Assume this function exists
        defer data.Release() // Always defer cleanup
        
        // Process in chunks to manage memory
        const chunkSize = 10000
        totalRows := data.Len()
        
        for start := 0; start < totalRows; start += chunkSize {
            end := start + chunkSize
            if end > totalRows {
                end = totalRows
            }
            
            // Process chunk
            chunk := data.Slice(start, end)
            defer chunk.Release() // Clean up chunk
            
            result, err := chunk.Lazy().
                Filter(gorilla.Col("active").Eq(gorilla.Lit(true))).
                Collect()
            if err != nil {
                return err
            }
            defer result.Release() // Clean up result
            
            // Process result...
            fmt.Printf("Processed chunk %d-%d: %d active records\n", start, end, result.Len())
        }
        
        return nil
    }
    
    if err := processLargeDataset(); err != nil {
        log.Fatal(err)
    }
    // All resources automatically cleaned up via deferred calls
}
```

### Memory-Efficient Operations

```go
func memoryEfficientExamples(df *gorilla.DataFrame) {
    // Use lazy evaluation to minimize memory allocation
    efficient, err := df.Lazy().
        // Filter early to reduce data size
        Filter(gorilla.Col("active").Eq(gorilla.Lit(true))).
        Filter(gorilla.Col("score").Gt(gorilla.Lit(70.0))).
        
        // Select only needed columns
        Select("id", "name", "score", "department").
        
        // Perform aggregations on reduced dataset
        GroupBy("department").
        Agg(
            gorilla.Count(gorilla.Col("*")).As("count"),
            gorilla.Mean(gorilla.Col("score")).As("avg_score"),
        ).
        
        // Final execution
        Collect()
    
    if err != nil {
        log.Fatal(err)
    }
    defer efficient.Release()
}
```

## Performance Optimization

### Optimizing Query Plans

```go
func performanceOptimizationExamples(df *gorilla.DataFrame) {
    // Good: Filter early, aggregate late
    optimized, err := df.Lazy().
        // 1. Filter first to reduce dataset size
        Filter(gorilla.Col("date").Gt(gorilla.Lit("2023-01-01"))).
        Filter(gorilla.Col("status").Eq(gorilla.Lit("active"))).
        
        // 2. Select only needed columns
        Select("customer_id", "product_id", "amount", "date").
        
        // 3. Perform expensive operations on reduced data
        GroupBy("customer_id", "product_id").
        Agg(
            gorilla.Sum(gorilla.Col("amount")).As("total_amount"),
            gorilla.Count(gorilla.Col("*")).As("transaction_count"),
        ).
        
        // 4. Filter aggregated results
        Filter(gorilla.Col("total_amount").Gt(gorilla.Lit(1000.0))).
        
        Collect()
    
    if err != nil {
        log.Fatal(err)
    }
    defer optimized.Release()
    
    // Bad: Aggregate first, filter late (less efficient)
    // This would process all data before filtering
}
```

### Parallel Processing Configuration

```go
func parallelProcessingExamples(df *gorilla.DataFrame) {
    // Configure for parallel processing
    config := config.OperationConfig{
        ForceParallel:   true,
        CustomChunkSize: 5000,
        MaxMemoryUsage:  1024 * 1024 * 100, // 100MB
    }
    
    configuredDF := df.WithConfig(config)
    defer configuredDF.Release()
    
    // Operations will use parallel processing
    result, err := configuredDF.Lazy().
        Filter(gorilla.Col("amount").Gt(gorilla.Lit(100.0))).
        GroupBy("category").
        Agg(gorilla.Sum(gorilla.Col("amount")).As("total")).
        Collect()
    
    if err != nil {
        log.Fatal(err)
    }
    defer result.Release()
}
```

## Error Handling

### Comprehensive Error Handling

```go
func errorHandlingExamples(df *gorilla.DataFrame) {
    // Wrap operations in error handling
    safeProcess := func(df *gorilla.DataFrame) (*gorilla.DataFrame, error) {
        // Validate input
        if df.Len() == 0 {
            return nil, fmt.Errorf("empty DataFrame provided")
        }
        
        if !df.HasColumn("amount") {
            return nil, fmt.Errorf("required column 'amount' not found")
        }
        
        // Process with error handling
        result, err := df.Lazy().
            Filter(gorilla.Col("amount").Gt(gorilla.Lit(0.0))).
            GroupBy("category").
            Agg(gorilla.Sum(gorilla.Col("amount")).As("total")).
            Collect()
        
        if err != nil {
            return nil, fmt.Errorf("processing failed: %w", err)
        }
        
        // Validate output
        if result.Len() == 0 {
            result.Release()
            return nil, fmt.Errorf("no results after processing")
        }
        
        return result, nil
    }
    
    result, err := safeProcess(df)
    if err != nil {
        log.Printf("Processing error: %v", err)
        return
    }
    defer result.Release()
    
    fmt.Printf("Successfully processed %d categories\n", result.Len())
}
```

### Recovery and Fallback Patterns

```go
func recoveryPatternExamples(df *gorilla.DataFrame) {
    // Try complex operation with fallback
    processWithFallback := func(df *gorilla.DataFrame) *gorilla.DataFrame {
        // Try optimized approach first
        result, err := df.Lazy().
            // Complex operation that might fail
            GroupBy("complex_key").
            Agg(gorilla.Mean(gorilla.Col("complex_calculation")).As("avg")).
            Collect()
        
        if err != nil {
            log.Printf("Complex operation failed: %v, falling back to simple aggregation", err)
            
            // Fallback to simpler operation
            fallback, fallbackErr := df.Lazy().
                GroupBy("simple_key").
                Agg(gorilla.Count(gorilla.Col("*")).As("count")).
                Collect()
            
            if fallbackErr != nil {
                log.Printf("Fallback also failed: %v", fallbackErr)
                return nil
            }
            
            return fallback
        }
        
        return result
    }
    
    result := processWithFallback(df)
    if result != nil {
        defer result.Release()
        fmt.Printf("Processing completed with %d results\n", result.Len())
    }
}
```

## Best Practices Summary

1. **Always use `defer` for resource cleanup** immediately after creation
2. **Filter early** in your query pipeline to reduce data size
3. **Select only needed columns** before expensive operations
4. **Use lazy evaluation** for complex multi-step operations
5. **Handle errors gracefully** with proper validation and fallbacks
6. **Test with realistic data sizes** to identify performance bottlenecks
7. **Use appropriate data types** for your use case
8. **Consider memory usage** for large datasets
9. **Leverage parallel processing** for performance-critical operations
10. **Validate data quality** throughout your pipeline

For more examples, see the individual example programs in the `examples/` directory.