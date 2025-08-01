# Migration Guide: From Pandas/Polars to Gorilla DataFrame

This guide helps users familiar with Pandas (Python) or Polars (Python/Rust) transition to the Gorilla DataFrame library. It provides side-by-side comparisons and explains the key differences in approach and syntax.

## Table of Contents

1. [Key Differences](#key-differences)
2. [Basic Operations](#basic-operations)
3. [Data Types and Creation](#data-types-and-creation)
4. [Filtering and Selection](#filtering-and-selection)
5. [Aggregations and GroupBy](#aggregations-and-groupby)
6. [Joins](#joins)
7. [Memory Management](#memory-management)
8. [Performance Considerations](#performance-considerations)
9. [Common Patterns](#common-patterns)

## Key Differences

### Architecture Differences

| Aspect | Pandas | Polars | Gorilla |
|--------|--------|--------|---------|
| **Language** | Python | Python/Rust | Go |
| **Memory Model** | NumPy arrays | Apache Arrow | Apache Arrow |
| **Evaluation** | Eager | Lazy (optional) | Lazy + Eager |
| **Parallelization** | Limited | Automatic | Automatic |
| **Memory Management** | GC-based | Rust-managed | Manual Release |
| **Type System** | Dynamic | Static | Static with generics |

### Philosophy Differences

- **Pandas**: Object-oriented, mutable DataFrames with extensive API surface
- **Polars**: Functional, immutable DataFrames with query optimization  
- **Gorilla**: Functional, immutable with explicit memory management and Go idioms

## Basic Operations

### Creating DataFrames

**Pandas:**
```python
import pandas as pd

df = pd.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35],
    'score': [95.5, 87.2, 92.1]
})
```

**Polars:**
```python
import polars as pl

df = pl.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35],
    'score': [95.5, 87.2, 92.1]
})
```

**Gorilla:**
```go
import (
    "github.com/apache/arrow-go/v18/arrow/memory"
    "github.com/paveg/gorilla"
)

mem := memory.NewGoAllocator()

names := gorilla.NewSeries("name", []string{"Alice", "Bob", "Charlie"}, mem)
ages := gorilla.NewSeries("age", []int64{25, 30, 35}, mem)  
scores := gorilla.NewSeries("score", []float64{95.5, 87.2, 92.1}, mem)
defer names.Release()
defer ages.Release()
defer scores.Release()

df := gorilla.NewDataFrame(names, ages, scores)
defer df.Release()
```

### Basic Information

**Pandas:**
```python
print(df.shape)        # (3, 3)
print(df.columns)      # ['name', 'age', 'score']
print(df.info())
```

**Polars:**
```python
print(df.shape)        # (3, 3)
print(df.columns)      # ['name', 'age', 'score'] 
print(df.describe())
```

**Gorilla:**
```go
fmt.Printf("Shape: %d rows x %d columns\n", df.Len(), df.Width())
fmt.Printf("Columns: %v\n", df.Columns())
fmt.Println(df) // String representation
```

## Data Types and Creation

### From CSV Files

**Pandas:**
```python
df = pd.read_csv('data.csv')
```

**Polars:**
```python
df = pl.read_csv('data.csv')
```

**Gorilla:**
```go
df, err := io.ReadCSV("data.csv", mem)
if err != nil {
    log.Fatal(err)
}
defer df.Release()
```

### Type Specifications

**Pandas:**
```python
df = pd.DataFrame({
    'id': pd.Series([1, 2, 3], dtype='int32'),
    'value': pd.Series([1.1, 2.2, 3.3], dtype='float32')
})
```

**Polars:**
```python
df = pl.DataFrame({
    'id': pl.Series([1, 2, 3], dtype=pl.Int32),
    'value': pl.Series([1.1, 2.2, 3.3], dtype=pl.Float32)
})
```

**Gorilla:**
```go
ids := gorilla.NewSeries("id", []int32{1, 2, 3}, mem)
values := gorilla.NewSeries("value", []float32{1.1, 2.2, 3.3}, mem)
defer ids.Release()
defer values.Release()

df := gorilla.NewDataFrame(ids, values)
defer df.Release()
```

## Filtering and Selection

### Column Selection

**Pandas:**
```python
subset = df[['name', 'age']]
```

**Polars:**
```python
subset = df.select(['name', 'age'])
```

**Gorilla:**
```go
subset := df.Select("name", "age")
defer subset.Release()
```

### Row Filtering

**Pandas:**
```python
adults = df[df['age'] > 25]
```

**Polars:**
```python
adults = df.filter(pl.col('age') > 25)
```

**Gorilla:**
```go
adults, err := df.Lazy().
    Filter(gorilla.Col("age").Gt(gorilla.Lit(int64(25)))).
    Collect()
if err != nil {
    log.Fatal(err)
}
defer adults.Release()
```

### Complex Filtering

**Pandas:**
```python
result = df[(df['age'] > 25) & (df['score'] > 90)]
```

**Polars:**
```python
result = df.filter((pl.col('age') > 25) & (pl.col('score') > 90))
```

**Gorilla:**
```go
result, err := df.Lazy().
    Filter(
        gorilla.Col("age").Gt(gorilla.Lit(int64(25))).And(
            gorilla.Col("score").Gt(gorilla.Lit(90.0)),
        ),
    ).
    Collect()
if err != nil {
    log.Fatal(err)
}
defer result.Release()
```

### Adding Columns

**Pandas:**
```python
df['age_group'] = df['age'].apply(lambda x: 'Young' if x < 30 else 'Old')
```

**Polars:**
```python
df = df.with_columns([
    pl.when(pl.col('age') < 30)
      .then(pl.lit('Young'))
      .otherwise(pl.lit('Old'))
      .alias('age_group')
])
```

**Gorilla:**
```go
result, err := df.Lazy().
    WithColumn("age_group",
        gorilla.If(
            gorilla.Col("age").Lt(gorilla.Lit(int64(30))),
            gorilla.Lit("Young"),
            gorilla.Lit("Old"),
        ),
    ).
    Collect()
if err != nil {
    log.Fatal(err)
}
defer result.Release()
```

## Aggregations and GroupBy

### Basic GroupBy

**Pandas:**
```python
result = df.groupby('department').agg({
    'salary': ['sum', 'mean', 'count']
})
```

**Polars:**
```python
result = df.group_by('department').agg([
    pl.col('salary').sum().alias('total_salary'),
    pl.col('salary').mean().alias('avg_salary'),
    pl.col('salary').count().alias('employee_count')
])
```

**Gorilla:**
```go
result, err := df.Lazy().
    GroupBy("department").
    Agg(
        gorilla.Sum(gorilla.Col("salary")).As("total_salary"),
        gorilla.Mean(gorilla.Col("salary")).As("avg_salary"),
        gorilla.Count(gorilla.Col("salary")).As("employee_count"),
    ).
    Collect()
if err != nil {
    log.Fatal(err)
}
defer result.Release()
```

### Multiple Group Keys

**Pandas:**
```python
result = df.groupby(['department', 'level']).agg({
    'salary': 'sum'
})
```

**Polars:**
```python
result = df.group_by(['department', 'level']).agg([
    pl.col('salary').sum()
])
```

**Gorilla:**
```go
result, err := df.Lazy().
    GroupBy("department", "level").
    Agg(
        gorilla.Sum(gorilla.Col("salary")).As("total_salary"),
    ).
    Collect()
if err != nil {
    log.Fatal(err)
}
defer result.Release()
```

### HAVING Clauses

**Pandas:**
```python
grouped = df.groupby('department').agg({'salary': 'sum'})
result = grouped[grouped['salary'] > 100000]
```

**Polars:**
```python
result = df.group_by('department').agg([
    pl.col('salary').sum().alias('total_salary')
]).filter(pl.col('total_salary') > 100000)
```

**Gorilla:**
```go
result, err := df.Lazy().
    GroupBy("department").
    Agg(
        gorilla.Sum(gorilla.Col("salary")).As("total_salary"),
    ).
    Having(gorilla.Col("total_salary").Gt(gorilla.Lit(100000.0))).
    Collect()
if err != nil {
    log.Fatal(err)
}
defer result.Release()
```

## Joins

### Inner Join

**Pandas:**
```python
result = df1.merge(df2, on='id', how='inner')
```

**Polars:**
```python
result = df1.join(df2, on='id', how='inner')
```

**Gorilla:**
```go
result, err := df1.Join(df2, &gorilla.JoinOptions{
    Type:     gorilla.InnerJoin,
    LeftKey:  "id",
    RightKey: "id",
})
if err != nil {
    log.Fatal(err)
}
defer result.Release()
```

### Multi-Key Joins

**Pandas:**
```python
result = df1.merge(df2, on=['key1', 'key2'], how='left')
```

**Polars:**
```python
result = df1.join(df2, on=['key1', 'key2'], how='left')
```

**Gorilla:**
```go
result, err := df1.Join(df2, &gorilla.JoinOptions{
    Type:      gorilla.LeftJoin,
    LeftKeys:  []string{"key1", "key2"},
    RightKeys: []string{"key1", "key2"},
})
if err != nil {
    log.Fatal(err)
}
defer result.Release()
```

## Memory Management

### Key Differences

**Pandas/Polars:**
- Automatic garbage collection
- Memory management handled by runtime
- Less explicit resource control

**Gorilla:**
- Explicit resource management
- Must call `Release()` on all DataFrames and Series
- Use `defer` pattern for automatic cleanup

### Best Practices

**Pandas/Polars:**
```python
# Memory is automatically managed
def process_data():
    df = pd.read_csv('large_file.csv')
    result = df.groupby('category').sum()
    return result
    # Memory freed by GC eventually
```

**Gorilla:**
```go
// Explicit memory management
func processData() (*gorilla.DataFrame, error) {
    df, err := io.ReadCSV("large_file.csv", mem)
    if err != nil {
        return nil, err
    }
    defer df.Release() // Cleanup input data
    
    result, err := df.Lazy().
        GroupBy("category").
        Sum("amount").
        Collect()
    if err != nil {
        return nil, err
    }
    // Return result - caller must release it
    return result, nil
}

// Usage
result, err := processData()
if err != nil {
    log.Fatal(err)
}
defer result.Release() // Caller handles cleanup
```

## Performance Considerations

### Query Optimization

**Pandas:**
```python
# No built-in query optimization
# Manual optimization required
df_filtered = df[df['active'] == True]
result = df_filtered.groupby('category').sum()
```

**Polars:**
```python
# Automatic query optimization with lazy evaluation
result = (
    df.lazy()
    .filter(pl.col('active') == True)
    .group_by('category')
    .sum()
    .collect()
)
```

**Gorilla:**
```go
// Automatic query optimization with lazy evaluation
result, err := df.Lazy().
    Filter(gorilla.Col("active").Eq(gorilla.Lit(true))).
    GroupBy("category").
    Sum("amount").
    Collect()
if err != nil {
    log.Fatal(err)
}
defer result.Release()
```

### Parallel Processing

**Pandas:**
```python
# Limited built-in parallelization
# Often requires manual chunking or external libraries
```

**Polars:**
```python
# Automatic parallelization
# No configuration usually needed
```

**Gorilla:**
```go
// Automatic parallelization with configuration options
config := config.OperationConfig{
    ParallelThreshold: 1000,
    WorkerPoolSize:    runtime.NumCPU(),
}

result, err := df.WithConfig(config).Lazy().
    Filter(gorilla.Col("amount").Gt(gorilla.Lit(100.0))).
    GroupBy("category").
    Sum("amount").
    Collect()
if err != nil {
    log.Fatal(err)
}
defer result.Release()
```

## Common Patterns

### Method Chaining

**Pandas:**
```python
result = (df
    .query('age > 25')
    .groupby('department')
    .agg({'salary': 'sum'})
    .reset_index()
    .sort_values('salary', ascending=False)
)
```

**Polars:**
```python
result = (df
    .filter(pl.col('age') > 25)
    .group_by('department')
    .agg(pl.col('salary').sum())
    .sort('salary', descending=True)
)
```

**Gorilla:**
```go
result, err := df.Lazy().
    Filter(gorilla.Col("age").Gt(gorilla.Lit(int64(25)))).
    GroupBy("department").
    Agg(gorilla.Sum(gorilla.Col("salary")).As("total_salary")).
    Sort("total_salary", false). // false = descending
    Collect()
if err != nil {
    log.Fatal(err)
}
defer result.Release()
```

### Window Functions

**Pandas:**
```python
df['rank'] = df.groupby('department')['salary'].rank(method='dense')
```

**Polars:**
```python
df = df.with_columns([
    pl.col('salary').rank('dense').over('department').alias('rank')
])
```

**Gorilla:**
```go
// Window functions available in expr package
result, err := df.Lazy().
    WithColumn("rank",
        expr.DenseRank().Over("department").OrderBy("salary", false),
    ).
    Collect()
if err != nil {
    log.Fatal(err)
}
defer result.Release()
```

### Case/When Expressions

**Pandas:**
```python
df['category'] = np.where(df['score'] > 90, 'A', 
                 np.where(df['score'] > 80, 'B', 'C'))
```

**Polars:**
```python
df = df.with_columns([
    pl.when(pl.col('score') > 90).then(pl.lit('A'))
      .when(pl.col('score') > 80).then(pl.lit('B'))
      .otherwise(pl.lit('C'))
      .alias('category')
])
```

**Gorilla:**
```go
result, err := df.Lazy().
    WithColumn("category",
        gorilla.Case().
            When(gorilla.Col("score").Gt(gorilla.Lit(90.0)), gorilla.Lit("A")).
            When(gorilla.Col("score").Gt(gorilla.Lit(80.0)), gorilla.Lit("B")).
            Otherwise(gorilla.Lit("C")),
    ).
    Collect()
if err != nil {
    log.Fatal(err)
}
defer result.Release()
```

## Error Handling

### Pandas/Polars Approach

**Pandas:**
```python
try:
    result = df.groupby('invalid_column').sum()
except KeyError as e:
    print(f"Column not found: {e}")
```

**Polars:**
```python
try:
    result = df.group_by('invalid_column').sum()
except pl.ColumnNotFoundError as e:
    print(f"Column not found: {e}")
```

### Gorilla Approach

**Gorilla:**
```go
result, err := df.Lazy().
    GroupBy("invalid_column").
    Sum("amount").
    Collect()
if err != nil {
    log.Printf("Operation failed: %v", err)
    return
}
defer result.Release()
```

## Migration Checklist

When migrating from Pandas/Polars to Gorilla:

- [ ] **Add memory management**: Use `defer df.Release()` pattern
- [ ] **Handle errors explicitly**: Check `err` returns from operations
- [ ] **Use typed data**: Specify concrete types for Series creation
- [ ] **Adopt lazy evaluation**: Use `.Lazy()` for complex operations
- [ ] **Update import statements**: Import Gorilla and Arrow packages
- [ ] **Convert column references**: Use `gorilla.Col("name")` instead of string references
- [ ] **Convert literals**: Use `gorilla.Lit(value)` for literal values  
- [ ] **Update aggregation syntax**: Use `gorilla.Sum()`, `gorilla.Count()`, etc.
- [ ] **Handle joins**: Use `JoinOptions` struct for join configuration
- [ ] **Test with real data**: Verify type compatibility and performance

## Performance Migration Tips

1. **Batch operations**: Use lazy evaluation to combine multiple operations
2. **Early filtering**: Place filters before expensive operations
3. **Memory monitoring**: Monitor memory usage during migration
4. **Parallel tuning**: Configure parallel processing for your workload
5. **Type optimization**: Use appropriate data types (int32 vs int64, float32 vs float64)

This migration guide provides a foundation for transitioning to Gorilla DataFrame. For specific use cases not covered here, consult the API documentation and examples in the `examples/` directory.