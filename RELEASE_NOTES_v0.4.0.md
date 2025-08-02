# Release Notes v0.4.0

## 🚀 Major Features

### DateTime Arithmetic Support
- **Comprehensive DateTime Operations**: Full support for datetime arithmetic in DataFrame operations
- **Date Functions**: `DateAdd`, `DateSub`, `DateDiff` with support for days, hours, minutes, months, and years
- **DateTime Extraction**: `Year()`, `Month()`, `Hour()` functions for temporal data analysis
- **LazyFrame Integration**: All datetime operations work seamlessly with LazyFrame workflows
- **GroupBy Support**: DateTime operations fully integrated with GroupBy and aggregation functions

### Performance Monitoring and Metrics
- **Built-in Metrics System**: Comprehensive performance monitoring for DataFrame operations
- **Memory Usage Tracking**: Real-time memory consumption monitoring and optimization
- **Execution Profiling**: Detailed performance metrics for operation timing and resource usage
- **Adaptive Processing**: Automatic optimization based on data size and system resources

### Enhanced Test Coverage and Quality
- **Comprehensive Test Suite**: Significantly expanded test coverage across all modules
- **Integration Testing**: Real-world scenario testing for complex DataFrame workflows
- **Performance Benchmarks**: Extensive benchmarking suite for performance regression detection
- **Code Quality**: Enhanced linting and code quality standards

## 🛠️ Improvements

### CLI Enhancements
- **Customizable Data Size**: New `--rows` option for controlling demo data size
- **Better Output Formatting**: Improved display and formatting options
- **Enhanced Benchmarking**: More detailed benchmark reporting and analysis

### Code Modernization
- **Go 1.18+ Features**: Updated to use modern Go features and type system improvements
- **Interface Modernization**: Complete migration from `interface{}` to `any` type
- **Memory Management**: Enhanced Arrow memory management with better resource cleanup patterns

### Developer Experience
- **Improved Error Messages**: More descriptive and actionable error messages
- **Better Documentation**: Enhanced code documentation and examples
- **Development Tools**: Updated development tools and pre-commit hooks

## 🔧 Technical Details

### DateTime Implementation
```go
// DateTime arithmetic in LazyFrame workflows
result, err := df.Lazy().
    WithColumn("expiry_date", expr.Col("created_at").DateAdd(expr.Days(30))).
    WithColumn("year", expr.Col("created_at").Year()).
    WithColumn("month", expr.Col("created_at").Month()).
    GroupBy("month").
    Agg(
        expr.Sum(expr.Col("amount")).As("total_amount"),
        expr.Count(expr.Col("type")).As("transaction_count"),
    ).
    Collect()
```

### Performance Monitoring
```go
// Built-in performance monitoring
monitor := monitoring.NewMetrics()
result, err := df.Lazy().
    WithMonitoring(monitor).
    Filter(expr.Col("age").Gt(expr.Lit(25))).
    Collect()

metrics := monitor.GetMetrics()
fmt.Printf("Memory used: %d MB\n", metrics.MemoryUsage/1024/1024)
fmt.Printf("Execution time: %v\n", metrics.ExecutionTime)
```

## 🐛 Bug Fixes

- **Count Aggregation**: Fixed issues with `Count` aggregation in multi-aggregation GroupBy operations
- **Memory Leaks**: Resolved Arrow array memory management issues in parallel processing
- **Type Safety**: Enhanced type checking and error handling for datetime operations
- **Compilation Issues**: Fixed compilation errors related to incomplete debug functionality

## 📊 Performance Improvements

- **Parallel Processing**: Enhanced parallel execution with adaptive chunking
- **Memory Optimization**: Improved memory usage patterns and garbage collection
- **Arrow Integration**: Optimized Arrow array operations for better performance
- **Query Optimization**: Enhanced predicate pushdown and operation fusion

## 🎯 Use Cases

### Financial Analysis with Time Series
```go
// Analyze transaction patterns by month
result, err := transactionDF.Lazy().
    WithColumn("month", expr.Col("transaction_date").Month()).
    WithColumn("year", expr.Col("transaction_date").Year()).
    GroupBy("year", "month").
    Agg(
        expr.Sum(expr.Col("amount")).As("total_amount"),
        expr.Count(expr.Col("transaction_id")).As("transaction_count"),
        expr.Mean(expr.Col("amount")).As("avg_amount"),
    ).
    Having(expr.Col("total_amount").Gt(expr.Lit(10000.0))).
    Collect()
```

### Employee Analytics with Service Duration
```go
// Calculate years of service and analyze by department
currentDate := time.Now()
result, err := employeeDF.Lazy().
    WithColumn("years_of_service", 
        expr.DateDiff(expr.Col("hire_date"), expr.Lit(currentDate), "years")).
    Filter(expr.Col("years_of_service").Ge(expr.Lit(int64(2)))).
    GroupBy("department").
    Agg(
        expr.Count(expr.Col("employee_id")).As("employee_count"),
        expr.Mean(expr.Col("years_of_service")).As("avg_years_service"),
        expr.Sum(expr.Col("salary")).As("total_salary"),
    ).
    Collect()
```

## 🔄 Migration Guide

### DateTime Operations
- Replace manual date calculations with built-in `DateAdd`, `DateSub`, `DateDiff` functions
- Use datetime extraction functions (`Year()`, `Month()`, `Hour()`) for temporal grouping
- Migrate to LazyFrame workflows for better performance with datetime operations

### Count Aggregations
- Update `Count` aggregations to reference specific columns instead of `"*"`
- Example: `expr.Count(expr.Col("*"))` → `expr.Count(expr.Col("column_name"))`

### Memory Management
- Ensure proper `Release()` calls on all DataFrame and Series objects
- Use `defer` pattern for resource cleanup: `defer df.Release()`
- Consider using memory monitoring for large dataset processing

## 📈 What's Next

- **v0.5.0 Preview**: Date formatting, time zones, and business day calculations
- **Advanced Analytics**: Window functions and statistical operations
- **I/O Enhancements**: Parquet and JSON readers with streaming support
- **SQL Interface**: Extended SQL syntax support and query optimization

## 🙏 Acknowledgments

This release includes contributions and feedback from the Gorilla community. Special thanks to all contributors who helped test and improve the datetime arithmetic functionality.

---

**Full Changelog**: https://github.com/paveg/gorilla/compare/v0.3.1...v0.4.0