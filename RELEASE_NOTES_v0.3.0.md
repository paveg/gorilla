# Gorilla v0.3.0 Release Notes

## 🚀 HAVING Clause Support

Gorilla v0.3.0 introduces comprehensive **HAVING clause support** with full SQL compatibility and high-performance optimization.

### Key Features

**SQL-Compatible HAVING Clause**
```go
// Filter grouped data after aggregation
result, err := df.Lazy().
    GroupBy("department").
    Agg(gorilla.Sum(gorilla.Col("salary")).As("total_salary")).
    Having(gorilla.Sum(gorilla.Col("salary")).Gt(gorilla.Lit(100000))).
    Collect()
```

**High Performance**
- **7.57% memory overhead** (target: <10%)
- **15-20% throughput improvement** for complex queries
- Expression compilation and caching
- Memory pooling for reduced allocations

**Thread Safety**
- Race condition fixes in parallel execution
- Mutex synchronization for shared resources
- Thread-safe operations with independent data copies

### Breaking Changes
None - this release is fully backward compatible.

### Installation

```sh
go get github.com/paveg/gorilla@v0.3.0
```

### What's Next

v0.3.x will focus on:
- Arithmetic expressions in HAVING predicates
- Complex nested aggregation functions
- Advanced SQL compatibility features

For complete details, see the [CHANGELOG](https://github.com/paveg/gorilla/blob/main/CHANGELOG.md).