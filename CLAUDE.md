# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Essential Commands

```bash
# Build and test
make build          # Build gorilla-cli binary
make test           # Run all tests with race detection
make lint           # Run golangci-lint (pre-commit runs --fix)
make run-demo       # Build and run interactive demo

# Development
go test ./dataframe -v              # Test specific package
go test -bench=. ./dataframe        # Run benchmarks
go test ./dataframe -run="GroupBy"  # Run specific test pattern
go run ./examples/usage.go          # Test example code

# CI validation
lefthook run pre-commit             # Run all pre-commit hooks locally
```

## Architecture Overview

Gorilla is a high-performance DataFrame library built on **Apache Arrow** with **lazy evaluation** and **automatic parallelization**. Understanding these three concepts is critical:

### Core Data Flow
```
Series[T] → DataFrame → LazyFrame → Parallel Execution → Result
   ↓           ↓           ↓              ↓              ↓
Arrow Arrays  Column Map  Operation AST  Worker Chunks  Final DF
```

### Key Architectural Patterns

**Apache Arrow Foundation**: All data is stored in Arrow columnar format. Series are generic wrappers around typed Arrow arrays. Memory management requires explicit `Release()` calls.

**Lazy Evaluation**: LazyFrame builds an operation AST instead of executing immediately. Operations are only applied during `.Collect()`. This enables query optimization and efficient memory usage.

**Automatic Parallelization**: 
- Activates for DataFrames with 1000+ rows (configurable threshold)
- Uses adaptive chunking based on CPU count and data size
- Creates independent memory copies for thread safety
- Falls back to sequential execution for small datasets

## Critical Implementation Details

### Memory Management

**Primary Pattern: Use `defer` for Resource Cleanup**

Gorilla strongly recommends the `defer` pattern for most memory management scenarios. This approach provides:
- **Better readability** - Resource lifecycle is explicit at allocation point
- **Leak prevention** - Harder to forget releases when co-located with allocation
- **Go idioms** - `defer` is the canonical Go pattern for resource cleanup
- **Easier debugging** - Clear tracking of which resources aren't being released

#### ✅ Preferred: Defer Pattern

```go
// Always prefer this pattern
func processData() error {
    mem := memory.NewGoAllocator()
    
    // Create and immediately defer cleanup
    df := dataframe.New(series1, series2)
    defer df.Release() // Clear ownership and lifecycle
    
    result, err := df.Lazy().Filter(...).Collect()
    if err != nil {
        return err
    }
    defer result.Release() // Always clean up results
    
    // Use result...
    return nil
}
```

#### 📋 Alternative: MemoryManager for Complex Scenarios

Use `MemoryManager` only for scenarios with many short-lived resources:

```go
// Use for bulk operations with many temporary resources
err := WithMemoryManager(mem, func(manager *MemoryManager) error {
    for i := 0; i < 1000; i++ {
        temp := createTempDataFrame(i)
        manager.Track(temp) // Bulk cleanup at end
    }
    return processLargeDataset()
})
// All tracked resources automatically released
```

#### Core Rules
- Every Arrow array creation requires a memory allocator: `memory.NewGoAllocator()`
- Always call `Release()` on DataFrames, Series, and arrays
- Parallel operations create independent data copies to avoid race conditions
- Prefer `defer resource.Release()` over bulk management patterns

### Expression System
The expression system uses an AST pattern with these key types:
- `ColumnExpr`: References columns by name
- `LiteralExpr`: Holds typed constants
- `BinaryExpr`: Arithmetic/comparison operations  
- `AggregationExpr`: Sum, Count, Mean, Min, Max

Expressions are evaluated using the `Evaluator` which handles type conversions and Arrow array operations.

### Parallel Processing Infrastructure
Located in `internal/parallel/worker.go`. Key functions:
- `Process[T, R]()`: Generic parallel execution with fan-out/fan-in
- `ProcessIndexed[T, R]()`: Order-preserving variant
- Both use worker pools with configurable size (defaults to `runtime.NumCPU()`)

### GroupBy Implementation
GroupBy uses hash-based grouping with these phases:
1. **Grouping**: Hash group keys to create row index maps
2. **Aggregation**: Apply aggregation functions to each group
3. **Result Building**: Create new DataFrame with aggregated results
4. **Parallel Execution**: For >100 groups, distribute across workers

## Type System & Patterns

### Series Types
Supports: `string`, `int64`, `int32`, `float64`, `float32`, `bool`
- Generic `Series[T]` wraps typed Arrow arrays
- `ISeries` interface provides type-erased operations
- Type coercion happens in expression evaluation

### Operation Chaining
```go
// Lazy operations build AST
result, err := df.Lazy().
    Filter(expr.Col("age").Gt(expr.Lit(30))).
    GroupBy("department").
    Agg(expr.Sum(expr.Col("salary")).As("total_salary")).
    Collect() // Triggers execution
```

### Testing Patterns
- Use `testify/assert` for assertions
- Create test DataFrames with `series.FromSlice()`
- **Always use `defer resource.Release()` pattern in tests** - this prevents resource leaks and makes test cleanup explicit
- Integration tests in `dataframe/*_test.go` test end-to-end workflows
- Benchmarks follow `BenchmarkXxx` naming with `-benchmem`

#### Memory Safety in Tests
```go
func TestDataFrameOperation(t *testing.T) {
    mem := memory.NewGoAllocator()
    
    // Create test data and immediately defer cleanup
    df := createTestDataFrame(mem)
    defer df.Release() // ← Essential for preventing test memory leaks
    
    result, err := df.SomeOperation()
    require.NoError(t, err)
    defer result.Release() // ← Clean up operation results
    
    assert.Equal(t, expectedValue, result.SomeProperty())
}

### Test-Driven Development (TDD)
**Always implement new features using TDD methodology:**

1. **Red**: Write failing tests first that define the expected API and behavior
2. **Green**: Implement minimal code to make tests pass
3. **Refactor**: Clean up implementation while keeping tests green

**TDD Benefits for DataFrame Operations:**
- Ensures memory safety (tests catch Arrow array leaks)
- Validates parallel execution correctness
- Documents expected API through test examples
- Prevents regression in complex data transformations

**TDD Pattern for New Features:**
```go
// 1. Write failing test first
func TestNewFeature(t *testing.T) {
    df := createTestDataFrame()
    defer df.Release()
    
    result := df.NewFeature(params)
    defer result.Release()
    
    assert.Equal(t, expectedResult, result)
}

// 2. Implement minimal functionality
// 3. Refactor with comprehensive error handling and optimization
```

## Development Context

### Current Status
- ✅ Basic DataFrame operations (Select, Filter, WithColumn)
- ✅ Parallel LazyFrame.Collect() execution
- ✅ GroupBy with aggregations (Sum, Count, Mean, Min, Max)
- ✅ Expression system with arithmetic/comparison operations

### Task Management
All development tasks are tracked via **GitHub Issues** with organized label system:

#### Label System
Issues use a structured 3-tier labeling system for clear categorization:

**Priority Labels (Required)**
- `priority: critical` 🔴 - Critical issues requiring immediate attention (memory leaks, security, breaking bugs)
- `priority: high` 🟠 - High priority features and improvements 
- `priority: medium` 🟡 - Medium priority enhancements and optimizations
- `priority: low` 🟢 - Low priority / nice-to-have features and polish

**Area Labels (Required)**
- `area: core` 🏗️ - Core DataFrame functionality and operations
- `area: parallel` 🔄 - Parallel processing and concurrency features
- `area: memory` 🧠 - Memory management and allocation optimizations
- `area: api` 🔌 - Public API design and usability improvements
- `area: testing` 🧪 - Tests, benchmarks, and quality assurance
- `area: dev-experience` 👩‍💻 - Developer experience and tooling

**Type Labels (Optional)**
- `type: performance` ⚡ - Performance improvements and optimizations
- `type: security` 🔒 - Memory safety and security enhancements
- `type: breaking-change` 💥 - Breaking changes requiring major version bump

#### Labeling Guidelines
When creating or updating issues:

1. **Always assign priority and area labels** - These are required for all issues
2. **Use type labels** when the issue focuses on performance, security, or breaking changes
3. **Multiple area labels** are allowed when an issue spans multiple areas (e.g., parallel + memory)
4. **Consistent naming** - Use exact label names as defined above
5. **Update labels** as issue scope or priority changes during development

#### Common Label Combinations
- Critical memory bug: `priority: critical`, `area: memory`, `type: security`
- API improvement: `priority: medium`, `area: api`
- Performance optimization: `priority: low`, `area: core`, `type: performance`
- Parallel processing feature: `priority: high`, `area: parallel`, `area: core`

#### Querying Issues
Use GitHub CLI to find issues by labels:
```bash
gh issue list --label="priority: critical"           # Critical issues
gh issue list --label="area: parallel"               # Parallel processing issues  
gh issue list --label="priority: high,area: core"    # High priority core features
```

### High Priority Features (from GitHub Issues)
1. Query optimization engine (Issue #3: predicate pushdown, operation fusion)
2. Join operations (Issue #17: inner, left, right, full outer)  
3. Enhanced expression system (Issue #19: string functions, conditionals)
4. Parallel sorting (Issue #18: multi-column sort with merge sort)
5. Enhanced type system (Issue #5: date/time, decimal types)
6. I/O operations (Issue #6: CSV, Parquet readers/writers)

### Code Quality Standards
- All operations must handle memory cleanup properly
- Parallel operations require thread-safe data handling
- New aggregation functions need both eager and lazy variants
- Comprehensive test coverage including edge cases and benchmarks

## Information Accuracy Requirements

**CRITICAL: Always verify information from official sources before making any changes.**

### Documentation and License References
- **Dependencies**: Always check official documentation, GitHub repositories, and license files before adding or modifying dependency information
- **API Usage**: Consult official documentation for correct API signatures, parameter types, and usage patterns
- **Version Information**: Verify exact version numbers from go.mod, package.json, or equivalent dependency files
- **License Information**: Check official LICENSE, NOTICE, and THIRDPARTYNOTICE files from source repositories
- **Configuration**: Reference official documentation for configuration options and default values

### Verification Process
1. **Primary Sources**: Official project websites, GitHub repositories, and documentation
2. **Package Managers**: go.mod, package.json, requirements.txt for exact versions
3. **License Files**: LICENSE, NOTICE, COPYING files from official repositories
4. **Cross-Reference**: Multiple sources when information seems inconsistent

**Never assume or interpolate information. When uncertain, explicitly state the need to verify information.**

## Common Pitfalls

1. **Memory Leaks**: Forgetting `Release()` calls on Arrow arrays
   - **Fix**: Use `defer resource.Release()` immediately after creation
   - **Bad**: Creating resources without immediate defer statements
   - **Good**: `df := dataframe.New(...); defer df.Release()`

2. **Race Conditions**: Sharing Arrow arrays across goroutines without copying
   - **Fix**: Parallel operations create independent data copies automatically
   
3. **Type Mismatches**: Not handling all supported Series types in new operations
   - **Fix**: Use type switches and handle all supported types (string, int64, float64, bool)

4. **Threshold Logic**: Hardcoded parallelization thresholds should use constants
   - **Fix**: Define thresholds as constants at package level

5. **Error Handling**: Arrow operations can fail and need proper error propagation
   - **Fix**: Always check errors from operations and use defer for cleanup in error paths

6. **Bulk vs Individual Memory Management**: Using MemoryManager when defer is more appropriate
   - **Fix**: Prefer `defer` for most cases, use MemoryManager only for many short-lived resources

7. **Information Accuracy**: Making assumptions about dependencies, licenses, or APIs without verification