# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Essential Commands

```bash
# Build and test
make build          # Build gorilla-cli binary
make release        # Build release binary with version information and build flags
make test           # Run all tests with race detection (-short flag)
make test-all       # Run all tests including long-running tests (comprehensive)
make lint           # Run golangci-lint (pre-commit runs --fix)
make run-demo       # Build and run interactive demo

# Coverage reporting
make coverage       # Run tests with coverage and generate report
make coverage-html  # Run tests with coverage and open HTML report

# Development commands
go test ./internal/dataframe -v              # Test specific package
go test -bench=. ./internal/dataframe        # Run benchmarks
go test ./internal/dataframe -run="GroupBy"  # Run specific test pattern
go run ./examples/usage.go                   # Test example code

# Development setup and CI validation
make install-tools  # Install required development tools (lefthook, golangci-lint, gocovmerge)
make setup          # Complete development setup (install tools + git hooks)
lefthook run pre-commit             # Run all pre-commit hooks locally
```

## System Requirements

- **Go Version**: Requires Go 1.24.4 or later for latest language features and performance optimizations
- **Memory**: Minimum 4GB RAM recommended for large dataset processing
- **CPU**: Multi-core processor recommended for automatic parallelization benefits

## Architecture Overview

Gorilla is a high-performance DataFrame library built on **Apache Arrow** with **lazy evaluation** and **automatic parallelization**. Understanding these three concepts is critical:

### Core Data Flow
```
Series[T] → DataFrame → LazyFrame → Parallel Execution → Result
   ↓           ↓           ↓              ↓              ↓
Arrow Arrays  Column Map  Operation AST  Worker Chunks  Final DF
   ↓           ↓           ↓              ↓              ↓
memory.Resource → ResourceManager → MemoryMonitor → SpillableBatch
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

### Advanced Memory Management Architecture

Gorilla implements a sophisticated memory management system with several key interfaces:

**Resource Management Interfaces:**
- `memory.Resource`: Core interface for all memory-managed objects (EstimateMemory, ForceCleanup, SpillIfNeeded)
- `memory.ResourceManager`: Manages collections of resources with tracking and bulk operations
- `memory.MemoryUsageMonitor`: Monitors system memory pressure and triggers cleanup

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

**Spillable Architecture:**
- `SpillableBatch`: DataFrames that can be spilled to disk under memory pressure
- `BatchManager`: Manages collections of spillable batches with LRU eviction
- Automatic spilling when memory thresholds are exceeded

**Memory Utilities (internal/memory):**
- Consolidated memory estimation across all data types
- GC pressure monitoring and forced cleanup
- Memory-aware chunking for large dataset processing

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
- `FunctionExpr`: Function calls (If, Coalesce, Case)
- `CaseExpr`: Case expressions for conditional logic

Expressions are evaluated using the `Evaluator` which handles type conversions and Arrow array operations.

### Window Functions (Advanced)
Located in `internal/expr/window_evaluator.go` and `internal/expr/window_parallel.go`:
- **Row-based Functions**: RowNumber, Rank, DenseRank, NthValue  
- **Aggregate Functions**: Sum, Count, Mean over windows
- **Parallel Evaluation**: Automatic parallelization for large window operations
- **Memory Optimization**: Efficient buffering and type-specific array builders

Window functions use partition-based processing with automatic memory management.

### Parallel Processing Infrastructure
Located in `internal/parallel/`:
- **Basic Workers** (`worker.go`): `Process[T, R]()` and `ProcessIndexed[T, R]()` for generic parallel execution
- **Advanced Workers** (`advanced_worker.go`): Work-stealing pools with priority queues and backpressure control
- **Memory-Safe Processing** (`memory_safe.go`): Allocator pools and memory monitoring for safe parallel operations
- All use worker pools with configurable size (defaults to `runtime.NumCPU()`)

### GroupBy Implementation
GroupBy uses hash-based grouping with these phases:
1. **Grouping**: Hash group keys to create row index maps
2. **Aggregation**: Apply aggregation functions to each group
3. **HAVING Filtering**: Apply post-aggregation predicates (if specified)
4. **Result Building**: Create new DataFrame with aggregated and filtered results
5. **Parallel Execution**: For >100 groups, distribute across workers

### HAVING Clause Implementation
HAVING clauses filter grouped data after aggregation with these components:
- **AggregationContext**: Maps column names to aggregated values and user-defined aliases
- **Expression Validation**: Ensures HAVING predicates only reference GROUP BY columns or aggregation results
- **Alias Resolution**: Supports both user-defined aliases (`AS total_salary`) and auto-generated names
- **Performance Optimization**: Memory overhead <10% with expression caching and allocator reuse
- **SQL Compatibility**: Full support for standard SQL HAVING clause syntax

### Streaming & Large Dataset Architecture

**Core Components:**
- `StreamingProcessor`: Main orchestrator for chunk-based processing
- `MemoryAwareChunkReader`: Adaptive chunk sizing based on memory pressure  
- `SpillableBatch`: Memory-efficient batch storage with disk spillover
- `BatchManager`: Resource lifecycle management for streaming operations

**Key Features:**
- Automatic memory pressure detection and response
- Configurable chunk sizes with adaptive adjustment
- Error recovery and graceful degradation
- Integration with lazy evaluation and query optimization

**Memory Management:**
- Uses consolidated memory estimation utilities
- Implements Resource interface for consistent cleanup
- Automatic GC triggering under memory pressure
- Spillable batches for datasets larger than memory

### Query Optimization Engine
Located in `internal/dataframe/optimizer.go`:
- **PredicatePushdownRule**: Moves filters closer to data sources
- **FilterFusionRule**: Combines multiple filter operations
- **ProjectionPushdownRule**: Eliminates unnecessary column reads
- **ConstantFoldingRule**: Pre-evaluates constant expressions
- **OperationFusionRule**: Combines compatible operations
- Creates execution plans with cost estimation and dependency analysis

### Join Optimization (Advanced)
Located in `internal/dataframe/join_optimizer.go`:
- **Strategy Selection**: Automatically chooses optimal join algorithm based on data characteristics
  - **Hash Join**: Default strategy for most joins
  - **Broadcast Join**: For small tables (< 1000 rows)
  - **Merge Join**: For pre-sorted data
  - **Optimized Hash Join**: Custom hash map with memory efficiency
- **Performance Features**: Parallel hash map building, optimized memory layouts, adaptive work distribution

### SQL Interface
Complete SQL support via `SQLExecutor`:
- Standard SQL syntax (SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT)
- Advanced features (JOINs, subqueries, window functions, aggregations)
- Query validation and execution plan analysis
- Batch execution capabilities
- Full integration with DataFrame optimizations

## Type System & Patterns

### Series Types
Supports: `string`, `int64`, `int32`, `int16`, `int8`, `uint64`, `uint32`, `uint16`, `uint8`, `float64`, `float32`, `bool`
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
    Having(expr.Col("total_salary").Gt(expr.Lit(100000))).
    Collect() // Triggers execution
```

### Testing Patterns
- Use `testify/assert` for assertions
- Create test DataFrames with `series.FromSlice()`
- **Always use `defer resource.Release()` pattern in tests** - this prevents resource leaks and makes test cleanup explicit
- Integration tests in `dataframe/*_test.go` test end-to-end workflows
- Benchmarks follow `BenchmarkXxx` naming with `-benchmem`

#### Advanced Testing Patterns
- **Table-Driven Tests**: Use structured test data for complex scenarios (see `internal/expr/evaluator_test.go`)
- **Benchmark Memory Tracking**: Include `-benchmem` flag and track memory allocations
- **Parallel Test Safety**: Ensure tests can run with `go test -parallel` without race conditions
- **Resource Cleanup Validation**: Use `testing.TB.Cleanup()` for additional resource validation

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
```

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
- ✅ Basic DataFrame operations (Select, Filter, WithColumn, Sort)
- ✅ Parallel LazyFrame.Collect() execution with adaptive worker pools
- ✅ GroupBy with aggregations (Sum, Count, Mean, Min, Max) and parallel execution
- ✅ **HAVING clause support** with full SQL compatibility, alias resolution, and high-performance optimization
- ✅ Expression system with arithmetic/comparison operations and advanced functions (If, Coalesce, Case)
- ✅ Join operations (Inner, Left, Right, Full Outer) with multi-key support and optimization
- ✅ **Advanced join optimization** with automatic strategy selection (Hash, Broadcast, Merge, Optimized Hash)
- ✅ I/O operations (CSV reader/writer with automatic type inference)
- ✅ Enhanced type system (int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64, bool, string)
- ✅ Streaming and large dataset processing with memory management
- ✅ **Advanced memory management** with Resource interfaces, spillable batches, and memory monitoring
- ✅ Debug mode and execution plan visualization with performance profiling
- ✅ Comprehensive configuration system (JSON/YAML/env with performance tuning)
- ✅ Query optimization engine (predicate pushdown, filter fusion, join optimization, constant folding)
- ✅ **Window functions** with parallel evaluation (RowNumber, Rank, DenseRank, NthValue, aggregations)
- ✅ **Complete SQL interface** with query validation, execution plans, and batch processing
- ✅ CLI tool (gorilla-cli) with benchmarking and demo capabilities
- ✅ Memory management improvements with GC pressure monitoring and spillable batches

### Recent Code Quality Improvements
- ✅ **Cognitive Complexity Reduction**: All functions now have cognitive complexity ≤20 (gocognit compliance)
- ✅ **Comprehensive Linting**: Full golangci-lint v2 compatibility with 80+ enabled linters
- ✅ **Refactoring Methodology**: Applied systematic refactoring patterns (Extract Method, Early Returns, Helper Functions)
- ✅ **Memory Safety**: Enhanced resource cleanup patterns and spillable batch architecture
- ✅ **Performance Optimization**: Window function parallel evaluation and optimized memory estimation

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

### Completed Major Features
1. ✅ Query optimization engine (Issue #3: predicate pushdown, operation fusion, constant folding)
2. ✅ Join operations (Issue #17: inner, left, right, full outer with multi-key support)  
3. ✅ Enhanced expression system (Issue #19: string functions, conditionals, If/Coalesce/Case)
4. ✅ Parallel sorting (Issue #18: multi-column sort with parallel execution)
5. ✅ Enhanced type system (Issue #20: additional integer types int8-uint64, float32)
6. ✅ I/O operations (Issue #6: CSV readers/writers with type inference)
7. ✅ Debug mode and execution plan visualization (Issue #48)
8. ✅ Configurable processing parameters (Issue #47)
9. ✅ Memory management improvements (Issue #7)
10. ✅ **HAVING clause support (Milestone #105: full SQL compatibility with high-performance optimization)**

### Remaining Future Enhancements
1. **Enhanced type system**: Date/time and decimal types (Issue #5 - partial)
2. **Additional I/O formats**: Parquet, JSON, Arrow IPC readers/writers
3. **Advanced string operations**: Regex matching, advanced text processing
4. **Time series operations**: Resampling, time-based grouping
5. **Distributed processing**: Multi-node parallel execution

### Code Quality Standards
- All operations must handle memory cleanup properly
- Parallel operations require thread-safe data handling
- New aggregation functions need both eager and lazy variants
- Comprehensive test coverage including edge cases and benchmarks

### Go Code Style and Linting

This project uses **golangci-lint v2** with a strict configuration based on best practices. Key style requirements:

#### Required Linters
- **intrange**: Use Go 1.22+ integer range syntax (`for i := range count`) instead of traditional loops
- **embeddedstructfieldcheck**: Embedded fields must be separated from regular fields with a blank line
- **govet**: Avoid variable shadowing (e.g., `err := ...` inside a scope where `err` already exists)
- **mnd**: Magic numbers should be constants (except in tests and specific allowed functions)
- **nilerr**: Don't return nil when checking err != nil
- **perfsprint**: Use `errors.New` instead of `fmt.Errorf` for simple error messages
- **testifylint**: Use `require.Error` for error assertions in tests, not `assert.Error`
- **testpackage**: Test files should use `package foo_test` instead of `package foo`

#### Common Fixes
```go
// ❌ Bad: Traditional for loop
for i := 0; i < count; i++ {
    items[i] = process(i)
}

// ✅ Good: Go 1.22+ integer range
for i := range count {
    items[i] = process(i)
}

// ❌ Bad: Embedded field not separated
type SQLTranslator struct {
    *common.BaseTranslator
    evaluator *expr.Evaluator
}

// ✅ Good: Blank line after embedded field
type SQLTranslator struct {
    *common.BaseTranslator

    evaluator *expr.Evaluator
}

// ❌ Bad: Variable shadowing
func process() error {
    err := doSomething()
    if err != nil {
        if err := doAnotherThing(); err != nil { // shadows outer err
            return err
        }
    }
    return err
}

// ✅ Good: No shadowing
func process() error {
    err := doSomething()
    if err != nil {
        if anotherErr := doAnotherThing(); anotherErr != nil {
            return anotherErr
        }
    }
    return err
}

// ❌ Bad: Magic number
const defaultRowCount = 4

// ✅ Good: Named constant
const defaultTestRowCount = 4

// ❌ Bad: fmt.Errorf for simple errors
return fmt.Errorf("DataFrame cannot be nil")

// ✅ Good: errors.New for simple errors
return errors.New("DataFrame cannot be nil")
```

#### Test Package Convention
```go
// ❌ Bad: Same package for tests
package common

import "testing"

func TestBaseTranslator(t *testing.T) { ... }

// ✅ Good: Separate test package
package common_test

import (
    "testing"
    "github.com/paveg/gorilla/internal/sql/common"
)

func TestBaseTranslator(t *testing.T) { ... }
```

#### Running Linter
```bash
make lint              # Run linter (fails on violations)
golangci-lint run --fix  # Auto-fix some issues
```

See `.golangci.yml` for the complete linter configuration and all enabled checks.

### Pull Request Guidelines
When creating pull requests:
- **Always include `Closes #<issue-number>` or `Resolves #<issue-number>`** in the PR description when implementing a GitHub issue
- This automatically links the PR to the issue and closes it when the PR is merged
- Example: "Closes #6" or "Resolves #42"
- Place this after the summary section in the PR description

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
   - **Fix**: Always verify information from official sources before making changes

8. **Development Setup**: Not running complete setup process
   - **Fix**: Use `make setup` for complete development environment (tools + git hooks)
   - **Best Practice**: Run `make install-tools` first, then `lefthook install` for git hooks

9. **Test Coverage**: Running only short tests during development
   - **Fix**: Use `make test-all` for comprehensive testing including long-running tests
   - **Development**: Use `make test` for quick feedback, `make test-all` before commits

## Commit Message Guidelines

**CRITICAL**: All commits must follow the [Conventional Commits](https://www.conventionalcommits.org/) specification for automated changelog generation and proper version bumping.

### Commit Message Format
```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

### Required Commit Types
- **feat**: New features (triggers minor version bump)
- **fix**: Bug fixes (triggers patch version bump) 
- **docs**: Documentation changes only
- **style**: Code style changes (formatting, etc.)
- **refactor**: Code refactoring without functional changes
- **test**: Test additions or modifications
- **chore**: Build system, tooling, or dependency updates
- **perf**: Performance improvements
- **ci**: CI/CD configuration changes

### Breaking Changes
- Add `BREAKING CHANGE:` in the commit footer (triggers major version bump)
- Or add `!` after type: `feat!: redesign DataFrame API`

### Commit Examples
```bash
feat(io): add Parquet reader support

Implements basic Parquet file reading with schema inference
and integration with existing DataFrame operations.

Closes #78

fix(dataframe): resolve memory leak in GroupBy operations

Use proper defer pattern for resource cleanup in parallel operations.

Fixes #92

docs: update CONTRIBUTING.md with commit guidelines

Add conventional commit specification and examples for contributors.

chore(deps): update Apache Arrow to v18.3.1

Update to latest stable version for performance improvements.
```

### Scope Guidelines
Common scopes to use:
- `io`: I/O operations (CSV, Parquet, etc.)
- `dataframe`: Core DataFrame functionality
- `expr`: Expression system
- `parallel`: Parallel processing features
- `memory`: Memory management
- `cli`: Command-line interface
- `api`: Public API changes
- `test`: Test-related changes

### Validation
- The version-check workflow validates commit messages on PRs
- Use `lefthook run pre-commit` to validate locally
- Invalid commit messages will cause CI to fail

### Auto-Generated Signatures
All commits should end with:
```
🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>
```