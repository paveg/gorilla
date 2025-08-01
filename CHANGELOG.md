# Changelog

All notable changes to the Gorilla DataFrame library will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

## [0.4.0] - 2025-08-02

### Added

- **DateTime Arithmetic Support**: Comprehensive datetime operations for DataFrame workflows
  - `DateAdd`, `DateSub`, `DateDiff` functions with support for days, hours, minutes, months, years
  - DateTime extraction functions: `Year()`, `Month()`, `Hour()`
  - Full integration with LazyFrame and GroupBy operations
  - Example: `df.Lazy().WithColumn("expiry", expr.Col("date").DateAdd(expr.Days(30)))`

- **Performance Monitoring and Metrics System**: Built-in performance analysis
  - Real-time memory usage tracking and optimization
  - Execution profiling with detailed timing metrics
  - Adaptive processing based on data size and system resources
  - Comprehensive benchmarking suite for performance regression detection

- **Enhanced Test Coverage and Quality**: Significantly expanded testing infrastructure
  - Comprehensive integration testing for complex DataFrame workflows
  - Real-world scenario testing with financial and employee analytics use cases
  - Enhanced code quality standards with improved linting and pre-commit hooks

- **CLI Enhancements**: Improved command-line interface capabilities
  - New `--rows` option for customizable demo data size
  - Enhanced benchmark reporting and analysis features

### Improved

- **Code Modernization**: Updated to leverage Go 1.18+ features
  - Complete migration from `interface{}` to `any` type for better type safety
  - Enhanced Arrow memory management with improved resource cleanup patterns
  - Modernized JSON I/O implementation

- **Performance Optimizations**: Enhanced parallel processing and memory management
  - Adaptive chunking for optimal parallel execution
  - Improved memory usage patterns and garbage collection
  - Optimized Arrow array operations

### Fixed

- Count aggregation issues in multi-aggregation GroupBy operations
- Arrow array memory leaks in parallel processing scenarios
- Compilation errors related to incomplete debug functionality
- Type checking and error handling for datetime operations

### Migration Notes

- Update Count aggregations to reference specific columns instead of `"*"`
- Example: `expr.Count(expr.Col("*"))` → `expr.Count(expr.Col("column_name"))`

## [0.3.1] - 2025-08-01

### Added

- **Consolidated shared utilities** in `internal/common` package
  - String formatting patterns for expressions, SQL clauses, and functions
  - Safe type conversions with overflow detection and comprehensive type support
  - Centralized enum-to-string mappings with parsing capabilities
- Enhanced test reliability with optimized timing and timeout mechanisms

### Changed

- **Reduced code duplication** by ~50% across string formatting methods
- **Improved consistency** in enum-to-string conversions throughout codebase
- Optimized CI pipeline execution time with better test timing patterns

### Fixed

- Worker pool timeout issues in CI environments with graceful test skipping
- Race conditions in parallel test execution with proper synchronization
- golangci-lint var-naming warnings for internal/common package
- Test assertion improvements using require.Error for better error handling

## [0.3.0] - 2025-07-27

### Added

- **HAVING clause support** with full SQL compatibility and high-performance optimization
  - Expression validation ensuring proper column references
  - Alias resolution for user-defined and auto-generated aggregation names
  - Memory optimization achieving <10% overhead (7.57% measured)
  - Comprehensive test coverage and performance benchmarks
- Comprehensive package-level documentation for pkg.go.dev discoverability
- Example functions demonstrating basic usage, GroupBy, and Join operations
- Enhanced documentation for internal packages (io, parallel)
- Parquet I/O support with compression options (Snappy, GZIP, LZ4, ZSTD)
- Date/time extraction functions (Year, Month, Day, Hour, Minute, Second)
- Semantic versioning infrastructure and release management tools
- CLI version command with detailed build information
- Version information accessible via library API

### Changed

- Improved memory management with mutex synchronization for thread-safe operations
- Enhanced performance optimization framework with expression compilation and caching
- Optimized parallel execution with adaptive chunking and worker pools

### Fixed

- Race conditions in GroupByHavingOperation for parallel execution safety
- Type assertion error handling throughout test suite
- CI test stability with environment-aware performance thresholds
- Memory leak detection in HAVING operations with proper resource cleanup

## [0.1.0] - 2025-07-16

### Added

- Comprehensive CSV I/O operations with automatic type inference
- Join operations (Inner, Left, Right, Full Outer) with multi-key support
- GroupBy operations with Sum, Count, Mean, Min, Max aggregations
- Debug mode and execution plan visualization
- Query optimization engine with predicate pushdown and filter fusion
- Streaming processor for large datasets with memory management
- Enhanced type system supporting int8, int16, int32, uint32, uint16, uint8, float32
- Configurable processing parameters via JSON/YAML/environment variables
- CLI tool (gorilla-cli) with benchmarking and demo capabilities
- Advanced expression system with If, Coalesce, Case, and Concat functions
- Parallel processing with adaptive worker pools and thread-safe operations
- Sort operations with single and multi-column support
- Memory management improvements with GC pressure monitoring

### Changed

- Enhanced memory management with defer pattern recommendations
- Improved parallel processing thresholds and worker pool sizing
- Updated API documentation and examples

### Fixed

- Flaky trace ID generation test resolved with atomic counter
- Type assertion optimization to eliminate double assertions
- Memory leaks in parallel operations with proper resource cleanup
- CSV edge cases handling for quotes, escapes, and malformed data

---

## Contributing

When adding entries to this changelog:

1. Add new changes under the `[Unreleased]` section
2. Use the format: `- Brief description (Issue #X or PR #Y)`
3. Categorize changes as Added, Changed, Deprecated, Removed, Fixed, or Security
4. Move unreleased changes to a new version section when releasing
