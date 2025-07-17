# Changelog

All notable changes to the Gorilla DataFrame library will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Comprehensive package-level documentation for pkg.go.dev discoverability
- Example functions demonstrating basic usage, GroupBy, and Join operations
- Enhanced documentation for internal packages (io, parallel)
- Parquet I/O support with compression options (Snappy, GZIP, LZ4, ZSTD)
- Date/time extraction functions (Year, Month, Day, Hour, Minute, Second)
- Semantic versioning infrastructure and release management tools
- CLI version command with detailed build information
- Version information accessible via library API

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