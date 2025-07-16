# Contributing to Gorilla DataFrame Library

Thank you for your interest in contributing to Gorilla! This document provides guidelines and information for contributors.

## 🚀 Getting Started

### Prerequisites

- Go 1.21 or later
- Git
- Make (for build automation)

### Development Setup

1. **Fork and clone the repository:**
   ```bash
   git clone https://github.com/YOUR_USERNAME/gorilla.git
   cd gorilla
   ```

2. **Install dependencies:**
   ```bash
   go mod download
   ```

3. **Run tests to verify setup:**
   ```bash
   make test
   ```

4. **Build the project:**
   ```bash
   make build
   ```

## 🛠️ Development Workflow

### Essential Commands

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

# CI validation
lefthook run pre-commit             # Run all pre-commit hooks locally
```

### Code Quality Standards

- **Memory Management**: All operations must handle memory cleanup properly with `defer` patterns
- **Parallel Safety**: Parallel operations require thread-safe data handling
- **Test Coverage**: Comprehensive test coverage including edge cases and benchmarks
- **Documentation**: Update CLAUDE.md for development guidance and README.md for user-facing features

## 📝 Contributing Guidelines

### Issue Management

Issues use a structured 3-tier labeling system:

**Priority Labels (Required)**
- `priority: critical` 🔴 - Critical issues requiring immediate attention
- `priority: high` 🟠 - High priority features and improvements 
- `priority: medium` 🟡 - Medium priority enhancements and optimizations
- `priority: low` 🟢 - Low priority / nice-to-have features

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

### Pull Request Process

1. **Create a feature branch:**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Follow Test-Driven Development (TDD):**
   - Write failing tests first that define the expected API and behavior
   - Implement minimal code to make tests pass
   - Refactor with comprehensive error handling and optimization

3. **Memory Management Pattern:**
   ```go
   func TestNewFeature(t *testing.T) {
       mem := memory.NewGoAllocator()
       
       // Create and immediately defer cleanup
       df := createTestDataFrame(mem)
       defer df.Release() // ← Essential for preventing memory leaks
       
       result, err := df.NewFeature(params)
       require.NoError(t, err)
       defer result.Release() // ← Clean up operation results
       
       assert.Equal(t, expectedValue, result.SomeProperty())
   }
   ```

4. **PR Requirements:**
   - **Always include `Closes #<issue-number>` or `Resolves #<issue-number>`** in the PR description
   - Run `make test` and `make lint` before submitting
   - Update documentation as needed (README.md, CLAUDE.md)
   - Add entries to CHANGELOG.md under the `[Unreleased]` section

5. **PR Description Template:**
   ```markdown
   ## Summary
   Brief description of changes

   Closes #<issue-number>

   ## Changes
   - List of specific changes made

   ## Testing
   - Description of tests added/modified
   - Any manual testing performed

   ## Documentation
   - Documentation updates made
   ```

### Code Style Guidelines

- **No comments unless explicitly needed** - Code should be self-documenting
- **Use `defer` for resource cleanup** - Prevents memory leaks and makes cleanup explicit
- **Follow existing patterns** - Mimic code style, use existing libraries and utilities
- **Type safety** - Handle all supported Series types in new operations
- **Error handling** - Always check errors from operations and use defer for cleanup in error paths

### Architecture Guidelines

**Apache Arrow Foundation**: All data is stored in Arrow columnar format. Series are generic wrappers around typed Arrow arrays. Memory management requires explicit `Release()` calls.

**Lazy Evaluation**: LazyFrame builds an operation AST instead of executing immediately. Operations are only applied during `.Collect()`.

**Automatic Parallelization**: Activates for DataFrames with 1000+ rows (configurable threshold) using adaptive chunking based on CPU count and data size.

## 🧪 Testing

### Testing Patterns
- Use `testify/assert` for assertions
- Create test DataFrames with `series.NewSafe()`
- **Always use `defer resource.Release()` pattern in tests**
- Integration tests in `dataframe/*_test.go` test end-to-end workflows
- Benchmarks follow `BenchmarkXxx` naming with `-benchmem`

### Running Tests
```bash
# Run all tests
make test

# Run specific package tests
go test ./internal/dataframe -v

# Run benchmarks
go test -bench=. ./internal/dataframe

# Run tests with coverage
go test -cover ./...
```

## 📖 Documentation Updates

### When to Update Documentation

- **README.md**: Update when adding user-facing features or changing API
- **CLAUDE.md**: Update for development guidelines, architecture changes, or new patterns
- **CHANGELOG.md**: Add entries for all changes under `[Unreleased]` section
- **Code comments**: Only add when absolutely necessary for complex logic

### Documentation Style

- Keep explanations concise and practical
- Include working code examples
- Follow existing formatting and structure
- Use proper markdown formatting

## 🚨 Common Pitfalls

1. **Memory Leaks**: Forgetting `Release()` calls on Arrow arrays
2. **Race Conditions**: Sharing Arrow arrays across goroutines without copying
3. **Type Mismatches**: Not handling all supported Series types in new operations
4. **Hardcoded Thresholds**: Use constants at package level for parallelization thresholds
5. **Missing Error Handling**: Arrow operations can fail and need proper error propagation

## 💬 Getting Help

- **Issues**: Create a GitHub issue for bugs or feature requests
- **Discussions**: Use GitHub Discussions for questions and general discussion
- **Review Process**: PRs are reviewed by maintainers and may include feedback from Copilot

## 📄 License

By contributing to Gorilla, you agree that your contributions will be licensed under the same MIT License that covers the project.