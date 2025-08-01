# Test Utilities Package

This package provides common testing utilities to reduce code duplication across test files in the gorilla DataFrame library.

## Purpose

During similarity analysis of the codebase, we identified extensive duplication in test functions:

- **Memory Management Tests**: 95-99% similarity
- **SQL Translator Tests**: 92-100% similarity  
- **Expression Tests**: 96-99% similarity
- **DataFrame Creation**: 90%+ similarity across 50+ test functions

This package consolidates these patterns into reusable utilities.

## Code Duplication Reduction

**Before testutil**: Each test required 15-25 lines of setup code  
**After testutil**: Tests require 3-6 lines of setup code  
**Reduction**: 60-75% code reduction in test setup and assertions

## Core Utilities

### Memory Management

```go
// Replace: mem := memory.NewGoAllocator()
mem := testutil.SetupMemoryTest(t)
defer mem.Release()
```

### DataFrame Creation

```go
// Standard employee dataset (name, age, department, salary)
df := testutil.CreateTestDataFrame(mem.Allocator)
defer df.Release()

// Simple 2-column dataset (name, age)
simple := testutil.CreateSimpleTestDataFrame(mem.Allocator)
defer simple.Release()

// Custom configurations
custom := testutil.CreateTestDataFrame(mem.Allocator,
    testutil.WithRowCount(10),
    testutil.WithActiveColumn(),
)
defer custom.Release()
```

### Assertions

```go
// Replace complex DataFrame comparisons
testutil.AssertDataFrameEqual(t, expected, actual)

// Verify DataFrame structure
testutil.AssertDataFrameNotEmpty(t, df)
testutil.AssertDataFrameHasColumns(t, df, []string{"name", "age"})
```

### SQL Testing

```go
// Complete SQL test environment with registered tables
sqlCtx := testutil.SetupSQLTest(t)
defer sqlCtx.Release()

// Simple SQL testing environment
simple := testutil.SetupSimpleSQLTest(t)
defer simple.Release()

// Custom test tables
data := map[string]interface{}{
    "id": []int64{1, 2, 3},
    "name": []string{"A", "B", "C"},
}
table := testutil.CreateTestTableWithData(mem.Allocator, "products", data)
defer table.Release()
```

## Migration Examples

### Before (15+ lines)
```go
func TestSomeFeature(t *testing.T) {
    mem := memory.NewGoAllocator()
    
    names := series.New("name", []string{"Alice", "Bob", "Charlie", "David"}, mem)
    ages := series.New("age", []int64{25, 30, 35, 28}, mem)
    departments := series.New("department", []string{"Engineering", "Sales", "Engineering", "Marketing"}, mem)
    salaries := series.New("salary", []int64{100000, 80000, 120000, 75000}, mem)
    
    df := dataframe.New(names, ages, departments, salaries)
    defer df.Release()
    
    require.NotNil(t, df)
    assert.Greater(t, df.Len(), 0)
    assert.Greater(t, df.Width(), 0)
    assert.True(t, df.HasColumn("name"))
    assert.True(t, df.HasColumn("age"))
    
    // ... test logic
}
```

### After (6 lines)
```go
func TestSomeFeature(t *testing.T) {
    mem := testutil.SetupMemoryTest(t)
    defer mem.Release()
    
    df := testutil.CreateTestDataFrame(mem.Allocator)
    defer df.Release()
    
    testutil.AssertDataFrameNotEmpty(t, df)
    testutil.AssertDataFrameHasColumns(t, df, []string{"name", "age", "department", "salary"})
    
    // ... test logic
}
```

## Import Cycle Avoidance

The testutil package is designed to avoid import cycles:

- ✅ Tests can import testutil
- ✅ testutil imports series and dataframe packages  
- ❌ Internal packages should not import testutil to avoid cycles

For internal package tests, copy the patterns demonstrated in `examples_test.go`.

## Files

- `testutil.go` - Core DataFrame and memory utilities
- `sql.go` - SQL-specific test utilities  
- `testutil_test.go` - Tests for core utilities
- `sql_test.go` - Tests for SQL utilities
- `examples_test.go` - Migration examples and pattern demonstrations

## Benefits

1. **Maintainability**: Centralized common patterns
2. **Consistency**: Standardized test setup across packages
3. **Productivity**: Faster test development with reusable utilities
4. **Quality**: Fewer places for test bugs to hide
5. **Readability**: Tests focus on business logic, not setup boilerplate