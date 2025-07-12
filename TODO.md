# Gorilla DataFrame Library - Complete TODO List

This document provides a comprehensive overview of all remaining tasks to complete the high-performance concurrent DataFrame library.

## 🚨 Critical Priority (Performance Blockers)

### 1. Parallel LazyFrame.Collect() Implementation
**Location**: `internal/dataframe/lazy.go:450`
```go
// TODO: Implement parallel execution pipeline for LazyFrame.Collect()
```
**Description**: The most critical performance optimization needed. Currently operations are applied sequentially.

**Implementation Plan**:
1. Split DataFrame into row-based chunks  
2. Create tasks that apply full operation pipeline to each chunk
3. Use `parallel.Process` to execute tasks concurrently
4. Concatenate results from all chunks into final DataFrame

**Impact**: This will unlock the true concurrent processing power of the library.

---

## 🔥 High Priority (Core Features)

### 2. Query Optimization Engine
- **Predicate Pushdown**: Move filter operations early in pipeline
- **Projection Pushdown**: Only process columns needed by final result  
- **Operation Fusion**: Combine multiple operations into single pass

### 3. Essential Data Operations
- **GroupBy** with parallel aggregation
- **Join** operations (inner, left, right, full outer)
- **Sorting** with parallel merge sort

### 4. Type System Expansion
- Mixed arithmetic type coercion (int32/int64, float32/float64)
- Date/time/timestamp types with timezone support
- Decimal/money types for financial calculations

---

## 📊 Medium Priority (Advanced Features)

### 5. I/O Operations
- CSV reader/writer with parallel parsing
- Parquet file format support  
- Database connectivity (SQL execution)
- Streaming data ingestion

### 6. Memory Management
- Streaming processing for datasets larger than memory
- Memory usage monitoring and automatic spilling
- Memory pool management for Arrow arrays

### 7. Advanced Analytics
- Statistical functions (correlation, regression)
- Time series operations (resampling, rolling windows)
- Window functions (row_number, rank, lag/lead)

---

## 🛠️ Low Priority (Polish & UX)

### 8. Developer Experience
- DataFrame visualization for debugging
- Data profiling and quality checks
- Schema validation and type inference
- SQL-like query interface

### 9. Performance Optimizations
- Dynamic worker pool scaling
- CPU affinity and NUMA-aware allocation
- SIMD vectorized operations
- GPU acceleration support

---

## 📁 Detailed TODO Files

For comprehensive task lists in each component:

- **DataFrame**: `internal/dataframe/TODO.md` - 25+ tasks
- **Expression System**: `internal/expr/TODO.md` - 15+ tasks  
- **Parallel Processing**: `internal/parallel/TODO.md` - 12+ tasks
- **Series Operations**: `internal/series/TODO.md` - 18+ tasks

---

## 🎯 Next Recommended Steps

1. **Start with Critical**: Implement parallel `LazyFrame.Collect()` 
2. **Add GroupBy**: Essential for data analysis workflows
3. **Implement Joins**: Required for multi-table operations
4. **Optimize Memory**: Add chunking and streaming support

---

## 🔍 Searching for TODOs

Use these commands to find all TODOs in the codebase:

```bash
# Find all TODO comments in code
grep -r "TODO:" internal/

# Find all TODO markdown files  
find . -name "TODO.md"

# Search for specific TODO topics
grep -r "parallel\|optimization\|GroupBy" internal/*/TODO.md
```

This approach provides much better searchability and organization of remaining work!