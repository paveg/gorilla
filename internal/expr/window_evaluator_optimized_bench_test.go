package expr_test

import (
	"testing"
)

// Benchmark tests for window evaluator optimized internal methods
// These tests require access to internal methods that are not exported
// and cannot be run from the expr_test package

// SkipBenchmarkSortPartitionOptimizedVsOriginal compares optimized vs original implementation.
// SKIP: This test requires access to internal methods that are not exported.
func SkipBenchmarkSortPartitionOptimizedVsOriginal(b *testing.B) {
	b.Skip("This test requires access to internal methods")
}

// SkipBenchmarkSortPartitionOptimizedMultiColumn tests multi-column sorting performance.
// SKIP: This test requires access to internal methods that are not exported.
func SkipBenchmarkSortPartitionOptimizedMultiColumn(b *testing.B) {
	b.Skip("This test requires access to internal methods")
}

// SkipBenchmarkCompareRowsOptimized benchmarks the optimized type-specific comparators.
// SKIP: This test requires access to internal methods that are not exported.
func SkipBenchmarkCompareRowsOptimized(b *testing.B) {
	b.Skip("This test requires access to internal methods")
}
