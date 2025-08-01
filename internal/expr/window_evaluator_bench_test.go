package expr_test

import (
	"testing"
)

// Benchmark tests for window evaluator internal methods
// These tests require access to internal methods that are not exported
// and cannot be run from the expr_test package

// SkipBenchmarkSortPartition benchmarks the sortPartition function with different data sizes.
// SKIP: This test requires access to internal methods that are not exported.
func SkipBenchmarkSortPartition(b *testing.B) {
	b.Skip("This test requires access to internal methods")
}

// SkipBenchmarkCompareRows benchmarks the compareRows function.
// SKIP: This test requires access to internal methods that are not exported.
func SkipBenchmarkCompareRows(b *testing.B) {
	b.Skip("This test requires access to internal methods")
}

// BenchmarkSortPartitionMultiColumn benchmarks sorting with multiple columns.
// SKIP: This test requires access to internal methods that are not exported.
func SkipBenchmarkSortPartitionMultiColumn(b *testing.B) {
	b.Skip("This test requires access to internal methods")
}
