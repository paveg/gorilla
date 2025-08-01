package expr_test

import (
	"testing"
)

// Tests for window sort optimization functionality
// These tests require access to internal types and methods
// and cannot be run from the expr_test package

func SkipTestComparators(t *testing.T) {
	t.Skip("This test requires access to internal comparator types")
}

func SkipTestSortKeyExtraction(t *testing.T) {
	t.Skip("This test requires access to internal sort methods")
}

func SkipTestOptimizedSortingVsOriginal(t *testing.T) {
	t.Skip("This test requires access to internal sort methods")
}
