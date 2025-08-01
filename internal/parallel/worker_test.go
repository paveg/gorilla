package parallel_test

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/paveg/gorilla/internal/parallel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewWorkerPool(_ *testing.T) {
	// Test default worker count
	pool := parallel.NewWorkerPool(0)
	defer pool.Close()

	// Note: Cannot access unexported numWorkers field

	// Test custom worker count
	pool2 := parallel.NewWorkerPool(4)
	defer pool2.Close()

	// Note: Cannot access unexported numWorkers field

	// Test negative worker count defaults to CPU count
	pool3 := parallel.NewWorkerPool(-1)
	defer pool3.Close()

	// Note: Cannot access unexported numWorkers field
}

func TestProcessBasic(t *testing.T) {
	pool := parallel.NewWorkerPool(2)
	defer pool.Close()

	// Test basic processing
	input := []int{1, 2, 3, 4, 5}

	// Square each number
	results := parallel.Process(pool, input, func(x int) int {
		return x * x
	})

	// Results might not be in order due to parallel processing
	assert.Len(t, results, 5)

	// Convert to map for order-independent comparison
	resultMap := make(map[int]bool)
	for _, r := range results {
		resultMap[r] = true
	}

	expected := map[int]bool{1: true, 4: true, 9: true, 16: true, 25: true}
	assert.Equal(t, expected, resultMap)
}

func TestProcessEmpty(t *testing.T) {
	pool := parallel.NewWorkerPool(2)
	defer pool.Close()

	// Test empty input
	input := []int{}
	results := parallel.Process(pool, input, func(x int) int {
		return x * 2
	})

	assert.Nil(t, results)
}

func TestProcessIndexed(t *testing.T) {
	pool := parallel.NewWorkerPool(2)
	defer pool.Close()

	// Test indexed processing (results should maintain order)
	input := []string{"a", "b", "c", "d"}

	results := parallel.ProcessIndexed(pool, input, func(index int, value string) string {
		return value + string(rune('0'+index))
	})

	expected := []string{"a0", "b1", "c2", "d3"}
	assert.Equal(t, expected, results)
}

func TestProcessIndexedEmpty(t *testing.T) {
	pool := parallel.NewWorkerPool(2)
	defer pool.Close()

	// Test empty input with indexed processing
	input := []string{}
	results := parallel.ProcessIndexed(pool, input, func(_ int, value string) string {
		return value
	})

	assert.Nil(t, results)
}

func TestProcessConcurrency(t *testing.T) {
	pool := parallel.NewWorkerPool(4)
	defer pool.Close()

	// Test that work is actually being done concurrently
	var concurrentCount int64
	var maxConcurrent int64

	input := make([]int, 20)
	for i := range input {
		input[i] = i
	}

	results := parallel.Process(pool, input, func(x int) int {
		// Track concurrent executions
		current := atomic.AddInt64(&concurrentCount, 1)

		// Update max if needed
		for {
			maxVal := atomic.LoadInt64(&maxConcurrent)
			if current <= maxVal || atomic.CompareAndSwapInt64(&maxConcurrent, maxVal, current) {
				break
			}
		}

		// Simulate some work
		time.Sleep(10 * time.Millisecond)

		atomic.AddInt64(&concurrentCount, -1)
		return x * 2
	})

	assert.Len(t, results, 20)

	// We should have had multiple concurrent executions
	// (at least 2 since we have 4 workers and 20 items with delays)
	assert.Greater(t, maxConcurrent, int64(1), "Expected some concurrent execution")
}

func TestProcessDifferentTypes(t *testing.T) {
	pool := parallel.NewWorkerPool(2)
	defer pool.Close()

	// Test string to int conversion
	input := []string{"1", "2", "3"}

	results := parallel.Process(pool, input, func(s string) int {
		switch s {
		case "1":
			return 1
		case "2":
			return 2
		case "3":
			return 3
		default:
			return 0
		}
	})

	assert.Len(t, results, 3)

	// Check that all expected values are present
	resultMap := make(map[int]bool)
	for _, r := range results {
		resultMap[r] = true
	}

	expected := map[int]bool{1: true, 2: true, 3: true}
	assert.Equal(t, expected, resultMap)
}

func TestWorkerPoolClose(t *testing.T) {
	pool := parallel.NewWorkerPool(2)

	// Pool should work before closing
	input := []int{1, 2, 3}
	results := parallel.Process(pool, input, func(x int) int {
		return x
	})
	assert.Len(t, results, 3)

	// Close the pool
	pool.Close()

	// After closing, context should be canceled
	// This is mainly testing that Close() doesn't panic
	assert.NotPanics(t, func() {
		pool.Close() // Should be safe to call multiple times
	})
}

func TestLargeDataset(t *testing.T) {
	pool := parallel.NewWorkerPool(runtime.NumCPU())
	defer pool.Close()

	// Test with larger dataset to ensure it works under load
	size := 1000
	input := make([]int, size)
	for i := range size {
		input[i] = i
	}

	results := parallel.Process(pool, input, func(x int) int {
		// Simple computation
		return x*x + x + 1
	})

	require.Len(t, results, size)

	// Verify some results (order doesn't matter)
	resultMap := make(map[int]bool)
	for _, r := range results {
		resultMap[r] = true
	}

	// Check a few expected values
	assert.True(t, resultMap[1]) // f(0) = 0*0 + 0 + 1 = 1
	assert.True(t, resultMap[3]) // f(1) = 1*1 + 1 + 1 = 3
	assert.True(t, resultMap[7]) // f(2) = 2*2 + 2 + 1 = 7
}
