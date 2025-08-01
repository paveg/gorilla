//nolint:testpackage // requires internal access to unexported types and functions
package dataframe

import (
	"runtime"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSafeParallelProcessing tests memory-safe parallel DataFrame operations.
func TestSafeParallelProcessing(t *testing.T) {
	t.Run("safe chunk creation with independent allocators", testSafeChunkCreation)
	t.Run("concurrent parallel operations without race conditions", testConcurrentOperations)
	t.Run("memory pressure adaptive behavior", testMemoryPressureAdaptiveBehavior)
}

// testSafeChunkCreation tests safe chunk creation with independent allocators.
func testSafeChunkCreation(t *testing.T) {
	mem := memory.NewGoAllocator()
	df := createParallelTestDataFrame(mem, 2000)
	defer df.Release()

	result, err := df.SafeCollectParallel()
	require.NoError(t, err)
	defer result.Release()

	assert.Equal(t, df.Len(), result.Len())
	assert.Equal(t, df.Width(), result.Width())
}

// testConcurrentOperations tests concurrent parallel operations without race conditions.
func testConcurrentOperations(t *testing.T) {
	mem := memory.NewGoAllocator()
	df := createTestDataFrameWithValues(mem, 1500)
	defer df.Release()

	const numConcurrent = 5
	errors := runConcurrentOperations(df, numConcurrent)

	// Check for any race condition errors.
	for err := range errors {
		t.Errorf("Concurrent operation error: %v", err)
	}
}

// testMemoryPressureAdaptiveBehavior tests memory pressure adaptive behavior.
func testMemoryPressureAdaptiveBehavior(t *testing.T) {
	mem := memory.NewGoAllocator()
	df := createTestDataFrameWithSingleColumn(mem, "data", 5000)
	defer df.Release()

	result, err := df.SafeCollectParallelWithMonitoring()
	require.NoError(t, err)
	defer result.Release()

	assert.Equal(t, df.Len(), result.Len())
}

// createParallelTestDataFrame creates a large test DataFrame with name and age columns.
func createParallelTestDataFrame(mem memory.Allocator, size int) *DataFrame {
	names := make([]string, size)
	ages := make([]int64, size)
	for i := range size {
		names[i] = "User" + string(rune('A'+i%26))
		ages[i] = int64(20 + i%50)
	}

	namesSeries := series.New("name", names, mem)
	agesSeries := series.New("age", ages, mem)
	return New(namesSeries, agesSeries)
}

// createTestDataFrameWithValues creates a test DataFrame with a single values column.
func createTestDataFrameWithValues(mem memory.Allocator, size int) *DataFrame {
	values := make([]int64, size)
	for i := range size {
		values[i] = int64(i)
	}

	valuesSeries := series.New("values", values, mem)
	return New(valuesSeries)
}

// createTestDataFrameWithSingleColumn creates a test DataFrame with a single column.
func createTestDataFrameWithSingleColumn(mem memory.Allocator, columnName string, size int) *DataFrame {
	data := make([]int64, size)
	for i := range size {
		data[i] = int64(i)
	}

	series := series.New(columnName, data, mem)
	return New(series)
}

// runConcurrentOperations runs multiple concurrent parallel operations and returns errors channel.
func runConcurrentOperations(df *DataFrame, numConcurrent int) <-chan error {
	var wg sync.WaitGroup
	errors := make(chan error, numConcurrent)

	for i := range numConcurrent {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			processWorkerOperation(df, workerID, errors)
		}(i)
	}

	go func() {
		wg.Wait()
		close(errors)
	}()

	return errors
}

// processWorkerOperation processes a single worker operation with error handling.
func processWorkerOperation(df *DataFrame, workerID int, errors chan<- error) {
	threshold := int64(workerID * 100)
	result, err := df.Lazy().
		Filter(expr.Col("values").Gt(expr.Lit(threshold))).
		SafeCollectParallel()

	if err != nil {
		errors <- err
		return
	}

	if result != nil {
		defer result.Release()
		if result.Len() > df.Len() {
			errors <- assert.AnError
		}
	}
}

// TestMemoryLeakPrevention tests that the new safe parallel processing prevents memory leaks.
func TestMemoryLeakPrevention(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory leak test in short mode")
	}

	t.Run("no memory leaks in repeated parallel operations", func(t *testing.T) {
		// Force garbage collection and get baseline
		runtime.GC()
		runtime.GC()
		var memBefore runtime.MemStats
		runtime.ReadMemStats(&memBefore)

		// Perform repeated operations that previously caused memory leaks
		for range 100 {
			mem := memory.NewGoAllocator()

			size := 1000
			data := make([]int64, size)
			for j := range size {
				data[j] = int64(j)
			}

			series := series.New("data", data, mem)
			df := New(series)

			// Use safe parallel collection
			result, err := df.SafeCollectParallel()
			require.NoError(t, err)

			// Proper cleanup
			df.Release()
			if result != nil {
				result.Release()
			}
		}

		// Force garbage collection and check memory
		runtime.GC()
		runtime.GC()
		var memAfter runtime.MemStats
		runtime.ReadMemStats(&memAfter)

		// Memory growth should be minimal
		memGrowth := memAfter.Alloc - memBefore.Alloc
		t.Logf("Memory growth: %d bytes", memGrowth)

		// Should not grow by more than 5MB for this test
		assert.LessOrEqual(t, memGrowth, uint64(5*1024*1024))
	})
}

// TestRaceConditionPrevention tests that the new implementation prevents race conditions.
func TestRaceConditionPrevention(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping race condition test in short mode")
	}

	t.Run("parallel array access without corruption", func(t *testing.T) {
		mem := memory.NewGoAllocator()

		// Create test data
		size := 2000
		data := make([]int64, size)
		for i := range size {
			data[i] = int64(i)
		}

		series := series.New("data", data, mem)
		df := New(series)
		defer df.Release()

		const numWorkers = 10
		var wg sync.WaitGroup
		results := make(chan *DataFrame, numWorkers)
		errors := make(chan error, numWorkers)

		// Launch multiple workers accessing the same data
		for i := range numWorkers {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				// Each worker applies a different filter
				threshold := int64(workerID * 50)
				result, err := df.Lazy().
					Filter(expr.Col("data").Gt(expr.Lit(threshold))).
					SafeCollectParallel()

				if err != nil {
					errors <- err
					return
				}

				results <- result
			}(i)
		}

		// Wait for all workers to complete
		wg.Wait()
		close(results)
		close(errors)

		// Check for errors (indicating race conditions)
		for err := range errors {
			t.Errorf("Race condition detected: %v", err)
		}

		// Clean up results
		for result := range results {
			if result != nil {
				result.Release()
			}
		}
	})
}

// Benchmark safe vs unsafe parallel processing.
func BenchmarkSafeParallelProcessing(b *testing.B) {
	mem := memory.NewGoAllocator()

	size := 5000
	data := make([]int64, size)
	for i := range size {
		data[i] = int64(i)
	}

	series := series.New("data", data, mem)
	df := New(series)
	defer df.Release()

	b.ResetTimer()

	b.Run("safe parallel collection", func(b *testing.B) {
		for range b.N {
			result, err := df.SafeCollectParallel()
			if err != nil {
				b.Fatal(err)
			}
			if result != nil {
				result.Release()
			}
		}
	})
}
