package parallel

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAllocatorPool tests the memory allocator pool for safe reuse.
func TestAllocatorPool(t *testing.T) {
	t.Run("basic pool operations", func(t *testing.T) {
		pool := NewAllocatorPool(2) // Pool size of 2

		// Get an allocator
		alloc1 := pool.Get()
		require.NotNil(t, alloc1)

		// Get another allocator
		alloc2 := pool.Get()
		require.NotNil(t, alloc2)

		// They should be valid instances (sync.Pool may reuse)
		assert.NotNil(t, alloc1)
		assert.NotNil(t, alloc2)

		// Put them back
		pool.Put(alloc1)
		pool.Put(alloc2)

		// Get again - should reuse the pooled allocators
		alloc3 := pool.Get()
		require.NotNil(t, alloc3)
	})

	t.Run("concurrent access safety", func(t *testing.T) {
		pool := NewAllocatorPool(4)
		const numGoroutines = 10
		const operationsPerGoroutine = 50

		var wg sync.WaitGroup
		errors := make(chan error, numGoroutines)

		for range numGoroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range operationsPerGoroutine {
					alloc := pool.Get()
					if alloc == nil {
						errors <- assert.AnError
						return
					}

					// Simulate some work with the allocator
					time.Sleep(time.Microsecond)

					pool.Put(alloc)
				}
			}()
		}

		wg.Wait()
		close(errors)

		// Check for any errors
		for err := range errors {
			t.Errorf("Concurrent access error: %v", err)
		}
	})
}

// TestMemoryMonitor tests memory pressure detection and adaptive behavior.
func TestMemoryMonitor(t *testing.T) {
	t.Run("memory pressure detection", func(t *testing.T) {
		monitor := NewMemoryMonitor(1024, 4) // 1KB threshold, 4 max workers

		// Should allow allocation when under threshold
		assert.True(t, monitor.CanAllocate(512))

		// Simulate memory usage
		monitor.RecordAllocation(800)

		// Should still allow small allocation
		assert.True(t, monitor.CanAllocate(200))

		// Should not allow allocation that exceeds threshold
		assert.False(t, monitor.CanAllocate(300))
	})

	t.Run("adaptive parallelism", func(t *testing.T) {
		monitor := NewMemoryMonitor(1000, 8)

		// Initially should allow full parallelism
		assert.Equal(t, 8, monitor.AdjustParallelism())

		// Under memory pressure, should reduce parallelism
		monitor.RecordAllocation(850) // 85% of threshold
		assert.Equal(t, 4, monitor.AdjustParallelism())

		// High memory pressure, should reduce further
		monitor.RecordAllocation(950) // 95% of threshold
		assert.Equal(t, 2, monitor.AdjustParallelism())
	})
}

// TestChunkProcessor tests isolated chunk processing.
func TestChunkProcessor(t *testing.T) {
	t.Run("independent memory allocation", func(t *testing.T) {
		pool := NewAllocatorPool(2)
		defer pool.Close()

		processor1 := NewChunkProcessor(pool, 1)
		processor2 := NewChunkProcessor(pool, 2)

		// Each processor should have its own allocator context
		assert.NotEqual(t, processor1.ChunkID(), processor2.ChunkID())

		// Test that processors can work independently
		alloc1 := processor1.GetAllocator()
		alloc2 := processor2.GetAllocator()

		// They should be valid instances
		assert.NotNil(t, alloc1)
		assert.NotNil(t, alloc2)

		// Clean up
		processor1.Release()
		processor2.Release()
	})

	t.Run("concurrent chunk processing", func(t *testing.T) {
		pool := NewAllocatorPool(4)
		defer pool.Close()

		const numProcessors = 10
		var wg sync.WaitGroup
		errors := make(chan error, numProcessors)

		for i := range numProcessors {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				processor := NewChunkProcessor(pool, id)
				defer processor.Release()

				// Simulate some work
				alloc := processor.GetAllocator()
				if alloc == nil {
					errors <- assert.AnError
					return
				}

				// Simulate memory allocation work
				time.Sleep(time.Millisecond)
			}(i)
		}

		wg.Wait()
		close(errors)

		// Check for any errors
		for err := range errors {
			t.Errorf("Concurrent processing error: %v", err)
		}
	})
}

// TestSafeDataFrameCopy tests thread-safe DataFrame copying.
func TestSafeDataFrameCopy(t *testing.T) {
	t.Run("concurrent access to safe copy", func(t *testing.T) {
		// Test with mock data until full DataFrame integration
		mockData := "test_dataframe_data"
		safeDF := NewSafeDataFrame(mockData)

		const numGoroutines = 5
		var wg sync.WaitGroup
		errors := make(chan error, numGoroutines)

		for range numGoroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()

				// Test concurrent Clone calls
				cloned, err := safeDF.Clone(nil)
				if err != nil {
					errors <- err
					return
				}

				// Verify cloned data
				if cloned != mockData {
					errors <- assert.AnError
				}
			}()
		}

		wg.Wait()
		close(errors)

		// Check for any errors
		for err := range errors {
			t.Errorf("Concurrent Clone error: %v", err)
		}
	})

	t.Run("safe data frame creation and basic operations", func(t *testing.T) {
		testData := "sample_data"
		safeDF := NewSafeDataFrame(testData)

		// Test basic Clone operation
		cloned, err := safeDF.Clone(nil)
		require.NoError(t, err)
		assert.Equal(t, testData, cloned)
	})
}

// Benchmark allocator pool performance.
func BenchmarkAllocatorPool(b *testing.B) {
	pool := NewAllocatorPool(runtime.NumCPU())
	defer pool.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			alloc := pool.Get()
			// Simulate some work
			_ = alloc
			pool.Put(alloc)
		}
	})
}
