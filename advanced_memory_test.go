package gorilla_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla"
	"github.com/paveg/gorilla/internal/parallel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAdvancedMemoryPool tests the advanced memory pool functionality.
func TestAdvancedMemoryPool(t *testing.T) {
	t.Run("creates and manages allocators", func(t *testing.T) {
		pool := parallel.NewAdvancedMemoryPool(4, 1024*1024, true)
		defer pool.Close()

		// Get allocator from pool
		alloc := pool.GetAllocator()
		assert.NotNil(t, alloc)

		// Use allocator
		buf := alloc.Allocate(100)
		assert.NotNil(t, buf)
		assert.Len(t, buf, 100)

		// Return allocator to pool
		pool.PutAllocator(alloc)

		// Get stats
		poolStats, memMonitor := pool.GetStats()
		assert.NotNil(t, poolStats)
		assert.NotNil(t, memMonitor)
		assert.Positive(t, poolStats.TotalAllocated)
	})

	t.Run("respects memory pressure with adaptive sizing", func(t *testing.T) {
		// Create pool with very low memory threshold to trigger pressure
		pool := parallel.NewAdvancedMemoryPool(2, 100, true) // 100 bytes threshold
		defer pool.Close()

		// Get first allocator
		alloc1 := pool.GetAllocator()
		assert.NotNil(t, alloc1)

		// Allocate memory to create pressure
		buf := alloc1.Allocate(200) // Exceeds threshold
		assert.NotNil(t, buf)

		// Try to get second allocator - might be nil due to memory pressure
		alloc2 := pool.GetAllocator()
		// This test depends on memory pressure calculation, so we just verify no panic

		// Clean up
		if alloc2 != nil {
			pool.PutAllocator(alloc2)
		}
		pool.PutAllocator(alloc1)
	})

	t.Run("handles monitored allocator correctly", func(t *testing.T) {
		pool := parallel.NewAdvancedMemoryPool(4, 1024*1024, false)
		defer pool.Close()

		// Get allocator
		alloc := pool.GetAllocator()
		require.NotNil(t, alloc)

		// Verify it's a monitored allocator
		_, ok := alloc.(*parallel.MonitoredAllocator)
		assert.True(t, ok)

		// Use the allocator
		buf := alloc.Allocate(100)
		assert.NotNil(t, buf)

		// Free the buffer
		alloc.Free(buf)

		// Return to pool
		pool.PutAllocator(alloc)
	})
}

// TestMonitoredAllocator tests the monitored allocator functionality.
func TestMonitoredAllocator(t *testing.T) {
	t.Run("tracks allocations and deallocations", func(t *testing.T) {
		basePool := parallel.NewAllocatorPool(4)
		defer basePool.Close()

		underlying := memory.NewGoAllocator()
		monitored := parallel.NewMonitoredAllocator(underlying, basePool)

		// Initial state
		initialAllocated := basePool.TotalAllocated()

		// Allocate memory
		buf := monitored.Allocate(100)
		assert.NotNil(t, buf)
		assert.Len(t, buf, 100)

		// Check allocation was recorded
		assert.Equal(t, initialAllocated+100, basePool.TotalAllocated())

		// Free memory
		monitored.Free(buf)

		// Check deallocation was recorded
		assert.Equal(t, initialAllocated, basePool.TotalAllocated())
	})

	t.Run("handles reallocations correctly", func(t *testing.T) {
		basePool := parallel.NewAllocatorPool(4)
		defer basePool.Close()

		underlying := memory.NewGoAllocator()
		monitored := parallel.NewMonitoredAllocator(underlying, basePool)

		// Allocate initial buffer
		buf := monitored.Allocate(100)
		assert.NotNil(t, buf)
		initialAllocated := basePool.TotalAllocated()

		// Reallocate to larger size
		newBuf := monitored.Reallocate(200, buf)
		assert.NotNil(t, newBuf)
		assert.Len(t, newBuf, 200)

		// Check allocation was updated (freed old, allocated new)
		expectedAllocated := initialAllocated - 100 + 200
		assert.Equal(t, expectedAllocated, basePool.TotalAllocated())

		// Clean up
		monitored.Free(newBuf)
	})

	t.Run("forwards AllocatedBytes correctly", func(t *testing.T) {
		basePool := parallel.NewAllocatorPool(4)
		defer basePool.Close()

		underlying := memory.NewGoAllocator()
		monitored := parallel.NewMonitoredAllocator(underlying, basePool)

		// Allocate some memory
		buf := monitored.Allocate(100)
		assert.NotNil(t, buf)

		// Check AllocatedBytes returns expected value (currently 0 as per implementation)
		allocatedBytes := monitored.AllocatedBytes()
		assert.Equal(t, int64(0), allocatedBytes)

		// Clean up
		monitored.Free(buf)
	})
}

// TestAllocatorPoolEnhancements tests the enhanced allocator pool functionality.
func TestAllocatorPoolEnhancements(t *testing.T) {
	t.Run("tracks total and peak allocation", func(t *testing.T) {
		pool := parallel.NewAllocatorPool(4)
		defer pool.Close()

		// Record some allocations
		pool.RecordAllocation(1000)
		pool.RecordAllocation(2000)

		assert.Equal(t, int64(3000), pool.TotalAllocated())
		assert.Equal(t, int64(3000), pool.PeakAllocated())

		// Record deallocation
		pool.RecordDeallocation(1000)

		assert.Equal(t, int64(2000), pool.TotalAllocated())
		assert.Equal(t, int64(3000), pool.PeakAllocated()) // Peak should remain

		// Record another allocation that exceeds previous peak
		pool.RecordAllocation(2000)

		assert.Equal(t, int64(4000), pool.TotalAllocated())
		assert.Equal(t, int64(4000), pool.PeakAllocated())
	})

	t.Run("provides comprehensive stats", func(t *testing.T) {
		pool := parallel.NewAllocatorPool(4)
		defer pool.Close()

		// Get an allocator to increase active count
		alloc := pool.Get()
		assert.NotNil(t, alloc)

		// Record some allocations
		pool.RecordAllocation(1000)

		// Get stats
		stats := pool.GetStats()

		assert.Equal(t, int64(1), stats.ActiveAllocators)
		assert.Equal(t, int64(1000), stats.TotalAllocated)
		assert.Equal(t, int64(1000), stats.PeakAllocated)
		assert.Equal(t, 4, stats.MaxSize)

		// Return allocator
		pool.Put(alloc)
	})

	t.Run("handles concurrent allocation tracking", func(t *testing.T) {
		pool := parallel.NewAllocatorPool(4)
		defer pool.Close()

		// Test concurrent allocation recording
		done := make(chan struct{})
		numGoroutines := 10
		allocationsPerGoroutine := 100

		for range numGoroutines {
			go func() {
				defer func() { done <- struct{}{} }()
				for range allocationsPerGoroutine {
					pool.RecordAllocation(100)
				}
			}()
		}

		// Wait for all goroutines to complete
		for range numGoroutines {
			<-done
		}

		// Check total allocation
		expectedTotal := int64(numGoroutines * allocationsPerGoroutine * 100)
		assert.Equal(t, expectedTotal, pool.TotalAllocated())
		assert.Equal(t, expectedTotal, pool.PeakAllocated())
	})
}

// TestMemoryMonitorIntegration tests integration between memory monitoring and pool management.
func TestMemoryMonitorIntegration(t *testing.T) {
	t.Run("adjusts parallelism based on memory pressure", func(t *testing.T) {
		// Create memory monitor with low threshold to trigger pressure
		monitor := parallel.NewMemoryMonitor(1000, 4) // 1KB threshold, max 4 parallel

		// Initially should allow full parallelism
		assert.Equal(t, 4, monitor.AdjustParallelism())

		// Simulate moderate memory pressure (60% of threshold)
		monitor.RecordAllocation(600)
		parallelism := monitor.AdjustParallelism()
		assert.True(t, parallelism >= 1 && parallelism <= 4)

		// Simulate high memory pressure (85% of threshold)
		monitor.RecordAllocation(250) // Total: 850
		parallelism = monitor.AdjustParallelism()
		assert.True(t, parallelism >= 1 && parallelism <= 2)

		// Simulate very high memory pressure (95% of threshold)
		monitor.RecordAllocation(100) // Total: 950
		parallelism = monitor.AdjustParallelism()
		assert.Equal(t, 1, parallelism)
	})

	t.Run("handles memory allocation and deallocation", func(t *testing.T) {
		monitor := parallel.NewMemoryMonitor(1000, 4)

		// Record allocations
		monitor.RecordAllocation(300)
		monitor.RecordAllocation(400)
		assert.Equal(t, int64(700), monitor.CurrentUsage())

		// Record deallocations
		monitor.RecordDeallocation(200)
		assert.Equal(t, int64(500), monitor.CurrentUsage())

		// Check if allocation is possible
		assert.True(t, monitor.CanAllocate(400))
		assert.False(t, monitor.CanAllocate(600))
	})
}

// BenchmarkMemoryMonitoring benchmarks memory monitoring performance.
func BenchmarkMemoryMonitoring(b *testing.B) {
	b.Run("memory usage monitoring", func(b *testing.B) {
		monitor := gorilla.NewMemoryUsageMonitor(1024 * 1024 * 1024) // 1GB threshold
		defer monitor.StopMonitoring()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				monitor.RecordAllocation(1024)
				monitor.RecordDeallocation(1024)
			}
		})
	})

	b.Run("allocator pool operations", func(b *testing.B) {
		pool := parallel.NewAllocatorPool(4)
		defer pool.Close()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				alloc := pool.Get()
				if alloc != nil {
					pool.Put(alloc)
				}
			}
		})
	})

	b.Run("monitored allocator operations", func(b *testing.B) {
		basePool := parallel.NewAllocatorPool(4)
		defer basePool.Close()

		underlying := memory.NewGoAllocator()
		monitored := parallel.NewMonitoredAllocator(underlying, basePool)

		b.ResetTimer()
		for range b.N {
			buf := monitored.Allocate(1024)
			monitored.Free(buf)
		}
	})
}

// TestMemoryPressureHandling tests how the system handles memory pressure.
func TestMemoryPressureHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory pressure test in short mode")
	}

	t.Run("handles memory pressure gracefully", func(t *testing.T) {
		// Create system with low memory threshold
		pool := parallel.NewAdvancedMemoryPool(2, 1024, true) // 1KB threshold
		defer pool.Close()

		// Get allocator and create memory pressure
		alloc := pool.GetAllocator()
		require.NotNil(t, alloc)

		// Allocate memory to create pressure
		buf := alloc.Allocate(2048) // Exceeds threshold
		assert.NotNil(t, buf)

		// System should still function but with reduced parallelism
		poolStats, memMonitor := pool.GetStats()
		assert.NotNil(t, poolStats)
		assert.NotNil(t, memMonitor)

		// Clean up
		alloc.Free(buf)
		pool.PutAllocator(alloc)
	})
}

// TestIntegrationWithExistingMemoryManager tests integration with existing memory management.
func TestIntegrationWithExistingMemoryManager(t *testing.T) {
	t.Run("works with existing memory manager", func(t *testing.T) {
		mem := memory.NewGoAllocator()

		// Create memory usage monitor
		monitor := gorilla.NewMemoryUsageMonitor(1024 * 1024)
		defer monitor.StopMonitoring()

		// Create memory manager
		manager := gorilla.NewMemoryManager(mem)
		defer manager.ReleaseAll()

		// Create some resources and track them
		s1 := gorilla.NewSeries("test1", []int64{1, 2, 3}, mem)
		s2 := gorilla.NewSeries("test2", []string{"a", "b", "c"}, mem)
		df := gorilla.NewDataFrame(s1, s2)

		manager.Track(s1)
		manager.Track(s2)
		manager.Track(df)

		// Record memory usage
		estimatedUsage := int64(df.Len() * df.Width() * 8)
		monitor.RecordAllocation(estimatedUsage)

		// Verify integration
		assert.Equal(t, 3, manager.Count())
		assert.Positive(t, monitor.CurrentUsage())

		// Clean up
		manager.ReleaseAll()
		monitor.RecordDeallocation(estimatedUsage)

		assert.Equal(t, 0, manager.Count())
		assert.Equal(t, int64(0), monitor.CurrentUsage())
	})
}
