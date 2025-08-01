package gorilla_test

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// maxMemGrowth is the threshold for acceptable memory growth in tests.
const maxMemGrowth = uint64(1024 * 1024) // 1MB threshold

// TestMemoryManager tests the memory management utilities.
func TestMemoryManager(t *testing.T) {
	t.Run("track and release multiple resources", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		manager := gorilla.NewMemoryManager(mem)

		// Create some resources
		s1 := series.New("test1", []int64{1, 2, 3}, mem)
		s2 := series.New("test2", []string{"a", "b", "c"}, mem)
		df := gorilla.NewDataFrame(s1, s2)

		// Track resources
		manager.Track(s1)
		manager.Track(s2)
		manager.Track(df)

		// Should have 3 tracked resources
		assert.Equal(t, 3, manager.Count())

		// Release all should work without panic
		require.NotPanics(t, func() {
			manager.ReleaseAll()
		})

		// Count should be reset
		assert.Equal(t, 0, manager.Count())
	})

	t.Run("release all is idempotent", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		manager := gorilla.NewMemoryManager(mem)

		s1 := series.New("test", []int64{1, 2}, mem)
		manager.Track(s1)

		// Multiple calls should not panic
		require.NotPanics(t, func() {
			manager.ReleaseAll()
			manager.ReleaseAll()
		})
	})

	t.Run("concurrent access", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		manager := gorilla.NewMemoryManager(mem)

		var wg sync.WaitGroup
		const numGoroutines = 10
		const resourcesPerGoroutine = 5

		// Launch multiple goroutines to test concurrent access
		wg.Add(numGoroutines)
		for i := range numGoroutines {
			go func(goroutineID int) {
				defer wg.Done()
				for j := range resourcesPerGoroutine {
					s := series.New("test", []int64{int64(goroutineID), int64(j)}, mem)
					manager.Track(s)
				}
			}(i)
		}

		wg.Wait()

		// Should have tracked all resources
		expectedCount := numGoroutines * resourcesPerGoroutine
		assert.Equal(t, expectedCount, manager.Count())

		// ReleaseAll should work without issues
		require.NotPanics(t, func() {
			manager.ReleaseAll()
		})

		assert.Equal(t, 0, manager.Count())
	})
}

// TestWithDataFrame tests the automatic cleanup helper.
func TestWithDataFrame(t *testing.T) {
	t.Run("automatically releases dataframe", func(t *testing.T) {
		err := gorilla.WithDataFrame(func() *gorilla.DataFrame {
			mem := memory.NewGoAllocator()
			s1 := series.New("test", []int64{1, 2, 3}, mem)
			s2 := series.New("test2", []string{"a", "b", "c"}, mem)
			return gorilla.NewDataFrame(s1, s2)
		}, func(df *gorilla.DataFrame) error {
			assert.Equal(t, 2, df.Width())
			assert.Equal(t, 3, df.Len())
			return nil
		})

		require.NoError(t, err)
		// DataFrame should have been automatically released
		// We can't directly test this, but no panics indicate success
	})

	t.Run("propagates function error", func(t *testing.T) {
		expectedErr := assert.AnError

		err := gorilla.WithDataFrame(func() *gorilla.DataFrame {
			mem := memory.NewGoAllocator()
			s1 := series.New("test", []int64{1, 2}, mem)
			return gorilla.NewDataFrame(s1)
		}, func(_ *gorilla.DataFrame) error {
			return expectedErr
		})

		assert.Equal(t, expectedErr, err)
	})
}

// TestWithSeries tests the series automatic cleanup helper.
func TestWithSeries(t *testing.T) {
	t.Run("automatically releases series", func(t *testing.T) {
		err := gorilla.WithSeries(func() gorilla.ISeries {
			mem := memory.NewGoAllocator()
			return series.New("test", []int64{1, 2, 3, 4, 5}, mem)
		}, func(s gorilla.ISeries) error {
			assert.Equal(t, 5, s.Len())
			assert.Equal(t, "test", s.Name())
			return nil
		})

		require.NoError(t, err)
	})
}

// TestWithMemoryManager tests the scoped memory management helper.
func TestWithMemoryManager(t *testing.T) {
	t.Run("automatically releases tracked resources", func(t *testing.T) {
		mem := memory.NewGoAllocator()

		err := gorilla.WithMemoryManager(mem, func(manager *gorilla.MemoryManager) error {
			// Create and track multiple resources
			s1 := series.New("test1", []int64{1, 2, 3}, mem)
			s2 := series.New("test2", []string{"a", "b", "c"}, mem)
			df := gorilla.NewDataFrame(s1, s2)

			manager.Track(s1)
			manager.Track(s2)
			manager.Track(df)

			// Verify resources are tracked
			assert.Equal(t, 3, manager.Count())
			return nil
		})

		require.NoError(t, err)
		// All resources should have been automatically released
		// We can't directly test this, but no panics indicate success
	})

	t.Run("propagates function error", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		expectedErr := assert.AnError

		err := gorilla.WithMemoryManager(mem, func(manager *gorilla.MemoryManager) error {
			s1 := series.New("test", []int64{1, 2}, mem)
			manager.Track(s1)
			return expectedErr
		})

		assert.Equal(t, expectedErr, err)
		// Resources should still be released even when function returns error
	})
}

// TestMemoryLeakDetection tests for potential memory leaks.
func TestMemoryLeakDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory leak test in short mode")
	}

	t.Run("no memory growth in repeated operations", func(t *testing.T) {
		// Force garbage collection and get baseline
		runtime.GC()
		runtime.GC()
		var memBefore runtime.MemStats
		runtime.ReadMemStats(&memBefore)

		// Perform operations that should not leak memory
		for range 100 {
			err := gorilla.WithDataFrame(func() *gorilla.DataFrame {
				mem := memory.NewGoAllocator()
				s1 := series.New("values", []int64{1, 2, 3, 4, 5}, mem)
				s2 := series.New("names", []string{"a", "b", "c", "d", "e"}, mem)
				return gorilla.NewDataFrame(s1, s2)
			}, func(df *gorilla.DataFrame) error {
				// Perform some operations
				result, err := df.Lazy().
					Filter(gorilla.Col("values").Gt(gorilla.Lit(int64(2)))).
					Select("names").
					Collect()
				if err != nil {
					return err
				}
				defer result.Release()
				return nil
			})
			require.NoError(t, err)
		}

		// Force garbage collection and check memory
		runtime.GC()
		runtime.GC()
		var memAfter runtime.MemStats
		runtime.ReadMemStats(&memAfter)

		// Memory growth should be minimal (less than 1MB for this test)
		memGrowth := memAfter.Alloc - memBefore.Alloc
		t.Logf("Memory growth: %d bytes", memGrowth)
		assert.LessOrEqual(t, memGrowth, maxMemGrowth)
	})
}

// TestMemoryUsageMonitor tests the memory usage monitoring functionality.
func TestMemoryUsageMonitor(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory usage monitor tests in short mode")
	}
	t.Run("records allocations and deallocations", func(t *testing.T) {
		monitor := gorilla.NewMemoryUsageMonitor(1024 * 1024) // 1MB threshold
		defer monitor.StopMonitoring()

		// Record some allocations
		monitor.RecordAllocation(1000)
		monitor.RecordAllocation(2000)
		assert.Equal(t, int64(3000), monitor.CurrentUsage())

		// Record deallocations
		monitor.RecordDeallocation(1000)
		assert.Equal(t, int64(2000), monitor.CurrentUsage())

		// Check peak usage
		assert.Equal(t, int64(3000), monitor.PeakUsage())
	})

	t.Run("triggers spill callback when threshold exceeded", func(t *testing.T) {
		var spillCalled int32
		monitor := gorilla.NewMemoryUsageMonitor(1000) // 1KB threshold
		defer monitor.StopMonitoring()

		monitor.SetSpillCallback(func() error {
			atomic.AddInt32(&spillCalled, 1)
			return nil
		})

		// Trigger spill by exceeding threshold
		monitor.RecordAllocation(1500)

		// Wait a bit for the callback to be called
		time.Sleep(20 * time.Millisecond)
		assert.Equal(t, int32(1), atomic.LoadInt32(&spillCalled))
		assert.Equal(t, int64(1), monitor.SpillCount())
	})

	t.Run("provides comprehensive stats", func(t *testing.T) {
		monitor := gorilla.NewMemoryUsageMonitor(1024 * 1024)
		defer monitor.StopMonitoring()

		monitor.RecordAllocation(5000)
		stats := monitor.GetStats()

		assert.Equal(t, int64(5000), stats.AllocatedBytes)
		assert.Equal(t, int64(5000), stats.PeakAllocatedBytes)
		assert.Equal(t, int64(1), stats.ActiveAllocations)
		assert.True(t, stats.MemoryPressure >= 0.0 && stats.MemoryPressure <= 1.0)
	})

	t.Run("background monitoring works", func(_ *testing.T) {
		monitor := gorilla.NewMemoryUsageMonitor(1024 * 1024)

		monitor.SetCleanupCallback(func() error {
			return nil
		})

		monitor.StartMonitoring()
		defer monitor.StopMonitoring()

		// Monitoring should be active (we can't directly test the private field,
		// but we can verify the monitoring loop runs without crashing)

		// Wait for at least one monitoring cycle (reduced for CI efficiency)
		time.Sleep(50 * time.Millisecond)

		// This test mainly verifies the monitoring loop runs without crashing
	})
}
