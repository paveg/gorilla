package gorilla

import (
	"runtime"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMemoryManager tests the memory management utilities
func TestMemoryManager(t *testing.T) {
	t.Run("track and release multiple resources", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		manager := NewMemoryManager(mem)

		// Create some resources
		s1 := series.New("test1", []int64{1, 2, 3}, mem)
		s2 := series.New("test2", []string{"a", "b", "c"}, mem)
		df := NewDataFrame(s1, s2)

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
		manager := NewMemoryManager(mem)

		s1 := series.New("test", []int64{1, 2}, mem)
		manager.Track(s1)

		// Multiple calls should not panic
		require.NotPanics(t, func() {
			manager.ReleaseAll()
			manager.ReleaseAll()
		})
	})
}

// TestWithDataFrame tests the automatic cleanup helper
func TestWithDataFrame(t *testing.T) {
	t.Run("automatically releases dataframe", func(t *testing.T) {
		err := WithDataFrame(func() *DataFrame {
			mem := memory.NewGoAllocator()
			s1 := series.New("test", []int64{1, 2, 3}, mem)
			s2 := series.New("test2", []string{"a", "b", "c"}, mem)
			return NewDataFrame(s1, s2)
		}, func(df *DataFrame) error {
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

		err := WithDataFrame(func() *DataFrame {
			mem := memory.NewGoAllocator()
			s1 := series.New("test", []int64{1, 2}, mem)
			return NewDataFrame(s1)
		}, func(df *DataFrame) error {
			return expectedErr
		})

		assert.Equal(t, expectedErr, err)
	})
}

// TestWithSeries tests the series automatic cleanup helper
func TestWithSeries(t *testing.T) {
	t.Run("automatically releases series", func(t *testing.T) {
		err := WithSeries(func() ISeries {
			mem := memory.NewGoAllocator()
			return series.New("test", []int64{1, 2, 3, 4, 5}, mem)
		}, func(s ISeries) error {
			assert.Equal(t, 5, s.Len())
			assert.Equal(t, "test", s.Name())
			return nil
		})

		require.NoError(t, err)
	})
}

// TestMemoryLeakDetection tests for potential memory leaks
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
		for i := 0; i < 100; i++ {
			err := WithDataFrame(func() *DataFrame {
				mem := memory.NewGoAllocator()
				s1 := series.New("values", []int64{1, 2, 3, 4, 5}, mem)
				s2 := series.New("names", []string{"a", "b", "c", "d", "e"}, mem)
				return NewDataFrame(s1, s2)
			}, func(df *DataFrame) error {
				// Perform some operations
				result, err := df.Lazy().
					Filter(Col("values").Gt(Lit(int64(2)))).
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
		assert.LessOrEqual(t, memGrowth, uint64(1024*1024))
	})
}
