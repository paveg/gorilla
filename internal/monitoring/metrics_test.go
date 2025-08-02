//nolint:testpackage // requires internal access to unexported types and functions
package monitoring

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOperationMetrics(t *testing.T) {
	t.Run("create operation metrics", func(t *testing.T) {
		metrics := OperationMetrics{
			Duration:      100 * time.Millisecond,
			RowsProcessed: 1000,
			MemoryUsed:    1024 * 1024, // 1MB
			CPUTime:       80 * time.Millisecond,
			Operation:     "filter",
			Parallel:      true,
		}

		assert.Equal(t, 100*time.Millisecond, metrics.Duration)
		assert.Equal(t, int64(1000), metrics.RowsProcessed)
		assert.Equal(t, int64(1024*1024), metrics.MemoryUsed)
		assert.Equal(t, 80*time.Millisecond, metrics.CPUTime)
		assert.Equal(t, "filter", metrics.Operation)
		assert.True(t, metrics.Parallel)
	})
}

func TestMetricsCollector(t *testing.T) {
	t.Run("create disabled collector", func(t *testing.T) {
		collector := NewMetricsCollector(false)
		assert.NotNil(t, collector)
		assert.False(t, collector.IsEnabled())
		assert.Empty(t, collector.GetMetrics())
	})

	t.Run("create enabled collector", func(t *testing.T) {
		collector := NewMetricsCollector(true)
		assert.NotNil(t, collector)
		assert.True(t, collector.IsEnabled())
		assert.Empty(t, collector.GetMetrics())
	})

	t.Run("record operation with disabled collector", func(t *testing.T) {
		collector := NewMetricsCollector(false)

		callCount := 0
		err := collector.RecordOperation("test", func() error {
			callCount++
			return nil
		})

		require.NoError(t, err)
		assert.Equal(t, 1, callCount)
		assert.Empty(t, collector.GetMetrics())
	})

	t.Run("record operation with enabled collector", func(t *testing.T) {
		collector := NewMetricsCollector(true)

		callCount := 0
		err := collector.RecordOperation("test_operation", func() error {
			callCount++
			time.Sleep(10 * time.Millisecond) // Simulate work
			return nil
		})

		require.NoError(t, err)
		assert.Equal(t, 1, callCount)

		metrics := collector.GetMetrics()
		require.Len(t, metrics, 1)

		metric := metrics[0]
		assert.Equal(t, "test_operation", metric.Operation)
		assert.Greater(t, metric.Duration, 5*time.Millisecond)
		assert.GreaterOrEqual(t, metric.MemoryUsed, int64(0))
	})

	t.Run("record multiple operations", func(t *testing.T) {
		collector := NewMetricsCollector(true)

		operations := []string{"filter", "sort", "groupby"}
		for _, op := range operations {
			err := collector.RecordOperation(op, func() error {
				time.Sleep(1 * time.Millisecond)
				return nil
			})
			require.NoError(t, err)
		}

		metrics := collector.GetMetrics()
		require.Len(t, metrics, 3)

		for i, op := range operations {
			assert.Equal(t, op, metrics[i].Operation)
		}
	})

	t.Run("handle operation error", func(t *testing.T) {
		collector := NewMetricsCollector(true)

		expectedErr := assert.AnError
		err := collector.RecordOperation("error_op", func() error {
			return expectedErr
		})

		assert.Equal(t, expectedErr, err)

		// Should still record metrics even on error
		metrics := collector.GetMetrics()
		require.Len(t, metrics, 1)
		assert.Equal(t, "error_op", metrics[0].Operation)
	})

	t.Run("clear metrics", func(t *testing.T) {
		collector := NewMetricsCollector(true)

		err := collector.RecordOperation("test", func() error {
			return nil
		})
		require.NoError(t, err)
		assert.Len(t, collector.GetMetrics(), 1)

		collector.Clear()
		assert.Empty(t, collector.GetMetrics())
	})
}

func TestMetricsCollectorConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrency tests in short mode")
	}

	t.Run("concurrent operations", func(t *testing.T) {
		collector := NewMetricsCollector(true)

		numOps := 10
		done := make(chan bool, numOps)

		for i := range numOps {
			go func(_ int) {
				defer func() { done <- true }()

				err := collector.RecordOperation("concurrent_op", func() error {
					time.Sleep(1 * time.Millisecond)
					return nil
				})
				assert.NoError(t, err)
			}(i)
		}

		// Wait for all operations to complete
		for range numOps {
			<-done
		}

		metrics := collector.GetMetrics()
		assert.Len(t, metrics, numOps)

		for _, metric := range metrics {
			assert.Equal(t, "concurrent_op", metric.Operation)
		}
	})
}
