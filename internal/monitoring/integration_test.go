//nolint:testpackage // requires internal access to unexported types and functions
package monitoring

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGlobalCollector(t *testing.T) {
	// Clean up any existing global collector
	defer SetGlobalCollector(nil)

	t.Run("no global collector initially", func(t *testing.T) {
		SetGlobalCollector(nil)
		assert.Nil(t, GetGlobalCollector())
		assert.False(t, IsGlobalMonitoringEnabled())
	})

	t.Run("set and get global collector", func(t *testing.T) {
		collector := NewMetricsCollector(true)
		SetGlobalCollector(collector)

		retrieved := GetGlobalCollector()
		assert.Equal(t, collector, retrieved)
		assert.True(t, IsGlobalMonitoringEnabled())
	})

	t.Run("global operation recording with no collector", func(t *testing.T) {
		SetGlobalCollector(nil)

		called := false
		err := RecordGlobalOperation("test", func() error {
			called = true
			return nil
		})

		require.NoError(t, err)
		assert.True(t, called)
		assert.Empty(t, GetGlobalMetrics())
	})

	t.Run("global operation recording with collector", func(t *testing.T) {
		collector := NewMetricsCollector(true)
		SetGlobalCollector(collector)

		called := false
		err := RecordGlobalOperation("test_global", func() error {
			called = true
			time.Sleep(1 * time.Millisecond)
			return nil
		})

		require.NoError(t, err)
		assert.True(t, called)

		metrics := GetGlobalMetrics()
		require.Len(t, metrics, 1)
		assert.Equal(t, "test_global", metrics[0].Operation)
	})

	t.Run("enable global monitoring", func(t *testing.T) {
		SetGlobalCollector(nil)
		assert.False(t, IsGlobalMonitoringEnabled())

		EnableGlobalMonitoring()
		assert.True(t, IsGlobalMonitoringEnabled())
		assert.NotNil(t, GetGlobalCollector())
	})

	t.Run("disable global monitoring", func(t *testing.T) {
		EnableGlobalMonitoring()
		assert.True(t, IsGlobalMonitoringEnabled())

		DisableGlobalMonitoring()
		assert.False(t, IsGlobalMonitoringEnabled())
		// Collector still exists but is disabled
		assert.NotNil(t, GetGlobalCollector())
	})

	t.Run("clear global metrics", func(t *testing.T) {
		EnableGlobalMonitoring()

		err := RecordGlobalOperation("test", func() error {
			return nil
		})
		require.NoError(t, err)
		assert.Len(t, GetGlobalMetrics(), 1)

		ClearGlobalMetrics()
		assert.Empty(t, GetGlobalMetrics())
	})

	t.Run("global summary", func(t *testing.T) {
		EnableGlobalMonitoring()
		ClearGlobalMetrics()

		operations := []string{"filter", "sort", "groupby"}
		for _, op := range operations {
			err := RecordGlobalOperation(op, func() error {
				time.Sleep(1 * time.Millisecond)
				return nil
			})
			require.NoError(t, err)
		}

		summary := GetGlobalSummary()
		assert.Equal(t, 3, summary.TotalOperations)
		assert.Len(t, summary.OperationCounts, 3)
		assert.Equal(t, 1, summary.OperationCounts["filter"])
		assert.Equal(t, 1, summary.OperationCounts["sort"])
		assert.Equal(t, 1, summary.OperationCounts["groupby"])
	})

	t.Run("global summary with no collector", func(t *testing.T) {
		SetGlobalCollector(nil)

		summary := GetGlobalSummary()
		assert.Equal(t, 0, summary.TotalOperations)
		assert.Empty(t, summary.OperationCounts)
	})
}

func TestGlobalCollectorConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrency tests in short mode")
	}

	// Clean up any existing global collector
	defer SetGlobalCollector(nil)

	t.Run("concurrent access to global collector", func(t *testing.T) {
		EnableGlobalMonitoring()
		ClearGlobalMetrics()

		numOps := 10
		done := make(chan bool, numOps)

		for i := range numOps {
			go func(_ int) {
				defer func() { done <- true }()

				err := RecordGlobalOperation("concurrent_global", func() error {
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

		metrics := GetGlobalMetrics()
		assert.Len(t, metrics, numOps)

		for _, metric := range metrics {
			assert.Equal(t, "concurrent_global", metric.Operation)
		}
	})

	t.Run("concurrent set/get global collector", func(t *testing.T) {
		numOps := 5
		done := make(chan bool, numOps*2)

		// Concurrent setters
		for range numOps {
			go func() {
				defer func() { done <- true }()
				collector := NewMetricsCollector(true)
				SetGlobalCollector(collector)
			}()
		}

		// Concurrent getters
		for range numOps {
			go func() {
				defer func() { done <- true }()
				collector := GetGlobalCollector()
				// Just ensure we can get it without panic
				_ = collector
			}()
		}

		// Wait for all operations to complete
		for range numOps * 2 {
			<-done
		}

		// Should have some collector set
		assert.NotNil(t, GetGlobalCollector())
	})
}
