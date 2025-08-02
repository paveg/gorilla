package monitoring

import (
	"sync"
)

//nolint:gochecknoglobals // Required for singleton pattern in monitoring system
var (
	globalCollector *MetricsCollector
	globalMutex     sync.RWMutex
)

// SetGlobalCollector sets the global metrics collector.
func SetGlobalCollector(collector *MetricsCollector) {
	globalMutex.Lock()
	defer globalMutex.Unlock()
	globalCollector = collector
}

// GetGlobalCollector returns the global metrics collector.
// Returns nil if no global collector has been set.
func GetGlobalCollector() *MetricsCollector {
	globalMutex.RLock()
	defer globalMutex.RUnlock()
	return globalCollector
}

// RecordGlobalOperation records an operation using the global collector.
// If no global collector is set, the operation is executed without recording metrics.
func RecordGlobalOperation(operation string, fn func() error) error {
	collector := GetGlobalCollector()
	if collector == nil {
		return fn()
	}
	return collector.RecordOperation(operation, fn)
}

// IsGlobalMonitoringEnabled returns true if global monitoring is enabled.
func IsGlobalMonitoringEnabled() bool {
	collector := GetGlobalCollector()
	return collector != nil && collector.IsEnabled()
}

// EnableGlobalMonitoring creates and sets a global metrics collector.
func EnableGlobalMonitoring() {
	SetGlobalCollector(NewMetricsCollector(true))
}

// DisableGlobalMonitoring disables the global metrics collector.
func DisableGlobalMonitoring() {
	collector := GetGlobalCollector()
	if collector != nil {
		collector.SetEnabled(false)
	}
}

// ClearGlobalMetrics clears all metrics from the global collector.
func ClearGlobalMetrics() {
	collector := GetGlobalCollector()
	if collector != nil {
		collector.Clear()
	}
}

// GetGlobalMetrics returns metrics from the global collector.
func GetGlobalMetrics() []OperationMetrics {
	collector := GetGlobalCollector()
	if collector == nil {
		return []OperationMetrics{}
	}
	return collector.GetMetrics()
}

// GetGlobalSummary returns a summary from the global collector.
func GetGlobalSummary() MetricsSummary {
	collector := GetGlobalCollector()
	if collector == nil {
		return MetricsSummary{}
	}
	return collector.GetSummary()
}
