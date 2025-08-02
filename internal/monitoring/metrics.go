// Package monitoring provides performance monitoring and metrics collection for DataFrame operations.
package monitoring

import (
	"runtime"
	"sync"
	"time"
)

// OperationMetrics represents performance metrics for a single DataFrame operation.
type OperationMetrics struct {
	Duration      time.Duration `json:"duration"`
	RowsProcessed int64         `json:"rows_processed"`
	MemoryUsed    int64         `json:"memory_used"`
	CPUTime       time.Duration `json:"cpu_time"`
	Operation     string        `json:"operation"`
	Parallel      bool          `json:"parallel"`
}

// MetricsCollector collects and stores performance metrics for DataFrame operations.
type MetricsCollector struct {
	mu      sync.RWMutex
	metrics []OperationMetrics
	enabled bool
}

// NewMetricsCollector creates a new metrics collector.
func NewMetricsCollector(enabled bool) *MetricsCollector {
	return &MetricsCollector{
		metrics: make([]OperationMetrics, 0),
		enabled: enabled,
	}
}

// IsEnabled returns whether metrics collection is enabled.
func (mc *MetricsCollector) IsEnabled() bool {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.enabled
}

// RecordOperation executes the given function and records performance metrics.
func (mc *MetricsCollector) RecordOperation(operation string, fn func() error) error {
	if !mc.IsEnabled() {
		return fn()
	}

	// Capture memory stats before operation
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	// Record start time
	start := time.Now()

	// Execute the operation
	err := fn()

	// Calculate duration
	duration := time.Since(start)

	// Capture memory stats after operation
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	// Calculate memory used (approximate)
	memoryUsed := int64(memAfter.Alloc - memBefore.Alloc) //nolint:gosec // Memory values are expected to be safe

	// Create metrics record
	metrics := OperationMetrics{
		Duration:   duration,
		MemoryUsed: memoryUsed,
		CPUTime:    duration, // Simplified - in real implementation could be more accurate
		Operation:  operation,
		Parallel:   false, // Will be set by DataFrame operations
	}

	// Store metrics
	mc.mu.Lock()
	mc.metrics = append(mc.metrics, metrics)
	mc.mu.Unlock()

	return err
}

// GetMetrics returns a copy of all collected metrics.
func (mc *MetricsCollector) GetMetrics() []OperationMetrics {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	// Return a copy to avoid race conditions
	result := make([]OperationMetrics, len(mc.metrics))
	copy(result, mc.metrics)
	return result
}

// Clear removes all collected metrics.
func (mc *MetricsCollector) Clear() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.metrics = mc.metrics[:0]
}

// SetEnabled enables or disables metrics collection.
func (mc *MetricsCollector) SetEnabled(enabled bool) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.enabled = enabled
}

// GetSummary returns a summary of collected metrics.
func (mc *MetricsCollector) GetSummary() MetricsSummary {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	if len(mc.metrics) == 0 {
		return MetricsSummary{}
	}

	var totalDuration time.Duration
	var totalMemory int64
	var totalRows int64
	operationCounts := make(map[string]int)

	for _, metric := range mc.metrics {
		totalDuration += metric.Duration
		totalMemory += metric.MemoryUsed
		totalRows += metric.RowsProcessed
		operationCounts[metric.Operation]++
	}

	return MetricsSummary{
		TotalOperations: len(mc.metrics),
		TotalDuration:   totalDuration,
		TotalMemory:     totalMemory,
		TotalRows:       totalRows,
		OperationCounts: operationCounts,
		AverageDuration: totalDuration / time.Duration(len(mc.metrics)),
	}
}

// MetricsSummary provides aggregate statistics for collected metrics.
type MetricsSummary struct {
	TotalOperations int            `json:"total_operations"`
	TotalDuration   time.Duration  `json:"total_duration"`
	TotalMemory     int64          `json:"total_memory"`
	TotalRows       int64          `json:"total_rows"`
	OperationCounts map[string]int `json:"operation_counts"`
	AverageDuration time.Duration  `json:"average_duration"`
}
