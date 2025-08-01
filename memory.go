package gorilla

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/config"
)

const (
	// DefaultCheckInterval is the default interval for memory monitoring.
	DefaultCheckInterval = 5 * time.Second
	// HighMemoryPressureThreshold is the threshold for triggering cleanup.
	HighMemoryPressureThreshold = 0.8
	// DefaultMemoryThreshold is the default memory threshold (1GB).
	DefaultMemoryThreshold = 1024 * 1024 * 1024
)

// Releasable represents any resource that can be released to free memory.
//
// This interface is implemented by DataFrames, Series, and other resources
// that use Apache Arrow memory management. Always call Release() when done
// with a resource to prevent memory leaks.
//
// The recommended pattern is to use defer for automatic cleanup:
//
//	df := gorilla.NewDataFrame(series1, series2)
//	defer df.Release() // Automatic cleanup
type Releasable interface {
	Release()
}

// MemoryManager helps track and release multiple resources automatically.
//
// MemoryManager is useful for complex scenarios where many short-lived resources
// are created and need bulk cleanup. For most use cases, prefer the defer pattern
// with individual Release() calls for better readability.
//
// Use MemoryManager when:
//   - Creating many temporary resources in loops
//   - Complex operations with unpredictable resource lifetimes
//   - Bulk operations where individual defer statements are impractical
//
// The MemoryManager is safe for concurrent use from multiple goroutines.
//
// Example:
//
//	err := gorilla.WithMemoryManager(mem, func(manager *gorilla.MemoryManager) error {
//		for i := 0; i < 1000; i++ {
//			temp := createTempDataFrame(i)
//			manager.Track(temp) // Will be released automatically
//		}
//		return processData()
//	})
//	// All tracked resources are released here
type MemoryManager struct {
	allocator memory.Allocator
	resources []Releasable
	mu        sync.Mutex // Mutex to synchronize access to resources
}

// NewMemoryManager creates a new memory manager with the given allocator.
func NewMemoryManager(allocator memory.Allocator) *MemoryManager {
	return &MemoryManager{
		allocator: allocator,
		resources: make([]Releasable, 0),
	}
}

// Track adds a resource to be managed and automatically released.
func (m *MemoryManager) Track(resource Releasable) {
	if resource != nil {
		m.mu.Lock()
		m.resources = append(m.resources, resource)
		m.mu.Unlock()
	}
}

// Count returns the number of tracked resources.
func (m *MemoryManager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.resources)
}

// ReleaseAll releases all tracked resources and clears the tracking list.
func (m *MemoryManager) ReleaseAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, resource := range m.resources {
		if resource != nil {
			resource.Release()
		}
	}
	m.resources = m.resources[:0] // Clear the slice but keep capacity
}

// WithDataFrame provides automatic resource management for DataFrame operations.
//
// This helper function creates a DataFrame using the provided factory function,
// executes the given operation, and automatically releases the DataFrame when done.
// This pattern is useful for operations where you want guaranteed cleanup.
//
// The factory function should create and return a DataFrame. The operation function
// receives the DataFrame and performs the desired operations. Any error from the
// operation function is returned to the caller.
//
// Example:
//
//	err := gorilla.WithDataFrame(func() *gorilla.DataFrame {
//		mem := memory.NewGoAllocator()
//		series1 := gorilla.NewSeries("name", []string{"Alice", "Bob"}, mem)
//		series2 := gorilla.NewSeries("age", []int64{25, 30}, mem)
//		return gorilla.NewDataFrame(series1, series2)
//	}, func(df *gorilla.DataFrame) error {
//		result, err := df.Lazy().Filter(gorilla.Col("age").Gt(gorilla.Lit(25))).Collect()
//		if err != nil {
//			return err
//		}
//		defer result.Release()
//		fmt.Println(result)
//		return nil
//	})
//	// DataFrame is automatically released here
func WithDataFrame(factory func() *DataFrame, fn func(*DataFrame) error) error {
	df := factory()
	defer df.Release()
	return fn(df)
}

// WithSeries creates a Series, executes a function with it, and automatically releases it.
func WithSeries(factory func() ISeries, fn func(ISeries) error) error {
	s := factory()
	defer s.Release()
	return fn(s)
}

// WithMemoryManager creates a memory manager, executes a function with it, and releases all tracked resources.
func WithMemoryManager(allocator memory.Allocator, fn func(*MemoryManager) error) error {
	manager := NewMemoryManager(allocator)
	defer manager.ReleaseAll()
	return fn(manager)
}

// MemoryStats provides detailed memory usage statistics.
type MemoryStats struct {
	// Total allocated memory in bytes
	AllocatedBytes int64
	// Peak allocated memory in bytes
	PeakAllocatedBytes int64
	// Number of active allocations
	ActiveAllocations int64
	// Number of garbage collections triggered
	GCCount int64
	// Last garbage collection time
	LastGCTime time.Time
	// Memory pressure level (0.0 to 1.0)
	MemoryPressure float64
}

// MemoryUsageMonitor provides real-time memory usage monitoring and automatic spilling.
type MemoryUsageMonitor struct {
	// Memory threshold in bytes for triggering spilling
	spillThreshold int64
	// Current memory usage in bytes
	currentUsage int64
	// Peak memory usage in bytes
	peakUsage int64
	// Number of active resources being tracked
	activeResources int64
	// Number of times spilling was triggered
	spillCount int64
	// Callback function for spilling operations
	spillCallback func() error
	// Callback function for cleanup operations
	cleanupCallback func() error
	// Mutex for synchronizing access to monitor state
	mu sync.RWMutex
	// Channel for stopping the monitoring goroutine
	stopChan chan struct{}
	// Flag indicating if monitoring is active
	monitoring bool
	// Interval for checking memory usage
	checkInterval time.Duration
	// GC pressure threshold for triggering cleanup (0.0-1.0)
	gcPressureThreshold float64
}

// NewMemoryUsageMonitor creates a new memory usage monitor with the specified spill threshold.
func NewMemoryUsageMonitor(spillThreshold int64) *MemoryUsageMonitor {
	return &MemoryUsageMonitor{
		spillThreshold:      spillThreshold,
		stopChan:            make(chan struct{}),
		checkInterval:       DefaultCheckInterval,
		gcPressureThreshold: HighMemoryPressureThreshold, // Use default threshold
	}
}

// NewMemoryUsageMonitorFromConfig creates a new memory usage monitor using configuration values.
func NewMemoryUsageMonitorFromConfig(cfg config.Config) *MemoryUsageMonitor {
	spillThreshold := cfg.MemoryThreshold
	if spillThreshold == 0 {
		// If no threshold is set, use a reasonable default
		spillThreshold = DefaultMemoryThreshold
	}

	gcThreshold := cfg.GCPressureThreshold
	if gcThreshold == 0.0 {
		// Use default threshold if not configured
		gcThreshold = HighMemoryPressureThreshold
	}

	return &MemoryUsageMonitor{
		spillThreshold:      spillThreshold,
		stopChan:            make(chan struct{}),
		checkInterval:       DefaultCheckInterval,
		gcPressureThreshold: gcThreshold,
	}
}

// SetSpillCallback sets the callback function to be called when memory usage exceeds threshold.
func (m *MemoryUsageMonitor) SetSpillCallback(callback func() error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.spillCallback = callback
}

// SetCleanupCallback sets the callback function to be called for cleanup operations.
func (m *MemoryUsageMonitor) SetCleanupCallback(callback func() error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanupCallback = callback
}

// RecordAllocation records a new memory allocation.
func (m *MemoryUsageMonitor) RecordAllocation(bytes int64) {
	newUsage := atomic.AddInt64(&m.currentUsage, bytes)
	atomic.AddInt64(&m.activeResources, 1)

	// Update peak usage if necessary
	for {
		peak := atomic.LoadInt64(&m.peakUsage)
		if newUsage <= peak || atomic.CompareAndSwapInt64(&m.peakUsage, peak, newUsage) {
			break
		}
	}

	// Check if we need to trigger spilling
	if newUsage >= m.spillThreshold {
		m.triggerSpill()
	}
}

// RecordDeallocation records a memory deallocation.
func (m *MemoryUsageMonitor) RecordDeallocation(bytes int64) {
	atomic.AddInt64(&m.currentUsage, -bytes)
	atomic.AddInt64(&m.activeResources, -1)
}

// triggerSpill triggers the spilling callback if memory usage exceeds threshold.
func (m *MemoryUsageMonitor) triggerSpill() {
	m.mu.RLock()
	callback := m.spillCallback
	m.mu.RUnlock()

	if callback != nil {
		atomic.AddInt64(&m.spillCount, 1)
		go func() {
			if err := callback(); err != nil {
				// In a production system, this would be logged properly
				// For now, we track the error but continue operation
				atomic.AddInt64(&m.spillCount, -1) // Decrement on failure
			}
		}()
	}
}

// StartMonitoring starts the background memory monitoring goroutine.
func (m *MemoryUsageMonitor) StartMonitoring() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.monitoring {
		return
	}

	m.monitoring = true
	go m.monitorLoop()
}

// StopMonitoring stops the background memory monitoring goroutine.
func (m *MemoryUsageMonitor) StopMonitoring() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.monitoring {
		return
	}

	m.monitoring = false
	close(m.stopChan)
}

// monitorLoop is the main monitoring loop that runs in a separate goroutine.
func (m *MemoryUsageMonitor) monitorLoop() {
	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.checkMemoryPressure()
		case <-m.stopChan:
			return
		}
	}
}

// checkMemoryPressure checks system memory pressure and triggers cleanup if needed.
func (m *MemoryUsageMonitor) checkMemoryPressure() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Calculate memory pressure based on heap size and system memory
	pressure := float64(memStats.HeapInuse) / float64(memStats.Sys)

	// If memory pressure exceeds configured threshold, trigger cleanup
	if pressure > m.gcPressureThreshold {
		m.mu.RLock()
		callback := m.cleanupCallback
		m.mu.RUnlock()

		if callback != nil {
			go func() {
				if err := callback(); err != nil {
					// In a production system, this would be logged properly
					// For now, we continue as cleanup operations are best-effort
					return
				}
			}()
		}
	}
}

// GetStats returns current memory usage statistics.
func (m *MemoryUsageMonitor) GetStats() MemoryStats {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	currentUsage := atomic.LoadInt64(&m.currentUsage)
	peakUsage := atomic.LoadInt64(&m.peakUsage)
	activeResources := atomic.LoadInt64(&m.activeResources)

	pressure := float64(memStats.HeapInuse) / float64(memStats.Sys)

	return MemoryStats{
		AllocatedBytes:     currentUsage,
		PeakAllocatedBytes: peakUsage,
		ActiveAllocations:  activeResources,
		GCCount:            int64(memStats.NumGC),
		LastGCTime:         time.Now(), // Use current time as placeholder for GC time
		MemoryPressure:     pressure,
	}
}

// CurrentUsage returns the current memory usage in bytes.
func (m *MemoryUsageMonitor) CurrentUsage() int64 {
	return atomic.LoadInt64(&m.currentUsage)
}

// PeakUsage returns the peak memory usage in bytes.
func (m *MemoryUsageMonitor) PeakUsage() int64 {
	return atomic.LoadInt64(&m.peakUsage)
}

// SpillCount returns the number of times spilling was triggered.
func (m *MemoryUsageMonitor) SpillCount() int64 {
	return atomic.LoadInt64(&m.spillCount)
}
