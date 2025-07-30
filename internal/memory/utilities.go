// Package memory provides consolidated memory management utilities to reduce code duplication
// across streaming, batch processing, and parallel execution components.
//
// This package addresses the duplication patterns identified in similarity analysis:
// - GC triggering logic (forceGC pattern)
// - Memory estimation calculations (estimateMemoryUsage logic)
// - Resource lifecycle management (create/process/cleanup pattern)
// - Memory pressure detection and cleanup callbacks
// - Allocation/deallocation tracking
package memory

import (
	"reflect"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ResourceManager provides a unified interface for managing memory resources
// with estimation, cleanup, and spill capabilities.
type ResourceManager interface {
	// EstimateMemory returns the estimated memory usage in bytes
	EstimateMemory() int64
	// ForceCleanup performs immediate resource cleanup
	ForceCleanup() error
	// SpillIfNeeded spills resources to disk if memory pressure is high
	SpillIfNeeded() error
	// Release releases all managed resources
	Release()
}

// Resource represents a manageable memory resource
type Resource interface {
	EstimateMemory() int64
	ForceCleanup() error
	SpillIfNeeded() error
	Release()
}

// resourceManager is the concrete implementation of ResourceManager
type resourceManager struct {
	allocator           memory.Allocator
	gcPressureThreshold float64
	memoryThreshold     int64
	resources           []Resource
	mu                  sync.RWMutex
	released            bool
}

// Option configures the ResourceManager
type Option func(*resourceManager)

// WithAllocator sets the memory allocator
func WithAllocator(allocator memory.Allocator) Option {
	return func(rm *resourceManager) {
		rm.allocator = allocator
	}
}

// WithGCPressureThreshold sets the GC pressure threshold (0.0-1.0)
func WithGCPressureThreshold(threshold float64) Option {
	return func(rm *resourceManager) {
		rm.gcPressureThreshold = threshold
	}
}

// WithMemoryThreshold sets the memory threshold in bytes
func WithMemoryThreshold(threshold int64) Option {
	return func(rm *resourceManager) {
		rm.memoryThreshold = threshold
	}
}

// NewResourceManager creates a new resource manager with the specified options
func NewResourceManager(opts ...Option) (ResourceManager, error) {
	rm := &resourceManager{
		allocator:           memory.NewGoAllocator(),
		gcPressureThreshold: defaultGCPressureThreshold,
		memoryThreshold:     defaultMemoryThresholdGB * bytesToMBConversion * bytesToMBConversion, // Convert MB to bytes
		resources:           make([]Resource, 0),
	}

	for _, opt := range opts {
		opt(rm)
	}

	return rm, nil
}

// EstimateMemory returns the estimated memory usage of all managed resources
func (rm *resourceManager) EstimateMemory() int64 {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if rm.released {
		return 0
	}

	var total int64
	for _, resource := range rm.resources {
		if resource != nil {
			total += resource.EstimateMemory()
		}
	}

	return total
}

// ForceCleanup performs cleanup on all managed resources
func (rm *resourceManager) ForceCleanup() error {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if rm.released {
		return nil
	}

	// Trigger GC first
	ForceGC()

	// Then cleanup individual resources
	for _, resource := range rm.resources {
		if resource != nil {
			if err := resource.ForceCleanup(); err != nil {
				return err
			}
		}
	}

	return nil
}

// SpillIfNeeded checks memory pressure and spills resources if needed
func (rm *resourceManager) SpillIfNeeded() error {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if rm.released {
		return nil
	}

	// Check current memory pressure
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	pressure := float64(memStats.HeapInuse) / float64(memStats.Sys)

	// If pressure exceeds threshold, spill resources
	if pressure > rm.gcPressureThreshold {
		for _, resource := range rm.resources {
			if resource != nil {
				if err := resource.SpillIfNeeded(); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// Release releases all managed resources
func (rm *resourceManager) Release() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.released {
		return
	}

	for _, resource := range rm.resources {
		if resource != nil {
			resource.Release()
		}
	}

	rm.resources = rm.resources[:0]
	rm.released = true
}

// Track adds a resource to be managed
func (rm *resourceManager) Track(resource Resource) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if !rm.released && resource != nil {
		rm.resources = append(rm.resources, resource)
	}
}

// ForceGC consolidates the GC triggering logic found across multiple components.
// This function provides consistent garbage collection behavior and can be
// configured with different strategies in the future.
func ForceGC() {
	// Force garbage collection
	runtime.GC()

	// Force finalizer processing
	runtime.GC()

	// Give GC time to complete
	runtime.Gosched()
}

// EstimateMemoryUsage provides centralized memory estimation for various data types.
// This consolidates the memory calculation logic found in multiple components.
func EstimateMemoryUsage(resources ...interface{}) int64 {
	var total int64

	for _, resource := range resources {
		if resource == nil {
			continue
		}

		total += estimateSingleResource(resource)
	}

	return total
}

// EstimateMemoryUsageWithAllocator estimates memory usage considering allocator overhead
func EstimateMemoryUsageWithAllocator(allocator memory.Allocator, resources ...interface{}) int64 {
	baseUsage := EstimateMemoryUsage(resources...)

	// Add allocator overhead (estimated 10% overhead)
	const allocatorOverheadRatio = 0.1
	overhead := int64(float64(baseUsage) * allocatorOverheadRatio)

	return baseUsage + overhead
}

// estimateSingleResource estimates memory usage for a single resource
func estimateSingleResource(resource interface{}) int64 {
	if resource == nil {
		return 0
	}

	rv := reflect.ValueOf(resource)
	rt := reflect.TypeOf(resource)

	switch rv.Kind() {
	case reflect.Slice:
		return estimateSliceMemory(rv)
	case reflect.Map:
		return estimateMapMemory(rv)
	case reflect.Ptr:
		if rv.IsNil() {
			return 0
		}
		return int64(rt.Elem().Size()) + estimateSingleResource(rv.Elem().Interface())
	case reflect.String:
		return int64(rv.Len())
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return int64(rt.Size())
	case reflect.Array:
		elemSize := rt.Elem().Size()
		return int64(rv.Len()) * int64(elemSize)
	case reflect.Struct:
		return int64(rt.Size())
	case reflect.Interface:
		if rv.IsNil() {
			return 0
		}
		return int64(rt.Size()) + estimateSingleResource(rv.Elem().Interface())
	case reflect.Invalid, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return int64(rt.Size())
	default:
		return int64(rt.Size())
	}
}

// estimateSliceMemory estimates memory usage for slices
func estimateSliceMemory(rv reflect.Value) int64 {
	if rv.IsNil() {
		return 0
	}

	elemSize := rv.Type().Elem().Size()
	// Use unsafe.Slice instead of deprecated reflect.SliceHeader
	sliceHeader := int64(unsafe.Sizeof(uintptr(0)) * sliceHeaderFieldCount) // ptr, len, cap
	elementsSize := int64(rv.Cap()) * int64(elemSize)

	return sliceHeader + elementsSize
}

// estimateMapMemory estimates memory usage for maps
func estimateMapMemory(rv reflect.Value) int64 {
	if rv.IsNil() {
		return 0
	}

	// Rough estimation: map overhead + (key size + value size) * length
	keySize := rv.Type().Key().Size()
	valueSize := rv.Type().Elem().Size()
	mapOverhead := int64(mapOverheadBytes)

	return mapOverhead + int64(rv.Len())*(int64(keySize)+int64(valueSize))
}

// ResourceLifecycleManager manages the create/process/cleanup lifecycle pattern
// found across streaming and batch processing components.
type ResourceLifecycleManager struct {
	allocator memory.Allocator
	resources map[string]Resource
	mu        sync.RWMutex
}

// NewResourceLifecycleManager creates a new lifecycle manager
func NewResourceLifecycleManager(allocator memory.Allocator) *ResourceLifecycleManager {
	return &ResourceLifecycleManager{
		allocator: allocator,
		resources: make(map[string]Resource),
	}
}

// CreateResource creates a new resource using the provided factory function
func (rlm *ResourceLifecycleManager) CreateResource(
	id string,
	factory func(memory.Allocator) (Resource, error),
) (Resource, error) {
	rlm.mu.Lock()
	defer rlm.mu.Unlock()

	resource, err := factory(rlm.allocator)
	if err != nil {
		return nil, err
	}

	rlm.resources[id] = resource
	return resource, nil
}

// ProcessResource applies a processing function to a resource
func (rlm *ResourceLifecycleManager) ProcessResource(
	resource Resource,
	processor func(Resource) (Resource, error),
) (Resource, error) {
	return processor(resource)
}

// TrackedCount returns the number of tracked resources
func (rlm *ResourceLifecycleManager) TrackedCount() int {
	rlm.mu.RLock()
	defer rlm.mu.RUnlock()
	return len(rlm.resources)
}

// ReleaseAll releases all tracked resources
func (rlm *ResourceLifecycleManager) ReleaseAll() {
	rlm.mu.Lock()
	defer rlm.mu.Unlock()

	for _, resource := range rlm.resources {
		if resource != nil {
			resource.Release()
		}
	}

	rlm.resources = make(map[string]Resource)
}

// MemoryPressureHandler consolidates memory pressure detection and cleanup logic
type MemoryPressureHandler struct {
	threshold       int64
	gcThreshold     float64
	currentUsage    int64
	spillCallback   func() error
	cleanupCallback func() error
	monitoring      bool
	stopChan        chan struct{}
	mu              sync.RWMutex
	usageMu         sync.Mutex // Separate mutex for usage tracking
}

// NewMemoryPressureHandler creates a new memory pressure handler
func NewMemoryPressureHandler(threshold int64, gcThreshold float64) *MemoryPressureHandler {
	return &MemoryPressureHandler{
		threshold:   threshold,
		gcThreshold: gcThreshold,
		stopChan:    make(chan struct{}),
	}
}

// SetSpillCallback sets the callback for spilling operations
func (mph *MemoryPressureHandler) SetSpillCallback(callback func() error) {
	mph.mu.Lock()
	defer mph.mu.Unlock()
	mph.spillCallback = callback
}

// SetCleanupCallback sets the callback for cleanup operations
func (mph *MemoryPressureHandler) SetCleanupCallback(callback func() error) {
	mph.mu.Lock()
	defer mph.mu.Unlock()
	mph.cleanupCallback = callback
}

// RecordAllocation records memory allocation
func (mph *MemoryPressureHandler) RecordAllocation(bytes int64) {
	mph.usageMu.Lock()
	mph.currentUsage += bytes
	newUsage := mph.currentUsage
	mph.usageMu.Unlock()

	// Check if we need to trigger spilling
	if newUsage >= mph.threshold {
		mph.triggerSpill()
	}
}

// RecordDeallocation records memory deallocation
func (mph *MemoryPressureHandler) RecordDeallocation(bytes int64) {
	mph.usageMu.Lock()
	mph.currentUsage -= bytes
	mph.usageMu.Unlock()
}

// Start starts the memory pressure monitoring
func (mph *MemoryPressureHandler) Start() {
	mph.mu.Lock()
	defer mph.mu.Unlock()

	if mph.monitoring {
		return
	}

	mph.monitoring = true
	go mph.monitorLoop()
}

// Stop stops the memory pressure monitoring
func (mph *MemoryPressureHandler) Stop() {
	mph.mu.Lock()
	defer mph.mu.Unlock()

	if !mph.monitoring {
		return
	}

	mph.monitoring = false
	close(mph.stopChan)
}

// monitorLoop runs the memory pressure monitoring loop
func (mph *MemoryPressureHandler) monitorLoop() {
	ticker := time.NewTicker(monitoringIntervalSeconds * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			mph.checkMemoryPressure()
		case <-mph.stopChan:
			return
		}
	}
}

// checkMemoryPressure checks system memory pressure and triggers cleanup
func (mph *MemoryPressureHandler) checkMemoryPressure() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	pressure := float64(memStats.HeapInuse) / float64(memStats.Sys)

	if pressure > mph.gcThreshold {
		mph.triggerCleanup()
	}
}

// triggerSpill triggers the spill callback
func (mph *MemoryPressureHandler) triggerSpill() {
	mph.mu.RLock()
	callback := mph.spillCallback
	mph.mu.RUnlock()

	if callback != nil {
		go func() {
			_ = callback() // Error handling would be added in production
		}()
	}
}

// triggerCleanup triggers the cleanup callback
func (mph *MemoryPressureHandler) triggerCleanup() {
	mph.mu.RLock()
	callback := mph.cleanupCallback
	mph.mu.RUnlock()

	if callback != nil {
		go func() {
			_ = callback() // Error handling would be added in production
		}()
	}
}

// GCStrategy represents different garbage collection strategies
type GCStrategy int

const (
	// ConservativeGC triggers GC only under high memory pressure
	ConservativeGC GCStrategy = iota
	// AggressiveGC triggers GC more frequently
	AggressiveGC
	// AdaptiveGC adapts based on system conditions
	AdaptiveGC

	// Memory and performance constants
	mapOverheadBytes           = 48   // Approximate map header overhead in bytes
	monitoringIntervalSeconds  = 5    // Memory monitoring interval in seconds
	gcRateThreshold            = 0.1  // GC rate threshold for adaptive strategy
	aggressiveGCOffset         = 0.1  // Offset for aggressive GC triggering
	conservativeGCOffset       = 0.1  // Offset for conservative GC triggering
	adaptiveGCOffset           = 0.05 // Offset for adaptive GC triggering
	defaultGCPressureThreshold = 0.8  // Default GC pressure threshold
	defaultMemoryThresholdGB   = 1024 // Default memory threshold in MB (1GB)
	
	// Slice header constants
	sliceHeaderFieldCount = 3    // Number of fields in slice header (ptr, len, cap)
	bytesToMBConversion   = 1024 // Conversion factor from bytes to MB
)

// GCTrigger provides configurable GC triggering strategies
type GCTrigger struct {
	strategy  GCStrategy
	threshold float64
}

// NewGCTrigger creates a new GC trigger with the specified strategy
func NewGCTrigger(strategy GCStrategy, threshold float64) *GCTrigger {
	return &GCTrigger{
		strategy:  strategy,
		threshold: threshold,
	}
}

// ShouldTriggerGC determines if GC should be triggered based on memory pressure
func (gc *GCTrigger) ShouldTriggerGC(memoryPressure float64) bool {
	switch gc.strategy {
	case ConservativeGC:
		return memoryPressure > gc.threshold+conservativeGCOffset // Only trigger at very high pressure
	case AggressiveGC:
		return memoryPressure > (gc.threshold - aggressiveGCOffset) // Trigger earlier
	case AdaptiveGC:
		// Adapt based on current system load
		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)
		if memStats.Mallocs > 0 {
			gcRate := float64(memStats.NumGC) / float64(memStats.Mallocs)

			if gcRate > gcRateThreshold { // High GC rate, be more conservative
				return memoryPressure > gc.threshold+adaptiveGCOffset
			}
		}
		return memoryPressure > gc.threshold
	default:
		return memoryPressure > gc.threshold
	}
}
