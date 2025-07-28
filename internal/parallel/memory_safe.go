package parallel

import (
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/config"
)

const (
	// DefaultMemoryThreshold is the default memory threshold for parallel operations (1GB)
	DefaultMemoryThreshold = 1024 * 1024 * 1024
	// DefaultGCPressureThreshold is the default GC pressure threshold
	DefaultGCPressureThreshold = 0.8
	// ModeratePressureOffset is the offset for moderate pressure threshold
	ModeratePressureOffset = 0.2
	// VeryHighPressureOffset is the offset for very high pressure threshold
	VeryHighPressureOffset = 0.1
)

// AllocatorPool manages a pool of memory allocators for safe reuse in parallel processing
type AllocatorPool struct {
	pool           sync.Pool
	active         int64 // Number of allocators currently in use
	maxSize        int   // Maximum number of allocators to keep in pool
	released       bool  // Flag to track if pool has been closed
	mu             sync.RWMutex
	totalAllocated int64 // Total memory allocated by all allocators
	peakAllocated  int64 // Peak memory allocated by the pool
}

// NewAllocatorPool creates a new allocator pool with the specified maximum size
func NewAllocatorPool(maxSize int) *AllocatorPool {
	if maxSize <= 0 {
		maxSize = runtime.NumCPU()
	}

	return &AllocatorPool{
		pool: sync.Pool{
			New: func() interface{} {
				return memory.NewGoAllocator()
			},
		},
		maxSize: maxSize,
	}
}

// Get retrieves an allocator from the pool
func (p *AllocatorPool) Get() memory.Allocator {
	p.mu.RLock()
	if p.released {
		p.mu.RUnlock()
		return nil
	}
	p.mu.RUnlock()

	atomic.AddInt64(&p.active, 1)
	alloc, ok := p.pool.Get().(memory.Allocator)
	if !ok {
		// If type assertion fails, create a new allocator
		return memory.NewGoAllocator()
	}
	return alloc
}

// Put returns an allocator to the pool for reuse
func (p *AllocatorPool) Put(alloc memory.Allocator) {
	if alloc == nil {
		return
	}

	p.mu.RLock()
	if p.released {
		p.mu.RUnlock()
		return
	}
	p.mu.RUnlock()

	atomic.AddInt64(&p.active, -1)
	p.pool.Put(alloc)
}

// ActiveCount returns the number of allocators currently in use
func (p *AllocatorPool) ActiveCount() int64 {
	return atomic.LoadInt64(&p.active)
}

// Close shuts down the allocator pool
func (p *AllocatorPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.released = true
}

// MemoryMonitor tracks memory usage and provides adaptive parallelism control
type MemoryMonitor struct {
	threshold           int64   // Memory threshold in bytes
	currentUsage        int64   // Current memory usage (atomic)
	maxParallel         int     // Maximum parallelism allowed
	gcPressureThreshold float64 // GC pressure threshold for adaptive parallelism
}

// NewMemoryMonitor creates a new memory monitor with the specified threshold and max parallelism
func NewMemoryMonitor(threshold int64, maxParallel int) *MemoryMonitor {
	if maxParallel <= 0 {
		maxParallel = runtime.NumCPU()
	}

	return &MemoryMonitor{
		threshold:           threshold,
		maxParallel:         maxParallel,
		gcPressureThreshold: DefaultGCPressureThreshold,
	}
}

// NewMemoryMonitorFromConfig creates a new memory monitor using configuration values
func NewMemoryMonitorFromConfig(cfg config.Config) *MemoryMonitor {
	threshold := cfg.MemoryThreshold
	if threshold == 0 {
		// Use a reasonable default if not configured
		threshold = DefaultMemoryThreshold
	}

	maxParallel := cfg.MaxParallelism
	if maxParallel <= 0 {
		maxParallel = runtime.NumCPU()
	}

	gcThreshold := cfg.GCPressureThreshold
	if gcThreshold == 0.0 {
		gcThreshold = DefaultGCPressureThreshold
	}

	return &MemoryMonitor{
		threshold:           threshold,
		maxParallel:         maxParallel,
		gcPressureThreshold: gcThreshold,
	}
}

// CanAllocate checks if the requested memory size can be allocated without exceeding threshold
func (m *MemoryMonitor) CanAllocate(size int64) bool {
	current := atomic.LoadInt64(&m.currentUsage)
	return current+size <= m.threshold
}

// RecordAllocation records a memory allocation
func (m *MemoryMonitor) RecordAllocation(size int64) {
	atomic.AddInt64(&m.currentUsage, size)
}

// RecordDeallocation records a memory deallocation
func (m *MemoryMonitor) RecordDeallocation(size int64) {
	atomic.AddInt64(&m.currentUsage, -size)
}

// AdjustParallelism returns the recommended parallelism level based on current memory pressure
func (m *MemoryMonitor) AdjustParallelism() int {
	// Define thresholds relative to the configured GC pressure threshold
	// These are calculated as percentages of the configured threshold
	veryHighPressureThreshold := m.gcPressureThreshold + VeryHighPressureOffset
	highPressureThreshold := m.gcPressureThreshold // At gc threshold
	moderatePressureThreshold := m.gcPressureThreshold - ModeratePressureOffset

	const (
		veryHighPressureDivider    = 4
		highPressureDivider        = 2
		moderatePressureMultiplier = 3
		moderatePressureDivider    = 4
	)

	usage := atomic.LoadInt64(&m.currentUsage)
	ratio := float64(usage) / float64(m.threshold)

	switch {
	case ratio > veryHighPressureThreshold:
		// Very high memory pressure - reduce to minimum
		return maxInt(1, m.maxParallel/veryHighPressureDivider)
	case ratio > highPressureThreshold:
		// High memory pressure - reduce significantly
		return maxInt(1, m.maxParallel/highPressureDivider)
	case ratio > moderatePressureThreshold:
		// Moderate memory pressure - reduce moderately
		return maxInt(1, (m.maxParallel*moderatePressureMultiplier)/moderatePressureDivider)
	default:
		// Low memory pressure - allow full parallelism
		return m.maxParallel
	}
}

// CurrentUsage returns the current memory usage
func (m *MemoryMonitor) CurrentUsage() int64 {
	return atomic.LoadInt64(&m.currentUsage)
}

// ChunkProcessor provides isolated processing context for parallel chunks
type ChunkProcessor struct {
	allocatorPool *AllocatorPool
	allocator     memory.Allocator
	chunkID       int
	released      bool
	mu            sync.Mutex
}

// NewChunkProcessor creates a new chunk processor with isolated memory context
func NewChunkProcessor(pool *AllocatorPool, chunkID int) *ChunkProcessor {
	return &ChunkProcessor{
		allocatorPool: pool,
		allocator:     pool.Get(),
		chunkID:       chunkID,
	}
}

// GetAllocator returns the allocator for this chunk processor
func (cp *ChunkProcessor) GetAllocator() memory.Allocator {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.released {
		return nil
	}
	return cp.allocator
}

// ChunkID returns the unique ID for this chunk
func (cp *ChunkProcessor) ChunkID() int {
	return cp.chunkID
}

// Release returns the allocator to the pool and marks this processor as released
func (cp *ChunkProcessor) Release() {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.released {
		return
	}

	if cp.allocator != nil {
		cp.allocatorPool.Put(cp.allocator)
		cp.allocator = nil
	}
	cp.released = true
}

// SafeDataFrame provides thread-safe access to DataFrame data with copy-on-access semantics
type SafeDataFrame struct {
	mu   sync.RWMutex
	data interface{} // Will be *DataFrame when integrated
}

// NewSafeDataFrame creates a new thread-safe DataFrame wrapper
func NewSafeDataFrame(data interface{}) *SafeDataFrame {
	return &SafeDataFrame{
		data: data,
	}
}

// Clone creates an independent copy for safe parallel access
// This will be implemented with actual DataFrame integration
func (sdf *SafeDataFrame) Clone(allocator memory.Allocator) (interface{}, error) {
	sdf.mu.RLock()
	defer sdf.mu.RUnlock()

	// TODO: Implement actual DataFrame deep copy logic
	// For now, return the data (this will be updated with DataFrame integration)
	return sdf.data, nil
}

// maxInt returns the maximum of two integers
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// RecordAllocation records memory allocation in the pool
func (p *AllocatorPool) RecordAllocation(bytes int64) {
	newTotal := atomic.AddInt64(&p.totalAllocated, bytes)

	// Update peak if necessary
	for {
		peak := atomic.LoadInt64(&p.peakAllocated)
		if newTotal <= peak || atomic.CompareAndSwapInt64(&p.peakAllocated, peak, newTotal) {
			break
		}
	}
}

// RecordDeallocation records memory deallocation in the pool
func (p *AllocatorPool) RecordDeallocation(bytes int64) {
	atomic.AddInt64(&p.totalAllocated, -bytes)
}

// TotalAllocated returns the total memory allocated by the pool
func (p *AllocatorPool) TotalAllocated() int64 {
	return atomic.LoadInt64(&p.totalAllocated)
}

// PeakAllocated returns the peak memory allocated by the pool
func (p *AllocatorPool) PeakAllocated() int64 {
	return atomic.LoadInt64(&p.peakAllocated)
}

// PoolStats provides statistics about the allocator pool
type PoolStats struct {
	ActiveAllocators int64
	TotalAllocated   int64
	PeakAllocated    int64
	MaxSize          int
}

// GetStats returns current pool statistics
func (p *AllocatorPool) GetStats() PoolStats {
	return PoolStats{
		ActiveAllocators: atomic.LoadInt64(&p.active),
		TotalAllocated:   atomic.LoadInt64(&p.totalAllocated),
		PeakAllocated:    atomic.LoadInt64(&p.peakAllocated),
		MaxSize:          p.maxSize,
	}
}

// AdvancedMemoryPool provides enhanced memory pool management with adaptive sizing
type AdvancedMemoryPool struct {
	allocatorPool *AllocatorPool
	memoryMonitor *MemoryMonitor
	adaptiveSize  bool
}

// NewAdvancedMemoryPool creates a new advanced memory pool
func NewAdvancedMemoryPool(maxSize int, memoryThreshold int64, adaptiveSize bool) *AdvancedMemoryPool {
	allocatorPool := NewAllocatorPool(maxSize)
	memoryMonitor := NewMemoryMonitor(memoryThreshold, maxSize)

	return &AdvancedMemoryPool{
		allocatorPool: allocatorPool,
		memoryMonitor: memoryMonitor,
		adaptiveSize:  adaptiveSize,
	}
}

// GetAllocator gets an allocator from the pool with memory monitoring
func (amp *AdvancedMemoryPool) GetAllocator() memory.Allocator {
	// Check if we can allocate based on memory pressure
	if amp.adaptiveSize {
		parallelism := amp.memoryMonitor.AdjustParallelism()
		if amp.allocatorPool.ActiveCount() >= int64(parallelism) {
			// Return nil to indicate no allocator available due to memory pressure
			return nil
		}
	}

	alloc := amp.allocatorPool.Get()
	if alloc != nil {
		// Wrap allocator with monitoring
		return NewMonitoredAllocator(alloc, amp.allocatorPool)
	}

	return alloc
}

// PutAllocator returns an allocator to the pool
func (amp *AdvancedMemoryPool) PutAllocator(alloc memory.Allocator) {
	if monitored, ok := alloc.(*MonitoredAllocator); ok {
		amp.allocatorPool.Put(monitored.underlying)
	} else {
		amp.allocatorPool.Put(alloc)
	}
}

// GetStats returns comprehensive pool statistics
func (amp *AdvancedMemoryPool) GetStats() (PoolStats, *MemoryMonitor) {
	return amp.allocatorPool.GetStats(), amp.memoryMonitor
}

// Close closes the advanced memory pool
func (amp *AdvancedMemoryPool) Close() {
	amp.allocatorPool.Close()
}

// MonitoredAllocator wraps a memory allocator with monitoring capabilities
type MonitoredAllocator struct {
	underlying memory.Allocator
	pool       *AllocatorPool
}

// NewMonitoredAllocator creates a new monitored allocator
func NewMonitoredAllocator(underlying memory.Allocator, pool *AllocatorPool) *MonitoredAllocator {
	return &MonitoredAllocator{
		underlying: underlying,
		pool:       pool,
	}
}

// Allocate allocates memory and records the allocation
func (ma *MonitoredAllocator) Allocate(size int) []byte {
	buf := ma.underlying.Allocate(size)
	if buf != nil {
		ma.pool.RecordAllocation(int64(size))
	}
	return buf
}

// Reallocate reallocates memory and updates allocation records
func (ma *MonitoredAllocator) Reallocate(size int, b []byte) []byte {
	oldSize := len(b)
	newBuf := ma.underlying.Reallocate(size, b)
	if newBuf != nil {
		// Record the difference in allocation
		ma.pool.RecordDeallocation(int64(oldSize))
		ma.pool.RecordAllocation(int64(size))
	}
	return newBuf
}

// Free frees memory and records the deallocation
func (ma *MonitoredAllocator) Free(b []byte) {
	if b != nil {
		ma.pool.RecordDeallocation(int64(len(b)))
		ma.underlying.Free(b)
	}
}

// AllocatedBytes returns the number of bytes allocated by the underlying allocator
func (ma *MonitoredAllocator) AllocatedBytes() int64 {
	// Note: Arrow's memory.Allocator interface doesn't provide AllocatedBytes method
	// We could track this internally, but for now return 0
	return 0
}
