package parallel

import (
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

// AllocatorPool manages a pool of memory allocators for safe reuse in parallel processing
type AllocatorPool struct {
	pool     sync.Pool
	active   int64 // Number of allocators currently in use
	maxSize  int   // Maximum number of allocators to keep in pool
	released bool  // Flag to track if pool has been closed
	mu       sync.RWMutex
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
	return p.pool.Get().(memory.Allocator)
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
	threshold    int64 // Memory threshold in bytes
	currentUsage int64 // Current memory usage (atomic)
	maxParallel  int   // Maximum parallelism allowed
}

// NewMemoryMonitor creates a new memory monitor with the specified threshold and max parallelism
func NewMemoryMonitor(threshold int64, maxParallel int) *MemoryMonitor {
	if maxParallel <= 0 {
		maxParallel = runtime.NumCPU()
	}

	return &MemoryMonitor{
		threshold:   threshold,
		maxParallel: maxParallel,
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
	const (
		veryHighPressureThreshold  = 0.9
		highPressureThreshold      = 0.8
		moderatePressureThreshold  = 0.6
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
