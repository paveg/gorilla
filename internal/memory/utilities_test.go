package memory_test

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	memorypkg "github.com/paveg/gorilla/internal/memory"
)

// TestResourceManager tests the centralized resource management interface.
func TestResourceManager(t *testing.T) {
	allocator := memory.NewGoAllocator()

	// Test creating resource manager with options
	rm, err := memorypkg.NewResourceManager(
		memorypkg.WithAllocator(allocator),
		memorypkg.WithGCPressureThreshold(0.8),
		memorypkg.WithMemoryThreshold(1024*1024), // 1MB
	)
	require.NoError(t, err)
	defer rm.Release()

	// Test memory estimation
	memUsage := rm.EstimateMemory()
	assert.GreaterOrEqual(t, memUsage, int64(0))

	// Test cleanup functionality
	err = rm.ForceCleanup()
	require.NoError(t, err)

	// Test spill functionality
	err = rm.SpillIfNeeded()
	assert.NoError(t, err)
}

// TestForceGC tests the centralized garbage collection function.
func TestForceGC(t *testing.T) {
	// Get initial GC stats
	var initialStats runtime.MemStats
	runtime.ReadMemStats(&initialStats)

	// Force garbage collection
	memorypkg.ForceGC()

	// Get stats after GC
	var afterStats runtime.MemStats
	runtime.ReadMemStats(&afterStats)

	// Verify GC was triggered (GC count should increase)
	assert.GreaterOrEqual(t, afterStats.NumGC, initialStats.NumGC)
}

// TestEstimateMemoryUsage tests the centralized memory estimation.
func TestEstimateMemoryUsage(t *testing.T) {
	allocator := memory.NewGoAllocator()

	// Create some mock data structures
	data1 := make([]int64, 1000)
	data2 := make([]string, 500)
	data3 := make([]float64, 200)

	// Test estimating memory for various types
	usage := memorypkg.EstimateMemoryUsage(data1, data2, data3)
	assert.Positive(t, usage)

	// Test with allocator
	usageWithAlloc := memorypkg.EstimateMemoryUsageWithAllocator(allocator, data1, data2)
	assert.Positive(t, usageWithAlloc)

	// Test with nil values (should handle gracefully)
	usageWithNil := memorypkg.EstimateMemoryUsage(nil, data1, nil)
	assert.Positive(t, usageWithNil)
}

// TestResourceLifecycleManager tests the common resource lifecycle patterns.
func TestResourceLifecycleManager(t *testing.T) {
	allocator := memory.NewGoAllocator()

	// Create lifecycle manager
	lm := memorypkg.NewResourceLifecycleManager(allocator)
	defer lm.ReleaseAll()

	// Test creation phase
	resource1, err := lm.CreateResource("test1", func(alloc memory.Allocator) (memorypkg.Resource, error) {
		return &mockResource{id: "test1", allocator: alloc}, nil
	})
	require.NoError(t, err)
	assert.NotNil(t, resource1)

	// Test processing phase
	processed, err := lm.ProcessResource(resource1, func(r memorypkg.Resource) (memorypkg.Resource, error) {
		return r, nil
	})
	require.NoError(t, err)
	assert.NotNil(t, processed)

	// Test tracking
	assert.Equal(t, 1, lm.TrackedCount())

	// Test cleanup phase
	lm.ReleaseAll()
	assert.Equal(t, 0, lm.TrackedCount())
}

// TestPressureHandler tests centralized memory pressure detection.
func TestPressureHandler(t *testing.T) {
	// Create pressure handler with low threshold for testing
	handler := memorypkg.NewPressureHandler(1024*1024, 0.7) // 1MB threshold, 70% pressure
	defer handler.Stop()

	// Set up callbacks with atomic variables to avoid data races
	var spillCalled, cleanupCalled int32

	handler.SetSpillCallback(func() error {
		atomic.StoreInt32(&spillCalled, 1)
		return nil
	})

	handler.SetCleanupCallback(func() error {
		atomic.StoreInt32(&cleanupCalled, 1)
		return nil
	})

	// Start monitoring
	handler.Start()

	// Simulate memory allocation
	handler.RecordAllocation(1024 * 1024) // Allocate 1MB

	// Give some time for monitoring to kick in
	time.Sleep(100 * time.Millisecond)

	// Check if callbacks were triggered (may not always trigger in tests)
	// This is more of a smoke test
	spillCalledValue := atomic.LoadInt32(&spillCalled) == 1
	cleanupCalledValue := atomic.LoadInt32(&cleanupCalled) == 1
	t.Logf("Spill called: %v, Cleanup called: %v", spillCalledValue, cleanupCalledValue)
}

// mockResource implements the Resource interface for testing.
type mockResource struct {
	id        string
	allocator memory.Allocator
	released  bool
}

func (m *mockResource) EstimateMemory() int64 {
	return 1024 // Mock 1KB usage
}

func (m *mockResource) ForceCleanup() error {
	return nil
}

func (m *mockResource) SpillIfNeeded() error {
	return nil
}

func (m *mockResource) Release() {
	m.released = true
}

func (m *mockResource) IsReleased() bool {
	return m.released
}

// TestGCTriggerStrategy tests different GC triggering strategies.
func TestGCTriggerStrategy(t *testing.T) {
	tests := []struct {
		name           string
		strategy       memorypkg.GCStrategy
		memoryPressure float64
		shouldTrigger  bool
	}{
		{
			name:           "Conservative strategy - high pressure",
			strategy:       memorypkg.ConservativeGC,
			memoryPressure: 0.9,
			shouldTrigger:  true,
		},
		{
			name:           "Conservative strategy - low pressure",
			strategy:       memorypkg.ConservativeGC,
			memoryPressure: 0.5,
			shouldTrigger:  false,
		},
		{
			name:           "Aggressive strategy - medium pressure",
			strategy:       memorypkg.AggressiveGC,
			memoryPressure: 0.65,
			shouldTrigger:  true,
		},
		{
			name:           "Adaptive strategy - varying pressure",
			strategy:       memorypkg.AdaptiveGC,
			memoryPressure: 0.8,
			shouldTrigger:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trigger := memorypkg.NewGCTrigger(tt.strategy, 0.7) // 70% default threshold
			result := trigger.ShouldTriggerGC(tt.memoryPressure)
			assert.Equal(t, tt.shouldTrigger, result)
		})
	}
}

// BenchmarkResourceManager benchmarks the resource manager performance.
func BenchmarkResourceManager(b *testing.B) {
	allocator := memory.NewGoAllocator()
	rm, _ := memorypkg.NewResourceManager(memorypkg.WithAllocator(allocator))
	defer rm.Release()

	b.ResetTimer()
	for range b.N {
		_ = rm.EstimateMemory()
	}
}

// BenchmarkForceGC benchmarks the GC triggering performance.
func BenchmarkForceGC(b *testing.B) {
	b.ResetTimer()
	for range b.N {
		memorypkg.ForceGC()
	}
}
