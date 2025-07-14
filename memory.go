package gorilla

import (
	"sync"

	"github.com/apache/arrow-go/v18/arrow/memory"
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

// NewMemoryManager creates a new memory manager with the given allocator
func NewMemoryManager(allocator memory.Allocator) *MemoryManager {
	return &MemoryManager{
		allocator: allocator,
		resources: make([]Releasable, 0),
	}
}

// Track adds a resource to be managed and automatically released
func (m *MemoryManager) Track(resource Releasable) {
	if resource != nil {
		m.mu.Lock()
		m.resources = append(m.resources, resource)
		m.mu.Unlock()
	}
}

// Count returns the number of tracked resources
func (m *MemoryManager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.resources)
}

// ReleaseAll releases all tracked resources and clears the tracking list
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

// WithSeries creates a Series, executes a function with it, and automatically releases it
func WithSeries(factory func() ISeries, fn func(ISeries) error) error {
	s := factory()
	defer s.Release()
	return fn(s)
}

// WithMemoryManager creates a memory manager, executes a function with it, and releases all tracked resources
func WithMemoryManager(allocator memory.Allocator, fn func(*MemoryManager) error) error {
	manager := NewMemoryManager(allocator)
	defer manager.ReleaseAll()
	return fn(manager)
}
