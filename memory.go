package gorilla

import (
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Releasable represents any resource that can be released to free memory
type Releasable interface {
	Release()
}

// MemoryManager helps track and release multiple resources automatically
type MemoryManager struct {
	allocator memory.Allocator
	resources []Releasable
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
		m.resources = append(m.resources, resource)
	}
}

// Count returns the number of tracked resources
func (m *MemoryManager) Count() int {
	return len(m.resources)
}

// ReleaseAll releases all tracked resources and clears the tracking list
func (m *MemoryManager) ReleaseAll() {
	for _, resource := range m.resources {
		if resource != nil {
			resource.Release()
		}
	}
	m.resources = m.resources[:0] // Clear the slice but keep capacity
}

// WithDataFrame creates a DataFrame, executes a function with it, and automatically releases it
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
