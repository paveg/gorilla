package parallel

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFunctionalWorkStealing tests that work stealing actually works by verifying
// that workers can steal work from each other's queues
func TestFunctionalWorkStealing(t *testing.T) {
	t.Run("work is distributed to worker queues", func(t *testing.T) {
		pool := NewAdvancedWorkerPool(AdvancedWorkerPoolConfig{
			MinWorkers:         4,
			MaxWorkers:         4,
			WorkQueueSize:      5, // Small global queue
			EnableWorkStealing: true,
			EnableMetrics:      true,
		})
		defer pool.Close()

		// Create more work items to ensure imbalance
		items := make([]int, 50)
		for i := range items {
			items[i] = i + 1
		}

		// Process items
		results := ProcessGeneric(pool, items, func(x int) int {
			// Create some processing time variation
			if x%7 == 0 {
				time.Sleep(30 * time.Millisecond)
			} else {
				time.Sleep(2 * time.Millisecond)
			}
			return x * 2
		})

		// Verify results
		assert.Len(t, results, 50)

		// Verify that work stealing occurred
		metrics := pool.GetMetrics()
		t.Logf("Work stealing count: %d", metrics.WorkStealingCount)
		t.Logf("Total tasks processed: %d", metrics.TotalTasksProcessed)

		// With 4 workers and 50 tasks distributed round-robin, some workers should
		// finish early and steal from others
		assert.Greater(t, metrics.WorkStealingCount, int64(0), "Work stealing should have occurred")
	})

	t.Run("workers can steal from each other's queues", func(t *testing.T) {
		pool := NewAdvancedWorkerPool(AdvancedWorkerPoolConfig{
			MinWorkers:         2,
			MaxWorkers:         2,
			WorkQueueSize:      5, // Small global queue to force work stealing
			EnableWorkStealing: true,
			EnableMetrics:      true,
		})
		defer pool.Close()

		// Create a lot of work to ensure imbalance
		items := make([]int, 100)
		for i := range items {
			items[i] = i
		}

		var processedBy []int
		var processingMutex sync.Mutex

		results := ProcessGeneric(pool, items, func(x int) int {
			processingMutex.Lock()
			// Simple way to identify which worker processed this
			// We'll use the worker ID pattern later
			processedBy = append(processedBy, x)
			processingMutex.Unlock()

			// Some work takes longer to create imbalance
			if x%5 == 0 {
				time.Sleep(50 * time.Millisecond)
			} else {
				time.Sleep(1 * time.Millisecond)
			}
			return x * 2
		})

		assert.Len(t, results, 100)
		assert.Len(t, processedBy, 100)

		// Verify work stealing occurred
		metrics := pool.GetMetrics()
		t.Logf("Work stealing count: %d", metrics.WorkStealingCount)
		t.Logf("Total tasks processed: %d", metrics.TotalTasksProcessed)

		// Since we're distributing work via round-robin, work stealing should occur
		// when one worker finishes its local work and steals from others
		assert.Greater(t, metrics.WorkStealingCount, int64(0), "Work stealing should have occurred due to imbalanced work")
	})
}

// TestWorkStealingQueue tests the work stealing queue implementation
func TestWorkStealingQueue(t *testing.T) {
	t.Run("push and pop operations", func(t *testing.T) {
		queue := newWorkStealingQueue()

		// Queue should be empty initially
		assert.Nil(t, queue.steal())
		assert.Nil(t, queue.popLocal())

		// Push work items
		item1 := workItem{index: 1, data: "test1"}
		item2 := workItem{index: 2, data: "test2"}

		queue.pushLocal(item1)
		queue.pushLocal(item2)

		// Pop local should return items in LIFO order
		popped := queue.popLocal()
		require.NotNil(t, popped)
		assert.Equal(t, 2, popped.index)

		// Steal should return items in FIFO order (from the other end)
		stolen := queue.steal()
		require.NotNil(t, stolen)
		assert.Equal(t, 1, stolen.index)

		// Queue should be empty now
		assert.Nil(t, queue.steal())
		assert.Nil(t, queue.popLocal())
	})

	t.Run("concurrent push and steal operations", func(t *testing.T) {
		queue := newWorkStealingQueue()

		const numItems = 100
		const numWorkers = 4

		var wg sync.WaitGroup
		stolen := make(chan workItem, numItems)

		// Start stealers
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < numItems/numWorkers; j++ {
					for {
						if item := queue.steal(); item != nil {
							stolen <- *item
							break
						}
						time.Sleep(time.Microsecond)
					}
				}
			}()
		}

		// Push items
		for i := 0; i < numItems; i++ {
			queue.pushLocal(workItem{index: i, data: i})
		}

		wg.Wait()
		close(stolen)

		// Verify all items were stolen
		var stolenItems []workItem
		for item := range stolen {
			stolenItems = append(stolenItems, item)
		}

		assert.Len(t, stolenItems, numItems)
	})
}
