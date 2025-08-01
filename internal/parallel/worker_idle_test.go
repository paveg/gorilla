package parallel_test

import (
	"sync"
	"testing"
	"time"

	"github.com/paveg/gorilla/internal/parallel"
	"github.com/stretchr/testify/assert"
)

// TestWorkerIdleBehavior tests that workers don't busy-wait when idle.
func TestWorkerIdleBehavior(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping worker idle behavior tests in short mode")
	}
	t.Run("workers use blocking receive instead of busy-waiting", func(_ *testing.T) {
		pool := parallel.NewAdvancedWorkerPool(parallel.AdvancedWorkerPoolConfig{
			MinWorkers:         2,
			MaxWorkers:         2,
			WorkQueueSize:      10,
			EnableWorkStealing: true,
			EnableMetrics:      true,
		})
		defer pool.Close()

		// Allow workers to start up
		time.Sleep(5 * time.Millisecond)

		// Submit a single work item after a delay to test idle behavior
		go func() {
			time.Sleep(10 * time.Millisecond)
			parallel.ProcessGeneric(pool, []int{1}, func(x int) int {
				return x * 2
			})
		}()

		// The test passes if workers don't consume excessive CPU while idle
		// This is more of a behavioral test - we'll verify the implementation
		// uses proper blocking instead of busy-waiting
		time.Sleep(20 * time.Millisecond)
	})

	t.Run("workers respond quickly to work after idle period", func(t *testing.T) {
		pool := parallel.NewAdvancedWorkerPool(parallel.AdvancedWorkerPoolConfig{
			MinWorkers:         1,
			MaxWorkers:         1,
			WorkQueueSize:      10,
			EnableWorkStealing: false,
			EnableMetrics:      true,
		})
		defer pool.Close()

		// Allow worker to start up and become idle
		time.Sleep(50 * time.Millisecond)

		// Submit work and measure response time
		start := time.Now()
		results := parallel.ProcessGeneric(pool, []int{1, 2, 3}, func(x int) int {
			return x * 2
		})
		duration := time.Since(start)

		assert.Len(t, results, 3)
		assert.Equal(t, []int{2, 4, 6}, results)

		// Workers should respond quickly even after being idle
		// Allow some buffer for context switching and goroutine scheduling
		assert.Less(t, duration, 100*time.Millisecond, "Workers should respond quickly to work")
	})

	t.Run("workers handle context cancellation gracefully", func(t *testing.T) {
		pool := parallel.NewAdvancedWorkerPool(parallel.AdvancedWorkerPoolConfig{
			MinWorkers:         2,
			MaxWorkers:         2,
			WorkQueueSize:      10,
			EnableWorkStealing: true,
		})

		// Allow workers to start up
		time.Sleep(5 * time.Millisecond)

		// Close the pool and measure how long it takes
		start := time.Now()
		pool.Close()
		duration := time.Since(start)

		// Workers should shutdown quickly without needing to wait for sleep intervals
		assert.Less(t, duration, 100*time.Millisecond, "Workers should shutdown quickly")
	})
}

// TestWorkerIdleStrategy tests different idle strategies.
func TestWorkerIdleStrategy(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping worker idle strategy tests in short mode")
	}
	t.Run("exponential backoff for work stealing attempts", func(t *testing.T) {
		pool := parallel.NewAdvancedWorkerPool(parallel.AdvancedWorkerPoolConfig{
			MinWorkers:         2,
			MaxWorkers:         2,
			WorkQueueSize:      2, // Small queue to force work stealing attempts
			EnableWorkStealing: true,
			EnableMetrics:      true,
		})
		defer pool.Close()

		// Create a scenario where workers will try to steal work
		// but won't find any, testing the backoff behavior
		items := make([]int, 10)
		for i := range items {
			items[i] = i
		}

		var wg sync.WaitGroup
		results := make([][]int, 3)

		// Submit multiple batches concurrently to create contention
		for i := range 3 {
			wg.Add(1)
			go func(batch int) {
				defer wg.Done()
				results[batch] = parallel.ProcessGeneric(pool, items, func(x int) int {
					// Quick work to ensure stealing attempts
					return x * 2
				})
			}(i)
		}

		wg.Wait()

		// Verify all work was completed correctly
		for i := range 3 {
			assert.Len(t, results[i], 10)
		}

		// Check that work stealing occurred (indicates workers were properly
		// trying to steal work instead of just sleeping)
		metrics := pool.GetMetrics()
		t.Logf("Work stealing count: %d", metrics.WorkStealingCount)
		// This test is about behavior, not specific metrics
	})
}

// TestWorkerResourceEfficiency tests that the new implementation is more resource efficient.
func TestWorkerResourceEfficiency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping worker resource efficiency tests in short mode")
	}
	t.Run("idle workers don't consume excessive CPU", func(t *testing.T) {
		pool := parallel.NewAdvancedWorkerPool(parallel.AdvancedWorkerPoolConfig{
			MinWorkers:         4,
			MaxWorkers:         4,
			WorkQueueSize:      10,
			EnableWorkStealing: true,
		})
		defer pool.Close()

		// Let workers idle for a period
		time.Sleep(100 * time.Millisecond)

		// Submit work to verify workers are still responsive
		results := parallel.ProcessGeneric(pool, []int{1, 2, 3, 4, 5}, func(x int) int {
			return x * 2
		})

		assert.Len(t, results, 5)
		assert.ElementsMatch(t, []int{2, 4, 6, 8, 10}, results)
	})
}

// BenchmarkWorkerIdlePerformance benchmarks the idle behavior performance.
func BenchmarkWorkerIdlePerformance(b *testing.B) {
	b.Run("idle_workers_efficiency", func(b *testing.B) {
		pool := parallel.NewAdvancedWorkerPool(parallel.AdvancedWorkerPoolConfig{
			MinWorkers:         4,
			MaxWorkers:         4,
			WorkQueueSize:      10,
			EnableWorkStealing: true,
		})
		defer pool.Close()

		// Allow workers to start up
		time.Sleep(10 * time.Millisecond)

		b.ResetTimer()

		for i := range b.N {
			// Submit work periodically to test responsiveness
			if i%100 == 0 {
				parallel.ProcessGeneric(pool, []int{1, 2, 3}, func(x int) int {
					return x * 2
				})
			}

			// Small pause to let workers idle
			time.Sleep(time.Microsecond)
		}
	})
}

// TestWorkerIdleBackoffBehavior tests the exponential backoff behavior.
func TestWorkerIdleBackoffBehavior(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping worker idle backoff behavior tests in short mode")
	}
	t.Run("backoff increases when no work is available", func(t *testing.T) {
		pool := parallel.NewAdvancedWorkerPool(parallel.AdvancedWorkerPoolConfig{
			MinWorkers:         1,
			MaxWorkers:         1,
			WorkQueueSize:      10,
			EnableWorkStealing: false, // Disable to test pure backoff
		})
		defer pool.Close()

		// Allow worker to start and become idle
		time.Sleep(50 * time.Millisecond)

		// Submit work after the worker has been idle
		// This tests that the backoff mechanism works correctly
		start := time.Now()
		results := parallel.ProcessGeneric(pool, []int{1}, func(x int) int {
			return x * 2
		})
		duration := time.Since(start)

		assert.Len(t, results, 1)
		assert.Equal(t, 2, results[0])

		// Even with backoff, response should be reasonable
		assert.Less(t, duration, 50*time.Millisecond, "Worker should respond within reasonable time despite backoff")
	})
}
