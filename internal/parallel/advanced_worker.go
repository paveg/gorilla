package parallel

import (
	"container/heap"
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Constants for timeout management.
const (
	resultTimeoutSeconds = 5 // Timeout per work item result
)

// Constants for backoff management.
const (
	defaultMaxBackoffMs      = 10
	defaultBackoffMultiplier = 2
)

// AdvancedWorkerPool provides dynamic scaling, work stealing, and resource management.
type AdvancedWorkerPool struct {
	config        AdvancedWorkerPoolConfig
	ctx           context.Context
	cancel        context.CancelFunc
	workers       []*advancedWorker
	workQueue     chan workItem
	priorityQueue *PriorityQueue
	metrics       *PoolMetrics

	// Worker management
	currentWorkers int32
	minWorkers     int
	maxWorkers     int

	// Synchronization
	mu sync.RWMutex
	wg sync.WaitGroup

	// Scaling and monitoring
	lastScaleTime  time.Time
	scaleThreshold float64

	// Work stealing
	workStealingEnabled bool
	stealingQueues      []*workStealingQueue

	// Resource management
	memoryMonitor *MemoryMonitor
	cpuMonitor    *cpuMonitor

	closed bool
}

// AdvancedWorkerPoolConfig provides configuration for the advanced worker pool.
type AdvancedWorkerPoolConfig struct {
	MinWorkers         int
	MaxWorkers         int
	WorkQueueSize      int
	ScaleThreshold     float64
	EnableWorkStealing bool
	EnableMetrics      bool
	EnablePriority     bool
	MemoryMonitor      *MemoryMonitor
	ResourceLimits     ResourceLimits
	BackpressurePolicy BackpressurePolicy
}

// NewAdvancedWorkerPool creates a new advanced worker pool with the specified configuration.
func NewAdvancedWorkerPool(config AdvancedWorkerPoolConfig) *AdvancedWorkerPool {
	// Set defaults
	if config.MinWorkers <= 0 {
		config.MinWorkers = 1
	}
	if config.MaxWorkers <= 0 {
		config.MaxWorkers = runtime.NumCPU()
	}
	if config.WorkQueueSize <= 0 {
		config.WorkQueueSize = 100
	}
	if config.ScaleThreshold <= 0 {
		config.ScaleThreshold = 0.8
	}

	ctx, cancel := context.WithCancel(context.Background())

	pool := &AdvancedWorkerPool{
		config:              config,
		ctx:                 ctx,
		cancel:              cancel,
		workQueue:           make(chan workItem, config.WorkQueueSize),
		minWorkers:          config.MinWorkers,
		maxWorkers:          config.MaxWorkers,
		scaleThreshold:      config.ScaleThreshold,
		workStealingEnabled: config.EnableWorkStealing,
		memoryMonitor:       config.MemoryMonitor,
		lastScaleTime:       time.Now(),
	}

	// Initialize metrics if enabled
	if config.EnableMetrics {
		pool.metrics = &PoolMetrics{}
	}

	// Initialize priority queue if enabled
	if config.EnablePriority {
		pool.priorityQueue = NewPriorityQueue()
	}

	// Initialize work stealing queues if enabled
	if config.EnableWorkStealing {
		pool.stealingQueues = make([]*workStealingQueue, config.MaxWorkers)
		for i := range pool.stealingQueues {
			pool.stealingQueues[i] = newWorkStealingQueue()
		}
	}

	// Initialize CPU monitor if resource limits are specified
	if config.ResourceLimits.MaxCPUUsage > 0 {
		pool.cpuMonitor = newCPUMonitor(config.ResourceLimits.MaxCPUUsage)
	}

	// Start initial workers
	pool.scaleWorkers(config.MinWorkers)

	return pool
}

// ProcessGeneric executes work items using the advanced worker pool with generic types.
func ProcessGeneric[T, R any](pool *AdvancedWorkerPool, items []T, worker func(T) R) []R {
	if len(items) == 0 {
		return nil
	}

	// Convert to interface{} slice
	interfaceItems := make([]interface{}, len(items))
	for i, item := range items {
		interfaceItems[i] = item
	}

	// Wrap worker function
	interfaceWorker := func(item interface{}) interface{} {
		if typedItem, ok := item.(T); ok {
			return worker(typedItem)
		}
		var zero R
		return zero
	}

	// Process and convert results back
	interfaceResults := pool.Process(interfaceItems, interfaceWorker)
	results := make([]R, len(interfaceResults))
	for i, result := range interfaceResults {
		if typedResult, ok := result.(R); ok {
			results[i] = typedResult
		}
		// If type assertion fails, results[i] remains zero value
	}

	return results
}

// Process executes work items using the advanced worker pool.
func (pool *AdvancedWorkerPool) Process(items []interface{}, worker func(interface{}) interface{}) []interface{} {
	if len(items) == 0 {
		return nil
	}

	pool.mu.RLock()
	if pool.closed {
		pool.mu.RUnlock()
		return nil
	}
	pool.mu.RUnlock()

	// Check if we need to scale
	pool.checkAndScale(len(items))

	// Create result channel
	results := make(chan advancedIndexedResult, len(items))

	// Submit work items - distribute to worker queues if work stealing is enabled
	for i, item := range items {
		wi := workItem{
			index:  i,
			data:   item,
			worker: worker,
			result: results,
		}

		if !pool.distributeWorkItem(wi, i) {
			close(results)
			return nil
		}
	}

	// Collect results
	resultSlice := make([]interface{}, len(items))

	for range items {
		// Create timeout for each individual result to prevent hanging
		timeout := time.NewTimer(resultTimeoutSeconds * time.Second) // Shorter timeout per item
		select {
		case result := <-results:
			timeout.Stop()
			resultSlice[result.index] = result.result
		case <-pool.ctx.Done():
			timeout.Stop()
			return nil
		case <-timeout.C:
			// Timeout reached - return nil to prevent hanging
			return nil
		}
	}

	return resultSlice
}

// ProcessWithPriority executes priority tasks.
func (pool *AdvancedWorkerPool) ProcessWithPriority(tasks []PriorityTask, worker func(PriorityTask) int) []int {
	if len(tasks) == 0 {
		return nil
	}

	pool.mu.RLock()
	if pool.closed {
		pool.mu.RUnlock()
		return nil
	}
	pool.mu.RUnlock()

	// Submit to priority queue
	results := make(chan advancedIndexedResult, len(tasks))

	for i, task := range tasks {
		priorityItem := &PriorityItem{
			Priority: task.Priority,
			Index:    i,
			Task:     task,
			Worker:   worker,
			Result:   results,
		}
		heap.Push(pool.priorityQueue, priorityItem)
	}

	// Process priority queue in order
	for pool.priorityQueue.Len() > 0 {
		poppedItem := heap.Pop(pool.priorityQueue)
		item, ok := poppedItem.(*PriorityItem)
		if !ok {
			continue // Skip invalid items
		}

		wi := workItem{
			index: item.Index,
			data:  item.Task,
			worker: func(data interface{}) interface{} {
				if task, taskOk := data.(PriorityTask); taskOk {
					return item.Worker(task)
				}
				return nil
			},
			result: item.Result,
		}

		if !pool.distributeWorkItem(wi, item.Index) {
			return nil
		}
	}

	// Collect results
	resultSlice := make([]int, len(tasks))

	for range tasks {
		// Create timeout for each individual result to prevent hanging
		timeout := time.NewTimer(resultTimeoutSeconds * time.Second) // Shorter timeout per item
		select {
		case result := <-results:
			timeout.Stop()
			if intResult, ok := result.result.(int); ok {
				resultSlice[result.index] = intResult
			}
			// If type assertion fails, resultSlice[result.index] remains 0
		case <-pool.ctx.Done():
			timeout.Stop()
			return nil
		case <-timeout.C:
			// Timeout reached - return nil to prevent hanging
			return nil
		}
	}

	return resultSlice
}

// distributeWorkItem distributes a work item to worker queues or global queue.
func (pool *AdvancedWorkerPool) distributeWorkItem(item workItem, index int) bool {
	// Try to distribute work to worker queues first if work stealing is enabled
	distributed := false
	if pool.workStealingEnabled && len(pool.stealingQueues) > 0 {
		// Use round-robin distribution to worker queues
		targetWorker := index % len(pool.stealingQueues)
		if targetWorker < len(pool.stealingQueues) && pool.stealingQueues[targetWorker] != nil {
			pool.stealingQueues[targetWorker].pushLocal(item)
			distributed = true
		}
	}

	// If not distributed to worker queue, use global queue
	if !distributed {
		select {
		case pool.workQueue <- item:
		case <-pool.ctx.Done():
			return false
		default:
			// Handle backpressure
			if pool.config.BackpressurePolicy == BackpressureBlock {
				pool.workQueue <- item
			} else {
				// Drop or spill to disk (simplified for now)
				return true // Continue processing other items
			}
		}
	}
	return true
}

// checkAndScale checks if the worker pool needs to scale up or down.
func (pool *AdvancedWorkerPool) checkAndScale(_ int) {
	now := time.Now()
	if now.Sub(pool.lastScaleTime) < time.Second {
		return // Don't scale too frequently
	}

	current := int(atomic.LoadInt32(&pool.currentWorkers))
	queueUtilization := float64(len(pool.workQueue)) / float64(cap(pool.workQueue))

	// Check memory pressure
	if pool.memoryMonitor != nil {
		recommendedWorkers := pool.memoryMonitor.AdjustParallelism()
		if recommendedWorkers < current {
			pool.scaleWorkers(recommendedWorkers)
			return
		}
	}

	// Check CPU pressure
	if pool.cpuMonitor != nil {
		recommendedWorkers := pool.cpuMonitor.recommendedWorkers()
		if recommendedWorkers < current {
			pool.scaleWorkers(recommendedWorkers)
			return
		}
	}

	// Scale up if queue utilization is high and we have room
	if queueUtilization > pool.scaleThreshold && current < pool.maxWorkers {
		newWorkerCount := minInt(current+1, pool.maxWorkers)
		pool.scaleWorkers(newWorkerCount)
	}

	// Scale down if queue utilization is low
	if queueUtilization < 0.2 && current > pool.minWorkers {
		newWorkerCount := maxInt(current-1, pool.minWorkers)
		pool.scaleWorkers(newWorkerCount)
	}

	pool.lastScaleTime = now
}

// scaleWorkers adjusts the number of workers to the target count.
func (pool *AdvancedWorkerPool) scaleWorkers(targetCount int) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	current := int(atomic.LoadInt32(&pool.currentWorkers))

	if targetCount > current {
		// Scale up
		for i := current; i < targetCount; i++ {
			worker := pool.createWorker(i)
			pool.workers = append(pool.workers, worker)
			pool.wg.Add(1)
			go worker.run()
		}
	} else if targetCount < current {
		// Scale down
		for i := current - 1; i >= targetCount; i-- {
			if i < len(pool.workers) {
				pool.workers[i].stop()
				pool.workers = pool.workers[:i]
			}
		}
	}

	atomic.StoreInt32(&pool.currentWorkers, int32(targetCount)) //nolint:gosec // targetCount is bounded by config

	// Update metrics
	if pool.metrics != nil {
		currentMax := atomic.LoadInt32(&pool.metrics.MaxWorkerCount)
		//nolint:gosec // targetCount is bounded by maxWorkers config
		if int32(targetCount) > currentMax {
			atomic.StoreInt32(&pool.metrics.MaxWorkerCount, int32(targetCount))
		}
	}
}

// createWorker creates a new worker with the specified ID.
func (pool *AdvancedWorkerPool) createWorker(id int) *advancedWorker {
	worker := &advancedWorker{
		id:   id,
		pool: pool,
		ctx:  pool.ctx,
	}

	// Assign work stealing queue if enabled
	if pool.workStealingEnabled && id < len(pool.stealingQueues) {
		worker.stealingQueue = pool.stealingQueues[id]
	}

	return worker
}

// processPriorityQueue is no longer needed - integrated into ProcessWithPriority

// CurrentWorkerCount returns the current number of workers.
func (pool *AdvancedWorkerPool) CurrentWorkerCount() int {
	return int(atomic.LoadInt32(&pool.currentWorkers))
}

// GetMetrics returns the current pool metrics.
func (pool *AdvancedWorkerPool) GetMetrics() *PoolMetrics {
	if pool.metrics == nil {
		return nil
	}

	// Return a copy to avoid data races
	return &PoolMetrics{
		TotalTasksProcessed:  atomic.LoadInt64(&pool.metrics.TotalTasksProcessed),
		AverageTaskDuration:  pool.metrics.AverageTaskDuration,
		MaxWorkerCount:       atomic.LoadInt32(&pool.metrics.MaxWorkerCount),
		TotalProcessingTime:  pool.metrics.TotalProcessingTime,
		WorkStealingCount:    atomic.LoadInt64(&pool.metrics.WorkStealingCount),
		MemoryPressureEvents: atomic.LoadInt64(&pool.metrics.MemoryPressureEvents),
	}
}

// Close gracefully shuts down the worker pool.
func (pool *AdvancedWorkerPool) Close() {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return
	}
	pool.closed = true
	pool.mu.Unlock()

	// Cancel context to signal workers to stop
	pool.cancel()

	// Close work queue
	close(pool.workQueue)

	// Wait for all workers to finish
	pool.wg.Wait()

	// Close work stealing queues
	if pool.workStealingEnabled {
		for _, queue := range pool.stealingQueues {
			queue.close()
		}
	}
}

// Helper functions.
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// maxInt is already defined in memory_safe.go

// Internal types.
type workItem struct {
	index  int
	data   interface{}
	worker func(interface{}) interface{}
	result chan<- advancedIndexedResult
}

type advancedIndexedResult struct {
	index  int
	result interface{}
}

type advancedWorker struct {
	id            int
	pool          *AdvancedWorkerPool
	ctx           context.Context
	stealingQueue *workStealingQueue
	stopped       bool
	mu            sync.Mutex
}

func (w *advancedWorker) run() {
	defer w.pool.wg.Done()

	backoffManager := w.createBackoffManager()

	for {
		workItem := w.findWork(backoffManager)
		if workItem == nil {
			// Context was cancelled
			return
		}

		w.processWork(*workItem)
		backoffManager.reset()
	}
}

// backoffManager manages exponential backoff for idle workers.
type backoffManager struct {
	current           time.Duration
	maxBackoff        time.Duration
	backoffMultiplier int
}

// createBackoffManager creates a new backoff manager with default settings.
func (w *advancedWorker) createBackoffManager() *backoffManager {
	return &backoffManager{
		current:           time.Millisecond,
		maxBackoff:        defaultMaxBackoffMs * time.Millisecond,
		backoffMultiplier: defaultBackoffMultiplier,
	}
}

// reset resets the backoff to initial value.
func (bm *backoffManager) reset() {
	bm.current = time.Millisecond
}

// increase increases the backoff duration exponentially.
func (bm *backoffManager) increase() {
	bm.current = minDuration(bm.current*time.Duration(bm.backoffMultiplier), bm.maxBackoff)
}

// findWork attempts to find work using various strategies with backoff.
func (w *advancedWorker) findWork(backoffManager *backoffManager) *workItem {
	// Try local queue first
	if workItem := w.tryLocalQueue(); workItem != nil {
		return workItem
	}

	// Try global queue with timeout and stealing
	return w.tryGlobalQueueWithStealing(backoffManager)
}

// tryLocalQueue attempts to get work from local stealing queue.
func (w *advancedWorker) tryLocalQueue() *workItem {
	if w.pool.workStealingEnabled && w.stealingQueue != nil {
		return w.stealingQueue.popLocal()
	}
	return nil
}

// tryGlobalQueueWithStealing tries global queue with timeout and stealing fallback.
func (w *advancedWorker) tryGlobalQueueWithStealing(backoffManager *backoffManager) *workItem {
	select {
	case <-w.ctx.Done():
		return nil
	case item, ok := <-w.pool.workQueue:
		if !ok {
			return nil
		}
		return &item
	case <-time.After(backoffManager.current):
		return w.handleTimeout(backoffManager)
	}
}

// handleTimeout handles timeout by trying work stealing and adjusting backoff.
func (w *advancedWorker) handleTimeout(backoffManager *backoffManager) *workItem {
	var workItem *workItem

	if w.pool.workStealingEnabled {
		workItem = w.stealWork()
	}

	if workItem == nil {
		backoffManager.increase()
	}

	return workItem
}

func (w *advancedWorker) processWork(item workItem) {
	result := item.worker(item.data)

	// Update metrics
	if w.pool.metrics != nil {
		atomic.AddInt64(&w.pool.metrics.TotalTasksProcessed, 1)
		// For now, we'll skip the average calculation to avoid race conditions
		// This would need proper synchronization or atomic operations for thread safety
	}

	// Send result
	select {
	case item.result <- advancedIndexedResult{index: item.index, result: result}:
	case <-w.ctx.Done():
	}
}

func (w *advancedWorker) stealWork() *workItem {
	// Try to steal work from other workers
	for i, queue := range w.pool.stealingQueues {
		if i != w.id {
			if work := queue.steal(); work != nil {
				if w.pool.metrics != nil {
					atomic.AddInt64(&w.pool.metrics.WorkStealingCount, 1)
				}
				return work
			}
		}
	}
	return nil
}

func (w *advancedWorker) stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stopped = true
}

// Work stealing queue implementation.
type workStealingQueue struct {
	items  []workItem
	mu     sync.Mutex
	closed bool
}

func newWorkStealingQueue() *workStealingQueue {
	return &workStealingQueue{
		items: make([]workItem, 0),
	}
}

// pushLocal adds a work item to the local end of the queue (LIFO for owner).
func (q *workStealingQueue) pushLocal(item workItem) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return
	}

	q.items = append(q.items, item)
}

// popLocal removes a work item from the local end of the queue (LIFO for owner).
func (q *workStealingQueue) popLocal() *workItem {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed || len(q.items) == 0 {
		return nil
	}

	// Take from the end (LIFO for better cache locality)
	item := q.items[len(q.items)-1]
	q.items = q.items[:len(q.items)-1]
	return &item
}

// steal removes a work item from the remote end of the queue (FIFO for thieves).
func (q *workStealingQueue) steal() *workItem {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed || len(q.items) == 0 {
		return nil
	}

	// Take from the beginning (FIFO for thieves - reduces contention)
	item := q.items[0]
	q.items = q.items[1:]
	return &item
}

func (q *workStealingQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
}

// CPU monitor for resource limits.
type cpuMonitor struct {
	maxUsage float64
}

func newCPUMonitor(maxUsage float64) *cpuMonitor {
	return &cpuMonitor{
		maxUsage: maxUsage,
	}
}

func (c *cpuMonitor) recommendedWorkers() int {
	// Simplified CPU monitoring - would need actual CPU usage tracking
	return int(float64(runtime.NumCPU()) * c.maxUsage)
}

// PriorityQueue implements a priority queue using a binary heap.
//
//nolint:recvcheck // Required by heap.Interface: Len/Less/Swap need value receivers, Push/Pop need pointer receivers
type PriorityQueue []*PriorityItem

type PriorityItem struct {
	Priority int
	Index    int
	Task     PriorityTask
	Worker   func(PriorityTask) int
	Result   chan<- advancedIndexedResult
}

func NewPriorityQueue() *PriorityQueue {
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)
	return &pq
}

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// Higher priority values come first
	return pq[i].Priority > pq[j].Priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	if item, ok := x.(*PriorityItem); ok {
		*pq = append(*pq, item)
	}
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

// ResourceLimits defines constraints for worker pool resource usage.
type ResourceLimits struct {
	MaxCPUUsage    float64
	MaxMemoryUsage int64
}

type BackpressurePolicy int

const (
	BackpressureBlock BackpressurePolicy = iota
	BackpressureDrop
	BackpressureSpill
)

// PriorityTask represents a task with an associated priority level.
type PriorityTask struct {
	Priority int
	Value    int
}

type PoolMetrics struct {
	TotalTasksProcessed  int64
	AverageTaskDuration  time.Duration
	MaxWorkerCount       int32
	TotalProcessingTime  time.Duration
	WorkStealingCount    int64
	MemoryPressureEvents int64
}
