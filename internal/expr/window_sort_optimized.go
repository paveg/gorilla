package expr

import (
	"sort"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Configuration constants for optimization thresholds.
const (
	// parallelSortThreshold is the minimum partition size to use parallel sorting.
	parallelSortThreshold = 10000

	// maxWorkers limits the number of parallel sort workers.
	maxWorkers = 4

	// smallPartitionThreshold is the threshold below which to use original sorting.
	smallPartitionThreshold = 100

	// minWorkersForParallel is the minimum number of workers needed for parallel sorting.
	minWorkersForParallel = 2
)

// SortKey represents pre-computed sort keys for a row.
type SortKey struct {
	index int
	keys  []interface{}
}

// Comparator interface for type-specific comparison.
type Comparator interface {
	Compare(arr arrow.Array, i, j int) int
	CompareValues(v1, v2 interface{}) int
}

// compareNullsSortOptimized handles null comparison for any comparator.
func compareNullsSortOptimized(arr arrow.Array, i, j int) int {
	if arr.IsNull(i) && arr.IsNull(j) {
		return 0
	}
	if arr.IsNull(i) {
		return -1
	}
	if arr.IsNull(j) {
		return 1
	}
	return 0
}

// compareValuesSortOptimized compares two values with ascending/descending order.
func compareValuesSortOptimized[T interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64 | ~string
}](v1, v2 T, ascending bool) int {
	var result int
	switch {
	case v1 < v2:
		result = -1
	case v1 > v2:
		result = 1
	default:
		result = 0
	}
	if ascending {
		return result
	}
	return -result
}

// compareInterfaceValuesSortOptimized compares interface values with null handling.
func compareInterfaceValuesSortOptimized[T interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64 | ~string
}](v1, v2 interface{}, ascending bool) int {
	if v1 == nil && v2 == nil {
		return 0
	}
	if v1 == nil {
		return -1
	}
	if v2 == nil {
		return 1
	}
	return compareValuesSortOptimized(v1.(T), v2.(T), ascending)
}

// Int64Comparator provides optimized int64 comparison.
type Int64Comparator struct {
	ascending bool
}

func (c *Int64Comparator) Compare(arr arrow.Array, i, j int) int {
	if nullCmp := compareNullsSortOptimized(arr, i, j); nullCmp != 0 {
		return nullCmp
	}

	a := arr.(*array.Int64)
	return compareValuesSortOptimized(a.Value(i), a.Value(j), c.ascending)
}

func (c *Int64Comparator) CompareValues(v1, v2 interface{}) int {
	return compareInterfaceValuesSortOptimized[int64](v1, v2, c.ascending)
}

// Float64Comparator provides optimized float64 comparison.
type Float64Comparator struct {
	ascending bool
}

func (c *Float64Comparator) Compare(arr arrow.Array, i, j int) int {
	if nullCmp := compareNullsSortOptimized(arr, i, j); nullCmp != 0 {
		return nullCmp
	}

	a := arr.(*array.Float64)
	return compareValuesSortOptimized(a.Value(i), a.Value(j), c.ascending)
}

func (c *Float64Comparator) CompareValues(v1, v2 interface{}) int {
	return compareInterfaceValuesSortOptimized[float64](v1, v2, c.ascending)
}

// StringComparator provides optimized string comparison.
type StringComparator struct {
	ascending bool
}

func (c *StringComparator) Compare(arr arrow.Array, i, j int) int {
	if nullCmp := compareNullsSortOptimized(arr, i, j); nullCmp != 0 {
		return nullCmp
	}

	a := arr.(*array.String)
	return compareValuesSortOptimized(a.Value(i), a.Value(j), c.ascending)
}

func (c *StringComparator) CompareValues(v1, v2 interface{}) int {
	return compareInterfaceValuesSortOptimized[string](v1, v2, c.ascending)
}

// BooleanComparator provides optimized boolean comparison.
type BooleanComparator struct {
	ascending bool
}

func (c *BooleanComparator) Compare(arr arrow.Array, i, j int) int {
	if nullCmp := compareNullsSortOptimized(arr, i, j); nullCmp != 0 {
		return nullCmp
	}

	a := arr.(*array.Boolean)
	v1, v2 := a.Value(i), a.Value(j)

	if !v1 && v2 { // false < true
		if c.ascending {
			return -1
		}
		return 1
	}
	if v1 && !v2 { // true > false
		if c.ascending {
			return 1
		}
		return -1
	}
	return 0
}

func (c *BooleanComparator) CompareValues(v1, v2 interface{}) int {
	if v1 == nil && v2 == nil {
		return 0
	}
	if v1 == nil {
		return -1
	}
	if v2 == nil {
		return 1
	}

	val1, val2 := v1.(bool), v2.(bool)
	if !val1 && val2 { // false < true
		if c.ascending {
			return -1
		}
		return 1
	}
	if val1 && !val2 { // true > false
		if c.ascending {
			return 1
		}
		return -1
	}
	return 0
}

// createComparator creates a type-specific comparator for an arrow array.
func createComparator(arr arrow.Array, ascending bool) Comparator {
	switch arr.(type) {
	case *array.Int64:
		return &Int64Comparator{ascending: ascending}
	case *array.Float64:
		return &Float64Comparator{ascending: ascending}
	case *array.String:
		return &StringComparator{ascending: ascending}
	case *array.Boolean:
		return &BooleanComparator{ascending: ascending}
	default:
		return nil
	}
}

// sortPartitionOptimized is an optimized version of sortPartition.
func (e *Evaluator) sortPartitionOptimized(
	partition []int,
	orderBy []OrderByExpr,
	columns map[string]arrow.Array,
) []int {
	if len(orderBy) == 0 || len(partition) == 0 {
		return partition
	}

	// For small partitions, use the original implementation
	if len(partition) < smallPartitionThreshold {
		return e.sortPartition(partition, orderBy, columns)
	}

	// For large partitions, use parallel sorting if beneficial
	if len(partition) >= parallelSortThreshold {
		return e.sortPartitionParallelOptimized(partition, orderBy, columns)
	}

	// Use optimized sorting with pre-computed keys
	return e.sortPartitionWithKeys(partition, orderBy, columns)
}

// sortPartitionWithKeys sorts using pre-computed sort keys.
func (e *Evaluator) sortPartitionWithKeys(
	partition []int,
	orderBy []OrderByExpr,
	columns map[string]arrow.Array,
) []int {
	// Pre-compute sort keys
	sortKeys := make([]SortKey, len(partition))
	comparators := make([]Comparator, len(orderBy))

	// Create type-specific comparators
	for i, order := range orderBy {
		arr := columns[order.column]
		comparators[i] = createComparator(arr, order.ascending)
	}

	// Extract sort keys
	for i, idx := range partition {
		keys := make([]interface{}, len(orderBy))
		for j, order := range orderBy {
			arr := columns[order.column]
			if arr.IsNull(idx) {
				keys[j] = nil
			} else {
				keys[j] = e.getArrayValue(arr, idx)
			}
		}
		sortKeys[i] = SortKey{index: idx, keys: keys}
	}

	// Sort using pre-computed keys
	sort.Slice(sortKeys, func(i, j int) bool {
		for k, comp := range comparators {
			if comp != nil {
				cmp := comp.CompareValues(sortKeys[i].keys[k], sortKeys[j].keys[k])
				if cmp != 0 {
					return cmp < 0
				}
			}
		}
		return false
	})

	// Extract sorted indices
	result := make([]int, len(partition))
	for i, sk := range sortKeys {
		result[i] = sk.index
	}

	return result
}

// sortPartitionParallelOptimized performs parallel sorting for large partitions.
func (e *Evaluator) sortPartitionParallelOptimized(
	partition []int,
	orderBy []OrderByExpr,
	columns map[string]arrow.Array,
) []int {
	n := len(partition)
	if n < parallelSortThreshold*2 {
		// Not worth parallelizing for smaller sizes
		return e.sortPartitionWithKeys(partition, orderBy, columns)
	}

	// Determine number of workers
	numWorkers := maxWorkers
	chunkSize := n / numWorkers
	if chunkSize < parallelSortThreshold {
		numWorkers = n / parallelSortThreshold
		if numWorkers < minWorkersForParallel {
			return e.sortPartitionWithKeys(partition, orderBy, columns)
		}
		chunkSize = n / numWorkers
	}

	// Split partition into chunks
	chunks := make([][]int, numWorkers)
	for i := range numWorkers {
		start := i * chunkSize
		end := start + chunkSize
		if i == numWorkers-1 {
			end = n
		}
		chunks[i] = partition[start:end]
	}

	// Sort chunks in parallel
	var wg sync.WaitGroup
	sortedChunks := make([][]int, numWorkers)

	for i := range numWorkers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sortedChunks[idx] = e.sortPartitionWithKeys(chunks[idx], orderBy, columns)
		}(i)
	}

	wg.Wait()

	// Merge sorted chunks
	return e.mergeSortedChunks(sortedChunks, orderBy, columns)
}

// mergeSortedChunks merges multiple sorted chunks into a single sorted result.
func (e *Evaluator) mergeSortedChunks(
	chunks [][]int,
	orderBy []OrderByExpr,
	columns map[string]arrow.Array,
) []int {
	// Calculate total size
	totalSize := 0
	for _, chunk := range chunks {
		totalSize += len(chunk)
	}

	result := make([]int, 0, totalSize)
	indices := make([]int, len(chunks)) // Current index in each chunk

	// Create comparators
	comparators := make([]Comparator, len(orderBy))
	for i, order := range orderBy {
		arr := columns[order.column]
		comparators[i] = createComparator(arr, order.ascending)
	}

	// Merge using a min-heap approach
	for len(result) < totalSize {
		minChunk := -1

		// Find the chunk with the minimum next element
		for i, chunk := range chunks {
			if indices[i] >= len(chunk) {
				continue // This chunk is exhausted
			}

			if minChunk == -1 {
				minChunk = i
				continue
			}

			// Compare current minimum with this chunk's next element
			minIdx := chunks[minChunk][indices[minChunk]]
			curIdx := chunk[indices[i]]

			shouldSwap := false
			for j, order := range orderBy {
				arr := columns[order.column]
				comp := comparators[j]
				if comp != nil {
					cmp := comp.Compare(arr, minIdx, curIdx)
					if cmp > 0 {
						shouldSwap = true
						break
					} else if cmp < 0 {
						break
					}
				}
			}

			if shouldSwap {
				minChunk = i
			}
		}

		// Add the minimum element to result
		if minChunk != -1 {
			result = append(result, chunks[minChunk][indices[minChunk]])
			indices[minChunk]++
		}
	}

	return result
}
