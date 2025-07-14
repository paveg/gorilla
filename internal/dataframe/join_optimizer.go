package dataframe

import (
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/cespare/xxhash/v2"
	"github.com/paveg/gorilla/internal/parallel"
)

// Constants for join optimization thresholds
const (
	broadcastThreshold     = 1000  // rows for broadcast join
	mergeJoinThreshold     = 10000 // rows where merge join becomes beneficial
	optimizedHashThreshold = 5000  // rows for optimized hash map
	defaultWorkerCount     = 4     // default number of workers for parallel processing
	hashMapLoadFactor      = 0.75  // load factor for optimized hash map
	hashMapGrowthFactor    = 2     // growth factor for hash map resize
	hashMapCapacityFactor  = 1.3   // capacity factor for initial hash map size
)

// JoinStrategy represents different join algorithms
type JoinStrategy int

const (
	HashJoinStrategy JoinStrategy = iota
	BroadcastJoinStrategy
	MergeJoinStrategy
	OptimizedHashJoinStrategy
)

// TableStats holds statistics about a DataFrame for optimization decisions
type TableStats struct {
	RowCount      int
	SortedColumns map[string]bool
	Cardinality   map[string]int // distinct values per column
}

// JoinOptimizer selects optimal join strategy based on table statistics
type JoinOptimizer struct {
	leftStats  TableStats
	rightStats TableStats
}

// NewJoinOptimizer creates a new join optimizer with table statistics
func NewJoinOptimizer(left, right *DataFrame) *JoinOptimizer {
	return &JoinOptimizer{
		leftStats:  computeTableStats(left),
		rightStats: computeTableStats(right),
	}
}

// SelectStrategy chooses the optimal join strategy based on table characteristics
func (jo *JoinOptimizer) SelectStrategy(leftKeys, rightKeys []string) JoinStrategy {
	// Small right table: use broadcast join
	if jo.rightStats.RowCount < broadcastThreshold {
		return BroadcastJoinStrategy
	}

	// Both tables sorted on join keys: use merge join
	if len(leftKeys) == 1 && len(rightKeys) == 1 {
		leftSorted := jo.leftStats.SortedColumns[leftKeys[0]]
		rightSorted := jo.rightStats.SortedColumns[rightKeys[0]]
		if leftSorted && rightSorted {
			return MergeJoinStrategy
		}
	}

	// Large tables: use optimized hash join
	if jo.leftStats.RowCount > optimizedHashThreshold || jo.rightStats.RowCount > optimizedHashThreshold {
		return OptimizedHashJoinStrategy
	}

	// Default to standard hash join
	return HashJoinStrategy
}

// computeTableStats analyzes DataFrame characteristics for optimization
func computeTableStats(df *DataFrame) TableStats {
	stats := TableStats{
		RowCount:      df.Len(),
		SortedColumns: make(map[string]bool),
		Cardinality:   make(map[string]int),
	}

	// Check if columns are sorted
	for _, colName := range df.Columns() {
		col, _ := df.Column(colName)
		stats.SortedColumns[colName] = isColumnSorted(col)
		stats.Cardinality[colName] = estimateCardinality(col)
	}

	return stats
}

// isColumnSorted checks if a column is sorted in ascending order
func isColumnSorted(col ISeries) bool {
	if col.Len() <= 1 {
		return true
	}

	for i := 1; i < col.Len(); i++ {
		if compareValues(col, i-1, i) > 0 {
			return false
		}
	}
	return true
}

// compareValues compares two values in a series
func compareValues(col ISeries, i, j int) int {
	// For numeric comparisons, convert to proper types
	arr := col.Array()
	defer arr.Release()

	switch typedArr := arr.(type) {
	case *array.Int64:
		val1 := typedArr.Value(i)
		val2 := typedArr.Value(j)
		if val1 < val2 {
			return -1
		} else if val1 > val2 {
			return 1
		}
		return 0
	case *array.Int32:
		val1 := typedArr.Value(i)
		val2 := typedArr.Value(j)
		if val1 < val2 {
			return -1
		} else if val1 > val2 {
			return 1
		}
		return 0
	case *array.Float64:
		val1 := typedArr.Value(i)
		val2 := typedArr.Value(j)
		if val1 < val2 {
			return -1
		} else if val1 > val2 {
			return 1
		}
		return 0
	case *array.Float32:
		val1 := typedArr.Value(i)
		val2 := typedArr.Value(j)
		if val1 < val2 {
			return -1
		} else if val1 > val2 {
			return 1
		}
		return 0
	case *array.String:
		val1 := typedArr.Value(i)
		val2 := typedArr.Value(j)
		if val1 < val2 {
			return -1
		} else if val1 > val2 {
			return 1
		}
		return 0
	default:
		// Fallback to string comparison
		val1 := getStringValue(col, i)
		val2 := getStringValue(col, j)
		if val1 < val2 {
			return -1
		} else if val1 > val2 {
			return 1
		}
		return 0
	}
}

// estimateCardinality estimates distinct values in a column
func estimateCardinality(col ISeries) int {
	seen := make(map[string]bool)
	for i := 0; i < col.Len(); i++ {
		val := getStringValue(col, i)
		seen[val] = true
	}
	return len(seen)
}

// OptimizedJoin performs join using the selected optimal strategy
func (df *DataFrame) OptimizedJoin(right *DataFrame, options *JoinOptions) (*DataFrame, error) {
	leftKeys, rightKeys := normalizeJoinKeys(options)

	if err := validateJoinKeys(df, right, leftKeys, rightKeys); err != nil {
		return nil, err
	}

	optimizer := NewJoinOptimizer(df, right)
	strategy := optimizer.SelectStrategy(leftKeys, rightKeys)

	switch strategy {
	case BroadcastJoinStrategy:
		return df.broadcastJoin(right, leftKeys, rightKeys, options.Type)
	case MergeJoinStrategy:
		return df.mergeJoin(right, leftKeys, rightKeys, options.Type)
	case OptimizedHashJoinStrategy:
		return df.optimizedHashJoin(right, leftKeys, rightKeys, options.Type)
	case HashJoinStrategy:
		return df.Join(right, options) // fallback to standard join
	default:
		return df.Join(right, options) // fallback to standard join
	}
}

// broadcastJoin implements broadcast join for small right tables
func (df *DataFrame) broadcastJoin(
	right *DataFrame, leftKeys, rightKeys []string, joinType JoinType,
) (*DataFrame, error) {
	mem := memory.NewGoAllocator()

	// Build complete hash map from small right table (broadcast)
	rightHashMap := make(map[string][]int)
	for i := 0; i < right.Len(); i++ {
		key := buildJoinKey(right, rightKeys, i)
		rightHashMap[key] = append(rightHashMap[key], i)
	}

	// Process left table in parallel chunks
	const chunkSize = 1000
	numChunks := (df.Len() + chunkSize - 1) / chunkSize

	type chunkResult struct {
		leftIndices  []int
		rightIndices []int
	}

	results := make([]chunkResult, numChunks)
	var wg sync.WaitGroup

	for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			start := idx * chunkSize
			end := start + chunkSize
			if end > df.Len() {
				end = df.Len()
			}

			var leftIndices, rightIndices []int

			for i := start; i < end; i++ {
				key := buildJoinKey(df, leftKeys, i)
				if rightRows, exists := rightHashMap[key]; exists {
					for _, rightIdx := range rightRows {
						leftIndices = append(leftIndices, i)
						rightIndices = append(rightIndices, rightIdx)
					}
				} else if joinType == LeftJoin || joinType == FullOuterJoin {
					leftIndices = append(leftIndices, i)
					rightIndices = append(rightIndices, -1)
				}
			}

			results[idx] = chunkResult{
				leftIndices:  leftIndices,
				rightIndices: rightIndices,
			}
		}(chunkIdx)
	}

	wg.Wait()

	// Combine results
	var leftIndices, rightIndices []int
	for _, result := range results {
		leftIndices = append(leftIndices, result.leftIndices...)
		rightIndices = append(rightIndices, result.rightIndices...)
	}

	// Handle right/full outer join unmatched rows
	if joinType == RightJoin || joinType == FullOuterJoin {
		matched := make(map[int]bool)
		for _, idx := range rightIndices {
			if idx >= 0 {
				matched[idx] = true
			}
		}

		for i := 0; i < right.Len(); i++ {
			if !matched[i] {
				leftIndices = append(leftIndices, -1)
				rightIndices = append(rightIndices, i)
			}
		}
	}

	return df.buildJoinResult(right, leftIndices, rightIndices, mem)
}

// mergeJoin implements merge join for sorted data
func (df *DataFrame) mergeJoin(
	right *DataFrame, leftKeys, rightKeys []string, joinType JoinType,
) (*DataFrame, error) {
	if len(leftKeys) != 1 || len(rightKeys) != 1 {
		return nil, fmt.Errorf("merge join currently supports only single-column joins")
	}

	mem := memory.NewGoAllocator()

	// Ensure both DataFrames are sorted
	leftSorted := df
	rightSorted := right

	optimizer := NewJoinOptimizer(df, right)
	if !optimizer.leftStats.SortedColumns[leftKeys[0]] {
		var err error
		leftSorted, err = df.Sort(leftKeys[0], true)
		if err != nil {
			return nil, fmt.Errorf("failed to sort left DataFrame: %w", err)
		}
		defer leftSorted.Release()
	}
	if !optimizer.rightStats.SortedColumns[rightKeys[0]] {
		var err error
		rightSorted, err = right.Sort(rightKeys[0], true)
		if err != nil {
			return nil, fmt.Errorf("failed to sort right DataFrame: %w", err)
		}
		defer rightSorted.Release()
	}

	// Perform merge join
	var leftIndices, rightIndices []int
	leftIdx, rightIdx := 0, 0

	for leftIdx < leftSorted.Len() && rightIdx < rightSorted.Len() {
		leftKey := buildJoinKey(leftSorted, leftKeys, leftIdx)
		rightKey := buildJoinKey(rightSorted, rightKeys, rightIdx)

		cmp := compareKeys(leftKey, rightKey)

		switch {
		case cmp == 0:
			// Keys match - find all matching rows
			leftStart := leftIdx
			rightStart := rightIdx

			// Find end of matching left rows
			for leftIdx < leftSorted.Len() && buildJoinKey(leftSorted, leftKeys, leftIdx) == leftKey {
				leftIdx++
			}

			// Find end of matching right rows
			for rightIdx < rightSorted.Len() && buildJoinKey(rightSorted, rightKeys, rightIdx) == rightKey {
				rightIdx++
			}

			// Create cartesian product of matching rows
			for i := leftStart; i < leftIdx; i++ {
				for j := rightStart; j < rightIdx; j++ {
					leftIndices = append(leftIndices, i)
					rightIndices = append(rightIndices, j)
				}
			}
		case cmp < 0:
			// Left key is smaller
			if joinType == LeftJoin || joinType == FullOuterJoin {
				leftIndices = append(leftIndices, leftIdx)
				rightIndices = append(rightIndices, -1)
			}
			leftIdx++
		default:
			// Right key is smaller
			if joinType == RightJoin || joinType == FullOuterJoin {
				leftIndices = append(leftIndices, -1)
				rightIndices = append(rightIndices, rightIdx)
			}
			rightIdx++
		}
	}

	// Handle remaining unmatched rows
	if joinType == LeftJoin || joinType == FullOuterJoin {
		for i := leftIdx; i < leftSorted.Len(); i++ {
			leftIndices = append(leftIndices, i)
			rightIndices = append(rightIndices, -1)
		}
	}

	if joinType == RightJoin || joinType == FullOuterJoin {
		for i := rightIdx; i < rightSorted.Len(); i++ {
			leftIndices = append(leftIndices, -1)
			rightIndices = append(rightIndices, i)
		}
	}

	// If we sorted the DataFrames, we need to map indices back to original order
	if leftSorted != df || rightSorted != right {
		// This would require maintaining sort permutation - simplified for now
		return leftSorted.buildJoinResult(rightSorted, leftIndices, rightIndices, mem)
	}

	return df.buildJoinResult(right, leftIndices, rightIndices, mem)
}

// compareKeys compares two join keys
func compareKeys(key1, key2 string) int {
	if key1 < key2 {
		return -1
	} else if key1 > key2 {
		return 1
	}
	return 0
}

// OptimizedHashMap uses xxhash for better performance
type OptimizedHashMap struct {
	buckets    [][]hashEntry
	capacity   int
	size       int
	loadFactor float64
}

type hashEntry struct {
	key   string
	value []int
}

// NewOptimizedHashMap creates a new optimized hash map
func NewOptimizedHashMap(estimatedSize int) *OptimizedHashMap {
	capacity := nextPowerOfTwo(int(float64(estimatedSize) * hashMapCapacityFactor))
	return &OptimizedHashMap{
		buckets:    make([][]hashEntry, capacity),
		capacity:   capacity,
		loadFactor: hashMapLoadFactor,
	}
}

// Put adds a key-value pair to the hash map
func (ohm *OptimizedHashMap) Put(key string, value int) {
	hash := xxhash.Sum64String(key)
	bucketIdx := int(hash&0x7FFFFFFF) % ohm.capacity

	// Check if key already exists
	for i := range ohm.buckets[bucketIdx] {
		if ohm.buckets[bucketIdx][i].key == key {
			ohm.buckets[bucketIdx][i].value = append(ohm.buckets[bucketIdx][i].value, value)
			return
		}
	}

	// Add new entry
	ohm.buckets[bucketIdx] = append(ohm.buckets[bucketIdx], hashEntry{
		key:   key,
		value: []int{value},
	})
	ohm.size++

	// Check if resize is needed
	if float64(ohm.size) > float64(ohm.capacity)*ohm.loadFactor {
		ohm.resize()
	}
}

// Get retrieves values for a key
func (ohm *OptimizedHashMap) Get(key string) ([]int, bool) {
	hash := xxhash.Sum64String(key)
	bucketIdx := int(hash&0x7FFFFFFF) % ohm.capacity

	for _, entry := range ohm.buckets[bucketIdx] {
		if entry.key == key {
			return entry.value, true
		}
	}

	return nil, false
}

// resize doubles the capacity and rehashes all entries
func (ohm *OptimizedHashMap) resize() {
	newCapacity := ohm.capacity * hashMapGrowthFactor
	newBuckets := make([][]hashEntry, newCapacity)

	// Rehash all entries
	for _, bucket := range ohm.buckets {
		for _, entry := range bucket {
			hash := xxhash.Sum64String(entry.key)
			newBucketIdx := int(hash&0x7FFFFFFF) % newCapacity
			newBuckets[newBucketIdx] = append(newBuckets[newBucketIdx], entry)
		}
	}

	ohm.buckets = newBuckets
	ohm.capacity = newCapacity
}

// nextPowerOfTwo returns the next power of two >= n
func nextPowerOfTwo(n int) int {
	if n <= 1 {
		return 1
	}
	power := 1
	for power < n {
		power <<= 1
	}
	return power
}

// optimizedHashJoin uses the optimized hash map for better performance
func (df *DataFrame) optimizedHashJoin(
	right *DataFrame, leftKeys, rightKeys []string, joinType JoinType,
) (*DataFrame, error) {
	mem := memory.NewGoAllocator()

	// Build optimized hash map from right DataFrame
	rightHashMap := NewOptimizedHashMap(right.Len())

	// Use parallel processing for large right tables
	if right.Len() > optimizedHashThreshold {
		wp := parallel.NewWorkerPool(defaultWorkerCount)
		defer wp.Close()

		// Build hash map in parallel
		const chunkSize = 1000
		numChunks := (right.Len() + chunkSize - 1) / chunkSize

		type chunkData struct {
			start, end int
		}

		chunks := make([]chunkData, numChunks)
		for i := 0; i < numChunks; i++ {
			start := i * chunkSize
			end := start + chunkSize
			if end > right.Len() {
				end = right.Len()
			}
			chunks[i] = chunkData{start: start, end: end}
		}

		// Build partial hash maps in parallel
		partialMaps := parallel.Process(wp, chunks, func(chunk chunkData) *OptimizedHashMap {
			partial := NewOptimizedHashMap(chunk.end - chunk.start)
			for i := chunk.start; i < chunk.end; i++ {
				key := buildJoinKey(right, rightKeys, i)
				partial.Put(key, i)
			}
			return partial
		})

		// Merge partial maps
		for _, partial := range partialMaps {
			for _, bucket := range partial.buckets {
				for _, entry := range bucket {
					for _, idx := range entry.value {
						rightHashMap.Put(entry.key, idx)
					}
				}
			}
		}
	} else {
		// Sequential build for smaller tables
		for i := 0; i < right.Len(); i++ {
			key := buildJoinKey(right, rightKeys, i)
			rightHashMap.Put(key, i)
		}
	}

	// Perform join using optimized hash map
	var leftIndices, rightIndices []int

	switch joinType {
	case InnerJoin:
		for i := 0; i < df.Len(); i++ {
			key := buildJoinKey(df, leftKeys, i)
			if rightRows, exists := rightHashMap.Get(key); exists {
				for _, rightIdx := range rightRows {
					leftIndices = append(leftIndices, i)
					rightIndices = append(rightIndices, rightIdx)
				}
			}
		}

	case LeftJoin:
		for i := 0; i < df.Len(); i++ {
			key := buildJoinKey(df, leftKeys, i)
			if rightRows, exists := rightHashMap.Get(key); exists {
				for _, rightIdx := range rightRows {
					leftIndices = append(leftIndices, i)
					rightIndices = append(rightIndices, rightIdx)
				}
			} else {
				leftIndices = append(leftIndices, i)
				rightIndices = append(rightIndices, -1)
			}
		}

	case RightJoin, FullOuterJoin:
		// Track matched right rows
		matchedRight := make(map[int]bool)

		// Process all left rows
		for i := 0; i < df.Len(); i++ {
			key := buildJoinKey(df, leftKeys, i)
			if rightRows, exists := rightHashMap.Get(key); exists {
				for _, rightIdx := range rightRows {
					leftIndices = append(leftIndices, i)
					rightIndices = append(rightIndices, rightIdx)
					matchedRight[rightIdx] = true
				}
			} else if joinType == FullOuterJoin {
				leftIndices = append(leftIndices, i)
				rightIndices = append(rightIndices, -1)
			}
		}

		// Add unmatched right rows
		for i := 0; i < right.Len(); i++ {
			if !matchedRight[i] {
				leftIndices = append(leftIndices, -1)
				rightIndices = append(rightIndices, i)
			}
		}

	default:
		return nil, fmt.Errorf("unsupported join type: %v", joinType)
	}

	return df.buildJoinResult(right, leftIndices, rightIndices, mem)
}

// MergeJoinOptimizer tracks if DataFrames are pre-sorted
type MergeJoinOptimizer struct {
	leftSorted  bool
	rightSorted bool
	sortKeys    []string
}

// NewMergeJoinOptimizer creates a merge join optimizer
func NewMergeJoinOptimizer(leftSorted, rightSorted bool, sortKeys []string) *MergeJoinOptimizer {
	return &MergeJoinOptimizer{
		leftSorted:  leftSorted,
		rightSorted: rightSorted,
		sortKeys:    sortKeys,
	}
}

// Join performs optimized merge join
func (mjo *MergeJoinOptimizer) Join(left, right *DataFrame, options *JoinOptions) (*DataFrame, error) {
	// Set sorted flags in join options
	enhancedOptions := *options
	if mjo.leftSorted && mjo.rightSorted {
		// Both already sorted, use merge join directly
		return left.mergeJoin(right, options.LeftKeys, options.RightKeys, options.Type)
	}
	return left.OptimizedJoin(right, &enhancedOptions)
}
