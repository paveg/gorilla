package dataframe

import (
	"errors"
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	xxhash "github.com/cespare/xxhash/v2"
	"github.com/paveg/gorilla/internal/parallel"
)

// Constants for join optimization thresholds.
const (
	broadcastThreshold     = 1000       // rows for broadcast join
	mergeJoinThreshold     = 10000      // rows where merge join becomes beneficial
	optimizedHashThreshold = 5000       // rows for optimized hash map
	defaultWorkerCount     = 4          // default number of workers for parallel processing
	hashMapLoadFactor      = 0.75       // load factor for optimized hash map
	hashMapGrowthFactor    = 2          // growth factor for hash map resize
	hashMapCapacityFactor  = 1.3        // capacity factor for initial hash map size
	hashSignBitMask        = 0x7FFFFFFF // mask to remove sign bit from hash for positive modulo
)

// JoinStrategy represents different join algorithms.
type JoinStrategy int

const (
	HashJoinStrategy JoinStrategy = iota
	BroadcastJoinStrategy
	MergeJoinStrategy
	OptimizedHashJoinStrategy
)

// TableStats holds statistics about a DataFrame for optimization decisions.
type TableStats struct {
	RowCount      int
	SortedColumns map[string]bool
	Cardinality   map[string]int // distinct values per column
}

// JoinOptimizer selects optimal join strategy based on table statistics.
type JoinOptimizer struct {
	leftStats  TableStats
	rightStats TableStats
}

// NewJoinOptimizer creates a new join optimizer with table statistics.
func NewJoinOptimizer(left, right *DataFrame) *JoinOptimizer {
	return &JoinOptimizer{
		leftStats:  computeTableStats(left),
		rightStats: computeTableStats(right),
	}
}

// SelectStrategy chooses the optimal join strategy based on table characteristics.
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

// computeTableStats analyzes DataFrame characteristics for optimization.
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

// isColumnSorted checks if a column is sorted in ascending order.
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

// compareValues compares two values in a series.
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

// estimateCardinality estimates distinct values in a column.
func estimateCardinality(col ISeries) int {
	seen := make(map[string]bool)
	for i := range col.Len() {
		val := getStringValue(col, i)
		seen[val] = true
	}
	return len(seen)
}

// OptimizedJoin performs join using the selected optimal strategy.
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

// broadcastJoin implements broadcast join for small right tables.
func (df *DataFrame) broadcastJoin(
	right *DataFrame, leftKeys, rightKeys []string, joinType JoinType,
) (*DataFrame, error) {
	mem := memory.NewGoAllocator()

	// Build complete hash map from small right table (broadcast)
	rightHashMap := df.buildBroadcastHashMap(right, rightKeys)

	// Process left table in parallel chunks
	leftIndices, rightIndices := df.processBroadcastJoinParallel(leftKeys, rightHashMap, joinType)

	// Handle right/full outer join unmatched rows
	df.handleBroadcastUnmatchedRows(right, &leftIndices, &rightIndices, joinType)

	return df.buildJoinResult(right, leftIndices, rightIndices, mem)
}

// broadcastChunkResult represents the result of processing a chunk in broadcast join.
type broadcastChunkResult struct {
	leftIndices  []int
	rightIndices []int
}

// buildBroadcastHashMap builds a hash map from the right DataFrame for broadcast join.
func (df *DataFrame) buildBroadcastHashMap(right *DataFrame, rightKeys []string) map[string][]int {
	rightHashMap := make(map[string][]int)
	for i := range right.Len() {
		key := buildJoinKey(right, rightKeys, i)
		rightHashMap[key] = append(rightHashMap[key], i)
	}
	return rightHashMap
}

// processBroadcastJoinParallel processes the broadcast join using parallel chunks.
func (df *DataFrame) processBroadcastJoinParallel(
	leftKeys []string,
	rightHashMap map[string][]int,
	joinType JoinType,
) ([]int, []int) {
	const chunkSize = 1000
	numChunks := (df.Len() + chunkSize - 1) / chunkSize

	results := make([]broadcastChunkResult, numChunks)
	var wg sync.WaitGroup

	for chunkIdx := range numChunks {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = df.processBroadcastChunk(idx, chunkSize, leftKeys, rightHashMap, joinType)
		}(chunkIdx)
	}

	wg.Wait()

	// Combine results
	var leftIndices, rightIndices []int
	for _, result := range results {
		leftIndices = append(leftIndices, result.leftIndices...)
		rightIndices = append(rightIndices, result.rightIndices...)
	}

	return leftIndices, rightIndices
}

// processBroadcastChunk processes a single chunk for broadcast join.
func (df *DataFrame) processBroadcastChunk(
	chunkIdx, chunkSize int,
	leftKeys []string,
	rightHashMap map[string][]int,
	joinType JoinType,
) broadcastChunkResult {
	start := chunkIdx * chunkSize
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

	return broadcastChunkResult{
		leftIndices:  leftIndices,
		rightIndices: rightIndices,
	}
}

// handleBroadcastUnmatchedRows handles unmatched rows for right/full outer joins in broadcast join.
func (df *DataFrame) handleBroadcastUnmatchedRows(
	right *DataFrame,
	leftIndices, rightIndices *[]int,
	joinType JoinType,
) {
	if joinType == RightJoin || joinType == FullOuterJoin {
		matched := make(map[int]bool)
		for _, idx := range *rightIndices {
			if idx >= 0 {
				matched[idx] = true
			}
		}

		for i := range right.Len() {
			if !matched[i] {
				*leftIndices = append(*leftIndices, -1)
				*rightIndices = append(*rightIndices, i)
			}
		}
	}
}

// mergeJoin implements merge join for sorted data.
func (df *DataFrame) mergeJoin(
	right *DataFrame, leftKeys, rightKeys []string, joinType JoinType,
) (*DataFrame, error) {
	if len(leftKeys) != 1 || len(rightKeys) != 1 {
		return nil, errors.New("merge join currently supports only single-column joins")
	}

	mem := memory.NewGoAllocator()

	// Prepare sorted DataFrames
	leftSorted, rightSorted, err := df.prepareSortedDataFrames(right, leftKeys, rightKeys)
	if err != nil {
		return nil, err
	}
	defer df.releaseSortedDataFrames(leftSorted, rightSorted)

	// Perform merge join main loop
	leftIndices, rightIndices, finalLeftIdx, finalRightIdx := df.performMergeJoinLoop(
		leftSorted,
		rightSorted,
		leftKeys,
		rightKeys,
		joinType,
	)

	// Handle remaining unmatched rows for outer joins
	df.handleUnmatchedRows(leftSorted, rightSorted, &leftIndices, &rightIndices, joinType, finalLeftIdx, finalRightIdx)

	// Build result
	if leftSorted != df || rightSorted != right {
		// This would require maintaining sort permutation - simplified for now
		return leftSorted.buildJoinResult(rightSorted, leftIndices, rightIndices, mem)
	}

	return df.buildJoinResult(right, leftIndices, rightIndices, mem)
}

// prepareSortedDataFrames ensures both DataFrames are sorted by their join keys.
func (df *DataFrame) prepareSortedDataFrames(
	right *DataFrame,
	leftKeys, rightKeys []string,
) (*DataFrame, *DataFrame, error) {
	leftSorted := df
	rightSorted := right

	optimizer := NewJoinOptimizer(df, right)
	if !optimizer.leftStats.SortedColumns[leftKeys[0]] {
		var err error
		leftSorted, err = df.Sort(leftKeys[0], true)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to sort left DataFrame: %w", err)
		}
	}
	if !optimizer.rightStats.SortedColumns[rightKeys[0]] {
		var err error
		rightSorted, err = right.Sort(rightKeys[0], true)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to sort right DataFrame: %w", err)
		}
	}

	return leftSorted, rightSorted, nil
}

// releaseSortedDataFrames releases sorted DataFrames if they were created during preparation.
func (df *DataFrame) releaseSortedDataFrames(leftSorted, rightSorted *DataFrame) {
	if leftSorted != df {
		leftSorted.Release()
	}
	if rightSorted != df {
		rightSorted.Release()
	}
}

// performMergeJoinLoop performs the main merge join loop.
func (df *DataFrame) performMergeJoinLoop(
	leftSorted, rightSorted *DataFrame,
	leftKeys, rightKeys []string,
	joinType JoinType,
) ([]int, []int, int, int) {
	var leftIndices, rightIndices []int
	leftIdx, rightIdx := 0, 0

	for leftIdx < leftSorted.Len() && rightIdx < rightSorted.Len() {
		cmp := df.compareJoinKeys(leftSorted, rightSorted, leftKeys, rightKeys, leftIdx, rightIdx)

		switch {
		case cmp == 0:
			// Handle matching keys
			leftStart, rightStart := leftIdx, rightIdx
			leftIdx, rightIdx = df.findMatchingRowRanges(
				leftSorted,
				rightSorted,
				leftKeys,
				rightKeys,
				leftStart,
				rightStart,
			)
			df.addCartesianProduct(&leftIndices, &rightIndices, leftStart, leftIdx, rightStart, rightIdx)
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

	return leftIndices, rightIndices, leftIdx, rightIdx
}

// compareJoinKeys compares keys at given indices in sorted DataFrames.
func (df *DataFrame) compareJoinKeys(
	leftSorted, rightSorted *DataFrame,
	leftKeys, rightKeys []string,
	leftIdx, rightIdx int,
) int {
	if len(leftKeys) == 1 && len(rightKeys) == 1 {
		leftCol, _ := leftSorted.Column(leftKeys[0])
		rightCol, _ := rightSorted.Column(rightKeys[0])
		return compareTypedValues(leftCol, leftIdx, rightCol, rightIdx)
	}
	leftKey := buildJoinKey(leftSorted, leftKeys, leftIdx)
	rightKey := buildJoinKey(rightSorted, rightKeys, rightIdx)
	return compareKeys(leftKey, rightKey)
}

// findMatchingRowRanges finds the end indices of matching rows for both DataFrames.
func (df *DataFrame) findMatchingRowRanges(
	leftSorted, rightSorted *DataFrame,
	leftKeys, rightKeys []string,
	leftStart, rightStart int,
) (int, int) {
	leftIdx, rightIdx := leftStart, rightStart
	currentKey := buildJoinKey(leftSorted, leftKeys, leftStart)

	// Find end of matching left rows
	if len(leftKeys) == 1 {
		leftCol, _ := leftSorted.Column(leftKeys[0])
		for leftIdx < leftSorted.Len() && compareTypedValues(leftCol, leftStart, leftCol, leftIdx) == 0 {
			leftIdx++
		}
	} else {
		for leftIdx < leftSorted.Len() && buildJoinKey(leftSorted, leftKeys, leftIdx) == currentKey {
			leftIdx++
		}
	}

	// Find end of matching right rows
	if len(rightKeys) == 1 {
		rightCol, _ := rightSorted.Column(rightKeys[0])
		for rightIdx < rightSorted.Len() && compareTypedValues(rightCol, rightStart, rightCol, rightIdx) == 0 {
			rightIdx++
		}
	} else {
		for rightIdx < rightSorted.Len() && buildJoinKey(rightSorted, rightKeys, rightIdx) == currentKey {
			rightIdx++
		}
	}

	return leftIdx, rightIdx
}

// addCartesianProduct adds the cartesian product of matching rows to the indices.
func (df *DataFrame) addCartesianProduct(
	leftIndices, rightIndices *[]int,
	leftStart, leftEnd, rightStart, rightEnd int,
) {
	for i := leftStart; i < leftEnd; i++ {
		for j := rightStart; j < rightEnd; j++ {
			*leftIndices = append(*leftIndices, i)
			*rightIndices = append(*rightIndices, j)
		}
	}
}

// handleUnmatchedRows handles remaining unmatched rows for outer joins.
func (df *DataFrame) handleUnmatchedRows(
	leftSorted, rightSorted *DataFrame,
	leftIndices, rightIndices *[]int,
	joinType JoinType,
	finalLeftIdx, finalRightIdx int,
) {
	// Handle remaining unmatched rows using the actual final positions from the main loop
	if joinType == LeftJoin || joinType == FullOuterJoin {
		for i := finalLeftIdx; i < leftSorted.Len(); i++ {
			*leftIndices = append(*leftIndices, i)
			*rightIndices = append(*rightIndices, -1)
		}
	}

	if joinType == RightJoin || joinType == FullOuterJoin {
		for i := finalRightIdx; i < rightSorted.Len(); i++ {
			*leftIndices = append(*leftIndices, -1)
			*rightIndices = append(*rightIndices, i)
		}
	}
}

// compareKeys compares two join keys.
func compareKeys(key1, key2 string) int {
	if key1 < key2 {
		return -1
	} else if key1 > key2 {
		return 1
	}
	return 0
}

// compareTypedValues compares values from series at given indices.
func compareTypedValues(leftSeries ISeries, leftIdx int, rightSeries ISeries, rightIdx int) int {
	leftArr := leftSeries.Array()
	defer leftArr.Release()
	rightArr := rightSeries.Array()
	defer rightArr.Release()

	// Handle same type comparisons efficiently
	switch leftTyped := leftArr.(type) {
	case *array.Int64:
		return compareInt64Arrays(leftTyped, rightArr, leftIdx, rightIdx)
	case *array.Int32:
		return compareInt32Arrays(leftTyped, rightArr, leftIdx, rightIdx)
	case *array.Float64:
		return compareFloat64Arrays(leftTyped, rightArr, leftIdx, rightIdx)
	case *array.Float32:
		return compareFloat32Arrays(leftTyped, rightArr, leftIdx, rightIdx)
	case *array.String:
		return compareStringArrays(leftTyped, rightArr, leftIdx, rightIdx)
	}

	// Fallback to string comparison
	leftKey := getStringValue(leftSeries, leftIdx)
	rightKey := getStringValue(rightSeries, rightIdx)
	return compareKeys(leftKey, rightKey)
}

// Type-specific comparison helper functions.
func compareInt64Arrays(leftArr *array.Int64, rightArr arrow.Array, leftIdx, rightIdx int) int {
	if rightTyped, ok := rightArr.(*array.Int64); ok {
		left := leftArr.Value(leftIdx)
		right := rightTyped.Value(rightIdx)
		return compareOrderedValues(left, right)
	}
	return -2 // Type mismatch - should not happen in well-formed joins
}

func compareInt32Arrays(leftArr *array.Int32, rightArr arrow.Array, leftIdx, rightIdx int) int {
	if rightTyped, ok := rightArr.(*array.Int32); ok {
		left := leftArr.Value(leftIdx)
		right := rightTyped.Value(rightIdx)
		return compareOrderedValues(left, right)
	}
	return -2 // Type mismatch
}

func compareFloat64Arrays(leftArr *array.Float64, rightArr arrow.Array, leftIdx, rightIdx int) int {
	if rightTyped, ok := rightArr.(*array.Float64); ok {
		left := leftArr.Value(leftIdx)
		right := rightTyped.Value(rightIdx)
		return compareOrderedValues(left, right)
	}
	return -2 // Type mismatch
}

func compareFloat32Arrays(leftArr *array.Float32, rightArr arrow.Array, leftIdx, rightIdx int) int {
	if rightTyped, ok := rightArr.(*array.Float32); ok {
		left := leftArr.Value(leftIdx)
		right := rightTyped.Value(rightIdx)
		return compareOrderedValues(left, right)
	}
	return -2 // Type mismatch
}

func compareStringArrays(leftArr *array.String, rightArr arrow.Array, leftIdx, rightIdx int) int {
	if rightTyped, ok := rightArr.(*array.String); ok {
		left := leftArr.Value(leftIdx)
		right := rightTyped.Value(rightIdx)
		return compareOrderedValues(left, right)
	}
	return -2 // Type mismatch
}

// OptimizedHashMap uses xxhash for better performance.
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

// NewOptimizedHashMap creates a new optimized hash map.
func NewOptimizedHashMap(estimatedSize int) *OptimizedHashMap {
	capacity := nextPowerOfTwo(int(float64(estimatedSize) * hashMapCapacityFactor))
	return &OptimizedHashMap{
		buckets:    make([][]hashEntry, capacity),
		capacity:   capacity,
		loadFactor: hashMapLoadFactor,
	}
}

// Put adds a key-value pair to the hash map.
func (ohm *OptimizedHashMap) Put(key string, value int) {
	hash := xxhash.Sum64String(key)
	// Safe conversion: ensure capacity is within bounds
	if ohm.capacity <= 0 {
		return // Skip if invalid capacity
	}
	//nolint:gosec // capacity is validated to be positive above
	bucketIdx := int((hash & hashSignBitMask) % uint64(ohm.capacity))

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

// Get retrieves values for a key.
func (ohm *OptimizedHashMap) Get(key string) ([]int, bool) {
	hash := xxhash.Sum64String(key)
	// Safe conversion: ensure capacity is within bounds
	if ohm.capacity <= 0 {
		return nil, false // Return empty if invalid capacity
	}
	//nolint:gosec // capacity is validated to be positive above
	bucketIdx := int((hash & hashSignBitMask) % uint64(ohm.capacity))

	for _, entry := range ohm.buckets[bucketIdx] {
		if entry.key == key {
			return entry.value, true
		}
	}

	return nil, false
}

// resize doubles the capacity and rehashes all entries.
func (ohm *OptimizedHashMap) resize() {
	newCapacity := ohm.capacity * hashMapGrowthFactor
	newBuckets := make([][]hashEntry, newCapacity)

	// Rehash all entries
	for _, bucket := range ohm.buckets {
		for _, entry := range bucket {
			hash := xxhash.Sum64String(entry.key)
			// Safe conversion: ensure newCapacity is within bounds
			if newCapacity <= 0 {
				continue // Skip if invalid capacity
			}
			//nolint:gosec // newCapacity is validated to be positive above
			newBucketIdx := int((hash & hashSignBitMask) % uint64(newCapacity))
			newBuckets[newBucketIdx] = append(newBuckets[newBucketIdx], entry)
		}
	}

	ohm.buckets = newBuckets
	ohm.capacity = newCapacity
}

// nextPowerOfTwo returns the next power of two >= n.
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

// optimizedHashJoin uses the optimized hash map for better performance.
func (df *DataFrame) optimizedHashJoin(
	right *DataFrame, leftKeys, rightKeys []string, joinType JoinType,
) (*DataFrame, error) {
	mem := memory.NewGoAllocator()

	// Build optimized hash map from right DataFrame
	rightHashMap := df.buildOptimizedHashMap(right, rightKeys)

	// Perform join using optimized hash map
	leftIndices, rightIndices, err := df.performHashJoinByType(right, leftKeys, rightHashMap, joinType)
	if err != nil {
		return nil, err
	}

	return df.buildJoinResult(right, leftIndices, rightIndices, mem)
}

// buildOptimizedHashMap builds a hash map from the right DataFrame.
func (df *DataFrame) buildOptimizedHashMap(right *DataFrame, rightKeys []string) *OptimizedHashMap {
	rightHashMap := NewOptimizedHashMap(right.Len())

	// Use parallel processing for large right tables
	if right.Len() > optimizedHashThreshold {
		return df.buildHashMapParallel(right, rightKeys, rightHashMap)
	}

	// Sequential build for smaller tables
	for i := range right.Len() {
		key := buildJoinKey(right, rightKeys, i)
		rightHashMap.Put(key, i)
	}
	return rightHashMap
}

// buildHashMapParallel builds a hash map using parallel processing.
func (df *DataFrame) buildHashMapParallel(
	right *DataFrame,
	rightKeys []string,
	rightHashMap *OptimizedHashMap,
) *OptimizedHashMap {
	wp := parallel.NewWorkerPool(defaultWorkerCount)
	defer wp.Close()

	// Build hash map in parallel
	const chunkSize = 1000
	numChunks := (right.Len() + chunkSize - 1) / chunkSize

	type chunkData struct {
		start, end int
	}

	chunks := make([]chunkData, numChunks)
	for i := range numChunks {
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
	return rightHashMap
}

// performHashJoinByType performs the hash join based on join type.
func (df *DataFrame) performHashJoinByType(
	right *DataFrame,
	leftKeys []string,
	rightHashMap *OptimizedHashMap,
	joinType JoinType,
) ([]int, []int, error) {
	switch joinType {
	case InnerJoin:
		leftIndices, rightIndices := df.performInnerHashJoin(leftKeys, rightHashMap)
		return leftIndices, rightIndices, nil
	case LeftJoin:
		leftIndices, rightIndices := df.performLeftHashJoin(leftKeys, rightHashMap)
		return leftIndices, rightIndices, nil
	case RightJoin, FullOuterJoin:
		leftIndices, rightIndices := df.performOuterHashJoin(right, leftKeys, rightHashMap, joinType)
		return leftIndices, rightIndices, nil
	default:
		return nil, nil, fmt.Errorf("unsupported join type: %v", joinType)
	}
}

// performInnerHashJoin performs an inner hash join.
func (df *DataFrame) performInnerHashJoin(leftKeys []string, rightHashMap *OptimizedHashMap) ([]int, []int) {
	var leftIndices, rightIndices []int
	for i := range df.Len() {
		key := buildJoinKey(df, leftKeys, i)
		if rightRows, exists := rightHashMap.Get(key); exists {
			for _, rightIdx := range rightRows {
				leftIndices = append(leftIndices, i)
				rightIndices = append(rightIndices, rightIdx)
			}
		}
	}
	return leftIndices, rightIndices
}

// performLeftHashJoin performs a left hash join.
func (df *DataFrame) performLeftHashJoin(leftKeys []string, rightHashMap *OptimizedHashMap) ([]int, []int) {
	var leftIndices, rightIndices []int
	for i := range df.Len() {
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
	return leftIndices, rightIndices
}

// performOuterHashJoin performs right or full outer hash join.
func (df *DataFrame) performOuterHashJoin(
	right *DataFrame,
	leftKeys []string,
	rightHashMap *OptimizedHashMap,
	joinType JoinType,
) ([]int, []int) {
	var leftIndices, rightIndices []int
	// Track matched right rows
	matchedRight := make(map[int]bool)

	// Process all left rows
	for i := range df.Len() {
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
	for i := range right.Len() {
		if !matchedRight[i] {
			leftIndices = append(leftIndices, -1)
			rightIndices = append(rightIndices, i)
		}
	}

	return leftIndices, rightIndices
}

// MergeJoinOptimizer tracks if DataFrames are pre-sorted.
type MergeJoinOptimizer struct {
	leftSorted  bool
	rightSorted bool
	sortKeys    []string
}

// NewMergeJoinOptimizer creates a merge join optimizer.
func NewMergeJoinOptimizer(leftSorted, rightSorted bool, sortKeys []string) *MergeJoinOptimizer {
	return &MergeJoinOptimizer{
		leftSorted:  leftSorted,
		rightSorted: rightSorted,
		sortKeys:    sortKeys,
	}
}

// Join performs optimized merge join.
func (mjo *MergeJoinOptimizer) Join(left, right *DataFrame, options *JoinOptions) (*DataFrame, error) {
	// Set sorted flags in join options
	enhancedOptions := *options
	if mjo.leftSorted && mjo.rightSorted {
		// Both already sorted, use merge join directly
		return left.mergeJoin(right, options.LeftKeys, options.RightKeys, options.Type)
	}
	return left.OptimizedJoin(right, &enhancedOptions)
}
