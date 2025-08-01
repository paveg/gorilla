//nolint:testpackage // requires internal access to unexported types and functions
package dataframe

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJoinOptimizer_SelectStrategy(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("Small right table selects broadcast join", func(t *testing.T) {
		// Create large left DataFrame
		leftIDs := make([]int64, 2000)
		for i := range leftIDs {
			leftIDs[i] = int64(i)
		}
		left := New(series.New("id", leftIDs, mem))
		defer left.Release()

		// Create small right DataFrame
		rightIDs := make([]int64, 500)
		for i := range rightIDs {
			rightIDs[i] = int64(i * 2)
		}
		right := New(series.New("id", rightIDs, mem))
		defer right.Release()

		optimizer := NewJoinOptimizer(left, right)
		strategy := optimizer.SelectStrategy([]string{"id"}, []string{"id"})

		assert.Equal(t, BroadcastJoinStrategy, strategy)
	})

	t.Run("Sorted data selects merge join", func(t *testing.T) {
		// Create sorted DataFrames (smaller size to avoid optimized hash join threshold)
		size := 2000
		leftIDs := make([]int64, size)
		rightIDs := make([]int64, size/2)

		for i := range leftIDs {
			leftIDs[i] = int64(i)
		}
		for i := range rightIDs {
			rightIDs[i] = int64(i * 2)
		}

		left := New(series.New("id", leftIDs, mem))
		defer left.Release()

		right := New(series.New("id", rightIDs, mem))
		defer right.Release()

		optimizer := NewJoinOptimizer(left, right)
		strategy := optimizer.SelectStrategy([]string{"id"}, []string{"id"})

		assert.Equal(t, MergeJoinStrategy, strategy)
	})

	t.Run("Large unsorted data selects optimized hash join", func(t *testing.T) {
		// Create large unsorted DataFrames
		size := 10000
		leftIDs := make([]int64, size)
		rightIDs := make([]int64, size)

		// Pseudo-random unsorted data (using deterministic pattern)
		for i := range leftIDs {
			leftIDs[i] = int64((i*13 + 7) % (size * 2))
		}
		for i := range rightIDs {
			rightIDs[i] = int64((i*17 + 11) % (size * 2))
		}

		left := New(series.New("id", leftIDs, mem))
		defer left.Release()

		right := New(series.New("id", rightIDs, mem))
		defer right.Release()

		optimizer := NewJoinOptimizer(left, right)
		strategy := optimizer.SelectStrategy([]string{"id"}, []string{"id"})

		assert.Equal(t, OptimizedHashJoinStrategy, strategy)
	})
}

func TestBroadcastJoin(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("Inner broadcast join", func(t *testing.T) {
		// Large left DataFrame
		leftIDs := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		leftValues := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
		left := New(
			series.New("id", leftIDs, mem),
			series.New("value", leftValues, mem),
		)
		defer left.Release()

		// Small right DataFrame (broadcast)
		rightIDs := []int64{2, 4, 6}
		rightScores := []float64{2.5, 4.5, 6.5}
		right := New(
			series.New("id", rightIDs, mem),
			series.New("score", rightScores, mem),
		)
		defer right.Release()

		result, err := left.broadcastJoin(right, []string{"id"}, []string{"id"}, InnerJoin)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 3, result.Len())

		// Verify joined data
		idCol, _ := result.Column("id")
		valueCol, _ := result.Column("value")
		scoreCol, _ := result.Column("score")

		expectedIDs := []int64{2, 4, 6}
		expectedValues := []string{"b", "d", "f"}
		expectedScores := []float64{2.5, 4.5, 6.5}

		for i := range result.Len() {
			assert.Equal(t, expectedIDs[i], idCol.(*series.Series[int64]).Value(i))
			assert.Equal(t, expectedValues[i], valueCol.(*series.Series[string]).Value(i))
			assert.InDelta(t, expectedScores[i], scoreCol.(*series.Series[float64]).Value(i), 0.001)
		}
	})

	t.Run("Left broadcast join", func(t *testing.T) {
		leftIDs := []int64{1, 2, 3, 4, 5}
		leftValues := []string{"a", "b", "c", "d", "e"}
		left := New(
			series.New("id", leftIDs, mem),
			series.New("value", leftValues, mem),
		)
		defer left.Release()

		rightIDs := []int64{2, 4}
		rightScores := []float64{2.5, 4.5}
		right := New(
			series.New("id", rightIDs, mem),
			series.New("score", rightScores, mem),
		)
		defer right.Release()

		result, err := left.broadcastJoin(right, []string{"id"}, []string{"id"}, LeftJoin)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 5, result.Len())
	})
}

func TestMergeJoin(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("Inner merge join on sorted data", func(t *testing.T) {
		// Test with all 5 matching IDs
		leftIDs := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		leftValues := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
		left := New(
			series.New("id", leftIDs, mem),
			series.New("value", leftValues, mem),
		)
		defer left.Release()

		rightIDs := []int64{2, 4, 6, 8, 10}
		rightScores := []float64{2.5, 4.5, 6.5, 8.5, 10.5}
		right := New(
			series.New("id", rightIDs, mem),
			series.New("score", rightScores, mem),
		)
		defer right.Release()

		result, err := left.mergeJoin(right, []string{"id"}, []string{"id"}, InnerJoin)
		require.NoError(t, err)
		defer result.Release()

		// Debug output
		t.Logf("Result has %d rows", result.Len())
		if result.Len() > 0 {
			idCol, _ := result.Column("id")
			var actualIDs []int64
			for i := range result.Len() {
				actualIDs = append(actualIDs, idCol.(*series.Series[int64]).Value(i))
			}
			t.Logf("Actual IDs: %v", actualIDs)
		}

		assert.Equal(t, 5, result.Len())

		// Verify results
		idCol, _ := result.Column("id")
		expectedIDs := []int64{2, 4, 6, 8, 10}
		for i := range result.Len() {
			assert.Equal(t, expectedIDs[i], idCol.(*series.Series[int64]).Value(i))
		}
	})

	t.Run("Full outer merge join", func(t *testing.T) {
		leftIDs := []int64{1, 3, 5, 7}
		leftValues := []string{"a", "c", "e", "g"}
		left := New(
			series.New("id", leftIDs, mem),
			series.New("value", leftValues, mem),
		)
		defer left.Release()

		rightIDs := []int64{2, 3, 4, 5}
		rightScores := []float64{2.5, 3.5, 4.5, 5.5}
		right := New(
			series.New("id", rightIDs, mem),
			series.New("score", rightScores, mem),
		)
		defer right.Release()

		result, err := left.mergeJoin(right, []string{"id"}, []string{"id"}, FullOuterJoin)
		require.NoError(t, err)
		defer result.Release()

		// Debug output
		t.Logf("Result has %d rows", result.Len())
		t.Logf("Result columns: %v", result.Columns())
		if result.Len() > 0 {
			idCol, _ := result.Column("id")
			var actualIDs []int64
			for i := range result.Len() {
				actualIDs = append(actualIDs, idCol.(*series.Series[int64]).Value(i))
			}
			t.Logf("Actual IDs: %v", actualIDs)
			t.Logf("Expected IDs: [1, 2, 3, 4, 5, 7]")
		}

		// Should have all unique IDs: 1, 2, 3, 4, 5, 7
		assert.Equal(t, 6, result.Len())
	})

	t.Run("Merge join with duplicates", func(t *testing.T) {
		// Left has duplicates
		leftIDs := []int64{1, 2, 2, 3, 3, 3}
		leftValues := []string{"a1", "b1", "b2", "c1", "c2", "c3"}
		left := New(
			series.New("id", leftIDs, mem),
			series.New("value", leftValues, mem),
		)
		defer left.Release()

		// Right has duplicates
		rightIDs := []int64{2, 2, 3}
		rightScores := []float64{2.1, 2.2, 3.1}
		right := New(
			series.New("id", rightIDs, mem),
			series.New("score", rightScores, mem),
		)
		defer right.Release()

		result, err := left.mergeJoin(right, []string{"id"}, []string{"id"}, InnerJoin)
		require.NoError(t, err)
		defer result.Release()

		// Should create cartesian product:
		// id=2: 2 left rows × 2 right rows = 4 rows
		// id=3: 3 left rows × 1 right row = 3 rows
		// Total: 7 rows
		assert.Equal(t, 7, result.Len())
	})
}

func TestOptimizedHashMap(t *testing.T) {
	t.Run("Basic operations", func(t *testing.T) {
		hashMap := NewOptimizedHashMap(100)

		// Put and Get
		hashMap.Put("key1", 1)
		hashMap.Put("key2", 2)
		hashMap.Put("key1", 3) // Add to existing key

		val1, ok1 := hashMap.Get("key1")
		assert.True(t, ok1)
		assert.Equal(t, []int{1, 3}, val1)

		val2, ok2 := hashMap.Get("key2")
		assert.True(t, ok2)
		assert.Equal(t, []int{2}, val2)

		_, ok3 := hashMap.Get("key3")
		assert.False(t, ok3)
	})

	t.Run("Resize operation", func(t *testing.T) {
		hashMap := NewOptimizedHashMap(4) // Small initial capacity

		// Add enough entries to trigger resize
		for i := range 10 {
			key := fmt.Sprintf("key%d", i)
			hashMap.Put(key, i)
		}

		// Verify all entries are still accessible
		for i := range 10 {
			key := fmt.Sprintf("key%d", i)
			val, ok := hashMap.Get(key)
			assert.True(t, ok)
			assert.Equal(t, []int{i}, val)
		}
	})

	t.Run("Collision handling", func(t *testing.T) {
		hashMap := NewOptimizedHashMap(4)

		// Add keys that might collide
		keys := []string{"abc", "bcd", "cde", "def", "efg", "fgh"}
		for i, key := range keys {
			hashMap.Put(key, i)
		}

		// Verify all keys are retrievable
		for i, key := range keys {
			val, ok := hashMap.Get(key)
			assert.True(t, ok, "Key %s should exist", key)
			assert.Equal(t, []int{i}, val)
		}
	})
}

func TestOptimizedHashJoin(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("Large scale hash join", func(t *testing.T) {
		// Create large DataFrames
		size := 6000
		leftIDs := make([]int64, size)
		leftValues := make([]string, size)
		for i := range size {
			leftIDs[i] = int64(i)
			leftValues[i] = fmt.Sprintf("left_%d", i)
		}

		rightIDs := make([]int64, size/2)
		rightScores := make([]float64, size/2)
		for i := range size / 2 {
			rightIDs[i] = int64(i * 2) // Every other ID
			rightScores[i] = float64(i) * 1.5
		}

		left := New(
			series.New("id", leftIDs, mem),
			series.New("value", leftValues, mem),
		)
		defer left.Release()

		right := New(
			series.New("id", rightIDs, mem),
			series.New("score", rightScores, mem),
		)
		defer right.Release()

		result, err := left.optimizedHashJoin(right, []string{"id"}, []string{"id"}, InnerJoin)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, size/2, result.Len())
	})
}

func TestOptimizedJoin_Integration(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("Automatic strategy selection", func(t *testing.T) {
		// Test broadcast join selection (small right table)
		left := New(series.New("id", []int64{1, 2, 3, 4, 5}, mem))
		defer left.Release()

		right := New(series.New("id", []int64{2, 4}, mem))
		defer right.Release()

		options := &JoinOptions{
			Type:     InnerJoin,
			LeftKey:  "id",
			RightKey: "id",
		}

		result, err := left.OptimizedJoin(right, options)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 2, result.Len())
	})

	t.Run("Pre-sorted merge join optimization", func(t *testing.T) {
		// Create pre-sorted DataFrames
		leftIDs := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		rightIDs := []int64{2, 4, 6, 8, 10}

		left := New(series.New("id", leftIDs, mem))
		defer left.Release()

		right := New(series.New("id", rightIDs, mem))
		defer right.Release()

		optimizer := NewMergeJoinOptimizer(true, true, []string{"id"})
		options := &JoinOptions{
			Type:      InnerJoin,
			LeftKeys:  []string{"id"},
			RightKeys: []string{"id"},
		}

		result, err := optimizer.Join(left, right, options)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 5, result.Len())
	})
}

// Benchmark functions.
func BenchmarkJoinStrategies(b *testing.B) {
	mem := memory.NewGoAllocator()
	sizes := []int{1000, 5000, 10000}

	for _, size := range sizes {
		left, right := createJoinBenchmarkTestData(size, mem)
		defer left.Release()
		defer right.Release()

		runJoinBenchmarks(b, left, right, size)
	}
}

// createJoinBenchmarkTestData creates test DataFrames for join benchmarks.
func createJoinBenchmarkTestData(size int, mem memory.Allocator) (*DataFrame, *DataFrame) {
	leftIDs := make([]int64, size)
	rightIDs := make([]int64, size/2)

	for i := range leftIDs {
		leftIDs[i] = int64(i)
	}
	for i := range rightIDs {
		rightIDs[i] = int64(i * 2)
	}

	left := New(series.New("id", leftIDs, mem))
	right := New(series.New("id", rightIDs, mem))
	return left, right
}

// runJoinBenchmarks runs all join strategy benchmarks for a given size.
func runJoinBenchmarks(b *testing.B, left, right *DataFrame, size int) {
	benchmarkStandardHashJoin(b, left, right, size)
	benchmarkOptimizedHashJoin(b, left, right, size)
	benchmarkMergeJoin(b, left, right, size)
	benchmarkBroadcastJoinIfSmall(b, left, right, size)
}

// benchmarkStandardHashJoin runs standard hash join benchmark.
func benchmarkStandardHashJoin(b *testing.B, left, right *DataFrame, size int) {
	options := &JoinOptions{
		Type:     InnerJoin,
		LeftKey:  "id",
		RightKey: "id",
	}

	b.Run(fmt.Sprintf("StandardHashJoin_%d", size), func(b *testing.B) {
		for range b.N {
			result, err := left.Join(right, options)
			if err != nil {
				b.Fatal(err)
			}
			result.Release()
		}
	})
}

// benchmarkOptimizedHashJoin runs optimized hash join benchmark.
func benchmarkOptimizedHashJoin(b *testing.B, left, right *DataFrame, size int) {
	b.Run(fmt.Sprintf("OptimizedHashJoin_%d", size), func(b *testing.B) {
		for range b.N {
			result, err := left.optimizedHashJoin(right, []string{"id"}, []string{"id"}, InnerJoin)
			if err != nil {
				b.Fatal(err)
			}
			result.Release()
		}
	})
}

// benchmarkMergeJoin runs merge join benchmark.
func benchmarkMergeJoin(b *testing.B, left, right *DataFrame, size int) {
	b.Run(fmt.Sprintf("MergeJoin_%d", size), func(b *testing.B) {
		for range b.N {
			result, err := left.mergeJoin(right, []string{"id"}, []string{"id"}, InnerJoin)
			if err != nil {
				b.Fatal(err)
			}
			result.Release()
		}
	})
}

// benchmarkBroadcastJoinIfSmall runs broadcast join benchmark for small datasets.
func benchmarkBroadcastJoinIfSmall(b *testing.B, left, right *DataFrame, size int) {
	if size > 1000 {
		return
	}

	b.Run(fmt.Sprintf("BroadcastJoin_%d", size), func(b *testing.B) {
		for range b.N {
			result, err := left.broadcastJoin(right, []string{"id"}, []string{"id"}, InnerJoin)
			if err != nil {
				b.Fatal(err)
			}
			result.Release()
		}
	})
}

func BenchmarkOptimizedHashMap(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("OptimizedHashMap_Put_%d", size), func(b *testing.B) {
			for range b.N {
				hashMap := NewOptimizedHashMap(size)
				for j := range size {
					key := fmt.Sprintf("key%d", j)
					hashMap.Put(key, j)
				}
			}
		})

		b.Run(fmt.Sprintf("StandardMap_Put_%d", size), func(b *testing.B) {
			for range b.N {
				m := make(map[string][]int)
				for j := range size {
					key := fmt.Sprintf("key%d", j)
					m[key] = append(m[key], j)
				}
			}
		})
	}
}
