package expr

import (
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComparators(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("Int64Comparator", func(t *testing.T) {
		builder := array.NewInt64Builder(mem)
		defer builder.Release()

		builder.AppendValues([]int64{30, 10, 20}, []bool{true, true, true})
		builder.AppendNull()
		arr := builder.NewArray()
		defer arr.Release()

		comp := &Int64Comparator{ascending: true}

		// Test comparisons
		assert.Equal(t, 1, comp.Compare(arr, 0, 1))  // 30 > 10
		assert.Equal(t, -1, comp.Compare(arr, 1, 0)) // 10 < 30
		assert.Equal(t, 0, comp.Compare(arr, 0, 0))  // 30 == 30
		assert.Equal(t, 1, comp.Compare(arr, 0, 3))  // non-null > null
		assert.Equal(t, -1, comp.Compare(arr, 3, 0)) // null < non-null

		// Test descending
		compDesc := &Int64Comparator{ascending: false}
		assert.Equal(t, -1, compDesc.Compare(arr, 0, 1)) // 30 > 10 (desc)
		assert.Equal(t, 1, compDesc.Compare(arr, 1, 0))  // 10 < 30 (desc)
	})

	t.Run("Float64Comparator", func(t *testing.T) {
		builder := array.NewFloat64Builder(mem)
		defer builder.Release()

		builder.AppendValues([]float64{3.14, 1.41, 2.71}, []bool{true, true, true})
		arr := builder.NewArray()
		defer arr.Release()

		comp := &Float64Comparator{ascending: true}

		assert.Equal(t, 1, comp.Compare(arr, 0, 1))  // 3.14 > 1.41
		assert.Equal(t, -1, comp.Compare(arr, 1, 2)) // 1.41 < 2.71
		assert.Equal(t, 0, comp.Compare(arr, 0, 0))  // 3.14 == 3.14
	})

	t.Run("StringComparator", func(t *testing.T) {
		builder := array.NewStringBuilder(mem)
		defer builder.Release()

		builder.AppendValues([]string{"charlie", "alice", "bob"}, []bool{true, true, true})
		arr := builder.NewArray()
		defer arr.Release()

		comp := &StringComparator{ascending: true}

		assert.Equal(t, 1, comp.Compare(arr, 0, 1))  // "charlie" > "alice"
		assert.Equal(t, -1, comp.Compare(arr, 1, 2)) // "alice" < "bob"
		assert.Equal(t, 0, comp.Compare(arr, 0, 0))  // "charlie" == "charlie"
	})

	t.Run("BooleanComparator", func(t *testing.T) {
		builder := array.NewBooleanBuilder(mem)
		defer builder.Release()

		builder.AppendValues([]bool{true, false, true}, []bool{true, true, true})
		arr := builder.NewArray()
		defer arr.Release()

		comp := &BooleanComparator{ascending: true}

		assert.Equal(t, 1, comp.Compare(arr, 0, 1))  // true > false
		assert.Equal(t, -1, comp.Compare(arr, 1, 0)) // false < true
		assert.Equal(t, 0, comp.Compare(arr, 0, 2))  // true == true
	})
}

func TestSortPartitionOptimized(t *testing.T) {
	mem := memory.NewGoAllocator()
	evaluator := NewEvaluator(mem)

	t.Run("EmptyPartition", func(t *testing.T) {
		result := evaluator.sortPartitionOptimized([]int{}, []OrderByExpr{}, nil)
		assert.Empty(t, result)
	})

	t.Run("SingleElement", func(t *testing.T) {
		result := evaluator.sortPartitionOptimized([]int{0}, []OrderByExpr{}, nil)
		assert.Equal(t, []int{0}, result)
	})

	t.Run("SimpleSorting", func(t *testing.T) {
		// Create test data
		valueBuilder := array.NewInt64Builder(mem)
		defer valueBuilder.Release()
		valueBuilder.AppendValues([]int64{30, 10, 20, 40}, nil)
		valueArr := valueBuilder.NewArray()
		defer valueArr.Release()

		columns := map[string]arrow.Array{
			"value": valueArr,
		}

		partition := []int{0, 1, 2, 3}
		orderBy := []OrderByExpr{
			{column: "value", ascending: true},
		}

		result := evaluator.sortPartitionOptimized(partition, orderBy, columns)
		expected := []int{1, 2, 0, 3} // indices sorted by value: 10, 20, 30, 40
		assert.Equal(t, expected, result)
	})

	t.Run("MultiColumnSorting", func(t *testing.T) {
		// Create test data
		categoryBuilder := array.NewStringBuilder(mem)
		defer categoryBuilder.Release()
		categoryBuilder.AppendValues([]string{"B", "A", "B", "A"}, nil)
		categoryArr := categoryBuilder.NewArray()
		defer categoryArr.Release()

		valueBuilder := array.NewInt64Builder(mem)
		defer valueBuilder.Release()
		valueBuilder.AppendValues([]int64{30, 20, 10, 40}, nil)
		valueArr := valueBuilder.NewArray()
		defer valueArr.Release()

		columns := map[string]arrow.Array{
			"category": categoryArr,
			"value":    valueArr,
		}

		partition := []int{0, 1, 2, 3}
		orderBy := []OrderByExpr{
			{column: "category", ascending: true},
			{column: "value", ascending: true},
		}

		result := evaluator.sortPartitionOptimized(partition, orderBy, columns)
		expected := []int{1, 3, 2, 0} // A,20 -> A,40 -> B,10 -> B,30
		assert.Equal(t, expected, result)
	})

	t.Run("LargePartition", func(t *testing.T) {
		// Create large dataset
		size := 10000
		values := make([]int64, size)
		indices := make([]int, size)
		for i := 0; i < size; i++ {
			values[i] = rand.Int63n(1000) //nolint:gosec // Weak random OK for benchmarks
			indices[i] = i
		}

		valueBuilder := array.NewInt64Builder(mem)
		defer valueBuilder.Release()
		valueBuilder.AppendValues(values, nil)
		valueArr := valueBuilder.NewArray()
		defer valueArr.Release()

		columns := map[string]arrow.Array{
			"value": valueArr,
		}

		orderBy := []OrderByExpr{
			{column: "value", ascending: true},
		}

		result := evaluator.sortPartitionOptimized(indices, orderBy, columns)

		// Verify sorting is correct
		require.Len(t, result, size)
		for i := 1; i < len(result); i++ {
			v1 := values[result[i-1]]
			v2 := values[result[i]]
			assert.LessOrEqual(t, v1, v2, "Values should be in ascending order")
		}
	})
}

func TestSortPartitionWithKeys(t *testing.T) {
	mem := memory.NewGoAllocator()
	evaluator := NewEvaluator(mem)

	t.Run("PrecomputedKeys", func(t *testing.T) {
		// Create test data with nulls
		valueBuilder := array.NewInt64Builder(mem)
		defer valueBuilder.Release()
		valueBuilder.AppendValues([]int64{30, 10, 20}, []bool{true, true, true})
		valueBuilder.AppendNull()
		valueBuilder.Append(40)
		valueArr := valueBuilder.NewArray()
		defer valueArr.Release()

		columns := map[string]arrow.Array{
			"value": valueArr,
		}

		partition := []int{0, 1, 2, 3, 4}
		orderBy := []OrderByExpr{
			{column: "value", ascending: true},
		}

		result := evaluator.sortPartitionWithKeys(partition, orderBy, columns)
		expected := []int{3, 1, 2, 0, 4} // null, 10, 20, 30, 40
		assert.Equal(t, expected, result)
	})
}

func TestSortPartitionParallel(t *testing.T) {
	mem := memory.NewGoAllocator()
	evaluator := NewEvaluator(mem)

	t.Run("ParallelSorting", func(t *testing.T) {
		// Create large dataset that triggers parallel sorting
		size := parallelSortThreshold * 3
		values := make([]int64, size)
		indices := make([]int, size)
		for i := 0; i < size; i++ {
			values[i] = rand.Int63n(10000) //nolint:gosec // Weak random OK for benchmarks
			indices[i] = i
		}

		valueBuilder := array.NewInt64Builder(mem)
		defer valueBuilder.Release()
		valueBuilder.AppendValues(values, nil)
		valueArr := valueBuilder.NewArray()
		defer valueArr.Release()

		columns := map[string]arrow.Array{
			"value": valueArr,
		}

		orderBy := []OrderByExpr{
			{column: "value", ascending: true},
		}

		result := evaluator.sortPartitionParallelOptimized(indices, orderBy, columns)

		// Verify sorting is correct
		require.Len(t, result, size)
		for i := 1; i < len(result); i++ {
			v1 := values[result[i-1]]
			v2 := values[result[i]]
			assert.LessOrEqual(t, v1, v2, "Values should be in ascending order")
		}
	})
}

func TestMergeSortedChunks(t *testing.T) {
	mem := memory.NewGoAllocator()
	evaluator := NewEvaluator(mem)

	t.Run("MergeMultipleChunks", func(t *testing.T) {
		// Create test data
		valueBuilder := array.NewInt64Builder(mem)
		defer valueBuilder.Release()
		valueBuilder.AppendValues([]int64{10, 30, 50, 20, 40, 60}, nil)
		valueArr := valueBuilder.NewArray()
		defer valueArr.Release()

		columns := map[string]arrow.Array{
			"value": valueArr,
		}

		// Pre-sorted chunks
		chunks := [][]int{
			{0, 1, 2}, // values: 10, 30, 50
			{3, 4, 5}, // values: 20, 40, 60
		}

		orderBy := []OrderByExpr{
			{column: "value", ascending: true},
		}

		result := evaluator.mergeSortedChunks(chunks, orderBy, columns)
		expected := []int{0, 3, 1, 4, 2, 5} // 10, 20, 30, 40, 50, 60
		assert.Equal(t, expected, result)
	})
}
