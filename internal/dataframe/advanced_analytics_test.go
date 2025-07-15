package dataframe

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataFrameCorrelation(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data for correlation
	x := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	y := []float64{2.0, 4.0, 6.0, 8.0, 10.0} // Perfect positive correlation
	z := []float64{5.0, 4.0, 3.0, 2.0, 1.0}  // Perfect negative correlation

	df := New(
		series.New("x", x, mem),
		series.New("y", y, mem),
		series.New("z", z, mem),
	)
	defer df.Release()

	tests := []struct {
		name     string
		col1     string
		col2     string
		expected float64
		epsilon  float64
	}{
		{
			name:     "perfect positive correlation",
			col1:     "x",
			col2:     "y",
			expected: 1.0,
			epsilon:  0.0001,
		},
		{
			name:     "perfect negative correlation",
			col1:     "x",
			col2:     "z",
			expected: -1.0,
			epsilon:  0.0001,
		},
		{
			name:     "self correlation",
			col1:     "x",
			col2:     "x",
			expected: 1.0,
			epsilon:  0.0001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := df.Correlation(tt.col1, tt.col2)
			require.NoError(t, err)
			assert.InDelta(t, tt.expected, result, tt.epsilon)
		})
	}
}

func TestDataFrameCorrelationErrors(t *testing.T) {
	mem := memory.NewGoAllocator()

	x := []float64{1.0, 2.0, 3.0}
	y := []string{"a", "b", "c"}

	df := New(
		series.New("x", x, mem),
		series.New("y", y, mem),
	)
	defer df.Release()

	tests := []struct {
		name      string
		col1      string
		col2      string
		expectErr bool
	}{
		{
			name:      "missing column",
			col1:      "x",
			col2:      "nonexistent",
			expectErr: true,
		},
		{
			name:      "non-numeric column",
			col1:      "x",
			col2:      "y",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := df.Correlation(tt.col1, tt.col2)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDataFrameRollingWindow(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data for rolling window
	values := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}

	df := New(
		series.New("values", values, mem),
	)
	defer df.Release()

	tests := []struct {
		name       string
		column     string
		windowSize int
		operation  string
		expected   []float64
	}{
		{
			name:       "rolling mean window size 3",
			column:     "values",
			windowSize: 3,
			operation:  "mean",
			expected:   []float64{math.NaN(), math.NaN(), 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0},
		},
		{
			name:       "rolling sum window size 2",
			column:     "values",
			windowSize: 2,
			operation:  "sum",
			expected:   []float64{math.NaN(), 3.0, 5.0, 7.0, 9.0, 11.0, 13.0, 15.0, 17.0, 19.0},
		},
		{
			name:       "rolling max window size 3",
			column:     "values",
			windowSize: 3,
			operation:  "max",
			expected:   []float64{math.NaN(), math.NaN(), 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := df.RollingWindow(tt.column, tt.windowSize, tt.operation)
			require.NoError(t, err)
			defer result.Release()

			resultCol, exists := result.Column(tt.column + "_" + tt.operation)
			require.True(t, exists)

			arr := resultCol.Array()
			defer arr.Release()

			require.Equal(t, len(tt.expected), arr.Len())

			for i := 0; i < arr.Len(); i++ {
				expected := tt.expected[i]
				if math.IsNaN(expected) {
					assert.True(t, arr.IsNull(i) || math.IsNaN(arr.(*array.Float64).Value(i)))
				} else {
					assert.InDelta(t, expected, arr.(*array.Float64).Value(i), 0.0001)
				}
			}
		})
	}
}

func TestDataFrameWindowFunctions(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data for window functions
	values := []float64{3.0, 1.0, 4.0, 1.0, 5.0, 9.0, 2.0, 6.0}
	groups := []string{"A", "A", "B", "B", "A", "B", "A", "B"}

	df := New(
		series.New("values", values, mem),
		series.New("groups", groups, mem),
	)
	defer df.Release()

	t.Run("row_number function", func(t *testing.T) {
		result, err := df.WindowFunction("row_number")
		require.NoError(t, err)
		defer result.Release()

		rowNumCol, exists := result.Column("row_number")
		require.True(t, exists)

		arr := rowNumCol.Array()
		defer arr.Release()

		expected := []int64{1, 2, 3, 4, 5, 6, 7, 8}
		require.Equal(t, len(expected), arr.Len())

		for i := 0; i < arr.Len(); i++ {
			assert.Equal(t, expected[i], arr.(*array.Int64).Value(i))
		}
	})

	t.Run("rank function", func(t *testing.T) {
		result, err := df.WindowFunction("rank", "values")
		require.NoError(t, err)
		defer result.Release()

		rankCol, exists := result.Column("rank")
		require.True(t, exists)

		arr := rankCol.Array()
		defer arr.Release()

		// Expected ranks: 3,1,4,1,5,9,2,6 becomes 4,1,5,1,6,8,3,7
		expected := []int64{4, 1, 5, 1, 6, 8, 3, 7}
		require.Equal(t, len(expected), arr.Len())

		for i := 0; i < arr.Len(); i++ {
			assert.Equal(t, expected[i], arr.(*array.Int64).Value(i))
		}
	})

	t.Run("lag function", func(t *testing.T) {
		result, err := df.WindowFunction("lag", "values")
		require.NoError(t, err)
		defer result.Release()

		lagCol, exists := result.Column("lag")
		require.True(t, exists)

		arr := lagCol.Array()
		defer arr.Release()

		// First value should be null/NaN, then shifted values
		require.True(t, arr.IsNull(0))
		for i := 1; i < arr.Len(); i++ {
			expected := values[i-1]
			assert.InDelta(t, expected, arr.(*array.Float64).Value(i), 0.0001)
		}
	})

	t.Run("lead function", func(t *testing.T) {
		result, err := df.WindowFunction("lead", "values")
		require.NoError(t, err)
		defer result.Release()

		leadCol, exists := result.Column("lead")
		require.True(t, exists)

		arr := leadCol.Array()
		defer arr.Release()

		// Shifted values, last value should be null/NaN
		for i := 0; i < arr.Len()-1; i++ {
			expected := values[i+1]
			assert.InDelta(t, expected, arr.(*array.Float64).Value(i), 0.0001)
		}
		require.True(t, arr.IsNull(arr.Len()-1))
	})
}

func TestDataFrameWindowFunctionWithPartition(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data for partitioned window functions
	values := []float64{3.0, 1.0, 4.0, 1.0, 5.0, 9.0, 2.0, 6.0}
	groups := []string{"A", "A", "B", "B", "A", "B", "A", "B"}

	df := New(
		series.New("values", values, mem),
		series.New("groups", groups, mem),
	)
	defer df.Release()

	t.Run("row_number with partition", func(t *testing.T) {
		result, err := df.WindowFunctionWithPartition("row_number", "", "groups")
		require.NoError(t, err)
		defer result.Release()

		rowNumCol, exists := result.Column("row_number")
		require.True(t, exists)

		arr := rowNumCol.Array()
		defer arr.Release()

		// Expected row numbers within each group
		// Group A: rows 0,1,4,6 -> 1,2,3,4
		// Group B: rows 2,3,5,7 -> 1,2,3,4
		expected := []int64{1, 2, 1, 2, 3, 3, 4, 4}
		require.Equal(t, len(expected), arr.Len())

		for i := 0; i < arr.Len(); i++ {
			assert.Equal(t, expected[i], arr.(*array.Int64).Value(i))
		}
	})
}
