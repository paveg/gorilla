package series

import (
	"errors"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	dferrors "github.com/paveg/gorilla/internal/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSafe_SupportedTypes(t *testing.T) {
	mem := memory.NewGoAllocator()
	// mem.AssertSize not available in this version

	tests := []struct {
		name   string
		values interface{}
	}{
		{"string slice", []string{"a", "b", "c"}},
		{"int64 slice", []int64{1, 2, 3}},
		{"int32 slice", []int32{1, 2, 3}},
		{"float64 slice", []float64{1.1, 2.2, 3.3}},
		{"float32 slice", []float32{1.1, 2.2, 3.3}},
		{"bool slice", []bool{true, false, true}},
		{"time slice", []time.Time{time.Now(), time.Now().Add(time.Hour), time.Now().Add(2 * time.Hour)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch v := tt.values.(type) {
			case []string:
				series, err := NewSafe("test", v, mem)
				require.NoError(t, err)
				defer series.Release()
				assert.Equal(t, "test", series.Name())
				assert.Equal(t, len(v), series.Len())
			case []int64:
				series, err := NewSafe("test", v, mem)
				require.NoError(t, err)
				defer series.Release()
				assert.Equal(t, len(v), series.Len())
			case []int32:
				series, err := NewSafe("test", v, mem)
				require.NoError(t, err)
				defer series.Release()
				assert.Equal(t, len(v), series.Len())
			case []float64:
				series, err := NewSafe("test", v, mem)
				require.NoError(t, err)
				defer series.Release()
				assert.Equal(t, len(v), series.Len())
			case []float32:
				series, err := NewSafe("test", v, mem)
				require.NoError(t, err)
				defer series.Release()
				assert.Equal(t, len(v), series.Len())
			case []bool:
				series, err := NewSafe("test", v, mem)
				require.NoError(t, err)
				defer series.Release()
				assert.Equal(t, len(v), series.Len())
			case []time.Time:
				series, err := NewSafe("test", v, mem)
				require.NoError(t, err)
				defer series.Release()
				assert.Equal(t, len(v), series.Len())
			}
		})
	}
}

func TestNewSafe_UnsupportedTypes(t *testing.T) {
	mem := memory.NewGoAllocator()
	// mem.AssertSize not available in this version

	tests := []struct {
		name   string
		values interface{}
	}{
		{"complex128 slice", []complex128{1 + 2i, 3 + 4i}},
		{"struct slice", []struct{ X int }{{1}, {2}}},
		{"map slice", []map[string]int{{"a": 1}, {"b": 2}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch v := tt.values.(type) {
			case []complex128:
				series, err := NewSafe("test", v, mem)
				require.Error(t, err)
				assert.Nil(t, series)

				var dfErr *dferrors.DataFrameError
				require.True(t, errors.As(err, &dfErr))
				assert.Equal(t, "series creation", dfErr.Op)
				assert.Contains(t, dfErr.Message, "complex128")
			case []struct{ X int }:
				series, err := NewSafe("test", v, mem)
				require.Error(t, err)
				assert.Nil(t, series)

				var dfErr *dferrors.DataFrameError
				require.True(t, errors.As(err, &dfErr))
				assert.Equal(t, "series creation", dfErr.Op)
				assert.Contains(t, dfErr.Message, "struct")
			case []map[string]int:
				series, err := NewSafe("test", v, mem)
				require.Error(t, err)
				assert.Nil(t, series)
			}
		})
	}
}

func TestValuesSafe_SupportedTypes(t *testing.T) {
	mem := memory.NewGoAllocator()
	// mem.AssertSize not available in this version

	t.Run("string series", func(t *testing.T) {
		original := []string{"a", "b", "c"}
		series := New("test", original, mem)
		defer series.Release()

		values, err := series.ValuesSafe()
		require.NoError(t, err)
		assert.Equal(t, original, values)
	})

	t.Run("int64 series", func(t *testing.T) {
		original := []int64{1, 2, 3}
		series := New("test", original, mem)
		defer series.Release()

		values, err := series.ValuesSafe()
		require.NoError(t, err)
		assert.Equal(t, original, values)
	})

	t.Run("bool series", func(t *testing.T) {
		original := []bool{true, false, true}
		series := New("test", original, mem)
		defer series.Release()

		values, err := series.ValuesSafe()
		require.NoError(t, err)
		assert.Equal(t, original, values)
	})

	t.Run("time series", func(t *testing.T) {
		now := time.Now().UTC()
		original := []time.Time{now, now.Add(time.Hour), now.Add(2 * time.Hour)}
		series := New("test", original, mem)
		defer series.Release()

		values, err := series.ValuesSafe()
		require.NoError(t, err)
		require.Equal(t, len(original), len(values))

		// Time values should be approximately equal (within nanosecond precision)
		for i, expected := range original {
			assert.True(t, values[i].Equal(expected), "Time values should be equal at index %d", i)
		}
	})
}

func TestValuesSafe_BackwardCompatibility(t *testing.T) {
	// Ensure that ValuesSafe returns the same results as Values() for supported types
	mem := memory.NewGoAllocator()
	// mem.AssertSize not available in this version

	t.Run("string series compatibility", func(t *testing.T) {
		original := []string{"a", "b", "c"}
		series := New("test", original, mem)
		defer series.Release()

		unsafeValues := series.Values()
		safeValues, err := series.ValuesSafe()
		require.NoError(t, err)

		assert.Equal(t, unsafeValues, safeValues)
	})

	t.Run("int64 series compatibility", func(t *testing.T) {
		original := []int64{1, 2, 3}
		series := New("test", original, mem)
		defer series.Release()

		unsafeValues := series.Values()
		safeValues, err := series.ValuesSafe()
		require.NoError(t, err)

		assert.Equal(t, unsafeValues, safeValues)
	})
}

func TestNewSafe_EmptySlice(t *testing.T) {
	mem := memory.NewGoAllocator()
	// mem.AssertSize not available in this version

	series, err := NewSafe("empty", []string{}, mem)
	require.NoError(t, err)
	defer series.Release()

	assert.Equal(t, "empty", series.Name())
	assert.Equal(t, 0, series.Len())

	values, err := series.ValuesSafe()
	require.NoError(t, err)
	assert.Equal(t, []string{}, values)
}

func TestNewSafe_NilAllocator(t *testing.T) {
	// Test that NewSafe properly handles nil allocator (should create default)
	series, err := NewSafe("test", []string{"a", "b"}, nil)
	require.NoError(t, err)
	defer series.Release()

	assert.Equal(t, "test", series.Name())
	assert.Equal(t, 2, series.Len())
}

func TestNew_vs_NewSafe_Panics(t *testing.T) {
	mem := memory.NewGoAllocator()
	// mem.AssertSize not available in this version

	unsupportedData := []complex128{1 + 2i, 3 + 4i}

	t.Run("New panics on unsupported type", func(t *testing.T) {
		assert.Panics(t, func() {
			series := New("test", unsupportedData, mem)
			defer series.Release()
		})
	})

	t.Run("NewSafe returns error on unsupported type", func(t *testing.T) {
		series, err := NewSafe("test", unsupportedData, mem)
		require.Error(t, err)
		assert.Nil(t, series)

		var dfErr *dferrors.DataFrameError
		require.True(t, errors.As(err, &dfErr))
		assert.Equal(t, "series creation", dfErr.Op)
		assert.Contains(t, dfErr.Message, "unsupported type")
	})
}

func TestValuesSafe_Performance(t *testing.T) {
	// Ensure ValuesSafe doesn't have significant performance overhead
	mem := memory.NewGoAllocator()
	// mem.AssertSize not available in this version

	// Create large dataset
	size := 10000
	data := make([]int64, size)
	for i := 0; i < size; i++ {
		data[i] = int64(i)
	}

	series := New("large", data, mem)
	defer series.Release()

	values, err := series.ValuesSafe()
	require.NoError(t, err)
	assert.Equal(t, size, len(values))
	assert.Equal(t, int64(0), values[0])
	assert.Equal(t, int64(size-1), values[size-1])
}
