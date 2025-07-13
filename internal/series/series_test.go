package series

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestNewSeries(t *testing.T) {
	mem := memory.NewGoAllocator()

	tests := []struct {
		name           string
		columnName     string
		data           interface{}
		expectedLen    int
		expectedValues interface{}
	}{
		{
			name:           "string series",
			columnName:     "names",
			data:           []string{"alice", "bob", "charlie"},
			expectedLen:    3,
			expectedValues: []string{"alice", "bob", "charlie"},
		},
		{
			name:           "int64 series",
			columnName:     "ages",
			data:           []int64{25, 30, 35},
			expectedLen:    3,
			expectedValues: []int64{25, 30, 35},
		},
		{
			name:           "float64 series",
			columnName:     "scores",
			data:           []float64{85.5, 92.0, 78.3},
			expectedLen:    3,
			expectedValues: []float64{85.5, 92.0, 78.3},
		},
		{
			name:           "bool series",
			columnName:     "active",
			data:           []bool{true, false, true},
			expectedLen:    3,
			expectedValues: []bool{true, false, true},
		},
		{
			name:           "empty string series",
			columnName:     "empty",
			data:           []string{},
			expectedLen:    0,
			expectedValues: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var series interface{}

			switch data := tt.data.(type) {
			case []string:
				series = New(tt.columnName, data, mem)
			case []int64:
				series = New(tt.columnName, data, mem)
			case []float64:
				series = New(tt.columnName, data, mem)
			case []bool:
				series = New(tt.columnName, data, mem)
			}

			// Test basic properties
			switch s := series.(type) {
			case *Series[string]:
				defer s.Release()
				assert.Equal(t, tt.columnName, s.Name())
				assert.Equal(t, tt.expectedLen, s.Len())
				if tt.expectedLen > 0 {
					assert.Equal(t, tt.expectedValues, s.Values())
				}
			case *Series[int64]:
				defer s.Release()
				assert.Equal(t, tt.columnName, s.Name())
				assert.Equal(t, tt.expectedLen, s.Len())
				if tt.expectedLen > 0 {
					assert.Equal(t, tt.expectedValues, s.Values())
				}
			case *Series[float64]:
				defer s.Release()
				assert.Equal(t, tt.columnName, s.Name())
				assert.Equal(t, tt.expectedLen, s.Len())
				if tt.expectedLen > 0 {
					assert.Equal(t, tt.expectedValues, s.Values())
				}
			case *Series[bool]:
				defer s.Release()
				assert.Equal(t, tt.columnName, s.Name())
				assert.Equal(t, tt.expectedLen, s.Len())
				if tt.expectedLen > 0 {
					assert.Equal(t, tt.expectedValues, s.Values())
				}
			}
		})
	}
}

func TestSeriesValue(t *testing.T) {
	mem := memory.NewGoAllocator()

	data := []string{"first", "second", "third"}
	series := New("test", data, mem)
	defer series.Release()

	// Test valid indices
	assert.Equal(t, "first", series.Value(0))
	assert.Equal(t, "second", series.Value(1))
	assert.Equal(t, "third", series.Value(2))

	// Test invalid indices (should return zero value)
	assert.Equal(t, "", series.Value(-1))
	assert.Equal(t, "", series.Value(3))
	assert.Equal(t, "", series.Value(100))
}

func TestSeriesDataType(t *testing.T) {
	mem := memory.NewGoAllocator()

	tests := []struct {
		name      string
		data      interface{}
		typeCheck func(interface{}) bool
	}{
		{
			name: "string series has string type",
			data: []string{"a", "b"},
			typeCheck: func(s interface{}) bool {
				series := s.(*Series[string])
				return series.DataType().Name() == "utf8"
			},
		},
		{
			name: "int64 series has int64 type",
			data: []int64{1, 2},
			typeCheck: func(s interface{}) bool {
				series := s.(*Series[int64])
				return series.DataType().Name() == "int64"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var series interface{}

			switch data := tt.data.(type) {
			case []string:
				series = New("test", data, mem)
				defer series.(*Series[string]).Release()
			case []int64:
				series = New("test", data, mem)
				defer series.(*Series[int64]).Release()
			}

			assert.True(t, tt.typeCheck(series))
		})
	}
}

func TestSeriesString(t *testing.T) {
	mem := memory.NewGoAllocator()

	series := New("test_column", []string{"a", "b", "c"}, mem)
	defer series.Release()

	str := series.String()
	assert.Contains(t, str, "Series[string]")
	assert.Contains(t, str, "test_column")
	assert.Contains(t, str, "len=3")
}

func TestSeriesIsNull(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Test with regular data (no nulls)
	series := New("test", []string{"a", "b", "c"}, mem)
	defer series.Release()

	// All values should be non-null
	for i := 0; i < series.Len(); i++ {
		assert.False(t, series.IsNull(i))
	}
}

func TestUnsupportedType(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Test that unsupported types panic
	assert.Panics(t, func() {
		New("test", []complex64{1 + 2i, 3 + 4i}, mem)
	})
}
