package series_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
)

func TestNewSeries(t *testing.T) {
	mem := memory.NewGoAllocator()
	tests := createNewSeriesTestCases()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := createSeriesFromTestData(tt.columnName, tt.data, mem)
			validateSeriesResult(t, result, tt)
		})
	}
}

// newSeriesTestCase represents a test case for series creation.
type newSeriesTestCase struct {
	name           string
	columnName     string
	data           interface{}
	expectedLen    int
	expectedValues interface{}
}

// createNewSeriesTestCases creates all test cases for series creation.
func createNewSeriesTestCases() []newSeriesTestCase {
	return []newSeriesTestCase{
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
}

// createSeriesFromTestData creates a series from test data based on type.
func createSeriesFromTestData(columnName string, data interface{}, mem memory.Allocator) interface{} {
	switch typedData := data.(type) {
	case []string:
		return series.New(columnName, typedData, mem)
	case []int64:
		return series.New(columnName, typedData, mem)
	case []float64:
		return series.New(columnName, typedData, mem)
	case []bool:
		return series.New(columnName, typedData, mem)
	default:
		return nil
	}
}

// validateSeriesResult validates the created series against expected values.
func validateSeriesResult(t *testing.T, result interface{}, testCase newSeriesTestCase) {
	switch s := result.(type) {
	case *series.Series[string]:
		validateTypedSeries(t, s, testCase)
	case *series.Series[int64]:
		validateTypedSeries(t, s, testCase)
	case *series.Series[float64]:
		validateTypedSeries(t, s, testCase)
	case *series.Series[bool]:
		validateTypedSeries(t, s, testCase)
	default:
		t.Fatalf("Unexpected series type: %T", result)
	}
}

// validateTypedSeries validates a typed series against expected values.
func validateTypedSeries[T any](t *testing.T, s *series.Series[T], testCase newSeriesTestCase) {
	defer s.Release()

	assert.Equal(t, testCase.columnName, s.Name())
	assert.Equal(t, testCase.expectedLen, s.Len())

	if testCase.expectedLen > 0 {
		assert.Equal(t, testCase.expectedValues, s.Values())
	}
}

func TestSeriesValue(t *testing.T) {
	mem := memory.NewGoAllocator()

	data := []string{"first", "second", "third"}
	series := series.New("test", data, mem)
	defer series.Release()

	// Test valid indices
	assert.Equal(t, "first", series.Value(0))
	assert.Equal(t, "second", series.Value(1))
	assert.Equal(t, "third", series.Value(2))

	// Test invalid indices (should return zero value)
	assert.Empty(t, series.Value(-1))
	assert.Empty(t, series.Value(3))
	assert.Empty(t, series.Value(100))
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
				ser := s.(*series.Series[string])
				return ser.DataType().Name() == "utf8"
			},
		},
		{
			name: "int64 series has int64 type",
			data: []int64{1, 2},
			typeCheck: func(s interface{}) bool {
				ser := s.(*series.Series[int64])
				return ser.DataType().Name() == "int64"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result interface{}

			switch data := tt.data.(type) {
			case []string:
				result = series.New("test", data, mem)
				defer result.(*series.Series[string]).Release()
			case []int64:
				result = series.New("test", data, mem)
				defer result.(*series.Series[int64]).Release()
			}

			assert.True(t, tt.typeCheck(result))
		})
	}
}

func TestSeriesString(t *testing.T) {
	mem := memory.NewGoAllocator()

	series := series.New("test_column", []string{"a", "b", "c"}, mem)
	defer series.Release()

	str := series.String()
	assert.Contains(t, str, "Series[string]")
	assert.Contains(t, str, "test_column")
	assert.Contains(t, str, "len=3")
}

func TestSeriesIsNull(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Test with regular data (no nulls)
	series := series.New("test", []string{"a", "b", "c"}, mem)
	defer series.Release()

	// All values should be non-null
	for i := range series.Len() {
		assert.False(t, series.IsNull(i))
	}
}

func TestUnsupportedType(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Test that unsupported types panic
	assert.Panics(t, func() {
		series.New("test", []complex64{1 + 2i, 3 + 4i}, mem)
	})
}
