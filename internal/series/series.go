// Package series provides data structures for column operations
package series

import (
	"fmt"
	"reflect"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Series represents a typed data column with Apache Arrow backend
type Series[T any] struct {
	name  string
	array arrow.Array
}

// New creates a new Series from a slice of values
func New[T any](name string, values []T, mem memory.Allocator) *Series[T] {
	if mem == nil {
		mem = memory.NewGoAllocator()
	}

	var arr arrow.Array

	// Use type switching to create appropriate Arrow array
	switch v := any(values).(type) {
	case []string:
		builder := array.NewStringBuilder(mem)
		defer builder.Release()
		for _, val := range v {
			builder.Append(val)
		}
		arr = builder.NewArray()
	case []int64:
		builder := array.NewInt64Builder(mem)
		defer builder.Release()
		for _, val := range v {
			builder.Append(val)
		}
		arr = builder.NewArray()
	case []int32:
		builder := array.NewInt32Builder(mem)
		defer builder.Release()
		for _, val := range v {
			builder.Append(val)
		}
		arr = builder.NewArray()
	case []float64:
		builder := array.NewFloat64Builder(mem)
		defer builder.Release()
		for _, val := range v {
			builder.Append(val)
		}
		arr = builder.NewArray()
	case []float32:
		builder := array.NewFloat32Builder(mem)
		defer builder.Release()
		for _, val := range v {
			builder.Append(val)
		}
		arr = builder.NewArray()
	case []bool:
		builder := array.NewBooleanBuilder(mem)
		defer builder.Release()
		for _, val := range v {
			builder.Append(val)
		}
		arr = builder.NewArray()
	default:
		panic(fmt.Sprintf("unsupported type: %T", values))
	}

	return &Series[T]{
		name:  name,
		array: arr,
	}
}

// Name returns the column name
func (s *Series[T]) Name() string {
	return s.name
}

// Len returns the length of the series
func (s *Series[T]) Len() int {
	return s.array.Len()
}

// Values returns the data as a Go slice
func (s *Series[T]) Values() []T {
	result := make([]T, s.array.Len())

	switch arr := s.array.(type) {
	case *array.String:
		if any(result).([]string) != nil {
			values := any(result).([]string)
			for i := 0; i < arr.Len(); i++ {
				values[i] = arr.Value(i)
			}
		}
	case *array.Int64:
		if any(result).([]int64) != nil {
			values := any(result).([]int64)
			for i := 0; i < arr.Len(); i++ {
				values[i] = arr.Value(i)
			}
		}
	case *array.Int32:
		if any(result).([]int32) != nil {
			values := any(result).([]int32)
			for i := 0; i < arr.Len(); i++ {
				values[i] = arr.Value(i)
			}
		}
	case *array.Float64:
		if any(result).([]float64) != nil {
			values := any(result).([]float64)
			for i := 0; i < arr.Len(); i++ {
				values[i] = arr.Value(i)
			}
		}
	case *array.Float32:
		if any(result).([]float32) != nil {
			values := any(result).([]float32)
			for i := 0; i < arr.Len(); i++ {
				values[i] = arr.Value(i)
			}
		}
	case *array.Boolean:
		if any(result).([]bool) != nil {
			values := any(result).([]bool)
			for i := 0; i < arr.Len(); i++ {
				values[i] = arr.Value(i)
			}
		}
	default:
		panic(fmt.Sprintf("unsupported array type: %T", arr))
	}

	return result
}

// Value returns the value at the given index
func (s *Series[T]) Value(index int) T {
	if index < 0 || index >= s.array.Len() {
		var zero T
		return zero
	}

	var result T

	switch arr := s.array.(type) {
	case *array.String:
		if v, ok := any(&result).(*string); ok {
			*v = arr.Value(index)
		}
	case *array.Int64:
		if v, ok := any(&result).(*int64); ok {
			*v = arr.Value(index)
		}
	case *array.Int32:
		if v, ok := any(&result).(*int32); ok {
			*v = arr.Value(index)
		}
	case *array.Float64:
		if v, ok := any(&result).(*float64); ok {
			*v = arr.Value(index)
		}
	case *array.Float32:
		if v, ok := any(&result).(*float32); ok {
			*v = arr.Value(index)
		}
	case *array.Boolean:
		if v, ok := any(&result).(*bool); ok {
			*v = arr.Value(index)
		}
	}

	return result
}

// DataType returns the Arrow data type
func (s *Series[T]) DataType() arrow.DataType {
	return s.array.DataType()
}

// IsNull checks if the value at index is null
func (s *Series[T]) IsNull(index int) bool {
	return s.array.IsNull(index)
}

// String returns a string representation of the series
func (s *Series[T]) String() string {
	return fmt.Sprintf("Series[%s]: %s (len=%d)",
		reflect.TypeOf(new(T)).Elem().Name(),
		s.name,
		s.Len())
}

// Array returns the underlying Arrow array (retains a reference)
func (s *Series[T]) Array() arrow.Array {
	if s.array != nil {
		s.array.Retain()
		return s.array
	}
	return nil
}

// Release releases the underlying Arrow memory
func (s *Series[T]) Release() {
	if s.array != nil {
		s.array.Release()
	}
}
