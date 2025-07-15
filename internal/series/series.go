// Package series provides data structures for column operations
package series

import (
	"fmt"
	"reflect"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/errors"
)

const (
	nanosPerSecond = 1e9
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
	case []time.Time:
		// Use timestamp with nanosecond precision and timezone support
		builder := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Nanosecond})
		defer builder.Release()
		for _, val := range v {
			// Convert time.Time to nanoseconds since Unix epoch (in UTC)
			timestamp := arrow.Timestamp(val.UTC().UnixNano())
			builder.Append(timestamp)
		}
		arr = builder.NewArray()
	case []int16:
		builder := array.NewInt16Builder(mem)
		defer builder.Release()
		for _, val := range v {
			builder.Append(val)
		}
		arr = builder.NewArray()
	case []int8:
		builder := array.NewInt8Builder(mem)
		defer builder.Release()
		for _, val := range v {
			builder.Append(val)
		}
		arr = builder.NewArray()
	case []uint64:
		builder := array.NewUint64Builder(mem)
		defer builder.Release()
		for _, val := range v {
			builder.Append(val)
		}
		arr = builder.NewArray()
	case []uint32:
		builder := array.NewUint32Builder(mem)
		defer builder.Release()
		for _, val := range v {
			builder.Append(val)
		}
		arr = builder.NewArray()
	case []uint16:
		builder := array.NewUint16Builder(mem)
		defer builder.Release()
		for _, val := range v {
			builder.Append(val)
		}
		arr = builder.NewArray()
	case []uint8:
		builder := array.NewUint8Builder(mem)
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

// NewSafe creates a new Series from a slice of values with error handling
// This is the preferred method for production code as it returns errors instead of panicking
func NewSafe[T any](name string, values []T, mem memory.Allocator) (*Series[T], error) {
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
	case []time.Time:
		builder := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Nanosecond})
		defer builder.Release()
		for _, val := range v {
			// Convert time.Time to nanoseconds since Unix epoch (in UTC)
			timestamp := arrow.Timestamp(val.UTC().UnixNano())
			builder.Append(timestamp)
		}
		arr = builder.NewArray()
	case []int16:
		builder := array.NewInt16Builder(mem)
		defer builder.Release()
		for _, val := range v {
			builder.Append(val)
		}
		arr = builder.NewArray()
	case []int8:
		builder := array.NewInt8Builder(mem)
		defer builder.Release()
		for _, val := range v {
			builder.Append(val)
		}
		arr = builder.NewArray()
	case []uint64:
		builder := array.NewUint64Builder(mem)
		defer builder.Release()
		for _, val := range v {
			builder.Append(val)
		}
		arr = builder.NewArray()
	case []uint32:
		builder := array.NewUint32Builder(mem)
		defer builder.Release()
		for _, val := range v {
			builder.Append(val)
		}
		arr = builder.NewArray()
	case []uint16:
		builder := array.NewUint16Builder(mem)
		defer builder.Release()
		for _, val := range v {
			builder.Append(val)
		}
		arr = builder.NewArray()
	case []uint8:
		builder := array.NewUint8Builder(mem)
		defer builder.Release()
		for _, val := range v {
			builder.Append(val)
		}
		arr = builder.NewArray()
	default:
		return nil, errors.NewUnsupportedTypeError("series creation", fmt.Sprintf("%T", values))
	}

	return &Series[T]{
		name:  name,
		array: arr,
	}, nil
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
	case *array.Timestamp:
		if any(result).([]time.Time) != nil {
			values := any(result).([]time.Time)
			for i := 0; i < arr.Len(); i++ {
				// Convert Arrow timestamp (nanoseconds since Unix epoch) back to time.Time in UTC
				timestamp := arr.Value(i)
				nanos := int64(timestamp)
				values[i] = time.Unix(nanos/nanosPerSecond, nanos%nanosPerSecond).UTC()
			}
		}
	case *array.Int16:
		if any(result).([]int16) != nil {
			values := any(result).([]int16)
			for i := 0; i < arr.Len(); i++ {
				values[i] = arr.Value(i)
			}
		}
	case *array.Int8:
		if any(result).([]int8) != nil {
			values := any(result).([]int8)
			for i := 0; i < arr.Len(); i++ {
				values[i] = arr.Value(i)
			}
		}
	case *array.Uint64:
		if any(result).([]uint64) != nil {
			values := any(result).([]uint64)
			for i := 0; i < arr.Len(); i++ {
				values[i] = arr.Value(i)
			}
		}
	case *array.Uint32:
		if any(result).([]uint32) != nil {
			values := any(result).([]uint32)
			for i := 0; i < arr.Len(); i++ {
				values[i] = arr.Value(i)
			}
		}
	case *array.Uint16:
		if any(result).([]uint16) != nil {
			values := any(result).([]uint16)
			for i := 0; i < arr.Len(); i++ {
				values[i] = arr.Value(i)
			}
		}
	case *array.Uint8:
		if any(result).([]uint8) != nil {
			values := any(result).([]uint8)
			for i := 0; i < arr.Len(); i++ {
				values[i] = arr.Value(i)
			}
		}
	default:
		panic(fmt.Sprintf("unsupported array type: %T", arr))
	}

	return result
}

// ValuesSafe returns the data as a Go slice with error handling
// This is the preferred method for production code as it returns errors instead of panicking
func (s *Series[T]) ValuesSafe() ([]T, error) {
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
	case *array.Timestamp:
		if any(result).([]time.Time) != nil {
			values := any(result).([]time.Time)
			for i := 0; i < arr.Len(); i++ {
				// Convert Arrow timestamp (nanoseconds since Unix epoch) back to time.Time in UTC
				timestamp := arr.Value(i)
				nanos := int64(timestamp)
				values[i] = time.Unix(nanos/nanosPerSecond, nanos%nanosPerSecond).UTC()
			}
		}
	case *array.Int16:
		if any(result).([]int16) != nil {
			values := any(result).([]int16)
			for i := 0; i < arr.Len(); i++ {
				values[i] = arr.Value(i)
			}
		}
	case *array.Int8:
		if any(result).([]int8) != nil {
			values := any(result).([]int8)
			for i := 0; i < arr.Len(); i++ {
				values[i] = arr.Value(i)
			}
		}
	case *array.Uint64:
		if any(result).([]uint64) != nil {
			values := any(result).([]uint64)
			for i := 0; i < arr.Len(); i++ {
				values[i] = arr.Value(i)
			}
		}
	case *array.Uint32:
		if any(result).([]uint32) != nil {
			values := any(result).([]uint32)
			for i := 0; i < arr.Len(); i++ {
				values[i] = arr.Value(i)
			}
		}
	case *array.Uint16:
		if any(result).([]uint16) != nil {
			values := any(result).([]uint16)
			for i := 0; i < arr.Len(); i++ {
				values[i] = arr.Value(i)
			}
		}
	case *array.Uint8:
		if any(result).([]uint8) != nil {
			values := any(result).([]uint8)
			for i := 0; i < arr.Len(); i++ {
				values[i] = arr.Value(i)
			}
		}
	default:
		return nil, errors.NewUnsupportedTypeError("values extraction", fmt.Sprintf("%T", arr))
	}

	return result, nil
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
	case *array.Timestamp:
		if v, ok := any(&result).(*time.Time); ok {
			timestamp := arr.Value(index)
			nanos := int64(timestamp)
			*v = time.Unix(nanos/nanosPerSecond, nanos%nanosPerSecond).UTC()
		}
	case *array.Int16:
		if v, ok := any(&result).(*int16); ok {
			*v = arr.Value(index)
		}
	case *array.Int8:
		if v, ok := any(&result).(*int8); ok {
			*v = arr.Value(index)
		}
	case *array.Uint64:
		if v, ok := any(&result).(*uint64); ok {
			*v = arr.Value(index)
		}
	case *array.Uint32:
		if v, ok := any(&result).(*uint32); ok {
			*v = arr.Value(index)
		}
	case *array.Uint16:
		if v, ok := any(&result).(*uint16); ok {
			*v = arr.Value(index)
		}
	case *array.Uint8:
		if v, ok := any(&result).(*uint8); ok {
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
