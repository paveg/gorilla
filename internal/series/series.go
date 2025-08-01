// Package series provides data structures for column operations
package series

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/errors"
)

const (
	nanosPerSecond = 1e9
)

// buildArrowArray creates an Arrow array from the provided values slice.
// This is a helper function that centralizes the type-specific array creation logic.
func buildArrowArray[T any](values []T, mem memory.Allocator) (arrow.Array, error) {
	// Use type switching to create appropriate Arrow array
	switch v := any(values).(type) {
	case []string:
		return buildStringArray(v, mem), nil
	case []int64:
		return buildInt64Array(v, mem), nil
	case []int32:
		return buildInt32Array(v, mem), nil
	case []float64:
		return buildFloat64Array(v, mem), nil
	case []float32:
		return buildFloat32Array(v, mem), nil
	case []bool:
		return buildBoolArray(v, mem), nil
	case []time.Time:
		return buildTimestampArray(v, mem), nil
	case []int16:
		return buildInt16Array(v, mem), nil
	case []int8:
		return buildInt8Array(v, mem), nil
	case []uint64:
		return buildUint64Array(v, mem), nil
	case []uint32:
		return buildUint32Array(v, mem), nil
	case []uint16:
		return buildUint16Array(v, mem), nil
	case []uint8:
		return buildUint8Array(v, mem), nil
	default:
		return nil, errors.NewUnsupportedTypeError("series creation", fmt.Sprintf("%T", values))
	}
}

// Type-specific array builders.
func buildStringArray(values []string, mem memory.Allocator) arrow.Array {
	builder := array.NewStringBuilder(mem)
	defer builder.Release()
	for _, val := range values {
		builder.Append(val)
	}
	return builder.NewArray()
}

func buildInt64Array(values []int64, mem memory.Allocator) arrow.Array {
	builder := array.NewInt64Builder(mem)
	defer builder.Release()
	for _, val := range values {
		builder.Append(val)
	}
	return builder.NewArray()
}

func buildInt32Array(values []int32, mem memory.Allocator) arrow.Array {
	builder := array.NewInt32Builder(mem)
	defer builder.Release()
	for _, val := range values {
		builder.Append(val)
	}
	return builder.NewArray()
}

func buildFloat64Array(values []float64, mem memory.Allocator) arrow.Array {
	builder := array.NewFloat64Builder(mem)
	defer builder.Release()
	for _, val := range values {
		builder.Append(val)
	}
	return builder.NewArray()
}

func buildFloat32Array(values []float32, mem memory.Allocator) arrow.Array {
	builder := array.NewFloat32Builder(mem)
	defer builder.Release()
	for _, val := range values {
		builder.Append(val)
	}
	return builder.NewArray()
}

func buildBoolArray(values []bool, mem memory.Allocator) arrow.Array {
	builder := array.NewBooleanBuilder(mem)
	defer builder.Release()
	for _, val := range values {
		builder.Append(val)
	}
	return builder.NewArray()
}

func buildTimestampArray(values []time.Time, mem memory.Allocator) arrow.Array {
	// Use timestamp with nanosecond precision and timezone support
	builder := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Nanosecond})
	defer builder.Release()
	for _, val := range values {
		// Convert time.Time to nanoseconds since Unix epoch (in UTC)
		timestamp := arrow.Timestamp(val.UTC().UnixNano())
		builder.Append(timestamp)
	}
	return builder.NewArray()
}

func buildInt16Array(values []int16, mem memory.Allocator) arrow.Array {
	builder := array.NewInt16Builder(mem)
	defer builder.Release()
	for _, val := range values {
		builder.Append(val)
	}
	return builder.NewArray()
}

func buildInt8Array(values []int8, mem memory.Allocator) arrow.Array {
	builder := array.NewInt8Builder(mem)
	defer builder.Release()
	for _, val := range values {
		builder.Append(val)
	}
	return builder.NewArray()
}

func buildUint64Array(values []uint64, mem memory.Allocator) arrow.Array {
	builder := array.NewUint64Builder(mem)
	defer builder.Release()
	for _, val := range values {
		builder.Append(val)
	}
	return builder.NewArray()
}

func buildUint32Array(values []uint32, mem memory.Allocator) arrow.Array {
	builder := array.NewUint32Builder(mem)
	defer builder.Release()
	for _, val := range values {
		builder.Append(val)
	}
	return builder.NewArray()
}

func buildUint16Array(values []uint16, mem memory.Allocator) arrow.Array {
	builder := array.NewUint16Builder(mem)
	defer builder.Release()
	for _, val := range values {
		builder.Append(val)
	}
	return builder.NewArray()
}

func buildUint8Array(values []uint8, mem memory.Allocator) arrow.Array {
	builder := array.NewUint8Builder(mem)
	defer builder.Release()
	for _, val := range values {
		builder.Append(val)
	}
	return builder.NewArray()
}

// Series represents a typed data column with Apache Arrow backend.
type Series[T any] struct {
	name  string
	array arrow.Array
}

// New creates a new Series from a slice of values.
func New[T any](name string, values []T, mem memory.Allocator) *Series[T] {
	if mem == nil {
		mem = memory.NewGoAllocator()
	}

	arr, err := buildArrowArray(values, mem)
	if err != nil {
		panic(err.Error())
	}

	return &Series[T]{
		name:  name,
		array: arr,
	}
}

// NewSafe creates a new Series from a slice of values with error handling
// This is the preferred method for production code as it returns errors instead of panicking.
func NewSafe[T any](name string, values []T, mem memory.Allocator) (*Series[T], error) {
	if mem == nil {
		mem = memory.NewGoAllocator()
	}

	arr, err := buildArrowArray(values, mem)
	if err != nil {
		return nil, err
	}

	return &Series[T]{
		name:  name,
		array: arr,
	}, nil
}

// Name returns the column name.
func (s *Series[T]) Name() string {
	return s.name
}

// Len returns the length of the series.
func (s *Series[T]) Len() int {
	return s.array.Len()
}

// populateValues fills the provided slice with values from the Arrow array.
// This is a helper function to reduce code duplication between Values and ValuesSafe.
func (s *Series[T]) populateValues(result []T) error {
	switch arr := s.array.(type) {
	case *array.String:
		return s.populateStringValues(arr, result)
	case *array.Int64:
		return s.populateInt64Values(arr, result)
	case *array.Int32:
		return s.populateInt32Values(arr, result)
	case *array.Float64:
		return s.populateFloat64Values(arr, result)
	case *array.Float32:
		return s.populateFloat32Values(arr, result)
	case *array.Boolean:
		return s.populateBoolValues(arr, result)
	case *array.Timestamp:
		return s.populateTimestampValues(arr, result)
	case *array.Int16:
		return s.populateInt16Values(arr, result)
	case *array.Int8:
		return s.populateInt8Values(arr, result)
	case *array.Uint64:
		return s.populateUint64Values(arr, result)
	case *array.Uint32:
		return s.populateUint32Values(arr, result)
	case *array.Uint16:
		return s.populateUint16Values(arr, result)
	case *array.Uint8:
		return s.populateUint8Values(arr, result)
	default:
		return errors.NewUnsupportedTypeError("values extraction", fmt.Sprintf("%T", arr))
	}
}

// Type-specific value population helpers.
func (s *Series[T]) populateStringValues(arr *array.String, result []T) error {
	if values, ok := any(result).([]string); ok {
		for i := range arr.Len() {
			values[i] = arr.Value(i)
		}
		return nil
	}
	return errors.NewUnsupportedTypeError("string conversion", fmt.Sprintf("%T", result))
}

func (s *Series[T]) populateInt64Values(arr *array.Int64, result []T) error {
	if values, ok := any(result).([]int64); ok {
		for i := range arr.Len() {
			values[i] = arr.Value(i)
		}
		return nil
	}
	return errors.NewUnsupportedTypeError("int64 conversion", fmt.Sprintf("%T", result))
}

func (s *Series[T]) populateInt32Values(arr *array.Int32, result []T) error {
	if values, ok := any(result).([]int32); ok {
		for i := range arr.Len() {
			values[i] = arr.Value(i)
		}
		return nil
	}
	return errors.NewUnsupportedTypeError("int32 conversion", fmt.Sprintf("%T", result))
}

func (s *Series[T]) populateFloat64Values(arr *array.Float64, result []T) error {
	if values, ok := any(result).([]float64); ok {
		for i := range arr.Len() {
			values[i] = arr.Value(i)
		}
		return nil
	}
	return errors.NewUnsupportedTypeError("float64 conversion", fmt.Sprintf("%T", result))
}

func (s *Series[T]) populateFloat32Values(arr *array.Float32, result []T) error {
	if values, ok := any(result).([]float32); ok {
		for i := range arr.Len() {
			values[i] = arr.Value(i)
		}
		return nil
	}
	return errors.NewUnsupportedTypeError("float32 conversion", fmt.Sprintf("%T", result))
}

func (s *Series[T]) populateBoolValues(arr *array.Boolean, result []T) error {
	if values, ok := any(result).([]bool); ok {
		for i := range arr.Len() {
			values[i] = arr.Value(i)
		}
		return nil
	}
	return errors.NewUnsupportedTypeError("bool conversion", fmt.Sprintf("%T", result))
}

func (s *Series[T]) populateTimestampValues(arr *array.Timestamp, result []T) error {
	if values, ok := any(result).([]time.Time); ok {
		for i := range arr.Len() {
			// Convert Arrow timestamp (nanoseconds since Unix epoch) back to time.Time in UTC
			timestamp := arr.Value(i)
			nanos := int64(timestamp)
			values[i] = time.Unix(nanos/nanosPerSecond, nanos%nanosPerSecond).UTC()
		}
		return nil
	}
	return errors.NewUnsupportedTypeError("timestamp conversion", fmt.Sprintf("%T", result))
}

func (s *Series[T]) populateInt16Values(arr *array.Int16, result []T) error {
	if values, ok := any(result).([]int16); ok {
		for i := range arr.Len() {
			values[i] = arr.Value(i)
		}
		return nil
	}
	return errors.NewUnsupportedTypeError("int16 conversion", fmt.Sprintf("%T", result))
}

func (s *Series[T]) populateInt8Values(arr *array.Int8, result []T) error {
	if values, ok := any(result).([]int8); ok {
		for i := range arr.Len() {
			values[i] = arr.Value(i)
		}
		return nil
	}
	return errors.NewUnsupportedTypeError("int8 conversion", fmt.Sprintf("%T", result))
}

func (s *Series[T]) populateUint64Values(arr *array.Uint64, result []T) error {
	if values, ok := any(result).([]uint64); ok {
		for i := range arr.Len() {
			values[i] = arr.Value(i)
		}
		return nil
	}
	return errors.NewUnsupportedTypeError("uint64 conversion", fmt.Sprintf("%T", result))
}

func (s *Series[T]) populateUint32Values(arr *array.Uint32, result []T) error {
	if values, ok := any(result).([]uint32); ok {
		for i := range arr.Len() {
			values[i] = arr.Value(i)
		}
		return nil
	}
	return errors.NewUnsupportedTypeError("uint32 conversion", fmt.Sprintf("%T", result))
}

func (s *Series[T]) populateUint16Values(arr *array.Uint16, result []T) error {
	if values, ok := any(result).([]uint16); ok {
		for i := range arr.Len() {
			values[i] = arr.Value(i)
		}
		return nil
	}
	return errors.NewUnsupportedTypeError("uint16 conversion", fmt.Sprintf("%T", result))
}

func (s *Series[T]) populateUint8Values(arr *array.Uint8, result []T) error {
	if values, ok := any(result).([]uint8); ok {
		for i := range arr.Len() {
			values[i] = arr.Value(i)
		}
		return nil
	}
	return errors.NewUnsupportedTypeError("uint8 conversion", fmt.Sprintf("%T", result))
}

// Values returns the data as a Go slice.
// Note: This method will panic if the series contains null values or unsupported types.
// Use ValuesSafe for error handling instead.
func (s *Series[T]) Values() []T {
	result := make([]T, s.array.Len())
	err := s.populateValues(result)
	if err != nil {
		panic(err.Error())
	}
	return result
}

// ValuesSafe returns the data as a Go slice with error handling
// This is the preferred method for production code as it returns errors instead of panicking.
func (s *Series[T]) ValuesSafe() ([]T, error) {
	result := make([]T, s.array.Len())
	err := s.populateValues(result)
	return result, err
}

// Value returns the value at the given index.
func (s *Series[T]) Value(index int) T {
	if index < 0 || index >= s.array.Len() {
		var zero T
		return zero
	}

	var result T
	s.extractValueAtIndex(index, &result)
	return result
}

// extractValueAtIndex extracts the value at the given index based on the array type.
func (s *Series[T]) extractValueAtIndex(index int, result *T) {
	switch arr := s.array.(type) {
	case *array.String:
		s.extractStringValue(arr, index, result)
	case *array.Int64:
		s.extractInt64Value(arr, index, result)
	case *array.Int32:
		s.extractInt32Value(arr, index, result)
	case *array.Int16:
		s.extractInt16Value(arr, index, result)
	case *array.Int8:
		s.extractInt8Value(arr, index, result)
	case *array.Uint64:
		s.extractUint64Value(arr, index, result)
	case *array.Uint32:
		s.extractUint32Value(arr, index, result)
	case *array.Uint16:
		s.extractUint16Value(arr, index, result)
	case *array.Uint8:
		s.extractUint8Value(arr, index, result)
	case *array.Float64:
		s.extractFloat64Value(arr, index, result)
	case *array.Float32:
		s.extractFloat32Value(arr, index, result)
	case *array.Boolean:
		s.extractBooleanValue(arr, index, result)
	case *array.Timestamp:
		s.extractTimestampValue(arr, index, result)
	}
}

// Type-specific value extraction methods.
func (s *Series[T]) extractStringValue(arr *array.String, index int, result *T) {
	if v, ok := any(result).(*string); ok {
		*v = arr.Value(index)
	}
}

func (s *Series[T]) extractInt64Value(arr *array.Int64, index int, result *T) {
	if v, ok := any(result).(*int64); ok {
		*v = arr.Value(index)
	}
}

func (s *Series[T]) extractInt32Value(arr *array.Int32, index int, result *T) {
	if v, ok := any(result).(*int32); ok {
		*v = arr.Value(index)
	}
}

func (s *Series[T]) extractInt16Value(arr *array.Int16, index int, result *T) {
	if v, ok := any(result).(*int16); ok {
		*v = arr.Value(index)
	}
}

func (s *Series[T]) extractInt8Value(arr *array.Int8, index int, result *T) {
	if v, ok := any(result).(*int8); ok {
		*v = arr.Value(index)
	}
}

func (s *Series[T]) extractUint64Value(arr *array.Uint64, index int, result *T) {
	if v, ok := any(result).(*uint64); ok {
		*v = arr.Value(index)
	}
}

func (s *Series[T]) extractUint32Value(arr *array.Uint32, index int, result *T) {
	if v, ok := any(result).(*uint32); ok {
		*v = arr.Value(index)
	}
}

func (s *Series[T]) extractUint16Value(arr *array.Uint16, index int, result *T) {
	if v, ok := any(result).(*uint16); ok {
		*v = arr.Value(index)
	}
}

func (s *Series[T]) extractUint8Value(arr *array.Uint8, index int, result *T) {
	if v, ok := any(result).(*uint8); ok {
		*v = arr.Value(index)
	}
}

func (s *Series[T]) extractFloat64Value(arr *array.Float64, index int, result *T) {
	if v, ok := any(result).(*float64); ok {
		*v = arr.Value(index)
	}
}

func (s *Series[T]) extractFloat32Value(arr *array.Float32, index int, result *T) {
	if v, ok := any(result).(*float32); ok {
		*v = arr.Value(index)
	}
}

func (s *Series[T]) extractBooleanValue(arr *array.Boolean, index int, result *T) {
	if v, ok := any(result).(*bool); ok {
		*v = arr.Value(index)
	}
}

func (s *Series[T]) extractTimestampValue(arr *array.Timestamp, index int, result *T) {
	if v, ok := any(result).(*time.Time); ok {
		timestamp := arr.Value(index)
		nanos := int64(timestamp)
		*v = time.Unix(nanos/nanosPerSecond, nanos%nanosPerSecond).UTC()
	}
}

// DataType returns the Arrow data type.
func (s *Series[T]) DataType() arrow.DataType {
	return s.array.DataType()
}

// IsNull checks if the value at index is null.
func (s *Series[T]) IsNull(index int) bool {
	return s.array.IsNull(index)
}

// String returns a string representation of the series.
func (s *Series[T]) String() string {
	return fmt.Sprintf("Series[%s]: %s (len=%d)",
		reflect.TypeOf(new(T)).Elem().Name(),
		s.name,
		s.Len())
}

// Array returns the underlying Arrow array (retains a reference).
func (s *Series[T]) Array() arrow.Array {
	if s.array != nil {
		s.array.Retain()
		return s.array
	}
	return nil
}

// Release releases the underlying Arrow memory.
func (s *Series[T]) Release() {
	if s.array != nil {
		s.array.Release()
	}
}

// GetAsString returns the value at the given index as a string.
func (s *Series[T]) GetAsString(index int) string {
	if index < 0 || index >= s.array.Len() || s.array.IsNull(index) {
		return ""
	}

	switch arr := s.array.(type) {
	case *array.String:
		return arr.Value(index)
	case *array.Int64:
		return strconv.FormatInt(arr.Value(index), 10)
	case *array.Int32:
		return strconv.Itoa(int(arr.Value(index)))
	case *array.Int16:
		return strconv.Itoa(int(arr.Value(index)))
	case *array.Int8:
		return strconv.Itoa(int(arr.Value(index)))
	case *array.Uint64:
		return strconv.FormatUint(arr.Value(index), 10)
	case *array.Uint32:
		return strconv.FormatUint(uint64(arr.Value(index)), 10)
	case *array.Uint16:
		return strconv.FormatUint(uint64(arr.Value(index)), 10)
	case *array.Uint8:
		return strconv.FormatUint(uint64(arr.Value(index)), 10)
	case *array.Float64:
		return fmt.Sprintf("%g", arr.Value(index))
	case *array.Float32:
		return fmt.Sprintf("%g", arr.Value(index))
	case *array.Boolean:
		return strconv.FormatBool(arr.Value(index))
	default:
		return s.array.String()
	}
}
