package common

import (
	"fmt"
	"math"
	"strconv"
)

// TypeConverter provides common type conversion utilities.
type TypeConverter struct{}

// NewTypeConverter creates a new TypeConverter instance.
func NewTypeConverter() *TypeConverter {
	return &TypeConverter{}
}

// SafeInt64ToInt safely converts int64 to int, checking for overflow.
func (tc *TypeConverter) SafeInt64ToInt(value int64) (int, error) {
	if value > math.MaxInt32 || value < math.MinInt32 {
		return 0, fmt.Errorf("int64 value %d overflows int range", value)
	}
	return int(value), nil
}

// SafeFloat64ToFloat32 safely converts float64 to float32, checking for overflow.
func (tc *TypeConverter) SafeFloat64ToFloat32(value float64) (float32, error) {
	if math.IsInf(value, 0) || math.IsNaN(value) {
		return float32(value), nil // Preserve special values
	}
	if value > math.MaxFloat32 || value < -math.MaxFloat32 {
		return 0, fmt.Errorf("float64 value %f overflows float32 range", value)
	}
	return float32(value), nil
}

// ToInt64 converts various numeric types to int64.
func (tc *TypeConverter) ToInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("uint value %d overflows int64 range", v)
		}
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("uint64 value %d overflows int64 range", v)
		}
		return int64(v), nil
	case float32:
		if v > math.MaxInt64 || v < math.MinInt64 {
			return 0, fmt.Errorf("float32 value %f overflows int64 range", v)
		}
		return int64(v), nil
	case float64:
		if v > math.MaxInt64 || v < math.MinInt64 {
			return 0, fmt.Errorf("float64 value %f overflows int64 range", v)
		}
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", value)
	}
}

// ToFloat64 converts various numeric types to float64.
func (tc *TypeConverter) ToFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case string:
		return strconv.ParseFloat(v, 64)
	case bool:
		if v {
			return 1.0, nil
		}
		return 0.0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", value)
	}
}

// ToString converts various types to string.
func (tc *TypeConverter) ToString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return fmt.Sprintf("%g", v)
	case bool:
		return strconv.FormatBool(v)
	case fmt.Stringer:
		return v.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}

// ToBool converts various types to bool.
func (tc *TypeConverter) ToBool(value interface{}) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case int, int8, int16, int32, int64:
		return v != 0, nil
	case uint, uint8, uint16, uint32, uint64:
		return v != 0, nil
	case float32:
		return v != 0.0, nil
	case float64:
		return v != 0.0, nil
	case string:
		return strconv.ParseBool(v)
	default:
		return false, fmt.Errorf("cannot convert %T to bool", value)
	}
}

// IsNumericType checks if a value is of a numeric type.
func (tc *TypeConverter) IsNumericType(value interface{}) bool {
	switch value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return true
	default:
		return false
	}
}

// IsIntegerType checks if a value is of an integer type.
func (tc *TypeConverter) IsIntegerType(value interface{}) bool {
	switch value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	default:
		return false
	}
}

// IsFloatType checks if a value is of a floating-point type.
func (tc *TypeConverter) IsFloatType(value interface{}) bool {
	switch value.(type) {
	case float32, float64:
		return true
	default:
		return false
	}
}

// GetTypeName returns the type name of a value.
func (tc *TypeConverter) GetTypeName(value interface{}) string {
	switch value.(type) {
	case int:
		return "int"
	case int8:
		return "int8"
	case int16:
		return "int16"
	case int32:
		return "int32"
	case int64:
		return "int64"
	case uint:
		return "uint"
	case uint8:
		return "uint8"
	case uint16:
		return "uint16"
	case uint32:
		return "uint32"
	case uint64:
		return "uint64"
	case float32:
		return "float32"
	case float64:
		return "float64"
	case bool:
		return "bool"
	case string:
		return "string"
	default:
		return fmt.Sprintf("%T", value)
	}
}

// Default converter instance for convenience.
var defaultConverter = NewTypeConverter()

// Convenient functions using the default converter

// SafeInt64ToInt safely converts int64 to int using the default converter.
func SafeInt64ToInt(value int64) (int, error) {
	return defaultConverter.SafeInt64ToInt(value)
}

// SafeFloat64ToFloat32 safely converts float64 to float32 using the default converter.
func SafeFloat64ToFloat32(value float64) (float32, error) {
	return defaultConverter.SafeFloat64ToFloat32(value)
}

// ToInt64 converts various types to int64 using the default converter.
func ToInt64(value interface{}) (int64, error) {
	return defaultConverter.ToInt64(value)
}

// ToFloat64 converts various types to float64 using the default converter.
func ToFloat64(value interface{}) (float64, error) {
	return defaultConverter.ToFloat64(value)
}

// ToString converts various types to string using the default converter.
func ToString(value interface{}) string {
	return defaultConverter.ToString(value)
}

// ToBool converts various types to bool using the default converter.
func ToBool(value interface{}) (bool, error) {
	return defaultConverter.ToBool(value)
}

// IsNumericType checks if a value is numeric using the default converter.
func IsNumericType(value interface{}) bool {
	return defaultConverter.IsNumericType(value)
}

// IsIntegerType checks if a value is integer using the default converter.
func IsIntegerType(value interface{}) bool {
	return defaultConverter.IsIntegerType(value)
}

// IsFloatType checks if a value is float using the default converter.
func IsFloatType(value interface{}) bool {
	return defaultConverter.IsFloatType(value)
}

// GetTypeName returns the type name using the default converter.
func GetTypeName(value interface{}) string {
	return defaultConverter.GetTypeName(value)
}
