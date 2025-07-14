// Package validation provides input validation utilities for DataFrame operations.
// This package implements a comprehensive validation framework with reusable
// validators for common validation scenarios like column existence, length
// consistency, type checking, and bounds validation.
package validation

import (
	"fmt"
	"reflect"

	"github.com/paveg/gorilla/internal/errors"
)

// Validator interface for input validation
type Validator interface {
	Validate() error
}

// ColumnValidator validates column existence and properties
type ColumnValidator struct {
	df      ColumnProvider
	columns []string
	op      string
}

// ColumnProvider interface for types that provide column information
type ColumnProvider interface {
	HasColumn(name string) bool
	Columns() []string
	Len() int
	Width() int
}

// NewColumnValidator creates a validator for column operations
func NewColumnValidator(df ColumnProvider, op string, columns ...string) *ColumnValidator {
	return &ColumnValidator{
		df:      df,
		columns: columns,
		op:      op,
	}
}

// Validate checks if all columns exist in the DataFrame
func (v *ColumnValidator) Validate() error {
	for _, column := range v.columns {
		if !v.df.HasColumn(column) {
			return errors.NewColumnNotFoundError(v.op, column)
		}
	}
	return nil
}

// LengthValidator validates array length consistency
type LengthValidator struct {
	expected int
	actual   int
	op       string
	context  string
}

// NewLengthValidator creates a validator for length consistency
func NewLengthValidator(expected, actual int, op, context string) *LengthValidator {
	return &LengthValidator{
		expected: expected,
		actual:   actual,
		op:       op,
		context:  context,
	}
}

// Validate checks if lengths match
func (v *LengthValidator) Validate() error {
	if v.expected != v.actual {
		message := fmt.Sprintf("%s: expected length %d, got %d", v.context, v.expected, v.actual)
		return errors.NewValidationError(v.op, "", message)
	}
	return nil
}

// TypeValidator validates supported data types
type TypeValidator struct {
	value          interface{}
	supportedTypes []reflect.Type
	op             string
}

// NewTypeValidator creates a validator for type checking
func NewTypeValidator(value interface{}, op string, supportedTypes ...reflect.Type) *TypeValidator {
	return &TypeValidator{
		value:          value,
		supportedTypes: supportedTypes,
		op:             op,
	}
}

// Validate checks if the value type is supported
func (v *TypeValidator) Validate() error {
	valueType := reflect.TypeOf(v.value)

	for _, supportedType := range v.supportedTypes {
		if valueType == supportedType {
			return nil
		}
	}

	return errors.NewUnsupportedTypeError(v.op, valueType.String())
}

// IndexValidator validates index bounds
type IndexValidator struct {
	index int
	max   int
	op    string
}

// NewIndexValidator creates a validator for index operations
func NewIndexValidator(index, maxIndex int, op string) *IndexValidator {
	return &IndexValidator{
		index: index,
		max:   maxIndex,
		op:    op,
	}
}

// Validate checks if index is within bounds
func (v *IndexValidator) Validate() error {
	if v.index < 0 || v.index >= v.max {
		message := fmt.Sprintf("index %d out of bounds [0, %d)", v.index, v.max)
		return errors.NewValidationError(v.op, "", message)
	}
	return nil
}

// EmptyDataFrameValidator validates operations on empty DataFrames
type EmptyDataFrameValidator struct {
	df ColumnProvider
	op string
}

// NewEmptyDataFrameValidator creates a validator for empty DataFrame checks
func NewEmptyDataFrameValidator(df ColumnProvider, op string) *EmptyDataFrameValidator {
	return &EmptyDataFrameValidator{
		df: df,
		op: op,
	}
}

// Validate checks if DataFrame is empty when operation requires data
func (v *EmptyDataFrameValidator) Validate() error {
	if v.df.Len() == 0 {
		return &errors.DataFrameError{
			Op:      v.op,
			Message: "operation not supported on empty DataFrame",
		}
	}
	return nil
}

// CompoundValidator combines multiple validators
type CompoundValidator struct {
	validators []Validator
}

// NewCompoundValidator creates a validator that checks multiple conditions
func NewCompoundValidator(validators ...Validator) *CompoundValidator {
	return &CompoundValidator{
		validators: validators,
	}
}

// Validate runs all validators and returns the first error encountered
func (v *CompoundValidator) Validate() error {
	for _, validator := range v.validators {
		if err := validator.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// Convenience validation functions

// ValidateColumns is a convenience function for column validation
func ValidateColumns(df ColumnProvider, op string, columns ...string) error {
	return NewColumnValidator(df, op, columns...).Validate()
}

// ValidateLength is a convenience function for length validation
func ValidateLength(expected, actual int, op, context string) error {
	return NewLengthValidator(expected, actual, op, context).Validate()
}

// ValidateType is a convenience function for type validation
func ValidateType(value interface{}, op string, supportedTypes ...reflect.Type) error {
	return NewTypeValidator(value, op, supportedTypes...).Validate()
}

// ValidateIndex is a convenience function for index validation
func ValidateIndex(index, maxIndex int, op string) error {
	return NewIndexValidator(index, maxIndex, op).Validate()
}

// ValidateNotEmpty is a convenience function for empty DataFrame validation
func ValidateNotEmpty(df ColumnProvider, op string) error {
	return NewEmptyDataFrameValidator(df, op).Validate()
}
