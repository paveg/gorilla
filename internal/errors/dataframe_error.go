// Package errors provides standardized error types for DataFrame operations.
// This package defines DataFrameError for consistent error handling across
// all public APIs, with operation context and error wrapping support.
package errors

import (
	"fmt"
)

// DataFrameError represents standardized errors across all DataFrame operations
type DataFrameError struct {
	Op      string // Operation name (e.g., "Sort", "Filter", "Join")
	Column  string // Column name if applicable
	Message string // Human-readable error description
	Cause   error  // Underlying error cause
}

// Error implements the error interface
func (e *DataFrameError) Error() string {
	if e.Column != "" {
		return fmt.Sprintf("%s operation failed on column '%s': %s", e.Op, e.Column, e.Message)
	}
	return fmt.Sprintf("%s operation failed: %s", e.Op, e.Message)
}

// Unwrap returns the underlying cause for error wrapping support
func (e *DataFrameError) Unwrap() error {
	return e.Cause
}

// Is implements error equality checking for errors.Is()
func (e *DataFrameError) Is(target error) bool {
	if df, ok := target.(*DataFrameError); ok {
		return e.Op == df.Op && e.Column == df.Column && e.Message == df.Message
	}
	return false
}

// Common error constructors for consistent error creation

// NewColumnNotFoundError creates an error for operations on non-existent columns
func NewColumnNotFoundError(op, column string) *DataFrameError {
	return &DataFrameError{
		Op:      op,
		Column:  column,
		Message: "column does not exist",
	}
}

// NewInvalidInputError creates an error for invalid operation inputs
func NewInvalidInputError(op, message string) *DataFrameError {
	return &DataFrameError{
		Op:      op,
		Message: message,
	}
}

// NewUnsupportedTypeError creates an error for unsupported data types
func NewUnsupportedTypeError(op, typeName string) *DataFrameError {
	return &DataFrameError{
		Op:      op,
		Message: fmt.Sprintf("unsupported type: %s", typeName),
	}
}

// NewValidationError creates an error for input validation failures
func NewValidationError(op, column, message string) *DataFrameError {
	return &DataFrameError{
		Op:      op,
		Column:  column,
		Message: message,
	}
}

// NewInternalError creates an error for internal operation failures
func NewInternalError(op string, cause error) *DataFrameError {
	return &DataFrameError{
		Op:      op,
		Message: "internal error occurred",
		Cause:   cause,
	}
}

// Predefined error variables for common cases
var (
	// ErrEmptyDataFrame indicates operations on empty DataFrames
	ErrEmptyDataFrame = &DataFrameError{
		Op:      "validation",
		Message: "operation not supported on empty DataFrame",
	}

	// ErrMismatchedLength indicates length mismatches in operations
	ErrMismatchedLength = &DataFrameError{
		Op:      "validation",
		Message: "arrays must have the same length",
	}

	// ErrInvalidIndex indicates out-of-bounds index access
	ErrInvalidIndex = &DataFrameError{
		Op:      "indexing",
		Message: "index out of bounds",
	}
)
