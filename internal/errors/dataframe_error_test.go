package errors

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDataFrameError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      *DataFrameError
		expected string
	}{
		{
			name: "Error with column",
			err: &DataFrameError{
				Op:      "Sort",
				Column:  "age",
				Message: "column does not exist",
			},
			expected: "Sort operation failed on column 'age': column does not exist",
		},
		{
			name: "Error without column",
			err: &DataFrameError{
				Op:      "Join",
				Message: "mismatched lengths",
			},
			expected: "Join operation failed: mismatched lengths",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.err.Error())
		})
	}
}

func TestDataFrameError_Unwrap(t *testing.T) {
	cause := errors.New("underlying error")
	err := &DataFrameError{
		Op:      "Filter",
		Message: "evaluation failed",
		Cause:   cause,
	}

	assert.Equal(t, cause, err.Unwrap())
}

func TestDataFrameError_Is(t *testing.T) {
	err1 := &DataFrameError{
		Op:      "Sort",
		Column:  "age",
		Message: "column does not exist",
	}

	err2 := &DataFrameError{
		Op:      "Sort",
		Column:  "age",
		Message: "column does not exist",
	}

	err3 := &DataFrameError{
		Op:      "Filter",
		Column:  "age",
		Message: "column does not exist",
	}

	assert.True(t, err1.Is(err2))
	assert.False(t, err1.Is(err3))
	assert.False(t, err1.Is(errors.New("different error")))
}

func TestNewColumnNotFoundError(t *testing.T) {
	err := NewColumnNotFoundError("Sort", "missing_column")

	assert.Equal(t, "Sort", err.Op)
	assert.Equal(t, "missing_column", err.Column)
	assert.Equal(t, "column does not exist", err.Message)
	assert.Equal(t, "Sort operation failed on column 'missing_column': column does not exist", err.Error())
}

func TestNewInvalidInputError(t *testing.T) {
	err := NewInvalidInputError("SortBy", "arrays must have same length")

	assert.Equal(t, "SortBy", err.Op)
	assert.Equal(t, "", err.Column)
	assert.Equal(t, "arrays must have same length", err.Message)
	assert.Equal(t, "SortBy operation failed: arrays must have same length", err.Error())
}

func TestNewUnsupportedTypeError(t *testing.T) {
	err := NewUnsupportedTypeError("series creation", "[]complex128")

	assert.Equal(t, "series creation", err.Op)
	assert.Equal(t, "unsupported type: []complex128", err.Message)
}

func TestNewValidationError(t *testing.T) {
	err := NewValidationError("Sort", "age", "index out of bounds")

	assert.Equal(t, "Sort", err.Op)
	assert.Equal(t, "age", err.Column)
	assert.Equal(t, "index out of bounds", err.Message)
}

func TestNewInternalError(t *testing.T) {
	cause := errors.New("memory allocation failed")
	err := NewInternalError("GroupBy", cause)

	assert.Equal(t, "GroupBy", err.Op)
	assert.Equal(t, "internal error occurred", err.Message)
	assert.Equal(t, cause, err.Cause)
	assert.Equal(t, cause, err.Unwrap())
}

func TestPredefinedErrors(t *testing.T) {
	assert.Equal(t, "validation", ErrEmptyDataFrame.Op)
	assert.Equal(t, "operation not supported on empty DataFrame", ErrEmptyDataFrame.Message)

	assert.Equal(t, "validation", ErrMismatchedLength.Op)
	assert.Equal(t, "arrays must have the same length", ErrMismatchedLength.Message)

	assert.Equal(t, "indexing", ErrInvalidIndex.Op)
	assert.Equal(t, "index out of bounds", ErrInvalidIndex.Message)
}
