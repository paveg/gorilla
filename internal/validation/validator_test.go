package validation

import (
	"errors"
	"reflect"
	"testing"

	dferrors "github.com/paveg/gorilla/internal/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockColumnProvider implements ColumnProvider for testing
type MockColumnProvider struct {
	columns []string
	length  int
	width   int
}

func (m *MockColumnProvider) HasColumn(name string) bool {
	for _, col := range m.columns {
		if col == name {
			return true
		}
	}
	return false
}

func (m *MockColumnProvider) Columns() []string {
	return m.columns
}

func (m *MockColumnProvider) Len() int {
	return m.length
}

func (m *MockColumnProvider) Width() int {
	return m.width
}

func TestColumnValidator(t *testing.T) {
	// Create mock DataFrame with columns "id" and "name"
	mockDF := &MockColumnProvider{
		columns: []string{"id", "name"},
		length:  3,
		width:   2,
	}

	t.Run("Valid columns", func(t *testing.T) {
		validator := NewColumnValidator(mockDF, "Sort", "id", "name")
		err := validator.Validate()
		assert.NoError(t, err)
	})

	t.Run("Invalid column", func(t *testing.T) {
		validator := NewColumnValidator(mockDF, "Sort", "age")
		err := validator.Validate()
		require.Error(t, err)

		var dfErr *dferrors.DataFrameError
		require.True(t, errors.As(err, &dfErr))
		assert.Equal(t, "Sort", dfErr.Op)
		assert.Equal(t, "age", dfErr.Column)
		assert.Equal(t, "column does not exist", dfErr.Message)
	})

	t.Run("Mixed valid and invalid columns", func(t *testing.T) {
		validator := NewColumnValidator(mockDF, "SortBy", "id", "missing", "name")
		err := validator.Validate()
		require.Error(t, err)

		var dfErr *dferrors.DataFrameError
		require.True(t, errors.As(err, &dfErr))
		assert.Equal(t, "missing", dfErr.Column)
	})
}

func TestLengthValidator(t *testing.T) {
	t.Run("Equal lengths", func(t *testing.T) {
		validator := NewLengthValidator(3, 3, "SortBy", "columns and ascending arrays")
		err := validator.Validate()
		assert.NoError(t, err)
	})

	t.Run("Unequal lengths", func(t *testing.T) {
		validator := NewLengthValidator(3, 2, "SortBy", "columns and ascending arrays")
		err := validator.Validate()
		require.Error(t, err)

		var dfErr *dferrors.DataFrameError
		require.True(t, errors.As(err, &dfErr))
		assert.Equal(t, "SortBy", dfErr.Op)
		assert.Contains(t, dfErr.Message, "expected length 3, got 2")
	})
}

func TestTypeValidator(t *testing.T) {
	supportedTypes := []reflect.Type{
		reflect.TypeOf(""),
		reflect.TypeOf(int64(0)),
		reflect.TypeOf(float64(0)),
	}

	t.Run("Supported type - string", func(t *testing.T) {
		validator := NewTypeValidator("hello", "series creation", supportedTypes...)
		err := validator.Validate()
		assert.NoError(t, err)
	})

	t.Run("Supported type - int64", func(t *testing.T) {
		validator := NewTypeValidator(int64(123), "series creation", supportedTypes...)
		err := validator.Validate()
		assert.NoError(t, err)
	})

	t.Run("Unsupported type", func(t *testing.T) {
		validator := NewTypeValidator(complex128(1+2i), "series creation", supportedTypes...)
		err := validator.Validate()
		require.Error(t, err)

		var dfErr *dferrors.DataFrameError
		require.True(t, errors.As(err, &dfErr))
		assert.Equal(t, "series creation", dfErr.Op)
		assert.Contains(t, dfErr.Message, "complex128")
	})
}

func TestIndexValidator(t *testing.T) {
	t.Run("Valid index", func(t *testing.T) {
		validator := NewIndexValidator(2, 5, "indexing")
		err := validator.Validate()
		assert.NoError(t, err)
	})

	t.Run("Index too high", func(t *testing.T) {
		validator := NewIndexValidator(5, 5, "indexing")
		err := validator.Validate()
		require.Error(t, err)

		var dfErr *dferrors.DataFrameError
		require.True(t, errors.As(err, &dfErr))
		assert.Equal(t, "indexing", dfErr.Op)
		assert.Contains(t, dfErr.Message, "index 5 out of bounds [0, 5)")
	})

	t.Run("Negative index", func(t *testing.T) {
		validator := NewIndexValidator(-1, 5, "indexing")
		err := validator.Validate()
		require.Error(t, err)

		var dfErr *dferrors.DataFrameError
		require.True(t, errors.As(err, &dfErr))
		assert.Contains(t, dfErr.Message, "index -1 out of bounds")
	})
}

func TestEmptyDataFrameValidator(t *testing.T) {
	t.Run("Non-empty DataFrame", func(t *testing.T) {
		mockDF := &MockColumnProvider{
			columns: []string{"id"},
			length:  3,
			width:   1,
		}

		validator := NewEmptyDataFrameValidator(mockDF, "Sort")
		err := validator.Validate()
		assert.NoError(t, err)
	})

	t.Run("Empty DataFrame", func(t *testing.T) {
		mockDF := &MockColumnProvider{
			columns: []string{},
			length:  0,
			width:   0,
		}

		validator := NewEmptyDataFrameValidator(mockDF, "Sort")
		err := validator.Validate()
		require.Error(t, err)

		var dfErr *dferrors.DataFrameError
		require.True(t, errors.As(err, &dfErr))
		assert.Equal(t, "Sort", dfErr.Op)
		assert.Equal(t, "operation not supported on empty DataFrame", dfErr.Message)
	})
}

func TestCompoundValidator(t *testing.T) {
	mockDF := &MockColumnProvider{
		columns: []string{"id"},
		length:  3,
		width:   1,
	}

	t.Run("All validators pass", func(t *testing.T) {
		validator := NewCompoundValidator(
			NewColumnValidator(mockDF, "Sort", "id"),
			NewLengthValidator(3, 3, "SortBy", "test arrays"),
			NewEmptyDataFrameValidator(mockDF, "Sort"),
		)
		err := validator.Validate()
		assert.NoError(t, err)
	})

	t.Run("First validator fails", func(t *testing.T) {
		validator := NewCompoundValidator(
			NewColumnValidator(mockDF, "Sort", "missing"),
			NewLengthValidator(3, 3, "SortBy", "test arrays"),
		)
		err := validator.Validate()
		require.Error(t, err)

		var dfErr *dferrors.DataFrameError
		require.True(t, errors.As(err, &dfErr))
		assert.Equal(t, "missing", dfErr.Column)
	})

	t.Run("Second validator fails", func(t *testing.T) {
		validator := NewCompoundValidator(
			NewColumnValidator(mockDF, "Sort", "id"),
			NewLengthValidator(3, 2, "SortBy", "test arrays"),
		)
		err := validator.Validate()
		require.Error(t, err)

		var dfErr *dferrors.DataFrameError
		require.True(t, errors.As(err, &dfErr))
		assert.Equal(t, "SortBy", dfErr.Op)
		assert.Contains(t, dfErr.Message, "expected length 3, got 2")
	})
}

func TestConvenienceFunctions(t *testing.T) {
	mockDF := &MockColumnProvider{
		columns: []string{"id"},
		length:  3,
		width:   1,
	}

	t.Run("ValidateColumns", func(t *testing.T) {
		err := ValidateColumns(mockDF, "Sort", "id")
		assert.NoError(t, err)

		err = ValidateColumns(mockDF, "Sort", "missing")
		assert.Error(t, err)
	})

	t.Run("ValidateLength", func(t *testing.T) {
		err := ValidateLength(3, 3, "SortBy", "test")
		assert.NoError(t, err)

		err = ValidateLength(3, 2, "SortBy", "test")
		assert.Error(t, err)
	})

	t.Run("ValidateType", func(t *testing.T) {
		supportedTypes := []reflect.Type{reflect.TypeOf("")}

		err := ValidateType("string", "test", supportedTypes...)
		assert.NoError(t, err)

		err = ValidateType(123, "test", supportedTypes...)
		assert.Error(t, err)
	})

	t.Run("ValidateIndex", func(t *testing.T) {
		err := ValidateIndex(2, 5, "indexing")
		assert.NoError(t, err)

		err = ValidateIndex(5, 5, "indexing")
		assert.Error(t, err)
	})

	t.Run("ValidateNotEmpty", func(t *testing.T) {
		err := ValidateNotEmpty(mockDF, "Sort")
		assert.NoError(t, err)

		emptyMockDF := &MockColumnProvider{
			columns: []string{},
			length:  0,
			width:   0,
		}

		err = ValidateNotEmpty(emptyMockDF, "Sort")
		assert.Error(t, err)
	})
}
