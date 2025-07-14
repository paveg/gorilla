package errors

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnhancedDataFrameError_WithContext(t *testing.T) {
	err := NewColumnNotFoundError("Filter", "missing_col")
	enhanced := err.WithContext(map[string]string{
		"available_columns": "name, age, salary",
		"total_columns":     "3",
	})

	assert.Contains(t, enhanced.Error(), "missing_col")
	// Context should appear in debug format
	debugMsg := enhanced.Format(ErrorLevelDebug)
	assert.Contains(t, debugMsg, "available_columns: name, age, salary")
}

func TestEnhancedDataFrameError_WithHint(t *testing.T) {
	err := NewColumnNotFoundError("Select", "nam")
	enhanced := err.WithHint("Did you mean 'name'? Available columns: [name, age, salary]")

	errorMsg := enhanced.Error()
	assert.Contains(t, errorMsg, "Hint: Did you mean 'name'?")
}

func TestEnhancedDataFrameError_WithDataFrameInfo(t *testing.T) {
	info := DataFrameInfo{
		Rows:    1000,
		Columns: []string{"name", "age", "salary"},
		Types:   map[string]string{"name": "string", "age": "int64", "salary": "float64"},
	}

	err := NewColumnNotFoundError("GroupBy", "department")
	enhanced := err.WithDataFrameInfo(info)

	errorMsg := enhanced.Error()
	assert.Contains(t, errorMsg, "Available columns: [name, age, salary]")
	assert.Contains(t, errorMsg, "DataFrame has 1000 rows")
}

func TestColumnNotFoundErrorWithSuggestions(t *testing.T) {
	availableColumns := []string{"customer_name", "customer_age", "customer_email"}
	err := NewColumnNotFoundErrorWithSuggestions("Filter", "custmer_name", availableColumns)

	errorMsg := err.Error()
	assert.Contains(t, errorMsg, "Column 'custmer_name' does not exist")
	assert.Contains(t, errorMsg, "Did you mean 'customer_name'?")
	assert.Contains(t, errorMsg, "Available columns: [customer_name, customer_age, customer_email]")
}

func TestTypeMismatchErrorWithDetails(t *testing.T) {
	err := NewTypeMismatchError("Add", "age", "int64", "string")

	errorMsg := err.Error()
	assert.Contains(t, errorMsg, "Type mismatch in column 'age'")
	assert.Contains(t, errorMsg, "expected int64, got string")
	assert.Contains(t, errorMsg, "convert the column type")
}

func TestIndexOutOfBoundsErrorWithDetails(t *testing.T) {
	err := NewIndexOutOfBoundsError("Slice", 15, 10)

	errorMsg := err.Error()
	assert.Contains(t, errorMsg, "Index 15 is out of bounds")
	assert.Contains(t, errorMsg, "valid range: [0, 10)")
}

func TestErrorWithProgressiveDisclosure(t *testing.T) {
	info := DataFrameInfo{
		Rows:    500,
		Columns: []string{"id", "name", "score"},
		Types:   map[string]string{"id": "int64", "name": "string", "score": "float64"},
	}

	err := NewColumnNotFoundError("Sort", "points").
		WithHint("Did you mean 'score'?").
		WithDataFrameInfo(info).
		WithContext(map[string]string{
			"operation_step": "2/3",
			"query_id":       "sort_query_001",
		})

	t.Run("Simple format", func(t *testing.T) {
		simple := err.Format(ErrorLevelSimple)
		assert.Contains(t, simple, "column does not exist")
		assert.NotContains(t, simple, "Hint:")
		assert.NotContains(t, simple, "Available columns:")
	})

	t.Run("Detailed format", func(t *testing.T) {
		detailed := err.Format(ErrorLevelDetailed)
		assert.Contains(t, detailed, "column does not exist")
		assert.Contains(t, detailed, "Hint: Did you mean 'score'?")
		assert.Contains(t, detailed, "Available columns: [id, name, score]")
	})

	t.Run("Debug format", func(t *testing.T) {
		debug := err.Format(ErrorLevelDebug)
		assert.Contains(t, debug, "column does not exist")
		assert.Contains(t, debug, "operation_step: 2/3")
		assert.Contains(t, debug, "query_id: sort_query_001")
		assert.Contains(t, debug, "DataFrame has 500 rows")
	})
}

func TestOperationContextError(t *testing.T) {
	ctx := OperationContext{
		Operation:  "complex_query",
		Step:       2,
		TotalSteps: 5,
		DataInfo: DataFrameInfo{
			Rows:    1000,
			Columns: []string{"user_id", "timestamp", "value"},
		},
	}

	originalErr := NewColumnNotFoundError("Filter", "user_name")
	wrappedErr := ctx.WrapError(originalErr)

	errorMsg := wrappedErr.Error()
	assert.Contains(t, errorMsg, "complex_query")
	assert.Contains(t, errorMsg, "step 2/5")
	assert.Contains(t, errorMsg, "1000 rows")
}

func TestErrorChaining(t *testing.T) {
	err1 := NewColumnNotFoundError("Join", "user_id")
	err2 := NewInvalidInputError("Pipeline", "join operation failed")

	chained := err2.WithCause(err1)

	assert.Equal(t, err1, chained.Unwrap())

	// Test errors.Is functionality
	require.True(t, chained.Is(err2))
}

func TestValidationErrorWithDataFrameContext(t *testing.T) {
	info := DataFrameInfo{
		Rows:    0,
		Columns: []string{},
		Types:   map[string]string{},
	}

	err := NewValidationError("Sort", "", "cannot sort empty DataFrame").
		WithDataFrameInfo(info).
		WithHint("Add data to the DataFrame before sorting")

	errorMsg := err.Error()
	assert.Contains(t, errorMsg, "cannot sort empty DataFrame")
	assert.Contains(t, errorMsg, "DataFrame has 0 rows")
	assert.Contains(t, errorMsg, "Add data to the DataFrame")
}

func TestSimilarColumnSuggestions(t *testing.T) {
	tests := []struct {
		name               string
		searchColumn       string
		availableColumns   []string
		expectedSuggestion string
	}{
		{
			name:               "Single character typo",
			searchColumn:       "nam",
			availableColumns:   []string{"name", "age", "salary"},
			expectedSuggestion: "name",
		},
		{
			name:               "Character swap",
			searchColumn:       "agr",
			availableColumns:   []string{"name", "age", "salary"},
			expectedSuggestion: "age",
		},
		{
			name:               "Case mismatch",
			searchColumn:       "NAME",
			availableColumns:   []string{"name", "age", "salary"},
			expectedSuggestion: "name",
		},
		{
			name:               "Underscore vs camelCase",
			searchColumn:       "user_name",
			availableColumns:   []string{"userName", "userAge", "userEmail"},
			expectedSuggestion: "userName",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			suggestions := findSimilarColumns(tt.searchColumn, tt.availableColumns)
			assert.Contains(t, suggestions, tt.expectedSuggestion)
		})
	}
}
