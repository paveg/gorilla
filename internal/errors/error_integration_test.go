package errors

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNewErrorTypes tests the new error types added for enhanced error handling.
func TestNewErrorTypes(t *testing.T) {
	t.Run("UnsupportedOperationError", func(t *testing.T) {
		err := NewUnsupportedOperationError("Join", "Cannot join on boolean columns",
			[]string{"inner", "left", "right", "outer"})

		errMsg := err.Error()
		assert.Contains(t, errMsg, "Unsupported operation in Join")
		assert.Contains(t, errMsg, "Cannot join on boolean columns")
		assert.Contains(t, errMsg, "Supported options: inner, left, right, outer")
	})

	t.Run("UnsupportedOperationErrorWithoutOptions", func(t *testing.T) {
		err := NewUnsupportedOperationError("Custom", "This operation is not implemented", nil)

		errMsg := err.Error()
		assert.Contains(t, errMsg, "Unsupported operation in Custom")
		assert.Contains(t, errMsg, "This operation is not implemented")
		assert.NotContains(t, errMsg, "Supported options:")
	})

	t.Run("ConfigurationError", func(t *testing.T) {
		err := NewConfigurationError("WorkerPoolSize", -5, []string{"0 (auto)", "1-16"})

		errMsg := err.Error()
		assert.Contains(t, errMsg, "Invalid configuration for parameter 'WorkerPoolSize'")
		assert.Contains(t, errMsg, "value: -5")
		assert.Contains(t, errMsg, "Valid options: 0 (auto), 1-16")
	})

	t.Run("ConfigurationErrorWithoutOptions", func(t *testing.T) {
		err := NewConfigurationError("CustomParam", "invalid", nil)

		errMsg := err.Error()
		assert.Contains(t, errMsg, "Invalid configuration for parameter 'CustomParam'")
		assert.Contains(t, errMsg, "value: invalid")
		assert.NotContains(t, errMsg, "Valid options:")
	})

	t.Run("InvalidExpressionError", func(t *testing.T) {
		err := NewInvalidExpressionError("Filter", "Cannot use aggregation function in WHERE clause")

		errMsg := err.Error()
		assert.Contains(t, errMsg, "Filter operation failed")
		assert.Contains(t, errMsg, "Invalid expression in Filter: Cannot use aggregation function in WHERE clause")
		assert.Contains(t, errMsg, "Check the expression syntax")
	})
}

// TestErrorEnhancementCompatibility tests that new error types work with existing enhancement features.
func TestErrorEnhancementCompatibility(t *testing.T) {
	t.Run("UnsupportedOperationWithContext", func(t *testing.T) {
		err := NewUnsupportedOperationError("Sort", "Cannot sort null values", []string{"ascending", "descending"}).
			WithContext(map[string]string{
				"null_count": "25",
				"total_rows": "100",
			}).
			WithHint("Consider filtering out null values before sorting")

		debugMsg := err.Format(ErrorLevelDebug)
		assert.Contains(t, debugMsg, "null_count: 25")
		assert.Contains(t, debugMsg, "Consider filtering out null values")
	})

	t.Run("ConfigurationErrorWithDataFrameInfo", func(t *testing.T) {
		dataInfo := DataFrameInfo{
			Rows:    1000,
			Columns: []string{"col1", "col2", "col3"},
		}

		err := NewConfigurationError("ChunkSize", 0, []string{"1", "100", "1000"}).
			WithDataFrameInfo(dataInfo)

		detailedMsg := err.Format(ErrorLevelDetailed)
		assert.Contains(t, detailedMsg, "DataFrame has 1000 rows")
		assert.Contains(t, detailedMsg, "Available columns: [col1, col2, col3]")
	})
}
