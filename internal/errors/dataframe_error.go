// Package errors provides standardized error types for DataFrame operations.
// This package defines DataFrameError for consistent error handling across
// all public APIs, with operation context and error wrapping support.
package errors

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// Constants for similarity matching.
const (
	similarityThreshold      = 0.6  // Minimum similarity score for suggestions
	maxSuggestions           = 3    // Maximum number of suggestions to return
	scoreMultiplier          = 100  // Multiplier for scoring
	caseMismatchSimilarity   = 0.95 // Similarity score for case mismatches
	formatMismatchSimilarity = 0.9  // Similarity score for format mismatches (underscore vs camelCase)
)

// ErrorLevel defines the verbosity level for error formatting.
type ErrorLevel int

const (
	ErrorLevelSimple ErrorLevel = iota
	ErrorLevelDetailed
	ErrorLevelDebug
)

// DataFrameInfo contains information about a DataFrame for error context.
type DataFrameInfo struct {
	Rows    int               // Number of rows
	Columns []string          // Column names
	Types   map[string]string // Column name to type mapping
}

// OperationContext provides context for wrapping errors in multi-step operations.
type OperationContext struct {
	Operation  string        // High-level operation name
	Step       int           // Current step number
	TotalSteps int           // Total number of steps
	DataInfo   DataFrameInfo // Information about the DataFrame being processed
}

// DataFrameError represents standardized errors across all DataFrame operations.
type DataFrameError struct {
	Op       string            // Operation name (e.g., "Sort", "Filter", "Join")
	Column   string            // Column name if applicable
	Message  string            // Human-readable error description
	Cause    error             // Underlying error cause
	Hint     string            // Suggested solution or helpful tip
	Context  map[string]string // Additional context information
	DataInfo *DataFrameInfo    // DataFrame information for better context
}

// Error implements the error interface.
func (e *DataFrameError) Error() string {
	return e.Format(ErrorLevelDetailed)
}

// Format returns a formatted error message at the specified verbosity level.
func (e *DataFrameError) Format(level ErrorLevel) string {
	var buf strings.Builder

	// Base error message
	if e.Column != "" {
		fmt.Fprintf(&buf, "%s operation failed on column '%s': %s", e.Op, e.Column, e.Message)
	} else {
		fmt.Fprintf(&buf, "%s operation failed: %s", e.Op, e.Message)
	}

	// Add hint for detailed and debug levels
	if level >= ErrorLevelDetailed && e.Hint != "" {
		fmt.Fprintf(&buf, "\nHint: %s", e.Hint)
	}

	// Add DataFrame info for detailed and debug levels
	if level >= ErrorLevelDetailed && e.DataInfo != nil {
		if len(e.DataInfo.Columns) > 0 {
			fmt.Fprintf(&buf, "\nAvailable columns: [%s]", strings.Join(e.DataInfo.Columns, ", "))
		}
		fmt.Fprintf(&buf, "\nDataFrame has %d rows", e.DataInfo.Rows)
	}

	// Add context for debug level
	if level >= ErrorLevelDebug && len(e.Context) > 0 {
		fmt.Fprintf(&buf, "\nContext:")
		// Sort keys for consistent output
		keys := make([]string, 0, len(e.Context))
		for k := range e.Context {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(&buf, "\n  %s: %s", k, e.Context[k])
		}
	}

	return buf.String()
}

// Unwrap returns the underlying cause for error wrapping support.
func (e *DataFrameError) Unwrap() error {
	return e.Cause
}

// Is implements error equality checking for errors.Is().
func (e *DataFrameError) Is(target error) bool {
	if df, ok := target.(*DataFrameError); ok {
		return e.Op == df.Op && e.Column == df.Column && e.Message == df.Message
	}
	return false
}

// Common error constructors for consistent error creation

// NewColumnNotFoundError creates an error for operations on non-existent columns.
func NewColumnNotFoundError(op, column string) *DataFrameError {
	return &DataFrameError{
		Op:      op,
		Column:  column,
		Message: "column does not exist",
	}
}

// NewInvalidInputError creates an error for invalid operation inputs.
func NewInvalidInputError(op, message string) *DataFrameError {
	return &DataFrameError{
		Op:      op,
		Message: message,
	}
}

// NewUnsupportedTypeError creates an error for unsupported data types.
func NewUnsupportedTypeError(op, typeName string) *DataFrameError {
	return &DataFrameError{
		Op:      op,
		Message: fmt.Sprintf("unsupported type: %s", typeName),
	}
}

// NewValidationError creates an error for input validation failures.
func NewValidationError(op, column, message string) *DataFrameError {
	return &DataFrameError{
		Op:      op,
		Column:  column,
		Message: message,
	}
}

// NewInternalError creates an error for internal operation failures.
func NewInternalError(op string, cause error) *DataFrameError {
	return &DataFrameError{
		Op:      op,
		Message: "internal error occurred",
		Cause:   cause,
	}
}

// Enhanced error methods for fluent API

// WithContext adds additional context information to the error.
func (e *DataFrameError) WithContext(context map[string]string) *DataFrameError {
	enhanced := *e // Copy the error
	if enhanced.Context == nil {
		enhanced.Context = make(map[string]string)
	}
	for k, v := range context {
		enhanced.Context[k] = v
	}
	return &enhanced
}

// WithHint adds a helpful hint to the error.
func (e *DataFrameError) WithHint(hint string) *DataFrameError {
	enhanced := *e // Copy the error
	enhanced.Hint = hint
	return &enhanced
}

// WithDataFrameInfo adds DataFrame information for better context.
func (e *DataFrameError) WithDataFrameInfo(info DataFrameInfo) *DataFrameError {
	enhanced := *e // Copy the error
	enhanced.DataInfo = &info
	return &enhanced
}

// WithCause adds an underlying cause to the error.
func (e *DataFrameError) WithCause(cause error) *DataFrameError {
	enhanced := *e // Copy the error
	enhanced.Cause = cause
	return &enhanced
}

// WrapError wraps an error with operation context information.
func (ctx *OperationContext) WrapError(err error) *DataFrameError {
	dfErr := &DataFrameError{}
	if errors.As(err, &dfErr) {
		// Wrap existing DataFrameError with context
		return &DataFrameError{
			Op:       fmt.Sprintf("%s (step %d/%d)", ctx.Operation, ctx.Step, ctx.TotalSteps),
			Column:   dfErr.Column,
			Message:  dfErr.Message,
			Cause:    dfErr.Cause,
			Hint:     dfErr.Hint,
			Context:  dfErr.Context,
			DataInfo: &ctx.DataInfo,
		}
	}

	// Wrap generic error
	return &DataFrameError{
		Op:       fmt.Sprintf("%s (step %d/%d)", ctx.Operation, ctx.Step, ctx.TotalSteps),
		Message:  err.Error(),
		Cause:    err,
		DataInfo: &ctx.DataInfo,
	}
}

// Specialized error constructors

// NewColumnNotFoundErrorWithSuggestions creates a column not found error with similar column suggestions.
func NewColumnNotFoundErrorWithSuggestions(op, column string, availableColumns []string) *DataFrameError {
	message := fmt.Sprintf("Column '%s' does not exist", column)

	suggestions := findSimilarColumns(column, availableColumns)
	var hint string
	if len(suggestions) > 0 {
		hint = fmt.Sprintf(
			"Did you mean '%s'? Available columns: [%s]",
			suggestions[0],
			strings.Join(availableColumns, ", "),
		)
	} else {
		hint = fmt.Sprintf("Available columns: [%s]", strings.Join(availableColumns, ", "))
	}

	return &DataFrameError{
		Op:      op,
		Column:  column,
		Message: message,
		Hint:    hint,
	}
}

// NewTypeMismatchError creates an error for type mismatches.
func NewTypeMismatchError(op, column, expectedType, actualType string) *DataFrameError {
	message := fmt.Sprintf("Type mismatch in column '%s': expected %s, got %s", column, expectedType, actualType)
	hint := fmt.Sprintf("Please convert the column type from %s to %s before performing %s operation",
		actualType, expectedType, op)

	return &DataFrameError{
		Op:      op,
		Column:  column,
		Message: message,
		Hint:    hint,
	}
}

// NewIndexOutOfBoundsError creates an error for out-of-bounds index access.
func NewIndexOutOfBoundsError(op string, index, maxIndex int) *DataFrameError {
	message := fmt.Sprintf("Index %d is out of bounds for DataFrame with %d rows", index, maxIndex)
	hint := fmt.Sprintf("Please use valid index in valid range: [0, %d)", maxIndex)

	return &DataFrameError{
		Op:      op,
		Message: message,
		Hint:    hint,
	}
}

// NewUnsupportedOperationError creates an error for unsupported operations with suggestions.
func NewUnsupportedOperationError(op, reason string, supportedOptions []string) *DataFrameError {
	message := fmt.Sprintf("Unsupported operation in %s: %s", op, reason)
	var hint string
	if len(supportedOptions) > 0 {
		hint = fmt.Sprintf("Supported options: %s", strings.Join(supportedOptions, ", "))
	}

	return &DataFrameError{
		Op:      op,
		Message: message,
		Hint:    hint,
	}
}

// NewConfigurationError creates an error for invalid configuration values.
func NewConfigurationError(param string, value interface{}, validOptions []string) *DataFrameError {
	message := fmt.Sprintf("Invalid configuration for parameter '%s', value: %v", param, value)
	var hint string
	if len(validOptions) > 0 {
		hint = fmt.Sprintf("Valid options: %s", strings.Join(validOptions, ", "))
	}

	return &DataFrameError{
		Op:      "configuration",
		Message: message,
		Hint:    hint,
	}
}

// NewInvalidExpressionError creates an error for invalid expressions in operations.
func NewInvalidExpressionError(op, reason string) *DataFrameError {
	return NewInvalidExpressionErrorWithHint(op, reason,
		"Check the expression syntax and ensure all referenced columns exist")
}

// NewInvalidExpressionErrorWithHint creates an error for invalid expressions with a custom hint.
func NewInvalidExpressionErrorWithHint(op, reason, hint string) *DataFrameError {
	return &DataFrameError{
		Op:      op,
		Message: fmt.Sprintf("Invalid expression in %s: %s", op, reason),
		Hint:    hint,
	}
}

// findSimilarColumns finds column names similar to the search term using string similarity.
func findSimilarColumns(searchColumn string, availableColumns []string) []string {
	type columnScore struct {
		name  string
		score int
	}

	var candidates []columnScore
	searchLower := strings.ToLower(searchColumn)

	for _, col := range availableColumns {
		colLower := strings.ToLower(col)
		score := calculateSimilarity(searchLower, colLower)

		// Consider it a good match if similarity is high enough
		if score >= similarityThreshold {
			candidates = append(candidates, columnScore{name: col, score: int(score * scoreMultiplier)})
		}
	}

	// Sort by score (highest first)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].score > candidates[j].score
	})

	// Return top suggestions
	var suggestions []string
	for i, candidate := range candidates {
		if i >= maxSuggestions {
			break
		}
		suggestions = append(suggestions, candidate.name)
	}

	return suggestions
}

// calculateSimilarity calculates string similarity using a simple algorithm
// Returns a score between 0.0 (no similarity) and 1.0 (identical).
func calculateSimilarity(s1, s2 string) float64 {
	if s1 == s2 {
		return 1.0
	}

	// Handle case variations
	if strings.EqualFold(s1, s2) {
		return caseMismatchSimilarity
	}

	// Handle underscore vs camelCase
	s1Norm := strings.ReplaceAll(s1, "_", "")
	s2Norm := strings.ReplaceAll(s2, "_", "")
	if strings.EqualFold(s1Norm, s2Norm) {
		return formatMismatchSimilarity
	}

	// Levenshtein distance-based similarity
	maxLen := len(s1)
	if len(s2) > maxLen {
		maxLen = len(s2)
	}

	if maxLen == 0 {
		return 1.0
	}

	distance := levenshteinDistance(s1, s2)
	return 1.0 - float64(distance)/float64(maxLen)
}

// levenshteinDistance calculates the Levenshtein distance between two strings.
func levenshteinDistance(s1, s2 string) int {
	if s1 == "" {
		return len(s2)
	}
	if s2 == "" {
		return len(s1)
	}

	matrix := make([][]int, len(s1)+1)
	for i := range matrix {
		matrix[i] = make([]int, len(s2)+1)
		matrix[i][0] = i
	}

	for j := 0; j <= len(s2); j++ {
		matrix[0][j] = j
	}

	for i := 1; i <= len(s1); i++ {
		for j := 1; j <= len(s2); j++ {
			if s1[i-1] == s2[j-1] {
				matrix[i][j] = matrix[i-1][j-1]
			} else {
				matrix[i][j] = minThree(
					matrix[i-1][j]+1,   // deletion
					matrix[i][j-1]+1,   // insertion
					matrix[i-1][j-1]+1, // substitution
				)
			}
		}
	}

	return matrix[len(s1)][len(s2)]
}

// minThree returns the minimum of three integers.
func minThree(a, b, c int) int {
	if a < b && a < c {
		return a
	}
	if b < c {
		return b
	}
	return c
}

// Predefined error variables for common cases.
var (
	// ErrEmptyDataFrame indicates operations on empty DataFrames.
	ErrEmptyDataFrame = &DataFrameError{
		Op:      "validation",
		Message: "operation not supported on empty DataFrame",
	}

	// ErrMismatchedLength indicates length mismatches in operations.
	ErrMismatchedLength = &DataFrameError{
		Op:      "validation",
		Message: "arrays must have the same length",
	}

	// ErrInvalidIndex indicates out-of-bounds index access.
	ErrInvalidIndex = &DataFrameError{
		Op:      "indexing",
		Message: "index out of bounds",
	}
)
