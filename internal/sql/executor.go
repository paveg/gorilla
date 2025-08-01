package sql

import (
	"errors"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
)

// safeInt64ToInt safely converts int64 to int with bounds checking.
func safeInt64ToInt(value int64) (int, error) {
	if value < 0 {
		return 0, fmt.Errorf("negative value not allowed: %d", value)
	}
	if value > math.MaxInt {
		return 0, fmt.Errorf("value exceeds int range: %d", value)
	}
	return int(value), nil
}

// Executor executes SQL queries against registered DataFrames.
type Executor struct {
	translator *SQLTranslator
	mem        memory.Allocator
}

// SQLExecutor is deprecated, use Executor instead.
type SQLExecutor struct { //nolint:revive // Maintained for backward compatibility
	translator *SQLTranslator
	mem        memory.Allocator
}

// NewExecutor creates a new SQL executor.
func NewExecutor(mem memory.Allocator) *Executor {
	return &Executor{
		translator: NewSQLTranslator(mem),
		mem:        mem,
	}
}

// NewSQLExecutor creates a new SQL executor.
// Deprecated: Use NewExecutor instead.
func NewSQLExecutor(mem memory.Allocator) *SQLExecutor {
	return &SQLExecutor{
		translator: NewSQLTranslator(mem),
		mem:        mem,
	}
}

// RegisterTable registers a DataFrame with a table name.
func (e *SQLExecutor) RegisterTable(name string, df *dataframe.DataFrame) {
	e.translator.RegisterTable(name, df)
}

// Execute executes a SQL query and returns the result DataFrame.
func (e *SQLExecutor) Execute(query string) (*dataframe.DataFrame, error) {
	// Parse SQL query
	stmt, err := ParseSQL(query)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	// Validate SQL syntax
	if validationErr := e.translator.ValidateSQLSyntax(stmt); validationErr != nil {
		return nil, fmt.Errorf("validation error: %w", validationErr)
	}

	// Translate to LazyFrame operations
	lazy, err := e.translator.TranslateStatement(stmt)
	if err != nil {
		return nil, fmt.Errorf("translation error: %w", err)
	}
	defer lazy.Release()

	// Handle LIMIT/OFFSET at execution level
	if selectStmt, ok := stmt.(*SelectStatement); ok && selectStmt.LimitClause != nil {
		return e.executeWithLimit(lazy, selectStmt.LimitClause)
	}

	// Execute the query
	result, err := lazy.Collect()
	if err != nil {
		return nil, fmt.Errorf("execution error: %w", err)
	}

	return result, nil
}

// executeWithLimit executes query with LIMIT/OFFSET handling.
func (e *SQLExecutor) executeWithLimit(
	lazy *dataframe.LazyFrame,
	limitClause *LimitClause,
) (*dataframe.DataFrame, error) {
	// First collect the full result
	fullResult, err := lazy.Collect()
	if err != nil {
		return nil, fmt.Errorf("execution error: %w", err)
	}
	// Note: Don't defer release here since we're returning the result

	// Validate result is not nil
	if fullResult == nil {
		return nil, errors.New("query execution returned nil result")
	}

	// Apply OFFSET and LIMIT
	totalRows := fullResult.Len()

	// Safe conversion using helper function with explicit bounds checking
	offset, err := safeInt64ToInt(limitClause.Offset)
	if err != nil {
		defer fullResult.Release()
		return e.createEmptyDataFrame(fullResult), nil
	}

	// Handle OFFSET-only queries first (Count = OffsetOnlyLimit indicates no LIMIT)
	if limitClause.Count == OffsetOnlyLimit {
		// Validate offset bounds against actual data size
		if offset >= totalRows {
			// Return empty DataFrame with same schema
			emptyResult := e.createEmptyDataFrame(fullResult)
			defer fullResult.Release()
			return emptyResult, nil
		}
		// OFFSET-only: return all rows from offset to end
		slicedResult := fullResult.Slice(offset, totalRows)
		defer fullResult.Release()
		return slicedResult, nil
	}

	// Validate LIMIT value using same safe conversion pattern (only for non-OFFSET-only queries)
	_, err = safeInt64ToInt(limitClause.Count)
	if err != nil {
		defer fullResult.Release()
		return nil, fmt.Errorf("invalid LIMIT value: %w", err)
	}

	// Validate offset bounds against actual data size
	if offset >= totalRows {
		// Return empty DataFrame with same schema
		emptyResult := e.createEmptyDataFrame(fullResult)
		defer fullResult.Release()
		return emptyResult, nil
	}

	// Apply LIMIT and OFFSET using DataFrame slicing
	startIdx := offset

	// Convert LIMIT count safely (we already validated it's not -1)
	limitCount, err := safeInt64ToInt(limitClause.Count)
	if err != nil {
		defer fullResult.Release()
		return nil, fmt.Errorf("invalid LIMIT value: %w", err)
	}

	// Handle LIMIT 0 - return empty DataFrame with same schema
	if limitCount == 0 {
		emptyResult := e.createEmptyDataFrame(fullResult)
		defer fullResult.Release()
		return emptyResult, nil
	}

	// Calculate end index
	endIdx := startIdx + limitCount
	if endIdx > totalRows {
		endIdx = totalRows
	}

	// Use DataFrame.Slice() to get the desired subset
	slicedResult := fullResult.Slice(startIdx, endIdx)
	defer fullResult.Release()

	return slicedResult, nil
}

// createEmptyDataFrame creates an empty DataFrame with the same schema.
func (e *SQLExecutor) createEmptyDataFrame(template *dataframe.DataFrame) *dataframe.DataFrame {
	// For simplicity, return an empty DataFrame using the existing Slice method
	return template.Slice(0, 0)
}

// GetRegisteredTables returns the list of registered table names.
func (e *SQLExecutor) GetRegisteredTables() []string {
	return e.translator.GetRegisteredTables()
}

// ClearTables removes all registered tables.
func (e *SQLExecutor) ClearTables() {
	e.translator.ClearTables()
}

// Explain returns the execution plan for a SQL query.
func (e *SQLExecutor) Explain(query string) (string, error) {
	// Parse SQL query
	stmt, err := ParseSQL(query)
	if err != nil {
		return "", fmt.Errorf("parse error: %w", err)
	}

	// Validate SQL syntax
	if validationErr := e.translator.ValidateSQLSyntax(stmt); validationErr != nil {
		return "", fmt.Errorf("validation error: %w", validationErr)
	}

	// Translate to LazyFrame operations
	lazy, err := e.translator.TranslateStatement(stmt)
	if err != nil {
		return "", fmt.Errorf("translation error: %w", err)
	}
	defer lazy.Release()

	// Return the execution plan (using String representation for now)
	return lazy.String(), nil
}

// ValidateQuery validates a SQL query without executing it.
func (e *SQLExecutor) ValidateQuery(query string) error {
	// Parse SQL query
	stmt, err := ParseSQL(query)
	if err != nil {
		return fmt.Errorf("parse error: %w", err)
	}

	// Validate SQL syntax
	if validateErr := e.translator.ValidateSQLSyntax(stmt); validateErr != nil {
		return fmt.Errorf("validation error: %w", validateErr)
	}

	// Check table references
	if selectStmt, ok := stmt.(*SelectStatement); ok {
		if selectStmt.FromClause != nil {
			tableName := selectStmt.FromClause.TableName
			if _, exists := e.translator.tables[tableName]; !exists {
				return fmt.Errorf("table not found: %s", tableName)
			}
		}
	}

	return nil
}

// BatchExecute executes multiple SQL statements in sequence.
func (e *SQLExecutor) BatchExecute(queries []string) ([]*dataframe.DataFrame, error) {
	var results []*dataframe.DataFrame

	for i, query := range queries {
		result, err := e.Execute(query)
		if err != nil {
			// Clean up already executed results
			for _, r := range results {
				r.Release()
			}
			return nil, fmt.Errorf("error executing query %d: %w", i+1, err)
		}
		results = append(results, result)
	}

	return results, nil
}

// Query represents a prepared SQL query for reuse.
type Query struct {
	statement  Statement
	translator *SQLTranslator
	mem        memory.Allocator
	validated  bool
}

// PrepareQuery prepares a SQL query for multiple executions.
func (e *SQLExecutor) PrepareQuery(query string) (*Query, error) {
	// Parse SQL query
	stmt, err := ParseSQL(query)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	// Validate SQL syntax
	if validationErr := e.translator.ValidateSQLSyntax(stmt); validationErr != nil {
		return nil, fmt.Errorf("validation error: %w", validationErr)
	}

	return &Query{
		statement:  stmt,
		translator: e.translator,
		mem:        e.mem,
		validated:  true,
	}, nil
}

// Execute executes a prepared SQL query.
func (q *Query) Execute() (*dataframe.DataFrame, error) {
	if !q.validated {
		return nil, errors.New("query not validated")
	}

	// Translate to LazyFrame operations
	lazy, err := q.translator.TranslateStatement(q.statement)
	if err != nil {
		return nil, fmt.Errorf("translation error: %w", err)
	}
	defer lazy.Release()

	// Handle LIMIT/OFFSET if present
	if selectStmt, ok := q.statement.(*SelectStatement); ok && selectStmt.LimitClause != nil {
		return q.executeWithLimit(lazy, selectStmt.LimitClause)
	}

	// Execute the query
	result, err := lazy.Collect()
	if err != nil {
		return nil, fmt.Errorf("execution error: %w", err)
	}

	return result, nil
}

// executeWithLimit executes prepared query with LIMIT/OFFSET handling.
func (q *Query) executeWithLimit(lazy *dataframe.LazyFrame, limitClause *LimitClause) (*dataframe.DataFrame, error) {
	// This is a simplified version - in a full implementation, we would
	// integrate LIMIT/OFFSET into the LazyFrame operations for better performance

	// First collect the full result
	fullResult, err := lazy.Collect()
	if err != nil {
		return nil, fmt.Errorf("execution error: %w", err)
	}
	defer fullResult.Release()

	// Validate result is not nil
	if fullResult == nil {
		return nil, errors.New("query execution returned nil result")
	}

	// Apply OFFSET and LIMIT
	totalRows := fullResult.Len()

	// Safe conversion using helper function with explicit bounds checking
	offset, err := safeInt64ToInt(limitClause.Offset)
	if err != nil {
		emptyResult := fullResult.Slice(0, 0)
		return emptyResult, nil
	}

	// Handle OFFSET-only queries first (Count = OffsetOnlyLimit indicates no LIMIT)
	if limitClause.Count == OffsetOnlyLimit {
		// Validate offset bounds against actual data size
		if offset >= totalRows {
			// Return empty DataFrame with same schema
			emptyResult := fullResult.Slice(0, 0)
			return emptyResult, nil
		}
		// OFFSET-only: return all rows from offset to end
		slicedResult := fullResult.Slice(offset, totalRows)
		return slicedResult, nil
	}

	// Validate offset bounds against actual data size
	if offset >= totalRows {
		// Return empty DataFrame with same schema
		emptyResult := fullResult.Slice(0, 0)
		return emptyResult, nil
	}

	// Apply LIMIT and OFFSET using DataFrame slicing
	startIdx := offset

	// Convert LIMIT count safely (we already validated it's not -1)
	limitCount, err := safeInt64ToInt(limitClause.Count)
	if err != nil {
		return nil, fmt.Errorf("invalid LIMIT value: %w", err)
	}

	// Handle LIMIT 0 - return empty DataFrame with same schema
	if limitCount == 0 {
		emptyResult := fullResult.Slice(0, 0)
		return emptyResult, nil
	}

	// Calculate end index
	endIdx := startIdx + limitCount
	if endIdx > totalRows {
		endIdx = totalRows
	}

	// Use DataFrame.Slice() to get the desired subset
	slicedResult := fullResult.Slice(startIdx, endIdx)

	return slicedResult, nil
}

// String returns the SQL query string.
func (q *Query) String() string {
	if q.statement != nil {
		return q.statement.String()
	}
	return ""
}
