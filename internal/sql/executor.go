package sql

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
)

// SQLExecutor executes SQL queries against registered DataFrames
type SQLExecutor struct {
	translator *SQLTranslator
	mem        memory.Allocator
}

// NewSQLExecutor creates a new SQL executor
func NewSQLExecutor(mem memory.Allocator) *SQLExecutor {
	return &SQLExecutor{
		translator: NewSQLTranslator(mem),
		mem:        mem,
	}
}

// RegisterTable registers a DataFrame with a table name
func (e *SQLExecutor) RegisterTable(name string, df *dataframe.DataFrame) {
	e.translator.RegisterTable(name, df)
}

// Execute executes a SQL query and returns the result DataFrame
func (e *SQLExecutor) Execute(query string) (*dataframe.DataFrame, error) {
	// Parse SQL query
	stmt, err := ParseSQL(query)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	// Validate SQL syntax
	if err := e.translator.ValidateSQLSyntax(stmt); err != nil {
		return nil, fmt.Errorf("validation error: %w", err)
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

// executeWithLimit executes query with LIMIT/OFFSET handling
func (e *SQLExecutor) executeWithLimit(
	lazy *dataframe.LazyFrame,
	limitClause *LimitClause,
) (*dataframe.DataFrame, error) {
	// First collect the full result
	fullResult, err := lazy.Collect()
	if err != nil {
		return nil, fmt.Errorf("execution error: %w", err)
	}
	defer fullResult.Release()

	// Apply OFFSET and LIMIT
	totalRows := fullResult.Len()

	// Safe conversion with explicit bounds checking to prevent integer overflow
	const maxInt = int(^uint(0) >> 1) // Maximum value for int type

	// Validate and convert OFFSET with explicit bounds checking
	var offset int
	if limitClause.Offset < 0 {
		return e.createEmptyDataFrame(fullResult), nil
	} else if limitClause.Offset > int64(maxInt) {
		return e.createEmptyDataFrame(fullResult), nil
	} else {
		offset = int(limitClause.Offset) // Safe conversion after explicit bounds check
	}

	// Validate LIMIT value
	if limitClause.Count < 0 || limitClause.Count > int64(maxInt) {
		return nil, fmt.Errorf("invalid LIMIT/OFFSET values: offset=%d, count=%d", limitClause.Offset, limitClause.Count)
	}

	// Validate offset bounds against actual data size
	if offset >= totalRows {
		// Return empty DataFrame with same schema
		return e.createEmptyDataFrame(fullResult), nil
	}

	// For now, just return the original DataFrame
	// TODO: implement proper LIMIT/OFFSET functionality with safe count conversion
	_ = offset
	return fullResult, nil
}

// createEmptyDataFrame creates an empty DataFrame with the same schema
func (e *SQLExecutor) createEmptyDataFrame(template *dataframe.DataFrame) *dataframe.DataFrame {
	// For simplicity, return an empty DataFrame using the existing Slice method
	return template.Slice(0, 0)
}

// GetRegisteredTables returns the list of registered table names
func (e *SQLExecutor) GetRegisteredTables() []string {
	return e.translator.GetRegisteredTables()
}

// ClearTables removes all registered tables
func (e *SQLExecutor) ClearTables() {
	e.translator.ClearTables()
}

// Explain returns the execution plan for a SQL query
func (e *SQLExecutor) Explain(query string) (string, error) {
	// Parse SQL query
	stmt, err := ParseSQL(query)
	if err != nil {
		return "", fmt.Errorf("parse error: %w", err)
	}

	// Validate SQL syntax
	if err := e.translator.ValidateSQLSyntax(stmt); err != nil {
		return "", fmt.Errorf("validation error: %w", err)
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

// ValidateQuery validates a SQL query without executing it
func (e *SQLExecutor) ValidateQuery(query string) error {
	// Parse SQL query
	stmt, err := ParseSQL(query)
	if err != nil {
		return fmt.Errorf("parse error: %w", err)
	}

	// Validate SQL syntax
	if err := e.translator.ValidateSQLSyntax(stmt); err != nil {
		return fmt.Errorf("validation error: %w", err)
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

// BatchExecute executes multiple SQL statements in sequence
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

// SQLQuery represents a prepared SQL query for reuse
type SQLQuery struct {
	statement  SQLStatement
	translator *SQLTranslator
	mem        memory.Allocator
	validated  bool
}

// PrepareQuery prepares a SQL query for multiple executions
func (e *SQLExecutor) PrepareQuery(query string) (*SQLQuery, error) {
	// Parse SQL query
	stmt, err := ParseSQL(query)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	// Validate SQL syntax
	if err := e.translator.ValidateSQLSyntax(stmt); err != nil {
		return nil, fmt.Errorf("validation error: %w", err)
	}

	return &SQLQuery{
		statement:  stmt,
		translator: e.translator,
		mem:        e.mem,
		validated:  true,
	}, nil
}

// Execute executes a prepared SQL query
func (q *SQLQuery) Execute() (*dataframe.DataFrame, error) {
	if !q.validated {
		return nil, fmt.Errorf("query not validated")
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

// executeWithLimit executes prepared query with LIMIT/OFFSET handling
func (q *SQLQuery) executeWithLimit(lazy *dataframe.LazyFrame, limitClause *LimitClause) (*dataframe.DataFrame, error) {
	// This is a simplified version - in a full implementation, we would
	// integrate LIMIT/OFFSET into the LazyFrame operations for better performance

	// First collect the full result
	fullResult, err := lazy.Collect()
	if err != nil {
		return nil, fmt.Errorf("execution error: %w", err)
	}
	defer fullResult.Release()

	// Apply OFFSET and LIMIT
	totalRows := fullResult.Len()

	// Safe conversion with explicit bounds checking to prevent integer overflow
	const maxInt = int(^uint(0) >> 1) // Maximum value for int type

	// Validate and convert OFFSET with explicit bounds checking
	var offset int
	if limitClause.Offset < 0 {
		return fullResult, nil
	} else if limitClause.Offset > int64(maxInt) {
		return fullResult, nil
	} else {
		offset = int(limitClause.Offset) // Safe conversion after explicit bounds check
	}

	// Validate LIMIT value
	if limitClause.Count < 0 || limitClause.Count > int64(maxInt) {
		return nil, fmt.Errorf("invalid LIMIT/OFFSET values: offset=%d, count=%d", limitClause.Offset, limitClause.Count)
	}

	// Validate offset
	if offset >= totalRows {
		// Return empty DataFrame with same schema - this would need proper implementation
		// For now, return the full result (this is a placeholder)
		return fullResult, nil
	}

	// For now, return full result - proper slicing would be implemented here
	// Note: We return the fullResult directly without Retain since it's already managed
	return fullResult, nil
}

// String returns the SQL query string
func (q *SQLQuery) String() string {
	if q.statement != nil {
		return q.statement.String()
	}
	return ""
}
