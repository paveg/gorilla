package sqlutil

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
)

// BaseTranslator provides common SQL translation functionality.
type BaseTranslator struct {
	tables    map[string]*dataframe.DataFrame
	allocator memory.Allocator
}

// NewBaseTranslator creates a new base translator with common functionality.
func NewBaseTranslator(allocator memory.Allocator) *BaseTranslator {
	return &BaseTranslator{
		tables:    make(map[string]*dataframe.DataFrame),
		allocator: allocator,
	}
}

// RegisterTable registers a DataFrame with a table name.
func (bt *BaseTranslator) RegisterTable(name string, df *dataframe.DataFrame) {
	bt.tables[name] = df
}

// GetTable retrieves a registered table by name.
func (bt *BaseTranslator) GetTable(name string) (*dataframe.DataFrame, bool) {
	df, exists := bt.tables[name]
	return df, exists
}

// GetRegisteredTables returns the list of registered table names.
func (bt *BaseTranslator) GetRegisteredTables() []string {
	var tables []string
	for name := range bt.tables {
		tables = append(tables, name)
	}
	return tables
}

// ClearTables removes all registered tables.
func (bt *BaseTranslator) ClearTables() {
	bt.tables = make(map[string]*dataframe.DataFrame)
}

// ValidateTableExists validates that a table is registered.
func (bt *BaseTranslator) ValidateTableExists(tableName string) error {
	if _, exists := bt.tables[tableName]; !exists {
		return fmt.Errorf("table not found: %s", tableName)
	}
	return nil
}

// GetAllocator returns the memory allocator.
func (bt *BaseTranslator) GetAllocator() memory.Allocator {
	return bt.allocator
}

// StandardizeColumns standardizes column names according to SQL conventions.
func (bt *BaseTranslator) StandardizeColumns(columns []string) []string {
	// For now, return as-is, but this could be extended to handle
	// case-insensitive matching, trimming, etc.
	standardized := make([]string, len(columns))
	copy(standardized, columns)
	return standardized
}

// HandleCommonErrors provides standardized error handling for SQL operations.
func (bt *BaseTranslator) HandleCommonErrors(operation string, err error) error {
	if err == nil {
		return nil
	}

	// Use string-based error matching since errors.Is won't work with newly created error instances
	errMsg := err.Error()
	switch {
	case strings.Contains(errMsg, "table not found"):
		return fmt.Errorf("%s failed: %w", operation, err)
	case strings.Contains(errMsg, "column not found"):
		return fmt.Errorf("%s failed: %w", operation, err)
	case strings.Contains(errMsg, "invalid expression"):
		return fmt.Errorf("%s failed: invalid expression syntax: %w", operation, err)
	default:
		return fmt.Errorf("%s failed: %w", operation, err)
	}
}
