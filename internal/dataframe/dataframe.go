// Package dataframe provides high-performance DataFrame operations
package dataframe

import (
	"fmt"
	"strings"
)

// DataFrame represents a table of data with typed columns
type DataFrame struct {
	columns map[string]ISeries
	order   []string // Maintains column order
}

// New creates a new DataFrame from a slice of ISeries
func New(series ...ISeries) *DataFrame {
	columns := make(map[string]ISeries)
	order := make([]string, 0, len(series))

	for _, s := range series {
		name := s.Name()
		columns[name] = s
		order = append(order, name)
	}

	return &DataFrame{
		columns: columns,
		order:   order,
	}
}

// Columns returns the names of all columns in order
func (df *DataFrame) Columns() []string {
	if len(df.order) == 0 {
		return []string{}
	}
	return append([]string(nil), df.order...)
}

// Len returns the number of rows (assumes all columns have same length)
func (df *DataFrame) Len() int {
	if len(df.columns) == 0 {
		return 0
	}

	// Get the first column in order to determine length
	if len(df.order) > 0 {
		if series, exists := df.columns[df.order[0]]; exists {
			return series.Len()
		}
	}

	// Fallback: get any column to determine length
	for _, series := range df.columns {
		return series.Len()
	}
	return 0
}

// Width returns the number of columns
func (df *DataFrame) Width() int {
	return len(df.columns)
}

// Column returns the series for the given column name
func (df *DataFrame) Column(name string) (ISeries, bool) {
	series, exists := df.columns[name]
	return series, exists
}

// Select returns a new DataFrame with only the specified columns
func (df *DataFrame) Select(names ...string) *DataFrame {
	newColumns := make(map[string]ISeries)
	newOrder := make([]string, 0, len(names))

	for _, name := range names {
		if series, exists := df.columns[name]; exists {
			newColumns[name] = series
			newOrder = append(newOrder, name)
		}
	}

	return &DataFrame{
		columns: newColumns,
		order:   newOrder,
	}
}

// Drop returns a new DataFrame without the specified columns
func (df *DataFrame) Drop(names ...string) *DataFrame {
	dropSet := make(map[string]bool)
	for _, name := range names {
		dropSet[name] = true
	}

	newColumns := make(map[string]ISeries)
	newOrder := make([]string, 0, len(df.order))

	for _, name := range df.order {
		if !dropSet[name] {
			newColumns[name] = df.columns[name]
			newOrder = append(newOrder, name)
		}
	}

	return &DataFrame{
		columns: newColumns,
		order:   newOrder,
	}
}

// HasColumn checks if a column exists
func (df *DataFrame) HasColumn(name string) bool {
	_, exists := df.columns[name]
	return exists
}

// String returns a string representation of the DataFrame
func (df *DataFrame) String() string {
	if len(df.columns) == 0 {
		return "DataFrame[empty]"
	}

	parts := []string{fmt.Sprintf("DataFrame[%dx%d]", df.Len(), df.Width())}

	for _, name := range df.order {
		series := df.columns[name]
		parts = append(parts, fmt.Sprintf("  %s: %s", name, series.DataType().String()))
	}

	return strings.Join(parts, "\n")
}

// Release releases all underlying Arrow memory
func (df *DataFrame) Release() {
	for _, series := range df.columns {
		series.Release()
	}
}
