package expr

import (
	"fmt"
	"strings"
)

// AggregationContext manages mapping between aggregated column references
// and their actual column names in GROUP BY results.
//
// When HAVING expressions reference aggregation results like SUM(sales),
// the context maps these to the actual column names in the aggregated DataFrame
// (e.g., "sum_sales").
type AggregationContext struct {
	// columnMappings maps from original column expressions to aggregated column names
	// Example: "SUM(sales)" -> "sum_sales"
	columnMappings map[string]string

	// reverseMapping maps from aggregated column names back to expressions
	// Example: "sum_sales" -> "SUM(sales)"
	reverseMapping map[string]string
}

// NewAggregationContext creates a new AggregationContext
func NewAggregationContext() *AggregationContext {
	return &AggregationContext{
		columnMappings: make(map[string]string),
		reverseMapping: make(map[string]string),
	}
}

// AddMapping adds a mapping between an aggregation expression and its column name
func (ac *AggregationContext) AddMapping(exprStr, columnName string) {
	// Clean up old reverse mapping if expression already exists
	if oldColumnName, exists := ac.columnMappings[exprStr]; exists {
		delete(ac.reverseMapping, oldColumnName)
	}

	// Clean up old forward mapping if column name already exists
	if oldExprStr, exists := ac.reverseMapping[columnName]; exists {
		delete(ac.columnMappings, oldExprStr)
	}

	ac.columnMappings[exprStr] = columnName
	ac.reverseMapping[columnName] = exprStr
}

// GetColumnName returns the column name for the given expression string
func (ac *AggregationContext) GetColumnName(exprStr string) (string, bool) {
	columnName, exists := ac.columnMappings[exprStr]
	return columnName, exists
}

// GetExpression returns the expression string for the given column name
func (ac *AggregationContext) GetExpression(columnName string) (string, bool) {
	exprStr, exists := ac.reverseMapping[columnName]
	return exprStr, exists
}

// HasMapping checks if a mapping exists for the given expression string
func (ac *AggregationContext) HasMapping(exprStr string) bool {
	_, exists := ac.columnMappings[exprStr]
	return exists
}

// AllMappings returns all column mappings
func (ac *AggregationContext) AllMappings() map[string]string {
	result := make(map[string]string)
	for k, v := range ac.columnMappings {
		result[k] = v
	}
	return result
}

// Clear removes all mappings
func (ac *AggregationContext) Clear() {
	ac.columnMappings = make(map[string]string)
	ac.reverseMapping = make(map[string]string)
}

// String returns a string representation of the context
func (ac *AggregationContext) String() string {
	if len(ac.columnMappings) == 0 {
		return "AggregationContext{empty}"
	}

	var mappings []string
	for expr, col := range ac.columnMappings {
		mappings = append(mappings, fmt.Sprintf("%s->%s", expr, col))
	}
	return fmt.Sprintf("AggregationContext{%s}", strings.Join(mappings, ", "))
}

// ExpressionToColumnName converts an aggregation expression to its standardized column name
// This is used to generate consistent column names for aggregated results
func ExpressionToColumnName(expr Expr) string {
	switch e := expr.(type) {
	case *AggregationExpr:
		return e.String()
	case *ColumnExpr:
		return e.String()
	case *LiteralExpr:
		return e.String()
	case *BinaryExpr:
		return e.String()
	case *FunctionExpr:
		return e.String()
	default:
		return fmt.Sprintf("expr_%T", expr)
	}
}

// BuildContextFromAggregations creates an AggregationContext from a list of aggregation expressions
func BuildContextFromAggregations(aggregations []*AggregationExpr) *AggregationContext {
	ctx := NewAggregationContext()

	for _, agg := range aggregations {
		exprStr := agg.String()
		columnName := ExpressionToColumnName(agg)
		ctx.AddMapping(exprStr, columnName)
	}

	return ctx
}
