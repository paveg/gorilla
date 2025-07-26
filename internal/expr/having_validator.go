package expr

import (
	"fmt"
	"sort"
	"strings"
)

// HavingValidator provides validation for HAVING clause expressions.
// It ensures that HAVING expressions only reference columns that are available
// in GROUP BY results (either GROUP BY columns or aggregated columns).
// It supports comprehensive alias resolution for user-defined and default names.
type HavingValidator struct {
	aggregationContext *AggregationContext
	groupByColumns     []string
	aliasResolver      *AliasResolver // Enhanced alias resolution support
}

// NewHavingValidator creates a new HavingValidator with the given aggregation context
// and GROUP BY columns. For backward compatibility.
func NewHavingValidator(aggregationContext *AggregationContext, groupByColumns []string) *HavingValidator {
	return &HavingValidator{
		aggregationContext: aggregationContext,
		groupByColumns:     groupByColumns,
		aliasResolver:      nil, // Backward compatibility - no alias resolution
	}
}

// NewHavingValidatorWithAlias creates a new HavingValidator with enhanced alias resolution.
// This is the recommended constructor for full HAVING clause functionality.
func NewHavingValidatorWithAlias(
	aggregationContext *AggregationContext,
	groupByColumns []string,
	aliasResolver *AliasResolver,
) *HavingValidator {
	return &HavingValidator{
		aggregationContext: aggregationContext,
		groupByColumns:     groupByColumns,
		aliasResolver:      aliasResolver,
	}
}

// ValidateExpression recursively validates a HAVING expression to ensure all
// column references are valid (either GROUP BY columns or aggregated columns).
func (hv *HavingValidator) ValidateExpression(expr Expr) error {
	if expr == nil {
		return fmt.Errorf("expression cannot be nil")
	}

	switch e := expr.(type) {
	case *ColumnExpr:
		return hv.validateColumnReference(e.Name())
	case *AggregationExpr:
		return hv.validateAggregationExpression(e)
	case *BinaryExpr:
		return hv.validateBinaryExpression(e)
	case *FunctionExpr:
		return hv.validateFunctionExpression(e)
	case *LiteralExpr:
		// Literals are always valid as they don't reference columns
		return nil
	case *CaseExpr:
		return hv.validateCaseExpression(e)
	case *UnaryExpr:
		return hv.validateUnaryExpression(e)
	default:
		// For unknown expression types, assume they're valid
		// This allows for future extensibility
		return nil
	}
}

// validateColumnReference checks if a column reference is valid in HAVING context.
// Valid columns are either GROUP BY columns or aggregated columns.
// Uses alias resolution when available for enhanced user experience.
func (hv *HavingValidator) validateColumnReference(columnName string) error {
	// Use alias resolver if available for enhanced resolution
	if hv.aliasResolver != nil {
		return hv.aliasResolver.ValidateAlias(columnName)
	}

	// Fallback to original validation logic for backward compatibility
	return hv.validateColumnReferenceLegacy(columnName)
}

// validateColumnReferenceLegacy provides the original column validation logic.
// This is used for backward compatibility when no alias resolver is provided.
func (hv *HavingValidator) validateColumnReferenceLegacy(columnName string) error {
	// Check if it's a GROUP BY column
	for _, groupCol := range hv.groupByColumns {
		if groupCol == columnName {
			return nil
		}
	}

	// Check if it's an aggregated column (exists in aggregation context reverse mapping)
	if _, exists := hv.aggregationContext.GetExpression(columnName); exists {
		return nil
	}

	// Column is not available in HAVING context
	return hv.createColumnNotAvailableError(columnName)
}

// validateAggregationExpression checks if an aggregation expression is valid.
// It must exist in the aggregation context or be resolvable via alias resolver.
func (hv *HavingValidator) validateAggregationExpression(aggExpr *AggregationExpr) error {
	exprStr := aggExpr.String()

	// Use alias resolver if available for enhanced resolution
	if hv.aliasResolver != nil {
		// Check if the expression can be resolved through alias resolver
		if _, exists := hv.aliasResolver.GetColumnNameFromExpression(exprStr); exists {
			return nil
		}
		// Return alias resolver's error (which provides better messages)
		return hv.aliasResolver.ValidateAlias(exprStr)
	}

	// Fallback to original validation logic for backward compatibility
	if hv.aggregationContext.HasMapping(exprStr) {
		return nil
	}

	// Aggregation not found in context
	return hv.createAggregationNotFoundError(exprStr)
}

// validateBinaryExpression validates both sides of a binary expression.
func (hv *HavingValidator) validateBinaryExpression(binaryExpr *BinaryExpr) error {
	// Validate left side
	if err := hv.ValidateExpression(binaryExpr.Left()); err != nil {
		return err
	}

	// Validate right side
	if err := hv.ValidateExpression(binaryExpr.Right()); err != nil {
		return err
	}

	return nil
}

// validateFunctionExpression validates all arguments of a function expression.
func (hv *HavingValidator) validateFunctionExpression(funcExpr *FunctionExpr) error {
	// Validate all function arguments
	for _, arg := range funcExpr.Args() {
		if err := hv.ValidateExpression(arg); err != nil {
			return err
		}
	}

	return nil
}

// validateCaseExpression validates all parts of a CASE expression.
func (hv *HavingValidator) validateCaseExpression(caseExpr *CaseExpr) error {
	// Validate all WHEN clauses
	for _, when := range caseExpr.Whens() {
		if err := hv.ValidateExpression(when.condition); err != nil {
			return err
		}
		if err := hv.ValidateExpression(when.value); err != nil {
			return err
		}
	}

	// Validate ELSE clause if it exists
	if elseExpr := caseExpr.ElseValue(); elseExpr != nil {
		if err := hv.ValidateExpression(elseExpr); err != nil {
			return err
		}
	}

	return nil
}

// validateUnaryExpression validates the operand of a unary expression.
func (hv *HavingValidator) validateUnaryExpression(unaryExpr *UnaryExpr) error {
	return hv.ValidateExpression(unaryExpr.Operand())
}

// createColumnNotAvailableError creates a helpful error message when a column
// is not available in HAVING context.
func (hv *HavingValidator) createColumnNotAvailableError(columnName string) error {
	var availableColumns []string

	// Add GROUP BY columns
	availableColumns = append(availableColumns, hv.groupByColumns...)

	// Add aggregated columns
	for _, aggCol := range hv.aggregationContext.AllMappings() {
		availableColumns = append(availableColumns, aggCol)
	}

	// Sort for consistent error messages
	sort.Strings(availableColumns)

	if len(availableColumns) == 0 {
		return fmt.Errorf("column '%s' is not available in HAVING clause. "+
			"No columns available in GROUP BY result", columnName)
	}

	return fmt.Errorf("column '%s' is not available in HAVING clause. Available columns: %s",
		columnName, strings.Join(availableColumns, ", "))
}

// createAggregationNotFoundError creates a helpful error message when an
// aggregation expression is not found in the context.
func (hv *HavingValidator) createAggregationNotFoundError(exprStr string) error {
	var availableAggregations []string

	// Get all available aggregation expressions
	for aggExpr := range hv.aggregationContext.AllMappings() {
		availableAggregations = append(availableAggregations, aggExpr)
	}

	// Sort for consistent error messages
	sort.Strings(availableAggregations)

	if len(availableAggregations) == 0 {
		return fmt.Errorf("aggregation '%s' not found in aggregation context. "+
			"No aggregations available in GROUP BY result", exprStr)
	}

	return fmt.Errorf("aggregation '%s' not found in aggregation context. Available aggregations: %s",
		exprStr, strings.Join(availableAggregations, ", "))
}

// GetAvailableColumns returns all columns available in HAVING context
// (GROUP BY columns + aggregated columns). Uses alias resolver when available.
func (hv *HavingValidator) GetAvailableColumns() []string {
	// Use alias resolver if available for enhanced column listing
	if hv.aliasResolver != nil {
		return hv.aliasResolver.GetAllAvailableAliases()
	}

	// Fallback to original logic for backward compatibility
	var columns []string

	// Add GROUP BY columns
	columns = append(columns, hv.groupByColumns...)

	// Add aggregated columns
	for _, aggCol := range hv.aggregationContext.AllMappings() {
		columns = append(columns, aggCol)
	}

	// Sort for consistent ordering
	sort.Strings(columns)

	return columns
}

// GetAvailableAggregations returns all aggregation expressions available in the context.
func (hv *HavingValidator) GetAvailableAggregations() []string {
	var aggregations []string

	for aggExpr := range hv.aggregationContext.AllMappings() {
		aggregations = append(aggregations, aggExpr)
	}

	// Sort for consistent ordering
	sort.Strings(aggregations)

	return aggregations
}

// BuildHavingValidatorWithAlias creates a complete HAVING validator with alias resolution
// from GROUP BY columns and aggregation expressions. This is a convenience function
// that sets up both the AggregationContext and AliasResolver automatically.
func BuildHavingValidatorWithAlias(
	groupByColumns []string,
	aggregations []*AggregationExpr,
	caseInsensitive bool,
) (*HavingValidator, error) {
	// Build aggregation context
	aggregationContext := NewAggregationContext()
	for _, agg := range aggregations {
		exprStr := agg.String()
		columnName := ExpressionToColumnName(agg)
		aggregationContext.AddMapping(exprStr, columnName)
	}

	// Build alias resolver
	aliasResolver, err := BuildAliasResolver(groupByColumns, aggregations, caseInsensitive)
	if err != nil {
		return nil, fmt.Errorf("failed to build alias resolver: %w", err)
	}

	// Create validator with both context and alias resolver
	return NewHavingValidatorWithAlias(aggregationContext, groupByColumns, aliasResolver), nil
}
