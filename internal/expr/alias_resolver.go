package expr

import (
	"fmt"
	"sort"
	"strings"
)

// AliasResolver handles comprehensive alias resolution for HAVING clauses.
// It supports user-defined aliases, default aggregation names, and GROUP BY columns,
// providing a unified interface for column name resolution.
type AliasResolver struct {
	// aliasMap maps from alias names to actual column names
	// Examples: "total_salary" -> "sum_salary", "emp_count" -> "count_employee_id"
	aliasMap map[string]string

	// reverseMap maps from actual column names back to their preferred aliases
	// This helps with error message suggestions
	reverseMap map[string][]string

	// defaultNames maps from expression strings to their default column names
	// Examples: "sum(col(salary))" -> "sum_salary"
	defaultNames map[string]string

	// groupByColumns contains the original GROUP BY column names
	groupByColumns []string

	// caseInsensitive determines if alias matching should be case-insensitive
	caseInsensitive bool
}

// NewAliasResolver creates a new AliasResolver with the specified configuration.
func NewAliasResolver(caseInsensitive bool) *AliasResolver {
	return &AliasResolver{
		aliasMap:        make(map[string]string),
		reverseMap:      make(map[string][]string),
		defaultNames:    make(map[string]string),
		groupByColumns:  []string{},
		caseInsensitive: caseInsensitive,
	}
}

// AddGroupByColumn adds a GROUP BY column to the resolver.
// GROUP BY columns can be referenced by their original names in HAVING.
func (ar *AliasResolver) AddGroupByColumn(columnName string) {
	ar.groupByColumns = append(ar.groupByColumns, columnName)

	// GROUP BY columns map to themselves
	key := ar.normalizeKey(columnName)
	ar.aliasMap[key] = columnName
	ar.addReverseMapping(columnName, columnName)
}

// AddAggregation adds an aggregation expression with its optional user-defined alias.
// This handles both user aliases and default names for aggregation expressions.
func (ar *AliasResolver) AddAggregation(aggExpr *AggregationExpr) error {
	exprStr := aggExpr.String()
	userAlias := aggExpr.Alias()

	// Generate default column name
	defaultName := ar.generateDefaultName(aggExpr)
	ar.defaultNames[exprStr] = defaultName

	var finalColumnName string
	var aliasName string

	if userAlias != "" {
		// User provided an alias - this takes precedence
		finalColumnName = userAlias
		aliasName = userAlias

		// Check for conflicts with existing aliases
		if existing, exists := ar.aliasMap[ar.normalizeKey(userAlias)]; exists {
			return fmt.Errorf("alias conflict: '%s' is already mapped to '%s'", userAlias, existing)
		}
	} else {
		// No user alias - use default name
		finalColumnName = defaultName
		aliasName = defaultName
	}

	// Add the mapping
	key := ar.normalizeKey(aliasName)
	ar.aliasMap[key] = finalColumnName
	ar.addReverseMapping(finalColumnName, aliasName)

	// Also add mapping for default name if different from user alias
	if userAlias != "" && defaultName != userAlias {
		defaultKey := ar.normalizeKey(defaultName)
		ar.aliasMap[defaultKey] = finalColumnName
		ar.addReverseMapping(finalColumnName, defaultName)
	}

	return nil
}

// ResolveAlias attempts to resolve an alias to its actual column name.
// It checks user aliases, default names, and GROUP BY columns in that order.
func (ar *AliasResolver) ResolveAlias(alias string) (string, bool) {
	key := ar.normalizeKey(alias)

	if columnName, exists := ar.aliasMap[key]; exists {
		return columnName, true
	}

	return "", false
}

// GetAvailableAliases returns all valid aliases for a given column name.
// This is useful for error messages and user assistance.
func (ar *AliasResolver) GetAvailableAliases(columnName string) []string {
	aliases := ar.reverseMap[columnName]

	// Sort for consistent output
	sort.Strings(aliases)
	return aliases
}

// GetAllAvailableAliases returns all aliases that can be used in HAVING clauses.
// This includes GROUP BY columns, user-defined aliases, and default aggregation names.
func (ar *AliasResolver) GetAllAvailableAliases() []string {
	var allAliases []string

	for alias := range ar.aliasMap {
		// Denormalize the key back to its original form
		allAliases = append(allAliases, ar.denormalizeKey(alias))
	}

	// Sort for consistent output
	sort.Strings(allAliases)
	return allAliases
}

// GetColumnNameFromExpression resolves a column name from an aggregation expression string.
// This is used when validating direct aggregation references in HAVING.
func (ar *AliasResolver) GetColumnNameFromExpression(exprStr string) (string, bool) {
	if defaultName, exists := ar.defaultNames[exprStr]; exists {
		// Check if there's a user alias for this expression
		if columnName, found := ar.ResolveAlias(defaultName); found {
			return columnName, true
		}
		return defaultName, true
	}

	return "", false
}

// ValidateAlias checks if the given alias is valid and unambiguous.
func (ar *AliasResolver) ValidateAlias(alias string) error {
	if alias == "" {
		return fmt.Errorf("alias cannot be empty")
	}

	// Check if alias can be resolved
	if _, exists := ar.ResolveAlias(alias); !exists {
		return ar.createAliasNotFoundError(alias)
	}

	return nil
}

// generateDefaultName creates a default column name for an aggregation expression.
func (ar *AliasResolver) generateDefaultName(aggExpr *AggregationExpr) string {
	var aggPrefix string
	switch aggExpr.AggType() {
	case AggSum:
		aggPrefix = "sum"
	case AggCount:
		aggPrefix = "count"
	case AggMean:
		aggPrefix = "mean"
	case AggMin:
		aggPrefix = "min"
	case AggMax:
		aggPrefix = "max"
	default:
		aggPrefix = "agg"
	}

	// Extract column name from the column expression
	columnName := ar.extractColumnName(aggExpr.Column())

	return fmt.Sprintf("%s_%s", aggPrefix, columnName)
}

// extractColumnName extracts the column name from an expression for default naming.
func (ar *AliasResolver) extractColumnName(expr Expr) string {
	switch e := expr.(type) {
	case *ColumnExpr:
		return e.Name()
	case *LiteralExpr:
		return fmt.Sprintf("%v", e.Value())
	default:
		// For complex expressions, use a sanitized version of the string representation
		return ar.sanitizeColumnName(expr.String())
	}
}

// sanitizeColumnName converts an expression string to a valid column name.
func (ar *AliasResolver) sanitizeColumnName(exprStr string) string {
	// Remove common expression syntax and replace with underscores
	sanitized := strings.ReplaceAll(exprStr, "(", "_")
	sanitized = strings.ReplaceAll(sanitized, ")", "_")
	sanitized = strings.ReplaceAll(sanitized, " ", "_")
	sanitized = strings.ReplaceAll(sanitized, ".", "_")

	// Remove duplicate underscores and trim
	for strings.Contains(sanitized, "__") {
		sanitized = strings.ReplaceAll(sanitized, "__", "_")
	}
	sanitized = strings.Trim(sanitized, "_")

	return sanitized
}

// normalizeKey normalizes an alias key for case-insensitive matching if enabled.
func (ar *AliasResolver) normalizeKey(key string) string {
	if ar.caseInsensitive {
		return strings.ToLower(key)
	}
	return key
}

// denormalizeKey converts a normalized key back to a displayable form.
// For case-sensitive mode, this is a no-op. For case-insensitive,
// we need to find the original case from the reverse mapping.
func (ar *AliasResolver) denormalizeKey(normalizedKey string) string {
	if !ar.caseInsensitive {
		return normalizedKey
	}

	// For case-insensitive mode, find the original case from aliasMap
	for originalAlias, columnName := range ar.aliasMap {
		if ar.normalizeKey(originalAlias) == normalizedKey {
			// Return the actual column name that this alias maps to
			return columnName
		}
	}

	// Fallback to the normalized key
	return normalizedKey
}

// addReverseMapping adds a reverse mapping from column name to alias.
func (ar *AliasResolver) addReverseMapping(columnName, alias string) {
	if ar.reverseMap[columnName] == nil {
		ar.reverseMap[columnName] = []string{}
	}

	// Avoid duplicates
	for _, existing := range ar.reverseMap[columnName] {
		if existing == alias {
			return
		}
	}

	ar.reverseMap[columnName] = append(ar.reverseMap[columnName], alias)
}

// createAliasNotFoundError creates a helpful error message for unknown aliases.
func (ar *AliasResolver) createAliasNotFoundError(alias string) error {
	allAliases := ar.GetAllAvailableAliases()

	if len(allAliases) == 0 {
		return fmt.Errorf("alias '%s' not found. No aliases available in current context", alias)
	}

	return fmt.Errorf("alias '%s' not found. Available aliases: %s",
		alias, strings.Join(allAliases, ", "))
}

// BuildAliasResolver creates an AliasResolver from GROUP BY columns and aggregation expressions.
// This is a convenience function for common setup scenarios.
func BuildAliasResolver(
	groupByColumns []string,
	aggregations []*AggregationExpr,
	caseInsensitive bool,
) (*AliasResolver, error) {
	resolver := NewAliasResolver(caseInsensitive)

	// Add GROUP BY columns
	for _, column := range groupByColumns {
		resolver.AddGroupByColumn(column)
	}

	// Add aggregation expressions
	for _, aggExpr := range aggregations {
		if err := resolver.AddAggregation(aggExpr); err != nil {
			return nil, fmt.Errorf("failed to add aggregation %s: %w", aggExpr.String(), err)
		}
	}

	return resolver, nil
}
