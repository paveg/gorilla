package sql

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/expr"
)

// SQLTranslator translates SQL AST to DataFrame operations
type SQLTranslator struct {
	tables    map[string]*dataframe.DataFrame
	evaluator *expr.Evaluator
}

// NewSQLTranslator creates a new SQL translator
func NewSQLTranslator(mem memory.Allocator) *SQLTranslator {
	return &SQLTranslator{
		tables:    make(map[string]*dataframe.DataFrame),
		evaluator: expr.NewEvaluator(mem),
	}
}

// RegisterTable registers a DataFrame with a table name for SQL queries
func (t *SQLTranslator) RegisterTable(name string, df *dataframe.DataFrame) {
	t.tables[name] = df
}

// TranslateStatement translates a SQL statement to a LazyFrame
func (t *SQLTranslator) TranslateStatement(stmt SQLStatement) (*dataframe.LazyFrame, error) {
	switch s := stmt.(type) {
	case *SelectStatement:
		return t.translateSelect(s)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// translateSelect translates a SELECT statement to LazyFrame operations
func (t *SQLTranslator) translateSelect(stmt *SelectStatement) (*dataframe.LazyFrame, error) {
	// Start with FROM clause to get base DataFrame
	var lazy *dataframe.LazyFrame
	if stmt.FromClause != nil {
		df, exists := t.tables[stmt.FromClause.TableName]
		if !exists {
			return nil, fmt.Errorf("table not found: %s", stmt.FromClause.TableName)
		}
		lazy = df.Lazy()
	} else {
		return nil, fmt.Errorf("FROM clause is required")
	}

	// Apply WHERE clause
	if stmt.WhereClause != nil {
		lazy = lazy.Filter(stmt.WhereClause.Condition)
	}

	// Apply GROUP BY clause
	if stmt.GroupByClause != nil {
		groupCols, err := t.extractColumnNames(stmt.GroupByClause.Columns)
		if err != nil {
			return nil, fmt.Errorf("error in GROUP BY: %w", err)
		}

		// Collect aggregations from SELECT list
		aggExprs := t.extractAggregations(stmt.SelectList)

		if len(aggExprs) > 0 {
			// Extract aggregations with their aliases from SELECT list
			aggPtrs := make([]*expr.AggregationExpr, 0, len(aggExprs))
			for _, item := range stmt.SelectList {
				if !item.IsWildcard {
					if aggExpr, ok := item.Expression.(*expr.AggregationExpr); ok {
						// Apply alias if provided
						if item.Alias != "" {
							aggExpr = aggExpr.As(item.Alias)
						}
						aggPtrs = append(aggPtrs, aggExpr)
					}
				}
			}

			// Apply HAVING clause if present
			if stmt.HavingClause != nil {
				// Build aggregation context for HAVING validation
				aggContext := expr.NewAggregationContext()

				for _, agg := range aggPtrs {
					exprStr := agg.String()

					// Use the actual alias if provided, otherwise use a generated name with SQL-appropriate prefix
					var columnName string
					if agg.Alias() != "" {
						columnName = agg.Alias()
					} else {
						// Generate SQL-appropriate default name (e.g., "avg_salary" not "mean_salary")
						tempResolver := expr.NewAliasResolver(false)
						columnName = tempResolver.GenerateDefaultName(agg)
					}

					aggContext.AddMapping(exprStr, columnName)
				}

				// Build alias resolver from SELECT list
				aliasResolver := expr.NewAliasResolver(false) // case-sensitive by default

				// Add GROUP BY columns to alias resolver
				for _, col := range groupCols {
					aliasResolver.AddGroupByColumn(col)
				}

				// Add aggregations with their aliases
				for _, item := range stmt.SelectList {
					if !item.IsWildcard {
						if aggExpr, ok := item.Expression.(*expr.AggregationExpr); ok {
							// If the item has an alias, update the aggregation expression
							if item.Alias != "" {
								aggExpr = aggExpr.As(item.Alias)
							}
							if err := aliasResolver.AddAggregation(aggExpr); err != nil {
								return nil, fmt.Errorf("error adding aggregation to alias resolver: %w", err)
							}
						}
					}
				}

				// Create HAVING validator with alias support
				havingValidator := expr.NewHavingValidatorWithAlias(aggContext, groupCols, aliasResolver)

				// Validate HAVING expression
				if err := havingValidator.ValidateExpression(stmt.HavingClause.Condition); err != nil {
					return nil, fmt.Errorf("HAVING validation error: %w", err)
				}

				// Resolve aliases in HAVING expression
				resolvedHavingCondition, err := t.resolveAliasesInExpression(stmt.HavingClause.Condition, aliasResolver, aggContext)
				if err != nil {
					return nil, fmt.Errorf("error resolving aliases in HAVING clause: %w", err)
				}

				// Use AggWithHaving for combined GROUP BY + HAVING operation
				lazy = lazy.GroupBy(groupCols...).AggWithHaving(resolvedHavingCondition, aggPtrs...)
			} else {
				// No HAVING clause, use regular aggregation
				lazy = lazy.GroupBy(groupCols...).Agg(aggPtrs...)
			}
		} else {
			return nil, fmt.Errorf("GROUP BY requires aggregation functions in SELECT")
		}
	} else if stmt.HavingClause != nil {
		// HAVING without GROUP BY is already validated in validateSelectStatement
		return nil, fmt.Errorf("HAVING clause requires GROUP BY clause")
	}

	// Apply computed columns from SELECT (non-aggregation expressions)
	if stmt.GroupByClause == nil {
		computedCols := t.extractComputedColumns(stmt.SelectList)

		for alias, expression := range computedCols {
			lazy = lazy.WithColumn(alias, expression)
		}
	}

	// Apply column selection
	selectCols := t.extractSelectColumns(stmt.SelectList)

	if len(selectCols) > 0 && !t.isWildcardSelect(stmt.SelectList) {
		lazy = lazy.Select(selectCols...)
	}

	// Apply ORDER BY clause
	if stmt.OrderByClause != nil {
		sortCols, ascending, err := t.translateOrderBy(stmt.OrderByClause)
		if err != nil {
			return nil, fmt.Errorf("error in ORDER BY: %w", err)
		}
		lazy = lazy.SortBy(sortCols, ascending)
	}

	// Note: LIMIT/OFFSET will be handled during execution as they require
	// special handling in the collect phase

	return lazy, nil
}

// extractColumnNames extracts column names from expressions
func (t *SQLTranslator) extractColumnNames(expressions []expr.Expr) ([]string, error) {
	var columns []string

	for _, expression := range expressions {
		if colExpr, ok := expression.(*expr.ColumnExpr); ok {
			columns = append(columns, colExpr.Name())
		} else {
			return nil, fmt.Errorf("GROUP BY supports only column references, got %T", expression)
		}
	}

	return columns, nil
}

// extractAggregations extracts aggregation expressions from SELECT list
func (t *SQLTranslator) extractAggregations(selectList []SelectItem) []expr.Expr {
	var aggExprs []expr.Expr

	for _, item := range selectList {
		if item.IsWildcard {
			continue
		}

		if t.isAggregationExpression(item.Expression) {
			// Apply alias if provided
			aggExpr := item.Expression
			if item.Alias != "" {
				if aliasable, ok := aggExpr.(interface{ As(string) expr.Expr }); ok {
					aggExpr = aliasable.As(item.Alias)
				}
			}
			aggExprs = append(aggExprs, aggExpr)
		}
	}

	return aggExprs
}

// extractComputedColumns extracts computed column expressions (non-aggregations)
func (t *SQLTranslator) extractComputedColumns(selectList []SelectItem) map[string]expr.Expr {
	computedCols := make(map[string]expr.Expr)

	for _, item := range selectList {
		if item.IsWildcard {
			continue
		}

		// Skip aggregations (they're handled separately)
		if t.isAggregationExpression(item.Expression) {
			continue
		}

		// Skip simple column references (they don't need computed columns)
		if _, ok := item.Expression.(*expr.ColumnExpr); ok && item.Alias == "" {
			continue
		}

		// Determine column name/alias
		columnName := item.Alias
		if columnName == "" {
			columnName = item.Expression.String()
		}

		computedCols[columnName] = item.Expression
	}

	return computedCols
}

// extractSelectColumns extracts final column selection list
func (t *SQLTranslator) extractSelectColumns(selectList []SelectItem) []string {
	var columns []string

	for _, item := range selectList {
		if item.IsWildcard {
			return []string{} // Wildcard means select all columns
		}

		// Use alias if provided, otherwise derive from expression
		columnName := item.Alias
		if columnName == "" {
			if colExpr, ok := item.Expression.(*expr.ColumnExpr); ok {
				columnName = colExpr.Name()
			} else {
				columnName = item.Expression.String()
			}
		}

		columns = append(columns, columnName)
	}

	return columns
}

// isWildcardSelect checks if SELECT list contains wildcard
func (t *SQLTranslator) isWildcardSelect(selectList []SelectItem) bool {
	for _, item := range selectList {
		if item.IsWildcard {
			return true
		}
	}
	return false
}

// isAggregationExpression checks if expression is an aggregation
func (t *SQLTranslator) isAggregationExpression(expression expr.Expr) bool {
	switch e := expression.(type) {
	case *expr.AggregationExpr:
		return true
	case *expr.FunctionExpr:
		// Check if function name indicates aggregation
		funcName := strings.ToUpper(e.Name())
		switch funcName {
		case "COUNT", "SUM", "AVG", "MIN", "MAX", "MEAN":
			return true
		}
	}
	return false
}

// translateOrderBy translates ORDER BY clause
func (t *SQLTranslator) translateOrderBy(orderBy *OrderByClause) ([]string, []bool, error) {
	var columns []string
	var ascending []bool

	for _, item := range orderBy.OrderItems {
		// For now, only support column references in ORDER BY
		if colExpr, ok := item.Expression.(*expr.ColumnExpr); ok {
			columns = append(columns, colExpr.Name())
			ascending = append(ascending, item.Direction == AscendingOrder)
		} else {
			return nil, nil, fmt.Errorf("ORDER BY supports only column references, got %T", item.Expression)
		}
	}

	return columns, ascending, nil
}

// TranslateFunctionCall translates SQL function calls to Gorilla expressions
func (t *SQLTranslator) TranslateFunctionCall(fn *SQLFunction) (expr.Expr, error) {
	funcName := strings.ToUpper(fn.Name)

	switch funcName {
	// Aggregation functions
	case "COUNT":
		if len(fn.Args) == 0 {
			return expr.Count(expr.Lit(1)), nil
		}
		return expr.Count(fn.Args[0]), nil
	case "SUM":
		if len(fn.Args) != 1 {
			return nil, fmt.Errorf("SUM function requires exactly one argument")
		}
		return expr.Sum(fn.Args[0]), nil
	case "AVG", "MEAN":
		if len(fn.Args) != 1 {
			return nil, fmt.Errorf("%s function requires exactly one argument", funcName)
		}
		return expr.Mean(fn.Args[0]), nil
	case "MIN":
		if len(fn.Args) != 1 {
			return nil, fmt.Errorf("MIN function requires exactly one argument")
		}
		return expr.Min(fn.Args[0]), nil
	case "MAX":
		if len(fn.Args) != 1 {
			return nil, fmt.Errorf("MAX function requires exactly one argument")
		}
		return expr.Max(fn.Args[0]), nil

	// String functions
	case "UPPER":
		if len(fn.Args) != 1 {
			return nil, fmt.Errorf("UPPER function requires exactly one argument")
		}
		if colExpr, ok := fn.Args[0].(*expr.ColumnExpr); ok {
			return colExpr.Upper(), nil
		}
		return nil, fmt.Errorf("UPPER function requires a column expression")
	case "LOWER":
		if len(fn.Args) != 1 {
			return nil, fmt.Errorf("LOWER function requires exactly one argument")
		}
		if colExpr, ok := fn.Args[0].(*expr.ColumnExpr); ok {
			return colExpr.Lower(), nil
		}
		return nil, fmt.Errorf("LOWER function requires a column expression")
	case "LENGTH":
		if len(fn.Args) != 1 {
			return nil, fmt.Errorf("LENGTH function requires exactly one argument")
		}
		if colExpr, ok := fn.Args[0].(*expr.ColumnExpr); ok {
			return colExpr.Length(), nil
		}
		return nil, fmt.Errorf("LENGTH function requires a column expression")

	// Math functions
	case "ABS":
		if len(fn.Args) != 1 {
			return nil, fmt.Errorf("ABS function requires exactly one argument")
		}
		if colExpr, ok := fn.Args[0].(*expr.ColumnExpr); ok {
			return colExpr.Abs(), nil
		}
		return nil, fmt.Errorf("ABS function requires a column expression")
	case "ROUND":
		if len(fn.Args) != 1 {
			return nil, fmt.Errorf("ROUND function requires exactly one argument")
		}
		if colExpr, ok := fn.Args[0].(*expr.ColumnExpr); ok {
			return colExpr.Round(), nil
		}
		return nil, fmt.Errorf("ROUND function requires a column expression")

	// Date functions (basic implementations) - TODO: Implement date functions
	// case "NOW":
	//	if len(fn.Args) != 0 {
	//		return nil, fmt.Errorf("NOW function takes no arguments")
	//	}
	//	return expr.Now(), nil
	// case "DATE_ADD":
	//	if len(fn.Args) != 2 {
	//		return nil, fmt.Errorf("DATE_ADD function requires exactly two arguments")
	//	}
	//	return expr.DateAdd(fn.Args[0], fn.Args[1]), nil
	// case "DATE_SUB":
	//	if len(fn.Args) != 2 {
	//		return nil, fmt.Errorf("DATE_SUB function requires exactly two arguments")
	//	}
	//	return expr.DateSub(fn.Args[0], fn.Args[1]), nil
	// case "DATE_DIFF":
	//	if len(fn.Args) != 3 {
	//		return nil, fmt.Errorf("DATE_DIFF function requires exactly three arguments")
	//	}
	//	// Extract unit from third argument (should be string literal)
	//	if litExpr, ok := fn.Args[2].(*expr.LiteralExpr); ok {
	//		if unit, ok := litExpr.Value().(string); ok {
	//			return expr.DateDiff(fn.Args[0], fn.Args[1], unit), nil
	//		}
	//	}
	//	return nil, fmt.Errorf("DATE_DIFF third argument must be a string literal")

	default:
		// For unknown functions, return an error for now
		// TODO: Create a constructor for generic function expressions
		return nil, fmt.Errorf("function %s not supported yet", fn.Name)
	}
}

// ValidateSQLSyntax performs basic validation of SQL statement
func (t *SQLTranslator) ValidateSQLSyntax(stmt SQLStatement) error {
	switch s := stmt.(type) {
	case *SelectStatement:
		return t.validateSelectStatement(s)
	default:
		return fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// validateSelectStatement validates SELECT statement syntax
func (t *SQLTranslator) validateSelectStatement(stmt *SelectStatement) error {
	// Check that SELECT list is not empty
	if len(stmt.SelectList) == 0 {
		return fmt.Errorf("SELECT list cannot be empty")
	}

	// Validate GROUP BY usage
	if stmt.GroupByClause != nil {
		// When GROUP BY is used, all non-aggregated columns in SELECT must be in GROUP BY
		groupColumns := make(map[string]bool)
		for _, colExpr := range stmt.GroupByClause.Columns {
			if col, ok := colExpr.(*expr.ColumnExpr); ok {
				groupColumns[col.Name()] = true
			}
		}

		for _, item := range stmt.SelectList {
			if item.IsWildcard {
				return fmt.Errorf("wildcard (*) not allowed with GROUP BY")
			}

			if !t.isAggregationExpression(item.Expression) {
				if col, ok := item.Expression.(*expr.ColumnExpr); ok {
					if !groupColumns[col.Name()] {
						return fmt.Errorf("column '%s' must appear in GROUP BY clause or be used in aggregate function", col.Name())
					}
				}
			}
		}
	}

	// Validate HAVING clause
	if stmt.HavingClause != nil && stmt.GroupByClause == nil {
		return fmt.Errorf("HAVING clause requires GROUP BY clause")
	}

	// Additional HAVING validation will be done during translation when we have
	// the aggregation context and alias information

	// Validate ORDER BY references
	if stmt.OrderByClause != nil {
		// Basic validation - ensure expressions are valid
		for _, item := range stmt.OrderByClause.OrderItems {
			if item.Expression == nil {
				return fmt.Errorf("ORDER BY expression cannot be nil")
			}
		}
	}

	// Validate LIMIT clause
	if stmt.LimitClause != nil {
		if stmt.LimitClause.Count <= 0 {
			return fmt.Errorf("LIMIT count must be positive, got %d", stmt.LimitClause.Count)
		}
		if stmt.LimitClause.Offset < 0 {
			return fmt.Errorf("OFFSET must be non-negative, got %d", stmt.LimitClause.Offset)
		}
	}

	return nil
}

// GetRegisteredTables returns the list of registered table names
func (t *SQLTranslator) GetRegisteredTables() []string {
	var tables []string
	for name := range t.tables {
		tables = append(tables, name)
	}
	return tables
}

// ClearTables removes all registered tables
func (t *SQLTranslator) ClearTables() {
	t.tables = make(map[string]*dataframe.DataFrame)
}

// resolveAliasesInExpression recursively resolves aliases in an expression tree
func (t *SQLTranslator) resolveAliasesInExpression(
	expression expr.Expr,
	aliasResolver *expr.AliasResolver,
	aggContext *expr.AggregationContext,
) (expr.Expr, error) {
	switch e := expression.(type) {
	case *expr.ColumnExpr:
		// Check if this is an alias that needs to be resolved
		if resolved, isAlias := aliasResolver.ResolveAlias(e.Name()); isAlias {
			// Return a new ColumnExpr with the resolved name
			return expr.Col(resolved), nil
		}
		// Return the original column expression
		return e, nil

	case *expr.BinaryExpr:
		// Recursively resolve aliases in left and right operands
		left, err := t.resolveAliasesInExpression(e.Left(), aliasResolver, aggContext)
		if err != nil {
			return nil, err
		}

		right, err := t.resolveAliasesInExpression(e.Right(), aliasResolver, aggContext)
		if err != nil {
			return nil, err
		}

		// Create a new binary expression with resolved operands
		return expr.NewBinaryExpr(left, e.Op(), right), nil

	case *expr.AggregationExpr:
		// For HAVING clauses, aggregation expressions should be converted to column references
		// Use the aggregation context to find the corresponding column name
		exprStr := e.String()
		if columnName, found := aggContext.GetColumnName(exprStr); found {
			// Convert to a column reference to the aggregated column
			return expr.Col(columnName), nil
		}

		// If no mapping found in aggregation context, return as-is (this might cause an error during execution)
		return e, nil

	case *expr.LiteralExpr:
		// Literal expressions should remain as-is
		return e, nil

	default:
		// For any other expression types, return as-is
		return expression, nil
	}
}
