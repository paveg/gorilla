package sql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/sql/sqlutil"
)

// SQLTranslator translates SQL AST to DataFrame operations.
type SQLTranslator struct { //nolint:revive // Maintained for consistent API naming
	*sqlutil.BaseTranslator

	evaluator *expr.Evaluator
}

// NewSQLTranslator creates a new SQL translator.
func NewSQLTranslator(mem memory.Allocator) *SQLTranslator {
	return &SQLTranslator{
		BaseTranslator: sqlutil.NewBaseTranslator(mem),
		evaluator:      expr.NewEvaluator(mem),
	}
}

// TranslateStatement translates a SQL statement to a LazyFrame.
func (t *SQLTranslator) TranslateStatement(stmt Statement) (*dataframe.LazyFrame, error) {
	switch s := stmt.(type) {
	case *SelectStatement:
		return t.translateSelect(s)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// translateSelect translates a SELECT statement to LazyFrame operations.
func (t *SQLTranslator) translateSelect(stmt *SelectStatement) (*dataframe.LazyFrame, error) {
	// Process FROM clause
	lazy, err := t.processFromClause(stmt.FromClause)
	if err != nil {
		return nil, err
	}

	// Apply WHERE clause
	if stmt.WhereClause != nil {
		lazy = lazy.Filter(stmt.WhereClause.Condition)
	}

	// Process GROUP BY and HAVING clauses
	lazy, err = t.processGroupByAndHaving(stmt, lazy)
	if err != nil {
		return nil, err
	}

	// Apply computed columns from SELECT (non-aggregation expressions)
	if stmt.GroupByClause == nil {
		lazy = t.processComputedColumns(stmt.SelectList, lazy)
	}

	// Apply column selection
	lazy = t.processSelectColumns(stmt.SelectList, lazy)

	// Apply ORDER BY clause
	if stmt.OrderByClause != nil {
		lazy, err = t.processOrderBy(stmt.OrderByClause, lazy)
		if err != nil {
			return nil, err
		}
	}

	// Note: LIMIT/OFFSET will be handled during execution as they require
	// special handling in the collect phase

	return lazy, nil
}

// processFromClause processes the FROM clause and returns the initial LazyFrame.
func (t *SQLTranslator) processFromClause(fromClause *FromClause) (*dataframe.LazyFrame, error) {
	if fromClause == nil {
		return nil, t.HandleCommonErrors("FROM clause processing", errors.New("FROM clause is required"))
	}

	// Use common validation
	if err := t.ValidateTableExists(fromClause.TableName); err != nil {
		return nil, t.HandleCommonErrors("FROM clause processing", err)
	}

	df, _ := t.GetTable(fromClause.TableName)
	return df.Lazy(), nil
}

// processGroupByAndHaving processes GROUP BY and HAVING clauses.
func (t *SQLTranslator) processGroupByAndHaving(
	stmt *SelectStatement,
	lazy *dataframe.LazyFrame,
) (*dataframe.LazyFrame, error) {
	if stmt.GroupByClause == nil {
		if stmt.HavingClause != nil {
			return nil, errors.New("HAVING clause requires GROUP BY clause")
		}
		return lazy, nil
	}

	groupCols, err := t.extractColumnNames(stmt.GroupByClause.Columns)
	if err != nil {
		return nil, fmt.Errorf("error in GROUP BY: %w", err)
	}

	// Collect aggregations from SELECT list
	aggExprs := t.extractAggregations(stmt.SelectList)
	if len(aggExprs) == 0 {
		return nil, errors.New("GROUP BY requires aggregation functions in SELECT")
	}

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
		return t.processHavingClause(stmt, lazy, groupCols, aggPtrs)
	}

	// No HAVING clause, use regular aggregation
	return lazy.GroupBy(groupCols...).Agg(aggPtrs...), nil
}

// processHavingClause processes the HAVING clause with proper validation and alias resolution.
func (t *SQLTranslator) processHavingClause(
	stmt *SelectStatement,
	lazy *dataframe.LazyFrame,
	groupCols []string,
	aggPtrs []*expr.AggregationExpr,
) (*dataframe.LazyFrame, error) {
	// Build aggregation context for HAVING validation
	aggContext := expr.NewAggregationContext()

	for _, agg := range aggPtrs {
		exprStr := agg.String()

		// Use the actual alias if provided, otherwise use a generated name
		var columnName string
		if agg.Alias() != "" {
			columnName = agg.Alias()
		} else {
			// Generate SQL-appropriate default name
			tempResolver := expr.NewAliasResolver(false)
			columnName = tempResolver.GenerateDefaultName(agg)
		}

		aggContext.AddMapping(exprStr, columnName)
	}

	// Build alias resolver from SELECT list
	aliasResolver, err := t.buildAliasResolver(stmt.SelectList, groupCols)
	if err != nil {
		return nil, err
	}

	// Create HAVING validator with alias support
	havingValidator := expr.NewHavingValidatorWithAlias(aggContext, groupCols, aliasResolver)

	// Validate HAVING expression
	if validateErr := havingValidator.ValidateExpression(stmt.HavingClause.Condition); validateErr != nil {
		return nil, fmt.Errorf("HAVING validation error: %w", validateErr)
	}

	// Resolve aliases in HAVING expression
	resolvedHavingCondition, err := t.resolveAliasesInExpression(
		stmt.HavingClause.Condition,
		aliasResolver,
		aggContext,
	)
	if err != nil {
		return nil, fmt.Errorf("error resolving aliases in HAVING clause: %w", err)
	}

	// Use AggWithHaving for combined GROUP BY + HAVING operation
	return lazy.GroupBy(groupCols...).AggWithHaving(resolvedHavingCondition, aggPtrs...), nil
}

// buildAliasResolver builds an alias resolver from the SELECT list and GROUP BY columns.
func (t *SQLTranslator) buildAliasResolver(selectList []SelectItem, groupCols []string) (*expr.AliasResolver, error) {
	aliasResolver := expr.NewAliasResolver(false) // case-sensitive by default

	// Add GROUP BY columns to alias resolver
	for _, col := range groupCols {
		aliasResolver.AddGroupByColumn(col)
	}

	// Add aggregations with their aliases
	for _, item := range selectList {
		if !item.IsWildcard {
			if err := t.processSelectItemAggregation(item, aliasResolver); err != nil {
				return nil, err
			}
		}
	}

	return aliasResolver, nil
}

// processComputedColumns processes computed columns from the SELECT list.
func (t *SQLTranslator) processComputedColumns(
	selectList []SelectItem,
	lazy *dataframe.LazyFrame,
) *dataframe.LazyFrame {
	computedCols := t.extractComputedColumns(selectList)

	for alias, expression := range computedCols {
		lazy = lazy.WithColumn(alias, expression)
	}

	return lazy
}

// processSelectColumns processes column selection from the SELECT list.
func (t *SQLTranslator) processSelectColumns(
	selectList []SelectItem,
	lazy *dataframe.LazyFrame,
) *dataframe.LazyFrame {
	selectCols := t.extractSelectColumns(selectList)

	if len(selectCols) > 0 && !t.isWildcardSelect(selectList) {
		lazy = lazy.Select(selectCols...)
	}

	return lazy
}

// processOrderBy processes the ORDER BY clause.
func (t *SQLTranslator) processOrderBy(
	orderBy *OrderByClause,
	lazy *dataframe.LazyFrame,
) (*dataframe.LazyFrame, error) {
	sortCols, ascending, err := t.translateOrderBy(orderBy)
	if err != nil {
		return nil, fmt.Errorf("error in ORDER BY: %w", err)
	}
	return lazy.SortBy(sortCols, ascending), nil
}

// extractColumnNames extracts column names from expressions.
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

// extractAggregations extracts aggregation expressions from SELECT list.
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

// extractComputedColumns extracts computed column expressions (non-aggregations).
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

// extractSelectColumns extracts final column selection list.
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

// isWildcardSelect checks if SELECT list contains wildcard.
func (t *SQLTranslator) isWildcardSelect(selectList []SelectItem) bool {
	for _, item := range selectList {
		if item.IsWildcard {
			return true
		}
	}
	return false
}

// isAggregationExpression checks if expression is an aggregation.
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

// translateOrderBy translates ORDER BY clause.
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

// TranslateFunctionCall translates SQL function calls to Gorilla expressions.
func (t *SQLTranslator) TranslateFunctionCall(fn *Function) (expr.Expr, error) {
	funcName := strings.ToUpper(fn.Name)

	switch funcName {
	// Aggregation functions
	case "COUNT":
		return t.translateCountFunction(fn)
	case "SUM":
		return t.translateUnaryAggregationFunction(fn, "SUM", expr.Sum)
	case "AVG", "MEAN":
		return t.translateUnaryAggregationFunction(fn, funcName, expr.Mean)
	case "MIN":
		return t.translateUnaryAggregationFunction(fn, "MIN", expr.Min)
	case "MAX":
		return t.translateUnaryAggregationFunction(fn, "MAX", expr.Max)

	// String functions
	case "UPPER":
		return t.translateUnaryColumnFunction(fn, "UPPER", (*expr.ColumnExpr).Upper)
	case "LOWER":
		return t.translateUnaryColumnFunction(fn, "LOWER", (*expr.ColumnExpr).Lower)
	case "LENGTH":
		return t.translateUnaryColumnFunction(fn, "LENGTH", (*expr.ColumnExpr).Length)

	// Math functions
	case "ABS":
		return t.translateUnaryColumnFunction(fn, "ABS", (*expr.ColumnExpr).Abs)
	case "ROUND":
		return t.translateUnaryColumnFunction(fn, "ROUND", (*expr.ColumnExpr).Round)

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

// translateCountFunction handles COUNT function translation with special logic for COUNT(*).
func (t *SQLTranslator) translateCountFunction(fn *Function) (expr.Expr, error) {
	if len(fn.Args) == 0 {
		return expr.Count(expr.Lit(1)), nil
	}
	return expr.Count(fn.Args[0]), nil
}

// translateUnaryAggregationFunction handles aggregation functions that take exactly one argument.
func (t *SQLTranslator) translateUnaryAggregationFunction(
	fn *Function,
	funcName string,
	exprFunc func(expr.Expr) *expr.AggregationExpr,
) (expr.Expr, error) {
	if len(fn.Args) != 1 {
		return nil, fmt.Errorf("%s function requires exactly one argument", funcName)
	}
	return exprFunc(fn.Args[0]), nil
}

// translateUnaryColumnFunction handles functions that operate on a single column expression.
func (t *SQLTranslator) translateUnaryColumnFunction(
	fn *Function,
	funcName string,
	methodFunc func(*expr.ColumnExpr) *expr.FunctionExpr,
) (expr.Expr, error) {
	if len(fn.Args) != 1 {
		return nil, fmt.Errorf("%s function requires exactly one argument", funcName)
	}

	colExpr, ok := fn.Args[0].(*expr.ColumnExpr)
	if !ok {
		return nil, fmt.Errorf("%s function requires a column expression", funcName)
	}

	return methodFunc(colExpr), nil
}

// ValidateSQLSyntax performs basic validation of SQL statement.
func (t *SQLTranslator) ValidateSQLSyntax(stmt Statement) error {
	switch s := stmt.(type) {
	case *SelectStatement:
		return t.validateSelectStatement(s)
	default:
		return fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// validateSelectStatement validates SELECT statement syntax.
func (t *SQLTranslator) validateSelectStatement(stmt *SelectStatement) error {
	// Check that SELECT list is not empty
	if len(stmt.SelectList) == 0 {
		return errors.New("SELECT list cannot be empty")
	}

	// Validate GROUP BY usage
	if stmt.GroupByClause != nil {
		if err := t.validateGroupByUsage(stmt); err != nil {
			return err
		}
	}

	// Validate HAVING clause
	if stmt.HavingClause != nil && stmt.GroupByClause == nil {
		return errors.New("HAVING clause requires GROUP BY clause")
	}

	// Additional HAVING validation will be done during translation when we have
	// the aggregation context and alias information

	// Validate ORDER BY references
	if stmt.OrderByClause != nil {
		// Basic validation - ensure expressions are valid
		for _, item := range stmt.OrderByClause.OrderItems {
			if item.Expression == nil {
				return errors.New("ORDER BY expression cannot be nil")
			}
		}
	}

	// Validate LIMIT clause
	if stmt.LimitClause != nil {
		// Allow Count = OffsetOnlyLimit for OFFSET-only queries, and Count >= 0 for regular LIMIT
		if stmt.LimitClause.Count < OffsetOnlyLimit {
			return fmt.Errorf(
				"invalid LIMIT count %d, must be non-negative or %d",
				stmt.LimitClause.Count,
				OffsetOnlyLimit,
			)
		}
		if stmt.LimitClause.Offset < 0 {
			return fmt.Errorf("OFFSET must be non-negative, got %d", stmt.LimitClause.Offset)
		}
	}

	return nil
}

// GetRegisteredTables returns the list of registered table names.
func (t *SQLTranslator) GetRegisteredTables() []string {
	return t.BaseTranslator.GetRegisteredTables()
}

// ClearTables removes all registered tables.
func (t *SQLTranslator) ClearTables() {
	t.BaseTranslator.ClearTables()
}

// resolveAliasesInExpression recursively resolves aliases in an expression tree.
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

// processSelectItemAggregation processes a select item for aggregation handling.
func (t *SQLTranslator) processSelectItemAggregation(item SelectItem, aliasResolver *expr.AliasResolver) error {
	aggExpr, ok := item.Expression.(*expr.AggregationExpr)
	if !ok {
		return nil // Not an aggregation expression, skip
	}

	// If the item has an alias, update the aggregation expression
	if item.Alias != "" {
		// Type safety check: ensure the As method exists and returns correct type
		if aliasableAgg, aliasOk := interface{}(aggExpr).(interface {
			As(string) *expr.AggregationExpr
		}); aliasOk {
			aggExpr = aliasableAgg.As(item.Alias)
		} else {
			return fmt.Errorf("aggregation expression does not support aliasing: %T", aggExpr)
		}
	}

	if err := aliasResolver.AddAggregation(aggExpr); err != nil {
		return fmt.Errorf("error adding aggregation to alias resolver: %w", err)
	}

	return nil
}

// validateGroupByUsage validates GROUP BY clause usage with SELECT list.
func (t *SQLTranslator) validateGroupByUsage(stmt *SelectStatement) error {
	// When GROUP BY is used, all non-aggregated columns in SELECT must be in GROUP BY
	groupColumns := make(map[string]bool)
	for _, colExpr := range stmt.GroupByClause.Columns {
		if col, ok := colExpr.(*expr.ColumnExpr); ok {
			groupColumns[col.Name()] = true
		}
	}

	for _, item := range stmt.SelectList {
		if item.IsWildcard {
			return errors.New("wildcard (*) not allowed with GROUP BY")
		}

		if !t.isAggregationExpression(item.Expression) {
			if col, ok := item.Expression.(*expr.ColumnExpr); ok {
				if !groupColumns[col.Name()] {
					return fmt.Errorf(
						"column '%s' must appear in GROUP BY clause or be used in aggregate function",
						col.Name(),
					)
				}
			}
		}
	}

	return nil
}
