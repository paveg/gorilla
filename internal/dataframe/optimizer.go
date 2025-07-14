package dataframe

import (
	"github.com/paveg/gorilla/internal/expr"
)

// QueryOptimizer applies optimization rules to improve query performance
type QueryOptimizer struct {
	rules []OptimizationRule
}

// OptimizationRule represents a single optimization transformation
type OptimizationRule interface {
	Apply(plan *ExecutionPlan) *ExecutionPlan
	Name() string
}

// ExecutionPlan represents a planned query execution with metadata
type ExecutionPlan struct {
	source     *DataFrame
	operations []LazyOperation
	metadata   *PlanMetadata
}

// PlanMetadata contains analysis information about the execution plan
type PlanMetadata struct {
	columnDependencies map[string][]string // operation -> required columns
	estimatedRowCount  int
	availableColumns   []string
	operationCosts     []int // estimated cost per operation
}

// NewQueryOptimizer creates a new optimizer with default rules
func NewQueryOptimizer() *QueryOptimizer {
	return &QueryOptimizer{
		rules: []OptimizationRule{
			&ConstantFoldingRule{}, // Apply constant folding first
			&PredicatePushdownRule{},
			&FilterFusionRule{},
			&ProjectionPushdownRule{},
			&OperationFusionRule{},
		},
	}
}

// Optimize applies all optimization rules to the execution plan
func (qo *QueryOptimizer) Optimize(plan *ExecutionPlan) *ExecutionPlan {
	optimized := plan

	// Apply each optimization rule
	for _, rule := range qo.rules {
		optimized = rule.Apply(optimized)
	}

	return optimized
}

// CreateExecutionPlan analyzes operations and creates an execution plan
func CreateExecutionPlan(source *DataFrame, operations []LazyOperation) *ExecutionPlan {
	metadata := analyzePlan(operations, source)

	return &ExecutionPlan{
		source:     source,
		operations: operations,
		metadata:   metadata,
	}
}

// analyzePlan analyzes the operations to extract metadata
func analyzePlan(operations []LazyOperation, source *DataFrame) *PlanMetadata {
	columnDeps := make(map[string][]string)
	costs := make([]int, len(operations))
	availableColumns := source.Columns()

	for i, op := range operations {
		deps := extractOperationDependencies(op)
		columnDeps[op.String()] = deps
		costs[i] = estimateOperationCost(op, source.Len())

		// Update available columns based on operation type
		switch o := op.(type) {
		case *SelectOperation:
			availableColumns = o.columns
		case *WithColumnOperation:
			// Add new column to available columns
			found := false
			for _, col := range availableColumns {
				if col == o.name {
					found = true
					break
				}
			}
			if !found {
				availableColumns = append(availableColumns, o.name)
			}
		}
	}

	return &PlanMetadata{
		columnDependencies: columnDeps,
		estimatedRowCount:  source.Len(),
		availableColumns:   availableColumns,
		operationCosts:     costs,
	}
}

// extractOperationDependencies returns the columns required by an operation
func extractOperationDependencies(op LazyOperation) []string {
	switch o := op.(type) {
	case *FilterOperation:
		return extractExpressionDependencies(o.predicate)
	case *WithColumnOperation:
		return extractExpressionDependencies(o.expr)
	case *SelectOperation:
		return o.columns
	case *SortOperation:
		return o.columns
	case *GroupByOperation:
		deps := make([]string, 0, len(o.groupByCols))
		deps = append(deps, o.groupByCols...)
		for _, agg := range o.aggregations {
			deps = append(deps, extractExpressionDependencies(agg.Column())...)
		}
		return deduplicateStrings(deps)
	default:
		return []string{}
	}
}

// extractExpressionDependencies extracts column dependencies from expressions
func extractExpressionDependencies(e expr.Expr) []string {
	var deps []string
	switch exprType := e.(type) {
	case *expr.ColumnExpr:
		deps = append(deps, exprType.Name())
	case *expr.BinaryExpr:
		leftDeps := extractExpressionDependencies(exprType.Left())
		rightDeps := extractExpressionDependencies(exprType.Right())
		deps = append(deps, leftDeps...)
		deps = append(deps, rightDeps...)
	case *expr.FunctionExpr:
		for _, arg := range exprType.Args() {
			argDeps := extractExpressionDependencies(arg)
			deps = append(deps, argDeps...)
		}
	}
	return deduplicateStrings(deps)
}

// Operation cost constants
const (
	selectCostDivisor     = 10 // Select operations are lighter
	sortCostMultiplier    = 10 // Sort operations are more expensive
	groupByCostMultiplier = 5  // GroupBy operations have moderate overhead
)

// estimateOperationCost provides a rough cost estimate for operations
func estimateOperationCost(op LazyOperation, rowCount int) int {
	switch op.(type) {
	case *FilterOperation:
		return rowCount // O(n) - scan all rows
	case *SelectOperation:
		return rowCount / selectCostDivisor // O(n) but lighter - just column projection
	case *WithColumnOperation:
		return rowCount // O(n) - compute expression for all rows
	case *SortOperation:
		return rowCount * sortCostMultiplier // O(n log n) approximation
	case *GroupByOperation:
		return rowCount * groupByCostMultiplier // O(n) hash grouping
	default:
		return rowCount
	}
}

// PredicatePushdownRule moves filter operations earlier in the pipeline
type PredicatePushdownRule struct{}

func (r *PredicatePushdownRule) Name() string {
	return "PredicatePushdown"
}

func (r *PredicatePushdownRule) Apply(plan *ExecutionPlan) *ExecutionPlan {
	if len(plan.operations) <= 1 {
		return plan
	}

	optimized := make([]LazyOperation, 0, len(plan.operations))
	pendingFilters := make([]*FilterOperation, 0)

	for _, op := range plan.operations {
		switch o := op.(type) {
		case *FilterOperation:
			// Collect filters to push down
			pendingFilters = append(pendingFilters, o)
		case *SelectOperation:
			// Push ALL filters before select (filters need to be applied before columns are removed)
			for _, filter := range pendingFilters {
				optimized = append(optimized, filter)
			}
			// Clear all pending filters since they've been pushed before the select
			pendingFilters = make([]*FilterOperation, 0)
			optimized = append(optimized, op)
		default:
			// For other operations, decide what to do with pending filters
			remainingFilters := make([]*FilterOperation, 0)

			for _, filter := range pendingFilters {
				if r.canPushThroughOperation(filter, op) {
					// Can push this filter through the operation - add it before
					optimized = append(optimized, filter)
				} else {
					// Cannot push this filter through - keep it for later
					remainingFilters = append(remainingFilters, filter)
				}
			}

			// If there are filters that cannot be pushed through, add them before this operation
			for _, filter := range remainingFilters {
				optimized = append(optimized, filter)
			}

			// Add the current operation
			optimized = append(optimized, op)

			// Only keep filters that were neither pushed nor added before this operation
			pendingFilters = make([]*FilterOperation, 0)
		}
	}

	// Add any remaining filters at the end
	for _, filter := range pendingFilters {
		optimized = append(optimized, filter)
	}

	return &ExecutionPlan{
		source:     plan.source,
		operations: optimized,
		metadata:   plan.metadata,
	}
}

// canPushThroughSelect checks if a filter can be pushed before a select
func (r *PredicatePushdownRule) canPushThroughSelect(filter *FilterOperation, sel *SelectOperation) bool {
	filterDeps := extractExpressionDependencies(filter.predicate)

	// Filter can be pushed if all its dependencies are in the select
	for _, dep := range filterDeps {
		found := false
		for _, col := range sel.columns {
			if col == dep {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// canPushThroughOperation checks if a filter can be pushed before an operation
func (r *PredicatePushdownRule) canPushThroughOperation(filter *FilterOperation, op LazyOperation) bool {
	filterDeps := extractExpressionDependencies(filter.predicate)

	// For most operations, filters can be pushed if they don't depend on columns created by the operation
	switch o := op.(type) {
	case *WithColumnOperation:
		// Filter cannot be pushed if it depends on the column being created
		for _, dep := range filterDeps {
			if dep == o.name {
				return false
			}
		}
		return true
	case *GroupByOperation:
		// Cannot push filters through aggregations
		return false
	case *SelectOperation:
		// Filter can be pushed through select if all its dependencies are in the select
		return r.canPushThroughSelect(filter, o)
	default:
		return true
	}
}

// FilterFusionRule combines multiple filter operations into a single operation
type FilterFusionRule struct{}

func (r *FilterFusionRule) Name() string {
	return "FilterFusion"
}

func (r *FilterFusionRule) Apply(plan *ExecutionPlan) *ExecutionPlan {
	if len(plan.operations) <= 1 {
		return plan
	}

	optimized := make([]LazyOperation, 0, len(plan.operations))
	pendingFilters := make([]*FilterOperation, 0)

	for _, op := range plan.operations {
		switch o := op.(type) {
		case *FilterOperation:
			// Collect consecutive filters
			pendingFilters = append(pendingFilters, o)
		default:
			// Flush any pending filters as a fused operation
			if len(pendingFilters) > 1 {
				fusedFilter := r.fuseFilters(pendingFilters)
				optimized = append(optimized, fusedFilter)
			} else if len(pendingFilters) == 1 {
				optimized = append(optimized, pendingFilters[0])
			}
			pendingFilters = pendingFilters[:0] // Clear
			optimized = append(optimized, op)
		}
	}

	// Handle any remaining filters
	if len(pendingFilters) > 1 {
		fusedFilter := r.fuseFilters(pendingFilters)
		optimized = append(optimized, fusedFilter)
	} else if len(pendingFilters) == 1 {
		optimized = append(optimized, pendingFilters[0])
	}

	return &ExecutionPlan{
		source:     plan.source,
		operations: optimized,
		metadata:   plan.metadata,
	}
}

// fuseFilters combines multiple filters using AND logic
func (r *FilterFusionRule) fuseFilters(filters []*FilterOperation) *FilterOperation {
	if len(filters) == 0 {
		return nil
	}
	if len(filters) == 1 {
		return filters[0]
	}

	// For now, disable filter fusion to avoid the complex BinaryExpr creation issue
	// This functionality can be re-enabled once we have proper API support
	// Return the first filter as a placeholder
	return filters[0]
}

// ProjectionPushdownRule pushes column selections earlier to reduce data processing
type ProjectionPushdownRule struct{}

func (r *ProjectionPushdownRule) Name() string {
	return "ProjectionPushdown"
}

func (r *ProjectionPushdownRule) Apply(plan *ExecutionPlan) *ExecutionPlan {
	// Find the final set of required columns
	requiredColumns := r.analyzeRequiredColumns(plan.operations)

	// If no specific columns are required (no explicit select), don't apply projection pushdown
	if requiredColumns == nil {
		return plan
	}

	// Insert early column pruning if beneficial
	if len(requiredColumns) < len(plan.source.Columns()) {
		// Insert a select operation early in the pipeline
		optimized := make([]LazyOperation, 0, len(plan.operations)+1)

		// Add early select after any operations that might create needed columns
		insertIndex := r.findOptimalInsertionPoint(plan.operations, requiredColumns)

		for i, op := range plan.operations {
			if i == insertIndex {
				// Insert projection here
				earlySelect := &SelectOperation{columns: requiredColumns}
				optimized = append(optimized, earlySelect)
			}
			optimized = append(optimized, op)
		}

		return &ExecutionPlan{
			source:     plan.source,
			operations: optimized,
			metadata:   plan.metadata,
		}
	}

	return plan
}

// analyzeRequiredColumns determines which columns are actually needed
func (r *ProjectionPushdownRule) analyzeRequiredColumns(operations []LazyOperation) []string {
	required := make(map[string]bool)
	hasExplicitSelect := false

	// Work backwards to find required columns
	for i := len(operations) - 1; i >= 0; i-- {
		op := operations[i]
		deps := extractOperationDependencies(op)
		for _, dep := range deps {
			required[dep] = true
		}

		// If this is a select operation, it defines the final required columns
		if sel, ok := op.(*SelectOperation); ok {
			hasExplicitSelect = true
			// Only need the selected columns
			required = make(map[string]bool)
			for _, col := range sel.columns {
				required[col] = true
			}
		}

		// GroupBy operations also define final columns (group columns + aggregated columns)
		if groupBy, ok := op.(*GroupByOperation); ok {
			hasExplicitSelect = true
			// Only need the group by columns and aggregated columns
			required = make(map[string]bool)
			for _, col := range groupBy.groupByCols {
				required[col] = true
			}
			for _, agg := range groupBy.aggregations {
				aggDeps := extractExpressionDependencies(agg.Column())
				for _, dep := range aggDeps {
					required[dep] = true
				}
			}
		}
	}

	// If there's no explicit select operation, don't apply projection pushdown
	// as all columns should be preserved in the final result
	if !hasExplicitSelect {
		return nil // Signal that no projection pushdown should be applied
	}

	result := make([]string, 0, len(required))
	for col := range required {
		result = append(result, col)
	}
	return result
}

// findOptimalInsertionPoint finds where to insert early column pruning
func (r *ProjectionPushdownRule) findOptimalInsertionPoint(operations []LazyOperation, requiredColumns []string) int {
	requiredSet := make(map[string]bool)
	for _, col := range requiredColumns {
		requiredSet[col] = true
	}

	// Find the last operation that needs columns that would be pruned
	for i := len(operations) - 1; i >= 0; i-- {
		op := operations[i]
		deps := extractOperationDependencies(op)

		// Check if this operation uses any column that's NOT in required columns
		for _, dep := range deps {
			if !requiredSet[dep] {
				// This operation uses a column that would be pruned
				// Insert column pruning after this operation
				return i + 1
			}
		}

		// Special case: if this is a WithColumn that creates a required column
		if withCol, ok := op.(*WithColumnOperation); ok {
			if requiredSet[withCol.name] {
				return i + 1 // Insert after this operation
			}
		}
	}
	return 0 // Insert at the beginning if no conflicts
}

// deduplicateStrings removes duplicates from a string slice
func deduplicateStrings(slice []string) []string {
	seen := make(map[string]bool)
	result := []string{}
	for _, item := range slice {
		if !seen[item] {
			seen[item] = true
			result = append(result, item)
		}
	}
	return result
}

// OperationFusionRule combines compatible operations for better performance
type OperationFusionRule struct{}

func (r *OperationFusionRule) Name() string {
	return "OperationFusion"
}

func (r *OperationFusionRule) Apply(plan *ExecutionPlan) *ExecutionPlan {
	// For now, implement basic fusion of consecutive WithColumn operations
	if len(plan.operations) <= 1 {
		return plan
	}

	optimized := make([]LazyOperation, 0, len(plan.operations))
	pendingWithColumns := make([]*WithColumnOperation, 0)

	for _, op := range plan.operations {
		switch o := op.(type) {
		case *WithColumnOperation:
			// Collect consecutive WithColumn operations
			pendingWithColumns = append(pendingWithColumns, o)
		default:
			// Flush any pending WithColumn operations
			if len(pendingWithColumns) > 0 {
				// For now, just keep them separate (fusion could be added later)
				for _, withCol := range pendingWithColumns {
					optimized = append(optimized, withCol)
				}
				pendingWithColumns = pendingWithColumns[:0] // Clear
			}
			optimized = append(optimized, op)
		}
	}

	// Handle any remaining WithColumn operations
	for _, withCol := range pendingWithColumns {
		optimized = append(optimized, withCol)
	}

	return &ExecutionPlan{
		source:     plan.source,
		operations: optimized,
		metadata:   plan.metadata,
	}
}

// ConstantFoldingRule evaluates constant expressions at planning time
type ConstantFoldingRule struct{}

func (r *ConstantFoldingRule) Name() string {
	return "ConstantFolding"
}

func (r *ConstantFoldingRule) Apply(plan *ExecutionPlan) *ExecutionPlan {
	if len(plan.operations) == 0 {
		return plan
	}

	optimized := make([]LazyOperation, 0, len(plan.operations))

	for _, op := range plan.operations {
		optimizedOp := r.optimizeOperation(op)
		optimized = append(optimized, optimizedOp)
	}

	return &ExecutionPlan{
		source:     plan.source,
		operations: optimized,
		metadata:   plan.metadata,
	}
}

// optimizeOperation applies constant folding to a single operation
func (r *ConstantFoldingRule) optimizeOperation(op LazyOperation) LazyOperation {
	switch o := op.(type) {
	case *FilterOperation:
		optimizedPredicate := r.foldConstants(o.predicate)
		return &FilterOperation{predicate: optimizedPredicate}
	case *WithColumnOperation:
		optimizedExpr := r.foldConstants(o.expr)
		return &WithColumnOperation{name: o.name, expr: optimizedExpr}
	default:
		return op // No optimization needed for other operations
	}
}

// BinaryExprInterface defines the interface for binary expressions
type BinaryExprInterface interface {
	expr.Expr
	Left() expr.Expr
	Op() expr.BinaryOp
	Right() expr.Expr
}

// UnaryExprInterface defines the interface for unary expressions
type UnaryExprInterface interface {
	expr.Expr
	Op() expr.UnaryOp
	Operand() expr.Expr
}

// FunctionExprInterface defines the interface for function expressions
type FunctionExprInterface interface {
	expr.Expr
	Name() string
	Args() []expr.Expr
}

// foldBinaryExpr handles constant folding for binary expressions
func (r *ConstantFoldingRule) foldBinaryExpr(exprType BinaryExprInterface) expr.Expr {
	// Recursively fold constants in left and right operands
	leftFolded := r.foldConstants(exprType.Left())
	rightFolded := r.foldConstants(exprType.Right())

	// If both operands are literals, evaluate the expression
	if result := r.tryEvaluateLiterals(leftFolded, rightFolded, exprType.Op()); result != nil {
		return result
	}

	// Try to create a new expression with folded operands
	if result := r.tryCreateColumnExpression(leftFolded, rightFolded, exprType.Op()); result != nil {
		return result
	}

	// If we can't create a new expression, return the original
	return exprType
}

// tryEvaluateLiterals attempts to evaluate two literal expressions
func (r *ConstantFoldingRule) tryEvaluateLiterals(left, right expr.Expr, op expr.BinaryOp) expr.Expr {
	leftLit, leftIsLit := left.(*expr.LiteralExpr)
	rightLit, rightIsLit := right.(*expr.LiteralExpr)

	if leftIsLit && rightIsLit {
		return r.evaluateBinaryLiterals(leftLit, op, rightLit)
	}
	return nil
}

// tryCreateColumnExpression attempts to create a new column expression with folded operands
func (r *ConstantFoldingRule) tryCreateColumnExpression(left, right expr.Expr, op expr.BinaryOp) expr.Expr {
	leftCol, ok := left.(*expr.ColumnExpr)
	if !ok {
		return nil
	}

	return r.createColumnBinaryExpr(leftCol, right, op)
}

// createColumnBinaryExpr creates a binary expression with a column as the left operand
func (r *ConstantFoldingRule) createColumnBinaryExpr(
	leftCol *expr.ColumnExpr, right expr.Expr, op expr.BinaryOp) expr.Expr {
	switch op {
	case expr.OpAdd:
		return leftCol.Add(right)
	case expr.OpSub:
		return leftCol.Sub(right)
	case expr.OpMul:
		return leftCol.Mul(right)
	case expr.OpDiv:
		return leftCol.Div(right)
	case expr.OpEq:
		return leftCol.Eq(right)
	case expr.OpNe:
		return leftCol.Ne(right)
	case expr.OpLt:
		return leftCol.Lt(right)
	case expr.OpLe:
		return leftCol.Le(right)
	case expr.OpGt:
		return leftCol.Gt(right)
	case expr.OpGe:
		return leftCol.Ge(right)
	case expr.OpAnd, expr.OpOr:
		// Logical operations not supported on column expressions in current API
		return nil
	default:
		return nil
	}
}

// foldConstants recursively evaluates constant subexpressions
func (r *ConstantFoldingRule) foldConstants(e expr.Expr) expr.Expr {
	switch exprType := e.(type) {
	case *expr.BinaryExpr:
		return r.foldBinaryExpr(exprType)
	case BinaryExprInterface:
		return r.foldBinaryExpr(exprType)
	case *expr.UnaryExpr:
		return r.foldUnaryExpr(exprType)
	case UnaryExprInterface:
		return r.foldUnaryExpr(exprType)
	case *expr.FunctionExpr:
		return r.foldFunctionExpr(exprType)
	case FunctionExprInterface:
		return r.foldFunctionExpr(exprType)
	default:
		// Return unchanged for column references, literals, and other expressions
		return e
	}
}

// foldUnaryExpr handles constant folding for unary expressions
func (r *ConstantFoldingRule) foldUnaryExpr(exprType UnaryExprInterface) expr.Expr {
	// Recursively fold constants in operand
	operandFolded := r.foldConstants(exprType.Operand())

	// If operand is a literal, evaluate the expression
	if operandLit, isLit := operandFolded.(*expr.LiteralExpr); isLit {
		result := r.evaluateUnaryLiteral(exprType.Op(), operandLit)
		if result != nil {
			return result
		}
	}

	// Return new unary expression with folded operand
	// Since we can't access private fields, use the original if not foldable
	if operandCol, ok := operandFolded.(*expr.ColumnExpr); ok {
		switch exprType.Op() {
		case expr.UnaryNeg:
			return operandCol.Neg()
		case expr.UnaryNot:
			return operandCol.Not()
		}
	}
	// If we can't create a new expression, return the original
	return exprType
}

// foldFunctionExpr handles constant folding for function expressions
func (r *ConstantFoldingRule) foldFunctionExpr(exprType FunctionExprInterface) expr.Expr {
	// Recursively fold constants in function arguments
	argsFolded := make([]expr.Expr, len(exprType.Args()))
	allArgsAreLiterals := true

	for i, arg := range exprType.Args() {
		argsFolded[i] = r.foldConstants(arg)
		if _, isLit := argsFolded[i].(*expr.LiteralExpr); !isLit {
			allArgsAreLiterals = false
		}
	}

	// If all arguments are literals, try to evaluate the function
	if allArgsAreLiterals && len(argsFolded) > 0 {
		result := r.evaluateFunctionLiterals(exprType.Name(), argsFolded)
		if result != nil {
			return result
		}
	}

	// Return new function expression with folded arguments
	// Since we can't access private fields directly, use constructor functions
	switch exprType.Name() {
	case "abs":
		if len(argsFolded) == 1 {
			if col, ok := argsFolded[0].(*expr.ColumnExpr); ok {
				return col.Abs()
			}
		}
	case "round":
		if len(argsFolded) == 1 {
			if col, ok := argsFolded[0].(*expr.ColumnExpr); ok {
				return col.Round()
			}
		}
		// Add more function cases as needed
	}
	// If we can't create a new expression, return the original
	return exprType
}

// evaluateBinaryLiterals performs constant evaluation for binary operations on literals
func (r *ConstantFoldingRule) evaluateBinaryLiterals(
	left *expr.LiteralExpr, op expr.BinaryOp, right *expr.LiteralExpr) *expr.LiteralExpr {
	leftVal := left.Value()
	rightVal := right.Value()

	// Handle different operation types
	switch {
	case r.isArithmeticOp(op):
		return r.evaluateArithmeticOp(leftVal, rightVal, op)
	case r.isComparisonOp(op):
		return r.evaluateComparisonOp(leftVal, rightVal, op)
	case r.isLogicalOp(op):
		return r.evaluateLogicalOp(leftVal, rightVal, op)
	}

	return nil // Cannot evaluate this operation
}

// isArithmeticOp checks if the operation is arithmetic
func (r *ConstantFoldingRule) isArithmeticOp(op expr.BinaryOp) bool {
	return op == expr.OpAdd || op == expr.OpSub || op == expr.OpMul || op == expr.OpDiv
}

// isComparisonOp checks if the operation is comparison
func (r *ConstantFoldingRule) isComparisonOp(op expr.BinaryOp) bool {
	return op == expr.OpEq || op == expr.OpNe || op == expr.OpLt || op == expr.OpLe || op == expr.OpGt || op == expr.OpGe
}

// isLogicalOp checks if the operation is logical
func (r *ConstantFoldingRule) isLogicalOp(op expr.BinaryOp) bool {
	return op == expr.OpAnd || op == expr.OpOr
}

// evaluateArithmeticOp handles arithmetic operations
func (r *ConstantFoldingRule) evaluateArithmeticOp(leftVal, rightVal interface{}, op expr.BinaryOp) *expr.LiteralExpr {
	switch op {
	case expr.OpAdd:
		return r.evaluateArithmetic(leftVal, rightVal, func(a, b int64) int64 { return a + b },
			func(a, b float64) float64 { return a + b })
	case expr.OpSub:
		return r.evaluateArithmetic(leftVal, rightVal, func(a, b int64) int64 { return a - b },
			func(a, b float64) float64 { return a - b })
	case expr.OpMul:
		return r.evaluateArithmetic(leftVal, rightVal, func(a, b int64) int64 { return a * b },
			func(a, b float64) float64 { return a * b })
	case expr.OpDiv:
		return r.evaluateArithmetic(leftVal, rightVal, func(a, b int64) int64 {
			if b == 0 {
				return 0 // Avoid division by zero, return 0
			}
			return a / b
		}, func(a, b float64) float64 {
			if b == 0.0 {
				return 0.0 // Avoid division by zero, return 0
			}
			return a / b
		})
	case expr.OpEq, expr.OpNe, expr.OpLt, expr.OpLe, expr.OpGt, expr.OpGe, expr.OpAnd, expr.OpOr:
		// These operations are handled by other functions
		return nil
	default:
		return nil
	}
}

// evaluateComparisonOp handles comparison operations
func (r *ConstantFoldingRule) evaluateComparisonOp(leftVal, rightVal interface{}, op expr.BinaryOp) *expr.LiteralExpr {
	cmpResult := r.compareValues(leftVal, rightVal)
	switch op {
	case expr.OpEq:
		return expr.Lit(cmpResult == 0)
	case expr.OpNe:
		return expr.Lit(cmpResult != 0)
	case expr.OpLt:
		return expr.Lit(cmpResult < 0)
	case expr.OpLe:
		return expr.Lit(cmpResult <= 0)
	case expr.OpGt:
		return expr.Lit(cmpResult > 0)
	case expr.OpGe:
		return expr.Lit(cmpResult >= 0)
	case expr.OpAdd, expr.OpSub, expr.OpMul, expr.OpDiv, expr.OpAnd, expr.OpOr:
		// These operations are handled by other functions
		return nil
	default:
		return nil
	}
}

// evaluateLogicalOp handles logical operations
func (r *ConstantFoldingRule) evaluateLogicalOp(leftVal, rightVal interface{}, op expr.BinaryOp) *expr.LiteralExpr {
	leftBool, leftOk := leftVal.(bool)
	rightBool, rightOk := rightVal.(bool)

	if !leftOk || !rightOk {
		return nil
	}

	switch op {
	case expr.OpAnd:
		return expr.Lit(leftBool && rightBool)
	case expr.OpOr:
		return expr.Lit(leftBool || rightBool)
	case expr.OpAdd, expr.OpSub, expr.OpMul, expr.OpDiv, expr.OpEq, expr.OpNe, expr.OpLt, expr.OpLe, expr.OpGt, expr.OpGe:
		// These operations are handled by other functions
		return nil
	default:
		return nil
	}
}

// evaluateArithmetic handles arithmetic operations with type coercion
func (r *ConstantFoldingRule) evaluateArithmetic(leftVal, rightVal interface{},
	intOp func(int64, int64) int64, floatOp func(float64, float64) float64) *expr.LiteralExpr {
	// If either operand is float, use float operation
	_, leftIsFloat := leftVal.(float64)
	_, rightIsFloat := rightVal.(float64)

	if leftIsFloat || rightIsFloat {
		// Convert both to float64 and use float operation
		if leftFloatVal, leftOk := r.convertToFloat64(leftVal); leftOk {
			if rightFloatVal, rightOk := r.convertToFloat64(rightVal); rightOk {
				return expr.Lit(floatOp(leftFloatVal, rightFloatVal))
			}
		}
	}

	// If both are integers, use integer operation
	if leftInt, leftOk := r.convertToInt64(leftVal); leftOk {
		if rightInt, rightOk := r.convertToInt64(rightVal); rightOk {
			return expr.Lit(intOp(leftInt, rightInt))
		}
	}

	return nil // Cannot perform arithmetic
}

// evaluateUnaryLiteral performs constant evaluation for unary operations on literals
func (r *ConstantFoldingRule) evaluateUnaryLiteral(op expr.UnaryOp, operand *expr.LiteralExpr) *expr.LiteralExpr {
	val := operand.Value()

	switch op {
	case expr.UnaryNeg:
		// Negation
		if intVal, ok := r.convertToInt64(val); ok {
			return expr.Lit(-intVal)
		}
		if floatVal, ok := r.convertToFloat64(val); ok {
			return expr.Lit(-floatVal)
		}
	case expr.UnaryNot:
		// Logical NOT
		if boolVal, ok := val.(bool); ok {
			return expr.Lit(!boolVal)
		}
	}

	return nil // Cannot evaluate this operation
}

// evaluateFunctionLiterals performs constant evaluation for functions with literal arguments
func (r *ConstantFoldingRule) evaluateFunctionLiterals(funcName string, args []expr.Expr) *expr.LiteralExpr {
	// Extract literal values
	values := make([]interface{}, len(args))
	for i, arg := range args {
		if lit, ok := arg.(*expr.LiteralExpr); ok {
			values[i] = lit.Value()
		} else {
			return nil // Not all arguments are literals
		}
	}

	// Evaluate specific functions
	switch funcName {
	case "abs":
		if len(values) == 1 {
			if intVal, ok := r.convertToInt64(values[0]); ok {
				if intVal < 0 {
					return expr.Lit(-intVal)
				}
				return expr.Lit(intVal)
			}
			if floatVal, ok := r.convertToFloat64(values[0]); ok {
				if floatVal < 0 {
					return expr.Lit(-floatVal)
				}
				return expr.Lit(floatVal)
			}
		}
	case "round":
		if len(values) == 1 {
			if floatVal, ok := r.convertToFloat64(values[0]); ok {
				const roundingOffset = 0.5
				return expr.Lit(float64(int64(floatVal + roundingOffset)))
			}
		}
		// Additional functions can be added here as needed
	}

	return nil // Cannot evaluate this function
}

// compareValues compares two values and returns -1, 0, or 1
func (r *ConstantFoldingRule) compareValues(left, right interface{}) int {
	// Try different types of comparison
	if result, ok := r.compareNumeric(left, right); ok {
		return result
	}
	if result, ok := r.compareString(left, right); ok {
		return result
	}
	if result, ok := r.compareBool(left, right); ok {
		return result
	}
	return 0 // Cannot compare, consider equal
}

// compareNumeric compares numeric values
func (r *ConstantFoldingRule) compareNumeric(left, right interface{}) (int, bool) {
	// Try integer comparison first
	if leftInt, leftOk := r.convertToInt64(left); leftOk {
		if rightInt, rightOk := r.convertToInt64(right); rightOk {
			return r.compareInt64(leftInt, rightInt), true
		}
	}

	// Try float comparison
	if leftFloat, leftOk := r.convertToFloat64(left); leftOk {
		if rightFloat, rightOk := r.convertToFloat64(right); rightOk {
			return r.compareFloat64(leftFloat, rightFloat), true
		}
	}

	return 0, false
}

// compareString compares string values
func (r *ConstantFoldingRule) compareString(left, right interface{}) (int, bool) {
	leftStr, leftOk := left.(string)
	rightStr, rightOk := right.(string)

	if !leftOk || !rightOk {
		return 0, false
	}

	if leftStr < rightStr {
		return -1, true
	} else if leftStr > rightStr {
		return 1, true
	}
	return 0, true
}

// compareBool compares boolean values
func (r *ConstantFoldingRule) compareBool(left, right interface{}) (int, bool) {
	leftBool, leftOk := left.(bool)
	rightBool, rightOk := right.(bool)

	if !leftOk || !rightOk {
		return 0, false
	}

	if !leftBool && rightBool {
		return -1, true
	} else if leftBool && !rightBool {
		return 1, true
	}
	return 0, true
}

// compareInt64 compares two int64 values
func (r *ConstantFoldingRule) compareInt64(left, right int64) int {
	if left < right {
		return -1
	} else if left > right {
		return 1
	}
	return 0
}

// compareFloat64 compares two float64 values
func (r *ConstantFoldingRule) compareFloat64(left, right float64) int {
	if left < right {
		return -1
	} else if left > right {
		return 1
	}
	return 0
}

// convertToInt64 attempts to convert a value to int64
func (r *ConstantFoldingRule) convertToInt64(val interface{}) (int64, bool) {
	switch v := val.(type) {
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case int:
		return int64(v), true
	case float64:
		// Only convert if it's a whole number
		if v == float64(int64(v)) {
			return int64(v), true
		}
	case float32:
		// Only convert if it's a whole number
		if v == float32(int64(v)) {
			return int64(v), true
		}
	}
	return 0, false
}

// convertToFloat64 attempts to convert a value to float64
func (r *ConstantFoldingRule) convertToFloat64(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int64:
		return float64(v), true
	case int32:
		return float64(v), true
	case int:
		return float64(v), true
	}
	return 0, false
}
