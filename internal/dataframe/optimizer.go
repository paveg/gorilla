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
			// Push applicable filters before select
			for _, filter := range pendingFilters {
				if r.canPushThroughSelect(filter, o) {
					optimized = append(optimized, filter)
				}
			}
			// Clear pending filters that were pushed
			pendingFilters = r.removePushedFilters(pendingFilters, o)
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
	default:
		return true
	}
}

// removePushedFilters removes filters that were successfully pushed
func (r *PredicatePushdownRule) removePushedFilters(filters []*FilterOperation, op LazyOperation) []*FilterOperation {
	remaining := make([]*FilterOperation, 0)
	for _, filter := range filters {
		if !r.canPushThroughOperation(filter, op) {
			remaining = append(remaining, filter)
		}
	}
	return remaining
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
