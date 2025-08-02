package monitoring

import (
	"encoding/json"
)

// PlanNode represents a single operation in a query execution plan.
type PlanNode struct {
	Type        string     `json:"type"`
	Description string     `json:"description"`
	Children    []PlanNode `json:"children,omitempty"`
	Cost        int64      `json:"cost"`
}

// PlanMetrics contains performance metrics for a query plan.
type PlanMetrics struct {
	TotalCost     int64 `json:"total_cost"`
	RowsProcessed int64 `json:"rows_processed"`
	MemoryUsed    int64 `json:"memory_used"`
}

// QueryPlan represents a complete query execution plan with metrics.
type QueryPlan struct {
	Operations []PlanNode  `json:"operations"`
	Estimated  PlanMetrics `json:"estimated"`
	Actual     PlanMetrics `json:"actual,omitempty"`
}

// PlanBuilder helps construct query execution plans.
type PlanBuilder struct {
	operations []PlanNode
	estimated  PlanMetrics
	actual     PlanMetrics
}

// NewPlanBuilder creates a new plan builder.
func NewPlanBuilder() *PlanBuilder {
	return &PlanBuilder{
		operations: make([]PlanNode, 0),
	}
}

// AddOperation adds an operation to the query plan.
func (pb *PlanBuilder) AddOperation(opType, description string, cost int64) *PlanBuilder {
	node := PlanNode{
		Type:        opType,
		Description: description,
		Cost:        cost,
		Children:    make([]PlanNode, 0),
	}
	pb.operations = append(pb.operations, node)
	return pb
}

// AddOperationWithChildren adds an operation with child operations.
func (pb *PlanBuilder) AddOperationWithChildren(opType, description string, cost int64, children []PlanNode) *PlanBuilder {
	node := PlanNode{
		Type:        opType,
		Description: description,
		Cost:        cost,
		Children:    children,
	}
	pb.operations = append(pb.operations, node)
	return pb
}

// SetEstimated sets the estimated metrics for the query plan.
func (pb *PlanBuilder) SetEstimated(metrics PlanMetrics) *PlanBuilder {
	pb.estimated = metrics
	return pb
}

// SetActual sets the actual metrics for the query plan.
func (pb *PlanBuilder) SetActual(metrics PlanMetrics) *PlanBuilder {
	pb.actual = metrics
	return pb
}

// Build constructs and returns the final query plan.
func (pb *PlanBuilder) Build() QueryPlan {
	return QueryPlan{
		Operations: pb.operations,
		Estimated:  pb.estimated,
		Actual:     pb.actual,
	}
}

// ToJSON converts the query plan to JSON format.
func (qp *QueryPlan) ToJSON() ([]byte, error) {
	return json.MarshalIndent(qp, "", "  ")
}

// FromJSON creates a query plan from JSON data.
func (qp *QueryPlan) FromJSON(data []byte) error {
	return json.Unmarshal(data, qp)
}

// CalculateTotalCost calculates the total cost of all operations in the plan.
func (qp *QueryPlan) CalculateTotalCost() int64 {
	var total int64
	for _, op := range qp.Operations {
		total += calculateNodeCost(&op)
	}
	return total
}

// calculateNodeCost recursively calculates the cost of a node and its children.
func calculateNodeCost(node *PlanNode) int64 {
	cost := node.Cost
	for i := range node.Children {
		cost += calculateNodeCost(&node.Children[i])
	}
	return cost
}

// GetOperationCount returns the total number of operations in the plan.
func (qp *QueryPlan) GetOperationCount() int {
	count := len(qp.Operations)
	for i := range qp.Operations {
		count += countChildOperations(&qp.Operations[i])
	}
	return count
}

// countChildOperations recursively counts child operations.
func countChildOperations(node *PlanNode) int {
	count := len(node.Children)
	for i := range node.Children {
		count += countChildOperations(&node.Children[i])
	}
	return count
}

// String returns a string representation of the query plan.
func (qp *QueryPlan) String() string {
	data, err := qp.ToJSON()
	if err != nil {
		return "QueryPlan{error: " + err.Error() + "}"
	}
	return string(data)
}
