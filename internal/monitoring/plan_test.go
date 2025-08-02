//nolint:testpackage // requires internal access to unexported types and functions
package monitoring

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlanNode(t *testing.T) {
	t.Run("create simple plan node", func(t *testing.T) {
		node := PlanNode{
			Type:        "Filter",
			Description: "age > 30",
			Cost:        100,
		}

		assert.Equal(t, "Filter", node.Type)
		assert.Equal(t, "age > 30", node.Description)
		assert.Equal(t, int64(100), node.Cost)
		assert.Empty(t, node.Children)
	})

	t.Run("create plan node with children", func(t *testing.T) {
		child := PlanNode{
			Type:        "Scan",
			Description: "Table scan",
			Cost:        50,
		}

		parent := PlanNode{
			Type:        "Filter",
			Description: "age > 30",
			Cost:        150,
			Children:    []PlanNode{child},
		}

		assert.Equal(t, "Filter", parent.Type)
		assert.Equal(t, "age > 30", parent.Description)
		assert.Equal(t, int64(150), parent.Cost)
		require.Len(t, parent.Children, 1)
		assert.Equal(t, "Scan", parent.Children[0].Type)
	})
}

func TestQueryPlan(t *testing.T) {
	t.Run("create empty query plan", func(t *testing.T) {
		plan := QueryPlan{}
		assert.Empty(t, plan.Operations)
		assert.Zero(t, plan.Estimated.TotalCost)
		assert.Zero(t, plan.Actual.TotalCost)
	})

	t.Run("create query plan with operations", func(t *testing.T) {
		operations := []PlanNode{
			{Type: "Scan", Description: "Table scan", Cost: 100},
			{Type: "Filter", Description: "age > 30", Cost: 50},
		}

		estimated := PlanMetrics{
			TotalCost:     150,
			RowsProcessed: 1000,
			MemoryUsed:    1024,
		}

		plan := QueryPlan{
			Operations: operations,
			Estimated:  estimated,
		}

		assert.Len(t, plan.Operations, 2)
		assert.Equal(t, int64(150), plan.Estimated.TotalCost)
		assert.Equal(t, int64(1000), plan.Estimated.RowsProcessed)
		assert.Equal(t, int64(1024), plan.Estimated.MemoryUsed)
	})

	t.Run("marshal query plan to JSON", func(t *testing.T) {
		plan := QueryPlan{
			Operations: []PlanNode{
				{Type: "Filter", Description: "test filter", Cost: 100},
			},
			Estimated: PlanMetrics{
				TotalCost:     100,
				RowsProcessed: 500,
				MemoryUsed:    512,
			},
		}

		jsonData, err := json.Marshal(plan)
		require.NoError(t, err)
		assert.Contains(t, string(jsonData), "Filter")
		assert.Contains(t, string(jsonData), "test filter")
	})

	t.Run("unmarshal query plan from JSON", func(t *testing.T) {
		jsonStr := `{
			"operations": [
				{"type": "Scan", "description": "Table scan", "cost": 200}
			],
			"estimated": {
				"total_cost": 200,
				"rows_processed": 2000,
				"memory_used": 2048
			}
		}`

		var plan QueryPlan
		err := json.Unmarshal([]byte(jsonStr), &plan)
		require.NoError(t, err)

		require.Len(t, plan.Operations, 1)
		assert.Equal(t, "Scan", plan.Operations[0].Type)
		assert.Equal(t, "Table scan", plan.Operations[0].Description)
		assert.Equal(t, int64(200), plan.Operations[0].Cost)

		assert.Equal(t, int64(200), plan.Estimated.TotalCost)
		assert.Equal(t, int64(2000), plan.Estimated.RowsProcessed)
		assert.Equal(t, int64(2048), plan.Estimated.MemoryUsed)
	})
}

func TestPlanBuilder(t *testing.T) {
	t.Run("create plan builder", func(t *testing.T) {
		builder := NewPlanBuilder()
		assert.NotNil(t, builder)

		plan := builder.Build()
		assert.Empty(t, plan.Operations)
	})

	t.Run("add operation to plan", func(t *testing.T) {
		builder := NewPlanBuilder()

		builder.AddOperation("Filter", "age > 30", 100)
		plan := builder.Build()

		require.Len(t, plan.Operations, 1)
		assert.Equal(t, "Filter", plan.Operations[0].Type)
		assert.Equal(t, "age > 30", plan.Operations[0].Description)
		assert.Equal(t, int64(100), plan.Operations[0].Cost)
	})

	t.Run("add multiple operations", func(t *testing.T) {
		builder := NewPlanBuilder()

		builder.AddOperation("Scan", "Table scan", 50)
		builder.AddOperation("Filter", "age > 30", 75)
		builder.AddOperation("Sort", "name ASC", 100)

		plan := builder.Build()

		require.Len(t, plan.Operations, 3)
		assert.Equal(t, "Scan", plan.Operations[0].Type)
		assert.Equal(t, "Filter", plan.Operations[1].Type)
		assert.Equal(t, "Sort", plan.Operations[2].Type)
	})

	t.Run("set estimated metrics", func(t *testing.T) {
		builder := NewPlanBuilder()

		builder.SetEstimated(PlanMetrics{
			TotalCost:     300,
			RowsProcessed: 1500,
			MemoryUsed:    1024,
		})

		plan := builder.Build()
		assert.Equal(t, int64(300), plan.Estimated.TotalCost)
		assert.Equal(t, int64(1500), plan.Estimated.RowsProcessed)
		assert.Equal(t, int64(1024), plan.Estimated.MemoryUsed)
	})

	t.Run("set actual metrics", func(t *testing.T) {
		builder := NewPlanBuilder()

		builder.SetActual(PlanMetrics{
			TotalCost:     250,
			RowsProcessed: 1200,
			MemoryUsed:    800,
		})

		plan := builder.Build()
		assert.Equal(t, int64(250), plan.Actual.TotalCost)
		assert.Equal(t, int64(1200), plan.Actual.RowsProcessed)
		assert.Equal(t, int64(800), plan.Actual.MemoryUsed)
	})
}
