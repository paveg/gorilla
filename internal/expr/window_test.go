package expr

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWindowSpec_Basic(t *testing.T) {
	tests := []struct {
		name     string
		window   *WindowSpec
		expected string
	}{
		{
			name:     "empty window",
			window:   &WindowSpec{},
			expected: "OVER ()",
		},
		{
			name: "partition by single column",
			window: &WindowSpec{
				partitionBy: []string{"department"},
			},
			expected: "OVER (PARTITION BY department)",
		},
		{
			name: "partition by multiple columns",
			window: &WindowSpec{
				partitionBy: []string{"department", "location"},
			},
			expected: "OVER (PARTITION BY department, location)",
		},
		{
			name: "order by single column",
			window: &WindowSpec{
				orderBy: []OrderByExpr{
					{column: "salary", ascending: false},
				},
			},
			expected: "OVER (ORDER BY salary DESC)",
		},
		{
			name: "order by multiple columns",
			window: &WindowSpec{
				orderBy: []OrderByExpr{
					{column: "department", ascending: true},
					{column: "salary", ascending: false},
				},
			},
			expected: "OVER (ORDER BY department ASC, salary DESC)",
		},
		{
			name: "complete window spec",
			window: &WindowSpec{
				partitionBy: []string{"department"},
				orderBy: []OrderByExpr{
					{column: "salary", ascending: false},
				},
				frame: &WindowFrame{
					frameType: FrameTypeRows,
					start:     &FrameBoundary{boundaryType: BoundaryUnboundedPreceding},
					end:       &FrameBoundary{boundaryType: BoundaryCurrentRow},
				},
			},
			expected: "OVER (PARTITION BY department ORDER BY salary DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := tt.window.String()
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestWindowSpec_PartitionBy(t *testing.T) {
	window := &WindowSpec{}

	// Test single column
	result := window.PartitionBy("department")
	assert.Equal(t, []string{"department"}, result.partitionBy)

	// Test multiple columns
	result = window.PartitionBy("department", "location")
	assert.Equal(t, []string{"department", "location"}, result.partitionBy)

	// Test chaining
	result = window.PartitionBy("department").PartitionBy("location")
	assert.Equal(t, []string{"location"}, result.partitionBy) // Should replace, not append
}

func TestWindowSpec_OrderBy(t *testing.T) {
	window := &WindowSpec{}

	// Test single column ascending
	result := window.OrderBy("salary", true)
	require.Len(t, result.orderBy, 1)
	assert.Equal(t, "salary", result.orderBy[0].column)
	assert.True(t, result.orderBy[0].ascending)

	// Test single column descending
	window2 := &WindowSpec{} // Start fresh
	result = window2.OrderBy("salary", false)
	require.Len(t, result.orderBy, 1)
	assert.Equal(t, "salary", result.orderBy[0].column)
	assert.False(t, result.orderBy[0].ascending)

	// Test multiple columns (chaining should append, not replace)
	result = &WindowSpec{} // Start fresh
	result = result.OrderBy("department", true).OrderBy("salary", false)
	require.Len(t, result.orderBy, 2)
	assert.Equal(t, "department", result.orderBy[0].column)
	assert.True(t, result.orderBy[0].ascending)
	assert.Equal(t, "salary", result.orderBy[1].column)
	assert.False(t, result.orderBy[1].ascending)
}

func TestWindowFrame_Basic(t *testing.T) {
	tests := []struct {
		name     string
		frame    *WindowFrame
		expected string
	}{
		{
			name: "rows between unbounded preceding and current row",
			frame: &WindowFrame{
				frameType: FrameTypeRows,
				start:     &FrameBoundary{boundaryType: BoundaryUnboundedPreceding},
				end:       &FrameBoundary{boundaryType: BoundaryCurrentRow},
			},
			expected: "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
		},
		{
			name: "rows between current row and unbounded following",
			frame: &WindowFrame{
				frameType: FrameTypeRows,
				start:     &FrameBoundary{boundaryType: BoundaryCurrentRow},
				end:       &FrameBoundary{boundaryType: BoundaryUnboundedFollowing},
			},
			expected: "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
		},
		{
			name: "rows between 2 preceding and 2 following",
			frame: &WindowFrame{
				frameType: FrameTypeRows,
				start:     &FrameBoundary{boundaryType: BoundaryPreceding, offset: 2},
				end:       &FrameBoundary{boundaryType: BoundaryFollowing, offset: 2},
			},
			expected: "ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING",
		},
		{
			name: "range frame",
			frame: &WindowFrame{
				frameType: FrameTypeRange,
				start:     &FrameBoundary{boundaryType: BoundaryUnboundedPreceding},
				end:       &FrameBoundary{boundaryType: BoundaryCurrentRow},
			},
			expected: "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := tt.frame.String()
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestWindowExpr_Basic(t *testing.T) {
	// Test window expression creation
	expr := &WindowExpr{
		function: &AggregationExpr{
			column:  &ColumnExpr{name: "salary"},
			aggType: AggSum,
		},
		window: &WindowSpec{
			partitionBy: []string{"department"},
			orderBy: []OrderByExpr{
				{column: "salary", ascending: false},
			},
		},
	}

	assert.Equal(t, ExprWindow, expr.Type())
	assert.Contains(t, expr.String(), "sum(col(salary))")
	assert.Contains(t, expr.String(), "OVER (PARTITION BY department ORDER BY salary DESC)")
}

func TestWindowFunctions_RowNumber(t *testing.T) {
	// Test row number function
	rowNumExpr := RowNumber()
	assert.Equal(t, ExprWindowFunction, rowNumExpr.Type())
	assert.Equal(t, "ROW_NUMBER", rowNumExpr.funcName)
	assert.Nil(t, rowNumExpr.args)

	// Test with window spec
	window := &WindowSpec{
		partitionBy: []string{"department"},
		orderBy: []OrderByExpr{
			{column: "salary", ascending: false},
		},
	}

	windowExpr := rowNumExpr.Over(window)
	assert.Equal(t, ExprWindow, windowExpr.Type())
	assert.Contains(t, windowExpr.String(), "ROW_NUMBER()")
	assert.Contains(t, windowExpr.String(), "OVER (PARTITION BY department ORDER BY salary DESC)")
}

func TestWindowFunctions_Rank(t *testing.T) {
	// Test rank function
	rankExpr := Rank()
	assert.Equal(t, ExprWindowFunction, rankExpr.Type())
	assert.Equal(t, "RANK", rankExpr.funcName)
	assert.Nil(t, rankExpr.args)

	// Test with window spec
	window := &WindowSpec{
		orderBy: []OrderByExpr{
			{column: "score", ascending: false},
		},
	}

	windowExpr := rankExpr.Over(window)
	assert.Equal(t, ExprWindow, windowExpr.Type())
	assert.Contains(t, windowExpr.String(), "RANK()")
	assert.Contains(t, windowExpr.String(), "OVER (ORDER BY score DESC)")
}

func TestWindowFunctions_Lag(t *testing.T) {
	// Test lag function
	lagExpr := Lag(Col("salary"), 1)
	assert.Equal(t, ExprWindowFunction, lagExpr.Type())
	assert.Equal(t, "LAG", lagExpr.funcName)
	require.Len(t, lagExpr.args, 2)

	// Test with window spec
	window := &WindowSpec{
		partitionBy: []string{"employee_id"},
		orderBy: []OrderByExpr{
			{column: "date", ascending: true},
		},
	}

	windowExpr := lagExpr.Over(window)
	assert.Equal(t, ExprWindow, windowExpr.Type())
	assert.Contains(t, windowExpr.String(), "LAG(col(salary), lit(1))")
	assert.Contains(t, windowExpr.String(), "OVER (PARTITION BY employee_id ORDER BY date ASC)")
}

func TestChainedWindowConstruction(t *testing.T) {
	// Test fluent API for window construction
	window := NewWindow().
		PartitionBy("department").
		OrderBy("salary", false).
		Rows(Between(UnboundedPreceding(), CurrentRow()))

	expected := &WindowSpec{
		partitionBy: []string{"department"},
		orderBy: []OrderByExpr{
			{column: "salary", ascending: false},
		},
		frame: &WindowFrame{
			frameType: FrameTypeRows,
			start:     &FrameBoundary{boundaryType: BoundaryUnboundedPreceding},
			end:       &FrameBoundary{boundaryType: BoundaryCurrentRow},
		},
	}

	assert.Equal(t, expected.String(), window.String())
}
