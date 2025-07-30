package expr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestPhase4WindowFunctionCreation tests that the new window functions can be created.
func TestPhase4WindowFunctionCreation(t *testing.T) {
	// Test PERCENT_RANK function creation
	percentRank := PercentRank()
	assert.Equal(t, "PERCENT_RANK", percentRank.funcName)
	assert.Empty(t, percentRank.args)

	// Test CUME_DIST function creation
	cumeDist := CumeDist()
	assert.Equal(t, "CUME_DIST", cumeDist.funcName)
	assert.Empty(t, cumeDist.args)

	// Test NTH_VALUE function creation
	nthValue := NthValue(Col("test"), 2)
	assert.Equal(t, "NTH_VALUE", nthValue.funcName)
	assert.Len(t, nthValue.args, 2)

	// Test NTILE function creation
	ntile := Ntile(4)
	assert.Equal(t, "NTILE", ntile.funcName)
	assert.Len(t, ntile.args, 1)
}

// TestGroupsFrameType tests GROUPS frame type.
func TestGroupsFrameType(t *testing.T) {
	// Create window with GROUPS frame
	window := NewWindow().
		OrderBy("value", true).
		Groups(Between(Preceding(1), CurrentRow()))

	// Test frame type is set correctly
	assert.Equal(t, FrameTypeGroups, window.frame.frameType)
	assert.Equal(t, BoundaryPreceding, window.frame.start.boundaryType)
	assert.Equal(t, 1, window.frame.start.offset)
	assert.Equal(t, BoundaryCurrentRow, window.frame.end.boundaryType)
}

// TestWindowExpressionCreation tests window expression creation with new functions.
func TestWindowExpressionCreation(t *testing.T) {
	// Test PERCENT_RANK with window
	percentRankExpr := PercentRank().Over(
		NewWindow().
			PartitionBy("department").
			OrderBy("salary", true),
	)

	assert.NotNil(t, percentRankExpr)
	assert.Equal(t, "PERCENT_RANK", percentRankExpr.function.(*WindowFunctionExpr).funcName)
	assert.Len(t, percentRankExpr.window.partitionBy, 1)
	assert.Equal(t, "department", percentRankExpr.window.partitionBy[0])

	// Test NTILE with window
	ntileExpr := Ntile(4).Over(
		NewWindow().OrderBy("value", true),
	)

	assert.NotNil(t, ntileExpr)
	assert.Equal(t, "NTILE", ntileExpr.function.(*WindowFunctionExpr).funcName)
	assert.Len(t, ntileExpr.window.orderBy, 1)
	assert.Equal(t, "value", ntileExpr.window.orderBy[0].column)
	assert.True(t, ntileExpr.window.orderBy[0].ascending)
}
