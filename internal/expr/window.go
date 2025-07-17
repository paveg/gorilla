package expr

import (
	"fmt"
	"strings"
)

// Add new expression types for window functions
const (
	ExprWindow ExprType = iota + 100
	ExprWindowFunction
)

// WindowSpec represents a window specification for window functions
type WindowSpec struct {
	partitionBy []string
	orderBy     []OrderByExpr
	frame       *WindowFrame
}

// OrderByExpr represents a column ordering specification
type OrderByExpr struct {
	column    string
	ascending bool
}

// WindowFrame represents the frame specification for window functions
type WindowFrame struct {
	frameType FrameType
	start     *FrameBoundary
	end       *FrameBoundary
}

// FrameType represents the type of window frame
type FrameType int

const (
	FrameTypeRows FrameType = iota
	FrameTypeRange
	FrameTypeGroups
)

// FrameBoundary represents a frame boundary
type FrameBoundary struct {
	boundaryType BoundaryType
	offset       int
}

// BoundaryType represents the type of frame boundary
type BoundaryType int

const (
	BoundaryUnboundedPreceding BoundaryType = iota
	BoundaryPreceding
	BoundaryCurrentRow
	BoundaryFollowing
	BoundaryUnboundedFollowing
)

// WindowExpr represents a window function expression
type WindowExpr struct {
	function Expr
	window   *WindowSpec
}

// WindowFunctionExpr represents a window-specific function (ROW_NUMBER, RANK, etc.)
type WindowFunctionExpr struct {
	funcName string
	args     []Expr
}

// Window construction functions

// NewWindow creates a new window specification
func NewWindow() *WindowSpec {
	return &WindowSpec{}
}

// PartitionBy sets the partition columns for the window
func (w *WindowSpec) PartitionBy(columns ...string) *WindowSpec {
	w.partitionBy = columns
	return w
}

// OrderBy adds an ordering specification to the window
func (w *WindowSpec) OrderBy(column string, ascending bool) *WindowSpec {
	w.orderBy = append(w.orderBy, OrderByExpr{column: column, ascending: ascending})
	return w
}

// Rows sets a ROWS frame for the window
func (w *WindowSpec) Rows(frame *WindowFrame) *WindowSpec {
	frame.frameType = FrameTypeRows
	w.frame = frame
	return w
}

// Range sets a RANGE frame for the window
func (w *WindowSpec) Range(frame *WindowFrame) *WindowSpec {
	frame.frameType = FrameTypeRange
	w.frame = frame
	return w
}

// String returns the string representation of the window spec
func (w *WindowSpec) String() string {
	var parts []string

	if len(w.partitionBy) > 0 {
		parts = append(parts, "PARTITION BY "+strings.Join(w.partitionBy, ", "))
	}

	if len(w.orderBy) > 0 {
		var orderClauses []string
		for _, order := range w.orderBy {
			direction := "ASC"
			if !order.ascending {
				direction = "DESC"
			}
			orderClauses = append(orderClauses, order.column+" "+direction)
		}
		parts = append(parts, "ORDER BY "+strings.Join(orderClauses, ", "))
	}

	if w.frame != nil {
		parts = append(parts, w.frame.String())
	}

	return "OVER (" + strings.Join(parts, " ") + ")"
}

// Frame construction functions

// Between creates a window frame between two boundaries
func Between(start, end *FrameBoundary) *WindowFrame {
	return &WindowFrame{
		start: start,
		end:   end,
	}
}

// UnboundedPreceding creates an unbounded preceding boundary
func UnboundedPreceding() *FrameBoundary {
	return &FrameBoundary{boundaryType: BoundaryUnboundedPreceding}
}

// Preceding creates a preceding boundary with offset
func Preceding(offset int) *FrameBoundary {
	return &FrameBoundary{boundaryType: BoundaryPreceding, offset: offset}
}

// CurrentRow creates a current row boundary
func CurrentRow() *FrameBoundary {
	return &FrameBoundary{boundaryType: BoundaryCurrentRow}
}

// Following creates a following boundary with offset
func Following(offset int) *FrameBoundary {
	return &FrameBoundary{boundaryType: BoundaryFollowing, offset: offset}
}

// UnboundedFollowing creates an unbounded following boundary
func UnboundedFollowing() *FrameBoundary {
	return &FrameBoundary{boundaryType: BoundaryUnboundedFollowing}
}

// String returns the string representation of the frame
func (f *WindowFrame) String() string {
	frameTypeName := "ROWS"
	if f.frameType == FrameTypeRange {
		frameTypeName = "RANGE"
	} else if f.frameType == FrameTypeGroups {
		frameTypeName = "GROUPS"
	}

	return fmt.Sprintf("%s BETWEEN %s AND %s",
		frameTypeName, f.start.String(), f.end.String())
}

// String returns the string representation of the boundary
func (b *FrameBoundary) String() string {
	switch b.boundaryType {
	case BoundaryUnboundedPreceding:
		return "UNBOUNDED PRECEDING"
	case BoundaryPreceding:
		return fmt.Sprintf("%d PRECEDING", b.offset)
	case BoundaryCurrentRow:
		return "CURRENT ROW"
	case BoundaryFollowing:
		return fmt.Sprintf("%d FOLLOWING", b.offset)
	case BoundaryUnboundedFollowing:
		return "UNBOUNDED FOLLOWING"
	default:
		return "UNKNOWN"
	}
}

// Window function expressions

// Type returns the expression type
func (w *WindowExpr) Type() ExprType {
	return ExprWindow
}

// String returns the string representation
func (w *WindowExpr) String() string {
	return fmt.Sprintf("%s %s", w.function.String(), w.window.String())
}

// Type returns the expression type
func (w *WindowFunctionExpr) Type() ExprType {
	return ExprWindowFunction
}

// String returns the string representation
func (w *WindowFunctionExpr) String() string {
	if len(w.args) == 0 {
		return fmt.Sprintf("%s()", w.funcName)
	}

	var argStrings []string
	for _, arg := range w.args {
		argStrings = append(argStrings, arg.String())
	}
	return fmt.Sprintf("%s(%s)", w.funcName, strings.Join(argStrings, ", "))
}

// Over creates a window expression with the specified window
func (w *WindowFunctionExpr) Over(window *WindowSpec) *WindowExpr {
	return &WindowExpr{
		function: w,
		window:   window,
	}
}

// Window function constructors

// RowNumber creates a ROW_NUMBER() window function
func RowNumber() *WindowFunctionExpr {
	return &WindowFunctionExpr{
		funcName: "ROW_NUMBER",
		args:     nil,
	}
}

// Rank creates a RANK() window function
func Rank() *WindowFunctionExpr {
	return &WindowFunctionExpr{
		funcName: "RANK",
		args:     nil,
	}
}

// DenseRank creates a DENSE_RANK() window function
func DenseRank() *WindowFunctionExpr {
	return &WindowFunctionExpr{
		funcName: "DENSE_RANK",
		args:     nil,
	}
}

// Lag creates a LAG() window function
func Lag(expr Expr, offset int) *WindowFunctionExpr {
	return &WindowFunctionExpr{
		funcName: "LAG",
		args:     []Expr{expr, &LiteralExpr{value: offset}},
	}
}

// Lead creates a LEAD() window function
func Lead(expr Expr, offset int) *WindowFunctionExpr {
	return &WindowFunctionExpr{
		funcName: "LEAD",
		args:     []Expr{expr, &LiteralExpr{value: offset}},
	}
}

// FirstValue creates a FIRST_VALUE() window function
func FirstValue(expr Expr) *WindowFunctionExpr {
	return &WindowFunctionExpr{
		funcName: "FIRST_VALUE",
		args:     []Expr{expr},
	}
}

// LastValue creates a LAST_VALUE() window function
func LastValue(expr Expr) *WindowFunctionExpr {
	return &WindowFunctionExpr{
		funcName: "LAST_VALUE",
		args:     []Expr{expr},
	}
}

// Over creates a window expression with the specified window for aggregation functions
func (a *AggregationExpr) Over(window *WindowSpec) *WindowExpr {
	return &WindowExpr{
		function: a,
		window:   window,
	}
}
