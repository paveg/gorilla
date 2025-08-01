// Package sql provides SQL query parsing and execution for DataFrames
package sql

import (
	"fmt"
	"strings"

	"github.com/paveg/gorilla/internal/expr"
)

// StatementType represents the type of SQL statement.
type StatementType int

const (
	SelectStatementType StatementType = iota
	InsertStatementType
	UpdateStatementType
	DeleteStatementType
)

// Statement interface for all SQL statement types.
type Statement interface {
	StatementType() StatementType
	String() string
}

// SelectStatement represents a SQL SELECT statement.
type SelectStatement struct {
	SelectList    []SelectItem
	FromClause    *FromClause
	WhereClause   *WhereClause
	GroupByClause *GroupByClause
	HavingClause  *HavingClause
	OrderByClause *OrderByClause
	LimitClause   *LimitClause
}

func (s *SelectStatement) StatementType() StatementType {
	return SelectStatementType
}

func (s *SelectStatement) String() string {
	var parts []string

	// SELECT clause
	selectItems := make([]string, len(s.SelectList))
	for i, item := range s.SelectList {
		selectItems[i] = item.String()
	}
	parts = append(parts, "SELECT "+strings.Join(selectItems, ", "))

	// FROM clause
	if s.FromClause != nil {
		parts = append(parts, s.FromClause.String())
	}

	// WHERE clause
	if s.WhereClause != nil {
		parts = append(parts, s.WhereClause.String())
	}

	// GROUP BY clause
	if s.GroupByClause != nil {
		parts = append(parts, s.GroupByClause.String())
	}

	// HAVING clause
	if s.HavingClause != nil {
		parts = append(parts, s.HavingClause.String())
	}

	// ORDER BY clause
	if s.OrderByClause != nil {
		parts = append(parts, s.OrderByClause.String())
	}

	// LIMIT clause
	if s.LimitClause != nil {
		parts = append(parts, s.LimitClause.String())
	}

	return strings.Join(parts, " ")
}

// SelectItem represents an item in the SELECT list.
type SelectItem struct {
	Expression expr.Expr
	Alias      string
	IsWildcard bool
}

func (s *SelectItem) String() string {
	if s.IsWildcard {
		return "*"
	}

	result := s.Expression.String()
	if s.Alias != "" {
		result += " AS " + s.Alias
	}
	return result
}

// FromClause represents the FROM clause with table references.
type FromClause struct {
	TableName string
	Alias     string
	Joins     []JoinClause
}

func (f *FromClause) String() string {
	result := "FROM " + f.TableName
	if f.Alias != "" {
		result += " AS " + f.Alias
	}

	for _, join := range f.Joins {
		result += " " + join.String()
	}

	return result
}

// JoinType represents different types of joins.
type JoinType int

const (
	InnerJoinType JoinType = iota
	LeftJoinType
	RightJoinType
	FullJoinType
)

func (jt JoinType) String() string {
	switch jt {
	case InnerJoinType:
		return "INNER JOIN"
	case LeftJoinType:
		return "LEFT JOIN"
	case RightJoinType:
		return "RIGHT JOIN"
	case FullJoinType:
		return "FULL JOIN"
	default:
		return "INNER JOIN"
	}
}

// JoinClause represents a JOIN clause.
type JoinClause struct {
	Type      JoinType
	TableName string
	Alias     string
	OnClause  expr.Expr
}

func (j *JoinClause) String() string {
	result := j.Type.String() + " " + j.TableName
	if j.Alias != "" {
		result += " AS " + j.Alias
	}
	if j.OnClause != nil {
		result += " ON " + j.OnClause.String()
	}
	return result
}

// WhereClause represents the WHERE clause.
type WhereClause struct {
	Condition expr.Expr
}

func (w *WhereClause) String() string {
	return "WHERE " + w.Condition.String()
}

// GroupByClause represents the GROUP BY clause.
type GroupByClause struct {
	Columns []expr.Expr
}

func (g *GroupByClause) String() string {
	columnStrings := make([]string, len(g.Columns))
	for i, col := range g.Columns {
		columnStrings[i] = col.String()
	}
	return "GROUP BY " + strings.Join(columnStrings, ", ")
}

// HavingClause represents the HAVING clause.
type HavingClause struct {
	Condition expr.Expr
}

func (h *HavingClause) String() string {
	return "HAVING " + h.Condition.String()
}

// OrderByClause represents the ORDER BY clause.
type OrderByClause struct {
	OrderItems []OrderByItem
}

func (o *OrderByClause) String() string {
	itemStrings := make([]string, len(o.OrderItems))
	for i, item := range o.OrderItems {
		itemStrings[i] = item.String()
	}
	return "ORDER BY " + strings.Join(itemStrings, ", ")
}

// OrderDirection represents sort direction.
type OrderDirection int

const (
	AscendingOrder OrderDirection = iota
	DescendingOrder
)

func (od OrderDirection) String() string {
	switch od {
	case AscendingOrder:
		return AscOrder
	case DescendingOrder:
		return DescOrder
	default:
		return AscOrder
	}
}

// OrderByItem represents an item in the ORDER BY clause.
type OrderByItem struct {
	Expression expr.Expr
	Direction  OrderDirection
}

func (o *OrderByItem) String() string {
	return o.Expression.String() + " " + o.Direction.String()
}

// Constants for LIMIT clause handling.
const (
	// OffsetOnlyLimit is a special value used in LimitClause.Count to indicate
	// that this is an OFFSET-only query (no LIMIT constraint).
	OffsetOnlyLimit = -1
)

// LimitClause represents the LIMIT clause.
type LimitClause struct {
	Count  int64
	Offset int64
}

func (l *LimitClause) String() string {
	result := fmt.Sprintf("LIMIT %d", l.Count)
	if l.Offset > 0 {
		result += fmt.Sprintf(" OFFSET %d", l.Offset)
	}
	return result
}

// Function represents a SQL function call.
type Function struct {
	Name string
	Args []expr.Expr
}

func (f *Function) String() string {
	argStrings := make([]string, len(f.Args))
	for i, arg := range f.Args {
		argStrings[i] = arg.String()
	}
	return f.Name + "(" + strings.Join(argStrings, ", ") + ")"
}

// Sort directions.
const (
	AscOrder  = "ASC"
	DescOrder = "DESC"
)

// Aggregation functions.
const (
	CountFunction = "COUNT"
	SumFunction   = "SUM"
	AvgFunction   = "AVG"
	MinFunction   = "MIN"
	MaxFunction   = "MAX"
)

// String functions.
const (
	UpperFunction  = "UPPER"
	LowerFunction  = "LOWER"
	LengthFunction = "LENGTH"
	TrimFunction   = "TRIM"
	SubstrFunction = "SUBSTR"
)

// Math functions.
const (
	AbsFunction   = "ABS"
	RoundFunction = "ROUND"
	FloorFunction = "FLOOR"
	CeilFunction  = "CEIL"
)

// Date functions.
const (
	NowFunction      = "NOW"
	DateAddFunction  = "DATE_ADD"
	DateSubFunction  = "DATE_SUB"
	DateDiffFunction = "DATE_DIFF"
	ExtractFunction  = "EXTRACT"
)
