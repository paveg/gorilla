// Package gorilla provides a high-performance, concurrent DataFrame library.
// This package is the sole public API for the library.
package gorilla

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
)

// ISeries provides a type-erased interface for Series of any type
type ISeries interface {
	Name() string
	Len() int
	DataType() arrow.DataType
	IsNull(index int) bool
	String() string
	Array() arrow.Array
	Release()
}

// DataFrame is the public type for a DataFrame.
// It wraps the internal dataframe.DataFrame to hide implementation details.
type DataFrame struct {
	df *dataframe.DataFrame
}

// LazyFrame is the public type for lazy evaluation.
type LazyFrame struct {
	lf *dataframe.LazyFrame
}

// LazyGroupBy is the public type for lazy group by operations.
type LazyGroupBy struct {
	lgb *dataframe.LazyGroupBy
}

// GroupBy is the public type for eager group by operations.
type GroupBy struct {
	gb *dataframe.GroupBy
}

// Expression is the public type for defining operations.
type Expression struct {
	expr expr.Expr
}

// AggregationExpression is the public type for aggregation operations.
type AggregationExpression struct {
	aggr *expr.AggregationExpr
}

// JoinType represents the type of join operation
type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
	FullOuterJoin
)

// JoinOptions specifies parameters for join operations
type JoinOptions struct {
	Type      JoinType
	LeftKey   string   // Single join key for left DataFrame
	RightKey  string   // Single join key for right DataFrame
	LeftKeys  []string // Multiple join keys for left DataFrame
	RightKeys []string // Multiple join keys for right DataFrame
}

// NewDataFrame creates a new DataFrame from ISeries.
func NewDataFrame(series ...ISeries) *DataFrame {
	// Convert ISeries to dataframe.ISeries
	internalSeries := make([]dataframe.ISeries, len(series))
	for i, s := range series {
		internalSeries[i] = s
	}
	return &DataFrame{df: dataframe.New(internalSeries...)}
}

// NewSeries creates a new typed Series from values.
func NewSeries[T any](name string, values []T, mem memory.Allocator) ISeries {
	return series.New(name, values, mem)
}

// DataFrame methods

// Columns returns the column names in order.
func (d *DataFrame) Columns() []string {
	return d.df.Columns()
}

// Len returns the number of rows.
func (d *DataFrame) Len() int {
	return d.df.Len()
}

// Width returns the number of columns.
func (d *DataFrame) Width() int {
	return d.df.Width()
}

// Column returns the column with the given name.
func (d *DataFrame) Column(name string) (ISeries, bool) {
	return d.df.Column(name)
}

// Select returns a new DataFrame with only the specified columns.
func (d *DataFrame) Select(names ...string) *DataFrame {
	return &DataFrame{df: d.df.Select(names...)}
}

// Drop returns a new DataFrame without the specified columns.
func (d *DataFrame) Drop(names ...string) *DataFrame {
	return &DataFrame{df: d.df.Drop(names...)}
}

// HasColumn returns true if the DataFrame has the given column.
func (d *DataFrame) HasColumn(name string) bool {
	return d.df.HasColumn(name)
}

// String returns a string representation of the DataFrame.
func (d *DataFrame) String() string {
	return d.df.String()
}

// Slice returns a new DataFrame with rows from start to end (exclusive).
func (d *DataFrame) Slice(start, end int) *DataFrame {
	return &DataFrame{df: d.df.Slice(start, end)}
}

// Sort returns a new DataFrame sorted by the specified column.
func (d *DataFrame) Sort(column string, ascending bool) *DataFrame {
	return &DataFrame{df: d.df.Sort(column, ascending)}
}

// SortBy returns a new DataFrame sorted by multiple columns.
func (d *DataFrame) SortBy(columns []string, ascending []bool) *DataFrame {
	return &DataFrame{df: d.df.SortBy(columns, ascending)}
}

// Concat concatenates this DataFrame with others.
func (d *DataFrame) Concat(others ...*DataFrame) *DataFrame {
	internalDfs := make([]*dataframe.DataFrame, len(others))
	for i, other := range others {
		internalDfs[i] = other.df
	}
	return &DataFrame{df: d.df.Concat(internalDfs...)}
}

// Release frees the memory used by the DataFrame.
func (d *DataFrame) Release() {
	d.df.Release()
}

// GroupBy creates a GroupBy operation for eager evaluation.
func (d *DataFrame) GroupBy(columns ...string) *GroupBy {
	return &GroupBy{gb: d.df.GroupBy(columns...)}
}

// Join performs a join operation with another DataFrame.
func (d *DataFrame) Join(right *DataFrame, options *JoinOptions) (*DataFrame, error) {
	internalOptions := &dataframe.JoinOptions{
		Type:      dataframe.JoinType(options.Type),
		LeftKey:   options.LeftKey,
		RightKey:  options.RightKey,
		LeftKeys:  options.LeftKeys,
		RightKeys: options.RightKeys,
	}
	result, err := d.df.Join(right.df, internalOptions)
	if err != nil {
		return nil, err
	}
	return &DataFrame{df: result}, nil
}

// Lazy initiates a lazy operation on a DataFrame.
func (d *DataFrame) Lazy() *LazyFrame {
	return &LazyFrame{lf: d.df.Lazy()}
}

// LazyFrame methods

// Filter adds a filter operation to the LazyFrame.
func (lf *LazyFrame) Filter(predicate Expression) *LazyFrame {
	return &LazyFrame{lf: lf.lf.Filter(predicate.expr)}
}

// Select adds a select operation to the LazyFrame.
func (lf *LazyFrame) Select(columns ...string) *LazyFrame {
	return &LazyFrame{lf: lf.lf.Select(columns...)}
}

// WithColumn adds a new column to the LazyFrame.
func (lf *LazyFrame) WithColumn(name string, expr Expression) *LazyFrame {
	return &LazyFrame{lf: lf.lf.WithColumn(name, expr.expr)}
}

// Sort adds a sort operation to the LazyFrame.
func (lf *LazyFrame) Sort(column string, ascending bool) *LazyFrame {
	return &LazyFrame{lf: lf.lf.Sort(column, ascending)}
}

// SortBy adds a multi-column sort operation to the LazyFrame.
func (lf *LazyFrame) SortBy(columns []string, ascending []bool) *LazyFrame {
	return &LazyFrame{lf: lf.lf.SortBy(columns, ascending)}
}

// GroupBy adds a group by operation to the LazyFrame.
func (lf *LazyFrame) GroupBy(columns ...string) *LazyGroupBy {
	return &LazyGroupBy{lgb: lf.lf.GroupBy(columns...)}
}

// Join adds a join operation to the LazyFrame.
func (lf *LazyFrame) Join(right *LazyFrame, options *JoinOptions) *LazyFrame {
	internalOptions := &dataframe.JoinOptions{
		Type:      dataframe.JoinType(options.Type),
		LeftKey:   options.LeftKey,
		RightKey:  options.RightKey,
		LeftKeys:  options.LeftKeys,
		RightKeys: options.RightKeys,
	}
	return &LazyFrame{lf: lf.lf.Join(right.lf, internalOptions)}
}

// Collect executes all pending operations and returns the final DataFrame.
func (lf *LazyFrame) Collect() (*DataFrame, error) {
	result, err := lf.lf.Collect()
	if err != nil {
		return nil, err
	}
	return &DataFrame{df: result}, nil
}

// String returns a string representation of the LazyFrame operations.
func (lf *LazyFrame) String() string {
	return lf.lf.String()
}

// Release frees the memory used by the LazyFrame.
func (lf *LazyFrame) Release() {
	lf.lf.Release()
}

// LazyGroupBy methods

// Agg adds aggregation operations to the LazyGroupBy.
func (lgb *LazyGroupBy) Agg(aggregations ...*AggregationExpression) *LazyFrame {
	internalAggs := make([]*expr.AggregationExpr, len(aggregations))
	for i, agg := range aggregations {
		internalAggs[i] = agg.aggr
	}
	return &LazyFrame{lf: lgb.lgb.Agg(internalAggs...)}
}

// Sum adds a sum aggregation for the specified column.
func (lgb *LazyGroupBy) Sum(column string) *LazyFrame {
	return &LazyFrame{lf: lgb.lgb.Sum(column)}
}

// Count adds a count aggregation for the specified column.
func (lgb *LazyGroupBy) Count(column string) *LazyFrame {
	return &LazyFrame{lf: lgb.lgb.Count(column)}
}

// Mean adds a mean aggregation for the specified column.
func (lgb *LazyGroupBy) Mean(column string) *LazyFrame {
	return &LazyFrame{lf: lgb.lgb.Mean(column)}
}

// Min adds a min aggregation for the specified column.
func (lgb *LazyGroupBy) Min(column string) *LazyFrame {
	return &LazyFrame{lf: lgb.lgb.Min(column)}
}

// Max adds a max aggregation for the specified column.
func (lgb *LazyGroupBy) Max(column string) *LazyFrame {
	return &LazyFrame{lf: lgb.lgb.Max(column)}
}

// GroupBy methods

// Agg performs aggregation operations on the GroupBy.
func (gb *GroupBy) Agg(aggregations ...*AggregationExpression) *DataFrame {
	internalAggs := make([]*expr.AggregationExpr, len(aggregations))
	for i, agg := range aggregations {
		internalAggs[i] = agg.aggr
	}
	return &DataFrame{df: gb.gb.Agg(internalAggs...)}
}

// Expression factory functions

// Col returns an Expression representing a column reference.
func Col(name string) Expression {
	return Expression{expr: expr.Col(name)}
}

// Lit returns an Expression representing a literal value.
func Lit(value interface{}) Expression {
	return Expression{expr: expr.Lit(value)}
}

// Sum returns an aggregation expression for sum.
func Sum(column Expression) *AggregationExpression {
	return &AggregationExpression{aggr: expr.Sum(column.expr)}
}

// Count returns an aggregation expression for count.
func Count(column Expression) *AggregationExpression {
	return &AggregationExpression{aggr: expr.Count(column.expr)}
}

// Mean returns an aggregation expression for mean.
func Mean(column Expression) *AggregationExpression {
	return &AggregationExpression{aggr: expr.Mean(column.expr)}
}

// Min returns an aggregation expression for min.
func Min(column Expression) *AggregationExpression {
	return &AggregationExpression{aggr: expr.Min(column.expr)}
}

// Max returns an aggregation expression for max.
func Max(column Expression) *AggregationExpression {
	return &AggregationExpression{aggr: expr.Max(column.expr)}
}

// If returns a conditional expression.
func If(condition, thenValue, elseValue Expression) Expression {
	return Expression{expr: expr.If(condition.expr, thenValue.expr, elseValue.expr)}
}

// Coalesce returns the first non-null expression.
func Coalesce(exprs ...Expression) Expression {
	internalExprs := make([]expr.Expr, len(exprs))
	for i, e := range exprs {
		internalExprs[i] = e.expr
	}
	return Expression{expr: expr.Coalesce(internalExprs...)}
}

// Concat concatenates string expressions.
func Concat(exprs ...Expression) Expression {
	internalExprs := make([]expr.Expr, len(exprs))
	for i, e := range exprs {
		internalExprs[i] = e.expr
	}
	return Expression{expr: expr.Concat(internalExprs...)}
}

// Case starts a case expression.
func Case() Expression {
	return Expression{expr: expr.Case()}
}

// Expression methods for chaining
// These handle different expression types appropriately

// Eq returns an equality comparison expression.
// Comparison operations are supported by ColumnExpr only, others should use factory functions.
func (e Expression) Eq(other Expression) Expression {
	if colExpr, ok := e.expr.(*expr.ColumnExpr); ok {
		return Expression{expr: colExpr.Eq(other.expr)}
	}
	panic(fmt.Sprintf("Eq operation only supported on column expressions, got %T", e.expr))
}

func (e Expression) Ne(other Expression) Expression {
	if colExpr, ok := e.expr.(*expr.ColumnExpr); ok {
		return Expression{expr: colExpr.Ne(other.expr)}
	}
	panic(fmt.Sprintf("Ne operation only supported on column expressions, got %T", e.expr))
}

func (e Expression) Lt(other Expression) Expression {
	if colExpr, ok := e.expr.(*expr.ColumnExpr); ok {
		return Expression{expr: colExpr.Lt(other.expr)}
	}
	panic(fmt.Sprintf("Lt operation only supported on column expressions, got %T", e.expr))
}

func (e Expression) Le(other Expression) Expression {
	if colExpr, ok := e.expr.(*expr.ColumnExpr); ok {
		return Expression{expr: colExpr.Le(other.expr)}
	}
	panic(fmt.Sprintf("Le operation only supported on column expressions, got %T", e.expr))
}

func (e Expression) Gt(other Expression) Expression {
	if colExpr, ok := e.expr.(*expr.ColumnExpr); ok {
		return Expression{expr: colExpr.Gt(other.expr)}
	}
	panic(fmt.Sprintf("Gt operation only supported on column expressions, got %T", e.expr))
}

func (e Expression) Ge(other Expression) Expression {
	if colExpr, ok := e.expr.(*expr.ColumnExpr); ok {
		return Expression{expr: colExpr.Ge(other.expr)}
	}
	panic(fmt.Sprintf("Ge operation only supported on column expressions, got %T", e.expr))
}

// Add returns an addition expression.
// Arithmetic operations are supported by ColumnExpr and BinaryExpr.
func (e Expression) Add(other Expression) Expression {
	switch exprType := e.expr.(type) {
	case *expr.ColumnExpr:
		return Expression{expr: exprType.Add(other.expr)}
	case *expr.BinaryExpr:
		return Expression{expr: exprType.Add(other.expr)}
	default:
		panic(fmt.Sprintf("Add operation only supported on column and binary expressions, got %T", e.expr))
	}
}

func (e Expression) Sub(other Expression) Expression {
	switch exprType := e.expr.(type) {
	case *expr.ColumnExpr:
		return Expression{expr: exprType.Sub(other.expr)}
	case *expr.BinaryExpr:
		return Expression{expr: exprType.Sub(other.expr)}
	default:
		panic(fmt.Sprintf("Sub operation only supported on column and binary expressions, got %T", e.expr))
	}
}

func (e Expression) Mul(other Expression) Expression {
	switch exprType := e.expr.(type) {
	case *expr.ColumnExpr:
		return Expression{expr: exprType.Mul(other.expr)}
	case *expr.BinaryExpr:
		return Expression{expr: exprType.Mul(other.expr)}
	default:
		panic(fmt.Sprintf("Mul operation only supported on column and binary expressions, got %T", e.expr))
	}
}

func (e Expression) Div(other Expression) Expression {
	switch exprType := e.expr.(type) {
	case *expr.ColumnExpr:
		return Expression{expr: exprType.Div(other.expr)}
	case *expr.BinaryExpr:
		return Expression{expr: exprType.Div(other.expr)}
	default:
		panic(fmt.Sprintf("Div operation only supported on column and binary expressions, got %T", e.expr))
	}
}

// And returns a logical AND expression.
// Logical operations are supported by BinaryExpr only.
func (e Expression) And(other Expression) Expression {
	if binExpr, ok := e.expr.(*expr.BinaryExpr); ok {
		return Expression{expr: binExpr.And(other.expr)}
	}
	panic(fmt.Sprintf("And operation only supported on binary expressions, got %T", e.expr))
}

func (e Expression) Or(other Expression) Expression {
	if binExpr, ok := e.expr.(*expr.BinaryExpr); ok {
		return Expression{expr: binExpr.Or(other.expr)}
	}
	panic(fmt.Sprintf("Or operation only supported on binary expressions, got %T", e.expr))
}

// AggregationExpression methods

// As returns an aggregation expression with an alias.
func (a *AggregationExpression) As(alias string) *AggregationExpression {
	return &AggregationExpression{aggr: a.aggr.As(alias)}
}
