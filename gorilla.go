// Package gorilla provides a high-performance, in-memory DataFrame library for Go.
//
// Gorilla is built on Apache Arrow and provides fast, efficient data manipulation
// with lazy evaluation and automatic parallelization. It offers a clear, intuitive
// API for filtering, selecting, transforming, and analyzing tabular data.
//
// # Core Concepts
//
// DataFrame: A 2-dimensional table of data with named columns (Series).
// Series: A 1-dimensional array of homogeneous data representing a single column.
// LazyFrame: Deferred computation that builds an optimized query plan.
// Expression: Type-safe operations for filtering, transforming, and aggregating data.
//
// # Memory Management
//
// Gorilla uses Apache Arrow's memory management. Always call Release() on DataFrames
// and Series to prevent memory leaks. The recommended pattern is:
//
//	df := gorilla.NewDataFrame(series1, series2)
//	defer df.Release() // Essential for proper cleanup
//
// # Basic Usage
//
//	package main
//
//	import (
//		"fmt"
//		"github.com/apache/arrow-go/v18/arrow/memory"
//		"github.com/paveg/gorilla"
//	)
//
//	func main() {
//		mem := memory.NewGoAllocator()
//
//		// Create Series (columns)
//		names := gorilla.NewSeries("name", []string{"Alice", "Bob", "Charlie"}, mem)
//		ages := gorilla.NewSeries("age", []int64{25, 30, 35}, mem)
//		defer names.Release()
//		defer ages.Release()
//
//		// Create DataFrame
//		df := gorilla.NewDataFrame(names, ages)
//		defer df.Release()
//
//		// Lazy evaluation with method chaining
//		result, err := df.Lazy().
//			Filter(gorilla.Col("age").Gt(gorilla.Lit(int64(30)))).
//			Select("name").
//			Collect()
//		if err != nil {
//			panic(err)
//		}
//		defer result.Release()
//
//		fmt.Println(result)
//	}
//
// # Performance Features
//
// - Zero-copy operations using Apache Arrow columnar format
// - Automatic parallelization for DataFrames with 1000+ rows
// - Lazy evaluation with query optimization
// - Efficient join algorithms with automatic strategy selection
// - Memory-efficient aggregations and transformations
//
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

// ISeries provides a type-erased interface for Series of any supported type.
//
// ISeries allows different typed Series to be used together in DataFrames
// and operations. It wraps Apache Arrow arrays and provides common operations
// for all Series types.
//
// Supported data types: string, int64, int32, float64, float32, bool
//
// Memory management: Series implement the Release() method and must be
// released to prevent memory leaks:
//
//	series := gorilla.NewSeries("name", []string{"Alice", "Bob"}, mem)
//	defer series.Release()
//
// The interface provides:
//   - Name() - column name for use in DataFrames
//   - Len() - number of elements in the Series
//   - DataType() - Apache Arrow data type information
//   - IsNull(index) - null value checking
//   - String() - human-readable representation
//   - Array() - access to underlying Arrow array
//   - Release() - memory cleanup
type ISeries interface {
	Name() string             // Returns the name of the Series
	Len() int                 // Returns the number of elements
	DataType() arrow.DataType // Returns the Apache Arrow data type
	IsNull(index int) bool    // Checks if the value at index is null
	String() string           // Returns a string representation
	Array() arrow.Array       // Returns the underlying Arrow array
	Release()                 // Releases memory resources
}

// DataFrame represents a 2-dimensional table of data with named columns.
//
// A DataFrame is composed of multiple Series (columns) and provides operations
// for filtering, selecting, transforming, joining, and aggregating data.
// It supports both eager and lazy evaluation patterns.
//
// Key features:
//   - Zero-copy operations using Apache Arrow columnar format
//   - Automatic parallelization for large datasets (1000+ rows)
//   - Type-safe operations through the Expression system
//   - Memory-efficient storage and computation
//
// Memory management: DataFrames must be released to prevent memory leaks:
//
//	df := gorilla.NewDataFrame(series1, series2)
//	defer df.Release()
//
// Operations can be performed eagerly or using lazy evaluation:
//
//	// Eager: operations execute immediately
//	filtered := df.Filter(gorilla.Col("age").Gt(gorilla.Lit(30)))
//
//	// Lazy: operations build a query plan, execute on Collect()
//	result, err := df.Lazy().
//		Filter(gorilla.Col("age").Gt(gorilla.Lit(30))).
//		Select("name", "age").
//		Collect()
type DataFrame struct {
	df *dataframe.DataFrame
}

// LazyFrame provides deferred computation with query optimization.
//
// LazyFrame builds an Abstract Syntax Tree (AST) of operations without
// executing them immediately. This allows for query optimization, operation
// fusion, and efficient memory usage. Operations are only executed when
// Collect() is called.
//
// Benefits of lazy evaluation:
//   - Query optimization (predicate pushdown, operation fusion)
//   - Memory efficiency (only final results allocated)
//   - Parallel execution planning
//   - Operation chaining with method syntax
//
// Example:
//
//	lazyResult := df.Lazy().
//		Filter(gorilla.Col("age").Gt(gorilla.Lit(25))).
//		WithColumn("bonus", gorilla.Col("salary").Mul(gorilla.Lit(0.1))).
//		GroupBy("department").
//		Agg(gorilla.Sum(gorilla.Col("salary")).As("total_salary")).
//		Select("department", "total_salary")
//
//	// Execute the entire plan efficiently
//	result, err := lazyResult.Collect()
//	defer result.Release()
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

// Expression represents a type-safe operation that can be applied to DataFrame columns.
//
// Expressions are the building blocks for DataFrame operations like filtering,
// transformations, and aggregations. They provide a fluent, chainable API
// for constructing complex data operations.
//
// Expressions are created using factory functions:
//   - Col("name") - references a column
//   - Lit(value) - represents a literal value
//   - Sum(expr) - aggregation functions
//
// Expressions support method chaining for operations:
//   - Arithmetic: Add(), Sub(), Mul(), Div()
//   - Comparisons: Eq(), Ne(), Gt(), Ge(), Lt(), Le()
//   - Logical: And(), Or(), Not()
//   - String operations: StartsWith(), EndsWith(), Contains()
//
// Example:
//
//	// Complex expression with chaining
//	expr := gorilla.Col("salary").
//		Mul(gorilla.Lit(1.1)).  // 10% raise
//		Gt(gorilla.Lit(50000)). // Filter high earners
//		And(gorilla.Col("active").Eq(gorilla.Lit(true)))
//
//	result, err := df.Lazy().Filter(expr).Collect()
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

// NewDataFrame creates a new DataFrame from one or more Series.
//
// A DataFrame is a 2-dimensional table with named columns. Each Series becomes
// a column in the DataFrame. All Series must have the same length.
//
// Memory management: The returned DataFrame must be released by calling Release()
// to prevent memory leaks. Use defer for automatic cleanup:
//
//	df := gorilla.NewDataFrame(series1, series2)
//	defer df.Release()
//
// Example:
//
//	mem := memory.NewGoAllocator()
//	names := gorilla.NewSeries("name", []string{"Alice", "Bob"}, mem)
//	ages := gorilla.NewSeries("age", []int64{25, 30}, mem)
//	defer names.Release()
//	defer ages.Release()
//
//	df := gorilla.NewDataFrame(names, ages)
//	defer df.Release()
//
//	fmt.Println(df) // Displays the DataFrame
func NewDataFrame(series ...ISeries) *DataFrame {
	// Convert ISeries to dataframe.ISeries
	internalSeries := make([]dataframe.ISeries, len(series))
	for i, s := range series {
		internalSeries[i] = s
	}
	return &DataFrame{df: dataframe.New(internalSeries...)}
}

// NewSeries creates a new typed Series from a slice of values.
//
// A Series is a 1-dimensional array of homogeneous data that represents a single
// column in a DataFrame. The type parameter T determines the data type of the Series.
//
// Supported types: string, int64, int32, float64, float32, bool
//
// Parameters:
//   - name: The name of the Series (becomes the column name in a DataFrame)
//   - values: Slice of values to populate the Series
//   - mem: Apache Arrow memory allocator for managing the underlying storage
//
// Memory management: The returned Series must be released by calling Release()
// to prevent memory leaks. Use defer for automatic cleanup:
//
//	series := gorilla.NewSeries("age", []int64{25, 30, 35}, mem)
//	defer series.Release()
//
// Example:
//
//	mem := memory.NewGoAllocator()
//
//	// Create different types of Series
//	names := gorilla.NewSeries("name", []string{"Alice", "Bob", "Charlie"}, mem)
//	ages := gorilla.NewSeries("age", []int64{25, 30, 35}, mem)
//	scores := gorilla.NewSeries("score", []float64{95.5, 87.2, 92.1}, mem)
//	active := gorilla.NewSeries("active", []bool{true, false, true}, mem)
//
//	defer names.Release()
//	defer ages.Release()
//	defer scores.Release()
//	defer active.Release()
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
func (d *DataFrame) Sort(column string, ascending bool) (*DataFrame, error) {
	result, err := d.df.Sort(column, ascending)
	if err != nil {
		return nil, err
	}
	return &DataFrame{df: result}, nil
}

// SortBy returns a new DataFrame sorted by multiple columns.
func (d *DataFrame) SortBy(columns []string, ascending []bool) (*DataFrame, error) {
	result, err := d.df.SortBy(columns, ascending)
	if err != nil {
		return nil, err
	}
	return &DataFrame{df: result}, nil
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

// Col creates an Expression that references a column by name.
//
// This is the primary way to reference columns in filters, selections, and
// other DataFrame operations. The column name must exist in the DataFrame
// when the expression is evaluated.
//
// Example:
//
//	// Reference the "age" column
//	ageCol := gorilla.Col("age")
//
//	// Use in filters and operations
//	result, err := df.Lazy().
//		Filter(gorilla.Col("age").Gt(gorilla.Lit(30))).
//		Select(gorilla.Col("name"), gorilla.Col("age")).
//		Collect()
func Col(name string) Expression {
	return Expression{expr: expr.Col(name)}
}

// Lit creates an Expression that represents a literal (constant) value.
//
// This is used to create expressions with constant values for comparisons,
// arithmetic operations, and other transformations. The value type should
// match the expected operation type.
//
// Supported types: string, int64, int32, float64, float32, bool
//
// Example:
//
//	// Literal values for comparisons
//	age30 := gorilla.Lit(int64(30))
//	name := gorilla.Lit("Alice")
//	score := gorilla.Lit(95.5)
//	active := gorilla.Lit(true)
//
//	// Use in operations
//	result, err := df.Lazy().
//		Filter(gorilla.Col("age").Gt(gorilla.Lit(int64(25)))).
//		WithColumn("bonus", gorilla.Col("salary").Mul(gorilla.Lit(0.1))).
//		Collect()
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
	return Expression{expr: expr.Invalid(fmt.Sprintf("Eq operation only supported on column expressions, got %T", e.expr))}
}

func (e Expression) Ne(other Expression) Expression {
	if colExpr, ok := e.expr.(*expr.ColumnExpr); ok {
		return Expression{expr: colExpr.Ne(other.expr)}
	}
	return Expression{expr: expr.Invalid(fmt.Sprintf("Ne operation only supported on column expressions, got %T", e.expr))}
}

func (e Expression) Lt(other Expression) Expression {
	if colExpr, ok := e.expr.(*expr.ColumnExpr); ok {
		return Expression{expr: colExpr.Lt(other.expr)}
	}
	return Expression{expr: expr.Invalid(fmt.Sprintf("Lt operation only supported on column expressions, got %T", e.expr))}
}

func (e Expression) Le(other Expression) Expression {
	if colExpr, ok := e.expr.(*expr.ColumnExpr); ok {
		return Expression{expr: colExpr.Le(other.expr)}
	}
	return Expression{expr: expr.Invalid(fmt.Sprintf("Le operation only supported on column expressions, got %T", e.expr))}
}

func (e Expression) Gt(other Expression) Expression {
	if colExpr, ok := e.expr.(*expr.ColumnExpr); ok {
		return Expression{expr: colExpr.Gt(other.expr)}
	}
	return Expression{expr: expr.Invalid(fmt.Sprintf("Gt operation only supported on column expressions, got %T", e.expr))}
}

func (e Expression) Ge(other Expression) Expression {
	if colExpr, ok := e.expr.(*expr.ColumnExpr); ok {
		return Expression{expr: colExpr.Ge(other.expr)}
	}
	return Expression{expr: expr.Invalid(fmt.Sprintf("Ge operation only supported on column expressions, got %T", e.expr))}
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
		return Expression{expr: expr.Invalid(fmt.Sprintf("Add unsupported on %T", e.expr))}
	}
}

func (e Expression) Sub(other Expression) Expression {
	switch exprType := e.expr.(type) {
	case *expr.ColumnExpr:
		return Expression{expr: exprType.Sub(other.expr)}
	case *expr.BinaryExpr:
		return Expression{expr: exprType.Sub(other.expr)}
	default:
		return Expression{expr: expr.Invalid(fmt.Sprintf("Sub unsupported on %T", e.expr))}
	}
}

func (e Expression) Mul(other Expression) Expression {
	switch exprType := e.expr.(type) {
	case *expr.ColumnExpr:
		return Expression{expr: exprType.Mul(other.expr)}
	case *expr.BinaryExpr:
		return Expression{expr: exprType.Mul(other.expr)}
	default:
		return Expression{expr: expr.Invalid(fmt.Sprintf("Mul unsupported on %T", e.expr))}
	}
}

func (e Expression) Div(other Expression) Expression {
	switch exprType := e.expr.(type) {
	case *expr.ColumnExpr:
		return Expression{expr: exprType.Div(other.expr)}
	case *expr.BinaryExpr:
		return Expression{expr: exprType.Div(other.expr)}
	default:
		return Expression{expr: expr.Invalid(fmt.Sprintf("Div unsupported on %T", e.expr))}
	}
}

// And returns a logical AND expression.
// Logical operations are supported by BinaryExpr only.
func (e Expression) And(other Expression) Expression {
	if binExpr, ok := e.expr.(*expr.BinaryExpr); ok {
		return Expression{expr: binExpr.And(other.expr)}
	}
	return Expression{expr: expr.Invalid(fmt.Sprintf("And unsupported on %T", e.expr))}
}

func (e Expression) Or(other Expression) Expression {
	if binExpr, ok := e.expr.(*expr.BinaryExpr); ok {
		return Expression{expr: binExpr.Or(other.expr)}
	}
	return Expression{expr: expr.Invalid(fmt.Sprintf("Or operation only supported on binary expressions, got %T", e.expr))}
}

// AggregationExpression methods

// As returns an aggregation expression with an alias.
func (a *AggregationExpression) As(alias string) *AggregationExpression {
	return &AggregationExpression{aggr: a.aggr.As(alias)}
}
