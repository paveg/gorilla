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

// Expression Types
//
// The gorilla library now exposes expression types directly from the internal expr package
// for optimal performance. This eliminates wrapper overhead and type assertion costs.
//
// Available expression types:
//   - *expr.ColumnExpr - Column references (created by Col function)
//   - *expr.LiteralExpr - Literal values (created by Lit function)
//   - *expr.BinaryExpr - Binary operations (created by method chaining)
//   - *expr.AggregationExpr - Aggregation operations (created by Sum, Count, etc.)
//   - *expr.FunctionExpr - Function calls (created by If, Coalesce, etc.)
//   - *expr.CaseExpr - Case expressions (created by Case function)
//
// All expression types implement the expr.Expr interface and support method chaining
// for building complex operations without wrapper overhead.
//
// Example:
//
//	// Complex expression with chaining (all concrete types)
//	col := gorilla.Col("salary")        // returns *expr.ColumnExpr
//	expr1 := col.Mul(gorilla.Lit(1.1))  // returns *expr.BinaryExpr
//	expr2 := expr1.Gt(gorilla.Lit(50000)) // returns *expr.BinaryExpr
//	final := expr2.And(gorilla.Col("active").Eq(gorilla.Lit(true))) // returns *expr.BinaryExpr
//
//	result, err := df.Lazy().Filter(final).Collect()

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
//
// The predicate can be any expression that implements expr.Expr interface.
func (lf *LazyFrame) Filter(predicate expr.Expr) *LazyFrame {
	return &LazyFrame{lf: lf.lf.Filter(predicate)}
}

// Select adds a select operation to the LazyFrame.
func (lf *LazyFrame) Select(columns ...string) *LazyFrame {
	return &LazyFrame{lf: lf.lf.Select(columns...)}
}

// WithColumn adds a new column to the LazyFrame.
//
// The expression can be any expression that implements expr.Expr interface.
func (lf *LazyFrame) WithColumn(name string, expression expr.Expr) *LazyFrame {
	return &LazyFrame{lf: lf.lf.WithColumn(name, expression)}
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
//
// Takes aggregation expressions directly without wrapper overhead.
func (lgb *LazyGroupBy) Agg(aggregations ...*expr.AggregationExpr) *LazyFrame {
	return &LazyFrame{lf: lgb.lgb.Agg(aggregations...)}
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
//
// Takes aggregation expressions directly without wrapper overhead.
func (gb *GroupBy) Agg(aggregations ...*expr.AggregationExpr) *DataFrame {
	return &DataFrame{df: gb.gb.Agg(aggregations...)}
}

// Expression factory functions

// Col creates a column expression that references a column by name.
//
// This is the primary way to reference columns in filters, selections, and
// other DataFrame operations. The column name must exist in the DataFrame
// when the expression is evaluated.
//
// Returns a concrete *expr.ColumnExpr for optimal performance and type safety.
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
func Col(name string) *expr.ColumnExpr {
	return expr.Col(name)
}

// Lit creates a literal expression that represents a constant value.
//
// This is used to create expressions with constant values for comparisons,
// arithmetic operations, and other transformations. The value type should
// match the expected operation type.
//
// Supported types: string, int64, int32, float64, float32, bool
//
// Returns a concrete *expr.LiteralExpr for optimal performance and type safety.
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
func Lit(value interface{}) *expr.LiteralExpr {
	return expr.Lit(value)
}

// Sum returns an aggregation expression for sum.
//
// Returns a concrete *expr.AggregationExpr for optimal performance and type safety.
func Sum(column expr.Expr) *expr.AggregationExpr {
	return expr.Sum(column)
}

// Count returns an aggregation expression for count.
//
// Returns a concrete *expr.AggregationExpr for optimal performance and type safety.
func Count(column expr.Expr) *expr.AggregationExpr {
	return expr.Count(column)
}

// Mean returns an aggregation expression for mean.
//
// Returns a concrete *expr.AggregationExpr for optimal performance and type safety.
func Mean(column expr.Expr) *expr.AggregationExpr {
	return expr.Mean(column)
}

// Min returns an aggregation expression for min.
//
// Returns a concrete *expr.AggregationExpr for optimal performance and type safety.
func Min(column expr.Expr) *expr.AggregationExpr {
	return expr.Min(column)
}

// Max returns an aggregation expression for max.
//
// Returns a concrete *expr.AggregationExpr for optimal performance and type safety.
func Max(column expr.Expr) *expr.AggregationExpr {
	return expr.Max(column)
}

// If returns a conditional expression.
//
// Returns a concrete *expr.FunctionExpr for optimal performance and type safety.
func If(condition, thenValue, elseValue expr.Expr) *expr.FunctionExpr {
	return expr.If(condition, thenValue, elseValue)
}

// Coalesce returns the first non-null expression.
//
// Returns a concrete *expr.FunctionExpr for optimal performance and type safety.
func Coalesce(exprs ...expr.Expr) *expr.FunctionExpr {
	return expr.Coalesce(exprs...)
}

// Concat concatenates string expressions.
//
// Returns a concrete *expr.FunctionExpr for optimal performance and type safety.
func Concat(exprs ...expr.Expr) *expr.FunctionExpr {
	return expr.Concat(exprs...)
}

// Case starts a case expression.
//
// Returns a concrete *expr.CaseExpr for optimal performance and type safety.
func Case() *expr.CaseExpr {
	return expr.Case()
}

// Expression methods are now available directly on the concrete expression types
// returned by the factory functions (Col, Lit, etc.). This eliminates wrapper
// overhead and type assertion costs.
//
// For example:
//   - Col("age") returns *expr.ColumnExpr with methods like Gt(), Eq(), Add(), etc.
//   - Lit(30) returns *expr.LiteralExpr
//   - col.Gt(lit) returns *expr.BinaryExpr with methods like And(), Or(), etc.
//   - Sum(col) returns *expr.AggregationExpr with methods like As(), etc.
//
// All expression types implement the expr.Expr interface for use in DataFrame operations.
