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
	"github.com/paveg/gorilla/internal/config"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
	"github.com/paveg/gorilla/internal/sql"
	"github.com/paveg/gorilla/internal/version"
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
	Name() string                 // Returns the name of the Series
	Len() int                     // Returns the number of elements
	DataType() arrow.DataType     // Returns the Apache Arrow data type
	IsNull(index int) bool        // Checks if the value at index is null
	String() string               // Returns a string representation
	Array() arrow.Array           // Returns the underlying Arrow array
	Release()                     // Releases memory resources
	GetAsString(index int) string // Returns the value at index as a string
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

// WithConfig returns a new DataFrame with the specified operation configuration.
//
// This allows per-DataFrame configuration overrides for parallel execution,
// memory usage, and other operational parameters. The configuration is
// inherited by lazy operations performed on this DataFrame.
//
// Parameters:
//   - opConfig: The operation configuration to apply to this DataFrame
//
// Returns:
//   - *DataFrame: A new DataFrame with the specified configuration
//
// Example:
//
//	config := config.OperationConfig{
//		ForceParallel:   true,
//		CustomChunkSize: 5000,
//		MaxMemoryUsage:  1024 * 1024 * 100, // 100MB
//	}
//
//	configuredDF := df.WithConfig(config)
//	defer configuredDF.Release()
//
//	// All operations on configuredDF will use the custom configuration
//	result := configuredDF.Lazy().
//		Filter(Col("amount").Gt(Lit(1000))).
//		Collect()
func (d *DataFrame) WithConfig(opConfig config.OperationConfig) *DataFrame {
	return &DataFrame{df: d.df.WithConfig(opConfig)}
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

// Version returns the current library version.
//
// This returns the semantic version of the Gorilla DataFrame library.
// During development, this may return "dev". In releases, it returns
// the tagged version (e.g., "v1.0.0").
//
// Example:
//
//	fmt.Printf("Using Gorilla DataFrame Library %s\n", gorilla.Version())
func Version() string {
	return version.Version
}

// BuildInfo returns detailed build information.
//
// This includes version, build date, Git commit, Go version, and
// dependency information. Useful for debugging and support.
//
// Example:
//
//	info := gorilla.BuildInfo()
//	fmt.Printf("Version: %s\n", info.Version)
//	fmt.Printf("Build Date: %s\n", info.BuildDate)
//	fmt.Printf("Git Commit: %s\n", info.GitCommit)
//	fmt.Printf("Go Version: %s\n", info.GoVersion)
func BuildInfo() version.BuildInfo {
	return version.Info()
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

// Example demonstrates basic DataFrame creation and operations.
func Example() {
	mem := memory.NewGoAllocator()

	// Create Series (columns)
	names := NewSeries("name", []string{"Alice", "Bob", "Charlie"}, mem)
	ages := NewSeries("age", []int64{25, 30, 35}, mem)
	defer names.Release()
	defer ages.Release()

	// Create DataFrame
	df := NewDataFrame(names, ages)
	defer df.Release()

	// Lazy evaluation with method chaining
	const filterAge = 30
	result, err := df.Lazy().
		Filter(Col("age").Gt(Lit(int64(filterAge)))).
		Select("name").
		Collect()
	if err != nil {
		panic(err)
	}
	defer result.Release()

	fmt.Println(result)
}

// ExampleDataFrameGroupBy demonstrates GroupBy operations with aggregations.
func ExampleDataFrameGroupBy() {
	mem := memory.NewGoAllocator()

	// Create test data
	departments := NewSeries("department", []string{"Engineering", "Sales", "Engineering", "Sales"}, mem)
	salaries := NewSeries("salary", []int64{100000, 80000, 120000, 85000}, mem)
	defer departments.Release()
	defer salaries.Release()

	df := NewDataFrame(departments, salaries)
	defer df.Release()

	// Group by department and calculate aggregations
	result, err := df.Lazy().
		GroupBy("department").
		Agg(
			Sum(Col("salary")).As("total_salary"),
			Count(Col("salary")).As("employee_count"),
		).
		Collect()
	if err != nil {
		panic(err)
	}
	defer result.Release()

	fmt.Println(result)
}

// ExampleDataFrameJoin demonstrates join operations between DataFrames.
func ExampleDataFrameJoin() {
	mem := memory.NewGoAllocator()

	// Create employees DataFrame
	empIds := NewSeries("id", []int64{1, 2, 3}, mem)
	empNames := NewSeries("name", []string{"Alice", "Bob", "Charlie"}, mem)
	defer empIds.Release()
	defer empNames.Release()
	employees := NewDataFrame(empIds, empNames)
	defer employees.Release()

	// Create departments DataFrame
	deptIds := NewSeries("id", []int64{1, 2, 3}, mem)
	deptNames := NewSeries("department", []string{"Engineering", "Sales", "Marketing"}, mem)
	defer deptIds.Release()
	defer deptNames.Release()
	departments := NewDataFrame(deptIds, deptNames)
	defer departments.Release()

	// Perform inner join
	result, err := employees.Join(departments, &JoinOptions{
		Type:     InnerJoin,
		LeftKey:  "id",
		RightKey: "id",
	})
	if err != nil {
		panic(err)
	}
	defer result.Release()

	fmt.Println(result)
}

// SQLExecutor provides SQL query execution capabilities for DataFrames.
//
// SQLExecutor allows you to execute SQL queries against registered DataFrames,
// providing a familiar SQL interface for data analysis and manipulation. It supports
// standard SQL operations including SELECT, WHERE, GROUP BY, ORDER BY, and LIMIT.
//
// Key features:
//   - Standard SQL syntax support (SELECT, FROM, WHERE, GROUP BY, etc.)
//   - Integration with existing DataFrame operations and optimizations
//   - Query validation and error reporting
//   - Prepared statement support for query reuse
//   - EXPLAIN functionality for query plan analysis
//
// Memory management: SQLExecutor manages memory automatically, but result DataFrames
// must still be released by the caller:
//
//	executor := gorilla.NewSQLExecutor(mem)
//	result, err := executor.Execute("SELECT name FROM employees WHERE age > 30")
//	defer result.Release()
//
// Example:
//
//	mem := memory.NewGoAllocator()
//	executor := gorilla.NewSQLExecutor(mem)
//
//	// Register DataFrames as tables
//	executor.RegisterTable("employees", employeesDF)
//	executor.RegisterTable("departments", departmentsDF)
//
//	// Execute SQL queries
//	result, err := executor.Execute(`
//		SELECT e.name, d.department_name, e.salary
//		FROM employees e
//		JOIN departments d ON e.dept_id = d.id
//		WHERE e.salary > 50000
//		ORDER BY e.salary DESC
//		LIMIT 10
//	`)
//	if err != nil {
//		panic(err)
//	}
//	defer result.Release()
type SQLExecutor struct {
	executor *sql.SQLExecutor
}

// NewSQLExecutor creates a new SQL executor for DataFrame queries.
//
// The executor allows you to register DataFrames as tables and execute SQL queries
// against them. All SQL operations are translated to efficient DataFrame operations
// and benefit from the same optimizations as direct DataFrame API usage.
//
// Parameters:
//   - mem: Apache Arrow memory allocator for managing query results
//
// Example:
//
//	mem := memory.NewGoAllocator()
//	executor := gorilla.NewSQLExecutor(mem)
//
//	// Register a DataFrame as a table
//	executor.RegisterTable("sales", salesDF)
//
//	// Execute SQL queries
//	result, err := executor.Execute("SELECT * FROM sales WHERE amount > 1000")
//	defer result.Release()
func NewSQLExecutor(mem memory.Allocator) *SQLExecutor {
	return &SQLExecutor{
		executor: sql.NewSQLExecutor(mem),
	}
}

// RegisterTable registers a DataFrame with a table name for SQL queries.
//
// Once registered, the DataFrame can be referenced by the table name in SQL queries.
// Multiple DataFrames can be registered to support JOIN operations and complex queries.
//
// Parameters:
//   - name: Table name to use in SQL queries (case-sensitive)
//   - df: DataFrame to register as a table
//
// Example:
//
//	executor.RegisterTable("employees", employeesDF)
//	executor.RegisterTable("departments", departmentsDF)
//
//	// Now can use in SQL
//	result, err := executor.Execute("SELECT * FROM employees WHERE dept_id IN (SELECT id FROM departments)")
func (se *SQLExecutor) RegisterTable(name string, df *DataFrame) {
	se.executor.RegisterTable(name, df.df)
}

// Execute executes a SQL query and returns the result DataFrame.
//
// Supports standard SQL SELECT syntax including:
//   - Column selection: SELECT col1, col2, *
//   - Computed columns: SELECT col1, col2 * 2 AS doubled
//   - Filtering: WHERE conditions with AND, OR, comparison operators
//   - Aggregation: GROUP BY with SUM, COUNT, AVG, MIN, MAX functions
//   - Sorting: ORDER BY with ASC/DESC
//   - Limiting: LIMIT and OFFSET clauses
//   - String functions: UPPER, LOWER, LENGTH
//   - Math functions: ABS, ROUND
//   - Conditional logic: CASE WHEN expressions
//
// Parameters:
//   - query: SQL query string to execute
//
// Returns:
//   - DataFrame containing query results (must be released by caller)
//   - Error if query parsing, validation, or execution fails
//
// Example:
//
//	result, err := executor.Execute(`
//		SELECT department, AVG(salary) as avg_salary, COUNT(*) as employee_count
//		FROM employees
//		WHERE active = true
//		GROUP BY department
//		HAVING AVG(salary) > 50000
//		ORDER BY avg_salary DESC
//		LIMIT 5
//	`)
//	if err != nil {
//		return err
//	}
//	defer result.Release()
func (se *SQLExecutor) Execute(query string) (*DataFrame, error) {
	result, err := se.executor.Execute(query)
	if err != nil {
		return nil, err
	}
	return &DataFrame{df: result}, nil
}

// ValidateQuery validates a SQL query without executing it.
//
// Performs syntax checking, table reference validation, and semantic analysis
// to ensure the query is valid before execution. Useful for query validation
// in applications or prepared statement creation.
//
// Parameters:
//   - query: SQL query string to validate
//
// Returns:
//   - nil if query is valid
//   - Error describing validation issues
//
// Example:
//
//	if err := executor.ValidateQuery("SELECT * FROM employees WHERE age >"); err != nil {
//		fmt.Printf("Query validation failed: %v\n", err)
//	}
func (se *SQLExecutor) ValidateQuery(query string) error {
	return se.executor.ValidateQuery(query)
}

// Explain returns the execution plan for a SQL query.
//
// Shows how the SQL query will be translated to DataFrame operations,
// including optimization steps, operation order, and estimated performance
// characteristics. Useful for query optimization and debugging.
//
// Parameters:
//   - query: SQL query string to explain
//
// Returns:
//   - String representation of the execution plan
//   - Error if query parsing or translation fails
//
// Example:
//
//	plan, err := executor.Explain("SELECT name FROM employees WHERE salary > 50000")
//	if err != nil {
//		return err
//	}
//	fmt.Println("Execution Plan:")
//	fmt.Println(plan)
func (se *SQLExecutor) Explain(query string) (string, error) {
	return se.executor.Explain(query)
}

// GetRegisteredTables returns the list of registered table names.
//
// Useful for introspection and debugging to see which tables are available
// for SQL queries.
//
// Returns:
//   - Slice of table names that can be used in SQL queries
//
// Example:
//
//	tables := executor.GetRegisteredTables()
//	fmt.Printf("Available tables: %v\n", tables)
func (se *SQLExecutor) GetRegisteredTables() []string {
	return se.executor.GetRegisteredTables()
}

// ClearTables removes all registered tables.
//
// Useful for cleaning up or resetting the executor state. After calling this,
// all previously registered tables will need to be re-registered before use.
//
// Example:
//
//	executor.ClearTables()
//	// Need to re-register tables before executing queries
func (se *SQLExecutor) ClearTables() {
	se.executor.ClearTables()
}

// BatchExecute executes multiple SQL statements in sequence.
//
// Executes queries in order and returns all results. If any query fails,
// all previously successful results are cleaned up and an error is returned.
//
// Parameters:
//   - queries: Slice of SQL query strings to execute
//
// Returns:
//   - Slice of DataFrames containing results (all must be released by caller)
//   - Error if any query fails
//
// Example:
//
//	queries := []string{
//		"SELECT * FROM employees WHERE active = true",
//		"SELECT department, COUNT(*) FROM employees GROUP BY department",
//		"SELECT AVG(salary) FROM employees",
//	}
//	results, err := executor.BatchExecute(queries)
//	if err != nil {
//		return err
//	}
//	defer func() {
//		for _, result := range results {
//			result.Release()
//		}
//	}()
func (se *SQLExecutor) BatchExecute(queries []string) ([]*DataFrame, error) {
	results, err := se.executor.BatchExecute(queries)
	if err != nil {
		return nil, err
	}

	gorillaDFs := make([]*DataFrame, len(results))
	for i, result := range results {
		gorillaDFs[i] = &DataFrame{df: result}
	}

	return gorillaDFs, nil
}

// ExampleSQLExecutor demonstrates SQL query execution with DataFrames.
func ExampleSQLExecutor() {
	mem := memory.NewGoAllocator()

	// Create sample data
	names := NewSeries("name", []string{"Alice", "Bob", "Charlie", "David"}, mem)
	departments := NewSeries("department", []string{"Engineering", "Sales", "Engineering", "Marketing"}, mem)
	salaries := NewSeries("salary", []int64{100000, 80000, 120000, 75000}, mem)
	active := NewSeries("active", []bool{true, true, false, true}, mem)
	defer names.Release()
	defer departments.Release()
	defer salaries.Release()
	defer active.Release()

	employees := NewDataFrame(names, departments, salaries, active)
	defer employees.Release()

	// Create SQL executor and register table
	executor := NewSQLExecutor(mem)
	executor.RegisterTable("employees", employees)

	// Execute SQL queries
	result, err := executor.Execute(`
		SELECT department, AVG(salary) as avg_salary, COUNT(*) as employee_count
		FROM employees
		WHERE active = true
		GROUP BY department
		ORDER BY avg_salary DESC
	`)
	if err != nil {
		panic(err)
	}
	defer result.Release()

	fmt.Println(result)
}

// ExampleDataFrame_LazyEvaluation demonstrates lazy evaluation with optimization.
//
// This example shows how operations are deferred and optimized when using
// lazy evaluation, including automatic parallelization and query optimization.
func ExampleDataFrameLazyEvaluation() {
	mem := memory.NewGoAllocator()

	// Create sample employee data
	employees := NewSeries("employee", []string{"Alice", "Bob", "Charlie", "Diana", "Eve"}, mem)
	departments := NewSeries("department", []string{"Engineering", "Sales", "Engineering", "Marketing", "Sales"}, mem)
	salaries := NewSeries("salary", []int64{75000, 65000, 80000, 70000, 68000}, mem)
	ages := NewSeries("age", []int64{28, 35, 32, 29, 31}, mem)

	df := NewDataFrame(employees, departments, salaries, ages)
	defer df.Release()

	// Chain multiple operations - all deferred until Collect()
	const ageThreshold = 30
	result, err := df.Lazy().
		Filter(Col("age").Gt(Lit(int64(ageThreshold)))). // Filter for employees over threshold
		Select("employee", "department", "salary").
		Collect()

	if err != nil {
		panic(err)
	}
	defer result.Release()

	fmt.Printf("Employees over 30: %d\n", result.Len())
	// Output: Employees over 30: 3
}

// ExampleDataFrame_MemoryManagement demonstrates proper memory management patterns.
//
// This example shows the recommended patterns for memory management including
// defer usage, cleanup in error conditions, and resource lifecycle management.
func ExampleDataFrameMemoryManagement() {
	mem := memory.NewGoAllocator()

	// Create function that demonstrates proper cleanup
	processData := func() error {
		// Create series with immediate defer cleanup
		ids := NewSeries("id", []int64{1, 2, 3, 4}, mem)
		defer ids.Release() // Always defer cleanup immediately

		names := NewSeries("name", []string{"Alice", "Bob", "Charlie", "Diana"}, mem)
		defer names.Release()

		// Create DataFrame with defer cleanup
		df := NewDataFrame(ids, names)
		defer df.Release() // Essential - cleans up DataFrame resources

		// Perform operations that might error
		const idThreshold = 2
		result, err := df.Lazy().
			Filter(Col("id").Gt(Lit(int64(idThreshold)))).
			Collect()
		if err != nil {
			return err // Deferred cleanups still execute on error return
		}
		defer result.Release() // Clean up result DataFrame

		fmt.Printf("Processed %d rows\n", result.Len())
		return nil
	}

	if err := processData(); err != nil {
		panic(err)
	}
	// All resources automatically cleaned up via deferred calls
	// Output: Processed 2 rows
}
