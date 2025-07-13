// Package expr provides expression evaluation for DataFrame operations
package expr

import (
	"fmt"
)

// ExprType represents the type of expression
type ExprType int

const (
	ExprColumn ExprType = iota
	ExprLiteral
	ExprBinary
	ExprUnary
	ExprFunction
	ExprAggregation
	ExprCase
)

// Expr represents an expression that can be evaluated lazily
type Expr interface {
	Type() ExprType
	String() string
}

// ColumnExpr represents a column reference
type ColumnExpr struct {
	name string
}

func (c *ColumnExpr) Type() ExprType {
	return ExprColumn
}

func (c *ColumnExpr) String() string {
	return fmt.Sprintf("col(%s)", c.name)
}

func (c *ColumnExpr) Name() string {
	return c.name
}

// LiteralExpr represents a literal value
type LiteralExpr struct {
	value interface{}
}

func (l *LiteralExpr) Type() ExprType {
	return ExprLiteral
}

func (l *LiteralExpr) String() string {
	return fmt.Sprintf("lit(%v)", l.value)
}

func (l *LiteralExpr) Value() interface{} {
	return l.value
}

// BinaryOp represents binary operations
type BinaryOp int

const (
	OpAdd BinaryOp = iota
	OpSub
	OpMul
	OpDiv
	OpEq
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
	OpAnd
	OpOr
)

// BinaryExpr represents a binary operation
type BinaryExpr struct {
	left  Expr
	op    BinaryOp
	right Expr
}

func (b *BinaryExpr) Type() ExprType {
	return ExprBinary
}

func (b *BinaryExpr) String() string {
	var opStr string
	switch b.op {
	case OpAdd:
		opStr = "+"
	case OpSub:
		opStr = "-"
	case OpMul:
		opStr = "*"
	case OpDiv:
		opStr = "/"
	case OpEq:
		opStr = "=="
	case OpNe:
		opStr = "!="
	case OpLt:
		opStr = "<"
	case OpLe:
		opStr = "<="
	case OpGt:
		opStr = ">"
	case OpGe:
		opStr = ">="
	case OpAnd:
		opStr = "&&"
	case OpOr:
		opStr = "||"
	}
	return fmt.Sprintf("(%s %s %s)", b.left.String(), opStr, b.right.String())
}

func (b *BinaryExpr) Left() Expr {
	return b.left
}

func (b *BinaryExpr) Op() BinaryOp {
	return b.op
}

func (b *BinaryExpr) Right() Expr {
	return b.right
}

// UnaryOp represents unary operations
type UnaryOp int

const (
	UnaryNeg UnaryOp = iota
	UnaryNot
)

// UnaryExpr represents a unary operation
type UnaryExpr struct {
	op      UnaryOp
	operand Expr
}

func (u *UnaryExpr) Type() ExprType {
	return ExprUnary
}

func (u *UnaryExpr) String() string {
	var opStr string
	switch u.op {
	case UnaryNeg:
		opStr = "-"
	case UnaryNot:
		opStr = "!"
	}
	return fmt.Sprintf("(%s%s)", opStr, u.operand.String())
}

func (u *UnaryExpr) Op() UnaryOp {
	return u.op
}

func (u *UnaryExpr) Operand() Expr {
	return u.operand
}

// Constructor functions

// Col creates a column expression
func Col(name string) *ColumnExpr {
	return &ColumnExpr{name: name}
}

// Lit creates a literal expression
func Lit(value interface{}) *LiteralExpr {
	return &LiteralExpr{value: value}
}

// Binary operations on column expressions

// Add creates an addition expression
func (c *ColumnExpr) Add(other Expr) *BinaryExpr {
	return &BinaryExpr{left: c, op: OpAdd, right: other}
}

// Sub creates a subtraction expression
func (c *ColumnExpr) Sub(other Expr) *BinaryExpr {
	return &BinaryExpr{left: c, op: OpSub, right: other}
}

// Mul creates a multiplication expression
func (c *ColumnExpr) Mul(other Expr) *BinaryExpr {
	return &BinaryExpr{left: c, op: OpMul, right: other}
}

// Div creates a division expression
func (c *ColumnExpr) Div(other Expr) *BinaryExpr {
	return &BinaryExpr{left: c, op: OpDiv, right: other}
}

// Eq creates an equality expression
func (c *ColumnExpr) Eq(other Expr) *BinaryExpr {
	return &BinaryExpr{left: c, op: OpEq, right: other}
}

// Ne creates a not-equal expression
func (c *ColumnExpr) Ne(other Expr) *BinaryExpr {
	return &BinaryExpr{left: c, op: OpNe, right: other}
}

// Lt creates a less-than expression
func (c *ColumnExpr) Lt(other Expr) *BinaryExpr {
	return &BinaryExpr{left: c, op: OpLt, right: other}
}

// Le creates a less-than-or-equal expression
func (c *ColumnExpr) Le(other Expr) *BinaryExpr {
	return &BinaryExpr{left: c, op: OpLe, right: other}
}

// Gt creates a greater-than expression
func (c *ColumnExpr) Gt(other Expr) *BinaryExpr {
	return &BinaryExpr{left: c, op: OpGt, right: other}
}

// Ge creates a greater-than-or-equal expression
func (c *ColumnExpr) Ge(other Expr) *BinaryExpr {
	return &BinaryExpr{left: c, op: OpGe, right: other}
}

// Binary operations on binary expressions (for chaining)

// Add creates an addition expression
func (b *BinaryExpr) Add(other Expr) *BinaryExpr {
	return &BinaryExpr{left: b, op: OpAdd, right: other}
}

// Sub creates a subtraction expression
func (b *BinaryExpr) Sub(other Expr) *BinaryExpr {
	return &BinaryExpr{left: b, op: OpSub, right: other}
}

// Mul creates a multiplication expression
func (b *BinaryExpr) Mul(other Expr) *BinaryExpr {
	return &BinaryExpr{left: b, op: OpMul, right: other}
}

// Div creates a division expression
func (b *BinaryExpr) Div(other Expr) *BinaryExpr {
	return &BinaryExpr{left: b, op: OpDiv, right: other}
}

// And creates a logical AND expression
func (b *BinaryExpr) And(other Expr) *BinaryExpr {
	return &BinaryExpr{left: b, op: OpAnd, right: other}
}

// Or creates a logical OR expression
func (b *BinaryExpr) Or(other Expr) *BinaryExpr {
	return &BinaryExpr{left: b, op: OpOr, right: other}
}

// Math function methods for BinaryExpr

// Abs creates an absolute value function expression
func (b *BinaryExpr) Abs() *FunctionExpr {
	return &FunctionExpr{name: "abs", args: []Expr{b}}
}

// Round creates a round function expression
func (b *BinaryExpr) Round() *FunctionExpr {
	return &FunctionExpr{name: "round", args: []Expr{b}}
}

// RoundTo creates a round function expression with precision
func (b *BinaryExpr) RoundTo(precision Expr) *FunctionExpr {
	return &FunctionExpr{name: "round", args: []Expr{b, precision}}
}

// Floor creates a floor function expression
func (b *BinaryExpr) Floor() *FunctionExpr {
	return &FunctionExpr{name: "floor", args: []Expr{b}}
}

// Ceil creates a ceil function expression
func (b *BinaryExpr) Ceil() *FunctionExpr {
	return &FunctionExpr{name: "ceil", args: []Expr{b}}
}

// Sqrt creates a square root function expression
func (b *BinaryExpr) Sqrt() *FunctionExpr {
	return &FunctionExpr{name: "sqrt", args: []Expr{b}}
}

// Log creates a natural logarithm function expression
func (b *BinaryExpr) Log() *FunctionExpr {
	return &FunctionExpr{name: "log", args: []Expr{b}}
}

// Sin creates a sine function expression
func (b *BinaryExpr) Sin() *FunctionExpr {
	return &FunctionExpr{name: "sin", args: []Expr{b}}
}

// Cos creates a cosine function expression
func (b *BinaryExpr) Cos() *FunctionExpr {
	return &FunctionExpr{name: "cos", args: []Expr{b}}
}

// AggregationType represents the type of aggregation function
type AggregationType int

const (
	AggSum AggregationType = iota
	AggCount
	AggMean
	AggMin
	AggMax
)

// FunctionExpr represents a function call expression
type FunctionExpr struct {
	name string
	args []Expr
}

func (f *FunctionExpr) Type() ExprType {
	return ExprFunction
}

func (f *FunctionExpr) String() string {
	if len(f.args) == 0 {
		return fmt.Sprintf("%s()", f.name)
	}

	argStrs := make([]string, len(f.args))
	for i, arg := range f.args {
		argStrs[i] = arg.String()
	}

	result := f.name + "("
	for i, argStr := range argStrs {
		if i > 0 {
			result += ", "
		}
		result += argStr
	}
	result += ")"
	return result
}

func (f *FunctionExpr) Name() string {
	return f.name
}

func (f *FunctionExpr) Args() []Expr {
	return f.args
}

// AggregationExpr represents an aggregation function over a column
type AggregationExpr struct {
	column  Expr
	aggType AggregationType
	alias   string
}

func (a *AggregationExpr) Type() ExprType {
	return ExprAggregation
}

func (a *AggregationExpr) String() string {
	var aggName string
	switch a.aggType {
	case AggSum:
		aggName = "sum"
	case AggCount:
		aggName = "count"
	case AggMean:
		aggName = "mean"
	case AggMin:
		aggName = "min"
	case AggMax:
		aggName = "max"
	}
	return fmt.Sprintf("%s(%s)", aggName, a.column.String())
}

func (a *AggregationExpr) Column() Expr {
	return a.column
}

func (a *AggregationExpr) AggType() AggregationType {
	return a.aggType
}

func (a *AggregationExpr) Alias() string {
	return a.alias
}

// CaseWhen represents a condition and value pair in CASE expression
type CaseWhen struct {
	condition Expr
	value     Expr
}

// CaseExpr represents a CASE expression with multiple WHEN clauses
type CaseExpr struct {
	whens     []CaseWhen
	elseValue Expr
}

func (c *CaseExpr) Type() ExprType {
	return ExprCase
}

func (c *CaseExpr) String() string {
	result := "case"
	for _, when := range c.whens {
		result += fmt.Sprintf(" when %s then %s", when.condition.String(), when.value.String())
	}
	if c.elseValue != nil {
		result += fmt.Sprintf(" else %s", c.elseValue.String())
	}
	result += " end"
	return result
}

func (c *CaseExpr) Whens() []CaseWhen {
	return c.whens
}

func (c *CaseExpr) ElseValue() Expr {
	return c.elseValue
}

// When adds a condition-value pair to the case expression
func (c *CaseExpr) When(condition, value Expr) *CaseExpr {
	newWhens := make([]CaseWhen, len(c.whens)+1)
	copy(newWhens, c.whens)
	newWhens[len(c.whens)] = CaseWhen{condition: condition, value: value}

	return &CaseExpr{
		whens:     newWhens,
		elseValue: c.elseValue,
	}
}

// Else sets the default value for the case expression
func (c *CaseExpr) Else(value Expr) *CaseExpr {
	return &CaseExpr{
		whens:     c.whens,
		elseValue: value,
	}
}

// Aggregation constructor functions

// Sum creates a sum aggregation expression
func Sum(column Expr) *AggregationExpr {
	return &AggregationExpr{column: column, aggType: AggSum}
}

// Count creates a count aggregation expression
func Count(column Expr) *AggregationExpr {
	return &AggregationExpr{column: column, aggType: AggCount}
}

// Mean creates a mean aggregation expression
func Mean(column Expr) *AggregationExpr {
	return &AggregationExpr{column: column, aggType: AggMean}
}

// Min creates a min aggregation expression
func Min(column Expr) *AggregationExpr {
	return &AggregationExpr{column: column, aggType: AggMin}
}

// Max creates a max aggregation expression
func Max(column Expr) *AggregationExpr {
	return &AggregationExpr{column: column, aggType: AggMax}
}

// Aggregation methods on column expressions

// Sum creates a sum aggregation of this column
func (c *ColumnExpr) Sum() *AggregationExpr {
	return Sum(c)
}

// Count creates a count aggregation of this column
func (c *ColumnExpr) Count() *AggregationExpr {
	return Count(c)
}

// Mean creates a mean aggregation of this column
func (c *ColumnExpr) Mean() *AggregationExpr {
	return Mean(c)
}

// Min creates a min aggregation of this column
func (c *ColumnExpr) Min() *AggregationExpr {
	return Min(c)
}

// Max creates a max aggregation of this column
func (c *ColumnExpr) Max() *AggregationExpr {
	return Max(c)
}

// As sets an alias for the aggregation expression
func (a *AggregationExpr) As(alias string) *AggregationExpr {
	return &AggregationExpr{
		column:  a.column,
		aggType: a.aggType,
		alias:   alias,
	}
}

// Enhanced Expression System - Unary Operations

// Neg creates a negation (unary minus) expression
func (c *ColumnExpr) Neg() *UnaryExpr {
	return &UnaryExpr{op: UnaryNeg, operand: c}
}

// Not creates a logical NOT expression
func (c *ColumnExpr) Not() *UnaryExpr {
	return &UnaryExpr{op: UnaryNot, operand: c}
}

// Neg creates a negation expression for function expressions
func (f *FunctionExpr) Neg() *UnaryExpr {
	return &UnaryExpr{op: UnaryNeg, operand: f}
}

func (f *FunctionExpr) Not() *UnaryExpr {
	return &UnaryExpr{op: UnaryNot, operand: f}
}

// Math Functions

// Abs creates an absolute value function expression
func (c *ColumnExpr) Abs() *FunctionExpr {
	return &FunctionExpr{name: "abs", args: []Expr{c}}
}

// Round creates a round function expression
func (c *ColumnExpr) Round() *FunctionExpr {
	return &FunctionExpr{name: "round", args: []Expr{c}}
}

// RoundTo creates a round function expression with precision
func (c *ColumnExpr) RoundTo(precision Expr) *FunctionExpr {
	return &FunctionExpr{name: "round", args: []Expr{c, precision}}
}

// Floor creates a floor function expression
func (c *ColumnExpr) Floor() *FunctionExpr {
	return &FunctionExpr{name: "floor", args: []Expr{c}}
}

// Ceil creates a ceil function expression
func (c *ColumnExpr) Ceil() *FunctionExpr {
	return &FunctionExpr{name: "ceil", args: []Expr{c}}
}

// Sqrt creates a square root function expression
func (c *ColumnExpr) Sqrt() *FunctionExpr {
	return &FunctionExpr{name: "sqrt", args: []Expr{c}}
}

// Log creates a natural logarithm function expression
func (c *ColumnExpr) Log() *FunctionExpr {
	return &FunctionExpr{name: "log", args: []Expr{c}}
}

// Sin creates a sine function expression
func (c *ColumnExpr) Sin() *FunctionExpr {
	return &FunctionExpr{name: "sin", args: []Expr{c}}
}

// Cos creates a cosine function expression
func (c *ColumnExpr) Cos() *FunctionExpr {
	return &FunctionExpr{name: "cos", args: []Expr{c}}
}

// Abs creates an absolute value function expression
func (f *FunctionExpr) Abs() *FunctionExpr {
	return &FunctionExpr{name: "abs", args: []Expr{f}}
}

func (f *FunctionExpr) Round() *FunctionExpr {
	return &FunctionExpr{name: "round", args: []Expr{f}}
}

func (f *FunctionExpr) RoundTo(precision Expr) *FunctionExpr {
	return &FunctionExpr{name: "round", args: []Expr{f, precision}}
}

func (f *FunctionExpr) Floor() *FunctionExpr {
	return &FunctionExpr{name: "floor", args: []Expr{f}}
}

func (f *FunctionExpr) Ceil() *FunctionExpr {
	return &FunctionExpr{name: "ceil", args: []Expr{f}}
}

func (f *FunctionExpr) Sqrt() *FunctionExpr {
	return &FunctionExpr{name: "sqrt", args: []Expr{f}}
}

func (f *FunctionExpr) Log() *FunctionExpr {
	return &FunctionExpr{name: "log", args: []Expr{f}}
}

func (f *FunctionExpr) Sin() *FunctionExpr {
	return &FunctionExpr{name: "sin", args: []Expr{f}}
}

func (f *FunctionExpr) Cos() *FunctionExpr {
	return &FunctionExpr{name: "cos", args: []Expr{f}}
}

// String Functions

// Upper creates an UPPER function expression
func (c *ColumnExpr) Upper() *FunctionExpr {
	return &FunctionExpr{name: "upper", args: []Expr{c}}
}

// Lower creates a LOWER function expression
func (c *ColumnExpr) Lower() *FunctionExpr {
	return &FunctionExpr{name: "lower", args: []Expr{c}}
}

// Length creates a LENGTH function expression
func (c *ColumnExpr) Length() *FunctionExpr {
	return &FunctionExpr{name: "length", args: []Expr{c}}
}

// Trim creates a TRIM function expression
func (c *ColumnExpr) Trim() *FunctionExpr {
	return &FunctionExpr{name: "trim", args: []Expr{c}}
}

// Substring creates a SUBSTRING function expression
func (c *ColumnExpr) Substring(start, length Expr) *FunctionExpr {
	return &FunctionExpr{name: "substring", args: []Expr{c, start, length}}
}

// Upper creates an uppercase function expression
func (f *FunctionExpr) Upper() *FunctionExpr {
	return &FunctionExpr{name: "upper", args: []Expr{f}}
}

func (f *FunctionExpr) Lower() *FunctionExpr {
	return &FunctionExpr{name: "lower", args: []Expr{f}}
}

func (f *FunctionExpr) Length() *FunctionExpr {
	return &FunctionExpr{name: "length", args: []Expr{f}}
}

func (f *FunctionExpr) Trim() *FunctionExpr {
	return &FunctionExpr{name: "trim", args: []Expr{f}}
}

func (f *FunctionExpr) Substring(start, length Expr) *FunctionExpr {
	return &FunctionExpr{name: "substring", args: []Expr{f, start, length}}
}

// Type Casting Functions

// CastToString creates a cast to string function expression
func (c *ColumnExpr) CastToString() *FunctionExpr {
	return &FunctionExpr{name: "cast_string", args: []Expr{c}}
}

// CastToInt64 creates a cast to int64 function expression
func (c *ColumnExpr) CastToInt64() *FunctionExpr {
	return &FunctionExpr{name: "cast_int64", args: []Expr{c}}
}

// CastToFloat64 creates a cast to float64 function expression
func (c *ColumnExpr) CastToFloat64() *FunctionExpr {
	return &FunctionExpr{name: "cast_float64", args: []Expr{c}}
}

// CastToBool creates a cast to bool function expression
func (c *ColumnExpr) CastToBool() *FunctionExpr {
	return &FunctionExpr{name: "cast_bool", args: []Expr{c}}
}

// CastToString creates a string casting function expression
func (f *FunctionExpr) CastToString() *FunctionExpr {
	return &FunctionExpr{name: "cast_string", args: []Expr{f}}
}

func (f *FunctionExpr) CastToInt64() *FunctionExpr {
	return &FunctionExpr{name: "cast_int64", args: []Expr{f}}
}

func (f *FunctionExpr) CastToFloat64() *FunctionExpr {
	return &FunctionExpr{name: "cast_float64", args: []Expr{f}}
}

func (f *FunctionExpr) CastToBool() *FunctionExpr {
	return &FunctionExpr{name: "cast_bool", args: []Expr{f}}
}

// Add creates an addition expression for function expressions
func (f *FunctionExpr) Add(other Expr) *BinaryExpr {
	return &BinaryExpr{left: f, op: OpAdd, right: other}
}

func (f *FunctionExpr) Sub(other Expr) *BinaryExpr {
	return &BinaryExpr{left: f, op: OpSub, right: other}
}

func (f *FunctionExpr) Mul(other Expr) *BinaryExpr {
	return &BinaryExpr{left: f, op: OpMul, right: other}
}

func (f *FunctionExpr) Div(other Expr) *BinaryExpr {
	return &BinaryExpr{left: f, op: OpDiv, right: other}
}

func (f *FunctionExpr) Eq(other Expr) *BinaryExpr {
	return &BinaryExpr{left: f, op: OpEq, right: other}
}

func (f *FunctionExpr) Ne(other Expr) *BinaryExpr {
	return &BinaryExpr{left: f, op: OpNe, right: other}
}

func (f *FunctionExpr) Lt(other Expr) *BinaryExpr {
	return &BinaryExpr{left: f, op: OpLt, right: other}
}

func (f *FunctionExpr) Le(other Expr) *BinaryExpr {
	return &BinaryExpr{left: f, op: OpLe, right: other}
}

func (f *FunctionExpr) Gt(other Expr) *BinaryExpr {
	return &BinaryExpr{left: f, op: OpGt, right: other}
}

func (f *FunctionExpr) Ge(other Expr) *BinaryExpr {
	return &BinaryExpr{left: f, op: OpGe, right: other}
}

func (f *FunctionExpr) And(other Expr) *BinaryExpr {
	return &BinaryExpr{left: f, op: OpAnd, right: other}
}

func (f *FunctionExpr) Or(other Expr) *BinaryExpr {
	return &BinaryExpr{left: f, op: OpOr, right: other}
}

// Constructor Functions for Conditional Expressions

// If creates an IF function expression
func If(condition, thenValue, elseValue Expr) *FunctionExpr {
	return &FunctionExpr{name: "if", args: []Expr{condition, thenValue, elseValue}}
}

// Coalesce creates a COALESCE function expression
func Coalesce(exprs ...Expr) *FunctionExpr {
	return &FunctionExpr{name: "coalesce", args: exprs}
}

// Concat creates a CONCAT function expression
func Concat(exprs ...Expr) *FunctionExpr {
	return &FunctionExpr{name: "concat", args: exprs}
}

// Case creates a new CASE expression
func Case() *CaseExpr {
	return &CaseExpr{whens: make([]CaseWhen, 0), elseValue: nil}
}
