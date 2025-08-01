// Package expr provides expression evaluation for DataFrame operations
package expr

import (
	"fmt"

	"github.com/paveg/gorilla/internal/common"
)

// Type represents the type of expression.
type Type int

const (
	ExprColumn Type = iota
	ExprLiteral
	ExprBinary
	ExprUnary
	ExprFunction
	ExprAggregation
	ExprCase
	ExprInvalid
)

// EvaluationContext represents the context in which an expression is evaluated.
type EvaluationContext int

const (
	// RowContext evaluates expressions against raw row data (for WHERE clauses).
	RowContext EvaluationContext = iota

	// GroupContext evaluates expressions against aggregated group data (for HAVING clauses).
	GroupContext
)

func (ec EvaluationContext) String() string {
	switch ec {
	case RowContext, GroupContext:
		return common.FormatEvaluationContext(int(ec))
	default:
		return "UnknownContext"
	}
}

// Expr represents an expression that can be evaluated lazily.
type Expr interface {
	Type() Type
	String() string
}

// ContextualExpr represents expressions that can be evaluated in different contexts.
type ContextualExpr interface {
	Expr
	// SupportsContext returns whether this expression can be evaluated in the given context
	SupportsContext(ctx EvaluationContext) bool
}

// ColumnExpr represents a column reference.
type ColumnExpr struct {
	name string
}

func (c *ColumnExpr) Type() Type {
	return ExprColumn
}

func (c *ColumnExpr) String() string {
	return common.FormatFunction("col", c.name)
}

func (c *ColumnExpr) Name() string {
	return c.name
}

// SupportsContext returns whether this expression can be evaluated in the given context
// ColumnExpr can be evaluated in both contexts (but meaning differs).
func (c *ColumnExpr) SupportsContext(_ EvaluationContext) bool {
	return true // Columns exist in both raw and aggregated data
}

// LiteralExpr represents a literal value.
type LiteralExpr struct {
	value interface{}
}

func (l *LiteralExpr) Type() Type {
	return ExprLiteral
}

func (l *LiteralExpr) String() string {
	return common.FormatFunction("lit", common.ToString(l.value))
}

func (l *LiteralExpr) Value() interface{} {
	return l.value
}

// SupportsContext returns whether this expression can be evaluated in the given context
// LiteralExpr can be evaluated in any context.
func (l *LiteralExpr) SupportsContext(_ EvaluationContext) bool {
	return true
}

// BinaryOp represents binary operations.
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

// BinaryExpr represents a binary operation.
type BinaryExpr struct {
	left  Expr
	op    BinaryOp
	right Expr
}

// NewBinaryExpr creates a new binary expression.
func NewBinaryExpr(left Expr, op BinaryOp, right Expr) *BinaryExpr {
	return &BinaryExpr{left: left, op: op, right: right}
}

func (b *BinaryExpr) Type() Type {
	return ExprBinary
}

func (b *BinaryExpr) String() string {
	opStr := common.FormatBinaryOperator(int(b.op))
	return common.FormatBinaryOperation(b.left.String(), opStr, b.right.String())
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

// SupportsContext returns whether this expression can be evaluated in the given context
// BinaryExpr context support depends on operands.
func (b *BinaryExpr) SupportsContext(ctx EvaluationContext) bool {
	if leftContextual, ok := b.left.(ContextualExpr); ok {
		if !leftContextual.SupportsContext(ctx) {
			return false
		}
	}
	if rightContextual, ok := b.right.(ContextualExpr); ok {
		if !rightContextual.SupportsContext(ctx) {
			return false
		}
	}
	return true
}

// UnaryOp represents unary operations.
type UnaryOp int

const (
	UnaryNeg UnaryOp = iota
	UnaryNot
)

// UnaryExpr represents a unary operation.
type UnaryExpr struct {
	op      UnaryOp
	operand Expr
}

func (u *UnaryExpr) Type() Type {
	return ExprUnary
}

func (u *UnaryExpr) String() string {
	opStr := common.FormatUnaryOperator(int(u.op))
	return fmt.Sprintf("(%s%s)", opStr, u.operand.String())
}

func (u *UnaryExpr) Op() UnaryOp {
	return u.op
}

func (u *UnaryExpr) Operand() Expr {
	return u.operand
}

// SupportsContext returns whether this expression can be evaluated in the given context
// UnaryExpr context support depends on its operand.
func (u *UnaryExpr) SupportsContext(ctx EvaluationContext) bool {
	if operandContextual, ok := u.operand.(ContextualExpr); ok {
		return operandContextual.SupportsContext(ctx)
	}
	return true
}

// InvalidExpr represents an invalid expression with an error message.
type InvalidExpr struct {
	message string
}

func (i *InvalidExpr) Type() Type {
	return ExprInvalid
}

func (i *InvalidExpr) String() string {
	return common.FormatFunction("invalid", i.message)
}

func (i *InvalidExpr) Message() string {
	return i.message
}

// SupportsContext returns whether this expression can be evaluated in the given context
// InvalidExpr cannot be evaluated in any context.
func (i *InvalidExpr) SupportsContext(_ EvaluationContext) bool {
	return false
}

// Constructor functions

// Col creates a column expression.
func Col(name string) *ColumnExpr {
	return &ColumnExpr{name: name}
}

// Lit creates a literal expression.
func Lit(value interface{}) *LiteralExpr {
	return &LiteralExpr{value: value}
}

// Invalid creates an invalid expression with an error message.
func Invalid(message string) *InvalidExpr {
	return &InvalidExpr{message: message}
}

// Binary operations on column expressions

// Add creates an addition expression.
func (c *ColumnExpr) Add(other Expr) *BinaryExpr {
	return &BinaryExpr{left: c, op: OpAdd, right: other}
}

// Sub creates a subtraction expression.
func (c *ColumnExpr) Sub(other Expr) *BinaryExpr {
	return &BinaryExpr{left: c, op: OpSub, right: other}
}

// Mul creates a multiplication expression.
func (c *ColumnExpr) Mul(other Expr) *BinaryExpr {
	return &BinaryExpr{left: c, op: OpMul, right: other}
}

// Div creates a division expression.
func (c *ColumnExpr) Div(other Expr) *BinaryExpr {
	return &BinaryExpr{left: c, op: OpDiv, right: other}
}

// Eq creates an equality expression.
func (c *ColumnExpr) Eq(other Expr) *BinaryExpr {
	return &BinaryExpr{left: c, op: OpEq, right: other}
}

// Ne creates a not-equal expression.
func (c *ColumnExpr) Ne(other Expr) *BinaryExpr {
	return &BinaryExpr{left: c, op: OpNe, right: other}
}

// Lt creates a less-than expression.
func (c *ColumnExpr) Lt(other Expr) *BinaryExpr {
	return &BinaryExpr{left: c, op: OpLt, right: other}
}

// Le creates a less-than-or-equal expression.
func (c *ColumnExpr) Le(other Expr) *BinaryExpr {
	return &BinaryExpr{left: c, op: OpLe, right: other}
}

// Gt creates a greater-than expression.
func (c *ColumnExpr) Gt(other Expr) *BinaryExpr {
	return &BinaryExpr{left: c, op: OpGt, right: other}
}

// Ge creates a greater-than-or-equal expression.
func (c *ColumnExpr) Ge(other Expr) *BinaryExpr {
	return &BinaryExpr{left: c, op: OpGe, right: other}
}

// Binary operations on binary expressions (for chaining)

// Add creates an addition expression.
func (b *BinaryExpr) Add(other Expr) *BinaryExpr {
	return &BinaryExpr{left: b, op: OpAdd, right: other}
}

// Sub creates a subtraction expression.
func (b *BinaryExpr) Sub(other Expr) *BinaryExpr {
	return &BinaryExpr{left: b, op: OpSub, right: other}
}

// Mul creates a multiplication expression.
func (b *BinaryExpr) Mul(other Expr) *BinaryExpr {
	return &BinaryExpr{left: b, op: OpMul, right: other}
}

// Div creates a division expression.
func (b *BinaryExpr) Div(other Expr) *BinaryExpr {
	return &BinaryExpr{left: b, op: OpDiv, right: other}
}

// And creates a logical AND expression.
func (b *BinaryExpr) And(other Expr) *BinaryExpr {
	return &BinaryExpr{left: b, op: OpAnd, right: other}
}

// Or creates a logical OR expression.
func (b *BinaryExpr) Or(other Expr) *BinaryExpr {
	return &BinaryExpr{left: b, op: OpOr, right: other}
}

// Comparison operations on binary expressions (for chaining)

// Gt creates a greater-than expression.
func (b *BinaryExpr) Gt(other Expr) *BinaryExpr {
	return &BinaryExpr{left: b, op: OpGt, right: other}
}

// Lt creates a less-than expression.
func (b *BinaryExpr) Lt(other Expr) *BinaryExpr {
	return &BinaryExpr{left: b, op: OpLt, right: other}
}

// Eq creates an equality expression.
func (b *BinaryExpr) Eq(other Expr) *BinaryExpr {
	return &BinaryExpr{left: b, op: OpEq, right: other}
}

// Ne creates a not-equal expression.
func (b *BinaryExpr) Ne(other Expr) *BinaryExpr {
	return &BinaryExpr{left: b, op: OpNe, right: other}
}

// Ge creates a greater-than-or-equal expression.
func (b *BinaryExpr) Ge(other Expr) *BinaryExpr {
	return &BinaryExpr{left: b, op: OpGe, right: other}
}

// Le creates a less-than-or-equal expression.
func (b *BinaryExpr) Le(other Expr) *BinaryExpr {
	return &BinaryExpr{left: b, op: OpLe, right: other}
}

// Math function methods for BinaryExpr

// Abs creates an absolute value function expression.
func (b *BinaryExpr) Abs() *FunctionExpr {
	return &FunctionExpr{name: "abs", args: []Expr{b}}
}

// Round creates a round function expression.
func (b *BinaryExpr) Round() *FunctionExpr {
	return &FunctionExpr{name: "round", args: []Expr{b}}
}

// RoundTo creates a round function expression with precision.
func (b *BinaryExpr) RoundTo(precision Expr) *FunctionExpr {
	return &FunctionExpr{name: "round", args: []Expr{b, precision}}
}

// Floor creates a floor function expression.
func (b *BinaryExpr) Floor() *FunctionExpr {
	return &FunctionExpr{name: "floor", args: []Expr{b}}
}

// Ceil creates a ceil function expression.
func (b *BinaryExpr) Ceil() *FunctionExpr {
	return &FunctionExpr{name: "ceil", args: []Expr{b}}
}

// Sqrt creates a square root function expression.
func (b *BinaryExpr) Sqrt() *FunctionExpr {
	return &FunctionExpr{name: "sqrt", args: []Expr{b}}
}

// Log creates a natural logarithm function expression.
func (b *BinaryExpr) Log() *FunctionExpr {
	return &FunctionExpr{name: "log", args: []Expr{b}}
}

// Sin creates a sine function expression.
func (b *BinaryExpr) Sin() *FunctionExpr {
	return &FunctionExpr{name: "sin", args: []Expr{b}}
}

// Cos creates a cosine function expression.
func (b *BinaryExpr) Cos() *FunctionExpr {
	return &FunctionExpr{name: "cos", args: []Expr{b}}
}

// AggregationType represents the type of aggregation function.
type AggregationType int

const (
	AggSum AggregationType = iota
	AggCount
	AggMean
	AggMin
	AggMax
)

// Aggregation function name constants.
const (
	AggNameSum   = "sum"
	AggNameCount = "count"
	AggNameMean  = "mean"
	AggNameMin   = "min"
	AggNameMax   = "max"
)

// FunctionExpr represents a function call expression.
type FunctionExpr struct {
	name string
	args []Expr
}

// NewFunction creates a function expression.
func NewFunction(name string, args ...Expr) *FunctionExpr {
	return &FunctionExpr{name: name, args: args}
}

func (f *FunctionExpr) Type() Type {
	return ExprFunction
}

func (f *FunctionExpr) String() string {
	argStrs := make([]string, len(f.args))
	for i, arg := range f.args {
		argStrs[i] = arg.String()
	}
	return common.FormatFunction(f.name, argStrs...)
}

func (f *FunctionExpr) Name() string {
	return f.name
}

func (f *FunctionExpr) Args() []Expr {
	return f.args
}

// SupportsContext returns whether this expression can be evaluated in the given context
// FunctionExpr context support depends on its arguments.
func (f *FunctionExpr) SupportsContext(ctx EvaluationContext) bool {
	for _, arg := range f.args {
		if argContextual, ok := arg.(ContextualExpr); ok {
			if !argContextual.SupportsContext(ctx) {
				return false
			}
		}
	}
	return true
}

// AggregationExpr represents an aggregation function over a column.
type AggregationExpr struct {
	column  Expr
	aggType AggregationType
	alias   string
}

func (a *AggregationExpr) Type() Type {
	return ExprAggregation
}

func (a *AggregationExpr) String() string {
	aggName := common.FormatAggregationType(int(a.aggType))
	return common.FormatFunction(aggName, a.column.String())
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

// SupportsContext returns whether this expression can be evaluated in the given context
// AggregationExpr can only be evaluated in GroupContext.
func (a *AggregationExpr) SupportsContext(ctx EvaluationContext) bool {
	return ctx == GroupContext
}

// CaseWhen represents a condition and value pair in CASE expression.
type CaseWhen struct {
	condition Expr
	value     Expr
}

// CaseExpr represents a CASE expression with multiple WHEN clauses.
type CaseExpr struct {
	whens     []CaseWhen
	elseValue Expr
}

func (c *CaseExpr) Type() Type {
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

// SupportsContext returns whether this expression can be evaluated in the given context
// CaseExpr context support depends on all its conditions and values.
func (c *CaseExpr) SupportsContext(ctx EvaluationContext) bool {
	for _, when := range c.whens {
		if condContextual, ok := when.condition.(ContextualExpr); ok {
			if !condContextual.SupportsContext(ctx) {
				return false
			}
		}
		if valContextual, ok := when.value.(ContextualExpr); ok {
			if !valContextual.SupportsContext(ctx) {
				return false
			}
		}
	}
	if c.elseValue != nil {
		if elseContextual, ok := c.elseValue.(ContextualExpr); ok {
			if !elseContextual.SupportsContext(ctx) {
				return false
			}
		}
	}
	return true
}

// When adds a condition-value pair to the case expression.
func (c *CaseExpr) When(condition, value Expr) *CaseExpr {
	newWhens := make([]CaseWhen, len(c.whens)+1)
	copy(newWhens, c.whens)
	newWhens[len(c.whens)] = CaseWhen{condition: condition, value: value}

	return &CaseExpr{
		whens:     newWhens,
		elseValue: c.elseValue,
	}
}

// Else sets the default value for the case expression.
func (c *CaseExpr) Else(value Expr) *CaseExpr {
	return &CaseExpr{
		whens:     c.whens,
		elseValue: value,
	}
}

// Aggregation constructor functions

// Sum creates a sum aggregation expression.
func Sum(column Expr) *AggregationExpr {
	return &AggregationExpr{column: column, aggType: AggSum}
}

// Count creates a count aggregation expression.
func Count(column Expr) *AggregationExpr {
	return &AggregationExpr{column: column, aggType: AggCount}
}

// Mean creates a mean aggregation expression.
func Mean(column Expr) *AggregationExpr {
	return &AggregationExpr{column: column, aggType: AggMean}
}

// Min creates a min aggregation expression.
func Min(column Expr) *AggregationExpr {
	return &AggregationExpr{column: column, aggType: AggMin}
}

// Max creates a max aggregation expression.
func Max(column Expr) *AggregationExpr {
	return &AggregationExpr{column: column, aggType: AggMax}
}

// Aggregation methods on column expressions

// Sum creates a sum aggregation of this column.
func (c *ColumnExpr) Sum() *AggregationExpr {
	return Sum(c)
}

// Count creates a count aggregation of this column.
func (c *ColumnExpr) Count() *AggregationExpr {
	return Count(c)
}

// Mean creates a mean aggregation of this column.
func (c *ColumnExpr) Mean() *AggregationExpr {
	return Mean(c)
}

// Min creates a min aggregation of this column.
func (c *ColumnExpr) Min() *AggregationExpr {
	return Min(c)
}

// Max creates a max aggregation of this column.
func (c *ColumnExpr) Max() *AggregationExpr {
	return Max(c)
}

// As sets an alias for the aggregation expression.
func (a *AggregationExpr) As(alias string) *AggregationExpr {
	return &AggregationExpr{
		column:  a.column,
		aggType: a.aggType,
		alias:   alias,
	}
}

// Gt creates a greater-than expression.
func (a *AggregationExpr) Gt(other Expr) *BinaryExpr {
	return &BinaryExpr{left: a, op: OpGt, right: other}
}

// Lt creates a less-than expression.
func (a *AggregationExpr) Lt(other Expr) *BinaryExpr {
	return &BinaryExpr{left: a, op: OpLt, right: other}
}

// Eq creates an equality expression.
func (a *AggregationExpr) Eq(other Expr) *BinaryExpr {
	return &BinaryExpr{left: a, op: OpEq, right: other}
}

// Ne creates a not-equal expression.
func (a *AggregationExpr) Ne(other Expr) *BinaryExpr {
	return &BinaryExpr{left: a, op: OpNe, right: other}
}

// Ge creates a greater-than-or-equal expression.
func (a *AggregationExpr) Ge(other Expr) *BinaryExpr {
	return &BinaryExpr{left: a, op: OpGe, right: other}
}

// Le creates a less-than-or-equal expression.
func (a *AggregationExpr) Le(other Expr) *BinaryExpr {
	return &BinaryExpr{left: a, op: OpLe, right: other}
}

// And creates a logical AND expression.
func (a *AggregationExpr) And(other Expr) *BinaryExpr {
	return &BinaryExpr{left: a, op: OpAnd, right: other}
}

// Or creates a logical OR expression.
func (a *AggregationExpr) Or(other Expr) *BinaryExpr {
	return &BinaryExpr{left: a, op: OpOr, right: other}
}

// Arithmetic operations for AggregationExpr

// Add creates an addition expression.
func (a *AggregationExpr) Add(other Expr) *BinaryExpr {
	return &BinaryExpr{left: a, op: OpAdd, right: other}
}

// Sub creates a subtraction expression.
func (a *AggregationExpr) Sub(other Expr) *BinaryExpr {
	return &BinaryExpr{left: a, op: OpSub, right: other}
}

// Mul creates a multiplication expression.
func (a *AggregationExpr) Mul(other Expr) *BinaryExpr {
	return &BinaryExpr{left: a, op: OpMul, right: other}
}

// Div creates a division expression.
func (a *AggregationExpr) Div(other Expr) *BinaryExpr {
	return &BinaryExpr{left: a, op: OpDiv, right: other}
}

// Unary operations for AggregationExpr

// Neg creates a negation (unary minus) expression.
func (a *AggregationExpr) Neg() *UnaryExpr {
	return &UnaryExpr{op: UnaryNeg, operand: a}
}

// Not creates a logical NOT expression.
func (a *AggregationExpr) Not() *UnaryExpr {
	return &UnaryExpr{op: UnaryNot, operand: a}
}

// Math functions for AggregationExpr

// Abs creates an absolute value function expression.
func (a *AggregationExpr) Abs() *FunctionExpr {
	return &FunctionExpr{name: "abs", args: []Expr{a}}
}

// Round creates a round function expression.
func (a *AggregationExpr) Round() *FunctionExpr {
	return &FunctionExpr{name: "round", args: []Expr{a}}
}

// RoundTo creates a round function expression with precision.
func (a *AggregationExpr) RoundTo(precision Expr) *FunctionExpr {
	return &FunctionExpr{name: "round", args: []Expr{a, precision}}
}

// Floor creates a floor function expression.
func (a *AggregationExpr) Floor() *FunctionExpr {
	return &FunctionExpr{name: "floor", args: []Expr{a}}
}

// Ceil creates a ceil function expression.
func (a *AggregationExpr) Ceil() *FunctionExpr {
	return &FunctionExpr{name: "ceil", args: []Expr{a}}
}

// Sqrt creates a square root function expression.
func (a *AggregationExpr) Sqrt() *FunctionExpr {
	return &FunctionExpr{name: "sqrt", args: []Expr{a}}
}

// Log creates a natural logarithm function expression.
func (a *AggregationExpr) Log() *FunctionExpr {
	return &FunctionExpr{name: "log", args: []Expr{a}}
}

// Enhanced Expression System - Unary Operations

// Neg creates a negation (unary minus) expression.
func (c *ColumnExpr) Neg() *UnaryExpr {
	return &UnaryExpr{op: UnaryNeg, operand: c}
}

// Not creates a logical NOT expression.
func (c *ColumnExpr) Not() *UnaryExpr {
	return &UnaryExpr{op: UnaryNot, operand: c}
}

// Neg creates a negation expression for function expressions.
func (f *FunctionExpr) Neg() *UnaryExpr {
	return &UnaryExpr{op: UnaryNeg, operand: f}
}

func (f *FunctionExpr) Not() *UnaryExpr {
	return &UnaryExpr{op: UnaryNot, operand: f}
}

// Math Functions

// Abs creates an absolute value function expression.
func (c *ColumnExpr) Abs() *FunctionExpr {
	return &FunctionExpr{name: "abs", args: []Expr{c}}
}

// Round creates a round function expression.
func (c *ColumnExpr) Round() *FunctionExpr {
	return &FunctionExpr{name: "round", args: []Expr{c}}
}

// RoundTo creates a round function expression with precision.
func (c *ColumnExpr) RoundTo(precision Expr) *FunctionExpr {
	return &FunctionExpr{name: "round", args: []Expr{c, precision}}
}

// Floor creates a floor function expression.
func (c *ColumnExpr) Floor() *FunctionExpr {
	return &FunctionExpr{name: "floor", args: []Expr{c}}
}

// Ceil creates a ceil function expression.
func (c *ColumnExpr) Ceil() *FunctionExpr {
	return &FunctionExpr{name: "ceil", args: []Expr{c}}
}

// Sqrt creates a square root function expression.
func (c *ColumnExpr) Sqrt() *FunctionExpr {
	return &FunctionExpr{name: "sqrt", args: []Expr{c}}
}

// Log creates a natural logarithm function expression.
func (c *ColumnExpr) Log() *FunctionExpr {
	return &FunctionExpr{name: "log", args: []Expr{c}}
}

// Sin creates a sine function expression.
func (c *ColumnExpr) Sin() *FunctionExpr {
	return &FunctionExpr{name: "sin", args: []Expr{c}}
}

// Cos creates a cosine function expression.
func (c *ColumnExpr) Cos() *FunctionExpr {
	return &FunctionExpr{name: "cos", args: []Expr{c}}
}

// Abs creates an absolute value function expression.
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

// Upper creates an UPPER function expression.
func (c *ColumnExpr) Upper() *FunctionExpr {
	return &FunctionExpr{name: "upper", args: []Expr{c}}
}

// Lower creates a LOWER function expression.
func (c *ColumnExpr) Lower() *FunctionExpr {
	return &FunctionExpr{name: "lower", args: []Expr{c}}
}

// Length creates a LENGTH function expression.
func (c *ColumnExpr) Length() *FunctionExpr {
	return &FunctionExpr{name: "length", args: []Expr{c}}
}

// Trim creates a TRIM function expression.
func (c *ColumnExpr) Trim() *FunctionExpr {
	return &FunctionExpr{name: "trim", args: []Expr{c}}
}

// Substring creates a SUBSTRING function expression.
func (c *ColumnExpr) Substring(start, length Expr) *FunctionExpr {
	return &FunctionExpr{name: "substring", args: []Expr{c, start, length}}
}

// Upper creates an uppercase function expression.
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

// CastToString creates a cast to string function expression.
func (c *ColumnExpr) CastToString() *FunctionExpr {
	return &FunctionExpr{name: "cast_string", args: []Expr{c}}
}

// CastToInt64 creates a cast to int64 function expression.
func (c *ColumnExpr) CastToInt64() *FunctionExpr {
	return &FunctionExpr{name: "cast_int64", args: []Expr{c}}
}

// CastToFloat64 creates a cast to float64 function expression.
func (c *ColumnExpr) CastToFloat64() *FunctionExpr {
	return &FunctionExpr{name: "cast_float64", args: []Expr{c}}
}

// CastToBool creates a cast to bool function expression.
func (c *ColumnExpr) CastToBool() *FunctionExpr {
	return &FunctionExpr{name: "cast_bool", args: []Expr{c}}
}

// CastToString creates a string casting function expression.
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

// Add creates an addition expression for function expressions.
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

// If creates an IF function expression.
func If(condition, thenValue, elseValue Expr) *FunctionExpr {
	return &FunctionExpr{name: "if", args: []Expr{condition, thenValue, elseValue}}
}

// Coalesce creates a COALESCE function expression.
func Coalesce(exprs ...Expr) *FunctionExpr {
	return &FunctionExpr{name: "coalesce", args: exprs}
}

// Concat creates a CONCAT function expression.
func Concat(exprs ...Expr) *FunctionExpr {
	return &FunctionExpr{name: "concat", args: exprs}
}

// Case creates a new CASE expression.
func Case() *CaseExpr {
	return &CaseExpr{whens: make([]CaseWhen, 0), elseValue: nil}
}

// Date/Time Functions

// Year creates a YEAR function expression to extract year from date/time.
func (c *ColumnExpr) Year() *FunctionExpr {
	return &FunctionExpr{name: "year", args: []Expr{c}}
}

// Month creates a MONTH function expression to extract month from date/time.
func (c *ColumnExpr) Month() *FunctionExpr {
	return &FunctionExpr{name: "month", args: []Expr{c}}
}

// Day creates a DAY function expression to extract day from date/time.
func (c *ColumnExpr) Day() *FunctionExpr {
	return &FunctionExpr{name: "day", args: []Expr{c}}
}

// Hour creates an HOUR function expression to extract hour from timestamp.
func (c *ColumnExpr) Hour() *FunctionExpr {
	return &FunctionExpr{name: "hour", args: []Expr{c}}
}

// Minute creates a MINUTE function expression to extract minute from timestamp.
func (c *ColumnExpr) Minute() *FunctionExpr {
	return &FunctionExpr{name: "minute", args: []Expr{c}}
}

// Second creates a SECOND function expression to extract second from timestamp.
func (c *ColumnExpr) Second() *FunctionExpr {
	return &FunctionExpr{name: "second", args: []Expr{c}}
}

// Date/Time Functions for FunctionExpr

// Year creates a YEAR function expression to extract year from date/time.
func (f *FunctionExpr) Year() *FunctionExpr {
	return &FunctionExpr{name: "year", args: []Expr{f}}
}

// Month creates a MONTH function expression to extract month from date/time.
func (f *FunctionExpr) Month() *FunctionExpr {
	return &FunctionExpr{name: "month", args: []Expr{f}}
}

// Day creates a DAY function expression to extract day from date/time.
func (f *FunctionExpr) Day() *FunctionExpr {
	return &FunctionExpr{name: "day", args: []Expr{f}}
}

// Hour creates an HOUR function expression to extract hour from timestamp.
func (f *FunctionExpr) Hour() *FunctionExpr {
	return &FunctionExpr{name: "hour", args: []Expr{f}}
}

// Minute creates a MINUTE function expression to extract minute from timestamp.
func (f *FunctionExpr) Minute() *FunctionExpr {
	return &FunctionExpr{name: "minute", args: []Expr{f}}
}

// Second creates a SECOND function expression to extract second from timestamp.
func (f *FunctionExpr) Second() *FunctionExpr {
	return &FunctionExpr{name: "second", args: []Expr{f}}
}

// Date/Time Constructor Functions

// Year creates a YEAR function expression.
func Year(expr Expr) *FunctionExpr {
	return &FunctionExpr{name: "year", args: []Expr{expr}}
}

// Month creates a MONTH function expression.
func Month(expr Expr) *FunctionExpr {
	return &FunctionExpr{name: "month", args: []Expr{expr}}
}

// Day creates a DAY function expression.
func Day(expr Expr) *FunctionExpr {
	return &FunctionExpr{name: "day", args: []Expr{expr}}
}

// Hour creates an HOUR function expression.
func Hour(expr Expr) *FunctionExpr {
	return &FunctionExpr{name: "hour", args: []Expr{expr}}
}

// Minute creates a MINUTE function expression.
func Minute(expr Expr) *FunctionExpr {
	return &FunctionExpr{name: "minute", args: []Expr{expr}}
}

// Second creates a SECOND function expression.
func Second(expr Expr) *FunctionExpr {
	return &FunctionExpr{name: "second", args: []Expr{expr}}
}

// Date/Time Interval Types

// IntervalType represents different types of time intervals.
type IntervalType int

const (
	IntervalDays IntervalType = iota
	IntervalHours
	IntervalMinutes
	IntervalMonths
	IntervalYears
)

// IntervalExpr represents a time interval value and type.
type IntervalExpr struct {
	value        int64
	intervalType IntervalType
}

func (i *IntervalExpr) Type() Type {
	return ExprLiteral // Intervals are treated as literals
}

func (i *IntervalExpr) String() string {
	unit := common.FormatIntervalType(int(i.intervalType))
	return common.FormatFunction("interval", fmt.Sprintf("%d %s", i.value, unit))
}

func (i *IntervalExpr) Value() int64 {
	return i.value
}

func (i *IntervalExpr) IntervalType() IntervalType {
	return i.intervalType
}

// SupportsContext returns whether this expression can be evaluated in the given context
// IntervalExpr can be evaluated in any context (it's a literal-like expression).
func (i *IntervalExpr) SupportsContext(_ EvaluationContext) bool {
	return true
}

// Interval constructor functions

// Days creates an interval representing days.
func Days(value int64) *IntervalExpr {
	return &IntervalExpr{value: value, intervalType: IntervalDays}
}

// Hours creates an interval representing hours.
func Hours(value int64) *IntervalExpr {
	return &IntervalExpr{value: value, intervalType: IntervalHours}
}

// Minutes creates an interval representing minutes.
func Minutes(value int64) *IntervalExpr {
	return &IntervalExpr{value: value, intervalType: IntervalMinutes}
}

// Months creates an interval representing months.
func Months(value int64) *IntervalExpr {
	return &IntervalExpr{value: value, intervalType: IntervalMonths}
}

// Years creates an interval representing years.
func Years(value int64) *IntervalExpr {
	return &IntervalExpr{value: value, intervalType: IntervalYears}
}

// Date/Time Arithmetic Functions

// DateAdd creates a DATE_ADD function expression to add interval to date/time.
func DateAdd(dateExpr Expr, intervalExpr *IntervalExpr) *FunctionExpr {
	return &FunctionExpr{name: "date_add", args: []Expr{dateExpr, intervalExpr}}
}

// DateSub creates a DATE_SUB function expression to subtract interval from date/time.
func DateSub(dateExpr Expr, intervalExpr *IntervalExpr) *FunctionExpr {
	return &FunctionExpr{name: "date_sub", args: []Expr{dateExpr, intervalExpr}}
}

// DateDiff creates a DATE_DIFF function expression to calculate difference between dates.
func DateDiff(startDate, endDate Expr, unit string) *FunctionExpr {
	unitLiteral := &LiteralExpr{value: unit}
	return &FunctionExpr{name: "date_diff", args: []Expr{startDate, endDate, unitLiteral}}
}

// Date/Time Arithmetic methods for ColumnExpr

// DateAdd adds an interval to a date/time column.
func (c *ColumnExpr) DateAdd(intervalExpr *IntervalExpr) *FunctionExpr {
	return DateAdd(c, intervalExpr)
}

// DateSub subtracts an interval from a date/time column.
func (c *ColumnExpr) DateSub(intervalExpr *IntervalExpr) *FunctionExpr {
	return DateSub(c, intervalExpr)
}

// Date/Time Arithmetic methods for FunctionExpr

// DateAdd adds an interval to a date/time function result.
func (f *FunctionExpr) DateAdd(intervalExpr *IntervalExpr) *FunctionExpr {
	return DateAdd(f, intervalExpr)
}

// DateSub subtracts an interval from a date/time function result.
func (f *FunctionExpr) DateSub(intervalExpr *IntervalExpr) *FunctionExpr {
	return DateSub(f, intervalExpr)
}

// ValidateExpressionContext validates that an expression can be used in the given context.
func ValidateExpressionContext(expr Expr, ctx EvaluationContext) error {
	if contextual, ok := expr.(ContextualExpr); ok {
		if !contextual.SupportsContext(ctx) {
			return fmt.Errorf("expression %s cannot be used in %s", expr.String(), ctx.String())
		}
	}

	// Recursively validate sub-expressions
	switch e := expr.(type) {
	case *BinaryExpr:
		return validateBinaryExpr(e, ctx)
	case *UnaryExpr:
		return ValidateExpressionContext(e.operand, ctx)
	case *FunctionExpr:
		return validateFunctionExpr(e, ctx)
	case *CaseExpr:
		return validateCaseExpr(e, ctx)
	}

	return nil
}

// validateBinaryExpr validates a binary expression's sub-expressions.
func validateBinaryExpr(e *BinaryExpr, ctx EvaluationContext) error {
	if err := ValidateExpressionContext(e.left, ctx); err != nil {
		return err
	}
	return ValidateExpressionContext(e.right, ctx)
}

// validateFunctionExpr validates a function expression's arguments.
func validateFunctionExpr(e *FunctionExpr, ctx EvaluationContext) error {
	for _, arg := range e.args {
		if err := ValidateExpressionContext(arg, ctx); err != nil {
			return err
		}
	}
	return nil
}

// validateCaseExpr validates a case expression's conditions and values.
func validateCaseExpr(e *CaseExpr, ctx EvaluationContext) error {
	for _, when := range e.whens {
		if err := ValidateExpressionContext(when.condition, ctx); err != nil {
			return err
		}
		if err := ValidateExpressionContext(when.value, ctx); err != nil {
			return err
		}
	}
	if e.elseValue != nil {
		return ValidateExpressionContext(e.elseValue, ctx)
	}
	return nil
}
