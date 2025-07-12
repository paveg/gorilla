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
