package expr

import (
	"cmp"
	"errors"
	"fmt"
	"strconv"
	"time"

	"golang.org/x/exp/constraints"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	dferrors "github.com/paveg/gorilla/internal/errors"
)

// Type name constants.
const (
	typeInt32   = "int32"
	typeInt64   = "int64"
	typeFloat32 = "float32"
	typeFloat64 = "float64"
	typeDouble  = "double"

	// Time constants.
	nanosPerSecond = 1e9

	// Date/time arithmetic constants.
	dateAddArgsCount  = 2
	dateSubArgsCount  = 2
	dateDiffArgsCount = 3
	hoursPerDay       = 24
	monthsPerYear     = 12

	// Date/time unit constants.
	unitDays    = "days"
	unitHours   = "hours"
	unitMinutes = "minutes"
	unitSeconds = "seconds"
	unitMonths  = "months"
	unitYears   = "years"
)

// Type hierarchy levels.
const (
	levelInt32   = 1
	levelInt64   = 2
	levelFloat32 = 3
	levelFloat64 = 4
	levelDouble  = 4
)

// Evaluator evaluates expressions against Arrow arrays.
type Evaluator struct {
	mem memory.Allocator
}

// NewEvaluator creates a new expression evaluator.
func NewEvaluator(mem memory.Allocator) *Evaluator {
	if mem == nil {
		mem = memory.NewGoAllocator()
	}
	return &Evaluator{mem: mem}
}

// EvaluateWithContext evaluates an expression with a specific evaluation context.
func (e *Evaluator) EvaluateWithContext(
	expr Expr,
	columns map[string]arrow.Array,
	ctx EvaluationContext,
) (arrow.Array, error) {
	// Validate context support
	if err := e.validateContextSupport(expr, ctx); err != nil {
		return nil, err
	}

	switch ex := expr.(type) {
	case *ColumnExpr:
		return e.evaluateColumnWithContext(ex, columns, ctx)
	case *LiteralExpr:
		return e.evaluateLiteralWithContext(ex, columns, ctx)
	case *BinaryExpr:
		return e.evaluateBinaryWithContext(ex, columns, ctx)
	case *FunctionExpr:
		return e.evaluateFunctionWithContext(ex, columns, ctx)
	case *AggregationExpr:
		return e.evaluateAggregationWithContext(ex, columns, ctx)
	case *CaseExpr:
		return e.evaluateCaseWithContext(ex, columns, ctx)
	case *WindowExpr:
		return e.evaluateWindowWithContext(ex, columns, ctx)
	case *InvalidExpr:
		return nil, dferrors.NewInvalidExpressionError("Evaluate", ex.Message())
	default:
		supportedTypes := []string{"ColumnExpr", "LiteralExpr", "BinaryExpr", "AggregationExpr", "FunctionExpr", "CaseExpr", "WindowExpr"}
		return nil, dferrors.NewUnsupportedOperationError("Evaluate", fmt.Sprintf("expression type %T", expr), supportedTypes)
	}
}

// EvaluateBooleanWithContext evaluates an expression that should return a boolean array with context.
func (e *Evaluator) EvaluateBooleanWithContext(
	expr Expr,
	columns map[string]arrow.Array,
	ctx EvaluationContext,
) (arrow.Array, error) {
	// Validate context support
	if err := e.validateContextSupport(expr, ctx); err != nil {
		return nil, err
	}

	switch ex := expr.(type) {
	case *ColumnExpr:
		return e.evaluateColumnBooleanWithContext(ex, columns, ctx)
	case *LiteralExpr:
		return e.evaluateLiteralBooleanWithContext(ex, columns, ctx)
	case *BinaryExpr:
		return e.evaluateBinaryBooleanWithContext(ex, columns, ctx)
	case *InvalidExpr:
		return nil, fmt.Errorf("invalid expression: %s", ex.Message())
	default:
		return nil, fmt.Errorf("unsupported expression type for boolean evaluation: %T", expr)
	}
}

// validateContextSupport validates that an expression supports the given context.
func (e *Evaluator) validateContextSupport(expr Expr, ctx EvaluationContext) error {
	if contextualExpr, ok := expr.(ContextualExpr); ok {
		if !contextualExpr.SupportsContext(ctx) {
			return fmt.Errorf("expression %s does not support %s context", expr.String(), ctx.String())
		}
	}
	return nil
}

// EvaluateBoolean evaluates an expression that should return a boolean array.
func (e *Evaluator) EvaluateBoolean(expr Expr, columns map[string]arrow.Array) (arrow.Array, error) {
	switch ex := expr.(type) {
	case *ColumnExpr:
		return e.evaluateColumnBoolean(ex, columns)
	case *LiteralExpr:
		return e.evaluateLiteralBoolean(ex, columns)
	case *BinaryExpr:
		return e.evaluateBinaryBoolean(ex, columns)
	case *InvalidExpr:
		return nil, fmt.Errorf("invalid expression: %s", ex.Message())
	default:
		return nil, fmt.Errorf("unsupported expression type for boolean evaluation: %T", expr)
	}
}

// Evaluate evaluates an expression that returns a value array (numeric, string, etc.)
func (e *Evaluator) Evaluate(expr Expr, columns map[string]arrow.Array) (arrow.Array, error) {
	switch ex := expr.(type) {
	case *ColumnExpr:
		return e.evaluateColumn(ex, columns)
	case *LiteralExpr:
		return e.evaluateLiteral(ex, columns)
	case *BinaryExpr:
		return e.evaluateBinary(ex, columns)
	case *FunctionExpr:
		return e.evaluateFunction(ex, columns)
	case *WindowExpr:
		return e.EvaluateWindow(ex, columns)
	case *WindowFunctionExpr:
		return e.evaluateWindowFunction(ex, nil, columns)
	case *InvalidExpr:
		return nil, fmt.Errorf("invalid expression: %s", ex.Message())
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// Context-aware evaluation methods for each expression type

// evaluateColumnWithContext evaluates a column expression with context.
func (e *Evaluator) evaluateColumnWithContext(
	expr *ColumnExpr,
	columns map[string]arrow.Array,
	ctx EvaluationContext,
) (arrow.Array, error) {
	// Column evaluation is the same in both contexts, but the column names/data may differ
	_ = ctx // Context is validated before this method is called
	return e.evaluateColumn(expr, columns)
}

// evaluateLiteralWithContext evaluates a literal expression with context.
func (e *Evaluator) evaluateLiteralWithContext(
	expr *LiteralExpr,
	columns map[string]arrow.Array,
	ctx EvaluationContext,
) (arrow.Array, error) {
	// Literal evaluation is context-independent
	_ = ctx // Context is validated before this method is called
	return e.evaluateLiteral(expr, columns)
}

// evaluateBinaryWithContext evaluates a binary expression with context.
func (e *Evaluator) evaluateBinaryWithContext(
	expr *BinaryExpr,
	columns map[string]arrow.Array,
	ctx EvaluationContext,
) (arrow.Array, error) {
	// Validate context support for operands
	if err := e.validateContextSupport(expr.left, ctx); err != nil {
		return nil, fmt.Errorf("validating left operand context: %w", err)
	}
	if err := e.validateContextSupport(expr.right, ctx); err != nil {
		return nil, fmt.Errorf("validating right operand context: %w", err)
	}

	// Evaluate left and right operands with context
	left, err := e.EvaluateWithContext(expr.left, columns, ctx)
	if err != nil {
		return nil, fmt.Errorf("evaluating left operand: %w", err)
	}
	defer left.Release()

	right, err := e.EvaluateWithContext(expr.right, columns, ctx)
	if err != nil {
		return nil, fmt.Errorf("evaluating right operand: %w", err)
	}
	defer right.Release()

	// Apply the binary operation
	switch expr.op {
	case OpAdd, OpSub, OpMul, OpDiv:
		return e.evaluateArithmetic(left, right, expr.op)
	case OpEq, OpNe, OpLt, OpLe, OpGt, OpGe:
		return e.evaluateComparison(left, right, expr.op)
	case OpAnd, OpOr:
		return e.evaluateLogical(left, right, expr.op)
	default:
		return nil, fmt.Errorf("unsupported binary operation: %v", expr.op)
	}
}

// evaluateBinaryBooleanWithContext evaluates a binary expression that returns boolean with context.
func (e *Evaluator) evaluateBinaryBooleanWithContext(
	expr *BinaryExpr,
	columns map[string]arrow.Array,
	ctx EvaluationContext,
) (arrow.Array, error) {
	// Validate context support for operands
	if err := e.validateContextSupport(expr.left, ctx); err != nil {
		return nil, fmt.Errorf("validating left operand context: %w", err)
	}
	if err := e.validateContextSupport(expr.right, ctx); err != nil {
		return nil, fmt.Errorf("validating right operand context: %w", err)
	}

	// Evaluate left and right operands with context
	left, err := e.EvaluateWithContext(expr.left, columns, ctx)
	if err != nil {
		return nil, fmt.Errorf("evaluating left operand: %w", err)
	}
	defer left.Release()

	right, err := e.EvaluateWithContext(expr.right, columns, ctx)
	if err != nil {
		return nil, fmt.Errorf("evaluating right operand: %w", err)
	}
	defer right.Release()

	// Apply the binary operation
	switch expr.op {
	case OpEq, OpNe, OpLt, OpLe, OpGt, OpGe:
		return e.evaluateComparison(left, right, expr.op)
	case OpAnd, OpOr:
		return e.evaluateLogical(left, right, expr.op)
	case OpAdd, OpSub, OpMul, OpDiv:
		return nil, fmt.Errorf("arithmetic operation %v does not produce boolean result", expr.op)
	default:
		return nil, fmt.Errorf("binary operation %v does not produce boolean result", expr.op)
	}
}

// evaluateColumnBooleanWithContext evaluates a column expression that should return boolean with context.
func (e *Evaluator) evaluateColumnBooleanWithContext(
	expr *ColumnExpr,
	columns map[string]arrow.Array,
	ctx EvaluationContext,
) (arrow.Array, error) {
	// Column boolean evaluation is the same in both contexts
	_ = ctx // Context is validated before this method is called
	return e.evaluateColumnBoolean(expr, columns)
}

// evaluateLiteralBooleanWithContext evaluates a literal expression that should return boolean with context.
func (e *Evaluator) evaluateLiteralBooleanWithContext(
	expr *LiteralExpr,
	columns map[string]arrow.Array,
	ctx EvaluationContext,
) (arrow.Array, error) {
	// Literal boolean evaluation is context-independent
	_ = ctx // Context is validated before this method is called
	return e.evaluateLiteralBoolean(expr, columns)
}

// evaluateFunctionWithContext evaluates a function expression with context.
func (e *Evaluator) evaluateFunctionWithContext(
	expr *FunctionExpr,
	columns map[string]arrow.Array,
	ctx EvaluationContext,
) (arrow.Array, error) {
	// Validate context support for function arguments
	for i, arg := range expr.args {
		if err := e.validateContextSupport(arg, ctx); err != nil {
			return nil, fmt.Errorf("validating argument %d context for function %s: %w", i, expr.name, err)
		}
	}

	// Most functions work the same way regardless of context
	// The context validation ensures the arguments are appropriate for the context
	return e.evaluateFunction(expr, columns)
}

// evaluateAggregationWithContext evaluates an aggregation expression with context.
func (e *Evaluator) evaluateAggregationWithContext(
	expr *AggregationExpr,
	columns map[string]arrow.Array,
	ctx EvaluationContext,
) (arrow.Array, error) {
	// Aggregation expressions can only be evaluated in GroupContext
	if ctx != GroupContext {
		return nil, fmt.Errorf("aggregation expressions can only be evaluated in GroupContext, got %s", ctx.String())
	}

	// For aggregation expressions, we delegate to the existing evaluation
	// The context validation ensures we're in the right context
	// Note: This might need special handling depending on how aggregations are implemented
	// in the GroupBy operations, but for now we assume the columns contain pre-aggregated data
	return e.evaluateAggregationValue(expr, columns)
}

// evaluateWindowWithContext evaluates a window expression with context.
func (e *Evaluator) evaluateWindowWithContext(
	expr *WindowExpr,
	columns map[string]arrow.Array,
	ctx EvaluationContext,
) (arrow.Array, error) {
	// Window expressions can only be evaluated in RowContext
	if ctx != RowContext {
		return nil, fmt.Errorf("window expressions can only be evaluated in RowContext, got %s", ctx.String())
	}

	// Validate context support for the window function
	if err := e.validateContextSupport(expr.function, ctx); err != nil {
		return nil, fmt.Errorf("validating window function context: %w", err)
	}

	// Delegate to the existing window evaluation
	return e.EvaluateWindow(expr, columns)
}

// evaluateAggregationValue is a helper method for evaluating aggregation expressions
// when they appear in contexts where the aggregated value is already computed.
func (e *Evaluator) evaluateAggregationValue(
	expr *AggregationExpr,
	columns map[string]arrow.Array,
) (arrow.Array, error) {
	// In GroupContext, aggregation expressions typically reference pre-computed aggregated columns
	// The actual aggregation computation happens in the GroupBy operation
	// Here we just need to retrieve the aggregated value from the columns

	// Try to find the aggregated column by alias first, then by default naming
	var columnName string
	if expr.alias != "" {
		columnName = expr.alias
	} else {
		// Generate a default aggregation column name
		var aggName string
		switch expr.aggType {
		case AggSum:
			aggName = AggNameSum
		case AggCount:
			aggName = AggNameCount
		case AggMean:
			aggName = AggNameMean
		case AggMin:
			aggName = AggNameMin
		case AggMax:
			aggName = AggNameMax
		default:
			return nil, fmt.Errorf("unsupported aggregation type: %v", expr.aggType)
		}

		// Default naming pattern: aggName_columnName
		if colExpr, ok := expr.column.(*ColumnExpr); ok {
			columnName = fmt.Sprintf("%s_%s", aggName, colExpr.name)
		} else {
			columnName = fmt.Sprintf("%s_%s", aggName, expr.column.String())
		}
	}

	// Look up the aggregated column
	arr, exists := columns[columnName]
	if !exists {
		// Get available column names for suggestions
		availableColumns := make([]string, 0, len(columns))
		for colName := range columns {
			availableColumns = append(availableColumns, colName)
		}
		return nil, dferrors.NewColumnNotFoundErrorWithSuggestions("Aggregation", columnName, availableColumns)
	}

	// Return a reference to the existing array
	arr.Retain()
	return arr, nil
}

func (e *Evaluator) evaluateColumn(expr *ColumnExpr, columns map[string]arrow.Array) (arrow.Array, error) {
	arr, exists := columns[expr.name]
	if !exists {
		// Get available column names for suggestions
		availableColumns := make([]string, 0, len(columns))
		for colName := range columns {
			availableColumns = append(availableColumns, colName)
		}
		return nil, dferrors.NewColumnNotFoundErrorWithSuggestions("Column", expr.name, availableColumns)
	}
	// Return a reference to the existing array (caller should handle retention if needed)
	arr.Retain()
	return arr, nil
}

func (e *Evaluator) evaluateColumnBoolean(expr *ColumnExpr, columns map[string]arrow.Array) (arrow.Array, error) {
	arr, exists := columns[expr.name]
	if !exists {
		// Get available column names for suggestions
		availableColumns := make([]string, 0, len(columns))
		for colName := range columns {
			availableColumns = append(availableColumns, colName)
		}
		return nil, dferrors.NewColumnNotFoundErrorWithSuggestions("BooleanColumn", expr.name, availableColumns)
	}

	// Check if it's already a boolean array
	if _, ok := arr.(*array.Boolean); ok {
		arr.Retain()
		return arr, nil
	}

	return nil, fmt.Errorf("column %s is not a boolean type", expr.name)
}

func (e *Evaluator) evaluateLiteral(expr *LiteralExpr, columns map[string]arrow.Array) (arrow.Array, error) {
	// Create an array with the literal value repeated for all rows
	length := e.getArrayLength(columns)
	if length == 0 {
		return nil, errors.New("cannot determine array length for literal")
	}

	switch val := expr.value.(type) {
	case string:
		return e.evaluateStringLiteral(val, length)
	case int32:
		return e.evaluateInt32Literal(val, length)
	case int64:
		return e.evaluateInt64Literal(val, length)
	case int:
		return e.evaluateIntLiteral(val, length)
	case float32:
		return e.evaluateFloat32Literal(val, length)
	case float64:
		return e.evaluateFloat64Literal(val, length)
	case bool:
		return e.evaluateBooleanLiteral(val, length)
	case time.Time:
		return e.evaluateTimeLiteral(val, length)
	default:
		return nil, fmt.Errorf("unsupported literal type: %T", val)
	}
}

func (e *Evaluator) evaluateLiteralBoolean(expr *LiteralExpr, columns map[string]arrow.Array) (arrow.Array, error) {
	length := e.getArrayLength(columns)
	if length == 0 {
		return nil, errors.New("cannot determine array length for literal")
	}

	val, ok := expr.value.(bool)
	if !ok {
		return nil, fmt.Errorf("literal is not a boolean: %T", expr.value)
	}

	builder := array.NewBooleanBuilder(e.mem)
	defer builder.Release()
	for range length {
		builder.Append(val)
	}
	return builder.NewArray(), nil
}

func (e *Evaluator) evaluateBinaryBoolean(expr *BinaryExpr, columns map[string]arrow.Array) (arrow.Array, error) {
	// Evaluate left and right operands
	left, err := e.Evaluate(expr.left, columns)
	if err != nil {
		return nil, fmt.Errorf("evaluating left operand: %w", err)
	}
	defer left.Release()

	right, err := e.Evaluate(expr.right, columns)
	if err != nil {
		return nil, fmt.Errorf("evaluating right operand: %w", err)
	}
	defer right.Release()

	// Apply the binary operation
	switch expr.op {
	case OpEq, OpNe, OpLt, OpLe, OpGt, OpGe:
		return e.evaluateComparison(left, right, expr.op)
	case OpAnd, OpOr:
		return e.evaluateLogical(left, right, expr.op)
	case OpAdd, OpSub, OpMul, OpDiv:
		return nil, fmt.Errorf("arithmetic operation %v does not produce boolean result", expr.op)
	default:
		return nil, fmt.Errorf("binary operation %v does not produce boolean result", expr.op)
	}
}

func (e *Evaluator) evaluateBinary(expr *BinaryExpr, columns map[string]arrow.Array) (arrow.Array, error) {
	// Evaluate left and right operands
	left, err := e.Evaluate(expr.left, columns)
	if err != nil {
		return nil, fmt.Errorf("evaluating left operand: %w", err)
	}
	defer left.Release()

	right, err := e.Evaluate(expr.right, columns)
	if err != nil {
		return nil, fmt.Errorf("evaluating right operand: %w", err)
	}
	defer right.Release()

	// Apply the binary operation
	switch expr.op {
	case OpAdd, OpSub, OpMul, OpDiv:
		return e.evaluateArithmetic(left, right, expr.op)
	case OpEq, OpNe, OpLt, OpLe, OpGt, OpGe:
		return e.evaluateComparison(left, right, expr.op)
	case OpAnd, OpOr:
		return e.evaluateLogical(left, right, expr.op)
	default:
		return nil, fmt.Errorf("unsupported binary operation: %v", expr.op)
	}
}

func (e *Evaluator) evaluateArithmetic(left, right arrow.Array, op BinaryOp) (arrow.Array, error) {
	// Determine the promoted type for mixed arithmetic
	leftType := left.DataType().Name()
	rightType := right.DataType().Name()
	promotedType := e.getPromotedType(leftType, rightType)

	// Convert both operands to the promoted type and perform arithmetic
	leftConverted, err := e.convertToType(left, promotedType)
	if err != nil {
		return nil, fmt.Errorf("converting left operand to %s: %w", promotedType, err)
	}
	defer leftConverted.Release()

	rightConverted, err := e.convertToType(right, promotedType)
	if err != nil {
		return nil, fmt.Errorf("converting right operand to %s: %w", promotedType, err)
	}
	defer rightConverted.Release()

	// Perform arithmetic on the promoted type
	switch promotedType {
	case typeInt32:
		leftInt32, ok1 := leftConverted.(*array.Int32)
		rightInt32, ok2 := rightConverted.(*array.Int32)
		if !ok1 || !ok2 {
			return nil, errors.New("type assertion failed for int32 arithmetic")
		}
		return e.evaluateInt32Arithmetic(leftInt32, rightInt32, op)
	case typeInt64:
		leftInt64, ok1 := leftConverted.(*array.Int64)
		rightInt64, ok2 := rightConverted.(*array.Int64)
		if !ok1 || !ok2 {
			return nil, errors.New("type assertion failed for int64 arithmetic")
		}
		return e.evaluateInt64Arithmetic(leftInt64, rightInt64, op)
	case typeFloat32:
		leftFloat32, ok1 := leftConverted.(*array.Float32)
		rightFloat32, ok2 := rightConverted.(*array.Float32)
		if !ok1 || !ok2 {
			return nil, errors.New("type assertion failed for float32 arithmetic")
		}
		return e.evaluateFloat32Arithmetic(leftFloat32, rightFloat32, op)
	case typeFloat64:
		leftFloat64, ok1 := leftConverted.(*array.Float64)
		rightFloat64, ok2 := rightConverted.(*array.Float64)
		if !ok1 || !ok2 {
			return nil, errors.New("type assertion failed for float64 arithmetic")
		}
		return e.evaluateFloat64Arithmetic(leftFloat64, rightFloat64, op)
	default:
		return nil, fmt.Errorf("unsupported promoted type for arithmetic: %s", promotedType)
	}
}

func (e *Evaluator) evaluateInt64Arithmetic(left, right *array.Int64, op BinaryOp) (arrow.Array, error) {
	return evaluateArithmetic(e.mem, left, right, op, array.NewInt64Builder)
}

func (e *Evaluator) evaluateFloat64Arithmetic(left, right *array.Float64, op BinaryOp) (arrow.Array, error) {
	return evaluateArithmetic(e.mem, left, right, op, array.NewFloat64Builder)
}

// evaluateArithmetic is a generic function for arithmetic operations on numeric arrays.
func evaluateArithmetic[T constraints.Signed | constraints.Float, A ArithmeticArray[T], B ArithmeticBuilder[T]](
	mem memory.Allocator, left, right A, op BinaryOp, newBuilder func(memory.Allocator) B,
) (arrow.Array, error) {
	builder := newBuilder(mem)
	defer builder.Release()

	for i := range left.Len() {
		if left.IsNull(i) || right.IsNull(i) {
			builder.AppendNull()
			continue
		}

		l := left.Value(i)
		r := right.Value(i)

		var result T
		switch op {
		case OpAdd:
			result = l + r
		case OpSub:
			result = l - r
		case OpMul:
			result = l * r
		case OpDiv:
			// Handle division by zero for integer types
			if r == 0 {
				if isIntegerType(result) {
					builder.AppendNull()
					continue
				}
			}
			result = l / r
		case OpEq, OpNe, OpLt, OpLe, OpGt, OpGe, OpAnd, OpOr:
			return nil, fmt.Errorf("operation %v is not supported for arithmetic evaluation", op)
		default:
			return nil, fmt.Errorf("unsupported arithmetic operation: %v", op)
		}

		builder.Append(result)
	}

	return builder.NewArray(), nil
}

// ArithmeticArray defines interface for arrays supporting arithmetic operations.
type ArithmeticArray[T any] interface {
	Len() int
	IsNull(int) bool
	Value(int) T
}

type ArithmeticBuilder[T any] interface {
	Release()
	AppendNull()
	Append(T)
	NewArray() arrow.Array
}

// Helper function to check if a type is integer.
func isIntegerType[T any](val T) bool {
	switch any(val).(type) {
	case int32, int64:
		return true
	default:
		return false
	}
}

func (e *Evaluator) evaluateComparison(left, right arrow.Array, op BinaryOp) (arrow.Array, error) {
	// For numeric types, use type coercion
	leftType := left.DataType().Name()
	rightType := right.DataType().Name()

	// Check if both types are numeric
	if e.isNumericType(leftType) && e.isNumericType(rightType) {
		return e.evaluateNumericComparison(left, right, op)
	}

	// Handle same-type comparisons for non-numeric types
	switch leftArr := left.(type) {
	case *array.String:
		if rightArr, ok := right.(*array.String); ok {
			return e.evaluateStringComparison(leftArr, rightArr, op)
		}
	case *array.Boolean:
		if rightArr, ok := right.(*array.Boolean); ok {
			return e.evaluateBooleanComparison(leftArr, rightArr, op)
		}
	}

	return nil, fmt.Errorf("unsupported comparison between %T and %T", left, right)
}

// isNumericType checks if a type name represents a numeric type.
func (e *Evaluator) isNumericType(typeName string) bool {
	switch typeName {
	case typeInt32, typeInt64, typeFloat32, typeFloat64, typeDouble:
		return true
	default:
		return false
	}
}

// evaluateNumericComparison handles comparisons between numeric types with coercion.
func (e *Evaluator) evaluateNumericComparison(left, right arrow.Array, op BinaryOp) (arrow.Array, error) {
	// Determine the promoted type for comparison
	leftType := left.DataType().Name()
	rightType := right.DataType().Name()
	promotedType := e.getPromotedType(leftType, rightType)

	// Convert both operands to the promoted type
	leftConverted, err := e.convertToType(left, promotedType)
	if err != nil {
		return nil, fmt.Errorf("converting left operand to %s: %w", promotedType, err)
	}
	defer leftConverted.Release()

	rightConverted, err := e.convertToType(right, promotedType)
	if err != nil {
		return nil, fmt.Errorf("converting right operand to %s: %w", promotedType, err)
	}
	defer rightConverted.Release()

	// Perform comparison on the promoted type
	switch promotedType {
	case typeInt32:
		leftInt32, ok1 := leftConverted.(*array.Int32)
		rightInt32, ok2 := rightConverted.(*array.Int32)
		if !ok1 || !ok2 {
			return nil, errors.New("type assertion failed for int32 comparison")
		}
		return e.evaluateInt32Comparison(leftInt32, rightInt32, op)
	case typeInt64:
		leftInt64, ok1 := leftConverted.(*array.Int64)
		rightInt64, ok2 := rightConverted.(*array.Int64)
		if !ok1 || !ok2 {
			return nil, errors.New("type assertion failed for int64 comparison")
		}
		return e.evaluateInt64Comparison(leftInt64, rightInt64, op)
	case typeFloat32:
		leftFloat32, ok1 := leftConverted.(*array.Float32)
		rightFloat32, ok2 := rightConverted.(*array.Float32)
		if !ok1 || !ok2 {
			return nil, errors.New("type assertion failed for float32 comparison")
		}
		return e.evaluateFloat32Comparison(leftFloat32, rightFloat32, op)
	case "float64":
		leftFloat64, ok1 := leftConverted.(*array.Float64)
		rightFloat64, ok2 := rightConverted.(*array.Float64)
		if !ok1 || !ok2 {
			return nil, errors.New("type assertion failed for float64 comparison")
		}
		return e.evaluateFloat64Comparison(leftFloat64, rightFloat64, op)
	default:
		return nil, fmt.Errorf("unsupported promoted type for comparison: %s", promotedType)
	}
}

// compareValues performs comparison operation on two comparable values.
func compareValues[T cmp.Ordered](left, right T, op BinaryOp) (bool, error) {
	switch op {
	case OpEq:
		return left == right, nil
	case OpNe:
		return left != right, nil
	case OpLt:
		return left < right, nil
	case OpLe:
		return left <= right, nil
	case OpGt:
		return left > right, nil
	case OpGe:
		return left >= right, nil
	case OpAdd, OpSub, OpMul, OpDiv:
		return false, fmt.Errorf("operation %v is not supported for comparison evaluation", op)
	case OpAnd, OpOr:
		return false, fmt.Errorf("operation %v is not supported for comparison evaluation", op)
	default:
		return false, fmt.Errorf("unsupported comparison operation: %v", op)
	}
}

func (e *Evaluator) evaluateInt64Comparison(left, right *array.Int64, op BinaryOp) (arrow.Array, error) {
	builder := array.NewBooleanBuilder(e.mem)
	defer builder.Release()

	for i := range left.Len() {
		if left.IsNull(i) || right.IsNull(i) {
			builder.AppendNull()
			continue
		}

		result, err := compareValues(left.Value(i), right.Value(i), op)
		if err != nil {
			return nil, err
		}
		builder.Append(result)
	}

	return builder.NewArray(), nil
}

func (e *Evaluator) evaluateFloat64Comparison(left, right *array.Float64, op BinaryOp) (arrow.Array, error) {
	builder := array.NewBooleanBuilder(e.mem)
	defer builder.Release()

	for i := range left.Len() {
		if left.IsNull(i) || right.IsNull(i) {
			builder.AppendNull()
			continue
		}

		result, err := compareValues(left.Value(i), right.Value(i), op)
		if err != nil {
			return nil, err
		}
		builder.Append(result)
	}

	return builder.NewArray(), nil
}

func (e *Evaluator) evaluateStringComparison(left, right *array.String, op BinaryOp) (arrow.Array, error) {
	builder := array.NewBooleanBuilder(e.mem)
	defer builder.Release()

	for i := range left.Len() {
		if left.IsNull(i) || right.IsNull(i) {
			builder.AppendNull()
			continue
		}

		result, err := compareValues(left.Value(i), right.Value(i), op)
		if err != nil {
			return nil, err
		}
		builder.Append(result)
	}

	return builder.NewArray(), nil
}

func (e *Evaluator) evaluateBooleanComparison(left, right *array.Boolean, op BinaryOp) (arrow.Array, error) {
	builder := array.NewBooleanBuilder(e.mem)
	defer builder.Release()

	for i := range left.Len() {
		if left.IsNull(i) || right.IsNull(i) {
			builder.AppendNull()
			continue
		}

		l := left.Value(i)
		r := right.Value(i)

		var result bool
		switch op {
		case OpEq:
			result = l == r
		case OpNe:
			result = l != r
		case OpLt, OpLe, OpGt, OpGe:
			return nil, fmt.Errorf("operation %v is not supported for boolean comparison", op)
		case OpAdd, OpSub, OpMul, OpDiv:
			return nil, fmt.Errorf("operation %v is not supported for boolean comparison", op)
		case OpAnd, OpOr:
			return nil, fmt.Errorf("operation %v is not supported for boolean comparison", op)
		default:
			return nil, fmt.Errorf("unsupported boolean comparison operation: %v", op)
		}

		builder.Append(result)
	}

	return builder.NewArray(), nil
}

func (e *Evaluator) evaluateLogical(left, right arrow.Array, op BinaryOp) (arrow.Array, error) {
	leftBool, ok1 := left.(*array.Boolean)
	rightBool, ok2 := right.(*array.Boolean)

	if !ok1 || !ok2 {
		return nil, errors.New("logical operations require boolean operands")
	}

	builder := array.NewBooleanBuilder(e.mem)
	defer builder.Release()

	for i := range left.Len() {
		if leftBool.IsNull(i) || rightBool.IsNull(i) {
			builder.AppendNull()
			continue
		}

		l := leftBool.Value(i)
		r := rightBool.Value(i)

		var result bool
		switch op {
		case OpAnd:
			result = l && r
		case OpOr:
			result = l || r
		case OpAdd, OpSub, OpMul, OpDiv:
			return nil, fmt.Errorf("operation %v is not supported for logical evaluation", op)
		case OpEq, OpNe, OpLt, OpLe, OpGt, OpGe:
			return nil, fmt.Errorf("operation %v is not supported for logical evaluation", op)
		default:
			return nil, fmt.Errorf("unsupported logical operation: %v", op)
		}

		builder.Append(result)
	}

	return builder.NewArray(), nil
}

// evaluateStringLiteral creates a string array with the literal value repeated.
func (e *Evaluator) evaluateStringLiteral(val string, length int) (arrow.Array, error) {
	builder := array.NewStringBuilder(e.mem)
	defer builder.Release()
	for range length {
		builder.Append(val)
	}
	return builder.NewArray(), nil
}

// evaluateInt32Literal creates an int32 array with the literal value repeated.
func (e *Evaluator) evaluateInt32Literal(val int32, length int) (arrow.Array, error) {
	builder := array.NewInt32Builder(e.mem)
	defer builder.Release()
	for range length {
		builder.Append(val)
	}
	return builder.NewArray(), nil
}

// evaluateInt64Literal creates an int64 array with the literal value repeated.
func (e *Evaluator) evaluateInt64Literal(val int64, length int) (arrow.Array, error) {
	builder := array.NewInt64Builder(e.mem)
	defer builder.Release()
	for range length {
		builder.Append(val)
	}
	return builder.NewArray(), nil
}

// evaluateIntLiteral creates an int64 array from a platform-dependent int value.
func (e *Evaluator) evaluateIntLiteral(val int, length int) (arrow.Array, error) {
	// Handle platform-dependent int type by converting to int64
	builder := array.NewInt64Builder(e.mem)
	defer builder.Release()
	for range length {
		builder.Append(int64(val))
	}
	return builder.NewArray(), nil
}

// evaluateFloat32Literal creates a float32 array with the literal value repeated.
func (e *Evaluator) evaluateFloat32Literal(val float32, length int) (arrow.Array, error) {
	builder := array.NewFloat32Builder(e.mem)
	defer builder.Release()
	for range length {
		builder.Append(val)
	}
	return builder.NewArray(), nil
}

// evaluateFloat64Literal creates a float64 array with the literal value repeated.
func (e *Evaluator) evaluateFloat64Literal(val float64, length int) (arrow.Array, error) {
	builder := array.NewFloat64Builder(e.mem)
	defer builder.Release()
	for range length {
		builder.Append(val)
	}
	return builder.NewArray(), nil
}

// evaluateBooleanLiteral creates a boolean array with the literal value repeated.
func (e *Evaluator) evaluateBooleanLiteral(val bool, length int) (arrow.Array, error) {
	builder := array.NewBooleanBuilder(e.mem)
	defer builder.Release()
	for range length {
		builder.Append(val)
	}
	return builder.NewArray(), nil
}

// evaluateTimeLiteral creates a timestamp array from a time.Time value.
func (e *Evaluator) evaluateTimeLiteral(val time.Time, length int) (arrow.Array, error) {
	// Create a timestamp array from time.Time
	timestampType := &arrow.TimestampType{Unit: arrow.Nanosecond}
	builder := array.NewTimestampBuilder(e.mem, timestampType)
	defer builder.Release()
	for range length {
		builder.Append(arrow.Timestamp(val.UnixNano()))
	}
	return builder.NewArray(), nil
}

func (e *Evaluator) getArrayLength(columns map[string]arrow.Array) int {
	for _, arr := range columns {
		return arr.Len()
	}
	return 0
}

// getPromotedType determines the promoted type for mixed arithmetic operations.
func (e *Evaluator) getPromotedType(leftType, rightType string) string {
	// Handle Arrow type names (double = float64)
	if leftType == typeDouble {
		leftType = typeFloat64
	}
	if rightType == typeDouble {
		rightType = typeFloat64
	}

	// Type promotion hierarchy for arithmetic operations
	typeHierarchy := map[string]int{
		typeInt32:   levelInt32,
		typeInt64:   levelInt64,
		typeFloat32: levelFloat32,
		typeFloat64: levelFloat64,
		typeDouble:  levelDouble, // Arrow uses "double" for float64
	}

	leftLevel, leftExists := typeHierarchy[leftType]
	rightLevel, rightExists := typeHierarchy[rightType]

	if !leftExists || !rightExists {
		// If either type is not in our hierarchy, return the original types
		if leftExists {
			return leftType
		}
		if rightExists {
			return rightType
		}
		return leftType
	}

	// Special case: int64 + float32 should promote to float64 for precision
	if (leftType == typeInt64 && rightType == typeFloat32) || (leftType == typeFloat32 && rightType == typeInt64) {
		return typeFloat64
	}

	// Return the higher type in the hierarchy
	promotedType := leftType
	if rightLevel > leftLevel {
		promotedType = rightType
	}

	return promotedType
}

// convertToType converts an Arrow array to the target type.
func (e *Evaluator) convertToType(arr arrow.Array, targetType string) (arrow.Array, error) {
	sourceType := arr.DataType().Name()

	// Handle double = float64 equivalence
	if sourceType == typeDouble {
		sourceType = typeFloat64
	}
	if targetType == typeDouble {
		targetType = typeFloat64
	}

	// If already the target type, return a retained copy
	if sourceType == targetType {
		arr.Retain()
		return arr, nil
	}

	// Type conversion matrix
	switch sourceType {
	case typeInt32:
		int32Array, ok := arr.(*array.Int32)
		if !ok {
			return nil, errors.New("type assertion failed for int32 conversion")
		}
		return e.convertInt32ToType(int32Array, targetType)
	case typeInt64:
		int64Array, ok := arr.(*array.Int64)
		if !ok {
			return nil, errors.New("type assertion failed for int64 conversion")
		}
		return e.convertInt64ToType(int64Array, targetType)
	case typeFloat32:
		float32Array, ok := arr.(*array.Float32)
		if !ok {
			return nil, errors.New("type assertion failed for float32 conversion")
		}
		return e.convertFloat32ToType(float32Array, targetType)
	case typeFloat64, typeDouble:
		float64Array, ok := arr.(*array.Float64)
		if !ok {
			return nil, errors.New("type assertion failed for float64 conversion")
		}
		return e.convertFloat64ToType(float64Array, targetType)
	default:
		return nil, fmt.Errorf("unsupported source type for conversion: %s", sourceType)
	}
}

// Type conversion methods.

// convertIntegerToType is a generic function to convert integer arrays to different numeric types.
// T represents the source integer type (int32 or int64), and arr is the source array.
func convertIntegerToType[T constraints.Signed](mem memory.Allocator, arr interface {
	Len() int
	IsNull(int) bool
	Value(int) T
}, sourceType, targetType string) (arrow.Array, error) {
	switch targetType {
	case typeInt32:
		return convertToInt32Array(mem, arr)
	case typeInt64:
		return convertToInt64Array(mem, arr)
	case typeFloat32:
		return convertToFloat32Array(mem, arr)
	case typeFloat64, typeDouble:
		return convertToFloat64Array(mem, arr)
	default:
		return nil, fmt.Errorf("cannot convert %s to %s", sourceType, targetType)
	}
}

// convertToInt32Array converts a generic integer array to int32 array.
func convertToInt32Array[T constraints.Signed](mem memory.Allocator, arr interface {
	Len() int
	IsNull(int) bool
	Value(int) T
}) (arrow.Array, error) {
	builder := array.NewInt32Builder(mem)
	defer builder.Release()
	for i := range arr.Len() {
		if arr.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(int32(arr.Value(i))) // #nosec G115 Note: potential overflow is expected in type conversion
		}
	}
	return builder.NewArray(), nil
}

// convertToInt64Array converts a generic integer array to int64 array.
func convertToInt64Array[T constraints.Signed](mem memory.Allocator, arr interface {
	Len() int
	IsNull(int) bool
	Value(int) T
}) (arrow.Array, error) {
	builder := array.NewInt64Builder(mem)
	defer builder.Release()
	for i := range arr.Len() {
		if arr.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(int64(arr.Value(i)))
		}
	}
	return builder.NewArray(), nil
}

// convertToFloat32Array converts a generic integer array to float32 array.
func convertToFloat32Array[T constraints.Signed](mem memory.Allocator, arr interface {
	Len() int
	IsNull(int) bool
	Value(int) T
}) (arrow.Array, error) {
	builder := array.NewFloat32Builder(mem)
	defer builder.Release()
	for i := range arr.Len() {
		if arr.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(float32(arr.Value(i)))
		}
	}
	return builder.NewArray(), nil
}

// convertToFloat64Array converts a generic integer array to float64 array.
func convertToFloat64Array[T constraints.Signed](mem memory.Allocator, arr interface {
	Len() int
	IsNull(int) bool
	Value(int) T
}) (arrow.Array, error) {
	builder := array.NewFloat64Builder(mem)
	defer builder.Release()
	for i := range arr.Len() {
		if arr.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(float64(arr.Value(i)))
		}
	}
	return builder.NewArray(), nil
}

func (e *Evaluator) convertInt32ToType(arr *array.Int32, targetType string) (arrow.Array, error) {
	return convertIntegerToType(e.mem, arr, typeInt32, targetType)
}

func (e *Evaluator) convertInt64ToType(arr *array.Int64, targetType string) (arrow.Array, error) {
	return convertIntegerToType(e.mem, arr, typeInt64, targetType)
}

func (e *Evaluator) convertFloat32ToType(arr *array.Float32, targetType string) (arrow.Array, error) {
	switch targetType {
	case typeInt32:
		builder := array.NewInt32Builder(e.mem)
		defer builder.Release()
		for i := range arr.Len() {
			if arr.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(int32(arr.Value(i))) // Note: truncation
			}
		}
		return builder.NewArray(), nil
	case typeInt64:
		builder := array.NewInt64Builder(e.mem)
		defer builder.Release()
		for i := range arr.Len() {
			if arr.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(int64(arr.Value(i))) // Note: truncation
			}
		}
		return builder.NewArray(), nil
	case typeFloat64, typeDouble:
		builder := array.NewFloat64Builder(e.mem)
		defer builder.Release()
		for i := range arr.Len() {
			if arr.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(float64(arr.Value(i)))
			}
		}
		return builder.NewArray(), nil
	default:
		return nil, fmt.Errorf("cannot convert float32 to %s", targetType)
	}
}

func (e *Evaluator) convertFloat64ToType(arr *array.Float64, targetType string) (arrow.Array, error) {
	switch targetType {
	case typeInt32:
		builder := array.NewInt32Builder(e.mem)
		defer builder.Release()
		for i := range arr.Len() {
			if arr.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(int32(arr.Value(i))) // Note: truncation
			}
		}
		return builder.NewArray(), nil
	case typeInt64:
		builder := array.NewInt64Builder(e.mem)
		defer builder.Release()
		for i := range arr.Len() {
			if arr.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(int64(arr.Value(i))) // Note: truncation
			}
		}
		return builder.NewArray(), nil
	case typeFloat32:
		builder := array.NewFloat32Builder(e.mem)
		defer builder.Release()
		for i := range arr.Len() {
			if arr.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(float32(arr.Value(i))) // Note: potential precision loss
			}
		}
		return builder.NewArray(), nil
	default:
		return nil, fmt.Errorf("cannot convert float64 to %s", targetType)
	}
}

// Additional arithmetic methods for int32 and float32.
func (e *Evaluator) evaluateInt32Arithmetic(left, right *array.Int32, op BinaryOp) (arrow.Array, error) {
	return evaluateArithmetic(e.mem, left, right, op, array.NewInt32Builder)
}

func (e *Evaluator) evaluateFloat32Arithmetic(left, right *array.Float32, op BinaryOp) (arrow.Array, error) {
	return evaluateArithmetic(e.mem, left, right, op, array.NewFloat32Builder)
}

// Additional comparison methods for int32 and float32.
func (e *Evaluator) evaluateInt32Comparison(left, right *array.Int32, op BinaryOp) (arrow.Array, error) {
	builder := array.NewBooleanBuilder(e.mem)
	defer builder.Release()

	for i := range left.Len() {
		if left.IsNull(i) || right.IsNull(i) {
			builder.AppendNull()
			continue
		}

		l := left.Value(i)
		r := right.Value(i)

		result, err := compareValues(l, r, op)
		if err != nil {
			return nil, err
		}

		builder.Append(result)
	}

	return builder.NewArray(), nil
}

func (e *Evaluator) evaluateFloat32Comparison(left, right *array.Float32, op BinaryOp) (arrow.Array, error) {
	builder := array.NewBooleanBuilder(e.mem)
	defer builder.Release()

	for i := range left.Len() {
		if left.IsNull(i) || right.IsNull(i) {
			builder.AppendNull()
			continue
		}

		l := left.Value(i)
		r := right.Value(i)

		result, err := compareValues(l, r, op)
		if err != nil {
			return nil, err
		}

		builder.Append(result)
	}

	return builder.NewArray(), nil
}

// evaluateFunction evaluates a function expression.
func (e *Evaluator) evaluateFunction(expr *FunctionExpr, columns map[string]arrow.Array) (arrow.Array, error) {
	switch expr.name {
	case "year":
		return e.evaluateDateTimeFunction(expr, columns, extractYear)
	case "month":
		return e.evaluateDateTimeFunction(expr, columns, extractMonth)
	case "day":
		return e.evaluateDateTimeFunction(expr, columns, extractDay)
	case "hour":
		return e.evaluateDateTimeFunction(expr, columns, extractHour)
	case "minute":
		return e.evaluateDateTimeFunction(expr, columns, extractMinute)
	case "second":
		return e.evaluateDateTimeFunction(expr, columns, extractSecond)
	case "date_add":
		return e.evaluateDateAdd(expr, columns)
	case "date_sub":
		return e.evaluateDateSub(expr, columns)
	case "date_diff":
		return e.evaluateDateDiff(expr, columns)
	case "if":
		return e.evaluateIfFunction(expr, columns)
	case "concat":
		return e.evaluateConcatFunction(expr, columns)
	default:
		return nil, fmt.Errorf("unsupported function: %s", expr.name)
	}
}

// Date/time extraction function types.
type dateTimeExtractor func(time.Time) int64

func extractYear(t time.Time) int64 {
	return int64(t.Year())
}

func extractMonth(t time.Time) int64 {
	return int64(t.Month())
}

func extractDay(t time.Time) int64 {
	return int64(t.Day())
}

func extractHour(t time.Time) int64 {
	return int64(t.Hour())
}

func extractMinute(t time.Time) int64 {
	return int64(t.Minute())
}

func extractSecond(t time.Time) int64 {
	return int64(t.Second())
}

// evaluateDateTimeFunction evaluates date/time extraction functions.
func (e *Evaluator) evaluateDateTimeFunction(
	expr *FunctionExpr,
	columns map[string]arrow.Array,
	extractor dateTimeExtractor,
) (arrow.Array, error) {
	if len(expr.args) != 1 {
		return nil, fmt.Errorf("date/time function %s requires exactly 1 argument, got %d", expr.name, len(expr.args))
	}

	// Evaluate the argument
	arg, err := e.Evaluate(expr.args[0], columns)
	if err != nil {
		return nil, fmt.Errorf("evaluating argument for %s: %w", expr.name, err)
	}
	defer arg.Release()

	// Check if the argument is a timestamp array
	timestampArr, ok := arg.(*array.Timestamp)
	if !ok {
		return nil, fmt.Errorf("date/time function %s requires a timestamp argument, got %T", expr.name, arg)
	}

	// Build the result array
	builder := array.NewInt64Builder(e.mem)
	defer builder.Release()

	for i := range timestampArr.Len() {
		if timestampArr.IsNull(i) {
			builder.AppendNull()
			continue
		}

		// Convert Arrow timestamp to Go time.Time
		tsValue := timestampArr.Value(i)
		nanos := int64(tsValue)
		t := time.Unix(nanos/nanosPerSecond, nanos%nanosPerSecond).UTC() // Arrow timestamp is in nanoseconds

		// Apply the extractor function
		result := extractor(t)
		builder.Append(result)
	}

	return builder.NewArray(), nil
}

// evaluateDateAdd evaluates DATE_ADD function to add interval to date/time.
func (e *Evaluator) evaluateDateAdd(expr *FunctionExpr, columns map[string]arrow.Array) (arrow.Array, error) {
	return e.evaluateDateArithmetic(expr, columns, "date_add", dateAddArgsCount, true)
}

// evaluateDateSub evaluates DATE_SUB function to subtract interval from date/time.
func (e *Evaluator) evaluateDateSub(expr *FunctionExpr, columns map[string]arrow.Array) (arrow.Array, error) {
	return e.evaluateDateArithmetic(expr, columns, "date_sub", dateSubArgsCount, false)
}

// evaluateDateArithmetic provides generic date arithmetic logic for add/subtract operations.
func (e *Evaluator) evaluateDateArithmetic(
	expr *FunctionExpr,
	columns map[string]arrow.Array,
	funcName string,
	expectedArgCount int,
	isAdd bool,
) (arrow.Array, error) {
	if len(expr.args) != expectedArgCount {
		return nil, fmt.Errorf(
			"%s function requires exactly %d arguments, got %d",
			funcName,
			expectedArgCount,
			len(expr.args),
		)
	}

	// Evaluate the date argument
	dateArg, err := e.Evaluate(expr.args[0], columns)
	if err != nil {
		return nil, fmt.Errorf("evaluating date argument for %s: %w", funcName, err)
	}
	defer dateArg.Release()

	// Check if the date argument is a timestamp array
	timestampArr, ok := dateArg.(*array.Timestamp)
	if !ok {
		return nil, fmt.Errorf("%s function requires a timestamp argument, got %T", funcName, dateArg)
	}

	// Get the interval argument - should be an IntervalExpr
	intervalArg := expr.args[1]
	intervalExpr, ok := intervalArg.(*IntervalExpr)
	if !ok {
		return nil, fmt.Errorf("%s function requires an interval argument, got %T", funcName, intervalArg)
	}

	// Validate the interval type is supported
	if !e.isValidIntervalType(intervalExpr.IntervalType()) {
		return nil, fmt.Errorf("%s function unsupported interval type: %v", funcName, intervalExpr.IntervalType())
	}

	// Build the result array
	builder := array.NewTimestampBuilder(e.mem, &arrow.TimestampType{Unit: arrow.Nanosecond})
	defer builder.Release()

	for i := range timestampArr.Len() {
		if timestampArr.IsNull(i) {
			builder.AppendNull()
			continue
		}

		// Convert Arrow timestamp to Go time.Time
		tsValue := timestampArr.Value(i)
		nanos := int64(tsValue)
		t := time.Unix(nanos/nanosPerSecond, nanos%nanosPerSecond).UTC()

		// Apply the interval operation
		var result time.Time
		if isAdd {
			result = e.addInterval(t, intervalExpr)
		} else {
			result = e.subtractInterval(t, intervalExpr)
		}

		// Convert back to nanoseconds and append
		resultNanos := result.UnixNano()
		builder.Append(arrow.Timestamp(resultNanos))
	}

	return builder.NewArray(), nil
}

// evaluateDateDiff evaluates DATE_DIFF function to calculate difference between dates.
func (e *Evaluator) evaluateDateDiff(expr *FunctionExpr, columns map[string]arrow.Array) (arrow.Array, error) {
	if len(expr.args) != dateDiffArgsCount {
		return nil, fmt.Errorf(
			"date_diff function requires exactly %d arguments, got %d",
			dateDiffArgsCount,
			len(expr.args),
		)
	}

	// Evaluate the start date argument
	startArg, err := e.Evaluate(expr.args[0], columns)
	if err != nil {
		return nil, fmt.Errorf("evaluating start date argument for date_diff: %w", err)
	}
	defer startArg.Release()

	// Evaluate the end date argument
	endArg, err := e.Evaluate(expr.args[1], columns)
	if err != nil {
		return nil, fmt.Errorf("evaluating end date argument for date_diff: %w", err)
	}
	defer endArg.Release()

	// Check if both date arguments are timestamp arrays
	startTimestampArr, ok := startArg.(*array.Timestamp)
	if !ok {
		return nil, fmt.Errorf("date_diff function requires timestamp arguments, got start: %T", startArg)
	}

	endTimestampArr, ok := endArg.(*array.Timestamp)
	if !ok {
		return nil, fmt.Errorf("date_diff function requires timestamp arguments, got end: %T", endArg)
	}

	// Get the unit argument - should be a string literal
	unitArg := expr.args[2]
	unitLiteral, ok := unitArg.(*LiteralExpr)
	if !ok {
		return nil, fmt.Errorf("date_diff function requires a string unit argument, got %T", unitArg)
	}

	unit, ok := unitLiteral.Value().(string)
	if !ok {
		return nil, fmt.Errorf("date_diff function unit must be a string, got %T", unitLiteral.Value())
	}

	// Validate the unit is supported
	if !e.isValidDateDiffUnit(unit) {
		return nil, fmt.Errorf("date_diff function unsupported unit: %s (supported units: %s, %s, %s, %s, %s, %s)",
			unit, unitDays, unitHours, unitMinutes, unitSeconds, unitMonths, unitYears)
	}

	// Ensure both arrays have the same length
	if startTimestampArr.Len() != endTimestampArr.Len() {
		return nil, fmt.Errorf("date_diff function requires arrays of equal length: start=%d, end=%d",
			startTimestampArr.Len(), endTimestampArr.Len())
	}

	// Build the result array
	builder := array.NewInt64Builder(e.mem)
	defer builder.Release()

	for i := range startTimestampArr.Len() {
		if startTimestampArr.IsNull(i) || endTimestampArr.IsNull(i) {
			builder.AppendNull()
			continue
		}

		// Convert Arrow timestamps to Go time.Time
		startNanos := int64(startTimestampArr.Value(i))
		endNanos := int64(endTimestampArr.Value(i))

		startTime := time.Unix(startNanos/nanosPerSecond, startNanos%nanosPerSecond).UTC()
		endTime := time.Unix(endNanos/nanosPerSecond, endNanos%nanosPerSecond).UTC()

		// Calculate the difference based on unit
		diff := e.calculateDateDiff(startTime, endTime, unit)
		builder.Append(diff)
	}

	return builder.NewArray(), nil
}

// addInterval adds an interval to a time value.
func (e *Evaluator) addInterval(t time.Time, interval *IntervalExpr) time.Time {
	value := interval.Value()

	switch interval.IntervalType() {
	case IntervalDays:
		return t.AddDate(0, 0, int(value))
	case IntervalHours:
		return t.Add(time.Duration(value) * time.Hour)
	case IntervalMinutes:
		return t.Add(time.Duration(value) * time.Minute)
	case IntervalMonths:
		return t.AddDate(0, int(value), 0)
	case IntervalYears:
		return t.AddDate(int(value), 0, 0)
	default:
		// This should never happen due to validation, but being explicit
		return t
	}
}

// subtractInterval subtracts an interval from a time value.
func (e *Evaluator) subtractInterval(t time.Time, interval *IntervalExpr) time.Time {
	value := interval.Value()

	switch interval.IntervalType() {
	case IntervalDays:
		return t.AddDate(0, 0, -int(value))
	case IntervalHours:
		return t.Add(-time.Duration(value) * time.Hour)
	case IntervalMinutes:
		return t.Add(-time.Duration(value) * time.Minute)
	case IntervalMonths:
		return t.AddDate(0, -int(value), 0)
	case IntervalYears:
		return t.AddDate(-int(value), 0, 0)
	default:
		// This should never happen due to validation, but being explicit
		return t
	}
}

// isValidDateDiffUnit validates if a date diff unit is supported.
func (e *Evaluator) isValidDateDiffUnit(unit string) bool {
	switch unit {
	case unitDays, unitHours, unitMinutes, unitSeconds, unitMonths, unitYears:
		return true
	default:
		return false
	}
}

// isValidIntervalType validates if an interval type is supported.
func (e *Evaluator) isValidIntervalType(intervalType IntervalType) bool {
	switch intervalType {
	case IntervalDays, IntervalHours, IntervalMinutes, IntervalMonths, IntervalYears:
		return true
	default:
		return false
	}
}

// calculateDateDiff calculates the difference between two dates in the specified unit.
func (e *Evaluator) calculateDateDiff(startTime, endTime time.Time, unit string) int64 {
	switch unit {
	case unitDays:
		diff := endTime.Sub(startTime)
		return int64(diff / (hoursPerDay * time.Hour))
	case unitHours:
		diff := endTime.Sub(startTime)
		return int64(diff / time.Hour)
	case unitMinutes:
		diff := endTime.Sub(startTime)
		return int64(diff / time.Minute)
	case unitSeconds:
		diff := endTime.Sub(startTime)
		return int64(diff / time.Second)
	case unitMonths:
		years := endTime.Year() - startTime.Year()
		months := int(endTime.Month()) - int(startTime.Month())
		totalMonths := years*monthsPerYear + months

		// Adjust if end day is before start day in the month
		if endTime.Day() < startTime.Day() {
			totalMonths--
		}
		return int64(totalMonths)
	case unitYears:
		years := endTime.Year() - startTime.Year()

		// Adjust if end date is before start date in the year
		if endTime.Month() < startTime.Month() ||
			(endTime.Month() == startTime.Month() && endTime.Day() < startTime.Day()) {
			years--
		}
		return int64(years)
	default:
		// This should never happen due to validation, but being explicit
		return 0
	}
}

// EvaluateWindow evaluates a window expression.
func (e *Evaluator) EvaluateWindow(expr *WindowExpr, columns map[string]arrow.Array) (arrow.Array, error) {
	// Get the data length from one of the columns
	if len(columns) == 0 {
		return nil, errors.New("no columns provided for window function evaluation")
	}

	var dataLength int
	for _, arr := range columns {
		if arr != nil {
			dataLength = arr.Len()
			break
		}
	}

	// Handle empty datasets - return empty result array
	if dataLength == 0 {
		// Create an empty result array of the appropriate type
		builder := array.NewInt64Builder(e.mem)
		defer builder.Release()
		return builder.NewArray(), nil
	}

	// Handle different types of window functions
	switch fn := expr.function.(type) {
	case *WindowFunctionExpr:
		return e.evaluateWindowFunction(fn, expr.window, columns)
	case *AggregationExpr:
		return e.evaluateWindowAggregation(fn, expr.window, columns)
	default:
		return nil, fmt.Errorf("unsupported window function type: %T", expr.function)
	}
}

// evaluateWindowFunction evaluates window-specific functions (ROW_NUMBER, RANK, LAG, LEAD, etc.)
func (e *Evaluator) evaluateWindowFunction(
	expr *WindowFunctionExpr,
	window *WindowSpec,
	columns map[string]arrow.Array,
) (arrow.Array, error) {
	dataLength := getDataLength(columns)

	// Try parallel execution for supported functions
	switch expr.funcName {
	case "ROW_NUMBER":
		return e.evaluateRowNumber(window, columns, dataLength)
	case "RANK":
		// Try parallel execution first, fall back to sequential if needed
		if e.shouldUseWindowParallelExecution(window, columns, dataLength) {
			return e.evaluateRankParallel(window, columns, dataLength)
		}
		return e.evaluateRank(window, columns, dataLength)
	case "DENSE_RANK":
		// Try parallel execution first, fall back to sequential if needed
		if e.shouldUseWindowParallelExecution(window, columns, dataLength) {
			return e.evaluateDenseRankParallel(window, columns, dataLength)
		}
		return e.evaluateDenseRank(window, columns, dataLength)
	case "LAG":
		// Try parallel execution first, fall back to sequential if needed
		if e.shouldUseWindowParallelExecution(window, columns, dataLength) {
			return e.evaluateLagParallel(expr, window, columns, dataLength)
		}
		return e.evaluateLag(expr, window, columns, dataLength)
	case "LEAD":
		// Try parallel execution first, fall back to sequential if needed
		if e.shouldUseWindowParallelExecution(window, columns, dataLength) {
			return e.evaluateLeadParallel(expr, window, columns, dataLength)
		}
		return e.evaluateLead(expr, window, columns, dataLength)
	case "FIRST_VALUE":
		return e.evaluateFirstValue(expr, window, columns, dataLength)
	case "LAST_VALUE":
		return e.evaluateLastValue(expr, window, columns, dataLength)
	case "PERCENT_RANK":
		return e.evaluatePercentRank(expr, window, columns, dataLength)
	case "CUME_DIST":
		return e.evaluateCumeDist(expr, window, columns, dataLength)
	case "NTH_VALUE":
		return e.evaluateNthValue(expr, window, columns, dataLength)
	case "NTILE":
		return e.evaluateNtile(expr, window, columns, dataLength)
	default:
		return nil, fmt.Errorf("unsupported window function: %s", expr.funcName)
	}
}

// evaluateWindowAggregation evaluates aggregation functions with OVER clause.
func (e *Evaluator) evaluateWindowAggregation(
	expr *AggregationExpr,
	window *WindowSpec,
	columns map[string]arrow.Array,
) (arrow.Array, error) {
	dataLength := getDataLength(columns)

	switch expr.aggType {
	case AggSum:
		return e.evaluateWindowSum(expr, window, columns, dataLength)
	case AggCount:
		return e.evaluateWindowCount(expr, window, columns, dataLength)
	case AggMean:
		return e.evaluateWindowMean(expr, window, columns, dataLength)
	case AggMin:
		return e.evaluateWindowMin(expr, window, columns, dataLength)
	case AggMax:
		return e.evaluateWindowMax(expr, window, columns, dataLength)
	default:
		return nil, fmt.Errorf("unsupported window aggregation: %v", expr.aggType)
	}
}

// evaluateRowNumber implements ROW_NUMBER() window function.
func (e *Evaluator) evaluateRowNumber(
	window *WindowSpec,
	columns map[string]arrow.Array,
	dataLength int,
) (arrow.Array, error) {
	// Get partitions
	partitions, err := e.getPartitions(window, columns, dataLength)
	if err != nil {
		return nil, fmt.Errorf("getting partitions: %w", err)
	}

	// Create result array
	builder := array.NewInt64Builder(e.mem)
	defer builder.Release()

	result := make([]int64, dataLength)

	// Process each partition
	for _, partition := range partitions {
		// Sort partition if ORDER BY is specified
		sortedIndices := partition
		if len(window.orderBy) > 0 {
			sortedIndices = e.sortPartition(partition, window.orderBy, columns)
		}

		// Assign row numbers within partition
		for i, idx := range sortedIndices {
			result[idx] = int64(i + 1)
		}
	}

	// Build the result array
	for i := range dataLength {
		builder.Append(result[i])
	}

	return builder.NewArray(), nil
}

// Helper functions

func getDataLength(columns map[string]arrow.Array) int {
	for _, arr := range columns {
		return arr.Len()
	}
	return 0
}

// getPartitions creates partitions based on PARTITION BY clause.
func (e *Evaluator) getPartitions(window *WindowSpec, columns map[string]arrow.Array, dataLength int) ([][]int, error) {
	if len(window.partitionBy) == 0 {
		// No partitioning, single partition with all rows
		partition := make([]int, dataLength)
		for i := range dataLength {
			partition[i] = i
		}
		return [][]int{partition}, nil
	}

	// Group by partition columns
	partitionMap := make(map[string][]int)

	for i := range dataLength {
		key, err := e.getPartitionKey(i, window.partitionBy, columns)
		if err != nil {
			return nil, fmt.Errorf("getting partition key for row %d: %w", i, err)
		}
		partitionMap[key] = append(partitionMap[key], i)
	}

	// Convert map to slice
	partitions := make([][]int, 0, len(partitionMap))
	for _, partition := range partitionMap {
		partitions = append(partitions, partition)
	}

	return partitions, nil
}

// getPartitionKey creates a string key for partitioning.
func (e *Evaluator) getPartitionKey(
	rowIndex int,
	partitionColumns []string,
	columns map[string]arrow.Array,
) (string, error) {
	key := ""
	for i, colName := range partitionColumns {
		if i > 0 {
			key += "|"
		}

		arr, exists := columns[colName]
		if !exists {
			return "", fmt.Errorf("partition column not found: %s", colName)
		}

		if arr.IsNull(rowIndex) {
			key += "NULL"
		} else {
			switch a := arr.(type) {
			case *array.String:
				key += a.Value(rowIndex)
			case *array.Int64:
				key += strconv.FormatInt(a.Value(rowIndex), 10)
			case *array.Float64:
				key += fmt.Sprintf("%g", a.Value(rowIndex))
			case *array.Boolean:
				key += strconv.FormatBool(a.Value(rowIndex))
			default:
				key += fmt.Sprintf("%v", arr)
			}
		}
	}
	return key, nil
}

// evaluateCaseWithContext evaluates a CASE expression.
func (e *Evaluator) evaluateCaseWithContext(
	_ *CaseExpr,
	_ map[string]arrow.Array,
	_ EvaluationContext,
) (arrow.Array, error) {
	return nil, errors.New("CASE expressions not yet implemented")
}

// evaluateIfFunction evaluates an IF function (condition, thenValue, elseValue).
func (e *Evaluator) evaluateIfFunction(expr *FunctionExpr, columns map[string]arrow.Array) (arrow.Array, error) {
	const ifFunctionArgCount = 3
	if len(expr.args) != ifFunctionArgCount {
		return nil, fmt.Errorf("IF function requires exactly %d arguments, got %d", ifFunctionArgCount, len(expr.args))
	}

	// Evaluate all arguments
	condition, err := e.Evaluate(expr.args[0], columns)
	if err != nil {
		return nil, fmt.Errorf("evaluating IF condition: %w", err)
	}
	defer condition.Release()

	thenValue, err := e.Evaluate(expr.args[1], columns)
	if err != nil {
		return nil, fmt.Errorf("evaluating IF then value: %w", err)
	}
	defer thenValue.Release()

	elseValue, err := e.Evaluate(expr.args[2], columns)
	if err != nil {
		return nil, fmt.Errorf("evaluating IF else value: %w", err)
	}
	defer elseValue.Release()

	// Check condition is boolean
	conditionBool, ok := condition.(*array.Boolean)
	if !ok {
		return nil, fmt.Errorf("IF condition must be boolean, got %T", condition)
	}

	// Validate that both then and else values are of the same type
	if thenValue.DataType().ID() != elseValue.DataType().ID() {
		return nil, fmt.Errorf("IF function requires then and else values to be of the same type, got %s and %s",
			thenValue.DataType(), elseValue.DataType())
	}

	// For now, implement simple case where both then and else are same type
	// This is enough for basic HAVING clause functionality
	//nolint:exhaustive // We only support a subset of Arrow types for IF function
	switch thenValue.DataType().ID() {
	case arrow.FLOAT64:
		return e.evaluateIfFloat64Simple(conditionBool, thenValue, elseValue)
	case arrow.INT64:
		return e.evaluateIfInt64Simple(conditionBool, thenValue, elseValue)
	case arrow.STRING:
		return e.evaluateIfStringSimple(conditionBool, thenValue, elseValue)
	default:
		return nil, fmt.Errorf("IF function result type %s not yet supported", thenValue.DataType())
	}
}

// evaluateIfFloat64Simple evaluates IF function with float64 result.
func (e *Evaluator) evaluateIfFloat64Simple(
	condition *array.Boolean,
	thenValue, elseValue arrow.Array,
) (arrow.Array, error) {
	thenFloat, ok := thenValue.(*array.Float64)
	if !ok {
		return nil, fmt.Errorf("expected thenValue to be *array.Float64, got %T", thenValue)
	}
	elseFloat, ok := elseValue.(*array.Float64)
	if !ok {
		return nil, fmt.Errorf("expected elseValue to be *array.Float64, got %T", elseValue)
	}

	return e.buildIfFloat64(condition, thenFloat, elseFloat), nil
}

// evaluateIfInt64Simple evaluates IF function with int64 result.
func (e *Evaluator) evaluateIfInt64Simple(
	condition *array.Boolean,
	thenValue, elseValue arrow.Array,
) (arrow.Array, error) {
	thenInt, ok := thenValue.(*array.Int64)
	if !ok {
		return nil, fmt.Errorf("expected thenValue to be *array.Int64, got %T", thenValue)
	}
	elseInt, ok := elseValue.(*array.Int64)
	if !ok {
		return nil, fmt.Errorf("expected elseValue to be *array.Int64, got %T", elseValue)
	}

	return e.buildIfInt64(condition, thenInt, elseInt), nil
}

// evaluateIfStringSimple evaluates IF function with string result.
func (e *Evaluator) evaluateIfStringSimple(
	condition *array.Boolean,
	thenValue, elseValue arrow.Array,
) (arrow.Array, error) {
	thenStr, ok := thenValue.(*array.String)
	if !ok {
		return nil, fmt.Errorf("expected thenValue to be *array.String, got %T", thenValue)
	}
	elseStr, ok := elseValue.(*array.String)
	if !ok {
		return nil, fmt.Errorf("expected elseValue to be *array.String, got %T", elseValue)
	}

	return e.buildIfString(condition, thenStr, elseStr), nil
}

// buildIfFloat64 builds a float64 array based on IF condition logic.
func (e *Evaluator) buildIfFloat64(
	condition *array.Boolean,
	thenValue, elseValue *array.Float64,
) arrow.Array {
	builder := array.NewFloat64Builder(e.mem)
	defer builder.Release()

	for i := range condition.Len() {
		e.appendIfFloat64Value(builder, condition, thenValue, elseValue, i)
	}

	return builder.NewArray()
}

// appendIfFloat64Value appends a float64 value based on IF condition logic.
func (e *Evaluator) appendIfFloat64Value(
	builder *array.Float64Builder,
	condition *array.Boolean,
	thenValue, elseValue *array.Float64,
	i int,
) {
	if condition.IsNull(i) {
		builder.AppendNull()
		return
	}

	if condition.Value(i) {
		if thenValue.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(thenValue.Value(i))
		}
		return
	}

	if elseValue.IsNull(i) {
		builder.AppendNull()
	} else {
		builder.Append(elseValue.Value(i))
	}
}

// appendIfInt64Value appends an int64 value based on IF condition logic.
func (e *Evaluator) appendIfInt64Value(
	builder *array.Int64Builder,
	condition *array.Boolean,
	thenValue, elseValue *array.Int64,
	i int,
) {
	if condition.IsNull(i) {
		builder.AppendNull()
		return
	}

	if condition.Value(i) {
		if thenValue.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(thenValue.Value(i))
		}
		return
	}

	if elseValue.IsNull(i) {
		builder.AppendNull()
	} else {
		builder.Append(elseValue.Value(i))
	}
}

// appendIfStringValue appends a string value based on IF condition logic.
func (e *Evaluator) appendIfStringValue(
	builder *array.StringBuilder,
	condition *array.Boolean,
	thenValue, elseValue *array.String,
	i int,
) {
	if condition.IsNull(i) {
		builder.AppendNull()
		return
	}

	if condition.Value(i) {
		if thenValue.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(thenValue.Value(i))
		}
		return
	}

	if elseValue.IsNull(i) {
		builder.AppendNull()
	} else {
		builder.Append(elseValue.Value(i))
	}
}

// buildIfInt64 builds an int64 array based on IF condition logic.
func (e *Evaluator) buildIfInt64(
	condition *array.Boolean,
	thenValue, elseValue *array.Int64,
) arrow.Array {
	builder := array.NewInt64Builder(e.mem)
	defer builder.Release()

	for i := range condition.Len() {
		e.appendIfInt64Value(builder, condition, thenValue, elseValue, i)
	}

	return builder.NewArray()
}

// buildIfString builds a string array based on IF condition logic.
func (e *Evaluator) buildIfString(
	condition *array.Boolean,
	thenValue, elseValue *array.String,
) arrow.Array {
	builder := array.NewStringBuilder(e.mem)
	defer builder.Release()

	for i := range condition.Len() {
		e.appendIfStringValue(builder, condition, thenValue, elseValue, i)
	}

	return builder.NewArray()
}

// evaluateConcatFunction evaluates a CONCAT function.
func (e *Evaluator) evaluateConcatFunction(expr *FunctionExpr, columns map[string]arrow.Array) (arrow.Array, error) {
	if len(expr.args) == 0 {
		return nil, errors.New("CONCAT function requires at least 1 argument")
	}

	// Evaluate all arguments
	argArrays, err := e.evaluateConcatArguments(expr.args, columns)
	if err != nil {
		return nil, err
	}
	defer e.releaseConcatArguments(argArrays)

	// Validate all arrays have the same length
	if validationErr := e.validateConcatArrayLengths(argArrays); validationErr != nil {
		return nil, validationErr
	}

	// Build result
	return e.buildConcatResult(argArrays)
}

// evaluateConcatArguments evaluates all arguments for CONCAT function.
func (e *Evaluator) evaluateConcatArguments(args []Expr, columns map[string]arrow.Array) ([]arrow.Array, error) {
	argArrays := make([]arrow.Array, len(args))
	for i, arg := range args {
		arr, err := e.Evaluate(arg, columns)
		if err != nil {
			return nil, fmt.Errorf("evaluating CONCAT argument %d: %w", i, err)
		}
		argArrays[i] = arr
	}
	return argArrays, nil
}

// releaseConcatArguments releases all argument arrays.
func (e *Evaluator) releaseConcatArguments(argArrays []arrow.Array) {
	for _, arr := range argArrays {
		arr.Release()
	}
}

// validateConcatArrayLengths validates that all arrays have the same length.
func (e *Evaluator) validateConcatArrayLengths(argArrays []arrow.Array) error {
	if len(argArrays) == 0 {
		return nil
	}

	expectedLen := argArrays[0].Len()
	for i, arr := range argArrays[1:] {
		if arr.Len() != expectedLen {
			return fmt.Errorf("CONCAT argument %d has length %d, expected %d", i+1, arr.Len(), expectedLen)
		}
	}
	return nil
}

// buildConcatResult builds the final concatenated string array.
func (e *Evaluator) buildConcatResult(argArrays []arrow.Array) (arrow.Array, error) {
	if len(argArrays) == 0 {
		return nil, errors.New("no arguments provided for CONCAT")
	}

	builder := array.NewStringBuilder(e.mem)
	defer builder.Release()

	for row := range argArrays[0].Len() {
		result, hasNull := e.concatenateRowValues(argArrays, row)
		if hasNull {
			builder.AppendNull()
		} else {
			builder.Append(result)
		}
	}

	return builder.NewArray(), nil
}

// concatenateRowValues concatenates values from all arrays for a specific row.
func (e *Evaluator) concatenateRowValues(argArrays []arrow.Array, row int) (string, bool) {
	var result string
	for _, arr := range argArrays {
		if arr.IsNull(row) {
			return "", true // hasNull = true
		}

		strValue, err := e.convertArrayValueToString(arr, row)
		if err != nil {
			return "", true // Treat conversion error as null
		}
		result += strValue
	}
	return result, false // hasNull = false
}

// convertArrayValueToString converts an array value at a specific row to string.
func (e *Evaluator) convertArrayValueToString(arr arrow.Array, row int) (string, error) {
	switch typedArr := arr.(type) {
	case *array.String:
		return typedArr.Value(row), nil
	case *array.Int64:
		return strconv.FormatInt(typedArr.Value(row), 10), nil
	case *array.Float64:
		return fmt.Sprintf("%g", typedArr.Value(row)), nil
	default:
		return "", fmt.Errorf("unsupported array type: %T", arr)
	}
}
