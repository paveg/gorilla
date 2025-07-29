package expr

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	dferrors "github.com/paveg/gorilla/internal/errors"
)

// Type name constants
const (
	typeInt32   = "int32"
	typeInt64   = "int64"
	typeFloat32 = "float32"
	typeFloat64 = "float64"
	typeDouble  = "double"

	// Time constants
	nanosPerSecond = 1e9

	// Date/time arithmetic constants
	dateAddArgsCount  = 2
	dateSubArgsCount  = 2
	dateDiffArgsCount = 3
	hoursPerDay       = 24
	monthsPerYear     = 12

	// Date/time unit constants
	unitDays    = "days"
	unitHours   = "hours"
	unitMinutes = "minutes"
	unitSeconds = "seconds"
	unitMonths  = "months"
	unitYears   = "years"
)

// Type hierarchy levels
const (
	levelInt32   = 1
	levelInt64   = 2
	levelFloat32 = 3
	levelFloat64 = 4
	levelDouble  = 4
)

// Evaluator evaluates expressions against Arrow arrays
type Evaluator struct {
	mem memory.Allocator
}

// NewEvaluator creates a new expression evaluator
func NewEvaluator(mem memory.Allocator) *Evaluator {
	if mem == nil {
		mem = memory.NewGoAllocator()
	}
	return &Evaluator{mem: mem}
}

// EvaluateWithContext evaluates an expression with a specific evaluation context
func (e *Evaluator) EvaluateWithContext(expr Expr, columns map[string]arrow.Array, ctx EvaluationContext) (arrow.Array, error) {
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

// EvaluateBooleanWithContext evaluates an expression that should return a boolean array with context
func (e *Evaluator) EvaluateBooleanWithContext(expr Expr, columns map[string]arrow.Array, ctx EvaluationContext) (arrow.Array, error) {
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

// validateContextSupport validates that an expression supports the given context
func (e *Evaluator) validateContextSupport(expr Expr, ctx EvaluationContext) error {
	if contextualExpr, ok := expr.(ContextualExpr); ok {
		if !contextualExpr.SupportsContext(ctx) {
			return fmt.Errorf("expression %s does not support %s context", expr.String(), ctx.String())
		}
	}
	return nil
}

// EvaluateBoolean evaluates an expression that should return a boolean array
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

// evaluateColumnWithContext evaluates a column expression with context
func (e *Evaluator) evaluateColumnWithContext(expr *ColumnExpr, columns map[string]arrow.Array, ctx EvaluationContext) (arrow.Array, error) {
	// Column evaluation is the same in both contexts, but the column names/data may differ
	_ = ctx // Context is validated before this method is called
	return e.evaluateColumn(expr, columns)
}

// evaluateLiteralWithContext evaluates a literal expression with context
func (e *Evaluator) evaluateLiteralWithContext(expr *LiteralExpr, columns map[string]arrow.Array, ctx EvaluationContext) (arrow.Array, error) {
	// Literal evaluation is context-independent
	_ = ctx // Context is validated before this method is called
	return e.evaluateLiteral(expr, columns)
}

// evaluateBinaryWithContext evaluates a binary expression with context
func (e *Evaluator) evaluateBinaryWithContext(expr *BinaryExpr, columns map[string]arrow.Array, ctx EvaluationContext) (arrow.Array, error) {
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

// evaluateBinaryBooleanWithContext evaluates a binary expression that returns boolean with context
func (e *Evaluator) evaluateBinaryBooleanWithContext(expr *BinaryExpr, columns map[string]arrow.Array, ctx EvaluationContext) (arrow.Array, error) {
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
	default:
		return nil, fmt.Errorf("binary operation %v does not produce boolean result", expr.op)
	}
}

// evaluateColumnBooleanWithContext evaluates a column expression that should return boolean with context
func (e *Evaluator) evaluateColumnBooleanWithContext(expr *ColumnExpr, columns map[string]arrow.Array, ctx EvaluationContext) (arrow.Array, error) {
	// Column boolean evaluation is the same in both contexts
	_ = ctx // Context is validated before this method is called
	return e.evaluateColumnBoolean(expr, columns)
}

// evaluateLiteralBooleanWithContext evaluates a literal expression that should return boolean with context
func (e *Evaluator) evaluateLiteralBooleanWithContext(expr *LiteralExpr, columns map[string]arrow.Array, ctx EvaluationContext) (arrow.Array, error) {
	// Literal boolean evaluation is context-independent
	_ = ctx // Context is validated before this method is called
	return e.evaluateLiteralBoolean(expr, columns)
}

// evaluateFunctionWithContext evaluates a function expression with context
func (e *Evaluator) evaluateFunctionWithContext(expr *FunctionExpr, columns map[string]arrow.Array, ctx EvaluationContext) (arrow.Array, error) {
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

// evaluateAggregationWithContext evaluates an aggregation expression with context
func (e *Evaluator) evaluateAggregationWithContext(expr *AggregationExpr, columns map[string]arrow.Array, ctx EvaluationContext) (arrow.Array, error) {
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

// evaluateWindowWithContext evaluates a window expression with context
func (e *Evaluator) evaluateWindowWithContext(expr *WindowExpr, columns map[string]arrow.Array, ctx EvaluationContext) (arrow.Array, error) {
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
// when they appear in contexts where the aggregated value is already computed
func (e *Evaluator) evaluateAggregationValue(expr *AggregationExpr, columns map[string]arrow.Array) (arrow.Array, error) {
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
		return nil, fmt.Errorf("cannot determine array length for literal")
	}

	switch val := expr.value.(type) {
	case string:
		builder := array.NewStringBuilder(e.mem)
		defer builder.Release()
		for i := 0; i < length; i++ {
			builder.Append(val)
		}
		return builder.NewArray(), nil
	case int32:
		builder := array.NewInt32Builder(e.mem)
		defer builder.Release()
		for i := 0; i < length; i++ {
			builder.Append(val)
		}
		return builder.NewArray(), nil
	case int64:
		builder := array.NewInt64Builder(e.mem)
		defer builder.Release()
		for i := 0; i < length; i++ {
			builder.Append(val)
		}
		return builder.NewArray(), nil
	case int:
		// Handle platform-dependent int type by converting to int64
		builder := array.NewInt64Builder(e.mem)
		defer builder.Release()
		for i := 0; i < length; i++ {
			builder.Append(int64(val))
		}
		return builder.NewArray(), nil
	case float32:
		builder := array.NewFloat32Builder(e.mem)
		defer builder.Release()
		for i := 0; i < length; i++ {
			builder.Append(val)
		}
		return builder.NewArray(), nil
	case float64:
		builder := array.NewFloat64Builder(e.mem)
		defer builder.Release()
		for i := 0; i < length; i++ {
			builder.Append(val)
		}
		return builder.NewArray(), nil
	case bool:
		builder := array.NewBooleanBuilder(e.mem)
		defer builder.Release()
		for i := 0; i < length; i++ {
			builder.Append(val)
		}
		return builder.NewArray(), nil
	case time.Time:
		// Create a timestamp array from time.Time
		timestampType := &arrow.TimestampType{Unit: arrow.Nanosecond}
		builder := array.NewTimestampBuilder(e.mem, timestampType)
		defer builder.Release()
		for i := 0; i < length; i++ {
			builder.Append(arrow.Timestamp(val.UnixNano()))
		}
		return builder.NewArray(), nil
	default:
		return nil, fmt.Errorf("unsupported literal type: %T", val)
	}
}

func (e *Evaluator) evaluateLiteralBoolean(expr *LiteralExpr, columns map[string]arrow.Array) (arrow.Array, error) {
	length := e.getArrayLength(columns)
	if length == 0 {
		return nil, fmt.Errorf("cannot determine array length for literal")
	}

	val, ok := expr.value.(bool)
	if !ok {
		return nil, fmt.Errorf("literal is not a boolean: %T", expr.value)
	}

	builder := array.NewBooleanBuilder(e.mem)
	defer builder.Release()
	for i := 0; i < length; i++ {
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
		return e.evaluateInt32Arithmetic(leftConverted.(*array.Int32), rightConverted.(*array.Int32), op)
	case typeInt64:
		return e.evaluateInt64Arithmetic(leftConverted.(*array.Int64), rightConverted.(*array.Int64), op)
	case typeFloat32:
		return e.evaluateFloat32Arithmetic(leftConverted.(*array.Float32), rightConverted.(*array.Float32), op)
	case typeFloat64:
		return e.evaluateFloat64Arithmetic(leftConverted.(*array.Float64), rightConverted.(*array.Float64), op)
	default:
		return nil, fmt.Errorf("unsupported promoted type for arithmetic: %s", promotedType)
	}
}

func (e *Evaluator) evaluateInt64Arithmetic(left, right *array.Int64, op BinaryOp) (arrow.Array, error) {
	builder := array.NewInt64Builder(e.mem)
	defer builder.Release()

	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) || right.IsNull(i) {
			builder.AppendNull()
			continue
		}

		l := left.Value(i)
		r := right.Value(i)

		var result int64
		switch op {
		case OpAdd:
			result = l + r
		case OpSub:
			result = l - r
		case OpMul:
			result = l * r
		case OpDiv:
			if r == 0 {
				builder.AppendNull()
				continue
			}
			result = l / r
		default:
			return nil, fmt.Errorf("unsupported arithmetic operation: %v", op)
		}

		builder.Append(result)
	}

	return builder.NewArray(), nil
}

func (e *Evaluator) evaluateFloat64Arithmetic(left, right *array.Float64, op BinaryOp) (arrow.Array, error) {
	builder := array.NewFloat64Builder(e.mem)
	defer builder.Release()

	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) || right.IsNull(i) {
			builder.AppendNull()
			continue
		}

		l := left.Value(i)
		r := right.Value(i)

		var result float64
		switch op {
		case OpAdd:
			result = l + r
		case OpSub:
			result = l - r
		case OpMul:
			result = l * r
		case OpDiv:
			result = l / r // Division by zero results in +/-Inf, which is handled by Go
		default:
			return nil, fmt.Errorf("unsupported arithmetic operation: %v", op)
		}

		builder.Append(result)
	}

	return builder.NewArray(), nil
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

// isNumericType checks if a type name represents a numeric type
func (e *Evaluator) isNumericType(typeName string) bool {
	switch typeName {
	case typeInt32, typeInt64, typeFloat32, typeFloat64, typeDouble:
		return true
	default:
		return false
	}
}

// evaluateNumericComparison handles comparisons between numeric types with coercion
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
		return e.evaluateInt32Comparison(leftConverted.(*array.Int32), rightConverted.(*array.Int32), op)
	case typeInt64:
		return e.evaluateInt64Comparison(leftConverted.(*array.Int64), rightConverted.(*array.Int64), op)
	case typeFloat32:
		return e.evaluateFloat32Comparison(leftConverted.(*array.Float32), rightConverted.(*array.Float32), op)
	case "float64":
		return e.evaluateFloat64Comparison(leftConverted.(*array.Float64), rightConverted.(*array.Float64), op)
	default:
		return nil, fmt.Errorf("unsupported promoted type for comparison: %s", promotedType)
	}
}

func (e *Evaluator) evaluateInt64Comparison(left, right *array.Int64, op BinaryOp) (arrow.Array, error) {
	builder := array.NewBooleanBuilder(e.mem)
	defer builder.Release()

	for i := 0; i < left.Len(); i++ {
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
		case OpLt:
			result = l < r
		case OpLe:
			result = l <= r
		case OpGt:
			result = l > r
		case OpGe:
			result = l >= r
		default:
			return nil, fmt.Errorf("unsupported comparison operation: %v", op)
		}

		builder.Append(result)
	}

	return builder.NewArray(), nil
}

func (e *Evaluator) evaluateFloat64Comparison(left, right *array.Float64, op BinaryOp) (arrow.Array, error) {
	builder := array.NewBooleanBuilder(e.mem)
	defer builder.Release()

	for i := 0; i < left.Len(); i++ {
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
		case OpLt:
			result = l < r
		case OpLe:
			result = l <= r
		case OpGt:
			result = l > r
		case OpGe:
			result = l >= r
		default:
			return nil, fmt.Errorf("unsupported comparison operation: %v", op)
		}

		builder.Append(result)
	}

	return builder.NewArray(), nil
}

func (e *Evaluator) evaluateStringComparison(left, right *array.String, op BinaryOp) (arrow.Array, error) {
	builder := array.NewBooleanBuilder(e.mem)
	defer builder.Release()

	for i := 0; i < left.Len(); i++ {
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
		case OpLt:
			result = l < r
		case OpLe:
			result = l <= r
		case OpGt:
			result = l > r
		case OpGe:
			result = l >= r
		default:
			return nil, fmt.Errorf("unsupported comparison operation: %v", op)
		}

		builder.Append(result)
	}

	return builder.NewArray(), nil
}

func (e *Evaluator) evaluateBooleanComparison(left, right *array.Boolean, op BinaryOp) (arrow.Array, error) {
	builder := array.NewBooleanBuilder(e.mem)
	defer builder.Release()

	for i := 0; i < left.Len(); i++ {
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
		return nil, fmt.Errorf("logical operations require boolean operands")
	}

	builder := array.NewBooleanBuilder(e.mem)
	defer builder.Release()

	for i := 0; i < left.Len(); i++ {
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
		default:
			return nil, fmt.Errorf("unsupported logical operation: %v", op)
		}

		builder.Append(result)
	}

	return builder.NewArray(), nil
}

func (e *Evaluator) getArrayLength(columns map[string]arrow.Array) int {
	for _, arr := range columns {
		return arr.Len()
	}
	return 0
}

// getPromotedType determines the promoted type for mixed arithmetic operations
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

// convertToType converts an Arrow array to the target type
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
		return e.convertInt32ToType(arr.(*array.Int32), targetType)
	case typeInt64:
		return e.convertInt64ToType(arr.(*array.Int64), targetType)
	case typeFloat32:
		return e.convertFloat32ToType(arr.(*array.Float32), targetType)
	case typeFloat64, typeDouble:
		return e.convertFloat64ToType(arr.(*array.Float64), targetType)
	default:
		return nil, fmt.Errorf("unsupported source type for conversion: %s", sourceType)
	}
}

// Type conversion methods
func (e *Evaluator) convertInt32ToType(arr *array.Int32, targetType string) (arrow.Array, error) {
	switch targetType {
	case typeInt64:
		builder := array.NewInt64Builder(e.mem)
		defer builder.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(int64(arr.Value(i)))
			}
		}
		return builder.NewArray(), nil
	case typeFloat32:
		builder := array.NewFloat32Builder(e.mem)
		defer builder.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(float32(arr.Value(i)))
			}
		}
		return builder.NewArray(), nil
	case typeFloat64, typeDouble:
		builder := array.NewFloat64Builder(e.mem)
		defer builder.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(float64(arr.Value(i)))
			}
		}
		return builder.NewArray(), nil
	default:
		return nil, fmt.Errorf("cannot convert int32 to %s", targetType)
	}
}

func (e *Evaluator) convertInt64ToType(arr *array.Int64, targetType string) (arrow.Array, error) {
	switch targetType {
	case typeInt32:
		builder := array.NewInt32Builder(e.mem)
		defer builder.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(int32(arr.Value(i))) // #nosec G115 Note: potential overflow is expected in type conversion
			}
		}
		return builder.NewArray(), nil
	case typeFloat32:
		builder := array.NewFloat32Builder(e.mem)
		defer builder.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(float32(arr.Value(i)))
			}
		}
		return builder.NewArray(), nil
	case typeFloat64, typeDouble:
		builder := array.NewFloat64Builder(e.mem)
		defer builder.Release()
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(float64(arr.Value(i)))
			}
		}
		return builder.NewArray(), nil
	default:
		return nil, fmt.Errorf("cannot convert int64 to %s", targetType)
	}
}

func (e *Evaluator) convertFloat32ToType(arr *array.Float32, targetType string) (arrow.Array, error) {
	switch targetType {
	case typeInt32:
		builder := array.NewInt32Builder(e.mem)
		defer builder.Release()
		for i := 0; i < arr.Len(); i++ {
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
		for i := 0; i < arr.Len(); i++ {
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
		for i := 0; i < arr.Len(); i++ {
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
		for i := 0; i < arr.Len(); i++ {
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
		for i := 0; i < arr.Len(); i++ {
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
		for i := 0; i < arr.Len(); i++ {
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

// Additional arithmetic methods for int32 and float32
func (e *Evaluator) evaluateInt32Arithmetic(left, right *array.Int32, op BinaryOp) (arrow.Array, error) {
	builder := array.NewInt32Builder(e.mem)
	defer builder.Release()

	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) || right.IsNull(i) {
			builder.AppendNull()
			continue
		}

		l := left.Value(i)
		r := right.Value(i)

		var result int32
		switch op {
		case OpAdd:
			result = l + r
		case OpSub:
			result = l - r
		case OpMul:
			result = l * r
		case OpDiv:
			if r == 0 {
				builder.AppendNull()
				continue
			}
			result = l / r
		default:
			return nil, fmt.Errorf("unsupported arithmetic operation: %v", op)
		}

		builder.Append(result)
	}

	return builder.NewArray(), nil
}

func (e *Evaluator) evaluateFloat32Arithmetic(left, right *array.Float32, op BinaryOp) (arrow.Array, error) {
	builder := array.NewFloat32Builder(e.mem)
	defer builder.Release()

	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) || right.IsNull(i) {
			builder.AppendNull()
			continue
		}

		l := left.Value(i)
		r := right.Value(i)

		var result float32
		switch op {
		case OpAdd:
			result = l + r
		case OpSub:
			result = l - r
		case OpMul:
			result = l * r
		case OpDiv:
			result = l / r // Division by zero results in +/-Inf, which is handled by Go
		default:
			return nil, fmt.Errorf("unsupported arithmetic operation: %v", op)
		}

		builder.Append(result)
	}

	return builder.NewArray(), nil
}

// Additional comparison methods for int32 and float32
func (e *Evaluator) evaluateInt32Comparison(left, right *array.Int32, op BinaryOp) (arrow.Array, error) {
	builder := array.NewBooleanBuilder(e.mem)
	defer builder.Release()

	for i := 0; i < left.Len(); i++ {
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
		case OpLt:
			result = l < r
		case OpLe:
			result = l <= r
		case OpGt:
			result = l > r
		case OpGe:
			result = l >= r
		default:
			return nil, fmt.Errorf("unsupported comparison operation: %v", op)
		}

		builder.Append(result)
	}

	return builder.NewArray(), nil
}

func (e *Evaluator) evaluateFloat32Comparison(left, right *array.Float32, op BinaryOp) (arrow.Array, error) {
	builder := array.NewBooleanBuilder(e.mem)
	defer builder.Release()

	for i := 0; i < left.Len(); i++ {
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
		case OpLt:
			result = l < r
		case OpLe:
			result = l <= r
		case OpGt:
			result = l > r
		case OpGe:
			result = l >= r
		default:
			return nil, fmt.Errorf("unsupported comparison operation: %v", op)
		}

		builder.Append(result)
	}

	return builder.NewArray(), nil
}

// evaluateFunction evaluates a function expression
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

// Date/time extraction function types
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

// evaluateDateTimeFunction evaluates date/time extraction functions
func (e *Evaluator) evaluateDateTimeFunction(expr *FunctionExpr, columns map[string]arrow.Array, extractor dateTimeExtractor) (arrow.Array, error) {
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

	for i := 0; i < timestampArr.Len(); i++ {
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

// evaluateDateAdd evaluates DATE_ADD function to add interval to date/time
func (e *Evaluator) evaluateDateAdd(expr *FunctionExpr, columns map[string]arrow.Array) (arrow.Array, error) {
	if len(expr.args) != dateAddArgsCount {
		return nil, fmt.Errorf("date_add function requires exactly %d arguments, got %d", dateAddArgsCount, len(expr.args))
	}

	// Evaluate the date argument
	dateArg, err := e.Evaluate(expr.args[0], columns)
	if err != nil {
		return nil, fmt.Errorf("evaluating date argument for date_add: %w", err)
	}
	defer dateArg.Release()

	// Check if the date argument is a timestamp array
	timestampArr, ok := dateArg.(*array.Timestamp)
	if !ok {
		return nil, fmt.Errorf("date_add function requires a timestamp argument, got %T", dateArg)
	}

	// Get the interval argument - should be an IntervalExpr
	intervalArg := expr.args[1]
	intervalExpr, ok := intervalArg.(*IntervalExpr)
	if !ok {
		return nil, fmt.Errorf("date_add function requires an interval argument, got %T", intervalArg)
	}

	// Validate the interval type is supported
	if !e.isValidIntervalType(intervalExpr.IntervalType()) {
		return nil, fmt.Errorf("date_add function unsupported interval type: %v", intervalExpr.IntervalType())
	}

	// Build the result array
	builder := array.NewTimestampBuilder(e.mem, &arrow.TimestampType{Unit: arrow.Nanosecond})
	defer builder.Release()

	for i := 0; i < timestampArr.Len(); i++ {
		if timestampArr.IsNull(i) {
			builder.AppendNull()
			continue
		}

		// Convert Arrow timestamp to Go time.Time
		tsValue := timestampArr.Value(i)
		nanos := int64(tsValue)
		t := time.Unix(nanos/nanosPerSecond, nanos%nanosPerSecond).UTC()

		// Add the interval
		result := e.addInterval(t, intervalExpr)

		// Convert back to nanoseconds and append
		resultNanos := result.UnixNano()
		builder.Append(arrow.Timestamp(resultNanos))
	}

	return builder.NewArray(), nil
}

// evaluateDateSub evaluates DATE_SUB function to subtract interval from date/time
func (e *Evaluator) evaluateDateSub(expr *FunctionExpr, columns map[string]arrow.Array) (arrow.Array, error) {
	if len(expr.args) != dateSubArgsCount {
		return nil, fmt.Errorf("date_sub function requires exactly %d arguments, got %d", dateSubArgsCount, len(expr.args))
	}

	// Evaluate the date argument
	dateArg, err := e.Evaluate(expr.args[0], columns)
	if err != nil {
		return nil, fmt.Errorf("evaluating date argument for date_sub: %w", err)
	}
	defer dateArg.Release()

	// Check if the date argument is a timestamp array
	timestampArr, ok := dateArg.(*array.Timestamp)
	if !ok {
		return nil, fmt.Errorf("date_sub function requires a timestamp argument, got %T", dateArg)
	}

	// Get the interval argument - should be an IntervalExpr
	intervalArg := expr.args[1]
	intervalExpr, ok := intervalArg.(*IntervalExpr)
	if !ok {
		return nil, fmt.Errorf("date_sub function requires an interval argument, got %T", intervalArg)
	}

	// Validate the interval type is supported
	if !e.isValidIntervalType(intervalExpr.IntervalType()) {
		return nil, fmt.Errorf("date_sub function unsupported interval type: %v", intervalExpr.IntervalType())
	}

	// Build the result array
	builder := array.NewTimestampBuilder(e.mem, &arrow.TimestampType{Unit: arrow.Nanosecond})
	defer builder.Release()

	for i := 0; i < timestampArr.Len(); i++ {
		if timestampArr.IsNull(i) {
			builder.AppendNull()
			continue
		}

		// Convert Arrow timestamp to Go time.Time
		tsValue := timestampArr.Value(i)
		nanos := int64(tsValue)
		t := time.Unix(nanos/nanosPerSecond, nanos%nanosPerSecond).UTC()

		// Subtract the interval (add negative)
		result := e.subtractInterval(t, intervalExpr)

		// Convert back to nanoseconds and append
		resultNanos := result.UnixNano()
		builder.Append(arrow.Timestamp(resultNanos))
	}

	return builder.NewArray(), nil
}

// evaluateDateDiff evaluates DATE_DIFF function to calculate difference between dates
func (e *Evaluator) evaluateDateDiff(expr *FunctionExpr, columns map[string]arrow.Array) (arrow.Array, error) {
	if len(expr.args) != dateDiffArgsCount {
		return nil, fmt.Errorf("date_diff function requires exactly %d arguments, got %d", dateDiffArgsCount, len(expr.args))
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

	for i := 0; i < startTimestampArr.Len(); i++ {
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

// addInterval adds an interval to a time value
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

// subtractInterval subtracts an interval from a time value
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

// isValidDateDiffUnit validates if a date diff unit is supported
func (e *Evaluator) isValidDateDiffUnit(unit string) bool {
	switch unit {
	case unitDays, unitHours, unitMinutes, unitSeconds, unitMonths, unitYears:
		return true
	default:
		return false
	}
}

// isValidIntervalType validates if an interval type is supported
func (e *Evaluator) isValidIntervalType(intervalType IntervalType) bool {
	switch intervalType {
	case IntervalDays, IntervalHours, IntervalMinutes, IntervalMonths, IntervalYears:
		return true
	default:
		return false
	}
}

// calculateDateDiff calculates the difference between two dates in the specified unit
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

// EvaluateWindow evaluates a window expression
func (e *Evaluator) EvaluateWindow(expr *WindowExpr, columns map[string]arrow.Array) (arrow.Array, error) {
	// Get the data length from one of the columns
	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns provided for window function evaluation")
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
func (e *Evaluator) evaluateWindowFunction(expr *WindowFunctionExpr, window *WindowSpec, columns map[string]arrow.Array) (arrow.Array, error) {
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
	default:
		return nil, fmt.Errorf("unsupported window function: %s", expr.funcName)
	}
}

// evaluateWindowAggregation evaluates aggregation functions with OVER clause
func (e *Evaluator) evaluateWindowAggregation(expr *AggregationExpr, window *WindowSpec, columns map[string]arrow.Array) (arrow.Array, error) {
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

// evaluateRowNumber implements ROW_NUMBER() window function
func (e *Evaluator) evaluateRowNumber(window *WindowSpec, columns map[string]arrow.Array, dataLength int) (arrow.Array, error) {
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
	for i := 0; i < dataLength; i++ {
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

// getPartitions creates partitions based on PARTITION BY clause
func (e *Evaluator) getPartitions(window *WindowSpec, columns map[string]arrow.Array, dataLength int) ([][]int, error) {
	if len(window.partitionBy) == 0 {
		// No partitioning, single partition with all rows
		partition := make([]int, dataLength)
		for i := 0; i < dataLength; i++ {
			partition[i] = i
		}
		return [][]int{partition}, nil
	}

	// Group by partition columns
	partitionMap := make(map[string][]int)

	for i := 0; i < dataLength; i++ {
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

// getPartitionKey creates a string key for partitioning
func (e *Evaluator) getPartitionKey(rowIndex int, partitionColumns []string, columns map[string]arrow.Array) (string, error) {
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
				key += fmt.Sprintf("%d", a.Value(rowIndex))
			case *array.Float64:
				key += fmt.Sprintf("%g", a.Value(rowIndex))
			case *array.Boolean:
				key += fmt.Sprintf("%t", a.Value(rowIndex))
			default:
				key += fmt.Sprintf("%v", arr)
			}
		}
	}
	return key, nil
}

// evaluateCaseWithContext evaluates a CASE expression
func (e *Evaluator) evaluateCaseWithContext(expr *CaseExpr, columns map[string]arrow.Array, ctx EvaluationContext) (arrow.Array, error) {
	return nil, fmt.Errorf("CASE expressions not yet implemented")
}

// evaluateIfFunction evaluates an IF function (condition, thenValue, elseValue)
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

	// For now, implement simple case where both then and else are same type
	// This is enough for basic HAVING clause functionality
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

// evaluateIfFloat64Simple evaluates IF function with float64 result
func (e *Evaluator) evaluateIfFloat64Simple(condition *array.Boolean, thenValue, elseValue arrow.Array) (arrow.Array, error) {
	builder := array.NewFloat64Builder(e.mem)
	defer builder.Release()

	thenFloat := thenValue.(*array.Float64)
	elseFloat := elseValue.(*array.Float64)

	for i := 0; i < condition.Len(); i++ {
		if condition.IsNull(i) {
			builder.AppendNull()
		} else if condition.Value(i) {
			if thenFloat.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(thenFloat.Value(i))
			}
		} else {
			if elseFloat.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(elseFloat.Value(i))
			}
		}
	}

	return builder.NewArray(), nil
}

// evaluateIfInt64Simple evaluates IF function with int64 result
func (e *Evaluator) evaluateIfInt64Simple(condition *array.Boolean, thenValue, elseValue arrow.Array) (arrow.Array, error) {
	builder := array.NewInt64Builder(e.mem)
	defer builder.Release()

	thenInt := thenValue.(*array.Int64)
	elseInt := elseValue.(*array.Int64)

	for i := 0; i < condition.Len(); i++ {
		if condition.IsNull(i) {
			builder.AppendNull()
		} else if condition.Value(i) {
			if thenInt.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(thenInt.Value(i))
			}
		} else {
			if elseInt.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(elseInt.Value(i))
			}
		}
	}

	return builder.NewArray(), nil
}

// evaluateIfStringSimple evaluates IF function with string result
func (e *Evaluator) evaluateIfStringSimple(condition *array.Boolean, thenValue, elseValue arrow.Array) (arrow.Array, error) {
	builder := array.NewStringBuilder(e.mem)
	defer builder.Release()

	thenStr := thenValue.(*array.String)
	elseStr := elseValue.(*array.String)

	for i := 0; i < condition.Len(); i++ {
		if condition.IsNull(i) {
			builder.AppendNull()
		} else if condition.Value(i) {
			if thenStr.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(thenStr.Value(i))
			}
		} else {
			if elseStr.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(elseStr.Value(i))
			}
		}
	}

	return builder.NewArray(), nil
}

// evaluateConcatFunction evaluates a CONCAT function
func (e *Evaluator) evaluateConcatFunction(expr *FunctionExpr, columns map[string]arrow.Array) (arrow.Array, error) {
	if len(expr.args) == 0 {
		return nil, fmt.Errorf("CONCAT function requires at least 1 argument")
	}

	// Evaluate all arguments
	argArrays := make([]arrow.Array, len(expr.args))
	for i, arg := range expr.args {
		arr, err := e.Evaluate(arg, columns)
		if err != nil {
			return nil, fmt.Errorf("evaluating CONCAT argument %d: %w", i, err)
		}
		argArrays[i] = arr
		defer arr.Release()
	}

	// Build result
	builder := array.NewStringBuilder(e.mem)
	defer builder.Release()

	for row := 0; row < argArrays[0].Len(); row++ {
		var result string
		hasNull := false

		for _, arr := range argArrays {
			if arr.IsNull(row) {
				hasNull = true
				break
			}

			// Convert to string based on type
			switch typedArr := arr.(type) {
			case *array.String:
				result += typedArr.Value(row)
			case *array.Int64:
				result += fmt.Sprintf("%d", typedArr.Value(row))
			case *array.Float64:
				result += fmt.Sprintf("%g", typedArr.Value(row))
			default:
				result += fmt.Sprintf("%v", arr)
			}
		}

		if hasNull {
			builder.AppendNull()
		} else {
			builder.Append(result)
		}
	}

	return builder.NewArray(), nil
}
