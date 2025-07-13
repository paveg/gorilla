package expr

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Type name constants
const (
	typeInt32   = "int32"
	typeInt64   = "int64"
	typeFloat32 = "float32"
	typeFloat64 = "float64"
	typeDouble  = "double"
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

// EvaluateBoolean evaluates an expression that should return a boolean array
func (e *Evaluator) EvaluateBoolean(expr Expr, columns map[string]arrow.Array) (arrow.Array, error) {
	switch ex := expr.(type) {
	case *ColumnExpr:
		return e.evaluateColumnBoolean(ex, columns)
	case *LiteralExpr:
		return e.evaluateLiteralBoolean(ex, columns)
	case *BinaryExpr:
		return e.evaluateBinaryBoolean(ex, columns)
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
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

func (e *Evaluator) evaluateColumn(expr *ColumnExpr, columns map[string]arrow.Array) (arrow.Array, error) {
	arr, exists := columns[expr.name]
	if !exists {
		return nil, fmt.Errorf("column not found: %s", expr.name)
	}
	// Return a reference to the existing array (caller should handle retention if needed)
	arr.Retain()
	return arr, nil
}

func (e *Evaluator) evaluateColumnBoolean(expr *ColumnExpr, columns map[string]arrow.Array) (arrow.Array, error) {
	arr, exists := columns[expr.name]
	if !exists {
		return nil, fmt.Errorf("column not found: %s", expr.name)
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
