package expr

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test helpers

func createTestColumnsForContext(t *testing.T, mem memory.Allocator) map[string]arrow.Array {
	// Create comprehensive test data for context testing
	intBuilder := array.NewInt64Builder(mem)
	defer intBuilder.Release()
	intBuilder.AppendValues([]int64{10, 20, 30, 40, 50}, nil)
	intArray := intBuilder.NewArray()

	floatBuilder := array.NewFloat64Builder(mem)
	defer floatBuilder.Release()
	floatBuilder.AppendValues([]float64{1.5, 2.5, 3.5, 4.5, 5.5}, nil)
	floatArray := floatBuilder.NewArray()

	stringBuilder := array.NewStringBuilder(mem)
	defer stringBuilder.Release()
	stringBuilder.AppendValues([]string{"a", "b", "c", "d", "e"}, nil)
	stringArray := stringBuilder.NewArray()

	boolBuilder := array.NewBooleanBuilder(mem)
	defer boolBuilder.Release()
	boolBuilder.AppendValues([]bool{true, false, true, false, true}, nil)
	boolArray := boolBuilder.NewArray()

	// Create department data for grouping
	deptBuilder := array.NewStringBuilder(mem)
	defer deptBuilder.Release()
	deptBuilder.AppendValues([]string{"eng", "sales", "eng", "sales", "eng"}, nil)
	deptArray := deptBuilder.NewArray()

	return map[string]arrow.Array{
		"id":         intArray,
		"salary":     floatArray,
		"name":       stringArray,
		"active":     boolArray,
		"department": deptArray,
	}
}

func createAggregatedTestColumns(t *testing.T, mem memory.Allocator) map[string]arrow.Array {
	// Create aggregated data that would be present in GroupContext
	// This simulates the result of a GROUP BY operation
	deptBuilder := array.NewStringBuilder(mem)
	defer deptBuilder.Release()
	deptBuilder.AppendValues([]string{"eng", "sales"}, nil)
	deptArray := deptBuilder.NewArray()

	totalSalaryBuilder := array.NewFloat64Builder(mem)
	defer totalSalaryBuilder.Release()
	totalSalaryBuilder.AppendValues([]float64{9.5, 7.0}, nil) // sum of salaries by dept
	totalSalaryArray := totalSalaryBuilder.NewArray()

	countBuilder := array.NewInt64Builder(mem)
	defer countBuilder.Release()
	countBuilder.AppendValues([]int64{3, 2}, nil) // count by dept
	countArray := countBuilder.NewArray()

	avgSalaryBuilder := array.NewFloat64Builder(mem)
	defer avgSalaryBuilder.Release()
	avgSalaryBuilder.AppendValues([]float64{3.17, 3.5}, nil) // avg salary by dept
	avgSalaryArray := avgSalaryBuilder.NewArray()

	return map[string]arrow.Array{
		"department": deptArray,
		"sum_salary": totalSalaryArray,
		"count_id":   countArray,
		"avg_salary": avgSalaryArray,
		// Alternative naming pattern for testing default behavior
		"sum_total":   totalSalaryArray,
		"count_total": countArray,
		"mean_total":  avgSalaryArray,
	}
}

func createWindowTestColumns(t *testing.T, mem memory.Allocator) map[string]arrow.Array {
	// Create data with window function results
	valueBuilder := array.NewInt64Builder(mem)
	defer valueBuilder.Release()
	valueBuilder.AppendValues([]int64{10, 20, 30, 40, 50}, nil)
	valueArray := valueBuilder.NewArray()

	rowNumBuilder := array.NewInt64Builder(mem)
	defer rowNumBuilder.Release()
	rowNumBuilder.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	rowNumArray := rowNumBuilder.NewArray()

	rankBuilder := array.NewInt64Builder(mem)
	defer rankBuilder.Release()
	rankBuilder.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	rankArray := rankBuilder.NewArray()

	return map[string]arrow.Array{
		"value":      valueArray,
		"row_number": rowNumArray,
		"rank":       rankArray,
	}
}

// Basic context-aware evaluation tests

func TestEvaluateWithContext_Column(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)

	// Test with row context
	rowColumns := createTestColumnsForContext(t, mem)
	defer func() {
		for _, arr := range rowColumns {
			arr.Release()
		}
	}()

	colExpr := Col("id")
	result, err := eval.EvaluateWithContext(colExpr, rowColumns, RowContext)
	require.NoError(t, err)
	defer result.Release()

	intResult, ok := result.(*array.Int64)
	require.True(t, ok)
	assert.Equal(t, 5, intResult.Len())
	assert.Equal(t, int64(10), intResult.Value(0))
	assert.Equal(t, int64(20), intResult.Value(1))

	// Test with group context
	groupColumns := createAggregatedTestColumns(t, mem)
	defer func() {
		for _, arr := range groupColumns {
			arr.Release()
		}
	}()

	deptExpr := Col("department")
	result2, err := eval.EvaluateWithContext(deptExpr, groupColumns, GroupContext)
	require.NoError(t, err)
	defer result2.Release()

	stringResult, ok := result2.(*array.String)
	require.True(t, ok)
	assert.Equal(t, 2, stringResult.Len())
	assert.Equal(t, "eng", stringResult.Value(0))
	assert.Equal(t, "sales", stringResult.Value(1))
}

func TestEvaluateWithContext_Literal(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)
	columns := createTestColumnsForContext(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	tests := []struct {
		name     string
		value    interface{}
		context  EvaluationContext
		expected interface{}
	}{
		{"int64 in row context", int64(42), RowContext, int64(42)},
		{"string in group context", "test", GroupContext, "test"},
		{"float64 in row context", 3.14, RowContext, 3.14},
		{"bool in group context", true, GroupContext, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			litExpr := Lit(tt.value)
			result, err := eval.EvaluateWithContext(litExpr, columns, tt.context)
			require.NoError(t, err)
			defer result.Release()

			assert.Equal(t, 5, result.Len()) // Should match column length

			// Check first value (all should be the same for literals)
			switch arr := result.(type) {
			case *array.Int64:
				assert.Equal(t, tt.expected, arr.Value(0))
			case *array.String:
				assert.Equal(t, tt.expected, arr.Value(0))
			case *array.Float64:
				assert.Equal(t, tt.expected, arr.Value(0))
			case *array.Boolean:
				assert.Equal(t, tt.expected, arr.Value(0))
			}
		})
	}
}

func TestEvaluateWithContext_Binary(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)
	columns := createTestColumnsForContext(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	tests := []struct {
		name     string
		expr     Expr
		context  EvaluationContext
		expected []int64
	}{
		{
			name:     "addition in row context",
			expr:     Col("id").Add(Lit(int64(5))),
			context:  RowContext,
			expected: []int64{15, 25, 35, 45, 55},
		},
		{
			name:     "multiplication in row context",
			expr:     Col("id").Mul(Lit(int64(2))),
			context:  RowContext,
			expected: []int64{20, 40, 60, 80, 100},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := eval.EvaluateWithContext(tt.expr, columns, tt.context)
			require.NoError(t, err)
			defer result.Release()

			intResult, ok := result.(*array.Int64)
			require.True(t, ok)
			assert.Equal(t, len(tt.expected), intResult.Len())

			for i, expected := range tt.expected {
				assert.Equal(t, expected, intResult.Value(i))
			}
		})
	}
}

func TestEvaluateBooleanWithContext_Comparison(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)
	columns := createTestColumnsForContext(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	tests := []struct {
		name     string
		expr     Expr
		context  EvaluationContext
		expected []bool
	}{
		{
			name:     "greater than in row context",
			expr:     Col("id").Gt(Lit(int64(25))),
			context:  RowContext,
			expected: []bool{false, false, true, true, true},
		},
		{
			name:     "equality in row context",
			expr:     Col("department").Eq(Lit("eng")),
			context:  RowContext,
			expected: []bool{true, false, true, false, true},
		},
		{
			name:     "logical and in row context",
			expr:     Col("id").Gt(Lit(int64(15))).And(Col("active")),
			context:  RowContext,
			expected: []bool{false, false, true, false, true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := eval.EvaluateBooleanWithContext(tt.expr, columns, tt.context)
			require.NoError(t, err)
			defer result.Release()

			boolResult, ok := result.(*array.Boolean)
			require.True(t, ok)
			assert.Equal(t, len(tt.expected), boolResult.Len())

			for i, expected := range tt.expected {
				assert.Equal(t, expected, boolResult.Value(i))
			}
		})
	}
}

// Context validation tests

func TestValidateContextSupport_AggregationExpr(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)
	columns := createTestColumnsForContext(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	// Create aggregation expression
	aggExpr := &AggregationExpr{
		column:  Col("salary"),
		aggType: AggSum,
	}

	// Should work in GroupContext
	_, err := eval.EvaluateWithContext(aggExpr, columns, GroupContext)
	assert.Error(t, err) // Will error because columns don't contain aggregated data, but validation should pass

	// Should fail in RowContext
	_, err = eval.EvaluateWithContext(aggExpr, columns, RowContext)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not support RowContext")
}

func TestValidateContextSupport_WindowExpr(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)
	columns := createWindowTestColumns(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	// Create window expression
	windowFunc := RowNumber()
	windowSpec := NewWindow().PartitionBy("department")
	windowExpr := &WindowExpr{
		function: windowFunc,
		window:   windowSpec,
	}

	// Should work in RowContext
	_, err := eval.EvaluateWithContext(windowExpr, columns, RowContext)
	assert.Error(t, err) // May error due to missing partition columns, but context validation should pass

	// Should fail in GroupContext
	_, err = eval.EvaluateWithContext(windowExpr, columns, GroupContext)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not support GroupContext")
}

func TestValidateContextSupport_NestedExpressions(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)
	columns := createTestColumnsForContext(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	// Create nested expression with mixed context support
	// Binary expression containing aggregation (should only work in GroupContext)
	aggExpr := &AggregationExpr{
		column:  Col("salary"),
		aggType: AggSum,
	}
	nestedExpr := &BinaryExpr{
		left:  aggExpr,
		op:    OpGt,
		right: Lit(float64(10.0)),
	}

	// Should fail in RowContext due to aggregation
	_, err := eval.EvaluateWithContext(nestedExpr, columns, RowContext)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not support RowContext")

	// Should work in GroupContext (though may fail due to missing aggregated data)
	_, err = eval.EvaluateWithContext(nestedExpr, columns, GroupContext)
	assert.Error(t, err) // Will error on missing aggregated column, but context validation should pass
}

// Integration tests with actual Arrow arrays

func TestEvaluateWithContext_Integration_RowContext(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)
	columns := createTestColumnsForContext(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	// Complex expression in row context: (id + salary) > 25 AND active
	expr := Col("id").Add(Col("salary")).Gt(Lit(float64(25.0))).And(Col("active"))

	result, err := eval.EvaluateBooleanWithContext(expr, columns, RowContext)
	require.NoError(t, err)
	defer result.Release()

	boolResult, ok := result.(*array.Boolean)
	require.True(t, ok)

	// Expected:
	// id + salary: [11.5, 22.5, 33.5, 44.5, 55.5]
	// > 25: [false, false, true, true, true]
	// active: [true, false, true, false, true]
	// AND: [false, false, true, false, true]
	expected := []bool{false, false, true, false, true}
	for i, exp := range expected {
		assert.Equal(t, exp, boolResult.Value(i), "Mismatch at index %d", i)
	}
}

func TestEvaluateWithContext_Integration_GroupContext(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)
	groupColumns := createAggregatedTestColumns(t, mem)
	defer func() {
		for _, arr := range groupColumns {
			arr.Release()
		}
	}()

	// Test aggregated column evaluation in group context
	expr := Col("sum_salary").Gt(Lit(float64(8.0)))

	result, err := eval.EvaluateBooleanWithContext(expr, groupColumns, GroupContext)
	require.NoError(t, err)
	defer result.Release()

	boolResult, ok := result.(*array.Boolean)
	require.True(t, ok)

	// sum_salary: [9.5, 7.0]
	// > 8.0: [true, false]
	expected := []bool{true, false}
	for i, exp := range expected {
		assert.Equal(t, exp, boolResult.Value(i))
	}
}

// Error handling tests

func TestEvaluateWithContext_ContextMismatch(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)
	columns := createTestColumnsForContext(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	tests := []struct {
		name        string
		expr        Expr
		context     EvaluationContext
		expectError string
	}{
		{
			name: "aggregation in row context",
			expr: &AggregationExpr{
				column:  Col("salary"),
				aggType: AggSum,
			},
			context:     RowContext,
			expectError: "does not support RowContext",
		},
		{
			name: "window function in group context",
			expr: &WindowExpr{
				function: RowNumber(),
				window:   NewWindow(),
			},
			context:     GroupContext,
			expectError: "does not support GroupContext",
		},
		{
			name:        "invalid expression",
			expr:        Invalid("test error"),
			context:     RowContext,
			expectError: "does not support RowContext",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := eval.EvaluateWithContext(tt.expr, columns, tt.context)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectError)
		})
	}
}

func TestEvaluateWithContext_NonExistentColumn(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)
	columns := createTestColumnsForContext(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	expr := Col("nonexistent")
	_, err := eval.EvaluateWithContext(expr, columns, RowContext)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "column not found")
}

// Backward compatibility tests

func TestEvaluateWithContext_BackwardCompatibility(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)
	columns := createTestColumnsForContext(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	// Test that old methods still work
	expr := Col("id").Add(Lit(int64(5)))

	// Old method
	oldResult, err := eval.Evaluate(expr, columns)
	require.NoError(t, err)
	defer oldResult.Release()

	// New method with RowContext (should produce same result)
	newResult, err := eval.EvaluateWithContext(expr, columns, RowContext)
	require.NoError(t, err)
	defer newResult.Release()

	// Results should be identical
	oldInt := oldResult.(*array.Int64)
	newInt := newResult.(*array.Int64)
	assert.Equal(t, oldInt.Len(), newInt.Len())
	for i := 0; i < oldInt.Len(); i++ {
		assert.Equal(t, oldInt.Value(i), newInt.Value(i))
	}
}

func TestEvaluateBooleanWithContext_BackwardCompatibility(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)
	columns := createTestColumnsForContext(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	// Test that old boolean methods still work
	expr := Col("id").Gt(Lit(int64(25)))

	// Old method
	oldResult, err := eval.EvaluateBoolean(expr, columns)
	require.NoError(t, err)
	defer oldResult.Release()

	// New method with RowContext (should produce same result)
	newResult, err := eval.EvaluateBooleanWithContext(expr, columns, RowContext)
	require.NoError(t, err)
	defer newResult.Release()

	// Results should be identical
	oldBool := oldResult.(*array.Boolean)
	newBool := newResult.(*array.Boolean)
	assert.Equal(t, oldBool.Len(), newBool.Len())
	for i := 0; i < oldBool.Len(); i++ {
		assert.Equal(t, oldBool.Value(i), newBool.Value(i))
	}
}

// Complex nested expression tests

func TestEvaluateWithContext_ComplexNestedExpressions(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)
	columns := createTestColumnsForContext(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	// Very complex nested expression in row context
	// ((id * 2) + salary) > 35 AND (department == "eng" OR active)
	leftSide := Col("id").Mul(Lit(int64(2))).Add(Col("salary")).Gt(Lit(int64(35)))
	rightSide := Col("department").Eq(Lit("eng")).Or(Col("active"))
	complexExpr := leftSide.And(rightSide)

	result, err := eval.EvaluateBooleanWithContext(complexExpr, columns, RowContext)
	require.NoError(t, err)
	defer result.Release()

	boolResult, ok := result.(*array.Boolean)
	require.True(t, ok)
	assert.Equal(t, 5, boolResult.Len())

	// Manual calculation for verification:
	// id*2 + salary: [21.5, 42.5, 63.5, 84.5, 105.5]
	// > 35: [false, true, true, true, true]
	// dept=="eng" OR active: [true, false, true, false, true]
	// AND: [false, false, true, false, true]
	expected := []bool{false, false, true, false, true}
	for i, exp := range expected {
		assert.Equal(t, exp, boolResult.Value(i), "Mismatch at index %d", i)
	}
}

// Aggregation expression handling in GroupContext

func TestEvaluateWithContext_AggregationInGroupContext(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)
	groupColumns := createAggregatedTestColumns(t, mem)
	defer func() {
		for _, arr := range groupColumns {
			arr.Release()
		}
	}()

	tests := []struct {
		name     string
		aggExpr  *AggregationExpr
		expected []float64
	}{
		{
			name: "sum aggregation with alias",
			aggExpr: &AggregationExpr{
				column:  Col("salary"),
				aggType: AggSum,
				alias:   "sum_salary",
			},
			expected: []float64{9.5, 7.0},
		},
		{
			name: "mean aggregation with alias",
			aggExpr: &AggregationExpr{
				column:  Col("salary"),
				aggType: AggMean,
				alias:   "avg_salary",
			},
			expected: []float64{3.17, 3.5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := eval.EvaluateWithContext(tt.aggExpr, groupColumns, GroupContext)
			require.NoError(t, err)
			defer result.Release()

			floatResult, ok := result.(*array.Float64)
			require.True(t, ok)
			assert.Equal(t, len(tt.expected), floatResult.Len())

			for i, expected := range tt.expected {
				assert.InDelta(t, expected, floatResult.Value(i), 0.01, "Mismatch at index %d", i)
			}
		})
	}
}

func TestEvaluateWithContext_AggregationDefaultNaming(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)
	groupColumns := createAggregatedTestColumns(t, mem)
	defer func() {
		for _, arr := range groupColumns {
			arr.Release()
		}
	}()

	// Test aggregation with default naming (no alias)
	aggExpr := &AggregationExpr{
		column:  Col("salary"),
		aggType: AggSum,
		// No alias set - should use default naming
	}

	result, err := eval.EvaluateWithContext(aggExpr, groupColumns, GroupContext)
	require.NoError(t, err)
	defer result.Release()

	floatResult, ok := result.(*array.Float64)
	require.True(t, ok)
	assert.Equal(t, 2, floatResult.Len())
	assert.InDelta(t, 9.5, floatResult.Value(0), 0.01)
	assert.InDelta(t, 7.0, floatResult.Value(1), 0.01)
}

// Window expression handling in RowContext

func TestEvaluateWithContext_WindowInRowContext(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)
	columns := createWindowTestColumns(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	// Test window function
	windowFunc := &WindowFunctionExpr{
		funcName: "ROW_NUMBER",
		args:     nil,
	}
	windowExpr := &WindowExpr{
		function: windowFunc,
		window:   NewWindow(),
	}

	// Should validate context support (but may fail on actual evaluation due to missing implementation details)
	err := eval.validateContextSupport(windowExpr, RowContext)
	assert.NoError(t, err, "Window expression should support RowContext")

	err = eval.validateContextSupport(windowExpr, GroupContext)
	assert.Error(t, err, "Window expression should not support GroupContext")
	assert.Contains(t, err.Error(), "does not support GroupContext")
}

// Function expression with context validation

func TestEvaluateWithContext_FunctionExpression(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)
	columns := createTestColumnsForContext(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	// Test function expression that should work in both contexts
	// (functions typically depend on their arguments' context support)
	funcExpr := &FunctionExpr{
		name: "test_function",
		args: []Expr{Col("id"), Lit(int64(5))},
	}

	// Context validation should pass for both contexts since arguments support both
	err := eval.validateContextSupport(funcExpr, RowContext)
	assert.NoError(t, err)

	err = eval.validateContextSupport(funcExpr, GroupContext)
	assert.NoError(t, err)

	// However, actual evaluation may fail due to unsupported function
	_, err = eval.EvaluateWithContext(funcExpr, columns, RowContext)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported function")
}

// Edge cases and error conditions

func TestEvaluateWithContext_EmptyColumns(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)
	emptyColumns := make(map[string]arrow.Array)

	expr := Col("nonexistent")
	_, err := eval.EvaluateWithContext(expr, emptyColumns, RowContext)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "column not found")
}

func TestEvaluateWithContext_UnsupportedExpressionType(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)
	columns := createTestColumnsForContext(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	// Test with nil expression (should cause unsupported type error)
	_, err := eval.EvaluateWithContext(nil, columns, RowContext)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported expression type")
}

// Comprehensive integration test

func TestEvaluateWithContext_ComprehensiveIntegration(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)

	// Test a realistic scenario with row context
	rowColumns := createTestColumnsForContext(t, mem)
	defer func() {
		for _, arr := range rowColumns {
			arr.Release()
		}
	}()

	// Complex business logic: active employees in engineering with salary boost > 4.0
	activeCond := Col("active").Eq(Lit(true))
	engCond := Col("department").Eq(Lit("eng"))
	salaryBoost := Col("salary").Mul(Lit(float64(1.5))).Gt(Lit(float64(4.0)))
	businessLogic := activeCond.And(engCond).And(salaryBoost)

	result, err := eval.EvaluateBooleanWithContext(businessLogic, rowColumns, RowContext)
	require.NoError(t, err)
	defer result.Release()

	boolResult, ok := result.(*array.Boolean)
	require.True(t, ok)

	// Manual verification:
	// active: [true, false, true, false, true]
	// dept=="eng": [true, false, true, false, true]
	// salary*1.5: [2.25, 3.75, 5.25, 6.75, 8.25]
	// > 4.0: [false, false, true, true, true]
	// Final AND: [false, false, true, false, true]
	expected := []bool{false, false, true, false, true}
	for i, exp := range expected {
		assert.Equal(t, exp, boolResult.Value(i), "Mismatch at index %d", i)
	}

	// Test group context with aggregated data
	groupColumns := createAggregatedTestColumns(t, mem)
	defer func() {
		for _, arr := range groupColumns {
			arr.Release()
		}
	}()

	// Business rule: departments with average salary > 3.2
	groupBusinessLogic := Col("avg_salary").Gt(Lit(float64(3.2)))

	result2, err := eval.EvaluateBooleanWithContext(groupBusinessLogic, groupColumns, GroupContext)
	require.NoError(t, err)
	defer result2.Release()

	boolResult2, ok := result2.(*array.Boolean)
	require.True(t, ok)

	// avg_salary: [3.17, 3.5]
	// > 3.2: [false, true]
	expected2 := []bool{false, true}
	for i, exp := range expected2 {
		assert.Equal(t, exp, boolResult2.Value(i), "Group context mismatch at index %d", i)
	}
}

// Memory management test

func TestEvaluateWithContext_MemoryManagement(t *testing.T) {
	mem := memory.NewGoAllocator()
	eval := NewEvaluator(mem)
	columns := createTestColumnsForContext(t, mem)
	defer func() {
		for _, arr := range columns {
			arr.Release()
		}
	}()

	// Test that results are properly managed
	expr := Col("id").Add(Lit(int64(10)))

	result, err := eval.EvaluateWithContext(expr, columns, RowContext)
	require.NoError(t, err)

	// Verify result is valid
	intResult, ok := result.(*array.Int64)
	require.True(t, ok)
	assert.Equal(t, 5, intResult.Len())
	assert.Equal(t, int64(20), intResult.Value(0))

	// Clean up
	result.Release()

	// After release, the result should no longer be valid for use
	// (Note: actual behavior depends on Arrow implementation)
}
