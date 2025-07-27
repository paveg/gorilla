package dataframe

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCompiledHavingEvaluator_Basic tests the basic functionality of the optimization framework
func TestCompiledHavingEvaluator_Basic(t *testing.T) {
	mem := memory.NewGoAllocator()

	tests := []struct {
		name      string
		predicate expr.Expr
		testData  map[string][]float64
		expected  []bool
	}{
		{
			name:      "simple_greater_than",
			predicate: expr.Col("sum_salary").Gt(expr.Lit(100000.0)),
			testData: map[string][]float64{
				"sum_salary": {50000.0, 150000.0, 75000.0, 200000.0},
			},
			expected: []bool{false, true, false, true},
		},
		{
			name:      "constant_true_expression",
			predicate: expr.Lit(true),
			testData: map[string][]float64{
				"sum_salary": {50000.0, 150000.0, 75000.0},
			},
			expected: []bool{true, true, true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create performance hint
			hint := PerformanceHint{
				ExpectedGroupCount:       len(tt.expected),
				ExpectedSelectivity:      0.5,
				PreferMemoryOptimization: true,
				EnableParallelization:    false,       // Disable for small datasets
				MaxMemoryUsage:           1024 * 1024, // 1MB
			}

			// Create compiled evaluator
			evaluator, err := NewCompiledHavingEvaluator(tt.predicate, hint)
			require.NoError(t, err)
			defer evaluator.Release()

			// Compile the expression
			err = evaluator.CompileExpression()
			require.NoError(t, err)

			// Create test data arrays
			aggregatedData := make(map[string]interface{})
			var arraysToRelease []arrow.Array
			defer func() {
				// Release all arrays at the end of the test
				for _, arr := range arraysToRelease {
					arr.Release()
				}
			}()

			for colName, values := range tt.testData {
				builder := array.NewFloat64Builder(mem)
				for _, val := range values {
					builder.Append(val)
				}
				arr := builder.NewFloat64Array()
				arraysToRelease = append(arraysToRelease, arr)
				aggregatedData[colName] = arr
				builder.Release()
			}

			// Convert to arrow.Array map (simplified for test)
			// In actual usage, this would be done by the GroupBy system

			// Get metrics to verify framework is working
			metrics := evaluator.GetMetrics()
			assert.Greater(t, metrics.CompilationTime.Nanoseconds(), int64(0), "Compilation should take measurable time")

			// Test that evaluator is ready
			assert.NotNil(t, evaluator.compiledExpr, "Expression should be compiled")
		})
	}
}

// TestCompiledHavingEvaluator_PerformanceHints tests performance optimization hints
func TestCompiledHavingEvaluator_PerformanceHints(t *testing.T) {
	predicate := expr.Col("avg_salary").Gt(expr.Lit(75000.0))

	tests := []struct {
		name string
		hint PerformanceHint
	}{
		{
			name: "memory_optimized",
			hint: PerformanceHint{
				ExpectedGroupCount:       100,
				ExpectedSelectivity:      0.2,
				PreferMemoryOptimization: true,
				EnableParallelization:    false,
				MaxMemoryUsage:           512 * 1024, // 512KB
			},
		},
		{
			name: "parallel_optimized",
			hint: PerformanceHint{
				ExpectedGroupCount:       1000,
				ExpectedSelectivity:      0.8,
				PreferMemoryOptimization: false,
				EnableParallelization:    true,
				MaxMemoryUsage:           64 * 1024 * 1024, // 64MB
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evaluator, err := NewCompiledHavingEvaluator(predicate, tt.hint)
			require.NoError(t, err)
			defer evaluator.Release()

			// Verify the evaluator was created with the correct hints
			assert.Equal(t, tt.hint.ExpectedGroupCount, evaluator.performanceHint.ExpectedGroupCount)
			assert.Equal(t, tt.hint.EnableParallelization, evaluator.performanceHint.EnableParallelization)

			// Compile and verify it works
			err = evaluator.CompileExpression()
			require.NoError(t, err)

			// Check that compiled expression exists
			assert.NotNil(t, evaluator.compiledExpr)
		})
	}
}

// TestCompiledHavingEvaluator_ConstantOptimization tests constant expression optimization
func TestCompiledHavingEvaluator_ConstantOptimization(t *testing.T) {
	tests := []struct {
		name           string
		predicate      expr.Expr
		expectConstant bool
		expectedValue  interface{}
	}{
		{
			name:           "literal_true",
			predicate:      expr.Lit(true),
			expectConstant: true,
			expectedValue:  true,
		},
		{
			name:           "literal_false",
			predicate:      expr.Lit(false),
			expectConstant: true,
			expectedValue:  false,
		},
		{
			name:           "constant_arithmetic",
			predicate:      expr.NewBinaryExpr(expr.Lit(10.0), expr.OpGt, expr.Lit(5.0)),
			expectConstant: true,
			expectedValue:  true,
		},
		{
			name:           "non_constant_column_ref",
			predicate:      expr.Col("sum_salary").Gt(expr.Lit(100000.0)),
			expectConstant: false,
			expectedValue:  nil,
		},
	}

	hint := PerformanceHint{
		ExpectedGroupCount:       10,
		ExpectedSelectivity:      0.5,
		PreferMemoryOptimization: true,
		EnableParallelization:    false,
		MaxMemoryUsage:           1024 * 1024,
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evaluator, err := NewCompiledHavingEvaluator(tt.predicate, hint)
			require.NoError(t, err)
			defer evaluator.Release()

			// Compile the expression
			err = evaluator.CompileExpression()
			require.NoError(t, err)

			// Check constant optimization
			assert.Equal(t, tt.expectConstant, evaluator.compiledExpr.isConstant)
			if tt.expectConstant {
				assert.Equal(t, tt.expectedValue, evaluator.compiledExpr.constantValue)
			}
		})
	}
}

// TestCompiledHavingEvaluator_MemoryManagement tests proper memory cleanup
func TestCompiledHavingEvaluator_MemoryManagement(t *testing.T) {
	predicate := expr.Col("count").Gt(expr.Lit(int64(10)))
	hint := PerformanceHint{
		ExpectedGroupCount:       50,
		ExpectedSelectivity:      0.3,
		PreferMemoryOptimization: true,
		EnableParallelization:    false,
		MaxMemoryUsage:           1024 * 1024,
	}

	// Create and use evaluator
	evaluator, err := NewCompiledHavingEvaluator(predicate, hint)
	require.NoError(t, err)

	// Verify memory pool is initialized
	assert.NotNil(t, evaluator.memoryPool)
	assert.NotNil(t, evaluator.memoryPool.GetAllocator())

	// Test memory pool functionality
	boolBuilder := evaluator.memoryPool.GetBooleanBuilder()
	assert.NotNil(t, boolBuilder)

	// Return builder to pool
	evaluator.memoryPool.PutBooleanBuilder(boolBuilder)

	// Compile expression
	err = evaluator.CompileExpression()
	require.NoError(t, err)

	// Release should clean up all resources
	evaluator.Release()

	// After release, memory pool should be cleaned up
	// Note: We can't easily test this without exposing internal state,
	// but the release call should not panic
}
