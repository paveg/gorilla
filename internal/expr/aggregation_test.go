package expr_test

import (
	"testing"

	"github.com/paveg/gorilla/internal/expr"
)

func TestAggregationExpressions(t *testing.T) {
	col := expr.Col("value")

	tests := []struct {
		name     string
		expr     *expr.AggregationExpr
		expected expr.AggregationType
		wantStr  string
	}{
		{
			name:     "Sum",
			expr:     expr.Sum(col),
			expected: expr.AggSum,
			wantStr:  "sum(col(value))",
		},
		{
			name:     "Count",
			expr:     expr.Count(col),
			expected: expr.AggCount,
			wantStr:  "count(col(value))",
		},
		{
			name:     "Mean",
			expr:     expr.Mean(col),
			expected: expr.AggMean,
			wantStr:  "mean(col(value))",
		},
		{
			name:     "Min",
			expr:     expr.Min(col),
			expected: expr.AggMin,
			wantStr:  "min(col(value))",
		},
		{
			name:     "Max",
			expr:     expr.Max(col),
			expected: expr.AggMax,
			wantStr:  "max(col(value))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expr.Type() != expr.ExprAggregation {
				t.Errorf("Expected expr.ExprAggregation, got %v", tt.expr.Type())
			}

			if tt.expr.AggType() != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, tt.expr.AggType())
			}

			if tt.expr.String() != tt.wantStr {
				t.Errorf("Expected %s, got %s", tt.wantStr, tt.expr.String())
			}

			if tt.expr.Column() != col {
				t.Error("Column reference should match")
			}
		})
	}
}

func TestColumnAggregationMethods(t *testing.T) {
	col := expr.Col("test")

	// Test that column methods create proper aggregations
	sumExpr := col.Sum()
	if sumExpr.AggType() != expr.AggSum {
		t.Error("col.Sum() should create expr.AggSum")
	}

	countExpr := col.Count()
	if countExpr.AggType() != expr.AggCount {
		t.Error("col.Count() should create expr.AggCount")
	}

	meanExpr := col.Mean()
	if meanExpr.AggType() != expr.AggMean {
		t.Error("col.Mean() should create expr.AggMean")
	}

	minExpr := col.Min()
	if minExpr.AggType() != expr.AggMin {
		t.Error("col.Min() should create expr.AggMin")
	}

	maxExpr := col.Max()
	if maxExpr.AggType() != expr.AggMax {
		t.Error("col.Max() should create expr.AggMax")
	}
}

func TestAggregationAlias(t *testing.T) {
	col := expr.Col("value")
	agg := expr.Sum(col).As("total")

	if agg.Alias() != "total" {
		t.Errorf("Expected alias 'total', got '%s'", agg.Alias())
	}

	if agg.AggType() != expr.AggSum {
		t.Error("Alias should not change aggregation type")
	}

	if agg.Column() != col {
		t.Error("Alias should not change column reference")
	}
}

// TestFunctionExpr is commented out due to access to unexported fields
/*
func TestFunctionExpr(t *testing.T) {
	// Test basic function expression (though not used in current implementation)
	// This ensures the type exists and works as expected
	funcExpr := &expr.FunctionExpr{
		name: "test_func",
		args: []expr.Expr{expr.Col("a"), expr.Lit(42)},
	}

	if funcExpr.Type() != expr.ExprFunction {
		t.Errorf("Expected expr.ExprFunction, got %v", funcExpr.Type())
	}

	if funcExpr.Name() != "test_func" {
		t.Errorf("Expected 'test_func', got '%s'", funcExpr.Name())
	}

	if len(funcExpr.Args()) != 2 {
		t.Errorf("Expected 2 args, got %d", len(funcExpr.Args()))
	}

	expectedStr := "test_func(col(a), lit(42))"
	if funcExpr.String() != expectedStr {
		t.Errorf("Expected '%s', got '%s'", expectedStr, funcExpr.String())
	}
}
*/
