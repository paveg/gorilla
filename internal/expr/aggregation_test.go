package expr

import (
	"testing"
)

func TestAggregationExpressions(t *testing.T) {
	col := Col("value")

	tests := []struct {
		name     string
		expr     *AggregationExpr
		expected AggregationType
		wantStr  string
	}{
		{
			name:     "Sum",
			expr:     Sum(col),
			expected: AggSum,
			wantStr:  "sum(col(value))",
		},
		{
			name:     "Count",
			expr:     Count(col),
			expected: AggCount,
			wantStr:  "count(col(value))",
		},
		{
			name:     "Mean",
			expr:     Mean(col),
			expected: AggMean,
			wantStr:  "mean(col(value))",
		},
		{
			name:     "Min",
			expr:     Min(col),
			expected: AggMin,
			wantStr:  "min(col(value))",
		},
		{
			name:     "Max",
			expr:     Max(col),
			expected: AggMax,
			wantStr:  "max(col(value))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expr.Type() != ExprAggregation {
				t.Errorf("Expected ExprAggregation, got %v", tt.expr.Type())
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
	col := Col("test")

	// Test that column methods create proper aggregations
	sumExpr := col.Sum()
	if sumExpr.AggType() != AggSum {
		t.Error("col.Sum() should create AggSum")
	}

	countExpr := col.Count()
	if countExpr.AggType() != AggCount {
		t.Error("col.Count() should create AggCount")
	}

	meanExpr := col.Mean()
	if meanExpr.AggType() != AggMean {
		t.Error("col.Mean() should create AggMean")
	}

	minExpr := col.Min()
	if minExpr.AggType() != AggMin {
		t.Error("col.Min() should create AggMin")
	}

	maxExpr := col.Max()
	if maxExpr.AggType() != AggMax {
		t.Error("col.Max() should create AggMax")
	}
}

func TestAggregationAlias(t *testing.T) {
	col := Col("value")
	expr := Sum(col).As("total")

	if expr.Alias() != "total" {
		t.Errorf("Expected alias 'total', got '%s'", expr.Alias())
	}

	if expr.AggType() != AggSum {
		t.Error("Alias should not change aggregation type")
	}

	if expr.Column() != col {
		t.Error("Alias should not change column reference")
	}
}

func TestFunctionExpr(t *testing.T) {
	// Test basic function expression (though not used in current implementation)
	// This ensures the type exists and works as expected
	funcExpr := &FunctionExpr{
		name: "test_func",
		args: []Expr{Col("a"), Lit(42)},
	}

	if funcExpr.Type() != ExprFunction {
		t.Errorf("Expected ExprFunction, got %v", funcExpr.Type())
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
