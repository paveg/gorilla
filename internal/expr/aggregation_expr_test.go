package expr_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
)

func TestAggregationExprBasicFunctionality(t *testing.T) {
	tests := []struct {
		name           string
		aggregation    *expr.AggregationExpr
		expectedType   expr.AggregationType
		expectedString string
	}{
		{"Sum aggregation", expr.Sum(expr.Col("value")), expr.AggSum, "sum(col(value))"},
		{"Count aggregation", expr.Count(expr.Col("id")), expr.AggCount, "count(col(id))"},
		{"Mean aggregation", expr.Mean(expr.Col("score")), expr.AggMean, "mean(col(score))"},
		{"Min aggregation", expr.Min(expr.Col("age")), expr.AggMin, "min(col(age))"},
		{"Max aggregation", expr.Max(expr.Col("salary")), expr.AggMax, "max(col(salary))"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expectedType, tt.aggregation.AggType())
			assert.Equal(t, tt.expectedString, tt.aggregation.String())
		})
	}
}

func TestAggregationExprComparison(t *testing.T) {
	t.Run("Sum greater than literal", func(t *testing.T) {
		sumExpr := expr.Sum(expr.Col("salary"))
		comparison := sumExpr.Gt(expr.Lit(50000))
		expected := "(sum(col(salary)) > lit(50000))"
		assert.Equal(t, expected, comparison.String())
	})

	t.Run("Count equal to literal", func(t *testing.T) {
		countExpr := expr.Count(expr.Col("id"))
		comparison := countExpr.Eq(expr.Lit(5))
		expected := "(count(col(id)) == lit(5))"
		assert.Equal(t, expected, comparison.String())
	})

	t.Run("Average less than literal", func(t *testing.T) {
		avgExpr := expr.Mean(expr.Col("score"))
		comparison := avgExpr.Lt(expr.Lit(85.5))
		expected := "(mean(col(score)) < lit(85.5))"
		assert.Equal(t, expected, comparison.String())
	})

	t.Run("Min less than or equal to literal", func(t *testing.T) {
		minExpr := expr.Min(expr.Col("age"))
		comparison := minExpr.Le(expr.Lit(18))
		expected := "(min(col(age)) <= lit(18))"
		assert.Equal(t, expected, comparison.String())
	})

	t.Run("Max greater than or equal to literal", func(t *testing.T) {
		maxExpr := expr.Max(expr.Col("price"))
		comparison := maxExpr.Ge(expr.Lit(100.0))
		expected := "(max(col(price)) >= lit(100))"
		assert.Equal(t, expected, comparison.String())
	})

	t.Run("Sum not equal to literal", func(t *testing.T) {
		sumExpr := expr.Sum(expr.Col("amount"))
		comparison := sumExpr.Ne(expr.Lit(0))
		expected := "(sum(col(amount)) != lit(0))"
		assert.Equal(t, expected, comparison.String())
	})
}

func TestAggregationExprArithmetic(t *testing.T) {
	t.Run("Sum addition with literal", func(t *testing.T) {
		sumExpr := expr.Sum(expr.Col("base_salary"))
		result := sumExpr.Add(expr.Lit(1000))
		expected := "(sum(col(base_salary)) + lit(1000))"
		assert.Equal(t, expected, result.String())
	})

	t.Run("Average multiplication with literal", func(t *testing.T) {
		avgExpr := expr.Mean(expr.Col("score"))
		result := avgExpr.Mul(expr.Lit(1.1))
		expected := "(mean(col(score)) * lit(1.1))"
		assert.Equal(t, expected, result.String())
	})

	t.Run("Count subtraction with literal", func(t *testing.T) {
		countExpr := expr.Count(expr.Col("items"))
		result := countExpr.Sub(expr.Lit(1))
		expected := "(count(col(items)) - lit(1))"
		assert.Equal(t, expected, result.String())
	})

	t.Run("Sum division with literal", func(t *testing.T) {
		sumExpr := expr.Sum(expr.Col("total"))
		result := sumExpr.Div(expr.Lit(2))
		expected := "(sum(col(total)) / lit(2))"
		assert.Equal(t, expected, result.String())
	})
}

func TestAggregationExprAliasing(t *testing.T) {
	t.Run("aggregation with alias", func(t *testing.T) {
		agg := expr.Sum(expr.Col("revenue")).As("total_revenue")

		assert.Equal(t, "total_revenue", agg.Alias())
		assert.Equal(t, expr.AggSum, agg.AggType())
		assert.Equal(t, "col(revenue)", agg.Column().String())
	})

	t.Run("aggregation alias chaining", func(t *testing.T) {
		baseAgg := expr.Mean(expr.Col("price"))
		aliasedAgg := baseAgg.As("avg_price")
		realiasedAgg := aliasedAgg.As("average_price")

		assert.Equal(t, "average_price", realiasedAgg.Alias())
		assert.Equal(t, expr.AggMean, realiasedAgg.AggType())
	})

	t.Run("comparison with aliased aggregation", func(t *testing.T) {
		avgScore := expr.Mean(expr.Col("score")).As("average_score")
		comparison := avgScore.Gt(expr.Lit(80.0))

		// The string representation should include the aggregation function
		assert.Contains(t, comparison.String(), "mean(col(score))")
		assert.Contains(t, comparison.String(), "lit(80)")
	})
}

func TestAggregationExprWithOtherAggregations(t *testing.T) {
	t.Run("aggregation comparison with aggregation", func(t *testing.T) {
		avgSalary := expr.Mean(expr.Col("salary"))
		maxSalary := expr.Max(expr.Col("salary"))

		// Test: AVG(salary) < MAX(salary)
		comparison := avgSalary.Lt(maxSalary)
		expected := "(mean(col(salary)) < max(col(salary)))"
		assert.Equal(t, expected, comparison.String())
	})

	t.Run("count comparison with sum", func(t *testing.T) {
		countEmployees := expr.Count(expr.Col("id"))
		sumDivision := expr.Sum(expr.Col("division_count"))

		// Test: COUNT(id) >= SUM(division_count)
		comparison := countEmployees.Ge(sumDivision)
		expected := "(count(col(id)) >= sum(col(division_count)))"
		assert.Equal(t, expected, comparison.String())
	})
}

func TestAggregationExprEvaluationBasics(t *testing.T) {
	// Test basic aggregation evaluation functionality
	mem := memory.NewGoAllocator()

	t.Run("aggregation creation and type checking", func(t *testing.T) {
		// Create test data
		data := series.New("salary", []float64{50000, 60000, 70000}, mem)
		defer data.Release()

		// Create aggregation expressions
		sumAgg := expr.Sum(expr.Col("salary"))
		countAgg := expr.Count(expr.Col("salary"))
		meanAgg := expr.Mean(expr.Col("salary"))

		// Verify aggregation types
		assert.Equal(t, expr.AggSum, sumAgg.AggType())
		assert.Equal(t, expr.AggCount, countAgg.AggType())
		assert.Equal(t, expr.AggMean, meanAgg.AggType())

		// Verify string representations
		assert.Equal(t, "sum(col(salary))", sumAgg.String())
		assert.Equal(t, "count(col(salary))", countAgg.String())
		assert.Equal(t, "mean(col(salary))", meanAgg.String())
	})
}

func TestAggregationExprComplexNesting(t *testing.T) {
	t.Run("nested aggregation comparisons", func(t *testing.T) {
		// Complex expression: (SUM(salary) > 100000) AND (COUNT(*) >= 5)
		sumCondition := expr.Sum(expr.Col("salary")).Gt(expr.Lit(100000))
		countCondition := expr.Count(expr.Lit(1)).Ge(expr.Lit(5))

		combinedCondition := sumCondition.And(countCondition)

		expected := "((sum(col(salary)) > lit(100000)) && (count(lit(1)) >= lit(5)))"
		assert.Equal(t, expected, combinedCondition.String())
	})

	t.Run("aggregation with arithmetic in comparison", func(t *testing.T) {
		// Expression: AVG(score) * 1.1 > 85
		avgExpr := expr.Mean(expr.Col("score"))
		scaledAvg := avgExpr.Mul(expr.Lit(1.1))
		condition := scaledAvg.Gt(expr.Lit(85.0))

		expected := "((mean(col(score)) * lit(1.1)) > lit(85))"
		assert.Equal(t, expected, condition.String())
	})
}
