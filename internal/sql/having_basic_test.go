//nolint:testpackage // requires internal access to unexported types and functions
package sql

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBasicHavingTranslation(t *testing.T) {
	mem := memory.NewGoAllocator()
	translator := NewSQLTranslator(mem)

	// Create test data
	departments := series.New(
		"department",
		[]string{"Engineering", "Sales", "Engineering", "Sales", "Engineering"},
		mem,
	)
	salaries := series.New("salary", []float64{90000, 75000, 85000, 80000, 95000}, mem)

	df := dataframe.New(departments, salaries)
	defer df.Release()

	translator.RegisterTable("employees", df)

	t.Run("Basic HAVING with aggregation", func(t *testing.T) {
		query := `
			SELECT department, AVG(salary) as avg_salary
			FROM employees
			GROUP BY department
			HAVING AVG(salary) > 80000
		`

		stmt, err := ParseSQL(query)
		require.NoError(t, err, "Failed to parse SQL")

		lazy, err := translator.TranslateStatement(stmt)
		require.NoError(t, err, "Failed to translate SQL")

		result, err := lazy.Collect()
		require.NoError(t, err, "Failed to collect result")
		defer result.Release()

		// Engineering avg = (90000 + 85000 + 95000) / 3 = 90000 (passes)
		// Sales avg = (75000 + 80000) / 2 = 77500 (filtered out)
		assert.Equal(t, 1, result.Len(), "Should have 1 row")

		// Check department
		deptCol, exists := result.Column("department")
		assert.True(t, exists, "Should have department column")

		// Verify it's Engineering
		deptArray := deptCol.Array()
		defer deptArray.Release()
		assert.Equal(t, "Engineering", deptArray.(*array.String).Value(0))
	})

	t.Run("HAVING validation prevents direct column reference", func(t *testing.T) {
		query := `
			SELECT department, AVG(salary) as avg_salary
			FROM employees
			GROUP BY department
			HAVING salary > 80000
		`

		stmt, err := ParseSQL(query)
		require.NoError(t, err, "Should parse SQL")

		_, err = translator.TranslateStatement(stmt)
		require.Error(t, err, "Should fail translation due to HAVING validation")
		assert.Contains(t, err.Error(), "HAVING validation error")
	})

	t.Run("HAVING validation requires GROUP BY", func(t *testing.T) {
		query := `
			SELECT department, salary
			FROM employees
			HAVING AVG(salary) > 80000
		`

		stmt, err := ParseSQL(query)
		require.NoError(t, err, "Should parse SQL")

		_, err = translator.TranslateStatement(stmt)
		require.Error(t, err, "Should fail translation")
		assert.Contains(t, err.Error(), "HAVING clause requires GROUP BY")
	})
}
