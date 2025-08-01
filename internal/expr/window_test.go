package expr_test

import (
	"testing"

	"github.com/paveg/gorilla/internal/expr"
	"github.com/stretchr/testify/assert"
)

func TestWindowSpec_Basic(t *testing.T) {
	tests := []struct {
		name     string
		window   *expr.WindowSpec
		expected string
	}{
		{
			name:     "empty window",
			window:   expr.NewWindow(),
			expected: "OVER ()",
		},
		{
			name:     "partition by single column",
			window:   expr.NewWindow().PartitionBy("department"),
			expected: "OVER (PARTITION BY department)",
		},
		{
			name:     "partition by multiple columns",
			window:   expr.NewWindow().PartitionBy("department", "location"),
			expected: "OVER (PARTITION BY department, location)",
		},
		{
			name:     "order by single column",
			window:   expr.NewWindow().OrderBy("salary", false),
			expected: "OVER (ORDER BY salary DESC)",
		},
		{
			name:     "order by multiple columns",
			window:   expr.NewWindow().OrderBy("department", true).OrderBy("salary", false),
			expected: "OVER (ORDER BY department ASC, salary DESC)",
		},
		{
			name: "complete window spec with frame",
			window: expr.NewWindow().
				PartitionBy("department").
				OrderBy("salary", false).
				Rows(expr.Between(expr.UnboundedPreceding(), expr.CurrentRow())),
			expected: "OVER (PARTITION BY department ORDER BY salary DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := tt.window.String()
			assert.Equal(t, tt.expected, actual)
		})
	}
}

// The following tests require access to unexported fields and are skipped
