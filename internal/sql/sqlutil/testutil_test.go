package sqlutil_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/sql/sqlutil"
	"github.com/stretchr/testify/assert"
)

func TestCreateTestDataFrame(t *testing.T) {
	allocator := memory.NewGoAllocator()

	t.Run("default config", func(t *testing.T) {
		df := sqlutil.CreateTestDataFrame(allocator, nil)
		defer df.Release()

		assert.Equal(t, 4, df.Len())
		assert.Equal(t, 5, df.Width()) // name, age, department, salary, active

		expectedColumns := []string{"name", "age", "department", "salary", "active"}
		for _, col := range expectedColumns {
			assert.True(t, df.HasColumn(col), "Missing column: %s", col)
		}
	})

	t.Run("custom config", func(t *testing.T) {
		config := &sqlutil.TestDataFrameConfig{
			IncludeActive: false,
			RowCount:      10,
		}

		df := sqlutil.CreateTestDataFrame(allocator, config)
		defer df.Release()

		assert.Equal(t, 10, df.Len())
		assert.Equal(t, 4, df.Width()) // name, age, department, salary (no active)

		assert.True(t, df.HasColumn("name"))
		assert.True(t, df.HasColumn("age"))
		assert.True(t, df.HasColumn("department"))
		assert.True(t, df.HasColumn("salary"))
		assert.False(t, df.HasColumn("active"))
	})

	t.Run("custom data", func(t *testing.T) {
		customData := map[string]interface{}{
			"id":    []int64{1, 2, 3},
			"name":  []string{"Test1", "Test2", "Test3"},
			"score": []float64{1.1, 2.2, 3.3},
			"pass":  []bool{true, false, true},
		}

		config := &sqlutil.TestDataFrameConfig{
			CustomData: customData,
		}

		df := sqlutil.CreateTestDataFrame(allocator, config)
		defer df.Release()

		assert.Equal(t, 3, df.Len())
		assert.Equal(t, 4, df.Width())

		for colName := range customData {
			assert.True(t, df.HasColumn(colName), "Missing custom column: %s", colName)
		}
	})
}

func TestCreateTestDataFrameFromData(t *testing.T) {
	allocator := memory.NewGoAllocator()

	data := map[string]interface{}{
		"names":  []string{"Alice", "Bob"},
		"ages":   []int64{25, 30},
		"scores": []float64{85.5, 92.3},
		"active": []bool{true, false},
	}

	df := sqlutil.CreateTestDataFrameFromData(allocator, data)
	defer df.Release()

	assert.Equal(t, 2, df.Len())
	assert.Equal(t, 4, df.Width())

	for colName := range data {
		assert.True(t, df.HasColumn(colName), "Missing column: %s", colName)
	}
}

// Note: TestGenerateTestData functions are not tested here because
// generateTest* functions are unexported internal helpers
