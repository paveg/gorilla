package sqlutil_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/sql/sqlutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBaseTranslator(t *testing.T) {
	allocator := memory.NewGoAllocator()
	bt := sqlutil.NewBaseTranslator(allocator)

	t.Run("empty initially", func(t *testing.T) {
		tables := bt.GetRegisteredTables()
		assert.Empty(t, tables)
	})

	t.Run("register and retrieve table", func(t *testing.T) {
		// Create test DataFrame
		config := sqlutil.DefaultTestDataFrameConfig()
		df := sqlutil.CreateTestDataFrame(allocator, config)
		defer df.Release()

		// Register table
		bt.RegisterTable("test_table", df)

		// Verify registration
		tables := bt.GetRegisteredTables()
		assert.Len(t, tables, 1)
		assert.Contains(t, tables, "test_table")

		// Retrieve table
		retrievedDF, exists := bt.GetTable("test_table")
		assert.True(t, exists)
		assert.NotNil(t, retrievedDF)
	})

	t.Run("validate table exists", func(t *testing.T) {
		// Valid table
		err := bt.ValidateTableExists("test_table")
		require.NoError(t, err)

		// Invalid table
		err = bt.ValidateTableExists("nonexistent_table")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "table not found: nonexistent_table")
	})

	t.Run("clear tables", func(t *testing.T) {
		bt.ClearTables()
		tables := bt.GetRegisteredTables()
		assert.Empty(t, tables)
	})

	t.Run("get allocator", func(t *testing.T) {
		returnedAllocator := bt.GetAllocator()
		assert.Equal(t, allocator, returnedAllocator)
	})

	t.Run("standardize columns", func(t *testing.T) {
		input := []string{"Name", "AGE", "department"}
		result := bt.StandardizeColumns(input)
		assert.Equal(t, input, result) // Currently returns as-is
	})
}

func TestHandleCommonErrors(t *testing.T) {
	allocator := memory.NewGoAllocator()
	bt := sqlutil.NewBaseTranslator(allocator)

	tests := []struct {
		name      string
		operation string
		inputErr  error
		expectErr bool
		contains  string
	}{
		{
			name:      "nil error",
			operation: "test",
			inputErr:  nil,
			expectErr: false,
		},
		{
			name:      "generic error",
			operation: "parse",
			inputErr:  assert.AnError,
			expectErr: true,
			contains:  "parse failed:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := bt.HandleCommonErrors(tt.operation, tt.inputErr)

			if tt.expectErr {
				require.Error(t, err)
				if tt.contains != "" {
					assert.Contains(t, err.Error(), tt.contains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}
