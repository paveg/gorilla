package sqlutil_test

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/sql/sqlutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSafeInt64ToInt(t *testing.T) {
	tests := []struct {
		name      string
		input     int64
		expected  int
		expectErr bool
		errMsg    string
	}{
		{
			name:      "valid positive",
			input:     100,
			expected:  100,
			expectErr: false,
		},
		{
			name:      "zero",
			input:     0,
			expected:  0,
			expectErr: false,
		},
		{
			name:      "negative",
			input:     -1,
			expected:  0,
			expectErr: true,
			errMsg:    "negative value not allowed",
		},
		{
			name:      "max int",
			input:     int64(math.MaxInt),
			expected:  math.MaxInt,
			expectErr: false,
		},
		{
			name:      "large value on 32-bit",
			input:     math.MaxInt32 + 1,
			expected:  math.MaxInt32 + 1,
			expectErr: false, // This will work on 64-bit systems
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := sqlutil.SafeInt64ToInt(tt.input)

			if tt.expectErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestCreateEmptyDataFrame(t *testing.T) {
	allocator := memory.NewGoAllocator()

	// Create test DataFrame
	df := sqlutil.CreateTestDataFrame(allocator, nil)
	defer df.Release()

	// Verify original DataFrame has expected structure
	require.Equal(t, 4, df.Len())
	require.Equal(t, 5, df.Width())

	// Create empty DataFrame from template
	empty := sqlutil.CreateEmptyDataFrame(df)
	defer empty.Release()

	// Verify empty DataFrame has no rows (column structure may vary based on DataFrame implementation)
	assert.Equal(t, 0, empty.Len())
}

func TestApplyLimitOffset(t *testing.T) {
	allocator := memory.NewGoAllocator()

	// Create test DataFrame with 10 rows
	config := &sqlutil.TestDataFrameConfig{
		IncludeActive: true,
		RowCount:      10,
	}
	df := sqlutil.CreateTestDataFrame(allocator, config)
	defer df.Release()

	t.Run("nil DataFrame", func(t *testing.T) {
		result, err := sqlutil.ApplyLimitOffset(nil, 5, 0)
		require.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "DataFrame cannot be nil")
	})

	t.Run("simple limit", func(t *testing.T) {
		result, err := sqlutil.ApplyLimitOffset(df, 5, 0)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 5, result.Len())
		assert.Equal(t, df.Width(), result.Width())
	})

	t.Run("limit with offset", func(t *testing.T) {
		result, err := sqlutil.ApplyLimitOffset(df, 3, 2)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 3, result.Len())
		assert.Equal(t, df.Width(), result.Width())
	})

	t.Run("offset only (limit = -1)", func(t *testing.T) {
		result, err := sqlutil.ApplyLimitOffset(df, -1, 3)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 7, result.Len()) // 10 - 3 = 7
		assert.Equal(t, df.Width(), result.Width())
	})

	t.Run("offset beyond bounds", func(t *testing.T) {
		result, err := sqlutil.ApplyLimitOffset(df, 5, 20)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 0, result.Len()) // Empty result
	})

	t.Run("limit zero", func(t *testing.T) {
		result, err := sqlutil.ApplyLimitOffset(df, 0, 0)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 0, result.Len())
	})

	t.Run("limit exceeds available rows", func(t *testing.T) {
		result, err := sqlutil.ApplyLimitOffset(df, 100, 0)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 10, result.Len()) // All available rows
		assert.Equal(t, df.Width(), result.Width())
	})

	t.Run("negative offset", func(t *testing.T) {
		result, err := sqlutil.ApplyLimitOffset(df, 5, -1)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 0, result.Len()) // Empty due to invalid offset
	})

	t.Run("large limit that works", func(t *testing.T) {
		result, err := sqlutil.ApplyLimitOffset(df, 1000, 0)
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 10, result.Len()) // All available rows
		assert.Equal(t, df.Width(), result.Width())
	})
}
