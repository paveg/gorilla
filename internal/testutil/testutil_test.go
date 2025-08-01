package testutil_test

import (
	"testing"

	"github.com/paveg/gorilla/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetupMemoryTest(t *testing.T) {
	mem := testutil.SetupMemoryTest(t)
	defer mem.Release()

	// Test that allocator is not nil
	require.NotNil(t, mem.Allocator)

	// Test that we can use the allocator
	df := testutil.CreateTestDataFrame(mem.Allocator)
	defer df.Release()

	assert.NotNil(t, df)
}

func TestCreateTestDataFrame(t *testing.T) {
	mem := testutil.SetupMemoryTest(t)
	defer mem.Release()

	t.Run("default configuration", func(t *testing.T) {
		df := testutil.CreateTestDataFrame(mem.Allocator)
		defer df.Release()

		// Test default structure
		assert.Equal(t, 4, df.Len())
		assert.Equal(t, 4, df.Width()) // name, age, department, salary

		expectedColumns := []string{"name", "age", "department", "salary"}
		testutil.AssertDataFrameHasColumns(t, df, expectedColumns)
	})

	t.Run("with active column", func(t *testing.T) {
		df := testutil.CreateTestDataFrame(mem.Allocator, testutil.WithActiveColumn())
		defer df.Release()

		assert.Equal(t, 4, df.Len())
		assert.Equal(t, 5, df.Width()) // includes active column
		assert.True(t, df.HasColumn("active"))
	})

	t.Run("with custom row count", func(t *testing.T) {
		df := testutil.CreateTestDataFrame(mem.Allocator, testutil.WithRowCount(10))
		defer df.Release()

		assert.Equal(t, 10, df.Len())
		assert.Equal(t, 4, df.Width())
	})
}

func TestCreateSimpleTestDataFrame(t *testing.T) {
	mem := testutil.SetupMemoryTest(t)
	defer mem.Release()

	df := testutil.CreateSimpleTestDataFrame(mem.Allocator)
	defer df.Release()

	assert.Equal(t, 2, df.Len())
	assert.Equal(t, 2, df.Width())

	expectedColumns := []string{"name", "age"}
	testutil.AssertDataFrameHasColumns(t, df, expectedColumns)
}

func TestAssertDataFrameEqual(t *testing.T) {
	mem := testutil.SetupMemoryTest(t)
	defer mem.Release()

	df1 := testutil.CreateSimpleTestDataFrame(mem.Allocator)
	defer df1.Release()

	df2 := testutil.CreateSimpleTestDataFrame(mem.Allocator)
	defer df2.Release()

	// Should not panic - DataFrames should be equal
	testutil.AssertDataFrameEqual(t, df1, df2)
}

func TestAssertDataFrameHasColumns(t *testing.T) {
	mem := testutil.SetupMemoryTest(t)
	defer mem.Release()

	df := testutil.CreateTestDataFrame(mem.Allocator)
	defer df.Release()

	expectedColumns := []string{"name", "age", "department", "salary"}
	testutil.AssertDataFrameHasColumns(t, df, expectedColumns)
}

func TestAssertDataFrameNotEmpty(t *testing.T) {
	mem := testutil.SetupMemoryTest(t)
	defer mem.Release()

	df := testutil.CreateTestDataFrame(mem.Allocator)
	defer df.Release()

	testutil.AssertDataFrameNotEmpty(t, df)
}

// TestMemoryContextCleanup verifies that the memory context can be safely released.
func TestMemoryContextCleanup(t *testing.T) {
	mem := testutil.SetupMemoryTest(t)

	// Create some objects that use the allocator
	df := testutil.CreateTestDataFrame(mem.Allocator)
	defer df.Release()

	// Should not panic
	mem.Release()

	// Calling Release multiple times should be safe
	mem.Release()
}

// BenchmarkCreateTestDataFrame benchmarks the test DataFrame creation.
func BenchmarkCreateTestDataFrame(b *testing.B) {
	mem := testutil.SetupMemoryTest(b)
	defer mem.Release()

	b.ResetTimer()
	for range b.N {
		df := testutil.CreateTestDataFrame(mem.Allocator)
		df.Release()
	}
}
