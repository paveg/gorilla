//nolint:testpackage // requires internal access to unexported types and functions
package dataframe

import (
	"math/rand"
	"testing"
	"testing/quick"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
)

// TestDataFrameFilterProperties tests DataFrame filter properties using property-based testing.
func TestDataFrameFilterProperties(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based tests in short mode")
	}

	// Property: Filter then count should be <= original count
	property1 := func(data []int64, threshold int64) bool {
		if len(data) == 0 {
			return true
		}

		mem := memory.NewGoAllocator()
		dataSeries := series.New("values", data, mem)
		df := New(dataSeries)
		defer df.Release()

		filtered, err := df.Lazy().
			Filter(expr.Col("values").Gt(expr.Lit(threshold))).
			Collect()
		if err != nil {
			return false
		}
		defer filtered.Release()

		return filtered.Len() <= df.Len()
	}

	config := &quick.Config{MaxCount: 50} // Reduced for faster testing
	if err := quick.Check(property1, config); err != nil {
		t.Errorf("Filter count property failed: %v", err)
	}

	// Property: Filter with always-false condition should return empty DataFrame
	property2 := func(data []int64) bool {
		if len(data) == 0 {
			return true
		}

		mem := memory.NewGoAllocator()
		dataSeries := series.New("values", data, mem)
		df := New(dataSeries)
		defer df.Release()

		// Filter with condition that's always false
		maxVal := int64(0)
		for _, v := range data {
			if v > maxVal {
				maxVal = v
			}
		}

		filtered, err := df.Lazy().
			Filter(expr.Col("values").Gt(expr.Lit(maxVal + 1000))).
			Collect()
		if err != nil {
			return false
		}
		defer filtered.Release()

		return filtered.Len() == 0
	}

	if err := quick.Check(property2, config); err != nil {
		t.Errorf("Always-false filter property failed: %v", err)
	}
}

// TestDataFrameSortProperties tests DataFrame sort properties.
func TestDataFrameSortProperties(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based tests in short mode")
	}

	// Property: Sort should preserve row count
	property1 := func(data []int64) bool {
		if len(data) == 0 {
			return true
		}

		mem := memory.NewGoAllocator()
		dataSeries := series.New("values", data, mem)
		df := New(dataSeries)
		defer df.Release()

		sorted, err := df.Sort("values", true)
		if err != nil {
			return false
		}
		defer sorted.Release()

		return sorted.Len() == df.Len()
	}

	config := &quick.Config{MaxCount: 50}
	if err := quick.Check(property1, config); err != nil {
		t.Errorf("Sort count property failed: %v", err)
	}

	// Property: Sort should maintain column structure
	property2 := func(data []int64) bool {
		if len(data) == 0 {
			return true
		}

		mem := memory.NewGoAllocator()
		dataSeries := series.New("values", data, mem)
		df := New(dataSeries)
		defer df.Release()

		sorted, err := df.Sort("values", true)
		if err != nil {
			return false
		}
		defer sorted.Release()

		return len(sorted.Columns()) == len(df.Columns()) && sorted.Width() == df.Width()
	}

	if err := quick.Check(property2, config); err != nil {
		t.Errorf("Sort structure property failed: %v", err)
	}
}

// TestDataFrameWithColumnProperties tests WithColumn operation properties.
func TestDataFrameWithColumnProperties(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based tests in short mode")
	}

	// Property: Adding a column should increase width by 1
	property1 := func(data []int64, multiplier int64) bool {
		if len(data) == 0 || multiplier == 0 {
			return true // Skip degenerate cases
		}

		mem := memory.NewGoAllocator()
		dataSeries := series.New("values", data, mem)
		df := New(dataSeries)
		defer df.Release()

		originalWidth := df.Width()

		result, err := df.Lazy().
			WithColumn("multiplied", expr.Col("values").Mul(expr.Lit(multiplier))).
			Collect()
		if err != nil {
			return false
		}
		defer result.Release()

		return result.Width() == originalWidth+1
	}

	config := &quick.Config{MaxCount: 50}
	if err := quick.Check(property1, config); err != nil {
		t.Errorf("WithColumn width property failed: %v", err)
	}

	// Property: Adding a column should preserve row count
	property2 := func(data []int64) bool {
		if len(data) == 0 {
			return true
		}

		mem := memory.NewGoAllocator()
		dataSeries := series.New("values", data, mem)
		df := New(dataSeries)
		defer df.Release()

		result, err := df.Lazy().
			WithColumn("doubled", expr.Col("values").Mul(expr.Lit(int64(2)))).
			Collect()
		if err != nil {
			return false
		}
		defer result.Release()

		return result.Len() == df.Len()
	}

	if err := quick.Check(property2, config); err != nil {
		t.Errorf("WithColumn count property failed: %v", err)
	}
}

// TestDataFrameGroupByProperties tests GroupBy operation properties.
func TestDataFrameGroupByProperties(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based tests in short mode")
	}

	// Property: GroupBy result should have at most as many rows as unique group values
	property1 := func(seed int64) bool {
		rng := rand.New(rand.NewSource(seed))
		
		// Create test data with known cardinality
		size := 100
		data := make([]string, size)
		values := make([]int64, size)
		
		// Create data with at most 5 unique groups
		groups := []string{"A", "B", "C", "D", "E"}
		for i := range size {
			data[i] = groups[rng.Intn(len(groups))]
			values[i] = rng.Int63n(100)
		}

		mem := memory.NewGoAllocator()
		groupSeries := series.New("group", data, mem)
		valueSeries := series.New("value", values, mem)
		df := New(groupSeries, valueSeries)
		defer df.Release()

		result, err := df.Lazy().
			GroupBy("group").
			Agg(expr.Count(expr.Col("value")).As("count")).
			Collect()
		if err != nil {
			return false
		}
		defer result.Release()

		// Should have at most 5 groups (and at least 1 if we have data)
		return result.Len() <= len(groups) && result.Len() >= 1
	}

	config := &quick.Config{MaxCount: 20} // Reduced for performance
	if err := quick.Check(property1, config); err != nil {
		t.Errorf("GroupBy cardinality property failed: %v", err)
	}
}

// TestDataFrameInvariantProperties tests general DataFrame invariants.
func TestDataFrameInvariantProperties(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based tests in short mode")
	}

	// Property: DataFrame operations should maintain basic invariants
	property1 := func(data []int64) bool {
		if len(data) == 0 {
			return true
		}

		mem := memory.NewGoAllocator()
		dataSeries := series.New("values", data, mem)
		df := New(dataSeries)
		defer df.Release()

		// Len and Width should always be non-negative
		if df.Len() < 0 || df.Width() < 0 {
			return false
		}

		// Width should equal number of columns
		if df.Width() != len(df.Columns()) {
			return false
		}

		// Column names should be unique
		cols := df.Columns()
		seen := make(map[string]bool)
		for _, col := range cols {
			if seen[col] {
				return false // Duplicate column name
			}
			seen[col] = true
		}

		return true
	}

	config := &quick.Config{MaxCount: 100}
	if err := quick.Check(property1, config); err != nil {
		t.Errorf("DataFrame invariant property failed: %v", err)
	}
}

// TestCustomPropertyExample demonstrates custom property testing.
func TestCustomPropertyExample(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based tests in short mode")
	}

	// Simpler, more reliable property: Adding a constant column should preserve row count
	property := func(data []int64, constant int64) bool {
		if len(data) == 0 {
			return true
		}

		mem := memory.NewGoAllocator()
		dataSeries := series.New("values", data, mem)
		df := New(dataSeries)
		defer df.Release()

		originalLen := df.Len()

		// Add a constant column
		result, err := df.Lazy().
			WithColumn("constant", expr.Lit(constant)).
			Collect()

		if err != nil {
			return false
		}
		defer result.Release()

		// Row count should be preserved and width should increase
		return result.Len() == originalLen && result.Width() == df.Width()+1
	}

	config := &quick.Config{MaxCount: 50}
	err := quick.Check(property, config)
	assert.NoError(t, err, "Adding constant column should preserve row count")
}