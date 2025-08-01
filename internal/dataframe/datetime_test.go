//nolint:testpackage // requires internal access to unexported types and functions
package dataframe

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataFrameWithDateTime(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("DataFrame with timestamp column", func(t *testing.T) {
		// Create test data with timestamps
		names := []string{"Alice", "Bob", "Charlie"}
		timestamps := []time.Time{
			time.Date(2023, 1, 1, 9, 0, 0, 0, time.UTC),
			time.Date(2023, 1, 1, 10, 30, 0, 0, time.UTC),
			time.Date(2023, 1, 1, 11, 45, 0, 0, time.UTC),
		}
		values := []int64{100, 200, 300}

		// Create series
		nameSeries := series.New("name", names, mem)
		timestampSeries := series.New("timestamp", timestamps, mem)
		valueSeries := series.New("value", values, mem)

		defer nameSeries.Release()
		defer timestampSeries.Release()
		defer valueSeries.Release()

		// Create DataFrame
		df := New(nameSeries, timestampSeries, valueSeries)
		defer df.Release()

		// Verify basic properties
		assert.Equal(t, 3, df.Len())
		assert.Equal(t, 3, df.Width())
		assert.Equal(t, []string{"name", "timestamp", "value"}, df.Columns())

		// Verify column access
		tsCol, exists := df.Column("timestamp")
		require.True(t, exists)
		assert.Equal(t, "timestamp", tsCol.Name())
		assert.Equal(t, 3, tsCol.Len())
	})

	t.Run("DataFrame datetime operations", func(t *testing.T) {
		// Create events DataFrame with timestamps
		events := []string{"login", "purchase", "logout"}
		timestamps := []time.Time{
			time.Date(2023, 1, 1, 9, 0, 0, 0, time.UTC),
			time.Date(2023, 1, 1, 9, 15, 0, 0, time.UTC),
			time.Date(2023, 1, 1, 9, 30, 0, 0, time.UTC),
		}
		userIDs := []int64{1, 1, 1}

		eventSeries := series.New("event", events, mem)
		timestampSeries := series.New("timestamp", timestamps, mem)
		userSeries := series.New("user_id", userIDs, mem)

		defer eventSeries.Release()
		defer timestampSeries.Release()
		defer userSeries.Release()

		df := New(eventSeries, timestampSeries, userSeries)
		defer df.Release()

		// Test selection with timestamp column
		selected := df.Select("timestamp", "event")
		defer selected.Release()

		assert.Equal(t, 3, selected.Len())
		assert.Equal(t, 2, selected.Width())
		assert.Equal(t, []string{"timestamp", "event"}, selected.Columns())

		// Verify timestamp data is preserved
		tsCol, exists := selected.Column("timestamp")
		require.True(t, exists)
		assert.Equal(t, 3, tsCol.Len())
	})

	t.Run("DataFrame slicing with timestamps", func(t *testing.T) {
		timestamps := []time.Time{
			time.Date(2023, 1, 1, 8, 0, 0, 0, time.UTC),
			time.Date(2023, 1, 1, 9, 0, 0, 0, time.UTC),
			time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC),
			time.Date(2023, 1, 1, 11, 0, 0, 0, time.UTC),
			time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
		}
		values := []int64{10, 20, 30, 40, 50}

		timestampSeries := series.New("timestamp", timestamps, mem)
		valueSeries := series.New("value", values, mem)

		defer timestampSeries.Release()
		defer valueSeries.Release()

		df := New(timestampSeries, valueSeries)
		defer df.Release()

		// Test slicing
		sliced := df.Slice(1, 4)
		defer sliced.Release()

		assert.Equal(t, 3, sliced.Len()) // indices 1, 2, 3
		assert.Equal(t, 2, sliced.Width())

		// Verify timestamp data in slice
		tsCol, exists := sliced.Column("timestamp")
		require.True(t, exists)
		assert.Equal(t, 3, tsCol.Len())
	})

	t.Run("Empty DataFrame with timestamp column", func(t *testing.T) {
		var timestamps []time.Time
		var values []int64

		timestampSeries := series.New("timestamp", timestamps, mem)
		valueSeries := series.New("value", values, mem)

		defer timestampSeries.Release()
		defer valueSeries.Release()

		df := New(timestampSeries, valueSeries)
		defer df.Release()

		assert.Equal(t, 0, df.Len())
		assert.Equal(t, 2, df.Width())
		assert.Equal(t, []string{"timestamp", "value"}, df.Columns())
	})
}

func TestDataFrameStringWithDateTime(t *testing.T) {
	mem := memory.NewGoAllocator()

	timestamps := []time.Time{
		time.Date(2023, 1, 1, 9, 0, 0, 0, time.UTC),
	}
	values := []int64{100}

	timestampSeries := series.New("timestamp", timestamps, mem)
	valueSeries := series.New("value", values, mem)

	defer timestampSeries.Release()
	defer valueSeries.Release()

	df := New(timestampSeries, valueSeries)
	defer df.Release()

	str := df.String()
	assert.Contains(t, str, "DataFrame")
	assert.Contains(t, str, "timestamp")
	assert.Contains(t, str, "value")
	// DataFrame format shows as "DataFrame[1x2]" not "1 rows, 2 columns"
	assert.Contains(t, str, "1x2")
}
