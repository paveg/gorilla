package series

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDateTimeSeries(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("Time series creation", func(t *testing.T) {
		times := []time.Time{
			time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
			time.Date(2023, 1, 2, 13, 30, 0, 0, time.UTC),
			time.Date(2023, 1, 3, 14, 45, 0, 0, time.UTC),
		}

		series := New("timestamps", times, mem)
		defer series.Release()

		assert.Equal(t, "timestamps", series.Name())
		assert.Equal(t, 3, series.Len())

		// Verify we can retrieve the values
		values := series.Values()
		assert.Len(t, values, 3)
		assert.Equal(t, times[0], values[0])
		assert.Equal(t, times[1], values[1])
		assert.Equal(t, times[2], values[2])
	})

	t.Run("Time series with timezone", func(t *testing.T) {
		// Test with different timezone - Note: current implementation converts to UTC
		location, err := time.LoadLocation("America/New_York")
		require.NoError(t, err)

		times := []time.Time{
			time.Date(2023, 1, 1, 12, 0, 0, 0, location),
			time.Date(2023, 1, 2, 13, 30, 0, 0, location),
		}

		series := New("timestamps_tz", times, mem)
		defer series.Release()

		assert.Equal(t, "timestamps_tz", series.Name())
		assert.Equal(t, 2, series.Len())

		values := series.Values()
		// Current implementation converts to UTC, so we compare the equivalent UTC times
		expectedUTC1 := times[0].UTC()
		expectedUTC2 := times[1].UTC()

		assert.Equal(t, expectedUTC1, values[0])
		assert.Equal(t, expectedUTC2, values[1])

		// Current implementation stores all times in UTC
		assert.Equal(t, time.UTC, values[0].Location())
		assert.Equal(t, time.UTC, values[1].Location())
	})

	t.Run("Date only series", func(t *testing.T) {
		// Test date-only functionality (midnight UTC)
		dates := []time.Time{
			time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC),
			time.Date(2023, 1, 3, 0, 0, 0, 0, time.UTC),
		}

		series := New("dates", dates, mem)
		defer series.Release()

		assert.Equal(t, "dates", series.Name())
		assert.Equal(t, 3, series.Len())

		values := series.Values()
		for i, date := range dates {
			assert.Equal(t, date, values[i])
			// Verify it's midnight UTC
			assert.Equal(t, 0, values[i].Hour())
			assert.Equal(t, 0, values[i].Minute())
			assert.Equal(t, 0, values[i].Second())
			assert.Equal(t, time.UTC, values[i].Location())
		}
	})

	t.Run("Time series value access", func(t *testing.T) {
		times := []time.Time{
			time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
			time.Date(2023, 1, 2, 13, 30, 0, 0, time.UTC),
		}

		series := New("timestamps", times, mem)
		defer series.Release()

		// Test individual value access
		assert.Equal(t, times[0], series.Value(0))
		assert.Equal(t, times[1], series.Value(1))

		// Test out of bounds access
		var zeroTime time.Time
		assert.Equal(t, zeroTime, series.Value(-1))
		assert.Equal(t, zeroTime, series.Value(2))
	})

	t.Run("Empty time series", func(t *testing.T) {
		var times []time.Time

		series := New("empty_timestamps", times, mem)
		defer series.Release()

		assert.Equal(t, "empty_timestamps", series.Name())
		assert.Equal(t, 0, series.Len())
		assert.Empty(t, series.Values())
	})
}

func TestDateTimeSeriesDataType(t *testing.T) {
	mem := memory.NewGoAllocator()

	times := []time.Time{
		time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
	}

	series := New("timestamps", times, mem)
	defer series.Release()

	dataType := series.DataType()
	assert.NotNil(t, dataType)

	// Verify it's a timestamp type
	assert.Equal(t, "timestamp[ns]", dataType.String())
}

func TestDateTimeSeriesString(t *testing.T) {
	mem := memory.NewGoAllocator()

	times := []time.Time{
		time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
	}

	series := New("timestamps", times, mem)
	defer series.Release()

	str := series.String()
	// The reflection shows "Time" instead of "time.Time" for the type
	assert.Contains(t, str, "Series[Time]")
	assert.Contains(t, str, "timestamps")
	assert.Contains(t, str, "len=1")
}
