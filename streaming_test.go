package gorilla

import (
	"errors"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockChunkReader implements ChunkReader for testing.
type MockChunkReader struct {
	chunks [][]int64
	index  int
	closed bool
}

func NewMockChunkReader(chunks [][]int64) *MockChunkReader {
	return &MockChunkReader{
		chunks: chunks,
		index:  0,
	}
}

func (mcr *MockChunkReader) ReadChunk() (*DataFrame, error) {
	if mcr.closed {
		return nil, errors.New("reader is closed")
	}

	if mcr.index >= len(mcr.chunks) {
		return nil, io.EOF
	}

	mem := memory.NewGoAllocator()
	s1 := series.New("values", mcr.chunks[mcr.index], mem)
	mcr.index++

	return NewDataFrame(s1), nil
}

func (mcr *MockChunkReader) HasNext() bool {
	return mcr.index < len(mcr.chunks)
}

func (mcr *MockChunkReader) Close() error {
	mcr.closed = true
	return nil
}

// MockChunkWriter implements ChunkWriter for testing.
type MockChunkWriter struct {
	chunks []*DataFrame
	closed bool
}

func NewMockChunkWriter() *MockChunkWriter {
	return &MockChunkWriter{
		chunks: make([]*DataFrame, 0),
	}
}

func (mcw *MockChunkWriter) WriteChunk(df *DataFrame) error {
	if mcw.closed {
		return errors.New("writer is closed")
	}

	mcw.chunks = append(mcw.chunks, df)
	return nil
}

func (mcw *MockChunkWriter) Close() error {
	mcw.closed = true
	// Release all chunks
	for _, chunk := range mcw.chunks {
		if chunk != nil {
			chunk.Release()
		}
	}
	mcw.chunks = nil
	return nil
}

func (mcw *MockChunkWriter) GetChunks() []*DataFrame {
	return mcw.chunks
}

// MockStreamingOperation implements StreamingOperation for testing.
type MockStreamingOperation struct {
	applied  int
	released bool
}

func NewMockStreamingOperation() *MockStreamingOperation {
	return &MockStreamingOperation{}
}

func (mso *MockStreamingOperation) Apply(df *DataFrame) (*DataFrame, error) {
	mso.applied++
	// Return the same DataFrame for simplicity
	return df, nil
}

func (mso *MockStreamingOperation) Release() {
	mso.released = true
}

func (mso *MockStreamingOperation) TimesApplied() int {
	return mso.applied
}

func (mso *MockStreamingOperation) IsReleased() bool {
	return mso.released
}

// TestStreamingProcessor tests the streaming processor functionality.
func TestStreamingProcessor(t *testing.T) {
	t.Run("processes chunks successfully", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		monitor := NewMemoryUsageMonitor(1024 * 1024)
		defer monitor.StopMonitoring()

		processor := NewStreamingProcessor(100, mem, monitor)
		defer func() { _ = processor.Close() }()

		// Create test data
		chunks := [][]int64{
			{1, 2, 3},
			{4, 5, 6},
			{7, 8, 9},
		}

		reader := NewMockChunkReader(chunks)
		writer := NewMockChunkWriter()
		defer func() { _ = writer.Close() }()

		operation := NewMockStreamingOperation()
		operations := []StreamingOperation{operation}

		// Process streaming data
		err := processor.ProcessStreaming(reader, writer, operations)
		require.NoError(t, err)

		// Verify results
		assert.Equal(t, 3, operation.TimesApplied())
		assert.True(t, operation.IsReleased())
		assert.Len(t, writer.GetChunks(), 3)
	})

	t.Run("handles empty chunks gracefully", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		monitor := NewMemoryUsageMonitor(1024 * 1024)
		defer monitor.StopMonitoring()

		processor := NewStreamingProcessor(100, mem, monitor)
		defer func() { _ = processor.Close() }()

		// Create reader with no chunks
		reader := NewMockChunkReader([][]int64{})
		writer := NewMockChunkWriter()
		defer func() { _ = writer.Close() }()

		operation := NewMockStreamingOperation()
		operations := []StreamingOperation{operation}

		// Process streaming data
		err := processor.ProcessStreaming(reader, writer, operations)
		require.NoError(t, err)

		// Verify no operations were applied
		assert.Equal(t, 0, operation.TimesApplied())
		assert.True(t, operation.IsReleased())
		assert.Empty(t, writer.GetChunks())
	})

	t.Run("handles reader errors", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		monitor := NewMemoryUsageMonitor(1024 * 1024)
		defer monitor.StopMonitoring()

		processor := NewStreamingProcessor(100, mem, monitor)
		defer func() { _ = processor.Close() }()

		// Create reader that will be closed before processing
		reader := NewMockChunkReader([][]int64{{1, 2, 3}})
		_ = reader.Close() // Close reader to trigger error

		writer := NewMockChunkWriter()
		defer func() { _ = writer.Close() }()

		operation := NewMockStreamingOperation()
		operations := []StreamingOperation{operation}

		// Process streaming data should fail
		err := processor.ProcessStreaming(reader, writer, operations)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read chunk")
	})

	t.Run("handles closed processor", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		monitor := NewMemoryUsageMonitor(1024 * 1024)
		defer monitor.StopMonitoring()

		processor := NewStreamingProcessor(100, mem, monitor)
		_ = processor.Close() // Close processor before use

		reader := NewMockChunkReader([][]int64{{1, 2, 3}})
		writer := NewMockChunkWriter()
		defer func() { _ = writer.Close() }()

		operation := NewMockStreamingOperation()
		operations := []StreamingOperation{operation}

		// Process streaming data should fail
		err := processor.ProcessStreaming(reader, writer, operations)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "streaming processor is closed")
	})
}

// TestMemoryAwareChunkReader tests the memory-aware chunk reader.
func TestMemoryAwareChunkReader(t *testing.T) {
	t.Run("records memory usage for chunks", func(t *testing.T) {
		monitor := NewMemoryUsageMonitor(1024 * 1024)
		defer monitor.StopMonitoring()

		baseReader := NewMockChunkReader([][]int64{{1, 2, 3}})
		reader := NewMemoryAwareChunkReader(baseReader, monitor)

		// Read a chunk
		chunk, err := reader.ReadChunk()
		require.NoError(t, err)
		defer chunk.Release()

		// Verify memory usage was recorded
		assert.Positive(t, monitor.CurrentUsage())
		assert.Equal(t, int64(1), monitor.GetStats().ActiveAllocations)
	})

	t.Run("forwards reader methods correctly", func(t *testing.T) {
		monitor := NewMemoryUsageMonitor(1024 * 1024)
		defer monitor.StopMonitoring()

		baseReader := NewMockChunkReader([][]int64{{1, 2, 3}, {4, 5, 6}})
		reader := NewMemoryAwareChunkReader(baseReader, monitor)

		// Test HasNext
		assert.True(t, reader.HasNext())

		// Read first chunk
		chunk1, err := reader.ReadChunk()
		require.NoError(t, err)
		defer chunk1.Release()

		// Still has next
		assert.True(t, reader.HasNext())

		// Read second chunk
		chunk2, err := reader.ReadChunk()
		require.NoError(t, err)
		defer chunk2.Release()

		// No more chunks
		assert.False(t, reader.HasNext())

		// Close reader
		err = reader.Close()
		require.NoError(t, err)
	})
}

// TestSpillableBatch tests the spillable batch functionality.
func TestSpillableBatch(t *testing.T) {
	t.Run("creates and releases batch", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		s1 := series.New("test", []int64{1, 2, 3}, mem)
		df := NewDataFrame(s1)

		batch := NewSpillableBatch(df)
		defer batch.Release()

		// Get data should work
		data, err := batch.GetData()
		require.NoError(t, err)
		assert.Equal(t, df, data)
	})

	t.Run("spills and restores data", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		s1 := series.New("test", []int64{1, 2, 3}, mem)
		df := NewDataFrame(s1)

		batch := NewSpillableBatch(df)
		defer batch.Release()

		// Spill the batch
		err := batch.Spill()
		require.NoError(t, err)

		// Try to get data (should fail as loading from spill is not implemented)
		data, err := batch.GetData()
		assert.Error(t, err)
		assert.Nil(t, data)
		assert.Contains(t, err.Error(), "loading from spill not implemented")
	})

	t.Run("handles multiple spill calls", func(t *testing.T) {
		mem := memory.NewGoAllocator()
		s1 := series.New("test", []int64{1, 2, 3}, mem)
		df := NewDataFrame(s1)

		batch := NewSpillableBatch(df)
		defer batch.Release()

		// Spill multiple times should not cause error
		err := batch.Spill()
		require.NoError(t, err)

		err = batch.Spill()
		require.NoError(t, err)
	})
}

// TestBatchManager tests the batch manager functionality.
func TestBatchManager(t *testing.T) {
	t.Run("manages multiple batches", func(t *testing.T) {
		monitor := NewMemoryUsageMonitor(1024 * 1024)
		defer monitor.StopMonitoring()

		manager := NewBatchManager(monitor)

		// Create test data
		mem := memory.NewGoAllocator()
		s1 := series.New("test1", []int64{1, 2, 3}, mem)
		s2 := series.New("test2", []int64{4, 5, 6}, mem)
		df1 := NewDataFrame(s1)
		df2 := NewDataFrame(s2)

		// Add batches
		manager.AddBatch(df1)
		manager.AddBatch(df2)

		// Verify batches were added
		assert.Len(t, manager.batches, 2)

		// Release all batches
		manager.ReleaseAll()
		assert.Empty(t, manager.batches)
	})

	t.Run("spills LRU batch", func(t *testing.T) {
		monitor := NewMemoryUsageMonitor(1024 * 1024)
		defer monitor.StopMonitoring()

		manager := NewBatchManager(monitor)

		// Create test data
		mem := memory.NewGoAllocator()
		s1 := series.New("test", []int64{1, 2, 3}, mem)
		df := NewDataFrame(s1)

		// Add batch
		manager.AddBatch(df)

		// Spill LRU batch
		err := manager.SpillLRU()
		require.NoError(t, err)

		// Try to spill again (should fail as no non-spilled batches remain)
		err = manager.SpillLRU()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no batches available for spilling")

		// Clean up
		manager.ReleaseAll()
	})

	t.Run("handles empty manager", func(t *testing.T) {
		monitor := NewMemoryUsageMonitor(1024 * 1024)
		defer monitor.StopMonitoring()

		manager := NewBatchManager(monitor)

		// Try to spill from empty manager
		err := manager.SpillLRU()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no batches available for spilling")

		// Release all should not panic
		require.NotPanics(t, func() {
			manager.ReleaseAll()
		})
	})
}
