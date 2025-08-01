package gorilla

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/memory"
	memoryutil "github.com/paveg/gorilla/internal/memory"
)

// ErrEndOfStream indicates the end of the data stream.
var ErrEndOfStream = errors.New("end of stream")

const (
	// DefaultChunkSize is the default size for processing chunks.
	DefaultChunkSize = 1000
	// BytesPerValue is the estimated bytes per DataFrame value.
	BytesPerValue = 8
)

// StreamingProcessor handles processing of datasets larger than memory.
type StreamingProcessor struct {
	// Chunk size for processing data in batches
	chunkSize int
	// Memory allocator for Arrow arrays
	allocator memory.Allocator
	// Memory monitor for tracking usage
	monitor *MemoryUsageMonitor
	// Mutex for synchronizing access to processor state
	mu sync.RWMutex
	// Flag indicating if processor is closed
	closed bool
}

// NewStreamingProcessor creates a new streaming processor with specified chunk size.
func NewStreamingProcessor(chunkSize int, allocator memory.Allocator, monitor *MemoryUsageMonitor) *StreamingProcessor {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}

	return &StreamingProcessor{
		chunkSize: chunkSize,
		allocator: allocator,
		monitor:   monitor,
	}
}

// ChunkReader represents a source of data chunks for streaming processing.
type ChunkReader interface {
	// ReadChunk reads the next chunk of data
	ReadChunk() (*DataFrame, error)
	// HasNext returns true if there are more chunks to read
	HasNext() bool
	// Close closes the chunk reader and releases resources
	Close() error
}

// ChunkWriter represents a destination for processed data chunks.
type ChunkWriter interface {
	// WriteChunk writes a processed chunk of data
	WriteChunk(*DataFrame) error
	// Close closes the chunk writer and releases resources
	Close() error
}

// StreamingOperation represents an operation that can be applied to data chunks.
type StreamingOperation interface {
	// Apply applies the operation to a data chunk
	Apply(*DataFrame) (*DataFrame, error)
	// Release releases any resources held by the operation
	Release()
}

// ProcessStreaming processes data in streaming fashion using chunks.
func (sp *StreamingProcessor) ProcessStreaming(
	reader ChunkReader, writer ChunkWriter, operations []StreamingOperation,
) error {
	if err := sp.validateProcessor(); err != nil {
		return err
	}

	defer sp.cleanupOperations(operations)

	return sp.processAllChunks(reader, writer, operations)
}

// validateProcessor checks if the processor is in valid state.
func (sp *StreamingProcessor) validateProcessor() error {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	if sp.closed {
		return errors.New("streaming processor is closed")
	}
	return nil
}

// cleanupOperations releases all operations.
func (sp *StreamingProcessor) cleanupOperations(operations []StreamingOperation) {
	for _, op := range operations {
		if op != nil {
			op.Release()
		}
	}
}

// processAllChunks processes all chunks from reader to writer.
func (sp *StreamingProcessor) processAllChunks(
	reader ChunkReader,
	writer ChunkWriter,
	operations []StreamingOperation,
) error {
	for reader.HasNext() {
		if err := sp.processSingleChunk(reader, writer, operations); err != nil {
			return err
		}
	}
	return nil
}

// processSingleChunk processes a single chunk from reader to writer.
func (sp *StreamingProcessor) processSingleChunk(
	reader ChunkReader,
	writer ChunkWriter,
	operations []StreamingOperation,
) error {
	chunk, err := sp.readChunk(reader)
	if err != nil {
		return err
	}

	if chunk == nil {
		return nil
	}

	return sp.processAndWriteChunk(chunk, writer, operations)
}

// readChunk reads a single chunk from the reader.
func (sp *StreamingProcessor) readChunk(reader ChunkReader) (*DataFrame, error) {
	chunk, err := reader.ReadChunk()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, ErrEndOfStream
		}
		return nil, fmt.Errorf("failed to read chunk: %w", err)
	}
	return chunk, nil
}

// processAndWriteChunk processes and writes a single chunk.
func (sp *StreamingProcessor) processAndWriteChunk(
	chunk *DataFrame,
	writer ChunkWriter,
	operations []StreamingOperation,
) error {
	defer chunk.Release()

	processedChunk, err := sp.processChunk(chunk, operations)
	if err != nil {
		return fmt.Errorf("failed to process chunk: %w", err)
	}
	defer processedChunk.Release()

	if writeErr := writer.WriteChunk(processedChunk); writeErr != nil {
		return fmt.Errorf("failed to write chunk: %w", writeErr)
	}

	sp.handleMemoryPressure()
	return nil
}

// handleMemoryPressure checks and handles high memory pressure.
func (sp *StreamingProcessor) handleMemoryPressure() {
	if sp.monitor == nil {
		return
	}

	stats := sp.monitor.GetStats()
	if stats.MemoryPressure > HighMemoryPressureThreshold {
		sp.forceGC()
	}
}

// processChunk applies a series of operations to a data chunk.
func (sp *StreamingProcessor) processChunk(chunk *DataFrame, operations []StreamingOperation) (*DataFrame, error) {
	current := chunk

	for i, op := range operations {
		if op == nil {
			continue
		}

		result, err := op.Apply(current)
		if err != nil {
			return nil, fmt.Errorf("operation %d failed: %w", i, err)
		}

		// Release intermediate result if it's not the original chunk
		if current != chunk {
			current.Release()
		}

		current = result
	}

	return current, nil
}

// forceGC forces garbage collection to reclaim memory.
// Uses consolidated memory utilities for consistent GC behavior.
func (sp *StreamingProcessor) forceGC() {
	memoryutil.ForceGC()
}

// Close closes the streaming processor and releases resources.
func (sp *StreamingProcessor) Close() error {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	if sp.closed {
		return nil
	}

	sp.closed = true
	return nil
}

// MemoryAwareChunkReader wraps a ChunkReader with memory monitoring.
type MemoryAwareChunkReader struct {
	reader  ChunkReader
	monitor *MemoryUsageMonitor
}

// NewMemoryAwareChunkReader creates a new memory-aware chunk reader.
func NewMemoryAwareChunkReader(reader ChunkReader, monitor *MemoryUsageMonitor) *MemoryAwareChunkReader {
	return &MemoryAwareChunkReader{
		reader:  reader,
		monitor: monitor,
	}
}

// ReadChunk reads the next chunk and records memory usage.
func (mr *MemoryAwareChunkReader) ReadChunk() (*DataFrame, error) {
	chunk, err := mr.reader.ReadChunk()
	if err != nil {
		return nil, err
	}

	if chunk != nil && mr.monitor != nil {
		// Estimate memory usage of the chunk
		memoryUsage := mr.estimateMemoryUsage(chunk)
		mr.monitor.RecordAllocation(memoryUsage)
	}

	return chunk, nil
}

// HasNext returns true if there are more chunks to read.
func (mr *MemoryAwareChunkReader) HasNext() bool {
	return mr.reader.HasNext()
}

// Close closes the underlying reader.
func (mr *MemoryAwareChunkReader) Close() error {
	return mr.reader.Close()
}

// estimateMemoryUsage estimates the memory usage of a DataFrame.
// Uses consolidated memory estimation utilities for consistent calculation.
func (mr *MemoryAwareChunkReader) estimateMemoryUsage(df *DataFrame) int64 {
	// Use consolidated memory estimation that handles all data types properly
	if df == nil {
		return 0
	}
	
	// Create a slice representing the DataFrame's data for estimation
	// This approach works with the consolidated utility function
	width := df.Width()
	dataRepresentation := make([]interface{}, width)
	for i := range width {
		// Represent each column's memory footprint
		dataRepresentation[i] = make([]byte, df.Len()*BytesPerValue)
	}
	
	return memoryutil.EstimateMemoryUsage(dataRepresentation...)
}

// SpillableBatch represents a batch of data that can be spilled to disk.
// Implements the memory.Resource interface for consistent resource management.
type SpillableBatch struct {
	data     *DataFrame
	spilled  bool
	spillRef interface{} // Reference to spilled data (e.g., file path)
	mu       sync.RWMutex
}

// NewSpillableBatch creates a new spillable batch.
func NewSpillableBatch(data *DataFrame) *SpillableBatch {
	return &SpillableBatch{
		data: data,
	}
}

// GetData returns the data, loading from spill if necessary.
// If the data has been spilled to disk, this method attempts to reload it.
// Returns an error if the data cannot be loaded from spill storage.
func (sb *SpillableBatch) GetData() (*DataFrame, error) {
	sb.mu.RLock()
	if !sb.spilled {
		data := sb.data
		sb.mu.RUnlock()
		return data, nil
	}
	sb.mu.RUnlock()

	// Load from spill storage - this operation can fail
	// In a production system, proper error handling and retry logic would be implemented
	return sb.loadFromSpill()
}

// Spill spills the batch to disk and releases memory.
func (sb *SpillableBatch) Spill() error {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	if sb.spilled {
		return nil // Already spilled
	}

	// Spill to disk (simplified implementation)
	sb.spillToDisk()

	// Release memory
	if sb.data != nil {
		sb.data.Release()
		sb.data = nil
	}

	sb.spilled = true
	return nil
}

// spillToDisk spills the data to disk storage.
func (sb *SpillableBatch) spillToDisk() {
	// This is a placeholder implementation
	// In a real implementation, we would serialize the DataFrame to disk
	// For now, we'll just simulate the operation
	sb.spillRef = "spilled_data_placeholder"
}

// loadFromSpill loads data from spill storage.
// This is a placeholder implementation that demonstrates error handling for spill operations.
// In a production system, this would deserialize the DataFrame from persistent storage
// with proper error handling, validation, and retry logic.
func (sb *SpillableBatch) loadFromSpill() (*DataFrame, error) {
	if sb.spillRef == nil {
		return nil, errors.New("spill reference is nil, cannot load data")
	}

	// This is a placeholder implementation that always fails with a clear error message
	// In a real implementation, we would:
	// 1. Validate the spill reference
	// 2. Attempt to read from persistent storage
	// 3. Deserialize the DataFrame
	// 4. Handle any I/O or deserialization errors
	// 5. Implement retry logic for transient failures
	return nil, errors.New("loading from spill not implemented in this version")
}

// EstimateMemory returns the estimated memory usage of the batch.
// Implements the memory.Resource interface.
func (sb *SpillableBatch) EstimateMemory() int64 {
	sb.mu.RLock()
	defer sb.mu.RUnlock()
	
	if sb.spilled || sb.data == nil {
		return 0 // Spilled data doesn't consume memory
	}
	
	// Use consolidated memory estimation
	width := sb.data.Width()
	dataRepresentation := make([]interface{}, width)
	for i := range width {
		dataRepresentation[i] = make([]byte, sb.data.Len()*BytesPerValue)
	}
	
	return memoryutil.EstimateMemoryUsage(dataRepresentation...)
}

// ForceCleanup performs cleanup operations on the batch.
// Implements the memory.Resource interface.
func (sb *SpillableBatch) ForceCleanup() error {
	// Trigger global GC using consolidated utility
	memoryutil.ForceGC()
	return nil
}

// SpillIfNeeded checks if the batch should be spilled and performs spilling.
// Implements the memory.Resource interface.
func (sb *SpillableBatch) SpillIfNeeded() error {
	sb.mu.RLock()
	if sb.spilled {
		sb.mu.RUnlock()
		return nil // Already spilled
	}
	sb.mu.RUnlock()
	
	// Check if we should spill based on memory pressure
	estimatedMemory := sb.EstimateMemory()
	const spillThresholdMultiplier = 10
	if estimatedMemory > int64(DefaultChunkSize*BytesPerValue*spillThresholdMultiplier) { // Threshold for spilling
		return sb.Spill()
	}
	
	return nil
}

// Release releases the batch resources.
func (sb *SpillableBatch) Release() {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	if sb.data != nil {
		sb.data.Release()
		sb.data = nil
	}

	// Clean up spill reference if needed
	if sb.spilled && sb.spillRef != nil {
		// Clean up spilled data
		// In a production system, this would include proper cleanup of disk resources
		// and error handling for failed cleanup operations
		sb.spillRef = nil
	}
}

// BatchManager manages a collection of spillable batches.
// Implements the memory.ResourceManager interface for consistent resource management.
type BatchManager struct {
	batches []*SpillableBatch
	monitor *MemoryUsageMonitor
	mu      sync.RWMutex
}

// NewBatchManager creates a new batch manager.
func NewBatchManager(monitor *MemoryUsageMonitor) *BatchManager {
	return &BatchManager{
		batches: make([]*SpillableBatch, 0),
		monitor: monitor,
	}
}

// AddBatch adds a new batch to the manager.
// Uses the Track method for consistent resource management.
func (bm *BatchManager) AddBatch(data *DataFrame) {
	batch := NewSpillableBatch(data)
	bm.Track(batch)
}

// SpillLRU spills the least recently used batch to free memory.
func (bm *BatchManager) SpillLRU() error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	// Find the first non-spilled batch and spill it
	for _, batch := range bm.batches {
		if !batch.spilled {
			return batch.Spill()
		}
	}

	return errors.New("no batches available for spilling")
}

// EstimateMemory returns the total estimated memory usage of all managed batches.
// Implements the memory.Resource interface.
func (bm *BatchManager) EstimateMemory() int64 {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	var total int64
	for _, batch := range bm.batches {
		if batch != nil {
			total += batch.EstimateMemory()
		}
	}
	
	return total
}

// ForceCleanup performs cleanup operations on all managed batches.
// Implements the memory.Resource interface.
func (bm *BatchManager) ForceCleanup() error {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	// Trigger cleanup on all batches
	for _, batch := range bm.batches {
		if batch != nil {
			if err := batch.ForceCleanup(); err != nil {
				return err
			}
		}
	}
	
	// Trigger global GC
	memoryutil.ForceGC()
	return nil
}

// SpillIfNeeded checks memory pressure and spills batches if needed.
// Implements the memory.Resource interface.
func (bm *BatchManager) SpillIfNeeded() error {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	// Check if any batches need spilling
	for _, batch := range bm.batches {
		if batch != nil {
			if err := batch.SpillIfNeeded(); err != nil {
				return err
			}
		}
	}
	
	return nil
}

// Track adds a batch to be managed by the BatchManager.
// Implements the memory.ResourceManager interface.
func (bm *BatchManager) Track(resource memoryutil.Resource) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	
	// Type assert to SpillableBatch
	if batch, ok := resource.(*SpillableBatch); ok {
		bm.batches = append(bm.batches, batch)
	}
}

// Release releases the batch manager and all managed batches.
// Implements the memory.Resource interface.
func (bm *BatchManager) Release() {
	bm.ReleaseAll()
}

// ReleaseAll releases all batches and clears the manager.
func (bm *BatchManager) ReleaseAll() {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	for _, batch := range bm.batches {
		if batch != nil {
			batch.Release()
		}
	}

	bm.batches = bm.batches[:0]
}
