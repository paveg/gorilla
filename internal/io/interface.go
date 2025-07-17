// Package io provides I/O operations for reading and writing DataFrame data.
//
// This package includes readers and writers for various data formats,
// with automatic type inference and schema handling. The primary
// implementation is CSV I/O with support for streaming large datasets.
//
// Key components:
//   - DataReader/DataWriter interfaces for pluggable I/O backends
//   - CSVReader/CSVWriter for CSV file operations
//   - Type inference for automatic schema detection
//   - Configurable options for delimiters, headers, and batch sizes
//
// Memory management: All I/O operations integrate with Apache Arrow's
// memory management system and require proper cleanup with defer patterns.
package io

import (
	"io"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
)

const (
	// DefaultChunkSize is the default chunk size for parallel processing
	DefaultChunkSize = 1000
	// DefaultBatchSize is the default batch size for I/O operations
	DefaultBatchSize = 1000
)

// DataReader defines the interface for reading data from various sources
type DataReader interface {
	// Read reads data from the source and returns a DataFrame
	Read() (*dataframe.DataFrame, error)
}

// DataWriter defines the interface for writing data to various destinations
type DataWriter interface {
	// Write writes the DataFrame to the destination
	Write(df *dataframe.DataFrame) error
}

// CSVOptions contains configuration options for CSV operations
type CSVOptions struct {
	// Delimiter is the field delimiter (default: comma)
	Delimiter rune
	// Comment is the comment character (default: 0 = disabled)
	Comment rune
	// Header indicates whether the first row contains headers
	Header bool
	// SkipInitialSpace indicates whether to skip initial whitespace
	SkipInitialSpace bool
	// Parallel indicates whether to use parallel processing
	Parallel bool
	// ChunkSize is the size of chunks for parallel processing
	ChunkSize int
}

// DefaultCSVOptions returns default CSV options
func DefaultCSVOptions() CSVOptions {
	return CSVOptions{
		Delimiter:        ',',
		Comment:          0,
		Header:           true,
		SkipInitialSpace: false,
		Parallel:         false,
		ChunkSize:        DefaultChunkSize,
	}
}

// CSVReader reads CSV data and converts it to DataFrames
type CSVReader struct {
	reader  io.Reader
	options CSVOptions
	mem     memory.Allocator
}

// NewCSVReader creates a new CSV reader with the specified options
func NewCSVReader(reader io.Reader, options CSVOptions, mem memory.Allocator) *CSVReader {
	return &CSVReader{
		reader:  reader,
		options: options,
		mem:     mem,
	}
}

// CSVWriter writes DataFrames to CSV format
type CSVWriter struct {
	writer  io.Writer
	options CSVOptions
}

// NewCSVWriter creates a new CSV writer with the specified options
func NewCSVWriter(writer io.Writer, options CSVOptions) *CSVWriter {
	return &CSVWriter{
		writer:  writer,
		options: options,
	}
}

// ParquetOptions contains configuration options for Parquet operations
type ParquetOptions struct {
	// Compression type for Parquet files
	Compression string
	// BatchSize for reading/writing operations
	BatchSize int
}

// DefaultParquetOptions returns default Parquet options
func DefaultParquetOptions() ParquetOptions {
	return ParquetOptions{
		Compression: "snappy",
		BatchSize:   DefaultBatchSize,
	}
}

// ParquetReader reads Parquet data and converts it to DataFrames
type ParquetReader struct {
	reader  io.Reader
	options ParquetOptions
	mem     memory.Allocator
}

// NewParquetReader creates a new Parquet reader with the specified options
func NewParquetReader(reader io.Reader, options ParquetOptions, mem memory.Allocator) *ParquetReader {
	return &ParquetReader{
		reader:  reader,
		options: options,
		mem:     mem,
	}
}

// ParquetWriter writes DataFrames to Parquet format
type ParquetWriter struct {
	writer  io.Writer
	options ParquetOptions
}

// NewParquetWriter creates a new Parquet writer with the specified options
func NewParquetWriter(writer io.Writer, options ParquetOptions) *ParquetWriter {
	return &ParquetWriter{
		writer:  writer,
		options: options,
	}
}
