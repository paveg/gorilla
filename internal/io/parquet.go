package io

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/series"
)

// Read reads Parquet data and returns a DataFrame.
func (r *ParquetReader) Read() (*dataframe.DataFrame, error) {
	// Read all data into memory for Parquet reading
	data, err := io.ReadAll(r.reader)
	if err != nil {
		return nil, fmt.Errorf("reading data: %w", err)
	}
	readerAt := bytes.NewReader(data)

	// Create a Parquet file reader
	pqReader, err := file.NewParquetReader(readerAt)
	if err != nil {
		return nil, fmt.Errorf("creating parquet file reader: %w", err)
	}

	// Create an Arrow file reader
	arrowReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{}, r.mem)
	if err != nil {
		return nil, fmt.Errorf("creating arrow file reader: %w", err)
	}

	// Read the entire table
	table, err := arrowReader.ReadTable(context.Background())
	if err != nil {
		return nil, fmt.Errorf("reading table: %w", err)
	}
	defer table.Release()

	// Convert Arrow table to DataFrame
	return r.arrowTableToDataFrame(table)
}

// Write writes the DataFrame to Parquet format.
func (w *ParquetWriter) Write(df *dataframe.DataFrame) error {
	// Convert DataFrame to Arrow table
	table, err := w.dataFrameToArrowTable(df)
	if err != nil {
		return fmt.Errorf("converting DataFrame to Arrow table: %w", err)
	}
	defer table.Release()

	// Create compression codec
	var compression compress.Compression
	switch w.options.Compression {
	case "snappy":
		compression = compress.Codecs.Snappy
	case "gzip":
		compression = compress.Codecs.Gzip
	case "lz4":
		compression = compress.Codecs.Lz4Raw
	case "zstd":
		compression = compress.Codecs.Zstd
	case "uncompressed":
		compression = compress.Codecs.Uncompressed
	default:
		compression = compress.Codecs.Snappy
	}

	// Create writer properties
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compression),
		parquet.WithBatchSize(int64(w.options.BatchSize)),
	)

	// Create Arrow writer properties
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(memory.NewGoAllocator()))

	// Create file writer
	writer, err := pqarrow.NewFileWriter(table.Schema(), w.writer, props, arrowProps)
	if err != nil {
		return fmt.Errorf("creating file writer: %w", err)
	}
	defer func() {
		if closeErr := writer.Close(); closeErr != nil {
			// Log the error or handle it appropriately
			_ = closeErr
		}
	}()

	// Write the table
	err = writer.WriteTable(table, int64(df.Len()))
	if err != nil {
		return fmt.Errorf("writing table: %w", err)
	}

	return nil
}

// arrowTableToDataFrame converts an Arrow table to a DataFrame.
func (r *ParquetReader) arrowTableToDataFrame(table arrow.Table) (*dataframe.DataFrame, error) {
	if table.NumRows() == 0 {
		return dataframe.New(), nil
	}

	var seriesList []dataframe.ISeries
	schema := table.Schema()

	for i := range table.NumCols() {
		column := table.Column(int(i))
		field := schema.Field(int(i))

		// Create series from Arrow column
		series, err := r.arrowColumnToSeries(field.Name, column, field.Type)
		if err != nil {
			return nil, fmt.Errorf("converting column %s: %w", field.Name, err)
		}
		seriesList = append(seriesList, series)
	}

	return dataframe.New(seriesList...), nil
}

// arrowColumnToSeries converts an Arrow column to a Series.
func (r *ParquetReader) arrowColumnToSeries(
	name string, column *arrow.Column, dataType arrow.DataType,
) (dataframe.ISeries, error) {
	chunked := column.Data()
	if chunked.Len() == 0 {
		return r.createEmptySeriesByType(name, dataType)
	}

	// Get the first chunk (for simplicity, assume single chunk)
	arr := chunked.Chunk(0)
	return r.convertArrowArrayToSeries(name, arr, dataType)
}

// createEmptySeriesByType creates an empty series based on Arrow data type.
func (r *ParquetReader) createEmptySeriesByType(name string, dataType arrow.DataType) (dataframe.ISeries, error) {
	//nolint:exhaustive // Only handling supported types for now
	switch dataType.ID() {
	case arrow.INT64:
		return series.NewSafe(name, []int64{}, r.mem)
	case arrow.INT32:
		return series.NewSafe(name, []int32{}, r.mem)
	case arrow.FLOAT64:
		return series.NewSafe(name, []float64{}, r.mem)
	case arrow.FLOAT32:
		return series.NewSafe(name, []float32{}, r.mem)
	case arrow.STRING:
		return series.NewSafe(name, []string{}, r.mem)
	case arrow.BOOL:
		return series.NewSafe(name, []bool{}, r.mem)
	default:
		return series.NewSafe(name, []string{}, r.mem)
	}
}

// convertArrowArrayToSeries converts an Arrow array to a Series.
func (r *ParquetReader) convertArrowArrayToSeries(
	name string, arr arrow.Array, dataType arrow.DataType,
) (dataframe.ISeries, error) {
	//nolint:exhaustive // Only handling supported types for now
	switch dataType.ID() {
	case arrow.INT64:
		int64Array, ok := arr.(*array.Int64)
		if !ok {
			return nil, errors.New("failed to cast to int64 array")
		}
		return r.convertInt64Array(name, int64Array)
	case arrow.INT32:
		int32Array, ok := arr.(*array.Int32)
		if !ok {
			return nil, errors.New("failed to cast to int32 array")
		}
		return r.convertInt32Array(name, int32Array)
	case arrow.FLOAT64:
		float64Array, ok := arr.(*array.Float64)
		if !ok {
			return nil, errors.New("failed to cast to float64 array")
		}
		return r.convertFloat64Array(name, float64Array)
	case arrow.FLOAT32:
		float32Array, ok := arr.(*array.Float32)
		if !ok {
			return nil, errors.New("failed to cast to float32 array")
		}
		return r.convertFloat32Array(name, float32Array)
	case arrow.STRING:
		stringArray, ok := arr.(*array.String)
		if !ok {
			return nil, errors.New("failed to cast to string array")
		}
		return r.convertStringArray(name, stringArray)
	case arrow.BOOL:
		boolArray, ok := arr.(*array.Boolean)
		if !ok {
			return nil, errors.New("failed to cast to boolean array")
		}
		return r.convertBoolArray(name, boolArray)
	default:
		return nil, fmt.Errorf("unsupported Arrow type: %s", dataType)
	}
}

func (r *ParquetReader) convertInt64Array(name string, arr *array.Int64) (dataframe.ISeries, error) {
	values := make([]int64, arr.Len())
	for i := range arr.Len() {
		values[i] = arr.Value(i)
	}
	return series.NewSafe(name, values, r.mem)
}

func (r *ParquetReader) convertInt32Array(name string, arr *array.Int32) (dataframe.ISeries, error) {
	values := make([]int32, arr.Len())
	for i := range arr.Len() {
		values[i] = arr.Value(i)
	}
	return series.NewSafe(name, values, r.mem)
}

func (r *ParquetReader) convertFloat64Array(name string, arr *array.Float64) (dataframe.ISeries, error) {
	values := make([]float64, arr.Len())
	for i := range arr.Len() {
		values[i] = arr.Value(i)
	}
	return series.NewSafe(name, values, r.mem)
}

func (r *ParquetReader) convertFloat32Array(name string, arr *array.Float32) (dataframe.ISeries, error) {
	values := make([]float32, arr.Len())
	for i := range arr.Len() {
		values[i] = arr.Value(i)
	}
	return series.NewSafe(name, values, r.mem)
}

func (r *ParquetReader) convertStringArray(name string, arr *array.String) (dataframe.ISeries, error) {
	values := make([]string, arr.Len())
	for i := range arr.Len() {
		values[i] = arr.Value(i)
	}
	return series.NewSafe(name, values, r.mem)
}

func (r *ParquetReader) convertBoolArray(name string, arr *array.Boolean) (dataframe.ISeries, error) {
	values := make([]bool, arr.Len())
	for i := range arr.Len() {
		values[i] = arr.Value(i)
	}
	return series.NewSafe(name, values, r.mem)
}

// dataFrameToArrowTable converts a DataFrame to an Arrow table.
func (w *ParquetWriter) dataFrameToArrowTable(df *dataframe.DataFrame) (arrow.Table, error) {
	mem := memory.NewGoAllocator()

	if df.Len() == 0 {
		// Handle empty DataFrame
		fields := make([]arrow.Field, 0)
		columns := make([]arrow.Column, 0)
		schema := arrow.NewSchema(fields, nil)
		return array.NewTable(schema, columns, 0), nil
	}

	// Create schema and columns
	fields := make([]arrow.Field, 0, len(df.Columns()))
	columns := make([]arrow.Column, 0, len(df.Columns()))

	for _, colName := range df.Columns() {
		col, exists := df.Column(colName)
		if !exists {
			continue
		}

		// Convert series to Arrow array and create column
		arr, err := w.seriesToArrowArray(col, mem)
		if err != nil {
			return nil, fmt.Errorf("converting series %s: %w", colName, err)
		}

		field := arrow.Field{Name: colName, Type: arr.DataType()}
		fields = append(fields, field)

		chunked := arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
		column := arrow.NewColumn(field, chunked)
		columns = append(columns, *column)
	}

	schema := arrow.NewSchema(fields, nil)
	return array.NewTable(schema, columns, int64(df.Len())), nil
}

// seriesToArrowArray converts a Series to an Arrow array.
func (w *ParquetWriter) seriesToArrowArray(s dataframe.ISeries, mem memory.Allocator) (arrow.Array, error) {
	dataTypeName := s.DataType().Name()

	switch dataTypeName {
	case "int64":
		return w.convertInt64SeriesToArrow(s, mem)
	case "int32":
		return w.convertInt32SeriesToArrow(s, mem)
	case "float64":
		return w.convertFloat64SeriesToArrow(s, mem)
	case "float32":
		return w.convertFloat32SeriesToArrow(s, mem)
	case "utf8":
		return w.convertStringSeriesToArrow(s, mem)
	case "bool":
		return w.convertBoolSeriesToArrow(s, mem)
	default:
		return nil, fmt.Errorf("unsupported series type: %s", dataTypeName)
	}
}

// convertInt64SeriesToArrow converts an int64 series to Arrow array.
func (w *ParquetWriter) convertInt64SeriesToArrow(s dataframe.ISeries, mem memory.Allocator) (arrow.Array, error) {
	typed, ok := s.(*series.Series[int64])
	if !ok {
		return nil, errors.New("failed to cast to int64 series")
	}
	builder := array.NewInt64Builder(mem)
	defer builder.Release()

	values := typed.Values()
	builder.AppendValues(values, nil)
	return builder.NewArray(), nil
}

// convertInt32SeriesToArrow converts an int32 series to Arrow array.
func (w *ParquetWriter) convertInt32SeriesToArrow(s dataframe.ISeries, mem memory.Allocator) (arrow.Array, error) {
	typed, ok := s.(*series.Series[int32])
	if !ok {
		return nil, errors.New("failed to cast to int32 series")
	}
	builder := array.NewInt32Builder(mem)
	defer builder.Release()

	values := typed.Values()
	builder.AppendValues(values, nil)
	return builder.NewArray(), nil
}

// convertFloat64SeriesToArrow converts a float64 series to Arrow array.
func (w *ParquetWriter) convertFloat64SeriesToArrow(s dataframe.ISeries, mem memory.Allocator) (arrow.Array, error) {
	typed, ok := s.(*series.Series[float64])
	if !ok {
		return nil, errors.New("failed to cast to float64 series")
	}
	builder := array.NewFloat64Builder(mem)
	defer builder.Release()

	values := typed.Values()
	builder.AppendValues(values, nil)
	return builder.NewArray(), nil
}

// convertFloat32SeriesToArrow converts a float32 series to Arrow array.
func (w *ParquetWriter) convertFloat32SeriesToArrow(s dataframe.ISeries, mem memory.Allocator) (arrow.Array, error) {
	typed, ok := s.(*series.Series[float32])
	if !ok {
		return nil, errors.New("failed to cast to float32 series")
	}
	builder := array.NewFloat32Builder(mem)
	defer builder.Release()

	values := typed.Values()
	builder.AppendValues(values, nil)
	return builder.NewArray(), nil
}

// convertStringSeriesToArrow converts a string series to Arrow array.
func (w *ParquetWriter) convertStringSeriesToArrow(s dataframe.ISeries, mem memory.Allocator) (arrow.Array, error) {
	typed, ok := s.(*series.Series[string])
	if !ok {
		return nil, errors.New("failed to cast to string series")
	}
	builder := array.NewStringBuilder(mem)
	defer builder.Release()

	values := typed.Values()
	builder.AppendValues(values, nil)
	return builder.NewArray(), nil
}

// convertBoolSeriesToArrow converts a bool series to Arrow array.
func (w *ParquetWriter) convertBoolSeriesToArrow(s dataframe.ISeries, mem memory.Allocator) (arrow.Array, error) {
	typed, ok := s.(*series.Series[bool])
	if !ok {
		return nil, errors.New("failed to cast to bool series")
	}
	builder := array.NewBooleanBuilder(mem)
	defer builder.Release()

	values := typed.Values()
	builder.AppendValues(values, nil)
	return builder.NewArray(), nil
}
