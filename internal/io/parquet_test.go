package io_test

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParquetReader_Read(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("basic read", func(t *testing.T) {
		// Create test Parquet data
		buf := new(bytes.Buffer)

		// Write test data using Parquet writer (will be implemented)
		writer := io.NewParquetWriter(buf, io.DefaultParquetOptions())

		// Create test DataFrame
		df := createParquetTestDataFrame(t, mem)
		defer df.Release()

		err := writer.Write(df)
		require.NoError(t, err)

		// Read the Parquet data
		reader := io.NewParquetReader(bytes.NewReader(buf.Bytes()), io.DefaultParquetOptions(), mem)
		result, err := reader.Read()
		require.NoError(t, err)
		defer result.Release()

		// Verify the data
		assert.Equal(t, df.Len(), result.Len())
		assert.Len(t, result.Columns(), len(df.Columns()))
		assert.Equal(t, df.Columns(), result.Columns())
	})

	t.Run("empty file", func(t *testing.T) {
		reader := io.NewParquetReader(bytes.NewReader([]byte{}), io.DefaultParquetOptions(), mem)
		_, err := reader.Read()
		require.Error(t, err)
	})

	t.Run("with custom options", func(t *testing.T) {
		options := io.ParquetOptions{
			Compression: "gzip",
			BatchSize:   500,
		}

		buf := new(bytes.Buffer)
		writer := io.NewParquetWriter(buf, options)

		df := createParquetTestDataFrame(t, mem)
		defer df.Release()

		err := writer.Write(df)
		require.NoError(t, err)

		reader := io.NewParquetReader(bytes.NewReader(buf.Bytes()), options, mem)
		result, err := reader.Read()
		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, df.Len(), result.Len())
	})

	t.Run("multiple data types", func(t *testing.T) {
		// Test with various data types
		buf := new(bytes.Buffer)
		writer := io.NewParquetWriter(buf, io.DefaultParquetOptions())

		df := createMixedTypeDataFrame(t, mem)
		defer df.Release()

		err := writer.Write(df)
		require.NoError(t, err)

		reader := io.NewParquetReader(bytes.NewReader(buf.Bytes()), io.DefaultParquetOptions(), mem)
		result, err := reader.Read()
		require.NoError(t, err)
		defer result.Release()

		// Verify all column types are preserved
		for _, colName := range df.Columns() {
			origCol, _ := df.Column(colName)
			resultCol, _ := result.Column(colName)
			assert.Equal(t, origCol.DataType(), resultCol.DataType(), "column %s type mismatch", colName)
			assert.Equal(t, origCol.Len(), resultCol.Len(), "column %s length mismatch", colName)
		}
	})
}

func TestParquetWriter_Write(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("basic write", func(t *testing.T) {
		buf := new(bytes.Buffer)
		writer := io.NewParquetWriter(buf, io.DefaultParquetOptions())

		df := createParquetTestDataFrame(t, mem)
		defer df.Release()

		err := writer.Write(df)
		require.NoError(t, err)
		assert.Positive(t, buf.Len(), "buffer should contain data")
	})

	t.Run("empty DataFrame", func(t *testing.T) {
		buf := new(bytes.Buffer)
		writer := io.NewParquetWriter(buf, io.DefaultParquetOptions())

		df := createEmptyDataFrame(t, mem)
		defer df.Release()

		err := writer.Write(df)
		require.NoError(t, err)
	})

	t.Run("compression options", func(t *testing.T) {
		compressions := []string{"snappy", "gzip", "lz4", "zstd", "uncompressed"}

		for _, comp := range compressions {
			t.Run(comp, func(t *testing.T) {
				buf := new(bytes.Buffer)
				options := io.ParquetOptions{
					Compression: comp,
					BatchSize:   io.DefaultBatchSize,
				}
				writer := io.NewParquetWriter(buf, options)

				df := createParquetTestDataFrame(t, mem)
				defer df.Release()

				err := writer.Write(df)
				require.NoError(t, err)
				assert.Positive(t, buf.Len())
			})
		}
	})

	t.Run("large DataFrame", func(t *testing.T) {
		buf := new(bytes.Buffer)
		writer := io.NewParquetWriter(buf, io.DefaultParquetOptions())

		df := createLargeDataFrame(t, mem, 10000)
		defer df.Release()

		err := writer.Write(df)
		require.NoError(t, err)
		assert.Positive(t, buf.Len())
	})
}

func TestParquetRoundTrip(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("preserve data integrity", func(t *testing.T) {
		buf := new(bytes.Buffer)

		// Create original DataFrame
		original := createMixedTypeDataFrame(t, mem)
		defer original.Release()

		// Write to Parquet
		writer := io.NewParquetWriter(buf, io.DefaultParquetOptions())
		err := writer.Write(original)
		require.NoError(t, err)

		// Read back from Parquet
		reader := io.NewParquetReader(bytes.NewReader(buf.Bytes()), io.DefaultParquetOptions(), mem)
		result, err := reader.Read()
		require.NoError(t, err)
		defer result.Release()

		// Compare DataFrames
		assertDataFramesEqual(t, original, result)
	})

	t.Run("with nulls", func(t *testing.T) {
		buf := new(bytes.Buffer)

		// Create DataFrame with null values
		original := createDataFrameWithNulls(t, mem)
		defer original.Release()

		// Write and read back
		writer := io.NewParquetWriter(buf, io.DefaultParquetOptions())
		err := writer.Write(original)
		require.NoError(t, err)

		reader := io.NewParquetReader(bytes.NewReader(buf.Bytes()), io.DefaultParquetOptions(), mem)
		result, err := reader.Read()
		require.NoError(t, err)
		defer result.Release()

		// Verify nulls are preserved
		assertDataFramesEqual(t, original, result)
	})
}
