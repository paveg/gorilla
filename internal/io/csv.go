// Package io provides data input/output operations for DataFrames.
// It supports CSV reading and writing with type inference and various configuration options.
package io

import (
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/series"
)

const (
	// Boolean string constants
	trueStr  = "true"
	falseStr = "false"
	boolType = "bool"
)

// Read reads CSV data and returns a DataFrame
func (r *CSVReader) Read() (*dataframe.DataFrame, error) {
	// Create CSV reader
	csvReader := csv.NewReader(r.reader)
	csvReader.Comma = r.options.Delimiter
	csvReader.Comment = r.options.Comment
	csvReader.TrimLeadingSpace = r.options.SkipInitialSpace

	// Read all records
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("reading CSV: %w", err)
	}

	// Handle empty CSV
	if len(records) == 0 {
		return dataframe.New(), nil
	}

	var headers []string
	var dataRows [][]string

	if r.options.Header {
		if len(records) == 1 {
			// Only headers, no data
			headers = records[0]
			dataRows = [][]string{}
		} else {
			headers = records[0]
			dataRows = records[1:]
		}
	} else {
		// Generate default column names
		numCols := len(records[0])
		headers = make([]string, numCols)
		for i := 0; i < numCols; i++ {
			headers[i] = fmt.Sprintf("column_%d", i)
		}
		dataRows = records
	}

	// Handle case with no data rows
	if len(dataRows) == 0 {
		// Create empty series for each column
		var emptySeries []dataframe.ISeries
		for _, header := range headers {
			s, err := series.NewSafe(header, []string{}, r.mem)
			if err != nil {
				return nil, fmt.Errorf("creating empty series for column %s: %w", header, err)
			}
			emptySeries = append(emptySeries, s)
		}
		return dataframe.New(emptySeries...), nil
	}

	// Transpose data to work with columns
	numCols := len(headers)
	columns := make([][]string, numCols)
	for i := 0; i < numCols; i++ {
		columns[i] = make([]string, len(dataRows))
		for j, row := range dataRows {
			if i < len(row) {
				columns[i][j] = row[i]
			} else {
				columns[i][j] = ""
			}
		}
	}

	// Infer types and create series
	var seriesList []dataframe.ISeries
	for i, header := range headers {
		columnData := columns[i]
		s, err := r.createSeriesFromStrings(header, columnData)
		if err != nil {
			return nil, fmt.Errorf("creating series for column %s: %w", header, err)
		}
		seriesList = append(seriesList, s)
	}

	return dataframe.New(seriesList...), nil
}

// createSeriesFromStrings creates a series from string data, inferring the appropriate type
func (r *CSVReader) createSeriesFromStrings(name string, data []string) (dataframe.ISeries, error) {
	if len(data) == 0 {
		return series.NewSafe(name, []string{}, r.mem)
	}

	// Infer the type based on the data
	inferredType := r.inferDataType(data)

	// Create series based on inferred type
	switch inferredType {
	case boolType:
		return r.createBoolSeries(name, data)
	case "int":
		return r.createIntSeries(name, data)
	case "float":
		return r.createFloatSeries(name, data)
	default:
		return series.NewSafe(name, data, r.mem)
	}
}

// inferDataType determines the most appropriate data type for the given string data
func (r *CSVReader) inferDataType(data []string) string {
	canBeInt := true
	canBeFloat := true
	canBeBool := true
	hasNonEmptyValue := false

	for _, value := range data {
		if value == "" {
			continue // Skip empty values for type inference
		}
		hasNonEmptyValue = true

		// Check if it's a boolean
		if canBeBool {
			lower := strings.ToLower(value)
			if lower != trueStr && lower != falseStr {
				canBeBool = false
			}
		}

		// Check if it's an integer
		if canBeInt {
			_, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				canBeInt = false
			}
		}

		// Check if it's a float
		if canBeFloat {
			_, err := strconv.ParseFloat(value, 64)
			if err != nil {
				canBeFloat = false
			}
		}
	}

	// If all values are empty, default to string
	if !hasNonEmptyValue {
		return "string"
	}

	// Return the most specific type
	if canBeBool {
		return boolType
	}
	if canBeInt {
		return "int"
	}
	if canBeFloat {
		return "float"
	}
	return "string"
}

// createBoolSeries creates a boolean series from string data
func (r *CSVReader) createBoolSeries(name string, data []string) (dataframe.ISeries, error) {
	boolData := make([]bool, len(data))
	for i, value := range data {
		if value == "" {
			boolData[i] = false // default value for empty
		} else {
			boolData[i] = strings.EqualFold(value, trueStr)
		}
	}
	return series.NewSafe(name, boolData, r.mem)
}

// createIntSeries creates an integer series from string data
func (r *CSVReader) createIntSeries(name string, data []string) (dataframe.ISeries, error) {
	intData := make([]int64, len(data))
	for i, value := range data {
		if value == "" {
			intData[i] = 0 // default value for empty
		} else {
			val, _ := strconv.ParseInt(value, 10, 64)
			intData[i] = val
		}
	}
	return series.NewSafe(name, intData, r.mem)
}

// createFloatSeries creates a float series from string data
func (r *CSVReader) createFloatSeries(name string, data []string) (dataframe.ISeries, error) {
	floatData := make([]float64, len(data))
	for i, value := range data {
		if value == "" {
			floatData[i] = 0.0 // default value for empty
		} else {
			val, _ := strconv.ParseFloat(value, 64)
			floatData[i] = val
		}
	}
	return series.NewSafe(name, floatData, r.mem)
}

// Write writes the DataFrame to CSV format
func (w *CSVWriter) Write(df *dataframe.DataFrame) error {
	csvWriter := csv.NewWriter(w.writer)
	csvWriter.Comma = w.options.Delimiter
	defer csvWriter.Flush()

	// Write headers if required
	if w.options.Header {
		if err := csvWriter.Write(df.Columns()); err != nil {
			return fmt.Errorf("writing headers: %w", err)
		}
	}

	// Write data rows
	for i := 0; i < df.Len(); i++ {
		row := make([]string, df.Width())
		for j, colName := range df.Columns() {
			column, exists := df.Column(colName)
			if !exists {
				row[j] = ""
				continue
			}

			// Convert value to string using Arrow array
			value := w.getValueAsString(column, i)
			row[j] = value
		}
		if err := csvWriter.Write(row); err != nil {
			return fmt.Errorf("writing row %d: %w", i, err)
		}
	}

	return nil
}

// getValueAsString extracts a value from a column at the given index as a string
func (w *CSVWriter) getValueAsString(column dataframe.ISeries, index int) string {
	arr := column.Array()
	defer arr.Release()

	// Handle different Arrow array types
	switch typedArr := arr.(type) {
	case *array.String:
		return typedArr.Value(index)
	case *array.Int64:
		return strconv.FormatInt(typedArr.Value(index), 10)
	case *array.Int32:
		return strconv.FormatInt(int64(typedArr.Value(index)), 10)
	case *array.Float64:
		return strconv.FormatFloat(typedArr.Value(index), 'g', -1, 64)
	case *array.Float32:
		return strconv.FormatFloat(float64(typedArr.Value(index)), 'g', -1, 32)
	case *array.Boolean:
		if typedArr.Value(index) {
			return "true"
		}
		return "false"
	default:
		// Fallback for unknown types
		return ""
	}
}
