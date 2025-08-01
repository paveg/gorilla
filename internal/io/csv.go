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
	// Boolean string constants.
	trueStr  = "true"
	falseStr = "false"
	boolType = "bool"
)

// Read reads CSV data and returns a DataFrame.
func (r *CSVReader) Read() (*dataframe.DataFrame, error) {
	records, err := r.readCSVRecords()
	if err != nil {
		return nil, err
	}

	if len(records) == 0 {
		return dataframe.New(), nil
	}

	headers, dataRows := r.extractHeadersAndData(records)

	if len(dataRows) == 0 {
		return r.createEmptyDataFrame(headers)
	}

	columns := r.transposeDataToColumns(headers, dataRows)
	return r.createDataFrameFromColumns(headers, columns)
}

// readCSVRecords reads and parses CSV records.
func (r *CSVReader) readCSVRecords() ([][]string, error) {
	csvReader := csv.NewReader(r.reader)
	r.configureCSVReader(csvReader)

	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("reading CSV: %w", err)
	}
	return records, nil
}

// configureCSVReader configures the CSV reader with options.
func (r *CSVReader) configureCSVReader(csvReader *csv.Reader) {
	csvReader.Comma = r.options.Delimiter
	csvReader.Comment = r.options.Comment
	csvReader.TrimLeadingSpace = r.options.SkipInitialSpace
}

// extractHeadersAndData separates headers and data rows from records.
func (r *CSVReader) extractHeadersAndData(records [][]string) ([]string, [][]string) {
	if r.options.Header {
		return r.extractWithHeaders(records)
	}
	return r.generateDefaultHeaders(records), records
}

// extractWithHeaders extracts headers when Header option is true.
func (r *CSVReader) extractWithHeaders(records [][]string) ([]string, [][]string) {
	if len(records) == 1 {
		return records[0], [][]string{}
	}
	return records[0], records[1:]
}

// generateDefaultHeaders generates default column names when no headers.
func (r *CSVReader) generateDefaultHeaders(records [][]string) []string {
	numCols := len(records[0])
	headers := make([]string, numCols)
	for i := range numCols {
		headers[i] = fmt.Sprintf("column_%d", i)
	}
	return headers
}

// createEmptyDataFrame creates a DataFrame with empty series for each column.
func (r *CSVReader) createEmptyDataFrame(headers []string) (*dataframe.DataFrame, error) {
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

// transposeDataToColumns transposes row-based data to column-based data.
func (r *CSVReader) transposeDataToColumns(headers []string, dataRows [][]string) [][]string {
	numCols := len(headers)
	columns := make([][]string, numCols)

	for i := range numCols {
		columns[i] = make([]string, len(dataRows))
		for j, row := range dataRows {
			if i < len(row) {
				columns[i][j] = row[i]
			} else {
				columns[i][j] = ""
			}
		}
	}
	return columns
}

// createDataFrameFromColumns creates DataFrame from column data.
func (r *CSVReader) createDataFrameFromColumns(headers []string, columns [][]string) (*dataframe.DataFrame, error) {
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

// createSeriesFromStrings creates a series from string data, inferring the appropriate type.
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

// inferDataType determines the most appropriate data type for the given string data.
func (r *CSVReader) inferDataType(data []string) string {
	typeChecker := r.createTypeChecker()

	for _, value := range data {
		if value == "" {
			continue // Skip empty values for type inference
		}

		typeChecker.processValue(value)
	}

	return typeChecker.getMostSpecificType()
}

// typeChecker manages type inference for CSV data.
type typeChecker struct {
	canBeInt         bool
	canBeFloat       bool
	canBeBool        bool
	hasNonEmptyValue bool
}

// createTypeChecker creates a new type checker with initial state.
func (r *CSVReader) createTypeChecker() *typeChecker {
	return &typeChecker{
		canBeInt:   true,
		canBeFloat: true,
		canBeBool:  true,
	}
}

// processValue processes a single value and updates type compatibility.
func (tc *typeChecker) processValue(value string) {
	tc.hasNonEmptyValue = true

	tc.checkBoolCompatibility(value)
	tc.checkIntCompatibility(value)
	tc.checkFloatCompatibility(value)
}

// checkBoolCompatibility checks if value is compatible with bool type.
func (tc *typeChecker) checkBoolCompatibility(value string) {
	if !tc.canBeBool {
		return
	}

	lower := strings.ToLower(value)
	if lower != trueStr && lower != falseStr {
		tc.canBeBool = false
	}
}

// checkIntCompatibility checks if value is compatible with int type.
func (tc *typeChecker) checkIntCompatibility(value string) {
	if !tc.canBeInt {
		return
	}

	if _, err := strconv.ParseInt(value, 10, 64); err != nil {
		tc.canBeInt = false
	}
}

// checkFloatCompatibility checks if value is compatible with float type.
func (tc *typeChecker) checkFloatCompatibility(value string) {
	if !tc.canBeFloat {
		return
	}

	if _, err := strconv.ParseFloat(value, 64); err != nil {
		tc.canBeFloat = false
	}
}

// getMostSpecificType returns the most specific compatible type.
func (tc *typeChecker) getMostSpecificType() string {
	if !tc.hasNonEmptyValue {
		return "string"
	}

	if tc.canBeBool {
		return boolType
	}
	if tc.canBeInt {
		return "int"
	}
	if tc.canBeFloat {
		return "float"
	}
	return "string"
}

// createBoolSeries creates a boolean series from string data.
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

// createIntSeries creates an integer series from string data.
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

// createFloatSeries creates a float series from string data.
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

// Write writes the DataFrame to CSV format.
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
	for i := range df.Len() {
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

// getValueAsString extracts a value from a column at the given index as a string.
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
			return trueStr
		}
		return falseStr
	default:
		// Fallback for unknown types
		return ""
	}
}
