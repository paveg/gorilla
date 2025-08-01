package io

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/series"
)

// Type constants for data type inference.
const (
	typeInt64   = "int64"
	typeFloat64 = "float64"
	typeBool    = "bool"
	typeString  = "string"
)

// Read reads JSON data and returns a DataFrame.
func (r *JSONReader) Read() (*dataframe.DataFrame, error) {
	switch r.options.Format {
	case JSONArray:
		return r.readJSONArray()
	case JSONLines:
		return r.readJSONLines()
	default:
		return nil, fmt.Errorf("unsupported JSON format: %d", r.options.Format)
	}
}

// readJSONArray reads JSON array format.
func (r *JSONReader) readJSONArray() (*dataframe.DataFrame, error) {
	data, err := io.ReadAll(r.reader)
	if err != nil {
		return nil, fmt.Errorf("reading JSON data: %w", err)
	}

	var records []map[string]interface{}
	if unmarshalErr := json.Unmarshal(data, &records); unmarshalErr != nil {
		return nil, fmt.Errorf("unmarshaling JSON array: %w", unmarshalErr)
	}

	if len(records) == 0 {
		return dataframe.New(), nil
	}

	// Apply max records limit
	if r.options.MaxRecords > 0 && len(records) > r.options.MaxRecords {
		records = records[:r.options.MaxRecords]
	}

	return r.recordsToDataFrame(records)
}

// readJSONLines reads JSON Lines format.
func (r *JSONReader) readJSONLines() (*dataframe.DataFrame, error) {
	scanner := bufio.NewScanner(r.reader)
	var records []map[string]interface{}

	lineNum := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue // Skip empty lines
		}

		var record map[string]interface{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return nil, fmt.Errorf("unmarshaling JSON line %d: %w", lineNum+1, err)
		}

		records = append(records, record)
		lineNum++

		// Apply max records limit
		if r.options.MaxRecords > 0 && len(records) >= r.options.MaxRecords {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanning JSON lines: %w", err)
	}

	if len(records) == 0 {
		return dataframe.New(), nil
	}

	return r.recordsToDataFrame(records)
}

// recordsToDataFrame converts JSON records to a DataFrame.
func (r *JSONReader) recordsToDataFrame(records []map[string]interface{}) (*dataframe.DataFrame, error) {
	if len(records) == 0 {
		return dataframe.New(), nil
	}

	// Collect all unique column names
	columnSet := make(map[string]bool)
	for _, record := range records {
		for key := range record {
			columnSet[key] = true
		}
	}

	// Convert to sorted list for consistent ordering
	columns := make([]string, 0, len(columnSet))
	for col := range columnSet {
		columns = append(columns, col)
	}

	// Extract data by column
	columnData := make(map[string][]interface{})
	for _, col := range columns {
		columnData[col] = make([]interface{}, len(records))
		for i, record := range records {
			if value, exists := record[col]; exists {
				columnData[col][i] = value
			} else {
				columnData[col][i] = nil
			}
		}
	}

	// Create series for each column
	var seriesList []dataframe.ISeries
	for _, col := range columns {
		data := columnData[col]
		
		// Infer type and create series
		series, err := r.createSeriesFromData(col, data)
		if err != nil {
			return nil, fmt.Errorf("creating series for column %s: %w", col, err)
		}
		seriesList = append(seriesList, series)
	}

	return dataframe.New(seriesList...), nil
}

// createSeriesFromData creates a Series from interface{} data with type inference.
func (r *JSONReader) createSeriesFromData(name string, data []interface{}) (dataframe.ISeries, error) {
	if !r.options.TypeInference {
		// Convert all to strings if type inference is disabled
		stringData := make([]string, len(data))
		for i, v := range data {
			stringData[i] = r.interfaceToString(v)
		}
		return series.NewSafe(name, stringData, r.mem)
	}

	// Infer the most appropriate type
	dataType := r.inferDataType(data)

	switch dataType {
	case typeInt64:
		return r.createInt64Series(name, data)
	case typeFloat64:
		return r.createFloat64Series(name, data)
	case typeBool:
		return r.createBoolSeries(name, data)
	case typeString:
		return r.createStringSeries(name, data)
	default:
		return r.createStringSeries(name, data)
	}
}

// inferDataType infers the most appropriate data type for the column.
func (r *JSONReader) inferDataType(data []interface{}) string {
	typeFlags := r.analyzeDataTypes(data)
	
	// Early exit if we have explicit string values
	if typeFlags.hasString {
		return typeString
	}
	
	// Check for mixed number representations
	if r.hasMixedNumberTypes(data) {
		return typeString
	}
	
	return r.selectBestType(typeFlags)
}

// dataTypeFlags holds flags for detected data types.
type dataTypeFlags struct {
	hasInt    bool
	hasFloat  bool
	hasBool   bool
	hasString bool
}

// analyzeDataTypes analyzes the data and returns type flags.
func (r *JSONReader) analyzeDataTypes(data []interface{}) dataTypeFlags {
	var flags dataTypeFlags
	
	for _, v := range data {
		if v == nil || r.isNull(v) {
			continue
		}
		
		r.updateTypeFlags(v, &flags)
		
		// Early exit if we find explicit string values
		if flags.hasString {
			break
		}
	}
	
	return flags
}

// updateTypeFlags updates type flags based on a single value.
func (r *JSONReader) updateTypeFlags(v interface{}, flags *dataTypeFlags) {
	switch val := v.(type) {
	case bool:
		flags.hasBool = true
	case float64:
		if val == float64(int64(val)) {
			flags.hasInt = true
		} else {
			flags.hasFloat = true
		}
	case string:
		r.analyzeStringValue(val, flags)
	default:
		flags.hasString = true
	}
}

// analyzeStringValue analyzes a string value and updates flags.
func (r *JSONReader) analyzeStringValue(s string, flags *dataTypeFlags) {
	if r.isBoolString(s) {
		flags.hasBool = true
	} else if _, err := strconv.ParseInt(s, 10, 64); err == nil {
		flags.hasInt = true
	} else if _, floatErr := strconv.ParseFloat(s, 64); floatErr == nil {
		flags.hasFloat = true
	} else {
		flags.hasString = true
	}
}

// hasMixedNumberTypes checks if we have both actual numbers and string numbers.
func (r *JSONReader) hasMixedNumberTypes(data []interface{}) bool {
	hasActualNumbers := false
	hasStringNumbers := false
	
	for _, v := range data {
		if v == nil || r.isNull(v) {
			continue
		}
		
		switch val := v.(type) {
		case float64:
			hasActualNumbers = true
		case string:
			if r.isNumericString(val) {
				hasStringNumbers = true
			}
		}
	}
	
	return hasActualNumbers && hasStringNumbers
}

// isNumericString checks if a string represents a number.
func (r *JSONReader) isNumericString(s string) bool {
	if _, err := strconv.ParseInt(s, 10, 64); err == nil {
		return true
	}
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return true
	}
	return false
}

// selectBestType selects the best type based on flags.
func (r *JSONReader) selectBestType(flags dataTypeFlags) string {
	// Priority: float > int > bool > string
	if flags.hasFloat {
		return typeFloat64
	}
	if flags.hasInt {
		return typeInt64
	}
	if flags.hasBool {
		return typeBool
	}
	return typeString
}

// createInt64Series creates an int64 series from interface{} data.
func (r *JSONReader) createInt64Series(name string, data []interface{}) (dataframe.ISeries, error) {
	values := make([]int64, len(data))
	for i, v := range data {
		values[i] = r.toInt64(v)
	}
	return series.NewSafe(name, values, r.mem)
}

// createFloat64Series creates a float64 series from interface{} data.
func (r *JSONReader) createFloat64Series(name string, data []interface{}) (dataframe.ISeries, error) {
	values := make([]float64, len(data))
	for i, v := range data {
		values[i] = r.toFloat64(v)
	}
	return series.NewSafe(name, values, r.mem)
}

// createBoolSeries creates a bool series from interface{} data.
func (r *JSONReader) createBoolSeries(name string, data []interface{}) (dataframe.ISeries, error) {
	values := make([]bool, len(data))
	for i, v := range data {
		values[i] = r.toBool(v)
	}
	return series.NewSafe(name, values, r.mem)
}

// createStringSeries creates a string series from interface{} data.
func (r *JSONReader) createStringSeries(name string, data []interface{}) (dataframe.ISeries, error) {
	values := make([]string, len(data))
	for i, v := range data {
		values[i] = r.interfaceToString(v)
	}
	return series.NewSafe(name, values, r.mem)
}

// Type conversion helpers

func (r *JSONReader) isNull(v interface{}) bool {
	if v == nil {
		return true
	}
	str := r.interfaceToString(v)
	for _, nullValue := range r.options.NullValues {
		if str == nullValue {
			return true
		}
	}
	return false
}

func (r *JSONReader) isBoolString(s string) bool {
	lower := strings.ToLower(s)
	return lower == "true" || lower == "false" || lower == "t" || lower == "f" ||
		lower == "yes" || lower == "no" || lower == "y" || lower == "n" ||
		lower == "1" || lower == "0"
}

func (r *JSONReader) toInt64(v interface{}) int64 {
	if v == nil || r.isNull(v) {
		return 0
	}

	switch v := v.(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	case string:
		if val, err := strconv.ParseInt(v, 10, 64); err == nil {
			return val
		}
		return 0
	case bool:
		if v {
			return 1
		}
		return 0
	default:
		return 0
	}
}

func (r *JSONReader) toFloat64(v interface{}) float64 {
	if v == nil || r.isNull(v) {
		return 0.0
	}

	switch v := v.(type) {
	case float64:
		return v
	case int64:
		return float64(v)
	case int:
		return float64(v)
	case string:
		if val, err := strconv.ParseFloat(v, 64); err == nil {
			return val
		}
		return 0.0
	case bool:
		if v {
			return 1.0
		}
		return 0.0
	default:
		return 0.0
	}
}

func (r *JSONReader) toBool(v interface{}) bool {
	if v == nil || r.isNull(v) {
		return false
	}

	switch v := v.(type) {
	case bool:
		return v
	case float64:
		return v != 0
	case int64:
		return v != 0
	case string:
		lower := strings.ToLower(v)
		return lower == "true" || lower == "t" || lower == "yes" || lower == "y" || lower == "1"
	default:
		return false
	}
}

func (r *JSONReader) interfaceToString(v interface{}) string {
	if v == nil {
		return ""
	}

	switch v := v.(type) {
	case string:
		return v
	case float64:
		// Handle integers without decimal points
		if v == float64(int64(v)) {
			return strconv.FormatInt(int64(v), 10)
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// Write writes the DataFrame to JSON format.
func (w *JSONWriter) Write(df *dataframe.DataFrame) error {
	if df.Len() == 0 {
		switch w.options.Format {
		case JSONArray:
			_, err := w.writer.Write([]byte("[]"))
			return err
		case JSONLines:
			return nil // Empty output for JSON Lines
		}
	}

	switch w.options.Format {
	case JSONArray:
		return w.writeJSONArray(df)
	case JSONLines:
		return w.writeJSONLines(df)
	default:
		return fmt.Errorf("unsupported JSON format: %d", w.options.Format)
	}
}

// writeJSONArray writes DataFrame as JSON array.
func (w *JSONWriter) writeJSONArray(df *dataframe.DataFrame) error {
	records := w.dataFrameToRecords(df)
	
	data, err := json.Marshal(records)
	if err != nil {
		return fmt.Errorf("marshaling JSON array: %w", err)
	}

	_, err = w.writer.Write(data)
	return err
}

// writeJSONLines writes DataFrame as JSON Lines.
func (w *JSONWriter) writeJSONLines(df *dataframe.DataFrame) error {
	records := w.dataFrameToRecords(df)
	
	for _, record := range records {
		data, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("marshaling JSON record: %w", err)
		}
		
		if _, writeErr := w.writer.Write(data); writeErr != nil {
			return writeErr
		}
		if _, newlineErr := w.writer.Write([]byte("\n")); newlineErr != nil {
			return newlineErr
		}
	}

	return nil
}

// dataFrameToRecords converts DataFrame to slice of maps.
func (w *JSONWriter) dataFrameToRecords(df *dataframe.DataFrame) []map[string]interface{} {
	records := make([]map[string]interface{}, df.Len())
	
	for i := range df.Len() {
		record := make(map[string]interface{})
		
		for _, colName := range df.Columns() {
			col, exists := df.Column(colName)
			if !exists {
				continue
			}
			
			// Get value at index i
			value := w.getSeriesValue(col, i)
			record[colName] = value
		}
		
		records[i] = record
	}
	
	return records
}

// getSeriesValue gets value from series at specified index.
func (w *JSONWriter) getSeriesValue(s dataframe.ISeries, index int) interface{} {
	if index >= s.Len() {
		return nil
	}

	dataTypeName := s.DataType().Name()
	switch dataTypeName {
	case typeInt64:
		return w.getInt64Value(s, index)
	case "int32":
		return w.getInt32Value(s, index)
	case typeFloat64:
		return w.getFloat64Value(s, index)
	case "float32":
		return w.getFloat32Value(s, index)
	case "utf8":
		return w.getStringValue(s, index)
	case typeBool:
		return w.getBoolValue(s, index)
	}

	return nil
}

// getInt64Value extracts int64 value from series.
func (w *JSONWriter) getInt64Value(s dataframe.ISeries, index int) interface{} {
	if typed, ok := s.(*series.Series[int64]); ok {
		values := typed.Values()
		if index < len(values) {
			return values[index]
		}
	}
	return nil
}

// getInt32Value extracts int32 value from series.
func (w *JSONWriter) getInt32Value(s dataframe.ISeries, index int) interface{} {
	if typed, ok := s.(*series.Series[int32]); ok {
		values := typed.Values()
		if index < len(values) {
			return values[index]
		}
	}
	return nil
}

// getFloat64Value extracts float64 value from series.
func (w *JSONWriter) getFloat64Value(s dataframe.ISeries, index int) interface{} {
	if typed, ok := s.(*series.Series[float64]); ok {
		values := typed.Values()
		if index < len(values) {
			return values[index]
		}
	}
	return nil
}

// getFloat32Value extracts float32 value from series.
func (w *JSONWriter) getFloat32Value(s dataframe.ISeries, index int) interface{} {
	if typed, ok := s.(*series.Series[float32]); ok {
		values := typed.Values()
		if index < len(values) {
			return values[index]
		}
	}
	return nil
}

// getStringValue extracts string value from series.
func (w *JSONWriter) getStringValue(s dataframe.ISeries, index int) interface{} {
	if typed, ok := s.(*series.Series[string]); ok {
		values := typed.Values()
		if index < len(values) {
			return values[index]
		}
	}
	return nil
}

// getBoolValue extracts bool value from series.
func (w *JSONWriter) getBoolValue(s dataframe.ISeries, index int) interface{} {
	if typed, ok := s.(*series.Series[bool]); ok {
		values := typed.Values()
		if index < len(values) {
			return values[index]
		}
	}
	return nil
}