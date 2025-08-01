package expr

import (
	"errors"
	"fmt"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// evaluateRank implements RANK() window function.
func (e *Evaluator) evaluateRank(
	window *WindowSpec,
	columns map[string]arrow.Array,
	dataLength int,
) (arrow.Array, error) {
	// Get partitions
	partitions, err := e.getPartitions(window, columns, dataLength)
	if err != nil {
		return nil, fmt.Errorf("getting partitions: %w", err)
	}

	// Create result array
	builder := array.NewInt64Builder(e.mem)
	defer builder.Release()

	result := make([]int64, dataLength)

	// Process each partition
	for _, partition := range partitions {
		// Sort partition if ORDER BY is specified
		sortedIndices := partition
		if len(window.orderBy) > 0 {
			sortedIndices = e.sortPartitionOptimized(partition, window.orderBy, columns)
		}

		// Assign ranks within partition (same values get same rank)
		currentRank := int64(1)
		for i, idx := range sortedIndices {
			if i > 0 {
				// Check if current row has same values as previous row
				if !e.rowsEqual(sortedIndices[i-1], idx, window.orderBy, columns) {
					currentRank = int64(i + 1)
				}
			}
			result[idx] = currentRank
		}
	}

	// Build the result array
	for i := range dataLength {
		builder.Append(result[i])
	}

	return builder.NewArray(), nil
}

// evaluateDenseRank implements DENSE_RANK() window function.
func (e *Evaluator) evaluateDenseRank(
	window *WindowSpec,
	columns map[string]arrow.Array,
	dataLength int,
) (arrow.Array, error) {
	// Similar to RANK but without gaps
	partitions, err := e.getPartitions(window, columns, dataLength)
	if err != nil {
		return nil, fmt.Errorf("getting partitions: %w", err)
	}

	builder := array.NewInt64Builder(e.mem)
	defer builder.Release()

	result := make([]int64, dataLength)

	for _, partition := range partitions {
		sortedIndices := partition
		if len(window.orderBy) > 0 {
			sortedIndices = e.sortPartitionOptimized(partition, window.orderBy, columns)
		}

		currentRank := int64(1)
		for i, idx := range sortedIndices {
			if i > 0 {
				if !e.rowsEqual(sortedIndices[i-1], idx, window.orderBy, columns) {
					currentRank++
				}
			}
			result[idx] = currentRank
		}
	}

	for i := range dataLength {
		builder.Append(result[i])
	}

	return builder.NewArray(), nil
}

// evaluateLag implements LAG() window function.
func (e *Evaluator) evaluateLag(
	expr *WindowFunctionExpr,
	window *WindowSpec,
	columns map[string]arrow.Array,
	dataLength int,
) (arrow.Array, error) {
	if len(expr.args) < 1 {
		return nil, errors.New("LAG function requires at least 1 argument")
	}

	// Get the column to lag
	columnExpr, err := e.Evaluate(expr.args[0], columns)
	if err != nil {
		return nil, fmt.Errorf("evaluating LAG column: %w", err)
	}
	defer columnExpr.Release()

	// Get the offset (default to 1)
	offset := int64(1)
	if len(expr.args) > 1 {
		offsetExpr, offsetErr := e.Evaluate(expr.args[1], columns)
		if offsetErr != nil {
			return nil, fmt.Errorf("evaluating LAG offset: %w", offsetErr)
		}
		defer offsetExpr.Release()

		if offsetArr, ok := offsetExpr.(*array.Int64); ok && offsetArr.Len() > 0 {
			offset = offsetArr.Value(0)
		}
	}

	// Get partitions
	partitions, err := e.getPartitions(window, columns, dataLength)
	if err != nil {
		return nil, fmt.Errorf("getting partitions: %w", err)
	}

	// Create result array based on column type
	return e.createLagResult(columnExpr, partitions, window, columns, offset)
}

// evaluateLead implements LEAD() window function.
func (e *Evaluator) evaluateLead(
	expr *WindowFunctionExpr,
	window *WindowSpec,
	columns map[string]arrow.Array,
	dataLength int,
) (arrow.Array, error) {
	if len(expr.args) < 1 {
		return nil, errors.New("LEAD function requires at least 1 argument")
	}

	// Get the column to lead
	columnExpr, err := e.Evaluate(expr.args[0], columns)
	if err != nil {
		return nil, fmt.Errorf("evaluating LEAD column: %w", err)
	}
	defer columnExpr.Release()

	// Get the offset (default to 1)
	offset := int64(1)
	if len(expr.args) > 1 {
		offsetExpr, offsetErr := e.Evaluate(expr.args[1], columns)
		if offsetErr != nil {
			return nil, fmt.Errorf("evaluating LEAD offset: %w", offsetErr)
		}
		defer offsetExpr.Release()

		if offsetArr, ok := offsetExpr.(*array.Int64); ok && offsetArr.Len() > 0 {
			offset = offsetArr.Value(0)
		}
	}

	// Get partitions
	partitions, err := e.getPartitions(window, columns, dataLength)
	if err != nil {
		return nil, fmt.Errorf("getting partitions: %w", err)
	}

	// Create result array with negative offset for LEAD
	return e.createLagResult(columnExpr, partitions, window, columns, -offset)
}

// evaluateFirstValue implements FIRST_VALUE() window function.
func (e *Evaluator) evaluateFirstValue(
	expr *WindowFunctionExpr,
	window *WindowSpec,
	columns map[string]arrow.Array,
	dataLength int,
) (arrow.Array, error) {
	return e.evaluateWindowValueFunction(
		expr, window, columns, dataLength, "FIRST_VALUE", true,
	)
}

// evaluateLastValue implements LAST_VALUE() window function.
func (e *Evaluator) evaluateLastValue(
	expr *WindowFunctionExpr,
	window *WindowSpec,
	columns map[string]arrow.Array,
	dataLength int,
) (arrow.Array, error) {
	return e.evaluateWindowValueFunction(
		expr, window, columns, dataLength, "LAST_VALUE", false,
	)
}

// evaluateWindowSum implements SUM() with OVER clause.
func (e *Evaluator) evaluateWindowSum(
	expr *AggregationExpr,
	window *WindowSpec,
	columns map[string]arrow.Array,
	dataLength int,
) (arrow.Array, error) {
	// Get the column to sum
	columnExpr, err := e.Evaluate(expr.column, columns)
	if err != nil {
		return nil, fmt.Errorf("evaluating SUM column: %w", err)
	}
	defer columnExpr.Release()

	// Get partitions
	partitions, err := e.getPartitions(window, columns, dataLength)
	if err != nil {
		return nil, fmt.Errorf("getting partitions: %w", err)
	}

	return e.createWindowAggregationResult(columnExpr, partitions, AggNameSum)
}

// evaluateWindowCount implements COUNT() with OVER clause.
func (e *Evaluator) evaluateWindowCount(
	expr *AggregationExpr,
	window *WindowSpec,
	columns map[string]arrow.Array,
	dataLength int,
) (arrow.Array, error) {
	// Get the column to count
	columnExpr, err := e.Evaluate(expr.column, columns)
	if err != nil {
		return nil, fmt.Errorf("evaluating COUNT column: %w", err)
	}
	defer columnExpr.Release()

	// Get partitions
	partitions, err := e.getPartitions(window, columns, dataLength)
	if err != nil {
		return nil, fmt.Errorf("getting partitions: %w", err)
	}

	return e.createWindowAggregationResult(columnExpr, partitions, AggNameCount)
}

// evaluateWindowMean implements MEAN() with OVER clause.
func (e *Evaluator) evaluateWindowMean(
	expr *AggregationExpr,
	window *WindowSpec,
	columns map[string]arrow.Array,
	dataLength int,
) (arrow.Array, error) {
	// Get the column to average
	columnExpr, err := e.Evaluate(expr.column, columns)
	if err != nil {
		return nil, fmt.Errorf("evaluating MEAN column: %w", err)
	}
	defer columnExpr.Release()

	// Get partitions
	partitions, err := e.getPartitions(window, columns, dataLength)
	if err != nil {
		return nil, fmt.Errorf("getting partitions: %w", err)
	}

	return e.createWindowAggregationResult(columnExpr, partitions, AggNameMean)
}

// evaluateWindowMin implements MIN() with OVER clause.
func (e *Evaluator) evaluateWindowMin(
	expr *AggregationExpr,
	window *WindowSpec,
	columns map[string]arrow.Array,
	dataLength int,
) (arrow.Array, error) {
	// Get the column to find minimum
	columnExpr, err := e.Evaluate(expr.column, columns)
	if err != nil {
		return nil, fmt.Errorf("evaluating MIN column: %w", err)
	}
	defer columnExpr.Release()

	// Get partitions
	partitions, err := e.getPartitions(window, columns, dataLength)
	if err != nil {
		return nil, fmt.Errorf("getting partitions: %w", err)
	}

	return e.createWindowAggregationResult(columnExpr, partitions, AggNameMin)
}

// evaluateWindowMax implements MAX() with OVER clause.
func (e *Evaluator) evaluateWindowMax(
	expr *AggregationExpr,
	window *WindowSpec,
	columns map[string]arrow.Array,
	dataLength int,
) (arrow.Array, error) {
	// Get the column to find maximum
	columnExpr, err := e.Evaluate(expr.column, columns)
	if err != nil {
		return nil, fmt.Errorf("evaluating MAX column: %w", err)
	}
	defer columnExpr.Release()

	// Get partitions
	partitions, err := e.getPartitions(window, columns, dataLength)
	if err != nil {
		return nil, fmt.Errorf("getting partitions: %w", err)
	}

	return e.createWindowAggregationResult(columnExpr, partitions, AggNameMax)
}

// sortPartition sorts a partition based on ORDER BY clause.
func (e *Evaluator) sortPartition(
	partition []int,
	orderBy []OrderByExpr,
	columns map[string]arrow.Array,
) []int {
	if len(orderBy) == 0 {
		return partition
	}

	// Create a copy to avoid modifying the original
	sortedIndices := make([]int, len(partition))
	copy(sortedIndices, partition)

	// Use Go's efficient sort.Slice (O(n log n)) instead of bubble sort (O(n²))
	sort.Slice(sortedIndices, func(i, j int) bool {
		shouldSwap, err := e.compareRows(sortedIndices[i], sortedIndices[j], orderBy, columns)
		if err != nil {
			// In case of error, maintain original order
			return false
		}
		// The compareRows function returns true if row i should come after row j (swap needed)
		// But sort.Slice expects true if element i should come before element j
		// So we need to reverse the logic
		return !shouldSwap
	})

	return sortedIndices
}

// compareRows compares two rows based on ORDER BY clause.
func (e *Evaluator) compareRows(
	row1, row2 int,
	orderBy []OrderByExpr,
	columns map[string]arrow.Array,
) (bool, error) {
	for _, order := range orderBy {
		arr, exists := columns[order.column]
		if !exists {
			return false, fmt.Errorf("order by column not found: %s", order.column)
		}

		cmp, err := e.compareValues(arr, row1, row2)
		if err != nil {
			return false, fmt.Errorf("comparing values: %w", err)
		}

		if cmp != 0 {
			if order.ascending {
				return cmp > 0, nil
			}
			return cmp < 0, nil
		}
	}
	return false, nil
}

// compareValues compares two values in an array.
func (e *Evaluator) compareValues(arr arrow.Array, idx1, idx2 int) (int, error) {
	// Handle null values first
	if nullCmp := e.compareNullValues(arr, idx1, idx2); nullCmp != 0 {
		return nullCmp, nil
	}

	// Compare non-null values based on type
	switch a := arr.(type) {
	case *array.String:
		return e.compareStringValues(a.Value(idx1), a.Value(idx2)), nil
	case *array.Int64:
		return e.compareInt64Values(a.Value(idx1), a.Value(idx2)), nil
	case *array.Float64:
		return e.compareFloat64Values(a.Value(idx1), a.Value(idx2)), nil
	case *array.Boolean:
		return e.compareBooleanValues(a.Value(idx1), a.Value(idx2)), nil
	default:
		return 0, fmt.Errorf("unsupported type for comparison: %T", arr)
	}
}

// compareNullValues handles null value comparison logic.
func (e *Evaluator) compareNullValues(arr arrow.Array, idx1, idx2 int) int {
	isNull1, isNull2 := arr.IsNull(idx1), arr.IsNull(idx2)
	if isNull1 && isNull2 {
		return 0 // Both null, equal
	}
	if isNull1 {
		return -1 // Null is less than non-null
	}
	if isNull2 {
		return 1 // Non-null is greater than null
	}
	return 0 // Neither is null, continue with type-specific comparison
}

// compareStringValues compares two string values.
func (e *Evaluator) compareStringValues(v1, v2 string) int {
	if v1 < v2 {
		return -1
	} else if v1 > v2 {
		return 1
	}
	return 0
}

// compareInt64Values compares two int64 values.
func (e *Evaluator) compareInt64Values(v1, v2 int64) int {
	if v1 < v2 {
		return -1
	} else if v1 > v2 {
		return 1
	}
	return 0
}

// compareFloat64Values compares two float64 values.
func (e *Evaluator) compareFloat64Values(v1, v2 float64) int {
	if v1 < v2 {
		return -1
	} else if v1 > v2 {
		return 1
	}
	return 0
}

// compareBooleanValues compares two boolean values.
func (e *Evaluator) compareBooleanValues(v1, v2 bool) int {
	if !v1 && v2 {
		return -1 // false < true
	} else if v1 && !v2 {
		return 1 // true > false
	}
	return 0 // Both same
}

// rowsEqual checks if two rows have equal values for specified columns.
func (e *Evaluator) rowsEqual(
	row1, row2 int,
	orderBy []OrderByExpr,
	columns map[string]arrow.Array,
) bool {
	for _, order := range orderBy {
		arr, exists := columns[order.column]
		if !exists {
			return false
		}

		cmp, err := e.compareValues(arr, row1, row2)
		if err != nil || cmp != 0 {
			return false
		}
	}
	return true
}

// createLagResult creates result array for LAG/LEAD functions.
func (e *Evaluator) createLagResult(
	columnExpr arrow.Array,
	partitions [][]int,
	window *WindowSpec,
	columns map[string]arrow.Array,
	offset int64,
) (arrow.Array, error) {
	dataLength := columnExpr.Len()
	result := make([]interface{}, dataLength)

	// Process partitions with LAG/LEAD logic
	for _, partition := range partitions {
		sortedIndices := partition
		if len(window.orderBy) > 0 {
			sortedIndices = e.sortPartitionOptimized(partition, window.orderBy, columns)
		}

		for i, idx := range sortedIndices {
			lagIndex := i + int(offset)
			if lagIndex >= 0 && lagIndex < len(sortedIndices) {
				srcIdx := sortedIndices[lagIndex]
				if columnExpr.IsNull(srcIdx) {
					result[idx] = nil
				} else {
					result[idx] = e.getArrayValue(columnExpr, srcIdx)
				}
			} else {
				result[idx] = nil
			}
		}
	}

	return e.buildTypedArrayResult(result, dataLength, e.getArrayType(columnExpr))
}

// evaluateWindowValueFunction helper for FIRST_VALUE/LAST_VALUE functions.
func (e *Evaluator) evaluateWindowValueFunction(
	expr *WindowFunctionExpr,
	window *WindowSpec,
	columns map[string]arrow.Array,
	dataLength int,
	funcName string,
	isFirst bool,
) (arrow.Array, error) {
	if len(expr.args) != 1 {
		return nil, fmt.Errorf("%s function requires exactly 1 argument", funcName)
	}

	// Get the column
	columnExpr, err := e.Evaluate(expr.args[0], columns)
	if err != nil {
		return nil, fmt.Errorf("evaluating %s column: %w", funcName, err)
	}
	defer columnExpr.Release()

	// Get partitions
	partitions, err := e.getPartitions(window, columns, dataLength)
	if err != nil {
		return nil, fmt.Errorf("getting partitions: %w", err)
	}

	return e.createFirstLastResult(columnExpr, partitions, window, columns, isFirst)
}

// createFirstLastResult creates result array for FIRST_VALUE/LAST_VALUE functions.
func (e *Evaluator) createFirstLastResult(
	columnExpr arrow.Array,
	partitions [][]int,
	window *WindowSpec,
	columns map[string]arrow.Array,
	isFirst bool,
) (arrow.Array, error) {
	dataLength := columnExpr.Len()
	result := make([]interface{}, dataLength)

	// Process partitions with FIRST_VALUE/LAST_VALUE logic
	for _, partition := range partitions {
		sortedIndices := partition
		if len(window.orderBy) > 0 {
			sortedIndices = e.sortPartitionOptimized(partition, window.orderBy, columns)
		}

		// Get first or last value
		var valueIdx int
		if isFirst {
			valueIdx = sortedIndices[0]
		} else {
			valueIdx = sortedIndices[len(sortedIndices)-1]
		}

		var value interface{}
		if columnExpr.IsNull(valueIdx) {
			value = nil
		} else {
			value = e.getArrayValue(columnExpr, valueIdx)
		}

		// Set the same value for all rows in partition
		for _, idx := range partition {
			result[idx] = value
		}
	}

	return e.buildTypedArrayResult(result, dataLength, e.getArrayType(columnExpr))
}

// createWindowAggregationResult creates result array for window aggregation functions.
func (e *Evaluator) createWindowAggregationResult(
	columnExpr arrow.Array,
	partitions [][]int,
	aggType string,
) (arrow.Array, error) {
	dataLength := columnExpr.Len()

	// For now, implement simple partition-based aggregation
	// TODO: Add support for window frames

	switch arr := columnExpr.(type) {
	case *array.Int64:
		return e.createInt64AggregationResult(arr, partitions, aggType, dataLength)
	case *array.Float64:
		return e.createFloat64AggregationResult(arr, partitions, aggType, dataLength)
	default:
		return nil, fmt.Errorf("unsupported column type for window aggregation: %T", columnExpr)
	}
}

// getArrayType returns the type string for an Arrow array.
func (e *Evaluator) getArrayType(arr arrow.Array) string {
	switch arr.(type) {
	case *array.Int64:
		return typeInt64
	case *array.String:
		return "string"
	default:
		return "unknown"
	}
}

// getArrayValue returns the value at the given index from an Arrow array.
func (e *Evaluator) getArrayValue(arr arrow.Array, idx int) interface{} {
	switch a := arr.(type) {
	case *array.Int64:
		return a.Value(idx)
	case *array.String:
		return a.Value(idx)
	default:
		return nil
	}
}

// buildTypedArrayResult is a helper function to build typed array results.
func (e *Evaluator) buildTypedArrayResult(
	result []interface{},
	dataLength int,
	arrayType string,
) (arrow.Array, error) {
	switch arrayType {
	case typeInt64:
		return e.buildInt64Array(result, dataLength)
	case "string":
		return e.buildStringArray(result, dataLength)
	case typeFloat64:
		return e.buildFloat64Array(result, dataLength)
	default:
		return nil, fmt.Errorf("unsupported array type: %s", arrayType)
	}
}

// buildInt64Array builds an Int64 array from interface{} slice.
func (e *Evaluator) buildInt64Array(result []interface{}, dataLength int) (arrow.Array, error) {
	builder := array.NewInt64Builder(e.mem)
	defer builder.Release()

	for i := range dataLength {
		if result[i] == nil {
			builder.AppendNull()
		} else if int64Val, ok := result[i].(int64); ok {
			builder.Append(int64Val)
		} else {
			builder.AppendNull() // Fallback for type assertion failure.
		}
	}
	return builder.NewArray(), nil
}

// buildStringArray builds a String array from interface{} slice.
func (e *Evaluator) buildStringArray(result []interface{}, dataLength int) (arrow.Array, error) {
	builder := array.NewStringBuilder(e.mem)
	defer builder.Release()

	for i := range dataLength {
		if result[i] == nil {
			builder.AppendNull()
		} else if stringVal, ok := result[i].(string); ok {
			builder.Append(stringVal)
		} else {
			builder.AppendNull() // Fallback for type assertion failure.
		}
	}
	return builder.NewArray(), nil
}

// buildFloat64Array builds a Float64 array from interface{} slice.
func (e *Evaluator) buildFloat64Array(result []interface{}, dataLength int) (arrow.Array, error) {
	builder := array.NewFloat64Builder(e.mem)
	defer builder.Release()

	for i := range dataLength {
		if result[i] == nil {
			builder.AppendNull()
		} else if float64Val, ok := result[i].(float64); ok {
			builder.Append(float64Val)
		} else {
			builder.AppendNull() // Fallback for type assertion failure.
		}
	}
	return builder.NewArray(), nil
}

// createInt64AggregationResult creates aggregation result for Int64 arrays.
func (e *Evaluator) createInt64AggregationResult(
	arr *array.Int64,
	partitions [][]int,
	aggType string,
	dataLength int,
) (arrow.Array, error) {
	result := make([]interface{}, dataLength)

	for _, partition := range partitions {
		aggValue := e.calculateInt64Aggregation(arr, partition, aggType)

		// Set the same value for all rows in partition
		for _, idx := range partition {
			result[idx] = aggValue
		}
	}

	return e.buildTypedArrayResult(result, dataLength, typeInt64)
}

// createFloat64AggregationResult creates aggregation result for Float64 arrays.
func (e *Evaluator) createFloat64AggregationResult(
	arr *array.Float64,
	partitions [][]int,
	aggType string,
	dataLength int,
) (arrow.Array, error) {
	result := make([]interface{}, dataLength)

	for _, partition := range partitions {
		aggValue := e.calculateFloat64Aggregation(arr, partition, aggType)

		// Set the same value for all rows in partition
		for _, idx := range partition {
			result[idx] = aggValue
		}
	}

	return e.buildTypedArrayResult(result, dataLength, typeFloat64)
}

// calculateInt64Aggregation calculates aggregation for Int64 values.
func (e *Evaluator) calculateInt64Aggregation(
	arr *array.Int64,
	partition []int,
	aggType string,
) int64 {
	var aggValue int64
	var count int64

	for _, idx := range partition {
		if !arr.IsNull(idx) {
			val := arr.Value(idx)
			switch aggType {
			case AggNameSum:
				aggValue += val
			case AggNameCount:
				count++
			case AggNameMean:
				aggValue += val
				count++
			case AggNameMin:
				if count == 0 || val < aggValue {
					aggValue = val
				}
				count++
			case AggNameMax:
				if count == 0 || val > aggValue {
					aggValue = val
				}
				count++
			}
		}
	}

	if aggType == AggNameMean && count > 0 {
		aggValue /= count
	}
	if aggType == AggNameCount {
		aggValue = count
	}

	return aggValue
}

// calculateFloat64Aggregation calculates aggregation for Float64 values.
func (e *Evaluator) calculateFloat64Aggregation(
	arr *array.Float64,
	partition []int,
	aggType string,
) float64 {
	var aggValue float64
	var count int64

	for _, idx := range partition {
		if !arr.IsNull(idx) {
			val := arr.Value(idx)
			switch aggType {
			case AggNameSum:
				aggValue += val
			case AggNameCount:
				count++
			case AggNameMean:
				aggValue += val
				count++
			case AggNameMin:
				if count == 0 || val < aggValue {
					aggValue = val
				}
				count++
			case AggNameMax:
				if count == 0 || val > aggValue {
					aggValue = val
				}
				count++
			}
		}
	}

	if aggType == AggNameMean && count > 0 {
		aggValue /= float64(count)
	}
	if aggType == AggNameCount {
		aggValue = float64(count)
	}

	return aggValue
}

// evaluatePercentRank implements PERCENT_RANK() window function.
func (e *Evaluator) evaluatePercentRank(
	_ *WindowFunctionExpr,
	window *WindowSpec,
	columns map[string]arrow.Array,
	dataLength int,
) (arrow.Array, error) {
	mem := e.mem

	// Create result builder
	builder := array.NewFloat64Builder(mem)
	defer builder.Release()

	// Handle partitioning
	if len(window.partitionBy) > 0 {
		e.processPartitionedPercentRank(builder, window, columns, dataLength)
	} else {
		e.processUnpartitionedPercentRank(builder, window, columns, dataLength)
	}

	return builder.NewArray(), nil
}

// evaluateCumeDist implements CUME_DIST() window function.
func (e *Evaluator) evaluateCumeDist(
	_ *WindowFunctionExpr,
	window *WindowSpec,
	columns map[string]arrow.Array,
	dataLength int,
) (arrow.Array, error) {
	mem := e.mem

	// Create result builder
	builder := array.NewFloat64Builder(mem)
	defer builder.Release()

	// Handle partitioning
	if len(window.partitionBy) > 0 {
		// Process each partition separately
		partitions := e.buildPartitions(window.partitionBy, columns, dataLength)

		for _, partition := range partitions {
			cumeDist := e.calculateCumulativeDistribution(partition, window, columns)

			for _, dist := range cumeDist {
				builder.Append(dist)
			}
		}
	} else {
		// No partitioning - calculate for entire dataset
		fullDataset := make([]int, dataLength)
		for i := range dataLength {
			fullDataset[i] = i
		}
		cumeDist := e.calculateCumulativeDistribution(fullDataset, window, columns)

		for _, dist := range cumeDist {
			builder.Append(dist)
		}
	}

	return builder.NewArray(), nil
}

const (
	// nthValueMinArgs is the minimum number of arguments required for NTH_VALUE function.
	nthValueMinArgs = 2
)

// evaluateNthValue implements NTH_VALUE() window function.
func (e *Evaluator) evaluateNthValue(
	expr *WindowFunctionExpr,
	_ *WindowSpec,
	columns map[string]arrow.Array,
	dataLength int,
) (arrow.Array, error) {
	n, column, err := e.validateAndExtractNthValueArgs(expr, columns)
	if err != nil {
		return nil, err
	}

	return e.buildNthValueResult(column, n, dataLength)
}

// validateAndExtractNthValueArgs validates NTH_VALUE arguments and extracts n and column.
func (e *Evaluator) validateAndExtractNthValueArgs(
	expr *WindowFunctionExpr,
	columns map[string]arrow.Array,
) (int, arrow.Array, error) {
	if len(expr.args) < nthValueMinArgs {
		return 0, nil, errors.New("NTH_VALUE requires two arguments")
	}

	n, err := e.extractNthValuePosition(expr.args[1])
	if err != nil {
		return 0, nil, err
	}

	column, err := e.extractNthValueColumn(expr.args[0], columns)
	if err != nil {
		return 0, nil, err
	}

	return n, column, nil
}

// extractNthValuePosition extracts and validates the position argument.
func (e *Evaluator) extractNthValuePosition(arg Expr) (int, error) {
	nLit, ok := arg.(*LiteralExpr)
	if !ok {
		return 0, errors.New("NTH_VALUE second argument must be a literal")
	}

	n, ok := nLit.value.(int)
	if !ok || n <= 0 {
		return 0, errors.New("NTH_VALUE second argument must be a positive integer")
	}

	return n, nil
}

// extractNthValueColumn extracts and validates the column argument.
func (e *Evaluator) extractNthValueColumn(arg Expr, columns map[string]arrow.Array) (arrow.Array, error) {
	colExpr, ok := arg.(*ColumnExpr)
	if !ok {
		return nil, errors.New("NTH_VALUE first argument must be a column")
	}

	column, exists := columns[colExpr.name]
	if !exists {
		return nil, fmt.Errorf("column %s not found", colExpr.name)
	}

	return column, nil
}

// buildNthValueResult builds the result array based on column type.
func (e *Evaluator) buildNthValueResult(column arrow.Array, n, dataLength int) (arrow.Array, error) {
	switch arr := column.(type) {
	case *array.Int64:
		return e.buildNthValueInt64Result(arr, n, dataLength)
	case *array.String:
		return e.buildNthValueStringResult(arr, n, dataLength)
	default:
		return nil, fmt.Errorf("unsupported column type for NTH_VALUE: %T", column)
	}
}

// buildNthValueInt64Result builds Int64 result for NTH_VALUE.
func (e *Evaluator) buildNthValueInt64Result(arr *array.Int64, n, dataLength int) (arrow.Array, error) {
	builder := array.NewInt64Builder(e.mem)
	defer builder.Release()

	for range dataLength {
		if n <= arr.Len() && !arr.IsNull(n-1) {
			builder.Append(arr.Value(n - 1))
		} else {
			builder.AppendNull()
		}
	}
	return builder.NewArray(), nil
}

// buildNthValueStringResult builds String result for NTH_VALUE.
func (e *Evaluator) buildNthValueStringResult(arr *array.String, n, dataLength int) (arrow.Array, error) {
	builder := array.NewStringBuilder(e.mem)
	defer builder.Release()

	for range dataLength {
		if n <= arr.Len() && !arr.IsNull(n-1) {
			builder.Append(arr.Value(n - 1))
		} else {
			builder.AppendNull()
		}
	}
	return builder.NewArray(), nil
}

// evaluateNtile implements NTILE() window function.
func (e *Evaluator) evaluateNtile(
	expr *WindowFunctionExpr,
	window *WindowSpec,
	columns map[string]arrow.Array,
	dataLength int,
) (arrow.Array, error) {
	if len(expr.args) == 0 {
		return nil, errors.New("NTILE requires one argument")
	}

	// Get the number of buckets
	bucketsLit, ok := expr.args[0].(*LiteralExpr)
	if !ok {
		return nil, errors.New("NTILE argument must be a literal")
	}
	buckets, ok := bucketsLit.value.(int)
	if !ok {
		return nil, errors.New("NTILE argument must be an integer")
	}

	if buckets <= 0 {
		return nil, errors.New("NTILE buckets must be positive")
	}

	mem := e.mem
	builder := array.NewInt64Builder(mem)
	defer builder.Release()

	// Handle partitioning
	if len(window.partitionBy) > 0 {
		// Process each partition separately
		partitions := e.buildPartitions(window.partitionBy, columns, dataLength)

		for _, partition := range partitions {
			ntiles := e.calculateNtiles(len(partition), buckets)
			for _, ntile := range ntiles {
				builder.Append(int64(ntile))
			}
		}
	} else {
		// No partitioning - calculate for entire dataset
		ntiles := e.calculateNtiles(dataLength, buckets)
		for _, ntile := range ntiles {
			builder.Append(int64(ntile))
		}
	}

	return builder.NewArray(), nil
}

// Helper methods for window function calculations

// calculateRanks calculates ranks for ordering.
func (e *Evaluator) calculateRanks(_ *WindowSpec, _ map[string]arrow.Array, dataLength int) []int {
	// Simplified rank calculation - in reality this would need proper ordering
	ranks := make([]int, dataLength)
	for i := range dataLength {
		ranks[i] = i + 1
	}
	return ranks
}

// calculateRanksForPartition calculates ranks within a partition.
func (e *Evaluator) calculateRanksForPartition(
	partition []int,
	_ *WindowSpec,
	_ map[string]arrow.Array,
) []int {
	ranks := make([]int, len(partition))
	for i := range partition {
		ranks[i] = i + 1
	}
	return ranks
}

// calculateCumulativeDistribution calculates cumulative distribution.
func (e *Evaluator) calculateCumulativeDistribution(
	partition []int,
	_ *WindowSpec,
	_ map[string]arrow.Array,
) []float64 {
	size := len(partition)
	cumeDist := make([]float64, size)
	for i := range size {
		cumeDist[i] = float64(i+1) / float64(size)
	}
	return cumeDist
}

// calculateNtiles distributes rows into buckets.
func (e *Evaluator) calculateNtiles(rowCount, buckets int) []int {
	ntiles := make([]int, rowCount)

	// Calculate base bucket size and remainder
	baseSize := rowCount / buckets
	remainder := rowCount % buckets

	// Distribute rows into buckets
	currentRow := 0
	for bucket := 1; bucket <= buckets; bucket++ {
		bucketSize := baseSize
		if remainder > 0 {
			bucketSize++
			remainder--
		}

		for i := 0; i < bucketSize && currentRow < rowCount; i++ {
			ntiles[currentRow] = bucket
			currentRow++
		}
	}

	return ntiles
}

// buildPartitions creates partitions based on partition columns.
func (e *Evaluator) buildPartitions(
	_ []string,
	_ map[string]arrow.Array,
	dataLength int,
) [][]int {
	// Simplified partitioning - in reality this would need proper grouping logic
	// For now, return a single partition with all rows
	partition := make([]int, dataLength)
	for i := range dataLength {
		partition[i] = i
	}
	return [][]int{partition}
}

// processPartitionedPercentRank processes percent rank for partitioned data.
func (e *Evaluator) processPartitionedPercentRank(
	builder *array.Float64Builder,
	window *WindowSpec,
	columns map[string]arrow.Array,
	dataLength int,
) {
	partitions := e.buildPartitions(window.partitionBy, columns, dataLength)

	for _, partition := range partitions {
		ranks := e.calculateRanksForPartition(partition, window, columns)

		// Convert ranks to percent ranks
		partitionSize := len(partition)
		for _, rank := range ranks {
			var percentRank float64
			if partitionSize <= 1 {
				percentRank = 0.0
			} else {
				percentRank = float64(rank-1) / float64(partitionSize-1)
			}
			builder.Append(percentRank)
		}
	}
}

// processUnpartitionedPercentRank processes percent rank for unpartitioned data.
func (e *Evaluator) processUnpartitionedPercentRank(
	builder *array.Float64Builder,
	window *WindowSpec,
	columns map[string]arrow.Array,
	dataLength int,
) {
	ranks := e.calculateRanks(window, columns, dataLength)

	for _, rank := range ranks {
		var percentRank float64
		if dataLength <= 1 {
			percentRank = 0.0
		} else {
			percentRank = float64(rank-1) / float64(dataLength-1)
		}
		builder.Append(percentRank)
	}
}
