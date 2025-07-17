package expr

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// evaluateRank implements RANK() window function
func (e *Evaluator) evaluateRank(window *WindowSpec, columns map[string]arrow.Array, dataLength int) (arrow.Array, error) {
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
			sortedIndices, err = e.sortPartition(partition, window.orderBy, columns)
			if err != nil {
				return nil, fmt.Errorf("sorting partition: %w", err)
			}
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
	for i := 0; i < dataLength; i++ {
		builder.Append(result[i])
	}

	return builder.NewArray(), nil
}

// evaluateDenseRank implements DENSE_RANK() window function
func (e *Evaluator) evaluateDenseRank(window *WindowSpec, columns map[string]arrow.Array, dataLength int) (arrow.Array, error) {
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
			sortedIndices, err = e.sortPartition(partition, window.orderBy, columns)
			if err != nil {
				return nil, fmt.Errorf("sorting partition: %w", err)
			}
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

	for i := 0; i < dataLength; i++ {
		builder.Append(result[i])
	}

	return builder.NewArray(), nil
}

// evaluateLag implements LAG() window function
func (e *Evaluator) evaluateLag(expr *WindowFunctionExpr, window *WindowSpec, columns map[string]arrow.Array, dataLength int) (arrow.Array, error) {
	if len(expr.args) < 1 {
		return nil, fmt.Errorf("LAG function requires at least 1 argument")
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
		offsetExpr, err := e.Evaluate(expr.args[1], columns)
		if err != nil {
			return nil, fmt.Errorf("evaluating LAG offset: %w", err)
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

// evaluateLead implements LEAD() window function
func (e *Evaluator) evaluateLead(expr *WindowFunctionExpr, window *WindowSpec, columns map[string]arrow.Array, dataLength int) (arrow.Array, error) {
	if len(expr.args) < 1 {
		return nil, fmt.Errorf("LEAD function requires at least 1 argument")
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
		offsetExpr, err := e.Evaluate(expr.args[1], columns)
		if err != nil {
			return nil, fmt.Errorf("evaluating LEAD offset: %w", err)
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

// evaluateFirstValue implements FIRST_VALUE() window function
func (e *Evaluator) evaluateFirstValue(expr *WindowFunctionExpr, window *WindowSpec, columns map[string]arrow.Array, dataLength int) (arrow.Array, error) {
	if len(expr.args) != 1 {
		return nil, fmt.Errorf("FIRST_VALUE function requires exactly 1 argument")
	}

	// Get the column
	columnExpr, err := e.Evaluate(expr.args[0], columns)
	if err != nil {
		return nil, fmt.Errorf("evaluating FIRST_VALUE column: %w", err)
	}
	defer columnExpr.Release()

	// Get partitions
	partitions, err := e.getPartitions(window, columns, dataLength)
	if err != nil {
		return nil, fmt.Errorf("getting partitions: %w", err)
	}

	return e.createFirstLastResult(columnExpr, partitions, window, columns, true)
}

// evaluateLastValue implements LAST_VALUE() window function
func (e *Evaluator) evaluateLastValue(expr *WindowFunctionExpr, window *WindowSpec, columns map[string]arrow.Array, dataLength int) (arrow.Array, error) {
	if len(expr.args) != 1 {
		return nil, fmt.Errorf("LAST_VALUE function requires exactly 1 argument")
	}

	// Get the column
	columnExpr, err := e.Evaluate(expr.args[0], columns)
	if err != nil {
		return nil, fmt.Errorf("evaluating LAST_VALUE column: %w", err)
	}
	defer columnExpr.Release()

	// Get partitions
	partitions, err := e.getPartitions(window, columns, dataLength)
	if err != nil {
		return nil, fmt.Errorf("getting partitions: %w", err)
	}

	return e.createFirstLastResult(columnExpr, partitions, window, columns, false)
}

// evaluateWindowSum implements SUM() with OVER clause
func (e *Evaluator) evaluateWindowSum(expr *AggregationExpr, window *WindowSpec, columns map[string]arrow.Array, dataLength int) (arrow.Array, error) {
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

	return e.createWindowAggregationResult(columnExpr, partitions, window, columns, "sum")
}

// evaluateWindowCount implements COUNT() with OVER clause
func (e *Evaluator) evaluateWindowCount(expr *AggregationExpr, window *WindowSpec, columns map[string]arrow.Array, dataLength int) (arrow.Array, error) {
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

	return e.createWindowAggregationResult(columnExpr, partitions, window, columns, "count")
}

// evaluateWindowMean implements MEAN() with OVER clause
func (e *Evaluator) evaluateWindowMean(expr *AggregationExpr, window *WindowSpec, columns map[string]arrow.Array, dataLength int) (arrow.Array, error) {
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

	return e.createWindowAggregationResult(columnExpr, partitions, window, columns, "mean")
}

// evaluateWindowMin implements MIN() with OVER clause
func (e *Evaluator) evaluateWindowMin(expr *AggregationExpr, window *WindowSpec, columns map[string]arrow.Array, dataLength int) (arrow.Array, error) {
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

	return e.createWindowAggregationResult(columnExpr, partitions, window, columns, "min")
}

// evaluateWindowMax implements MAX() with OVER clause
func (e *Evaluator) evaluateWindowMax(expr *AggregationExpr, window *WindowSpec, columns map[string]arrow.Array, dataLength int) (arrow.Array, error) {
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

	return e.createWindowAggregationResult(columnExpr, partitions, window, columns, "max")
}

// sortPartition sorts a partition based on ORDER BY clause
func (e *Evaluator) sortPartition(partition []int, orderBy []OrderByExpr, columns map[string]arrow.Array) ([]int, error) {
	if len(orderBy) == 0 {
		return partition, nil
	}

	// Create a copy to avoid modifying the original
	sortedIndices := make([]int, len(partition))
	copy(sortedIndices, partition)

	// Simple bubble sort for now (could be optimized)
	for i := 0; i < len(sortedIndices); i++ {
		for j := i + 1; j < len(sortedIndices); j++ {
			shouldSwap, err := e.compareRows(sortedIndices[i], sortedIndices[j], orderBy, columns)
			if err != nil {
				return nil, fmt.Errorf("comparing rows: %w", err)
			}
			if shouldSwap {
				sortedIndices[i], sortedIndices[j] = sortedIndices[j], sortedIndices[i]
			}
		}
	}

	return sortedIndices, nil
}

// compareRows compares two rows based on ORDER BY clause
func (e *Evaluator) compareRows(row1, row2 int, orderBy []OrderByExpr, columns map[string]arrow.Array) (bool, error) {
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
			} else {
				return cmp < 0, nil
			}
		}
	}
	return false, nil
}

// compareValues compares two values in an array
func (e *Evaluator) compareValues(arr arrow.Array, idx1, idx2 int) (int, error) {
	if arr.IsNull(idx1) && arr.IsNull(idx2) {
		return 0, nil
	}
	if arr.IsNull(idx1) {
		return -1, nil
	}
	if arr.IsNull(idx2) {
		return 1, nil
	}

	switch a := arr.(type) {
	case *array.String:
		v1, v2 := a.Value(idx1), a.Value(idx2)
		if v1 < v2 {
			return -1, nil
		} else if v1 > v2 {
			return 1, nil
		}
		return 0, nil
	case *array.Int64:
		v1, v2 := a.Value(idx1), a.Value(idx2)
		if v1 < v2 {
			return -1, nil
		} else if v1 > v2 {
			return 1, nil
		}
		return 0, nil
	case *array.Float64:
		v1, v2 := a.Value(idx1), a.Value(idx2)
		if v1 < v2 {
			return -1, nil
		} else if v1 > v2 {
			return 1, nil
		}
		return 0, nil
	case *array.Boolean:
		v1, v2 := a.Value(idx1), a.Value(idx2)
		if !v1 && v2 {
			return -1, nil
		} else if v1 && !v2 {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("unsupported type for comparison: %T", arr)
	}
}

// rowsEqual checks if two rows have equal values for specified columns
func (e *Evaluator) rowsEqual(row1, row2 int, orderBy []OrderByExpr, columns map[string]arrow.Array) bool {
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

// createLagResult creates result array for LAG/LEAD functions
func (e *Evaluator) createLagResult(columnExpr arrow.Array, partitions [][]int, window *WindowSpec, columns map[string]arrow.Array, offset int64) (arrow.Array, error) {
	dataLength := columnExpr.Len()

	// Create result array based on column type
	switch arr := columnExpr.(type) {
	case *array.Int64:
		builder := array.NewInt64Builder(e.mem)
		defer builder.Release()

		result := make([]interface{}, dataLength)

		for _, partition := range partitions {
			sortedIndices := partition
			if len(window.orderBy) > 0 {
				var err error
				sortedIndices, err = e.sortPartition(partition, window.orderBy, columns)
				if err != nil {
					return nil, fmt.Errorf("sorting partition: %w", err)
				}
			}

			for i, idx := range sortedIndices {
				lagIndex := i + int(offset)
				if lagIndex >= 0 && lagIndex < len(sortedIndices) {
					srcIdx := sortedIndices[lagIndex]
					if arr.IsNull(srcIdx) {
						result[idx] = nil
					} else {
						result[idx] = arr.Value(srcIdx)
					}
				} else {
					result[idx] = nil
				}
			}
		}

		for i := 0; i < dataLength; i++ {
			if result[i] == nil {
				builder.AppendNull()
			} else {
				builder.Append(result[i].(int64))
			}
		}

		return builder.NewArray(), nil

	case *array.String:
		builder := array.NewStringBuilder(e.mem)
		defer builder.Release()

		result := make([]interface{}, dataLength)

		for _, partition := range partitions {
			sortedIndices := partition
			if len(window.orderBy) > 0 {
				var err error
				sortedIndices, err = e.sortPartition(partition, window.orderBy, columns)
				if err != nil {
					return nil, fmt.Errorf("sorting partition: %w", err)
				}
			}

			for i, idx := range sortedIndices {
				lagIndex := i + int(offset)
				if lagIndex >= 0 && lagIndex < len(sortedIndices) {
					srcIdx := sortedIndices[lagIndex]
					if arr.IsNull(srcIdx) {
						result[idx] = nil
					} else {
						result[idx] = arr.Value(srcIdx)
					}
				} else {
					result[idx] = nil
				}
			}
		}

		for i := 0; i < dataLength; i++ {
			if result[i] == nil {
				builder.AppendNull()
			} else {
				builder.Append(result[i].(string))
			}
		}

		return builder.NewArray(), nil

	default:
		return nil, fmt.Errorf("unsupported column type for LAG/LEAD: %T", columnExpr)
	}
}

// createFirstLastResult creates result array for FIRST_VALUE/LAST_VALUE functions
func (e *Evaluator) createFirstLastResult(columnExpr arrow.Array, partitions [][]int, window *WindowSpec, columns map[string]arrow.Array, isFirst bool) (arrow.Array, error) {
	dataLength := columnExpr.Len()

	// Create result array based on column type
	switch arr := columnExpr.(type) {
	case *array.Int64:
		builder := array.NewInt64Builder(e.mem)
		defer builder.Release()

		result := make([]interface{}, dataLength)

		for _, partition := range partitions {
			sortedIndices := partition
			if len(window.orderBy) > 0 {
				var err error
				sortedIndices, err = e.sortPartition(partition, window.orderBy, columns)
				if err != nil {
					return nil, fmt.Errorf("sorting partition: %w", err)
				}
			}

			// Get first or last value
			var valueIdx int
			if isFirst {
				valueIdx = sortedIndices[0]
			} else {
				valueIdx = sortedIndices[len(sortedIndices)-1]
			}

			var value interface{}
			if arr.IsNull(valueIdx) {
				value = nil
			} else {
				value = arr.Value(valueIdx)
			}

			// Set the same value for all rows in partition
			for _, idx := range partition {
				result[idx] = value
			}
		}

		for i := 0; i < dataLength; i++ {
			if result[i] == nil {
				builder.AppendNull()
			} else {
				builder.Append(result[i].(int64))
			}
		}

		return builder.NewArray(), nil

	case *array.String:
		builder := array.NewStringBuilder(e.mem)
		defer builder.Release()

		result := make([]interface{}, dataLength)

		for _, partition := range partitions {
			sortedIndices := partition
			if len(window.orderBy) > 0 {
				var err error
				sortedIndices, err = e.sortPartition(partition, window.orderBy, columns)
				if err != nil {
					return nil, fmt.Errorf("sorting partition: %w", err)
				}
			}

			// Get first or last value
			var valueIdx int
			if isFirst {
				valueIdx = sortedIndices[0]
			} else {
				valueIdx = sortedIndices[len(sortedIndices)-1]
			}

			var value interface{}
			if arr.IsNull(valueIdx) {
				value = nil
			} else {
				value = arr.Value(valueIdx)
			}

			// Set the same value for all rows in partition
			for _, idx := range partition {
				result[idx] = value
			}
		}

		for i := 0; i < dataLength; i++ {
			if result[i] == nil {
				builder.AppendNull()
			} else {
				builder.Append(result[i].(string))
			}
		}

		return builder.NewArray(), nil

	default:
		return nil, fmt.Errorf("unsupported column type for FIRST_VALUE/LAST_VALUE: %T", columnExpr)
	}
}

// createWindowAggregationResult creates result array for window aggregation functions
func (e *Evaluator) createWindowAggregationResult(columnExpr arrow.Array, partitions [][]int, window *WindowSpec, columns map[string]arrow.Array, aggType string) (arrow.Array, error) {
	dataLength := columnExpr.Len()

	// For now, implement simple partition-based aggregation
	// TODO: Add support for window frames

	switch arr := columnExpr.(type) {
	case *array.Int64:
		builder := array.NewInt64Builder(e.mem)
		defer builder.Release()

		result := make([]int64, dataLength)

		for _, partition := range partitions {
			// Calculate aggregation for the partition
			var aggValue int64
			var count int64

			for _, idx := range partition {
				if !arr.IsNull(idx) {
					val := arr.Value(idx)
					switch aggType {
					case "sum":
						aggValue += val
					case "count":
						count++
					case "mean":
						aggValue += val
						count++
					case "min":
						if count == 0 || val < aggValue {
							aggValue = val
						}
						count++
					case "max":
						if count == 0 || val > aggValue {
							aggValue = val
						}
						count++
					}
				}
			}

			if aggType == "mean" && count > 0 {
				aggValue = aggValue / count
			}
			if aggType == "count" {
				aggValue = count
			}

			// Set the same value for all rows in partition
			for _, idx := range partition {
				result[idx] = aggValue
			}
		}

		for i := 0; i < dataLength; i++ {
			builder.Append(result[i])
		}

		return builder.NewArray(), nil

	case *array.Float64:
		builder := array.NewFloat64Builder(e.mem)
		defer builder.Release()

		result := make([]float64, dataLength)

		for _, partition := range partitions {
			// Calculate aggregation for the partition
			var aggValue float64
			var count int64

			for _, idx := range partition {
				if !arr.IsNull(idx) {
					val := arr.Value(idx)
					switch aggType {
					case "sum":
						aggValue += val
					case "count":
						count++
					case "mean":
						aggValue += val
						count++
					case "min":
						if count == 0 || val < aggValue {
							aggValue = val
						}
						count++
					case "max":
						if count == 0 || val > aggValue {
							aggValue = val
						}
						count++
					}
				}
			}

			if aggType == "mean" && count > 0 {
				aggValue = aggValue / float64(count)
			}
			if aggType == "count" {
				aggValue = float64(count)
			}

			// Set the same value for all rows in partition
			for _, idx := range partition {
				result[idx] = aggValue
			}
		}

		for i := 0; i < dataLength; i++ {
			builder.Append(result[i])
		}

		return builder.NewArray(), nil

	default:
		return nil, fmt.Errorf("unsupported column type for window aggregation: %T", columnExpr)
	}
}
