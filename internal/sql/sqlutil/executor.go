package sqlutil

import (
	"errors"
	"fmt"
	"math"

	"github.com/paveg/gorilla/internal/dataframe"
)

// SafeInt64ToInt safely converts int64 to int with bounds checking.
func SafeInt64ToInt(value int64) (int, error) {
	if value < 0 {
		return 0, fmt.Errorf("negative value not allowed: %d", value)
	}
	if value > math.MaxInt {
		return 0, fmt.Errorf("value exceeds int range: %d", value)
	}
	return int(value), nil
}

// CreateEmptyDataFrame creates an empty DataFrame with the same schema as the template.
func CreateEmptyDataFrame(template *dataframe.DataFrame) *dataframe.DataFrame {
	return template.Slice(0, 0)
}

// ApplyLimitOffset applies LIMIT and OFFSET operations to a DataFrame.
func ApplyLimitOffset(df *dataframe.DataFrame, limit, offset int64) (*dataframe.DataFrame, error) {
	if df == nil {
		return nil, errors.New("DataFrame cannot be nil")
	}

	totalRows := df.Len()

	// Safe conversion with bounds checking
	offsetInt, err := SafeInt64ToInt(offset)
	if err != nil {
		// NOTE: This returns empty results for invalid OFFSET values (e.g., negative values)
		// to maintain SQL compatibility. Some SQL engines treat negative OFFSET as 0,
		// others return empty results. This behavior is inconsistent with LIMIT validation
		// which returns errors. Consider making this behavior consistent in future versions.
		return CreateEmptyDataFrame(df), nil //nolint:nilerr // Intentional: invalid offset returns empty result
	}

	// Handle OFFSET-only queries (limit = -1 indicates no LIMIT)
	if limit == -1 {
		if offsetInt >= totalRows {
			return CreateEmptyDataFrame(df), nil
		}
		return df.Slice(offsetInt, totalRows), nil
	}

	// Validate LIMIT value
	limitInt, err := SafeInt64ToInt(limit)
	if err != nil {
		return nil, fmt.Errorf("invalid LIMIT value: %w", err)
	}

	// Validate offset bounds
	if offsetInt >= totalRows {
		return CreateEmptyDataFrame(df), nil
	}

	// Handle LIMIT 0
	if limitInt == 0 {
		return CreateEmptyDataFrame(df), nil
	}

	// Calculate end index
	startIdx := offsetInt
	endIdx := startIdx + limitInt
	if endIdx > totalRows {
		endIdx = totalRows
	}

	// Use DataFrame.Slice() to get the desired subset
	return df.Slice(startIdx, endIdx), nil
}
