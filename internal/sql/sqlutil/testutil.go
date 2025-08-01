package sqlutil

import (
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/series"
)

// TestDataFrameConfig configures test DataFrame creation.
type TestDataFrameConfig struct {
	IncludeActive bool
	RowCount      int
	CustomData    map[string]interface{}
}

// DefaultTestDataFrameConfig returns default test DataFrame configuration.
func DefaultTestDataFrameConfig() *TestDataFrameConfig {
	const defaultTestRowCount = 4
	return &TestDataFrameConfig{
		IncludeActive: true,
		RowCount:      defaultTestRowCount,
		CustomData:    nil,
	}
}

// CreateTestDataFrame creates a standard test DataFrame with employee data.
// This consolidates the createTestDataFrame pattern found across multiple SQL test files.
func CreateTestDataFrame(allocator memory.Allocator, config *TestDataFrameConfig) *dataframe.DataFrame {
	if config == nil {
		config = DefaultTestDataFrameConfig()
	}

	// Use custom data if provided
	if config.CustomData != nil {
		return CreateTestDataFrameFromData(allocator, config.CustomData)
	}

	// Generate standard employee test data
	names := generateTestNames(config.RowCount)
	ages := generateTestAges(config.RowCount)
	departments := generateTestDepartments(config.RowCount)
	salaries := generateTestSalaries(config.RowCount)

	seriesList := []dataframe.ISeries{
		series.New("name", names, allocator),
		series.New("age", ages, allocator),
		series.New("department", departments, allocator),
		series.New("salary", salaries, allocator),
	}

	if config.IncludeActive {
		active := generateTestActiveFlags(config.RowCount)
		seriesList = append(seriesList, series.New("active", active, allocator))
	}

	return dataframe.New(seriesList...)
}

// CreateTestDataFrameFromData creates a DataFrame from custom test data.
func CreateTestDataFrameFromData(allocator memory.Allocator, data map[string]interface{}) *dataframe.DataFrame {
	var seriesList []dataframe.ISeries

	for colName, colData := range data {
		switch values := colData.(type) {
		case []string:
			seriesList = append(seriesList, series.New(colName, values, allocator))
		case []int64:
			seriesList = append(seriesList, series.New(colName, values, allocator))
		case []float64:
			seriesList = append(seriesList, series.New(colName, values, allocator))
		case []bool:
			seriesList = append(seriesList, series.New(colName, values, allocator))
		}
	}

	return dataframe.New(seriesList...)
}

// Helper functions for generating test data

func generateTestNames(count int) []string {
	baseNames := []string{"Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry"}
	names := make([]string, count)
	for i := range count {
		names[i] = baseNames[i%len(baseNames)]
	}
	return names
}

func generateTestAges(count int) []int64 {
	baseAges := []int64{25, 30, 35, 28, 32, 45, 29, 38}
	ages := make([]int64, count)
	for i := range count {
		ages[i] = baseAges[i%len(baseAges)]
	}
	return ages
}

func generateTestDepartments(count int) []string {
	baseDepts := []string{"Engineering", "Sales", "Engineering", "Marketing", "HR", "Finance", "Engineering", "Sales"}
	departments := make([]string, count)
	for i := range count {
		departments[i] = baseDepts[i%len(baseDepts)]
	}
	return departments
}

func generateTestSalaries(count int) []int64 {
	baseSalaries := []int64{100000, 80000, 120000, 75000, 90000, 110000, 95000, 85000}
	salaries := make([]int64, count)
	for i := range count {
		salaries[i] = baseSalaries[i%len(baseSalaries)]
	}
	return salaries
}

func generateTestActiveFlags(count int) []bool {
	baseFlags := []bool{true, true, false, true, true, false, true, false}
	flags := make([]bool, count)
	for i := range count {
		flags[i] = baseFlags[i%len(baseFlags)]
	}
	return flags
}
