package gorilla

import (
	"runtime"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/config"
	"github.com/paveg/gorilla/internal/parallel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDataFrameWithConfig tests the public WithConfig method.
func TestDataFrameWithConfig(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data
	names := NewSeries("name", []string{"Alice", "Bob", "Charlie"}, mem)
	ages := NewSeries("age", []int64{25, 30, 35}, mem)
	defer names.Release()
	defer ages.Release()

	df := NewDataFrame(names, ages)
	defer df.Release()

	// Test WithConfig method exists and works
	opConfig := config.OperationConfig{
		ForceParallel:   true,
		CustomChunkSize: 1000,
		MaxMemoryUsage:  1024 * 1024 * 50, // 50MB
	}

	configuredDF := df.WithConfig(opConfig)
	defer configuredDF.Release()

	// Verify the configured DataFrame has the same data
	assert.Equal(t, df.Len(), configuredDF.Len())
	assert.Equal(t, df.Width(), configuredDF.Width())
	assert.Equal(t, df.Columns(), configuredDF.Columns())

	// Verify lazy operations work with configuration
	result, err := configuredDF.Lazy().
		Filter(Col("age").Gt(Lit(int64(25)))).
		Collect()
	require.NoError(t, err)
	defer result.Release()

	assert.Equal(t, 2, result.Len()) // Bob and Charlie
}

// TestConfigurationInheritance tests that configuration is inherited through operations.
func TestConfigurationInheritance(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data
	names := NewSeries("name", []string{"Alice", "Bob", "Charlie", "Diana"}, mem)
	ages := NewSeries("age", []int64{25, 30, 35, 40}, mem)
	salaries := NewSeries("salary", []int64{50000, 60000, 70000, 80000}, mem)
	defer names.Release()
	defer ages.Release()
	defer salaries.Release()

	df := NewDataFrame(names, ages, salaries)
	defer df.Release()

	// Configure with ForceParallel to test inheritance
	opConfig := config.OperationConfig{
		ForceParallel:   true,
		CustomChunkSize: 2, // Small chunk to force parallel execution
	}

	configuredDF := df.WithConfig(opConfig)
	defer configuredDF.Release()

	// Perform multiple chained operations
	result, err := configuredDF.Lazy().
		Filter(Col("age").Gt(Lit(int64(25)))).
		WithColumn("bonus", Col("salary").Mul(Lit(float64(0.1)))).
		Select("name", "salary", "bonus").
		Collect()
	require.NoError(t, err)
	defer result.Release()

	// Verify result correctness
	assert.Equal(t, 3, result.Len())   // Bob, Charlie, Diana
	assert.Equal(t, 3, result.Width()) // name, salary, bonus
	assert.True(t, result.HasColumn("bonus"))

	// Verify bonus calculation - the filter age > 25 should give us Bob, Charlie, Diana
	bonusCol, exists := result.Column("bonus")
	require.True(t, exists)

	// Get the underlying Arrow array and cast to Float64Array
	arr := bonusCol.Array()
	defer arr.Release()
	float64Arr := arr.(*array.Float64)

	// After filtering age > 25: Bob (30, 60000), Charlie (35, 70000), Diana (40, 80000)
	// The order is not guaranteed, so collect all values and verify they're all present
	require.Equal(t, 3, float64Arr.Len())

	actualValues := make([]float64, 3)
	for i := range actualValues {
		actualValues[i] = float64Arr.Value(i)
	}

	// Expected bonus values: Bob (6000), Charlie (7000), Diana (8000)
	expectedValues := []float64{6000, 7000, 8000}
	assert.ElementsMatch(t, expectedValues, actualValues)
}

// TestConfigLoadFromFile tests loading configuration from external files.
func TestConfigLoadFromFile(t *testing.T) {
	// Test that configuration can be loaded from YAML/JSON files
	// This verifies the integration mentioned in the README

	// Load from YAML example
	yamlConfig, err := config.LoadFromFile("examples/config/gorilla.yaml")
	require.NoError(t, err)

	// Verify configuration values
	assert.Equal(t, 2000, yamlConfig.ParallelThreshold)
	assert.Equal(t, 8, yamlConfig.WorkerPoolSize)
	assert.Equal(t, int64(1073741824), yamlConfig.MemoryThreshold) // 1GB
	assert.Equal(t, 0.75, yamlConfig.GCPressureThreshold)

	// Load from JSON example
	jsonConfig, err := config.LoadFromFile("examples/config/gorilla.json")
	require.NoError(t, err)

	// Verify JSON configuration
	assert.Equal(t, 1500, jsonConfig.ParallelThreshold)
	assert.Equal(t, 0, jsonConfig.WorkerPoolSize)
	assert.True(t, jsonConfig.FilterFusion)
	assert.True(t, jsonConfig.PredicatePushdown)
}

// TestReadmeExample tests the exact example from README.md.
func TestReadmeExample(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data
	names := NewSeries("name", []string{"Alice", "Bob", "Charlie"}, mem)
	ages := NewSeries("age", []int64{25, 30, 35}, mem)
	defer names.Release()
	defer ages.Release()

	// Create operation config from main config
	opConfig := config.OperationConfig{
		CustomChunkSize: 1000,
		ForceParallel:   false,
	}

	// This should match the README.md example
	df := NewDataFrame(names, ages).WithConfig(opConfig)
	defer df.Release()

	// Verify it works
	result, err := df.Lazy().
		Filter(Col("age").Gt(Lit(int64(25)))).
		Collect()
	require.NoError(t, err)
	defer result.Release()

	assert.Equal(t, 2, result.Len())
}

// TestDefaultOperationConfig tests behavior with default/empty configuration.
func TestDefaultOperationConfig(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data
	names := NewSeries("name", []string{"Alice", "Bob"}, mem)
	defer names.Release()

	df := NewDataFrame(names)
	defer df.Release()

	// Test with empty configuration
	emptyConfig := config.OperationConfig{}
	configuredDF := df.WithConfig(emptyConfig)
	defer configuredDF.Release()

	// Should still work with default behavior
	result, err := configuredDF.Lazy().
		Select("name").
		Collect()
	require.NoError(t, err)
	defer result.Release()

	assert.Equal(t, 2, result.Len())
	assert.Equal(t, 1, result.Width())
}

// TestMemoryConfigurationIntegration tests that memory configuration options are used.
func TestMemoryConfigurationIntegration(t *testing.T) {
	// Test that MemoryUsageMonitor uses configuration values
	testConfig := config.Config{
		MemoryThreshold:     500 * 1024 * 1024, // 500MB
		GCPressureThreshold: 0.6,               // Lower than default 0.8
	}

	monitor := NewMemoryUsageMonitorFromConfig(testConfig)
	defer monitor.StopMonitoring()

	// Verify the configuration was applied (we can't directly access private fields,
	// but we can verify behavior through the interface)
	assert.NotNil(t, monitor)

	// Test that parallel MemoryMonitor uses configuration values
	memMonitor := parallel.NewMemoryMonitorFromConfig(testConfig)
	assert.NotNil(t, memMonitor)

	// Test adaptive parallelism with custom threshold
	// Simulate 70% memory usage (above 60% threshold)
	memMonitor.RecordAllocation(350 * 1024 * 1024) // 350MB of 500MB = 70%
	parallelism := memMonitor.AdjustParallelism()

	// With 70% usage and 60% threshold, should reduce parallelism
	maxParallel := runtime.NumCPU()
	assert.Less(t, parallelism, maxParallel, "Parallelism should be reduced at 70%% usage with 60%% threshold")
}

// TestMemoryThresholdEdgeCases tests edge cases for memory pressure thresholds.
func TestMemoryThresholdEdgeCases(t *testing.T) {
	// Test with very high GC pressure threshold (0.95)
	highConfig := config.Config{
		MemoryThreshold:     100 * 1024 * 1024, // 100MB
		GCPressureThreshold: 0.95,
		MaxParallelism:      8,
	}

	highMonitor := parallel.NewMemoryMonitorFromConfig(highConfig)
	assert.NotNil(t, highMonitor)

	// Test with very low GC pressure threshold (0.1)
	lowConfig := config.Config{
		MemoryThreshold:     100 * 1024 * 1024, // 100MB
		GCPressureThreshold: 0.1,
		MaxParallelism:      8,
	}

	lowMonitor := parallel.NewMemoryMonitorFromConfig(lowConfig)
	assert.NotNil(t, lowMonitor)

	// Both should work without panic or errors
	// Edge cases are handled by math.Min and math.Max
}
