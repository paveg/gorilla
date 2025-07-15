package dataframe

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/config"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataFrame_WithConfig(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test DataFrame
	s1 := series.New("id", []int64{1, 2, 3, 4, 5}, mem)
	s2 := series.New("value", []float64{1.1, 2.2, 3.3, 4.4, 5.5}, mem)
	df := New(s1, s2)
	defer df.Release()

	// Test with custom operation config
	opConfig := config.OperationConfig{
		ForceParallel:   true,
		CustomChunkSize: 100,
		MaxMemoryUsage:  1024 * 1024, // 1MB
	}

	configuredDF := df.WithConfig(opConfig)
	assert.NotNil(t, configuredDF)
	assert.Equal(t, opConfig.ForceParallel, configuredDF.operationConfig.ForceParallel)
	assert.Equal(t, opConfig.CustomChunkSize, configuredDF.operationConfig.CustomChunkSize)
	assert.Equal(t, opConfig.MaxMemoryUsage, configuredDF.operationConfig.MaxMemoryUsage)
}

func TestDataFrame_ConfiguredParallelExecution(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test DataFrame with enough data to trigger parallel execution
	ids := make([]int64, 2000)
	values := make([]float64, 2000)
	for i := range ids {
		ids[i] = int64(i)
		values[i] = float64(i) * 1.5
	}

	s1 := series.New("id", ids, mem)
	s2 := series.New("value", values, mem)
	df := New(s1, s2)
	defer df.Release()

	// Test with custom configuration that forces parallel execution
	opConfig := config.OperationConfig{
		ForceParallel:   true,
		CustomChunkSize: 500,
	}

	configuredDF := df.WithConfig(opConfig)
	result, err := configuredDF.Lazy().Filter(expr.Col("id").Gt(expr.Lit(int64(1000)))).Collect()
	require.NoError(t, err)
	defer result.Release()

	// Should have filtered out records with id <= 1000
	assert.Equal(t, 999, result.Len()) // ids 1001-1999 = 999 records
}

func TestDataFrame_ConfiguredChunkSize(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test DataFrame
	ids := make([]int64, 1000)
	values := make([]float64, 1000)
	for i := range ids {
		ids[i] = int64(i)
		values[i] = float64(i) * 2.0
	}

	s1 := series.New("id", ids, mem)
	s2 := series.New("value", values, mem)
	df := New(s1, s2)
	defer df.Release()

	// Test with custom chunk size
	opConfig := config.OperationConfig{
		CustomChunkSize: 50,
	}

	configuredDF := df.WithConfig(opConfig)
	result, err := configuredDF.Lazy().Filter(expr.Col("id").Lt(expr.Lit(int64(100)))).Collect()
	require.NoError(t, err)
	defer result.Release()

	// Should have filtered records with id < 100
	assert.Equal(t, 100, result.Len())
}

func TestDataFrame_GlobalConfigIntegration(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Save original global config
	originalConfig := config.GetGlobalConfig()
	defer config.SetGlobalConfig(originalConfig)

	// Set custom global config
	customConfig := config.Config{
		ParallelThreshold:   500,
		WorkerPoolSize:      4,
		ChunkSize:           200,
		MaxParallelism:      8,
		GCPressureThreshold: 0.7,
		AllocatorPoolSize:   5,
		FilterFusion:        true,
		EnableProfiling:     true,
	}
	config.SetGlobalConfig(customConfig)

	// Create test DataFrame
	ids := make([]int64, 1000)
	values := make([]float64, 1000)
	for i := range ids {
		ids[i] = int64(i)
		values[i] = float64(i) * 1.5
	}

	s1 := series.New("id", ids, mem)
	s2 := series.New("value", values, mem)
	df := New(s1, s2)
	defer df.Release()

	// Test that global config is used in operations
	result, err := df.Lazy().Filter(expr.Col("id").Gt(expr.Lit(int64(500)))).Collect()
	require.NoError(t, err)
	defer result.Release()

	// Should have filtered records with id > 500
	assert.Equal(t, 499, result.Len()) // ids 501-999 = 499 records
}

func TestDataFrame_MemoryThresholdConfiguration(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test DataFrame
	ids := make([]int64, 500)
	values := make([]float64, 500)
	for i := range ids {
		ids[i] = int64(i)
		values[i] = float64(i) * 2.0
	}

	s1 := series.New("id", ids, mem)
	s2 := series.New("value", values, mem)
	df := New(s1, s2)
	defer df.Release()

	// Test with memory threshold configuration
	opConfig := config.OperationConfig{
		MaxMemoryUsage: 1024 * 1024 * 10, // 10MB
	}

	configuredDF := df.WithConfig(opConfig)
	result, err := configuredDF.Lazy().Filter(expr.Col("id").Lt(expr.Lit(int64(250)))).Collect()
	require.NoError(t, err)
	defer result.Release()

	// Should have filtered records with id < 250
	assert.Equal(t, 250, result.Len())
}

func TestDataFrame_DisableParallelConfiguration(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create large test DataFrame that would normally trigger parallel execution
	ids := make([]int64, 2000)
	values := make([]float64, 2000)
	for i := range ids {
		ids[i] = int64(i)
		values[i] = float64(i) * 1.5
	}

	s1 := series.New("id", ids, mem)
	s2 := series.New("value", values, mem)
	df := New(s1, s2)
	defer df.Release()

	// Test with disabled parallel execution
	opConfig := config.OperationConfig{
		DisableParallel: true,
	}

	configuredDF := df.WithConfig(opConfig)
	result, err := configuredDF.Lazy().Filter(expr.Col("id").Gt(expr.Lit(int64(1500)))).Collect()
	require.NoError(t, err)
	defer result.Release()

	// Should have filtered records with id > 1500
	assert.Equal(t, 499, result.Len()) // ids 1501-1999 = 499 records
}

func TestDataFrame_ConfigValidation(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test DataFrame
	s1 := series.New("id", []int64{1, 2, 3}, mem)
	s2 := series.New("value", []float64{1.1, 2.2, 3.3}, mem)
	df := New(s1, s2)
	defer df.Release()

	// Test with invalid operation config (negative values should be handled gracefully)
	opConfig := config.OperationConfig{
		CustomChunkSize: -1, // Invalid but should be handled
		MaxMemoryUsage:  -1, // Invalid but should be handled
	}

	configuredDF := df.WithConfig(opConfig)
	result, err := configuredDF.Lazy().Filter(expr.Col("id").Gt(expr.Lit(int64(0)))).Collect()
	require.NoError(t, err)
	defer result.Release()

	// Should still work with all records
	assert.Equal(t, 3, result.Len())
}

func TestDataFrame_ConfigurationPrecedence(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Save original global config
	originalConfig := config.GetGlobalConfig()
	defer config.SetGlobalConfig(originalConfig)

	// Set global config with one chunk size
	globalConfig := config.Config{
		ParallelThreshold:   1000,
		ChunkSize:           100,
		MaxParallelism:      8,
		GCPressureThreshold: 0.7,
		AllocatorPoolSize:   5,
	}
	config.SetGlobalConfig(globalConfig)

	// Create test DataFrame
	ids := make([]int64, 1000)
	values := make([]float64, 1000)
	for i := range ids {
		ids[i] = int64(i)
		values[i] = float64(i) * 2.0
	}

	s1 := series.New("id", ids, mem)
	s2 := series.New("value", values, mem)
	df := New(s1, s2)
	defer df.Release()

	// Test that operation config overrides global config
	opConfig := config.OperationConfig{
		CustomChunkSize: 50, // Should override global config
	}

	configuredDF := df.WithConfig(opConfig)
	result, err := configuredDF.Lazy().Filter(expr.Col("id").Lt(expr.Lit(int64(500)))).Collect()
	require.NoError(t, err)
	defer result.Release()

	// Should have filtered records with id < 500
	assert.Equal(t, 500, result.Len())
	// The custom chunk size should have been used (this is hard to verify directly,
	// but the operation should complete successfully)
}

func TestDataFrame_PerformanceTuning(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test DataFrame
	ids := make([]int64, 10000)
	values := make([]float64, 10000)
	for i := range ids {
		ids[i] = int64(i)
		values[i] = float64(i) * 1.5
	}

	s1 := series.New("id", ids, mem)
	s2 := series.New("value", values, mem)
	df := New(s1, s2)
	defer df.Release()

	// Test with performance tuning enabled
	globalConfig := config.GetGlobalConfig()
	tuner := config.NewPerformanceTuner(&globalConfig)
	optimizedConfig := tuner.OptimizeForDataset(10000, 2)

	// Apply the optimized configuration
	opConfig := config.OperationConfig{
		CustomChunkSize: optimizedConfig.ChunkSize,
	}

	configuredDF := df.WithConfig(opConfig)
	result, err := configuredDF.Lazy().Filter(expr.Col("id").Gt(expr.Lit(int64(5000)))).Collect()
	require.NoError(t, err)
	defer result.Release()

	// Should have filtered records with id > 5000
	assert.Equal(t, 4999, result.Len()) // ids 5001-9999 = 4999 records
}
