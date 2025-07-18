package config

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_DefaultValues(t *testing.T) {
	config := NewConfig()

	// Test default values match expected constants
	assert.Equal(t, 1000, config.ParallelThreshold)
	assert.Equal(t, 0, config.WorkerPoolSize) // 0 means auto-detect
	assert.Equal(t, 0, config.ChunkSize)      // 0 means auto-calculate
	assert.Equal(t, 16, config.MaxParallelism)
	assert.Equal(t, int64(0), config.MemoryThreshold)
	assert.Equal(t, 0.8, config.GCPressureThreshold)
	assert.Equal(t, 10, config.AllocatorPoolSize)
	assert.True(t, config.FilterFusion)
	assert.True(t, config.PredicatePushdown)
	assert.True(t, config.JoinOptimization)
	assert.False(t, config.EnableProfiling)
	assert.False(t, config.VerboseLogging)
	assert.False(t, config.MetricsCollection)
}

func TestConfig_Validation(t *testing.T) {
	tests := []struct {
		name          string
		config        Config
		expectedError string
	}{
		{
			name: "valid config",
			config: Config{
				ParallelThreshold:   500,
				WorkerPoolSize:      4,
				ChunkSize:           100,
				MaxParallelism:      8,
				GCPressureThreshold: 0.7,
				AllocatorPoolSize:   5,
			},
			expectedError: "",
		},
		{
			name: "negative parallel threshold",
			config: Config{
				ParallelThreshold:   -1,
				MaxParallelism:      8,
				GCPressureThreshold: 0.7,
				AllocatorPoolSize:   5,
			},
			expectedError: "ParallelThreshold must be positive, got -1",
		},
		{
			name: "negative worker pool size",
			config: Config{
				ParallelThreshold:   1000,
				WorkerPoolSize:      -1,
				MaxParallelism:      8,
				GCPressureThreshold: 0.7,
				AllocatorPoolSize:   5,
			},
			expectedError: "WorkerPoolSize must be non-negative, got -1",
		},
		{
			name: "negative chunk size",
			config: Config{
				ParallelThreshold:   1000,
				ChunkSize:           -1,
				MaxParallelism:      8,
				GCPressureThreshold: 0.7,
				AllocatorPoolSize:   5,
			},
			expectedError: "ChunkSize must be non-negative, got -1",
		},
		{
			name: "invalid GC pressure threshold",
			config: Config{
				ParallelThreshold:   1000,
				GCPressureThreshold: 1.5,
				MaxParallelism:      8,
				AllocatorPoolSize:   5,
			},
			expectedError: "GCPressureThreshold must be between 0 and 1, got 1.500000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectedError == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tt.expectedError)
			}
		})
	}
}

func TestConfig_LoadFromJSON(t *testing.T) {
	jsonData := `{
		"parallel_threshold": 2000,
		"worker_pool_size": 8,
		"chunk_size": 1000,
		"memory_threshold": 1073741824,
		"filter_fusion": false,
		"enable_profiling": true
	}`

	config, err := LoadFromJSON([]byte(jsonData))
	require.NoError(t, err)

	assert.Equal(t, 2000, config.ParallelThreshold)
	assert.Equal(t, 8, config.WorkerPoolSize)
	assert.Equal(t, 1000, config.ChunkSize)
	assert.Equal(t, int64(1073741824), config.MemoryThreshold)
	assert.False(t, config.FilterFusion)
	assert.True(t, config.EnableProfiling)
}

func TestConfig_LoadFromFile(t *testing.T) {
	// Create temporary test file
	tmpFile, err := os.CreateTemp("", "config_test_*.json")
	require.NoError(t, err)
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	jsonData := `{
		"parallel_threshold": 1500,
		"worker_pool_size": 4,
		"verbose_logging": true
	}`

	_, err = tmpFile.WriteString(jsonData)
	require.NoError(t, err)
	_ = tmpFile.Close()

	config, err := LoadFromFile(tmpFile.Name())
	require.NoError(t, err)

	assert.Equal(t, 1500, config.ParallelThreshold)
	assert.Equal(t, 4, config.WorkerPoolSize)
	assert.True(t, config.VerboseLogging)
}

func TestConfig_LoadFromEnv(t *testing.T) {
	// Save original environment
	originalThreshold := os.Getenv("GORILLA_PARALLEL_THRESHOLD")
	originalWorkerPool := os.Getenv("GORILLA_WORKER_POOL_SIZE")
	originalProfiling := os.Getenv("GORILLA_ENABLE_PROFILING")

	// Set test environment variables
	_ = os.Setenv("GORILLA_PARALLEL_THRESHOLD", "3000")
	_ = os.Setenv("GORILLA_WORKER_POOL_SIZE", "12")
	_ = os.Setenv("GORILLA_ENABLE_PROFILING", "true")

	// Restore environment after test
	defer func() {
		_ = os.Setenv("GORILLA_PARALLEL_THRESHOLD", originalThreshold)
		_ = os.Setenv("GORILLA_WORKER_POOL_SIZE", originalWorkerPool)
		_ = os.Setenv("GORILLA_ENABLE_PROFILING", originalProfiling)
	}()

	config := LoadFromEnv()

	assert.Equal(t, 3000, config.ParallelThreshold)
	assert.Equal(t, 12, config.WorkerPoolSize)
	assert.True(t, config.EnableProfiling)
}

func TestConfig_WithDefaults(t *testing.T) {
	config := Config{
		ParallelThreshold: 2000,
		// Other fields left as zero values
	}

	configWithDefaults := config.WithDefaults()

	assert.Equal(t, 2000, configWithDefaults.ParallelThreshold) // Should preserve set value
	assert.Equal(t, 0, configWithDefaults.WorkerPoolSize)       // Should get default (0 means auto-detect)
	assert.Equal(t, 16, configWithDefaults.MaxParallelism)      // Should get default
	// Note: Boolean fields don't get defaults in WithDefaults() method, they retain their zero values
	assert.False(t, configWithDefaults.FilterFusion) // Zero value, would need to be set explicitly
}

func TestGlobalConfig_SetAndGet(t *testing.T) {
	// Save original global config
	originalConfig := GetGlobalConfig()

	// Restore after test
	defer SetGlobalConfig(originalConfig)

	newConfig := Config{
		ParallelThreshold: 5000,
		WorkerPoolSize:    16,
		EnableProfiling:   true,
	}

	SetGlobalConfig(newConfig)
	retrievedConfig := GetGlobalConfig()

	assert.Equal(t, 5000, retrievedConfig.ParallelThreshold)
	assert.Equal(t, 16, retrievedConfig.WorkerPoolSize)
	assert.True(t, retrievedConfig.EnableProfiling)
}

func TestOperationConfig_Defaults(t *testing.T) {
	opConfig := OperationConfig{}

	assert.False(t, opConfig.ForceParallel)
	assert.False(t, opConfig.DisableParallel)
	assert.Equal(t, 0, opConfig.CustomChunkSize)
	assert.Equal(t, int64(0), opConfig.MaxMemoryUsage)
}

func TestConfig_ToJSON(t *testing.T) {
	config := Config{
		ParallelThreshold: 1500,
		WorkerPoolSize:    8,
		EnableProfiling:   true,
	}

	data, err := json.Marshal(config)
	require.NoError(t, err)

	// Verify we can unmarshal it back
	var unmarshaledConfig Config
	err = json.Unmarshal(data, &unmarshaledConfig)
	require.NoError(t, err)

	assert.Equal(t, config.ParallelThreshold, unmarshaledConfig.ParallelThreshold)
	assert.Equal(t, config.WorkerPoolSize, unmarshaledConfig.WorkerPoolSize)
	assert.Equal(t, config.EnableProfiling, unmarshaledConfig.EnableProfiling)
}

func TestConfig_PerformanceDefaults(t *testing.T) {
	config := NewConfig()

	// Test that performance-related defaults are sensible
	assert.Greater(t, config.ParallelThreshold, 0)
	assert.GreaterOrEqual(t, config.WorkerPoolSize, 0)
	assert.GreaterOrEqual(t, config.ChunkSize, 0)
	assert.Greater(t, config.MaxParallelism, 0)
	assert.GreaterOrEqual(t, config.MemoryThreshold, int64(0))
	assert.GreaterOrEqual(t, config.GCPressureThreshold, 0.0)
	assert.LessOrEqual(t, config.GCPressureThreshold, 1.0)
	assert.Greater(t, config.AllocatorPoolSize, 0)
}

func TestConfig_BooleanDefaults(t *testing.T) {
	config := NewConfig()

	// Test optimization defaults (should be enabled)
	assert.True(t, config.FilterFusion)
	assert.True(t, config.PredicatePushdown)
	assert.True(t, config.JoinOptimization)

	// Test debugging defaults (should be disabled)
	assert.False(t, config.EnableProfiling)
	assert.False(t, config.VerboseLogging)
	assert.False(t, config.MetricsCollection)
}

func TestConfig_UnsupportedFileFormat(t *testing.T) {
	// Create temporary file with unsupported extension
	tmpFile, err := os.CreateTemp("", "config_test_*.xyz")
	require.NoError(t, err)
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	_, _ = tmpFile.WriteString("some content")
	_ = tmpFile.Close()

	_, err = LoadFromFile(tmpFile.Name())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported config file format")
}

func TestConfig_InvalidJSON(t *testing.T) {
	invalidJSON := `{
		"parallel_threshold": "not_a_number",
		"worker_pool_size": 8
	}`

	_, err := LoadFromJSON([]byte(invalidJSON))
	assert.Error(t, err)
}

func TestConfig_LoadFromNonExistentFile(t *testing.T) {
	_, err := LoadFromFile("/nonexistent/config.json")
	assert.Error(t, err)
}

func TestConfig_MemoryCalculations(t *testing.T) {
	config := Config{
		MemoryThreshold: 1024 * 1024 * 100, // 100MB
	}

	// Test memory threshold is preserved
	assert.Equal(t, int64(1024*1024*100), config.MemoryThreshold)
}

func TestConfig_EnvironmentVariableParsing(t *testing.T) {
	// Test invalid environment variable values
	_ = os.Setenv("GORILLA_PARALLEL_THRESHOLD", "invalid_number")
	_ = os.Setenv("GORILLA_WORKER_POOL_SIZE", "not_a_number")
	_ = os.Setenv("GORILLA_ENABLE_PROFILING", "invalid_bool")

	defer func() {
		_ = os.Unsetenv("GORILLA_PARALLEL_THRESHOLD")
		_ = os.Unsetenv("GORILLA_WORKER_POOL_SIZE")
		_ = os.Unsetenv("GORILLA_ENABLE_PROFILING")
	}()

	// Should not panic and should use defaults for invalid values
	config := LoadFromEnv()
	assert.Equal(t, 1000, config.ParallelThreshold) // Default value
	assert.Equal(t, 0, config.WorkerPoolSize)       // Default value
	assert.False(t, config.EnableProfiling)         // Default value
}

func TestConfig_SystemInfo(t *testing.T) {
	info := GetSystemInfo()

	assert.Greater(t, info.CPUCount, 0)
	assert.Greater(t, info.MemorySize, int64(0))
	assert.NotEmpty(t, info.Architecture)
	assert.NotEmpty(t, info.OSType)
}

func TestConfig_ValidationRecommendations(t *testing.T) {
	validator := NewConfigValidator()

	config := Config{
		ParallelThreshold:   500,
		WorkerPoolSize:      0, // Should auto-detect
		ChunkSize:           0, // Should auto-calculate
		MaxParallelism:      8,
		GCPressureThreshold: 0.7,
		AllocatorPoolSize:   5,
	}

	validatedConfig, warnings, err := validator.Validate(config)
	require.NoError(t, err)

	assert.NotEmpty(t, warnings)                         // Should have warnings about auto-detection
	assert.Greater(t, validatedConfig.WorkerPoolSize, 0) // Should be auto-set
}

func TestConfig_LoadFromYAML(t *testing.T) {
	// Create temporary YAML file
	tmpFile, err := os.CreateTemp("", "config_test_*.yaml")
	require.NoError(t, err)
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	yamlData := `
parallel_threshold: 2000
worker_pool_size: 8
chunk_size: 1000
filter_fusion: true
enable_profiling: false
`

	_, err = tmpFile.WriteString(yamlData)
	require.NoError(t, err)
	_ = tmpFile.Close()

	config, err := LoadFromFile(tmpFile.Name())
	require.NoError(t, err)

	assert.Equal(t, 2000, config.ParallelThreshold)
	assert.Equal(t, 8, config.WorkerPoolSize)
	assert.Equal(t, 1000, config.ChunkSize)
	assert.True(t, config.FilterFusion)
	assert.False(t, config.EnableProfiling)
}

func TestConfig_PerformanceTuner(t *testing.T) {
	config := NewConfig()
	tuner := NewPerformanceTuner(&config)

	// Test optimization for small dataset
	optimized := tuner.OptimizeForDataset(50, 5)
	assert.Greater(t, optimized.ParallelThreshold, 50) // Should disable parallel for small dataset

	// Test optimization for large dataset
	optimized = tuner.OptimizeForDataset(1000000, 10)
	assert.Equal(t, 500, optimized.ParallelThreshold) // Should enable aggressive parallel with lower threshold
}

func TestConfig_ConfigWithTimeout(t *testing.T) {
	config := NewConfig()

	// Test that the config can be loaded within reasonable time
	start := time.Now()
	_ = config.WithDefaults()
	duration := time.Since(start)

	assert.Less(t, duration, 100*time.Millisecond) // Should be very fast
}
