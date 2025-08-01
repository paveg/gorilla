// Package main demonstrates how to use Gorilla's configurable processing parameters
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/config"
	"github.com/paveg/gorilla/internal/dataframe"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
)

// Constants for demo configuration.
const (
	// Dataset sizes for different examples.
	smallDatasetSize    = 1000
	mediumDatasetSize   = 2000
	largeDatasetSize    = 10000
	smallDatasetRows    = 100
	largeDatasetRows    = 1000000
	largeDatasetColumns = 5
	smallDatasetColumns = 50

	// Filter thresholds and values.
	valueFilterThreshold1 = 500.0
	valueFilterThreshold2 = 250.0
	valueFilterThreshold3 = 1500.0
	idFilterThreshold1    = 1000
	idFilterThreshold2    = 5000

	// Memory and performance settings.
	customChunkSize = 100
	memoryLimitMB   = 1 // 1MB
	bytesPerMB      = 1024 * 1024

	// Configuration values.
	workerPoolSize      = 4
	parallelThreshold   = 1000
	chunkSize           = 500
	maxParallelism      = 8
	gcPressureThreshold = 0.7
	allocatorPoolSize   = 10

	// Data generation multiplier.
	dataMultiplier = 1.5
)

func main() {
	fmt.Println("Gorilla DataFrame Configuration Examples")
	fmt.Println("=======================================")

	// Example 1: Using default configuration
	fmt.Println("\n1. Using Default Configuration:")
	demonstrateDefaultConfig()

	// Example 2: Loading configuration from file
	fmt.Println("\n2. Loading Configuration from File:")
	demonstrateFileConfig()

	// Example 3: Using environment variables
	fmt.Println("\n3. Using Environment Variables:")
	demonstrateEnvConfig()

	// Example 4: Per-operation configuration
	fmt.Println("\n4. Per-Operation Configuration:")
	demonstrateOperationConfig()

	// Example 5: Performance tuning
	fmt.Println("\n5. Performance Tuning:")
	demonstratePerformanceTuning()

	// Example 6: Configuration validation
	fmt.Println("\n6. Configuration Validation:")
	demonstrateConfigValidation()
}

func demonstrateDefaultConfig() {
	ctx := context.Background()
	logger := slog.Default()
	// Get the default global configuration
	defaultConfig := config.GetGlobalConfig()
	fmt.Printf("Default parallel threshold: %d\n", defaultConfig.ParallelThreshold)
	fmt.Printf("Default worker pool size: %d (0 = auto-detect)\n", defaultConfig.WorkerPoolSize)
	fmt.Printf("Default chunk size: %d (0 = auto-calculate)\n", defaultConfig.ChunkSize)
	fmt.Printf("Filter fusion enabled: %t\n", defaultConfig.FilterFusion)

	// Create a simple DataFrame operation
	mem := memory.NewGoAllocator()
	df := createSampleDataFrame(mem, smallDatasetSize)
	defer df.Release()

	// Perform operation with default configuration
	result, err := df.Lazy().
		Filter(expr.Col("value").Gt(expr.Lit(valueFilterThreshold1))).
		Collect()
	if err != nil {
		logger.ErrorContext(ctx, "Error", "err", err)
		return
	}
	defer result.Release()

	fmt.Printf("Filtered %d rows to %d rows using default config\n", df.Len(), result.Len())
}

func demonstrateFileConfig() {
	ctx := context.Background()
	logger := slog.Default()
	// Try to load configuration from YAML file
	configPath := "examples/config/gorilla.yaml"
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		fmt.Printf("Config file %s not found, skipping file config demo\n", configPath)
		return
	}

	fileConfig, err := config.LoadFromFile(configPath)
	if err != nil {
		fmt.Printf("Error loading config file: %v\n", err)
		return
	}

	fmt.Printf("Loaded config - Parallel threshold: %d\n", fileConfig.ParallelThreshold)
	fmt.Printf("Loaded config - Worker pool size: %d\n", fileConfig.WorkerPoolSize)
	fmt.Printf("Loaded config - Metrics collection: %t\n", fileConfig.MetricsCollection)

	// Set the loaded configuration as global
	originalConfig := config.GetGlobalConfig()
	config.SetGlobalConfig(fileConfig)
	defer config.SetGlobalConfig(originalConfig) // Restore original

	// Create DataFrame operation with file configuration
	mem := memory.NewGoAllocator()
	df := createSampleDataFrame(mem, mediumDatasetSize)
	defer df.Release()

	result, err := df.Lazy().
		Filter(expr.Col("id").Lt(expr.Lit(int64(idFilterThreshold1)))).
		Collect()
	if err != nil {
		logger.ErrorContext(ctx, "Error", "err", err)
		return
	}
	defer result.Release()

	fmt.Printf("Processed %d rows using file configuration\n", result.Len())
}

func demonstrateEnvConfig() {
	ctx := context.Background()
	logger := slog.Default()
	// Set some environment variables
	_ = os.Setenv("GORILLA_PARALLEL_THRESHOLD", "500")
	_ = os.Setenv("GORILLA_WORKER_POOL_SIZE", "4")
	_ = os.Setenv("GORILLA_ENABLE_PROFILING", "true")

	defer func() {
		_ = os.Unsetenv("GORILLA_PARALLEL_THRESHOLD")
		_ = os.Unsetenv("GORILLA_WORKER_POOL_SIZE")
		_ = os.Unsetenv("GORILLA_ENABLE_PROFILING")
	}()

	// Load configuration from environment
	envConfig := config.LoadFromEnv()
	fmt.Printf("Env config - Parallel threshold: %d\n", envConfig.ParallelThreshold)
	fmt.Printf("Env config - Worker pool size: %d\n", envConfig.WorkerPoolSize)
	fmt.Printf("Env config - Profiling enabled: %t\n", envConfig.EnableProfiling)

	// Set as global configuration
	originalConfig := config.GetGlobalConfig()
	config.SetGlobalConfig(envConfig)
	defer config.SetGlobalConfig(originalConfig) // Restore original

	// Use the environment configuration
	mem := memory.NewGoAllocator()
	df := createSampleDataFrame(mem, smallDatasetSize)
	defer df.Release()

	result, err := df.Lazy().
		Filter(expr.Col("value").Gt(expr.Lit(valueFilterThreshold2))).
		Collect()
	if err != nil {
		logger.ErrorContext(ctx, "Error", "err", err)
		return
	}
	defer result.Release()

	fmt.Printf("Environment config processed %d rows\n", result.Len())
}

func demonstrateOperationConfig() {
	ctx := context.Background()
	logger := slog.Default()
	mem := memory.NewGoAllocator()
	df := createSampleDataFrame(mem, mediumDatasetSize)
	defer df.Release()

	// Create custom operation configuration
	opConfig := config.OperationConfig{
		ForceParallel:   true,                       // Force parallel execution
		CustomChunkSize: customChunkSize,            // Use small chunks
		MaxMemoryUsage:  memoryLimitMB * bytesPerMB, // 1MB limit
	}

	// Apply the configuration to the DataFrame
	configuredDF := df.WithConfig(opConfig)

	result, err := configuredDF.Lazy().
		Filter(expr.Col("id").Gt(expr.Lit(int64(idFilterThreshold1)))).
		Collect()
	if err != nil {
		logger.ErrorContext(ctx, "Error", "err", err)
		return
	}
	defer result.Release()

	fmt.Printf("Operation config: Forced parallel execution with chunk size %d\n", opConfig.CustomChunkSize)
	fmt.Printf("Processed %d rows with custom operation config\n", result.Len())

	// Demonstrate disabling parallel execution
	seqConfig := config.OperationConfig{
		DisableParallel: true,
	}

	seqDF := df.WithConfig(seqConfig)
	seqResult, err := seqDF.Lazy().
		Filter(expr.Col("value").Lt(expr.Lit(valueFilterThreshold3))).
		Collect()
	if err != nil {
		logger.ErrorContext(ctx, "Error", "err", err)
		return
	}
	defer seqResult.Release()

	fmt.Printf("Sequential processing (parallel disabled): %d rows\n", seqResult.Len())
}

func demonstratePerformanceTuning() {
	ctx := context.Background()
	logger := slog.Default()
	globalConfig := config.GetGlobalConfig()
	tuner := config.NewPerformanceTuner(&globalConfig)

	// Optimize for a small dataset with many columns
	smallDataConfig := tuner.OptimizeForDataset(smallDatasetRows, smallDatasetColumns)
	fmt.Printf("Small dataset config - Parallel threshold: %d\n", smallDataConfig.ParallelThreshold)
	fmt.Printf("Small dataset config - Chunk size: %d\n", smallDataConfig.ChunkSize)

	// Optimize for a large dataset with few columns
	largeDataConfig := tuner.OptimizeForDataset(largeDatasetRows, largeDatasetColumns)
	fmt.Printf("Large dataset config - Parallel threshold: %d\n", largeDataConfig.ParallelThreshold)
	fmt.Printf("Large dataset config - Chunk size: %d\n", largeDataConfig.ChunkSize)

	// Apply optimized configuration
	mem := memory.NewGoAllocator()
	df := createSampleDataFrame(mem, largeDatasetSize)
	defer df.Release()

	opConfig := config.OperationConfig{
		CustomChunkSize: largeDataConfig.ChunkSize,
	}

	optimizedDF := df.WithConfig(opConfig)
	result, err := optimizedDF.Lazy().
		Filter(expr.Col("id").Gt(expr.Lit(int64(idFilterThreshold2)))).
		Collect()
	if err != nil {
		logger.ErrorContext(ctx, "Error", "err", err)
		return
	}
	defer result.Release()

	fmt.Printf("Performance tuned processing: %d rows\n", result.Len())
}

func demonstrateConfigValidation() {
	validator := config.NewConfigValidator()

	// Test valid configuration
	validConfig := config.Config{
		ParallelThreshold:   parallelThreshold,
		WorkerPoolSize:      workerPoolSize,
		ChunkSize:           chunkSize,
		MaxParallelism:      maxParallelism,
		GCPressureThreshold: gcPressureThreshold,
		AllocatorPoolSize:   allocatorPoolSize,
	}

	validated, warnings, err := validator.Validate(validConfig)
	if err != nil {
		fmt.Printf("Validation error: %v\n", err)
	} else {
		fmt.Printf("Configuration validated successfully\n")
		if len(warnings) > 0 {
			fmt.Printf("Warnings: %v\n", warnings)
		}
		fmt.Printf("Validated worker pool size: %d\n", validated.WorkerPoolSize)
	}

	// Test invalid configuration
	invalidConfig := config.Config{
		ParallelThreshold:   -1,  // Invalid
		GCPressureThreshold: 1.5, //nolint:mnd // Invalid value for demo
	}

	_, _, err = validator.Validate(invalidConfig)
	if err != nil {
		fmt.Printf("Expected validation error: %v\n", err)
	}
}

func createSampleDataFrame(mem memory.Allocator, size int) *dataframe.DataFrame {
	ids := make([]int64, size)
	values := make([]float64, size)

	for i := range size {
		ids[i] = int64(i)
		values[i] = float64(i) * dataMultiplier
	}

	s1 := series.New("id", ids, mem)
	s2 := series.New("value", values, mem)

	return dataframe.New(s1, s2)
}
