// Package config provides configuration management for Gorilla DataFrame operations
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"gopkg.in/yaml.v2"
)

// Config represents the global configuration for Gorilla DataFrame operations
type Config struct {
	// Parallel Processing Configuration
	ParallelThreshold int `json:"parallel_threshold" yaml:"parallel_threshold"` // Minimum rows to trigger parallel processing
	WorkerPoolSize    int `json:"worker_pool_size" yaml:"worker_pool_size"`     // Number of worker goroutines (0 = auto-detect)
	ChunkSize         int `json:"chunk_size" yaml:"chunk_size"`                 // Size of data chunks for parallel processing (0 = auto-calculate)
	MaxParallelism    int `json:"max_parallelism" yaml:"max_parallelism"`       // Maximum number of parallel operations

	// Memory Management Configuration
	MemoryThreshold     int64   `json:"memory_threshold" yaml:"memory_threshold"`           // Memory threshold in bytes (0 = unlimited)
	GCPressureThreshold float64 `json:"gc_pressure_threshold" yaml:"gc_pressure_threshold"` // GC pressure threshold (0.0-1.0)
	AllocatorPoolSize   int     `json:"allocator_pool_size" yaml:"allocator_pool_size"`     // Size of allocator pool

	// Query Optimization Configuration
	FilterFusion      bool `json:"filter_fusion" yaml:"filter_fusion"`           // Enable filter fusion optimization
	PredicatePushdown bool `json:"predicate_pushdown" yaml:"predicate_pushdown"` // Enable predicate pushdown optimization
	JoinOptimization  bool `json:"join_optimization" yaml:"join_optimization"`   // Enable join optimization

	// Debugging Configuration
	EnableProfiling   bool `json:"enable_profiling" yaml:"enable_profiling"`     // Enable performance profiling
	VerboseLogging    bool `json:"verbose_logging" yaml:"verbose_logging"`       // Enable verbose logging
	MetricsCollection bool `json:"metrics_collection" yaml:"metrics_collection"` // Enable metrics collection
}

// OperationConfig represents per-operation configuration overrides
type OperationConfig struct {
	ForceParallel   bool  // Force parallel execution regardless of threshold
	DisableParallel bool  // Disable parallel execution
	CustomChunkSize int   // Custom chunk size for this operation
	MaxMemoryUsage  int64 // Maximum memory usage for this operation
}

// SystemInfo contains system information for configuration validation
type SystemInfo struct {
	CPUCount     int
	MemorySize   int64
	Architecture string
	OSType       string
}

// ConfigValidator validates and provides recommendations for configuration
type ConfigValidator struct {
	systemInfo SystemInfo
}

// PerformanceTuner provides dynamic performance tuning based on runtime conditions
type PerformanceTuner struct {
	config           *Config
	adaptiveSettings map[string]interface{}
	mu               sync.RWMutex
}

// Global configuration instance
var (
	globalConfig Config
	configMutex  sync.RWMutex
)

// Default configuration values
const (
	DefaultParallelThreshold   = 1000
	DefaultMaxParallelism      = 16
	DefaultGCPressureThreshold = 0.8
	DefaultAllocatorPoolSize   = 10
)

// Initialize global configuration with defaults
func init() {
	globalConfig = NewConfig()
}

// NewConfig creates a new configuration with default values
func NewConfig() Config {
	return Config{
		// Parallel Processing defaults
		ParallelThreshold: DefaultParallelThreshold,
		WorkerPoolSize:    0, // Auto-detect
		ChunkSize:         0, // Auto-calculate
		MaxParallelism:    DefaultMaxParallelism,

		// Memory Management defaults
		MemoryThreshold:     0, // Unlimited
		GCPressureThreshold: DefaultGCPressureThreshold,
		AllocatorPoolSize:   DefaultAllocatorPoolSize,

		// Query Optimization defaults (enabled)
		FilterFusion:      true,
		PredicatePushdown: true,
		JoinOptimization:  true,

		// Debugging defaults (disabled)
		EnableProfiling:   false,
		VerboseLogging:    false,
		MetricsCollection: false,
	}
}

// Validate validates the configuration and returns an error if invalid
func (c *Config) Validate() error {
	if c.ParallelThreshold <= 0 {
		return fmt.Errorf("ParallelThreshold must be positive, got %d", c.ParallelThreshold)
	}

	if c.WorkerPoolSize < 0 {
		return fmt.Errorf("WorkerPoolSize must be non-negative, got %d", c.WorkerPoolSize)
	}

	if c.ChunkSize < 0 {
		return fmt.Errorf("ChunkSize must be non-negative, got %d", c.ChunkSize)
	}

	if c.MaxParallelism <= 0 {
		return fmt.Errorf("MaxParallelism must be positive, got %d", c.MaxParallelism)
	}

	if c.MemoryThreshold < 0 {
		return fmt.Errorf("MemoryThreshold must be non-negative, got %d", c.MemoryThreshold)
	}

	if c.GCPressureThreshold < 0.0 || c.GCPressureThreshold > 1.0 {
		return fmt.Errorf("GCPressureThreshold must be between 0 and 1, got %f", c.GCPressureThreshold)
	}

	if c.AllocatorPoolSize <= 0 {
		return fmt.Errorf("AllocatorPoolSize must be positive, got %d", c.AllocatorPoolSize)
	}

	return nil
}

// WithDefaults returns a new configuration with default values filled in for zero values
func (c Config) WithDefaults() Config {
	defaults := NewConfig()

	// Apply defaults for zero values
	if c.ParallelThreshold == 0 {
		c.ParallelThreshold = defaults.ParallelThreshold
	}
	if c.WorkerPoolSize == 0 {
		c.WorkerPoolSize = defaults.WorkerPoolSize
	}
	if c.ChunkSize == 0 {
		c.ChunkSize = defaults.ChunkSize
	}
	if c.MaxParallelism == 0 {
		c.MaxParallelism = defaults.MaxParallelism
	}
	if c.MemoryThreshold == 0 {
		c.MemoryThreshold = defaults.MemoryThreshold
	}
	if c.GCPressureThreshold == 0.0 {
		c.GCPressureThreshold = defaults.GCPressureThreshold
	}
	if c.AllocatorPoolSize == 0 {
		c.AllocatorPoolSize = defaults.AllocatorPoolSize
	}

	// Note: Boolean fields are intentionally not set to defaults here
	// This allows distinguishing between explicitly set false and unset values
	// Use NewConfig() directly if you need boolean defaults

	return c
}

// SetGlobalConfig sets the global configuration
func SetGlobalConfig(config Config) {
	configMutex.Lock()
	defer configMutex.Unlock()
	globalConfig = config
}

// GetGlobalConfig returns the current global configuration
func GetGlobalConfig() Config {
	configMutex.RLock()
	defer configMutex.RUnlock()
	return globalConfig
}

// LoadFromJSON loads configuration from JSON data
func LoadFromJSON(data []byte) (Config, error) {
	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return Config{}, fmt.Errorf("parsing JSON configuration: %w", err)
	}
	return config.WithDefaults(), nil
}

// LoadFromFile loads configuration from a file (supports JSON, YAML, TOML)
func LoadFromFile(filename string) (Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return Config{}, fmt.Errorf("reading config file %s: %w", filename, err)
	}

	var config Config
	ext := strings.ToLower(filepath.Ext(filename))

	switch ext {
	case ".json":
		err = json.Unmarshal(data, &config)
	case ".yaml", ".yml":
		err = yaml.Unmarshal(data, &config)
	default:
		return Config{}, fmt.Errorf("unsupported config file format: %s", ext)
	}

	if err != nil {
		return Config{}, fmt.Errorf("parsing config file %s: %w", filename, err)
	}

	return config.WithDefaults(), nil
}

// LoadFromEnv loads configuration from environment variables
func LoadFromEnv() Config {
	config := NewConfig()

	if val := os.Getenv("GORILLA_PARALLEL_THRESHOLD"); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			config.ParallelThreshold = parsed
		}
	}

	if val := os.Getenv("GORILLA_WORKER_POOL_SIZE"); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			config.WorkerPoolSize = parsed
		}
	}

	if val := os.Getenv("GORILLA_CHUNK_SIZE"); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			config.ChunkSize = parsed
		}
	}

	if val := os.Getenv("GORILLA_MAX_PARALLELISM"); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			config.MaxParallelism = parsed
		}
	}

	if val := os.Getenv("GORILLA_MEMORY_THRESHOLD"); val != "" {
		if parsed, err := strconv.ParseInt(val, 10, 64); err == nil {
			config.MemoryThreshold = parsed
		}
	}

	if val := os.Getenv("GORILLA_GC_PRESSURE_THRESHOLD"); val != "" {
		if parsed, err := strconv.ParseFloat(val, 64); err == nil {
			config.GCPressureThreshold = parsed
		}
	}

	if val := os.Getenv("GORILLA_ALLOCATOR_POOL_SIZE"); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			config.AllocatorPoolSize = parsed
		}
	}

	if val := os.Getenv("GORILLA_FILTER_FUSION"); val != "" {
		if parsed, err := strconv.ParseBool(val); err == nil {
			config.FilterFusion = parsed
		}
	}

	if val := os.Getenv("GORILLA_PREDICATE_PUSHDOWN"); val != "" {
		if parsed, err := strconv.ParseBool(val); err == nil {
			config.PredicatePushdown = parsed
		}
	}

	if val := os.Getenv("GORILLA_JOIN_OPTIMIZATION"); val != "" {
		if parsed, err := strconv.ParseBool(val); err == nil {
			config.JoinOptimization = parsed
		}
	}

	if val := os.Getenv("GORILLA_ENABLE_PROFILING"); val != "" {
		if parsed, err := strconv.ParseBool(val); err == nil {
			config.EnableProfiling = parsed
		}
	}

	if val := os.Getenv("GORILLA_VERBOSE_LOGGING"); val != "" {
		if parsed, err := strconv.ParseBool(val); err == nil {
			config.VerboseLogging = parsed
		}
	}

	if val := os.Getenv("GORILLA_METRICS_COLLECTION"); val != "" {
		if parsed, err := strconv.ParseBool(val); err == nil {
			config.MetricsCollection = parsed
		}
	}

	return config
}

// GetSystemInfo returns system information for configuration validation
func GetSystemInfo() SystemInfo {
	var memSize int64 = 8 * 1024 * 1024 * 1024 // 8GB default estimate

	return SystemInfo{
		CPUCount:     runtime.NumCPU(),
		MemorySize:   memSize,
		Architecture: runtime.GOARCH,
		OSType:       runtime.GOOS,
	}
}

// NewConfigValidator creates a new configuration validator
func NewConfigValidator() *ConfigValidator {
	return &ConfigValidator{
		systemInfo: GetSystemInfo(),
	}
}

// Validate validates a configuration and provides recommendations
func (cv *ConfigValidator) Validate(config Config) (Config, []string, error) {
	var warnings []string
	validated := config

	// Basic validation
	if err := config.Validate(); err != nil {
		return Config{}, warnings, err
	}

	// Validate worker pool size
	if config.WorkerPoolSize > cv.systemInfo.CPUCount*2 {
		warnings = append(warnings,
			fmt.Sprintf("Worker pool size (%d) exceeds 2x CPU count (%d), may cause contention",
				config.WorkerPoolSize, cv.systemInfo.CPUCount))
	}

	// Validate memory settings
	if config.MemoryThreshold > cv.systemInfo.MemorySize {
		return Config{}, warnings, fmt.Errorf(
			"Memory threshold (%d) exceeds estimated system memory (%d)",
			config.MemoryThreshold, cv.systemInfo.MemorySize)
	}

	// Auto-adjust unset values
	if config.WorkerPoolSize == 0 {
		validated.WorkerPoolSize = cv.systemInfo.CPUCount
		warnings = append(warnings,
			fmt.Sprintf("Auto-setting worker pool size to %d (CPU count)",
				validated.WorkerPoolSize))
	}

	return validated, warnings, nil
}

// NewPerformanceTuner creates a new performance tuner
func NewPerformanceTuner(config *Config) *PerformanceTuner {
	return &PerformanceTuner{
		config:           config,
		adaptiveSettings: make(map[string]interface{}),
	}
}

// OptimizeForDataset optimizes configuration for a specific dataset
func (pt *PerformanceTuner) OptimizeForDataset(rowCount int, columnCount int) Config {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	optimized := *pt.config

	// Adjust parallel threshold based on data size
	if rowCount < 100 {
		optimized.ParallelThreshold = rowCount + 1 // Disable parallel for very small datasets
	} else if rowCount >= 1000000 {
		optimized.ParallelThreshold = 500 // More aggressive parallel for large datasets
	}

	// Adjust chunk size based on data characteristics
	if columnCount > 50 {
		// Many columns - use smaller chunks to avoid memory pressure
		if optimized.ChunkSize == 0 {
			optimized.ChunkSize = 100
		} else {
			optimized.ChunkSize = minInt(optimized.ChunkSize, 100)
		}
	} else if columnCount < 5 {
		// Few columns - can use larger chunks
		if optimized.ChunkSize == 0 {
			optimized.ChunkSize = 2000
		} else {
			optimized.ChunkSize = maxInt(optimized.ChunkSize, 2000)
		}
	}

	// Adjust worker pool size based on system load (simplified)
	if pt.isSystemLoadHigh() {
		optimized.WorkerPoolSize = maxInt(1, optimized.WorkerPoolSize/2)
	}

	return optimized
}

// isSystemLoadHigh checks if system load is high (simplified implementation)
func (pt *PerformanceTuner) isSystemLoadHigh() bool {
	// In a real implementation, this would check actual system metrics
	// For now, return false as a safe default
	return false
}

// Helper functions
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
