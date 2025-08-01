package dataframe

import (
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

// Package-level constants for consistent usage across debug functionality.
const (
	// AvgBytesPerCell is the estimated average bytes per DataFrame cell.
	AvgBytesPerCell = 8
	// ParallelThreshold is the minimum number of rows to trigger parallel execution.
	ParallelThreshold = 1000
	// FilterSelectivity is the default assumed selectivity for filter operations.
	FilterSelectivity = 0.5
)

// DebugConfig configures debug mode settings.
type DebugConfig struct {
	Enabled           bool         `json:"enabled"`
	LogLevel          LogLevel     `json:"log_level"`
	ProfileOperations bool         `json:"profile_operations"`
	TrackMemory       bool         `json:"track_memory"`
	ShowOptimizations bool         `json:"show_optimizations"`
	OutputFormat      OutputFormat `json:"output_format"`
}

// LogLevel defines the verbosity of debug logging.
type LogLevel int

const (
	LogLevelInfo LogLevel = iota
	LogLevelDebug
	LogLevelTrace
)

// OutputFormat defines the output format for debug information.
type OutputFormat int

const (
	OutputFormatText OutputFormat = iota
	OutputFormatJSON
)

// DebugExecutionPlan represents the execution plan for DataFrame operations with debug information.
type DebugExecutionPlan struct {
	RootNode  *PlanNode         `json:"root"`
	Estimated PlanStats         `json:"estimated"`
	Actual    PlanStats         `json:"actual,omitempty"`
	Metadata  DebugPlanMetadata `json:"metadata"`
}

// PlanNode represents a node in the execution plan tree.
type PlanNode struct {
	ID          string            `json:"id"`
	Type        string            `json:"type"`        // "Filter", "Select", "GroupBy", etc.
	Description string            `json:"description"` // Human-readable operation description
	Children    []*PlanNode       `json:"children,omitempty"`
	Cost        PlanCost          `json:"cost"`
	Properties  map[string]string `json:"properties"`
}

// PlanCost contains cost information for a plan node.
type PlanCost struct {
	Estimated EstimatedCost `json:"estimated"`
	Actual    ActualCost    `json:"actual,omitempty"`
}

// EstimatedCost contains estimated cost metrics.
type EstimatedCost struct {
	Rows   int64         `json:"rows"`
	Memory int64         `json:"memory"`
	CPU    time.Duration `json:"cpu"`
}

// ActualCost contains actual cost metrics after execution.
type ActualCost struct {
	Rows     int64         `json:"rows"`
	Memory   int64         `json:"memory"`
	CPU      time.Duration `json:"cpu"`
	Duration time.Duration `json:"duration"`
}

// PlanStats contains overall plan statistics.
type PlanStats struct {
	TotalRows     int64         `json:"total_rows"`
	TotalMemory   int64         `json:"total_memory"`
	TotalDuration time.Duration `json:"total_duration"`
	PeakMemory    int64         `json:"peak_memory"`
	ParallelOps   int           `json:"parallel_ops"`
}

// DebugPlanMetadata contains metadata about the execution plan.
type DebugPlanMetadata struct {
	CreatedAt     time.Time `json:"created_at"`
	OptimizedAt   time.Time `json:"optimized_at,omitempty"`
	ExecutedAt    time.Time `json:"executed_at,omitempty"`
	Optimizations []string  `json:"optimizations,omitempty"`
}

// OperationTrace represents a traced operation during execution.
type OperationTrace struct {
	ID          string            `json:"id"`
	Operation   string            `json:"operation"`
	Input       Stats             `json:"input"`
	Output      Stats             `json:"output"`
	Duration    time.Duration     `json:"duration"`
	Memory      MemoryStats       `json:"memory"`
	Parallel    bool              `json:"parallel"`
	WorkerCount int               `json:"worker_count,omitempty"`
	Properties  map[string]string `json:"properties"`
}

// Stats contains statistics about a DataFrame.
type Stats struct {
	Rows    int      `json:"rows"`
	Columns int      `json:"columns"`
	Memory  int64    `json:"memory"`
	Schema  []string `json:"schema"`
}

// MemoryStats contains memory usage statistics.
type MemoryStats struct {
	Before int64 `json:"before"`
	After  int64 `json:"after"`
	Delta  int64 `json:"delta"`
}

// DebugContext manages debug-related state and provides thread-safe utilities.
type DebugContext struct {
	traceCounter int64
	contextID    int64
}

// contextIDCounter is used to assign unique IDs to each debug context.
//
//nolint:gochecknoglobals // This global counter is necessary for ensuring unique context IDs across all debug contexts.
var contextIDCounter int64

// NewDebugContext creates a new debug context.
func NewDebugContext() *DebugContext {
	return &DebugContext{
		contextID: atomic.AddInt64(&contextIDCounter, 1),
	}
}

// GenerateTraceID generates a unique trace ID using atomic operations for thread safety.
func (dc *DebugContext) GenerateTraceID() string {
	// Use context ID + local counter to ensure uniqueness across all contexts
	counter := atomic.AddInt64(&dc.traceCounter, 1)
	return fmt.Sprintf("trace_%d_%d_%d", time.Now().UnixNano(), dc.contextID, counter)
}

// QueryAnalyzer analyzes and traces query execution.
type QueryAnalyzer struct {
	operations   []OperationTrace
	config       DebugConfig
	debugContext *DebugContext
}

// NewQueryAnalyzer creates a new query analyzer.
func NewQueryAnalyzer(config DebugConfig) *QueryAnalyzer {
	return &QueryAnalyzer{
		operations:   make([]OperationTrace, 0),
		config:       config,
		debugContext: NewDebugContext(),
	}
}

// TraceOperation traces an operation execution.
func (qa *QueryAnalyzer) TraceOperation(
	op string, input *DataFrame, fn func() (*DataFrame, error),
) (*DataFrame, error) {
	if !qa.config.Enabled {
		return fn()
	}

	trace := OperationTrace{
		ID:        qa.debugContext.GenerateTraceID(),
		Operation: op,
		Input:     qa.captureStats(input),
	}

	start := time.Now()
	var memBefore runtime.MemStats
	if qa.config.TrackMemory {
		runtime.ReadMemStats(&memBefore)
	}

	result, err := fn()

	if qa.config.TrackMemory {
		var memAfter runtime.MemStats
		runtime.ReadMemStats(&memAfter)
		trace.Memory = MemoryStats{
			Before: convertMemoryStats(memBefore.Alloc),
			After:  convertMemoryStats(memAfter.Alloc),
			Delta:  convertMemoryStats(memAfter.Alloc - memBefore.Alloc),
		}
	}

	trace.Duration = time.Since(start)

	if result != nil {
		trace.Output = qa.captureStats(result)
	}

	qa.operations = append(qa.operations, trace)
	return result, err
}

// captureStats captures DataFrame statistics.
func (qa *QueryAnalyzer) captureStats(df *DataFrame) Stats {
	if df == nil {
		return Stats{}
	}

	stats := Stats{
		Rows:    df.Len(),
		Columns: df.Width(),
		Schema:  df.Columns(),
	}

	// Estimate memory usage
	// This is a simplified estimation - in practice would need more accurate calculation
	stats.Memory = int64(stats.Rows * stats.Columns * AvgBytesPerCell) // Assume 8 bytes per value average

	return stats
}

// GenerateReport generates an analysis report.
func (qa *QueryAnalyzer) GenerateReport() AnalysisReport {
	return AnalysisReport{
		Operations:  qa.operations,
		Summary:     qa.generateSummary(),
		Bottlenecks: qa.identifyBottlenecks(),
		Suggestions: qa.generateSuggestions(),
	}
}

// generateSummary generates a summary of operations.
func (qa *QueryAnalyzer) generateSummary() OperationSummary {
	var totalDuration time.Duration
	var totalMemory int64
	parallelOps := 0

	for i := range qa.operations {
		op := &qa.operations[i]
		totalDuration += op.Duration
		totalMemory += op.Memory.Delta
		if op.Parallel {
			parallelOps++
		}
	}

	return OperationSummary{
		TotalOperations: len(qa.operations),
		TotalDuration:   totalDuration,
		TotalMemory:     totalMemory,
		ParallelOps:     parallelOps,
	}
}

// identifyBottlenecks identifies performance bottlenecks.
func (qa *QueryAnalyzer) identifyBottlenecks() []Bottleneck {
	bottlenecks := make([]Bottleneck, 0)

	// Find operations that take >50% of total time
	var totalDuration time.Duration
	for i := range qa.operations {
		totalDuration += qa.operations[i].Duration
	}

	const bottleneckThreshold = 2 // 50% = 1/2
	for i := range qa.operations {
		op := &qa.operations[i]
		if op.Duration > totalDuration/bottleneckThreshold {
			bottlenecks = append(bottlenecks, Bottleneck{
				Operation: op.Operation,
				Duration:  op.Duration,
				Reason:    "Takes more than 50% of total execution time",
			})
		}
	}

	return bottlenecks
}

// generateSuggestions generates optimization suggestions.
func (qa *QueryAnalyzer) generateSuggestions() []string {
	suggestions := make([]string, 0)

	// Check for operations that could benefit from parallelization
	for i := range qa.operations {
		op := &qa.operations[i]
		if !op.Parallel && op.Input.Rows > ParallelThreshold {
			suggestions = append(suggestions,
				fmt.Sprintf("Consider parallelizing %s operation (processing %d rows)",
					op.Operation, op.Input.Rows))
		}
	}

	return suggestions
}

// AnalysisReport contains the complete analysis report.
type AnalysisReport struct {
	Operations  []OperationTrace `json:"operations"`
	Summary     OperationSummary `json:"summary"`
	Bottlenecks []Bottleneck     `json:"bottlenecks"`
	Suggestions []string         `json:"suggestions"`
}

// OperationSummary contains summary statistics.
type OperationSummary struct {
	TotalOperations int           `json:"total_operations"`
	TotalDuration   time.Duration `json:"total_duration"`
	TotalMemory     int64         `json:"total_memory"`
	ParallelOps     int           `json:"parallel_ops"`
}

// Bottleneck represents a performance bottleneck.
type Bottleneck struct {
	Operation string        `json:"operation"`
	Duration  time.Duration `json:"duration"`
	Reason    string        `json:"reason"`
}

// convertMemoryStats safely converts uint64 to int64 for memory statistics.
func convertMemoryStats(val uint64) int64 {
	const maxInt64 = int64(^uint64(0) >> 1)
	if val > uint64(maxInt64) {
		return maxInt64
	}
	return int64(val)
}

// Debug enables debug mode for the DataFrame.
func (df *DataFrame) Debug() *DataFrame {
	return df.WithDebugConfig(DebugConfig{
		Enabled:           true,
		LogLevel:          LogLevelDebug,
		ProfileOperations: true,
		TrackMemory:       true,
		ShowOptimizations: true,
		OutputFormat:      OutputFormatText,
	})
}

// WithDebugConfig sets the debug configuration for the DataFrame.
func (df *DataFrame) WithDebugConfig(_ DebugConfig) *DataFrame {
	// Create a new DataFrame with debug config
	// In a real implementation, we would add a debug field to DataFrame
	// For now, we'll store it in a context or similar mechanism
	return df
}

// RenderText renders the execution plan as text.
func (plan *DebugExecutionPlan) RenderText() string {
	var buf strings.Builder
	buf.WriteString("Execution Plan:\n")
	buf.WriteString("=============\n\n")

	plan.renderNode(plan.RootNode, &buf, 0)

	if plan.Actual.TotalDuration > 0 {
		buf.WriteString(fmt.Sprintf("\nTotal Execution Time: %v\n", plan.Actual.TotalDuration))
		buf.WriteString(fmt.Sprintf("Peak Memory Usage: %d bytes\n", plan.Actual.PeakMemory))
	}

	return buf.String()
}

// renderNode renders a plan node recursively.
func (plan *DebugExecutionPlan) renderNode(node *PlanNode, buf *strings.Builder, depth int) {
	if node == nil {
		return
	}

	indent := strings.Repeat("  ", depth)

	fmt.Fprintf(buf, "%s├─ %s: %s\n",
		indent, node.Type, node.Description)

	if node.Cost.Estimated.Rows > 0 {
		fmt.Fprintf(buf, "%s│  Estimated rows: %d\n",
			indent, node.Cost.Estimated.Rows)
	}

	if node.Cost.Actual.Rows > 0 {
		const percentageMultiplier = 100.0
		fmt.Fprintf(buf, "%s│  Actual rows: %d (%.1f%% of estimate)\n",
			indent, node.Cost.Actual.Rows,
			float64(node.Cost.Actual.Rows)/float64(node.Cost.Estimated.Rows)*percentageMultiplier)
	}

	if node.Properties["parallel"] == "true" {
		fmt.Fprintf(buf, "%s│  Parallel execution: %s workers\n",
			indent, node.Properties["worker_count"])
	}

	for _, child := range node.Children {
		plan.renderNode(child, buf, depth+1)
	}
}

// RenderJSON renders the execution plan as JSON.
func (plan *DebugExecutionPlan) RenderJSON() ([]byte, error) {
	return json.MarshalIndent(plan, "", "  ")
}
