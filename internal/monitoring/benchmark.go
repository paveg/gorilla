package monitoring

import (
	"fmt"
	"runtime"
	"strings"
	"time"
)

const (
	defaultDataSize   = 1000
	defaultIterations = 10
	bytesToMB         = 1024 * 1024
	percentageBase    = 100
)

// BenchmarkScenario represents a single performance benchmark scenario.
type BenchmarkScenario struct {
	Name        string
	Description string
	DataSize    int
	Operation   func() error
	Iterations  int
	Parallel    bool
}

// BenchmarkResult contains the results of running a benchmark scenario.
type BenchmarkResult struct {
	Scenario          BenchmarkScenario `json:"scenario"`
	Duration          time.Duration     `json:"duration"`
	AverageDuration   time.Duration     `json:"average_duration"`
	MinDuration       time.Duration     `json:"min_duration"`
	MaxDuration       time.Duration     `json:"max_duration"`
	MemoryAllocated   int64             `json:"memory_allocated"`
	MemoryAllocations int64             `json:"memory_allocations"`
	OperationsPerSec  float64           `json:"operations_per_sec"`
	Success           bool              `json:"success"`
	ErrorMessage      string            `json:"error_message,omitempty"`
}

// BenchmarkSuite manages and executes a collection of benchmark scenarios.
type BenchmarkSuite struct {
	scenarios []BenchmarkScenario
	results   []BenchmarkResult
}

// NewBenchmarkSuite creates a new benchmark suite.
func NewBenchmarkSuite() *BenchmarkSuite {
	return &BenchmarkSuite{
		scenarios: make([]BenchmarkScenario, 0),
		results:   make([]BenchmarkResult, 0),
	}
}

// AddScenario adds a benchmark scenario to the suite.
func (bs *BenchmarkSuite) AddScenario(scenario BenchmarkScenario) {
	bs.scenarios = append(bs.scenarios, scenario)
}

// AddQuickScenario adds a simple benchmark scenario with default parameters.
func (bs *BenchmarkSuite) AddQuickScenario(name, description string, operation func() error) {
	scenario := BenchmarkScenario{
		Name:        name,
		Description: description,
		DataSize:    defaultDataSize,
		Operation:   operation,
		Iterations:  defaultIterations,
		Parallel:    false,
	}
	bs.AddScenario(scenario)
}

// Run executes all benchmark scenarios and returns the results.
func (bs *BenchmarkSuite) Run() []BenchmarkResult {
	bs.results = make([]BenchmarkResult, 0, len(bs.scenarios))

	for _, scenario := range bs.scenarios {
		result := bs.runScenario(scenario)
		bs.results = append(bs.results, result)
	}

	return bs.results
}

// runScenario executes a single benchmark scenario.
func (bs *BenchmarkSuite) runScenario(scenario BenchmarkScenario) BenchmarkResult {
	if scenario.Iterations <= 0 {
		scenario.Iterations = 1
	}

	durations := make([]time.Duration, 0, scenario.Iterations)
	var totalDuration time.Duration
	var memBefore, memAfter runtime.MemStats
	success := true
	errorMessage := ""

	// Force GC before measuring memory
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	// Run the benchmark iterations
	for i := range scenario.Iterations {
		start := time.Now()

		if err := scenario.Operation(); err != nil {
			success = false
			errorMessage = fmt.Sprintf("Iteration %d failed: %v", i+1, err)
			break
		}

		duration := time.Since(start)
		durations = append(durations, duration)
		totalDuration += duration
	}

	// Measure memory after
	runtime.GC()
	runtime.ReadMemStats(&memAfter)

	// Calculate statistics
	var avgDuration, minDuration, maxDuration time.Duration

	if len(durations) > 0 {
		avgDuration = totalDuration / time.Duration(len(durations))
		minDuration = durations[0]
		maxDuration = durations[0]

		for _, d := range durations {
			if d < minDuration {
				minDuration = d
			}
			if d > maxDuration {
				maxDuration = d
			}
		}
	}

	// Calculate operations per second
	opsPerSec := 0.0
	if avgDuration > 0 {
		opsPerSec = 1.0 / avgDuration.Seconds()
	}

	return BenchmarkResult{
		Scenario:          scenario,
		Duration:          totalDuration,
		AverageDuration:   avgDuration,
		MinDuration:       minDuration,
		MaxDuration:       maxDuration,
		MemoryAllocated:   int64(memAfter.TotalAlloc - memBefore.TotalAlloc), //nolint:gosec // Safe memory calculation
		MemoryAllocations: int64(memAfter.Mallocs - memBefore.Mallocs),       //nolint:gosec // Safe memory calculation
		OperationsPerSec:  opsPerSec,
		Success:           success,
		ErrorMessage:      errorMessage,
	}
}

// GetResults returns the benchmark results.
func (bs *BenchmarkSuite) GetResults() []BenchmarkResult {
	return bs.results
}

// GenerateReport generates a markdown report of the benchmark results.
//

func (bs *BenchmarkSuite) GenerateReport() string {
	if len(bs.results) == 0 {
		return "# Benchmark Report\n\nNo benchmark results available.\n"
	}

	var report strings.Builder

	report.WriteString("# Gorilla DataFrame Benchmark Report\n\n")
	report.WriteString(fmt.Sprintf("Generated: %s\n\n", time.Now().Format(time.RFC3339)))

	bs.generateSummaryTable(&report)
	bs.generateDetailedResults(&report)
	bs.generatePerformanceInsights(&report)

	return report.String()
}

// generateSummaryTable generates the summary table section of the report.
func (bs *BenchmarkSuite) generateSummaryTable(report *strings.Builder) {
	report.WriteString("## Summary\n\n")
	report.WriteString("| Scenario | Iterations | Avg Duration | Ops/Sec | Memory (MB) | Status |\n")
	report.WriteString("|----------|------------|-------------|---------|-------------|--------|\n")

	for _, result := range bs.results {
		status := "✅ Success"
		if !result.Success {
			status = "❌ Failed"
		}

		memoryMB := float64(result.MemoryAllocated) / bytesToMB

		fmt.Fprintf(report, "| %s | %d | %v | %.2f | %.2f | %s |\n",
			result.Scenario.Name,
			result.Scenario.Iterations,
			result.AverageDuration,
			result.OperationsPerSec,
			memoryMB,
			status)
	}

	report.WriteString("\n")
}

// generateDetailedResults generates the detailed results section of the report.
func (bs *BenchmarkSuite) generateDetailedResults(report *strings.Builder) {
	report.WriteString("## Detailed Results\n\n")

	for _, result := range bs.results {
		fmt.Fprintf(report, "### %s\n\n", result.Scenario.Name)

		if result.Scenario.Description != "" {
			fmt.Fprintf(report, "**Description:** %s\n\n", result.Scenario.Description)
		}

		fmt.Fprintf(report, "- **Data Size:** %d\n", result.Scenario.DataSize)
		fmt.Fprintf(report, "- **Iterations:** %d\n", result.Scenario.Iterations)
		fmt.Fprintf(report, "- **Total Duration:** %v\n", result.Duration)
		fmt.Fprintf(report, "- **Average Duration:** %v\n", result.AverageDuration)
		fmt.Fprintf(report, "- **Min Duration:** %v\n", result.MinDuration)
		fmt.Fprintf(report, "- **Max Duration:** %v\n", result.MaxDuration)
		fmt.Fprintf(report, "- **Operations/Second:** %.2f\n", result.OperationsPerSec)
		fmt.Fprintf(report, "- **Memory Allocated:** %d bytes (%.2f MB)\n",
			result.MemoryAllocated, float64(result.MemoryAllocated)/bytesToMB)
		fmt.Fprintf(report, "- **Memory Allocations:** %d\n", result.MemoryAllocations)

		if !result.Success {
			fmt.Fprintf(report, "- **Error:** %s\n", result.ErrorMessage)
		}

		report.WriteString("\n")
	}
}

// generatePerformanceInsights generates the performance insights section of the report.
func (bs *BenchmarkSuite) generatePerformanceInsights(report *strings.Builder) {
	report.WriteString("## Performance Insights\n\n")

	if len(bs.results) > 1 {
		fastest, slowest := bs.findFastestAndSlowest()

		fmt.Fprintf(report, "- **Fastest Operation:** %s (%v average)\n",
			fastest.Scenario.Name, fastest.AverageDuration)
		fmt.Fprintf(report, "- **Slowest Operation:** %s (%v average)\n",
			slowest.Scenario.Name, slowest.AverageDuration)

		if slowest.AverageDuration > 0 {
			speedup := float64(slowest.AverageDuration) / float64(fastest.AverageDuration)
			fmt.Fprintf(report, "- **Performance Ratio:** %.2fx speedup (fastest vs slowest)\n", speedup)
		}
	}

	successful, _ := bs.countSuccessAndFailure()

	fmt.Fprintf(report, "- **Success Rate:** %d/%d (%.1f%%)\n",
		successful, len(bs.results), float64(successful)/float64(len(bs.results))*percentageBase)
}

// findFastestAndSlowest finds the fastest and slowest benchmark results.
func (bs *BenchmarkSuite) findFastestAndSlowest() (BenchmarkResult, BenchmarkResult) {
	fastest := bs.results[0]
	slowest := bs.results[0]

	for _, result := range bs.results[1:] {
		if result.Success && result.AverageDuration < fastest.AverageDuration {
			fastest = result
		}
		if result.Success && result.AverageDuration > slowest.AverageDuration {
			slowest = result
		}
	}

	return fastest, slowest
}

// countSuccessAndFailure counts successful and failed benchmark results.
func (bs *BenchmarkSuite) countSuccessAndFailure() (int, int) {
	var successful, failed int
	for _, result := range bs.results {
		if result.Success {
			successful++
		} else {
			failed++
		}
	}
	return successful, failed
}

// Clear removes all scenarios and results from the suite.
func (bs *BenchmarkSuite) Clear() {
	bs.scenarios = bs.scenarios[:0]
	bs.results = bs.results[:0]
}
