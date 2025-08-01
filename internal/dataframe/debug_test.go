//nolint:testpackage // requires internal access to unexported types and functions
package dataframe

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/paveg/gorilla/internal/expr"
	"github.com/paveg/gorilla/internal/series"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDebugConfig(t *testing.T) {
	t.Run("creates debug config with default values", func(t *testing.T) {
		config := DebugConfig{
			Enabled:           true,
			LogLevel:          LogLevelDebug,
			ProfileOperations: true,
			TrackMemory:       true,
			ShowOptimizations: true,
			OutputFormat:      OutputFormatText,
		}

		assert.True(t, config.Enabled)
		assert.Equal(t, LogLevelDebug, config.LogLevel)
		assert.True(t, config.ProfileOperations)
		assert.True(t, config.TrackMemory)
		assert.True(t, config.ShowOptimizations)
		assert.Equal(t, OutputFormatText, config.OutputFormat)
	})

	t.Run("creates debug config with JSON format", func(t *testing.T) {
		config := DebugConfig{
			Enabled:      true,
			LogLevel:     LogLevelTrace,
			OutputFormat: OutputFormatJSON,
		}

		assert.True(t, config.Enabled)
		assert.Equal(t, LogLevelTrace, config.LogLevel)
		assert.Equal(t, OutputFormatJSON, config.OutputFormat)
	})
}

func TestQueryAnalyzer(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data
	stringValues := []string{"Alice", "Bob", "Charlie", "David", "Eve"}
	ageValues := []int64{25, 30, 35, 40, 45}

	stringSeries, err := series.NewSafe("name", stringValues, mem)
	require.NoError(t, err)
	defer stringSeries.Release()

	ageSeries, err := series.NewSafe("age", ageValues, mem)
	require.NoError(t, err)
	defer ageSeries.Release()

	df := New(stringSeries, ageSeries)
	defer df.Release()

	t.Run("traces operation execution", func(t *testing.T) {
		config := DebugConfig{
			Enabled:     true,
			TrackMemory: true,
		}

		analyzer := NewQueryAnalyzer(config)

		// Trace a simple operation
		result, traceErr := analyzer.TraceOperation("filter", df, func() (*DataFrame, error) {
			return df.Lazy().Filter(expr.Col("age").Gt(expr.Lit(30))).Collect()
		})

		require.NoError(t, traceErr)
		defer result.Release()

		assert.Equal(t, 3, result.Len()) // Bob, Charlie, David, Eve should pass filter
		assert.Len(t, analyzer.operations, 1)

		op := analyzer.operations[0]
		assert.Equal(t, "filter", op.Operation)
		assert.Equal(t, 5, op.Input.Rows)
		assert.Equal(t, 2, op.Input.Columns)
		assert.Equal(t, 3, op.Output.Rows)
		assert.Positive(t, op.Duration)
	})

	t.Run("generates analysis report", func(t *testing.T) {
		config := DebugConfig{
			Enabled:     true,
			TrackMemory: true,
		}

		analyzer := NewQueryAnalyzer(config)

		// Trace multiple operations
		_, traceErr1 := analyzer.TraceOperation("filter", df, func() (*DataFrame, error) {
			return df.Lazy().Filter(expr.Col("age").Gt(expr.Lit(30))).Collect()
		})
		require.NoError(t, traceErr1)

		_, traceErr2 := analyzer.TraceOperation("select", df, func() (*DataFrame, error) {
			return df.Select("name"), nil
		})
		require.NoError(t, traceErr2)

		report := analyzer.GenerateReport()

		assert.Len(t, report.Operations, 2)
		assert.Equal(t, 2, report.Summary.TotalOperations)
		assert.Positive(t, report.Summary.TotalDuration)
	})

	t.Run("identifies bottlenecks", func(t *testing.T) {
		config := DebugConfig{
			Enabled:     true,
			TrackMemory: true,
		}

		analyzer := NewQueryAnalyzer(config)

		// Simulate a slow operation
		_, slowErr := analyzer.TraceOperation("slow_operation", df, func() (*DataFrame, error) {
			time.Sleep(100 * time.Millisecond)
			return df.Select("name"), nil
		})
		require.NoError(t, slowErr)

		// Add a fast operation
		_, fastErr := analyzer.TraceOperation("fast_operation", df, func() (*DataFrame, error) {
			return df.Select("age"), nil
		})
		require.NoError(t, fastErr)

		report := analyzer.GenerateReport()

		// The slow operation should be identified as a bottleneck
		assert.NotEmpty(t, report.Bottlenecks)
		assert.Contains(t, report.Bottlenecks[0].Operation, "slow_operation")
	})

	t.Run("generates optimization suggestions", func(t *testing.T) {
		config := DebugConfig{
			Enabled:     true,
			TrackMemory: true,
		}

		analyzer := NewQueryAnalyzer(config)

		// Create a larger dataset that would benefit from parallelization
		largeDF := createLargeTestDataFrame(mem, 2000)
		defer largeDF.Release()

		// Trace operation on large dataset
		_, largeErr := analyzer.TraceOperation("large_filter", largeDF, func() (*DataFrame, error) {
			return largeDF.Lazy().Filter(expr.Col("age").Gt(expr.Lit(30))).Collect()
		})
		require.NoError(t, largeErr)

		report := analyzer.GenerateReport()

		// Should suggest parallelization for large operations
		assert.NotEmpty(t, report.Suggestions)
		assert.Contains(t, report.Suggestions[0], "Consider parallelizing")
	})

	t.Run("disabled analyzer skips tracing", func(t *testing.T) {
		config := DebugConfig{
			Enabled: false,
		}

		analyzer := NewQueryAnalyzer(config)

		result, disabledErr := analyzer.TraceOperation("filter", df, func() (*DataFrame, error) {
			return df.Lazy().Filter(expr.Col("age").Gt(expr.Lit(30))).Collect()
		})

		require.NoError(t, disabledErr)
		defer result.Release()

		// Should not have recorded any operations
		assert.Empty(t, analyzer.operations)
	})
}

func TestDebugExecutionPlan(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data
	stringValues := []string{"Alice", "Bob", "Charlie", "David", "Eve"}
	ageValues := []int64{25, 30, 35, 40, 45}

	stringSeries, err := series.NewSafe("name", stringValues, mem)
	require.NoError(t, err)
	defer stringSeries.Release()

	ageSeries, err := series.NewSafe("age", ageValues, mem)
	require.NoError(t, err)
	defer ageSeries.Release()

	df := New(stringSeries, ageSeries)
	defer df.Release()

	t.Run("explains simple operation", func(t *testing.T) {
		lazyFrame := df.Lazy().
			Filter(expr.Col("age").Gt(expr.Lit(30))).
			Select("name")

		plan := lazyFrame.Explain()

		assert.NotNil(t, plan.RootNode)
		assert.Equal(t, "LazyFrame", plan.RootNode.Type)
		assert.Equal(t, int64(5), plan.Estimated.TotalRows)
		assert.Positive(t, plan.Estimated.TotalMemory)
		assert.True(t, plan.Metadata.CreatedAt.After(time.Time{}))
	})

	t.Run("explains complex operation chain", func(t *testing.T) {
		lazyFrame := df.Lazy().
			Filter(expr.Col("age").Gt(expr.Lit(25))).
			Select("name", "age").
			Filter(expr.Col("age").Lt(expr.Lit(40)))

		plan := lazyFrame.Explain()

		assert.NotNil(t, plan.RootNode)
		assert.NotEmpty(t, plan.RootNode.Children)

		// Walk through the plan tree to verify structure
		current := plan.RootNode
		operationTypes := []string{}
		for len(current.Children) > 0 {
			current = current.Children[0]
			operationTypes = append(operationTypes, current.Type)
		}

		// Should have Filter -> Select -> Filter -> Scan
		assert.Contains(t, operationTypes, "Filter")
		assert.Contains(t, operationTypes, "Select")
		assert.Contains(t, operationTypes, "Scan")
	})

	t.Run("explains with analyze", func(t *testing.T) {
		lazyFrame := df.Lazy().
			Filter(expr.Col("age").Gt(expr.Lit(30))).
			Select("name")

		plan, explainErr := lazyFrame.ExplainAnalyze()
		require.NoError(t, explainErr)

		assert.NotNil(t, plan.RootNode)
		assert.Positive(t, plan.Actual.TotalDuration)
		assert.Positive(t, plan.Actual.TotalRows)
		assert.Positive(t, plan.Actual.TotalMemory)
		assert.True(t, plan.Metadata.ExecutedAt.After(time.Time{}))
	})

	t.Run("detects parallel execution", func(t *testing.T) {
		// Create larger dataset to trigger parallel execution
		largeDF := createLargeTestDataFrame(mem, 2000)
		defer largeDF.Release()

		lazyFrame := largeDF.Lazy().
			Filter(expr.Col("age").Gt(expr.Lit(30)))

		plan := lazyFrame.Explain()

		// Should detect parallel execution for large datasets
		assert.Equal(t, "true", plan.RootNode.Properties["parallel"])
		assert.Positive(t, plan.Estimated.ParallelOps)
	})
}

func TestDebugExecutionPlanRendering(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data
	stringValues := []string{"Alice", "Bob", "Charlie"}
	ageValues := []int64{25, 30, 35}

	stringSeries, err := series.NewSafe("name", stringValues, mem)
	require.NoError(t, err)
	defer stringSeries.Release()

	ageSeries, err := series.NewSafe("age", ageValues, mem)
	require.NoError(t, err)
	defer ageSeries.Release()

	df := New(stringSeries, ageSeries)
	defer df.Release()

	t.Run("renders plan as text", func(t *testing.T) {
		lazyFrame := df.Lazy().
			Filter(expr.Col("age").Gt(expr.Lit(30))).
			Select("name")

		plan := lazyFrame.Explain()
		textPlan := plan.RenderText()

		assert.Contains(t, textPlan, "Execution Plan:")
		assert.Contains(t, textPlan, "LazyFrame")
		assert.Contains(t, textPlan, "Filter")
		assert.Contains(t, textPlan, "Select")
		assert.Contains(t, textPlan, "Scan")
		assert.Contains(t, textPlan, "Estimated rows:")
	})

	t.Run("renders plan as JSON", func(t *testing.T) {
		lazyFrame := df.Lazy().
			Filter(expr.Col("age").Gt(expr.Lit(30))).
			Select("name")

		plan := lazyFrame.Explain()
		jsonBytes, renderErr := plan.RenderJSON()
		require.NoError(t, renderErr)

		var jsonPlan map[string]interface{}
		unmarshalErr := json.Unmarshal(jsonBytes, &jsonPlan)
		require.NoError(t, unmarshalErr)

		assert.Contains(t, jsonPlan, "root")
		assert.Contains(t, jsonPlan, "estimated")
		assert.Contains(t, jsonPlan, "metadata")
	})

	t.Run("renders analyzed plan with actual stats", func(t *testing.T) {
		lazyFrame := df.Lazy().
			Filter(expr.Col("age").Gt(expr.Lit(30))).
			Select("name")

		plan, analyzeErr := lazyFrame.ExplainAnalyze()
		require.NoError(t, analyzeErr)

		textPlan := plan.RenderText()

		assert.Contains(t, textPlan, "Execution Plan:")
		assert.Contains(t, textPlan, "Total Execution Time:")
		assert.Contains(t, textPlan, "Peak Memory Usage:")
	})
}

func TestDataFrameDebugMethods(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create test data
	stringValues := []string{"Alice", "Bob", "Charlie"}
	ageValues := []int64{25, 30, 35}

	stringSeries, err := series.NewSafe("name", stringValues, mem)
	require.NoError(t, err)
	defer stringSeries.Release()

	ageSeries, err := series.NewSafe("age", ageValues, mem)
	require.NoError(t, err)
	defer ageSeries.Release()

	df := New(stringSeries, ageSeries)
	defer df.Release()

	t.Run("enables debug mode", func(t *testing.T) {
		debugDF := df.Debug()

		// Debug method should return a DataFrame (even if config isn't fully integrated yet)
		assert.NotNil(t, debugDF)
		assert.Equal(t, df.Len(), debugDF.Len())
		assert.Equal(t, df.Width(), debugDF.Width())
	})

	t.Run("sets custom debug config", func(t *testing.T) {
		config := DebugConfig{
			Enabled:           true,
			LogLevel:          LogLevelTrace,
			ProfileOperations: false,
			TrackMemory:       false,
			ShowOptimizations: false,
			OutputFormat:      OutputFormatJSON,
		}

		debugDF := df.WithDebugConfig(config)

		// WithDebugConfig should return a DataFrame
		assert.NotNil(t, debugDF)
		assert.Equal(t, df.Len(), debugDF.Len())
		assert.Equal(t, df.Width(), debugDF.Width())
	})
}

// Helper function to create a large test DataFrame.
func createLargeTestDataFrame(mem memory.Allocator, size int) *DataFrame {
	names := make([]string, size)
	ages := make([]int64, size)

	for i := range size {
		names[i] = "Person" + string(rune(i%26+'A'))
		ages[i] = int64(20 + i%50)
	}

	namesSeries, _ := series.NewSafe("name", names, mem)
	agesSeries, _ := series.NewSafe("age", ages, mem)

	return New(namesSeries, agesSeries)
}

func TestPlanNodeTree(t *testing.T) {
	t.Run("creates proper node hierarchy", func(t *testing.T) {
		root := &PlanNode{
			ID: "root",
		}

		filterNode := &PlanNode{
			ID:          "filter_1",
			Type:        "Filter",
			Description: "age > 30",
			Properties:  make(map[string]string),
		}

		scanNode := &PlanNode{
			ID:          "scan",
			Type:        "Scan",
			Description: "DataFrame",
			Properties:  make(map[string]string),
		}

		filterNode.Children = []*PlanNode{scanNode}
		root.Children = []*PlanNode{filterNode}

		assert.Equal(t, "root", root.ID)
		assert.Len(t, root.Children, 1)
		assert.Equal(t, "filter_1", root.Children[0].ID)
		assert.Len(t, root.Children[0].Children, 1)
		assert.Equal(t, "scan", root.Children[0].Children[0].ID)
	})
}

func TestCostEstimation(t *testing.T) {
	t.Run("estimates costs correctly", func(t *testing.T) {
		cost := PlanCost{
			Estimated: EstimatedCost{
				Rows:   1000,
				Memory: 8000,
				CPU:    100 * time.Millisecond,
			},
		}

		assert.Equal(t, int64(1000), cost.Estimated.Rows)
		assert.Equal(t, int64(8000), cost.Estimated.Memory)
		assert.Equal(t, 100*time.Millisecond, cost.Estimated.CPU)
	})

	t.Run("tracks actual costs", func(t *testing.T) {
		cost := PlanCost{
			Estimated: EstimatedCost{
				Rows:   1000,
				Memory: 8000,
			},
			Actual: ActualCost{
				Rows:     800,
				Memory:   6400,
				Duration: 150 * time.Millisecond,
			},
		}

		assert.Equal(t, int64(800), cost.Actual.Rows)
		assert.Equal(t, int64(6400), cost.Actual.Memory)
		assert.Equal(t, 150*time.Millisecond, cost.Actual.Duration)
	})
}

func TestMemoryStats(t *testing.T) {
	t.Run("calculates memory deltas", func(t *testing.T) {
		stats := MemoryStats{
			Before: 1000,
			After:  1500,
			Delta:  500,
		}

		assert.Equal(t, int64(1000), stats.Before)
		assert.Equal(t, int64(1500), stats.After)
		assert.Equal(t, int64(500), stats.Delta)
	})
}

func TestDebugContext(t *testing.T) {
	t.Run("creates new debug context", func(t *testing.T) {
		debugCtx := NewDebugContext()
		assert.NotNil(t, debugCtx)
		// Note: traceCounter and contextID are private fields, so we test functionality instead
		// by verifying that trace ID generation works
		id := debugCtx.GenerateTraceID()
		assert.True(t, strings.HasPrefix(id, "trace_"))
	})

	t.Run("generates unique trace IDs", func(t *testing.T) {
		debugCtx := NewDebugContext()
		id1 := debugCtx.GenerateTraceID()
		id2 := debugCtx.GenerateTraceID()

		assert.NotEqual(t, id1, id2)
		assert.True(t, strings.HasPrefix(id1, "trace_"))
		assert.True(t, strings.HasPrefix(id2, "trace_"))
	})

	t.Run("generates unique trace IDs across different contexts", func(t *testing.T) {
		debugCtx1 := NewDebugContext()
		debugCtx2 := NewDebugContext()

		id1 := debugCtx1.GenerateTraceID()
		id2 := debugCtx2.GenerateTraceID()
		id3 := debugCtx1.GenerateTraceID()

		assert.NotEqual(t, id1, id2)
		assert.NotEqual(t, id1, id3)
		assert.NotEqual(t, id2, id3)
		assert.True(t, strings.HasPrefix(id1, "trace_"))
		assert.True(t, strings.HasPrefix(id2, "trace_"))
		assert.True(t, strings.HasPrefix(id3, "trace_"))
	})

	t.Run("thread safety of trace ID generation", func(t *testing.T) {
		debugCtx := NewDebugContext()
		const numGoroutines = 100
		const numIDsPerGoroutine = 10

		var wg sync.WaitGroup
		idChannel := make(chan string, numGoroutines*numIDsPerGoroutine)

		// Launch goroutines to generate trace IDs concurrently
		for range numGoroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range numIDsPerGoroutine {
					id := debugCtx.GenerateTraceID()
					idChannel <- id
				}
			}()
		}

		wg.Wait()
		close(idChannel)

		// Collect all IDs and check for uniqueness
		ids := make(map[string]bool)
		count := 0
		for id := range idChannel {
			assert.False(t, ids[id], "Duplicate trace ID found: %s", id)
			assert.True(t, strings.HasPrefix(id, "trace_"))
			ids[id] = true
			count++
		}

		assert.Equal(t, numGoroutines*numIDsPerGoroutine, count)
		assert.Len(t, ids, numGoroutines*numIDsPerGoroutine)
	})
}

func TestTraceIDGeneration(t *testing.T) {
	t.Run("generates unique trace IDs", func(t *testing.T) {
		debugCtx := NewDebugContext()
		id1 := debugCtx.GenerateTraceID()
		id2 := debugCtx.GenerateTraceID()

		assert.NotEqual(t, id1, id2)
		assert.True(t, strings.HasPrefix(id1, "trace_"))
		assert.True(t, strings.HasPrefix(id2, "trace_"))
	})

	t.Run("generates unique trace IDs across different contexts", func(t *testing.T) {
		debugCtx1 := NewDebugContext()
		debugCtx2 := NewDebugContext()

		// Generate multiple IDs from each context to ensure internal counters work
		ids := make([]string, 0, 6)
		ids = append(ids, debugCtx1.GenerateTraceID())
		ids = append(ids, debugCtx2.GenerateTraceID())
		ids = append(ids, debugCtx1.GenerateTraceID())
		ids = append(ids, debugCtx2.GenerateTraceID())
		ids = append(ids, debugCtx1.GenerateTraceID())
		ids = append(ids, debugCtx2.GenerateTraceID())

		// Verify all IDs are unique
		uniqueIDs := make(map[string]bool)
		for _, id := range ids {
			assert.False(t, uniqueIDs[id], "Duplicate ID found: %s", id)
			assert.True(t, strings.HasPrefix(id, "trace_"))
			uniqueIDs[id] = true
		}

		assert.Len(t, uniqueIDs, len(ids))
	})
}
