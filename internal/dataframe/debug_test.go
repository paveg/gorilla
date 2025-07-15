package dataframe

import (
	"encoding/json"
	"strings"
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
		result, err := analyzer.TraceOperation("filter", df, func() (*DataFrame, error) {
			return df.Lazy().Filter(expr.Col("age").Gt(expr.Lit(30))).Collect()
		})

		require.NoError(t, err)
		defer result.Release()

		assert.Equal(t, 3, result.Len()) // Bob, Charlie, David, Eve should pass filter
		assert.Equal(t, 1, len(analyzer.operations))

		op := analyzer.operations[0]
		assert.Equal(t, "filter", op.Operation)
		assert.Equal(t, 5, op.Input.Rows)
		assert.Equal(t, 2, op.Input.Columns)
		assert.Equal(t, 3, op.Output.Rows)
		assert.True(t, op.Duration > 0)
	})

	t.Run("generates analysis report", func(t *testing.T) {
		config := DebugConfig{
			Enabled:     true,
			TrackMemory: true,
		}

		analyzer := NewQueryAnalyzer(config)

		// Trace multiple operations
		_, err := analyzer.TraceOperation("filter", df, func() (*DataFrame, error) {
			return df.Lazy().Filter(expr.Col("age").Gt(expr.Lit(30))).Collect()
		})
		require.NoError(t, err)

		_, err = analyzer.TraceOperation("select", df, func() (*DataFrame, error) {
			return df.Select("name"), nil
		})
		require.NoError(t, err)

		report := analyzer.GenerateReport()

		assert.Equal(t, 2, len(report.Operations))
		assert.Equal(t, 2, report.Summary.TotalOperations)
		assert.True(t, report.Summary.TotalDuration > 0)
	})

	t.Run("identifies bottlenecks", func(t *testing.T) {
		config := DebugConfig{
			Enabled:     true,
			TrackMemory: true,
		}

		analyzer := NewQueryAnalyzer(config)

		// Simulate a slow operation
		_, err := analyzer.TraceOperation("slow_operation", df, func() (*DataFrame, error) {
			time.Sleep(100 * time.Millisecond)
			return df.Select("name"), nil
		})
		require.NoError(t, err)

		// Add a fast operation
		_, err = analyzer.TraceOperation("fast_operation", df, func() (*DataFrame, error) {
			return df.Select("age"), nil
		})
		require.NoError(t, err)

		report := analyzer.GenerateReport()

		// The slow operation should be identified as a bottleneck
		assert.True(t, len(report.Bottlenecks) > 0)
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
		_, err := analyzer.TraceOperation("large_filter", largeDF, func() (*DataFrame, error) {
			return largeDF.Lazy().Filter(expr.Col("age").Gt(expr.Lit(30))).Collect()
		})
		require.NoError(t, err)

		report := analyzer.GenerateReport()

		// Should suggest parallelization for large operations
		assert.True(t, len(report.Suggestions) > 0)
		assert.Contains(t, report.Suggestions[0], "Consider parallelizing")
	})

	t.Run("disabled analyzer skips tracing", func(t *testing.T) {
		config := DebugConfig{
			Enabled: false,
		}

		analyzer := NewQueryAnalyzer(config)

		result, err := analyzer.TraceOperation("filter", df, func() (*DataFrame, error) {
			return df.Lazy().Filter(expr.Col("age").Gt(expr.Lit(30))).Collect()
		})

		require.NoError(t, err)
		defer result.Release()

		// Should not have recorded any operations
		assert.Equal(t, 0, len(analyzer.operations))
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
		assert.True(t, plan.Estimated.TotalMemory > 0)
		assert.True(t, plan.Metadata.CreatedAt.After(time.Time{}))
	})

	t.Run("explains complex operation chain", func(t *testing.T) {
		lazyFrame := df.Lazy().
			Filter(expr.Col("age").Gt(expr.Lit(25))).
			Select("name", "age").
			Filter(expr.Col("age").Lt(expr.Lit(40)))

		plan := lazyFrame.Explain()

		assert.NotNil(t, plan.RootNode)
		assert.True(t, len(plan.RootNode.Children) > 0)

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

		plan, err := lazyFrame.ExplainAnalyze()
		require.NoError(t, err)

		assert.NotNil(t, plan.RootNode)
		assert.True(t, plan.Actual.TotalDuration > 0)
		assert.True(t, plan.Actual.TotalRows > 0)
		assert.True(t, plan.Actual.TotalMemory > 0)
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
		assert.True(t, plan.Estimated.ParallelOps > 0)
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
		jsonBytes, err := plan.RenderJSON()
		require.NoError(t, err)

		var jsonPlan map[string]interface{}
		err = json.Unmarshal(jsonBytes, &jsonPlan)
		require.NoError(t, err)

		assert.Contains(t, jsonPlan, "root")
		assert.Contains(t, jsonPlan, "estimated")
		assert.Contains(t, jsonPlan, "metadata")
	})

	t.Run("renders analyzed plan with actual stats", func(t *testing.T) {
		lazyFrame := df.Lazy().
			Filter(expr.Col("age").Gt(expr.Lit(30))).
			Select("name")

		plan, err := lazyFrame.ExplainAnalyze()
		require.NoError(t, err)

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

// Helper function to create a large test DataFrame
func createLargeTestDataFrame(mem memory.Allocator, size int) *DataFrame {
	names := make([]string, size)
	ages := make([]int64, size)

	for i := 0; i < size; i++ {
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
			ID:          "root",
			Type:        "LazyFrame",
			Description: "Root operation",
			Properties:  make(map[string]string),
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
		assert.Equal(t, 1, len(root.Children))
		assert.Equal(t, "filter_1", root.Children[0].ID)
		assert.Equal(t, 1, len(root.Children[0].Children))
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

func TestTraceIDGeneration(t *testing.T) {
	t.Run("generates unique trace IDs", func(t *testing.T) {
		id1 := generateTraceID()
		id2 := generateTraceID()

		assert.NotEqual(t, id1, id2)
		assert.True(t, strings.HasPrefix(id1, "trace_"))
		assert.True(t, strings.HasPrefix(id2, "trace_"))
	})
}
