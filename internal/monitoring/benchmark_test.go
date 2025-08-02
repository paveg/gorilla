//nolint:testpackage // requires internal access to unexported types and functions
package monitoring

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBenchmarkScenario(t *testing.T) {
	t.Run("create benchmark scenario", func(t *testing.T) {
		scenario := BenchmarkScenario{
			Name:        "test_scenario",
			Description: "A test scenario",
			DataSize:    1000,
			Operation:   func() error { return nil },
			Iterations:  5,
			Parallel:    false,
		}

		assert.Equal(t, "test_scenario", scenario.Name)
		assert.Equal(t, "A test scenario", scenario.Description)
		assert.Equal(t, 1000, scenario.DataSize)
		assert.Equal(t, 5, scenario.Iterations)
		assert.False(t, scenario.Parallel)
		assert.NotNil(t, scenario.Operation)
	})
}

func TestBenchmarkSuite(t *testing.T) {
	t.Run("create empty benchmark suite", func(t *testing.T) {
		suite := NewBenchmarkSuite()
		assert.NotNil(t, suite)
		assert.Empty(t, suite.scenarios)
		assert.Empty(t, suite.results)
	})

	t.Run("add scenario to suite", func(t *testing.T) {
		suite := NewBenchmarkSuite()

		scenario := BenchmarkScenario{
			Name:       "test",
			Operation:  func() error { return nil },
			Iterations: 1,
		}

		suite.AddScenario(scenario)
		assert.Len(t, suite.scenarios, 1)
		assert.Equal(t, "test", suite.scenarios[0].Name)
	})

	t.Run("add quick scenario", func(t *testing.T) {
		suite := NewBenchmarkSuite()

		suite.AddQuickScenario("quick_test", "Quick test scenario", func() error {
			return nil
		})

		require.Len(t, suite.scenarios, 1)
		scenario := suite.scenarios[0]
		assert.Equal(t, "quick_test", scenario.Name)
		assert.Equal(t, "Quick test scenario", scenario.Description)
		assert.Equal(t, 1000, scenario.DataSize)
		assert.Equal(t, 10, scenario.Iterations)
		assert.False(t, scenario.Parallel)
	})

	t.Run("run successful benchmark", func(t *testing.T) {
		suite := NewBenchmarkSuite()

		callCount := 0
		suite.AddQuickScenario("success_test", "Successful test", func() error {
			callCount++
			time.Sleep(1 * time.Millisecond) // Simulate work
			return nil
		})

		results := suite.Run()

		require.Len(t, results, 1)
		result := results[0]

		assert.Equal(t, "success_test", result.Scenario.Name)
		assert.True(t, result.Success)
		assert.Empty(t, result.ErrorMessage)
		assert.Equal(t, 10, callCount) // Should run 10 iterations
		assert.Greater(t, result.Duration, 5*time.Millisecond)
		assert.Greater(t, result.AverageDuration, time.Duration(0))
		assert.Greater(t, result.OperationsPerSec, 0.0)
	})

	t.Run("run failing benchmark", func(t *testing.T) {
		suite := NewBenchmarkSuite()

		expectedError := errors.New("test error")
		suite.AddQuickScenario("fail_test", "Failing test", func() error {
			return expectedError
		})

		results := suite.Run()

		require.Len(t, results, 1)
		result := results[0]

		assert.Equal(t, "fail_test", result.Scenario.Name)
		assert.False(t, result.Success)
		assert.Contains(t, result.ErrorMessage, "test error")
	})

	t.Run("run multiple scenarios", func(t *testing.T) {
		suite := NewBenchmarkSuite()

		suite.AddQuickScenario("fast_op", "Fast operation", func() error {
			return nil
		})

		suite.AddQuickScenario("slow_op", "Slow operation", func() error {
			time.Sleep(5 * time.Millisecond)
			return nil
		})

		results := suite.Run()

		require.Len(t, results, 2)

		// Find fast and slow results
		var fastResult, slowResult *BenchmarkResult
		for i := range results {
			switch results[i].Scenario.Name {
			case "fast_op":
				fastResult = &results[i]
			case "slow_op":
				slowResult = &results[i]
			}
		}

		require.NotNil(t, fastResult)
		require.NotNil(t, slowResult)

		assert.True(t, fastResult.Success)
		assert.True(t, slowResult.Success)
		assert.Greater(t, slowResult.AverageDuration, fastResult.AverageDuration)
	})

	t.Run("benchmark with zero iterations defaults to 1", func(t *testing.T) {
		suite := NewBenchmarkSuite()

		scenario := BenchmarkScenario{
			Name:       "zero_iterations",
			Operation:  func() error { return nil },
			Iterations: 0,
		}

		suite.AddScenario(scenario)
		results := suite.Run()

		require.Len(t, results, 1)
		result := results[0]
		assert.True(t, result.Success)
		assert.Greater(t, result.Duration, time.Duration(0))
	})
}

func TestBenchmarkReport(t *testing.T) {
	t.Run("empty report", func(t *testing.T) {
		suite := NewBenchmarkSuite()
		report := suite.GenerateReport()

		assert.Contains(t, report, "Benchmark Report")
		assert.Contains(t, report, "No benchmark results available")
	})

	t.Run("report with results", func(t *testing.T) {
		suite := NewBenchmarkSuite()

		suite.AddQuickScenario("test_op", "Test operation", func() error {
			time.Sleep(1 * time.Millisecond)
			return nil
		})

		suite.Run()
		report := suite.GenerateReport()

		assert.Contains(t, report, "Gorilla DataFrame Benchmark Report")
		assert.Contains(t, report, "test_op")
		assert.Contains(t, report, "Summary")
		assert.Contains(t, report, "Detailed Results")
		assert.Contains(t, report, "Performance Insights")
		assert.Contains(t, report, "✅ Success")
	})

	t.Run("report with failed results", func(t *testing.T) {
		suite := NewBenchmarkSuite()

		suite.AddQuickScenario("fail_op", "Failing operation", func() error {
			return errors.New("test failure")
		})

		suite.Run()
		report := suite.GenerateReport()

		assert.Contains(t, report, "❌ Failed")
		assert.Contains(t, report, "test failure")
	})

	t.Run("report with multiple results shows performance comparison", func(t *testing.T) {
		suite := NewBenchmarkSuite()

		suite.AddQuickScenario("fast_op", "Fast operation", func() error {
			return nil
		})

		suite.AddQuickScenario("slow_op", "Slow operation", func() error {
			time.Sleep(10 * time.Millisecond)
			return nil
		})

		suite.Run()
		report := suite.GenerateReport()

		assert.Contains(t, report, "Fastest Operation")
		assert.Contains(t, report, "Slowest Operation")
		assert.Contains(t, report, "Performance Ratio")
		assert.Contains(t, report, "Success Rate")
	})
}

func TestBenchmarkSuiteMethods(t *testing.T) {
	t.Run("get results", func(t *testing.T) {
		suite := NewBenchmarkSuite()

		suite.AddQuickScenario("test", "Test", func() error {
			return nil
		})

		// Before running
		assert.Empty(t, suite.GetResults())

		// After running
		suite.Run()
		results := suite.GetResults()
		assert.Len(t, results, 1)
	})

	t.Run("clear suite", func(t *testing.T) {
		suite := NewBenchmarkSuite()

		suite.AddQuickScenario("test", "Test", func() error {
			return nil
		})
		suite.Run()

		assert.Len(t, suite.scenarios, 1)
		assert.Len(t, suite.results, 1)

		suite.Clear()

		assert.Empty(t, suite.scenarios)
		assert.Empty(t, suite.results)
	})
}

func TestBenchmarkResultStatistics(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping statistical tests in short mode")
	}

	t.Run("duration statistics", func(t *testing.T) {
		suite := NewBenchmarkSuite()

		// Create scenario with variable execution time
		variableDelay := []time.Duration{
			1 * time.Millisecond,
			2 * time.Millisecond,
			3 * time.Millisecond,
			4 * time.Millisecond,
			5 * time.Millisecond,
		}

		iteration := 0
		scenario := BenchmarkScenario{
			Name: "variable_timing",
			Operation: func() error {
				if iteration < len(variableDelay) {
					time.Sleep(variableDelay[iteration])
					iteration++
				}
				return nil
			},
			Iterations: len(variableDelay),
		}

		suite.AddScenario(scenario)
		results := suite.Run()

		require.Len(t, results, 1)
		result := results[0]

		assert.True(t, result.Success)
		assert.Greater(t, result.MaxDuration, result.MinDuration)
		assert.GreaterOrEqual(t, result.AverageDuration, result.MinDuration)
		assert.LessOrEqual(t, result.AverageDuration, result.MaxDuration)

		// Should have meaningful operations per second
		assert.Greater(t, result.OperationsPerSec, 0.0)
		assert.Less(t, result.OperationsPerSec, 10000.0) // Sanity check
	})
}
