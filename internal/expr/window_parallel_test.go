package expr_test

import (
	"testing"

	"github.com/paveg/gorilla/internal/expr"
	"github.com/stretchr/testify/assert"
)

// Tests for window parallel functionality
// Many of the original tests require access to internal methods
// and cannot be run from the expr_test package

func TestWindowParallelConfig(t *testing.T) {
	config := expr.DefaultWindowParallelConfig()

	assert.Equal(t, 4, config.MinPartitionsForParallel)
	assert.Equal(t, 1000, config.MinRowsForParallelSort)
	assert.True(t, config.AdaptiveParallelization)
	assert.Positive(t, config.MaxWorkers)
}

// The following tests require access to internal methods and are skipped
