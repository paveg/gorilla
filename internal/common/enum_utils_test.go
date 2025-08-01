package common_test

import (
	"testing"

	"github.com/paveg/gorilla/internal/common"
	"github.com/stretchr/testify/assert"
)

func TestEnumRegistry(t *testing.T) {
	registry := common.NewEnumRegistry()

	testMapping := common.EnumStringMap{
		0: "zero",
		1: "one",
		2: "two",
	}

	t.Run("RegisterEnum and FormatEnum", func(t *testing.T) {
		registry.RegisterEnum("TestEnum", testMapping)

		assert.Equal(t, "zero", registry.FormatEnum("TestEnum", 0))
		assert.Equal(t, "one", registry.FormatEnum("TestEnum", 1))
		assert.Equal(t, "two", registry.FormatEnum("TestEnum", 2))
		assert.Equal(t, "unknown_TestEnum(99)", registry.FormatEnum("TestEnum", 99))
	})

	t.Run("GetEnumMapping", func(t *testing.T) {
		registry.RegisterEnum("TestEnum", testMapping)

		mapping, exists := registry.GetEnumMapping("TestEnum")
		assert.True(t, exists)
		assert.Equal(t, testMapping, mapping)

		_, exists = registry.GetEnumMapping("NonExistent")
		assert.False(t, exists)
	})
}

func TestCommonEnumMappings(t *testing.T) {
	t.Run("BinaryOperatorMapping", func(t *testing.T) {
		assert.Equal(t, "+", common.BinaryOperatorMapping[0])
		assert.Equal(t, "-", common.BinaryOperatorMapping[1])
		assert.Equal(t, "==", common.BinaryOperatorMapping[4])
		assert.Equal(t, "&&", common.BinaryOperatorMapping[10])
	})

	t.Run("UnaryOperatorMapping", func(t *testing.T) {
		assert.Equal(t, "-", common.UnaryOperatorMapping[0])
		assert.Equal(t, "!", common.UnaryOperatorMapping[1])
	})

	t.Run("AggregationTypeMapping", func(t *testing.T) {
		assert.Equal(t, "sum", common.AggregationTypeMapping[0])
		assert.Equal(t, "count", common.AggregationTypeMapping[1])
		assert.Equal(t, "mean", common.AggregationTypeMapping[2])
	})

	t.Run("JoinTypeMapping", func(t *testing.T) {
		assert.Equal(t, "INNER", common.JoinTypeMapping[0])
		assert.Equal(t, "LEFT", common.JoinTypeMapping[1])
		assert.Equal(t, "RIGHT", common.JoinTypeMapping[2])
		assert.Equal(t, "FULL", common.JoinTypeMapping[3])
	})

	t.Run("OrderDirectionMapping", func(t *testing.T) {
		assert.Equal(t, "ASC", common.OrderDirectionMapping[0])
		assert.Equal(t, "DESC", common.OrderDirectionMapping[1])
	})

	t.Run("IntervalTypeMapping", func(t *testing.T) {
		assert.Equal(t, "days", common.IntervalTypeMapping[0])
		assert.Equal(t, "hours", common.IntervalTypeMapping[1])
		assert.Equal(t, "minutes", common.IntervalTypeMapping[2])
		assert.Equal(t, "months", common.IntervalTypeMapping[3])
		assert.Equal(t, "years", common.IntervalTypeMapping[4])
	})

	t.Run("EvaluationContextMapping", func(t *testing.T) {
		assert.Equal(t, "RowContext", common.EvaluationContextMapping[0])
		assert.Equal(t, "GroupContext", common.EvaluationContextMapping[1])
	})
}

func TestFormatFunctions(t *testing.T) {
	t.Run("FormatBinaryOperator", func(t *testing.T) {
		assert.Equal(t, "+", common.FormatBinaryOperator(0))
		assert.Equal(t, "==", common.FormatBinaryOperator(4))
		assert.Equal(t, "unknown(99)", common.FormatBinaryOperator(99))
	})

	t.Run("FormatUnaryOperator", func(t *testing.T) {
		assert.Equal(t, "-", common.FormatUnaryOperator(0))
		assert.Equal(t, "!", common.FormatUnaryOperator(1))
	})

	t.Run("FormatAggregationType", func(t *testing.T) {
		assert.Equal(t, "sum", common.FormatAggregationType(0))
		assert.Equal(t, "count", common.FormatAggregationType(1))
	})

	t.Run("FormatJoinType", func(t *testing.T) {
		assert.Equal(t, "INNER", common.FormatJoinType(0))
		assert.Equal(t, "LEFT", common.FormatJoinType(1))
	})

	t.Run("FormatOrderDirection", func(t *testing.T) {
		assert.Equal(t, "ASC", common.FormatOrderDirection(0))
		assert.Equal(t, "DESC", common.FormatOrderDirection(1))
	})

	t.Run("FormatIntervalType", func(t *testing.T) {
		assert.Equal(t, "days", common.FormatIntervalType(0))
		assert.Equal(t, "hours", common.FormatIntervalType(1))
	})

	t.Run("FormatEvaluationContext", func(t *testing.T) {
		assert.Equal(t, "RowContext", common.FormatEvaluationContext(0))
		assert.Equal(t, "GroupContext", common.FormatEvaluationContext(1))
	})
}

func TestStringToEnum(t *testing.T) {
	converter := common.NewStringToEnum()

	testMapping := common.EnumStringMap{
		0: "zero",
		1: "one",
		2: "two",
	}

	t.Run("RegisterReverseMapping and ParseEnum", func(t *testing.T) {
		converter.RegisterReverseMapping("TestEnum", testMapping)

		// Test exact matches
		value, found := converter.ParseEnum("TestEnum", "zero")
		assert.True(t, found)
		assert.Equal(t, 0, value)

		value, found = converter.ParseEnum("TestEnum", "one")
		assert.True(t, found)
		assert.Equal(t, 1, value)

		// Test case insensitive matches
		value, found = converter.ParseEnum("TestEnum", "ZERO")
		assert.True(t, found)
		assert.Equal(t, 0, value)

		value, found = converter.ParseEnum("TestEnum", "One")
		assert.True(t, found)
		assert.Equal(t, 1, value)

		// Test not found
		_, found = converter.ParseEnum("TestEnum", "three")
		assert.False(t, found)

		// Test non-existent type
		_, found = converter.ParseEnum("NonExistent", "zero")
		assert.False(t, found)
	})
}

func TestParseFunctions(t *testing.T) {
	t.Run("ParseBinaryOperator", func(t *testing.T) {
		value, found := common.ParseBinaryOperator("+")
		assert.True(t, found)
		assert.Equal(t, 0, value)

		value, found = common.ParseBinaryOperator("==")
		assert.True(t, found)
		assert.Equal(t, 4, value)

		_, found = common.ParseBinaryOperator("invalid")
		assert.False(t, found)
	})

	t.Run("ParseUnaryOperator", func(t *testing.T) {
		value, found := common.ParseUnaryOperator("-")
		assert.True(t, found)
		assert.Equal(t, 0, value)

		value, found = common.ParseUnaryOperator("!")
		assert.True(t, found)
		assert.Equal(t, 1, value)
	})

	t.Run("ParseAggregationType", func(t *testing.T) {
		value, found := common.ParseAggregationType("sum")
		assert.True(t, found)
		assert.Equal(t, 0, value)

		value, found = common.ParseAggregationType("COUNT")
		assert.True(t, found)
		assert.Equal(t, 1, value)
	})

	t.Run("ParseJoinType", func(t *testing.T) {
		value, found := common.ParseJoinType("INNER")
		assert.True(t, found)
		assert.Equal(t, 0, value)

		value, found = common.ParseJoinType("left")
		assert.True(t, found)
		assert.Equal(t, 1, value)
	})

	t.Run("ParseOrderDirection", func(t *testing.T) {
		value, found := common.ParseOrderDirection("ASC")
		assert.True(t, found)
		assert.Equal(t, 0, value)

		value, found = common.ParseOrderDirection("desc")
		assert.True(t, found)
		assert.Equal(t, 1, value)
	})

	t.Run("ParseIntervalType", func(t *testing.T) {
		value, found := common.ParseIntervalType("days")
		assert.True(t, found)
		assert.Equal(t, 0, value)

		value, found = common.ParseIntervalType("HOURS")
		assert.True(t, found)
		assert.Equal(t, 1, value)
	})

	t.Run("ParseEvaluationContext", func(t *testing.T) {
		value, found := common.ParseEvaluationContext("RowContext")
		assert.True(t, found)
		assert.Equal(t, 0, value)

		value, found = common.ParseEvaluationContext("groupcontext")
		assert.True(t, found)
		assert.Equal(t, 1, value)
	})
}
