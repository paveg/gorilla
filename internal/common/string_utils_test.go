package common_test

import (
	"testing"

	"github.com/paveg/gorilla/internal/common"
	"github.com/stretchr/testify/assert"
)

func TestStringFormatter(t *testing.T) {
	formatter := common.NewStringFormatter()

	t.Run("FormatFunction", func(t *testing.T) {
		assert.Equal(t, "func()", formatter.FormatFunction("func"))
		assert.Equal(t, "func(arg1)", formatter.FormatFunction("func", "arg1"))
		assert.Equal(t, "func(arg1, arg2)", formatter.FormatFunction("func", "arg1", "arg2"))
	})

	t.Run("FormatBinaryOperation", func(t *testing.T) {
		assert.Equal(t, "(a + b)", formatter.FormatBinaryOperation("a", "+", "b"))
		assert.Equal(t, "(x == y)", formatter.FormatBinaryOperation("x", "==", "y"))
	})

	t.Run("FormatUnaryOperation", func(t *testing.T) {
		assert.Equal(t, "!(expr)", formatter.FormatUnaryOperation("!", "expr"))
		assert.Equal(t, "-(value)", formatter.FormatUnaryOperation("-", "value"))
	})

	t.Run("FormatFieldAccess", func(t *testing.T) {
		assert.Equal(t, "field(42)", formatter.FormatFieldAccess("field", 42))
		assert.Equal(t, "name(test)", formatter.FormatFieldAccess("name", "test"))
	})

	t.Run("FormatSQLClause", func(t *testing.T) {
		assert.Equal(t, "WHERE condition", formatter.FormatSQLClause("where", "condition"))
		assert.Equal(t, "SELECT *", formatter.FormatSQLClause("select", "*"))
		assert.Empty(t, formatter.FormatSQLClause("where", ""))
	})

	t.Run("FormatOptionalClause", func(t *testing.T) {
		assert.Empty(t, formatter.FormatOptionalClause("where", nil))
		assert.Empty(t, formatter.FormatOptionalClause("where", ""))
		assert.Equal(t, "WHERE condition", formatter.FormatOptionalClause("where", "condition"))
	})

	t.Run("FormatCase", func(t *testing.T) {
		whens := []string{"when x > 0 then positive", "when x < 0 then negative"}
		expected := "case when x > 0 then positive when x < 0 then negative else zero end"
		assert.Equal(t, expected, formatter.FormatCase(whens, "zero"))
	})

	t.Run("FormatWhen", func(t *testing.T) {
		assert.Equal(t, "when x > 0 then positive", formatter.FormatWhen("x > 0", "positive"))
	})

	t.Run("FormatAlias", func(t *testing.T) {
		assert.Equal(t, "column", formatter.FormatAlias("column", ""))
		assert.Equal(t, "column AS alias", formatter.FormatAlias("column", "alias"))
	})

	t.Run("FormatJoin", func(t *testing.T) {
		assert.Equal(t, "INNER JOIN table", formatter.FormatJoin("INNER", "table", ""))
		assert.Equal(t, "LEFT JOIN table ON condition", formatter.FormatJoin("LEFT", "table", "condition"))
	})

	t.Run("FormatSort", func(t *testing.T) {
		assert.Equal(t, "column ASC", formatter.FormatSort("column", true))
		assert.Equal(t, "column DESC", formatter.FormatSort("column", false))
	})

	t.Run("FormatEnum", func(t *testing.T) {
		mapping := common.EnumStringMap{0: "zero", 1: "one", 2: "two"}
		assert.Equal(t, "zero", formatter.FormatEnum(0, mapping))
		assert.Equal(t, "one", formatter.FormatEnum(1, mapping))
		assert.Equal(t, "unknown(99)", formatter.FormatEnum(99, mapping))
	})
}

func TestDefaultFormatterFunctions(t *testing.T) {
	t.Run("FormatFunction", func(t *testing.T) {
		assert.Equal(t, "test()", common.FormatFunction("test"))
		assert.Equal(t, "test(arg)", common.FormatFunction("test", "arg"))
	})

	t.Run("FormatBinaryOperation", func(t *testing.T) {
		assert.Equal(t, "(a + b)", common.FormatBinaryOperation("a", "+", "b"))
	})

	t.Run("FormatUnaryOperation", func(t *testing.T) {
		assert.Equal(t, "!(x)", common.FormatUnaryOperation("!", "x"))
	})

	t.Run("FormatFieldAccess", func(t *testing.T) {
		assert.Equal(t, "field(value)", common.FormatFieldAccess("field", "value"))
	})

	t.Run("FormatSQLClause", func(t *testing.T) {
		assert.Equal(t, "SELECT *", common.FormatSQLClause("select", "*"))
	})

	t.Run("FormatOptionalClause", func(t *testing.T) {
		assert.Equal(t, "WHERE x > 0", common.FormatOptionalClause("where", "x > 0"))
		assert.Empty(t, common.FormatOptionalClause("where", nil))
	})

	t.Run("FormatAlias", func(t *testing.T) {
		assert.Equal(t, "col AS alias", common.FormatAlias("col", "alias"))
	})

	t.Run("FormatEnum", func(t *testing.T) {
		mapping := common.EnumStringMap{0: "first", 1: "second"}
		assert.Equal(t, "first", common.FormatEnum(0, mapping))
	})
}
