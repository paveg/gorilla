// Package common provides shared utilities for string representations and type conversions
package common

import (
	"fmt"
	"strings"
)

// StringFormatter provides common string formatting utilities.
type StringFormatter struct{}

// NewStringFormatter creates a new StringFormatter instance.
func NewStringFormatter() *StringFormatter {
	return &StringFormatter{}
}

// FormatFunction formats a function-like string representation
// Pattern: functionName(arg1, arg2, ...)
func (sf *StringFormatter) FormatFunction(name string, args ...string) string {
	if len(args) == 0 {
		return fmt.Sprintf("%s()", name)
	}
	return fmt.Sprintf("%s(%s)", name, strings.Join(args, ", "))
}

// FormatBinaryOperation formats a binary operation string representation
// Pattern: (left operator right).
func (sf *StringFormatter) FormatBinaryOperation(left, operator, right string) string {
	return fmt.Sprintf("(%s %s %s)", left, operator, right)
}

// FormatUnaryOperation formats a unary operation string representation
// Pattern: operator(operand).
func (sf *StringFormatter) FormatUnaryOperation(operator, operand string) string {
	return fmt.Sprintf("%s(%s)", operator, operand)
}

// FormatFieldAccess formats a field access string representation
// Pattern: fieldName(value).
func (sf *StringFormatter) FormatFieldAccess(field string, value interface{}) string {
	return fmt.Sprintf("%s(%v)", field, value)
}

// FormatSQLClause formats a SQL clause string representation
// Pattern: CLAUSE content.
func (sf *StringFormatter) FormatSQLClause(clauseName, content string) string {
	if content == "" {
		return ""
	}
	return fmt.Sprintf("%s %s", strings.ToUpper(clauseName), content)
}

// FormatList formats a list of items with separator.
func (sf *StringFormatter) FormatList(items []string, separator string) string {
	return strings.Join(items, separator)
}

// FormatOptionalClause formats an optional SQL clause.
func (sf *StringFormatter) FormatOptionalClause(clauseName string, content interface{}) string {
	if content == nil {
		return ""
	}

	switch v := content.(type) {
	case string:
		if v == "" {
			return ""
		}
		return sf.FormatSQLClause(clauseName, v)
	case fmt.Stringer:
		str := v.String()
		if str == "" {
			return ""
		}
		return sf.FormatSQLClause(clauseName, str)
	default:
		str := fmt.Sprintf("%v", v)
		if str == "" {
			return ""
		}
		return sf.FormatSQLClause(clauseName, str)
	}
}

// FormatCase formats a case expression string representation.
func (sf *StringFormatter) FormatCase(whens []string, elseValue string) string {
	result := "case"
	for _, when := range whens {
		result += " " + when
	}
	if elseValue != "" {
		result += " else " + elseValue
	}
	result += " end"
	return result
}

// FormatWhen formats a when clause for case expressions.
func (sf *StringFormatter) FormatWhen(condition, value string) string {
	return fmt.Sprintf("when %s then %s", condition, value)
}

// FormatAlias formats a column alias.
func (sf *StringFormatter) FormatAlias(expression, alias string) string {
	if alias == "" {
		return expression
	}
	return fmt.Sprintf("%s AS %s", expression, alias)
}

// FormatJoin formats a join clause.
func (sf *StringFormatter) FormatJoin(joinType, table, condition string) string {
	if condition == "" {
		return fmt.Sprintf("%s JOIN %s", joinType, table)
	}
	return fmt.Sprintf("%s JOIN %s ON %s", joinType, table, condition)
}

// FormatSort formats a sort specification.
func (sf *StringFormatter) FormatSort(column string, ascending bool) string {
	direction := "ASC"
	if !ascending {
		direction = "DESC"
	}
	return fmt.Sprintf("%s %s", column, direction)
}

// EnumStringMap represents a mapping from enum values to string representations.
type EnumStringMap map[int]string

// FormatEnum formats an enum value using the provided mapping.
func (sf *StringFormatter) FormatEnum(value int, mapping EnumStringMap) string {
	if str, exists := mapping[value]; exists {
		return str
	}
	return fmt.Sprintf("unknown(%d)", value)
}

// Default formatter instance for convenience.
var defaultFormatter = NewStringFormatter()

// Convenient functions using the default formatter

// FormatFunction formats a function-like string representation using the default formatter.
func FormatFunction(name string, args ...string) string {
	return defaultFormatter.FormatFunction(name, args...)
}

// FormatBinaryOperation formats a binary operation using the default formatter.
func FormatBinaryOperation(left, operator, right string) string {
	return defaultFormatter.FormatBinaryOperation(left, operator, right)
}

// FormatUnaryOperation formats a unary operation using the default formatter.
func FormatUnaryOperation(operator, operand string) string {
	return defaultFormatter.FormatUnaryOperation(operator, operand)
}

// FormatFieldAccess formats a field access using the default formatter.
func FormatFieldAccess(field string, value interface{}) string {
	return defaultFormatter.FormatFieldAccess(field, value)
}

// FormatSQLClause formats a SQL clause using the default formatter.
func FormatSQLClause(clauseName, content string) string {
	return defaultFormatter.FormatSQLClause(clauseName, content)
}

// FormatOptionalClause formats an optional SQL clause using the default formatter.
func FormatOptionalClause(clauseName string, content interface{}) string {
	return defaultFormatter.FormatOptionalClause(clauseName, content)
}

// FormatAlias formats a column alias using the default formatter.
func FormatAlias(expression, alias string) string {
	return defaultFormatter.FormatAlias(expression, alias)
}

// FormatEnum formats an enum value using the default formatter.
func FormatEnum(value int, mapping EnumStringMap) string {
	return defaultFormatter.FormatEnum(value, mapping)
}
