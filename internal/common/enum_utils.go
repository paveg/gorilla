package common

import (
	"fmt"
	"strings"
)

// EnumRegistry provides utilities for managing enum string representations.
type EnumRegistry struct {
	mappings map[string]EnumStringMap
}

// NewEnumRegistry creates a new EnumRegistry instance.
func NewEnumRegistry() *EnumRegistry {
	return &EnumRegistry{
		mappings: make(map[string]EnumStringMap),
	}
}

// RegisterEnum registers an enum type with its string mapping.
func (er *EnumRegistry) RegisterEnum(typeName string, mapping EnumStringMap) {
	er.mappings[typeName] = mapping
}

// FormatEnum formats an enum value for a registered type.
func (er *EnumRegistry) FormatEnum(typeName string, value int) string {
	if mapping, exists := er.mappings[typeName]; exists {
		if str, found := mapping[value]; found {
			return str
		}
	}
	return fmt.Sprintf("unknown_%s(%d)", typeName, value)
}

// GetEnumMapping returns the mapping for a registered enum type.
func (er *EnumRegistry) GetEnumMapping(typeName string) (EnumStringMap, bool) {
	mapping, exists := er.mappings[typeName]
	return mapping, exists
}

// Common enum mappings used throughout the codebase

// BinaryOperatorMapping maps binary operators to their string representations.
var BinaryOperatorMapping = EnumStringMap{
	0:  "+",  // OpAdd
	1:  "-",  // OpSub
	2:  "*",  // OpMul
	3:  "/",  // OpDiv
	4:  "==", // OpEq
	5:  "!=", // OpNe
	6:  "<",  // OpLt
	7:  "<=", // OpLe
	8:  ">",  // OpGt
	9:  ">=", // OpGe
	10: "&&", // OpAnd
	11: "||", // OpOr
}

// UnaryOperatorMapping maps unary operators to their string representations.
var UnaryOperatorMapping = EnumStringMap{
	0: "-", // UnaryNeg
	1: "!", // UnaryNot
}

// AggregationTypeMapping maps aggregation types to their string representations.
var AggregationTypeMapping = EnumStringMap{
	0: "sum",   // AggSum
	1: "count", // AggCount
	2: "mean",  // AggMean
	3: "min",   // AggMin
	4: "max",   // AggMax
}

// JoinTypeMapping maps join types to their string representations.
var JoinTypeMapping = EnumStringMap{
	0: "INNER", // InnerJoin
	1: "LEFT",  // LeftJoin
	2: "RIGHT", // RightJoin
	3: "FULL",  // FullJoin
}

// OrderDirectionMapping maps order directions to their string representations.
var OrderDirectionMapping = EnumStringMap{
	0: "ASC",  // Ascending
	1: "DESC", // Descending
}

// IntervalTypeMapping maps interval types to their string representations.
var IntervalTypeMapping = EnumStringMap{
	0: "days",    // IntervalDays
	1: "hours",   // IntervalHours
	2: "minutes", // IntervalMinutes
	3: "months",  // IntervalMonths
	4: "years",   // IntervalYears
}

// EvaluationContextMapping maps evaluation contexts to their string representations.
var EvaluationContextMapping = EnumStringMap{
	0: "RowContext",   // RowContext
	1: "GroupContext", // GroupContext
}

// Default enum registry with common mappings.
var defaultEnumRegistry = func() *EnumRegistry {
	registry := NewEnumRegistry()
	registry.RegisterEnum("BinaryOperator", BinaryOperatorMapping)
	registry.RegisterEnum("UnaryOperator", UnaryOperatorMapping)
	registry.RegisterEnum("AggregationType", AggregationTypeMapping)
	registry.RegisterEnum("JoinType", JoinTypeMapping)
	registry.RegisterEnum("OrderDirection", OrderDirectionMapping)
	registry.RegisterEnum("IntervalType", IntervalTypeMapping)
	registry.RegisterEnum("EvaluationContext", EvaluationContextMapping)
	return registry
}()

// FormatBinaryOperator formats a binary operator enum value.
func FormatBinaryOperator(op int) string {
	return FormatEnum(op, BinaryOperatorMapping)
}

// FormatUnaryOperator formats a unary operator enum value.
func FormatUnaryOperator(op int) string {
	return FormatEnum(op, UnaryOperatorMapping)
}

// FormatAggregationType formats an aggregation type enum value.
func FormatAggregationType(aggType int) string {
	return FormatEnum(aggType, AggregationTypeMapping)
}

// FormatJoinType formats a join type enum value.
func FormatJoinType(joinType int) string {
	return FormatEnum(joinType, JoinTypeMapping)
}

// FormatOrderDirection formats an order direction enum value.
func FormatOrderDirection(direction int) string {
	return FormatEnum(direction, OrderDirectionMapping)
}

// FormatIntervalType formats an interval type enum value.
func FormatIntervalType(intervalType int) string {
	return FormatEnum(intervalType, IntervalTypeMapping)
}

// FormatEvaluationContext formats an evaluation context enum value.
func FormatEvaluationContext(context int) string {
	return FormatEnum(context, EvaluationContextMapping)
}

// StringToEnum provides utilities for parsing enum values from strings.
type StringToEnum struct {
	reverseMappings map[string]map[string]int
}

// NewStringToEnum creates a new StringToEnum instance.
func NewStringToEnum() *StringToEnum {
	return &StringToEnum{
		reverseMappings: make(map[string]map[string]int),
	}
}

// RegisterReverseMapping registers a reverse mapping for an enum type.
func (ste *StringToEnum) RegisterReverseMapping(typeName string, mapping EnumStringMap) {
	reverseMap := make(map[string]int)
	for value, str := range mapping {
		reverseMap[strings.ToUpper(str)] = value
		reverseMap[strings.ToLower(str)] = value
		reverseMap[str] = value
	}
	ste.reverseMappings[typeName] = reverseMap
}

// ParseEnum parses a string to its enum value.
func (ste *StringToEnum) ParseEnum(typeName, str string) (int, bool) {
	if reverseMap, exists := ste.reverseMappings[typeName]; exists {
		if value, found := reverseMap[str]; found {
			return value, true
		}
		if value, found := reverseMap[strings.ToUpper(str)]; found {
			return value, true
		}
		if value, found := reverseMap[strings.ToLower(str)]; found {
			return value, true
		}
	}
	return 0, false
}

// Default string-to-enum converter with common mappings.
var defaultStringToEnum = func() *StringToEnum {
	converter := NewStringToEnum()
	converter.RegisterReverseMapping("BinaryOperator", BinaryOperatorMapping)
	converter.RegisterReverseMapping("UnaryOperator", UnaryOperatorMapping)
	converter.RegisterReverseMapping("AggregationType", AggregationTypeMapping)
	converter.RegisterReverseMapping("JoinType", JoinTypeMapping)
	converter.RegisterReverseMapping("OrderDirection", OrderDirectionMapping)
	converter.RegisterReverseMapping("IntervalType", IntervalTypeMapping)
	converter.RegisterReverseMapping("EvaluationContext", EvaluationContextMapping)
	return converter
}()

// ParseBinaryOperator parses a binary operator string.
func ParseBinaryOperator(str string) (int, bool) {
	return defaultStringToEnum.ParseEnum("BinaryOperator", str)
}

// ParseUnaryOperator parses a unary operator string.
func ParseUnaryOperator(str string) (int, bool) {
	return defaultStringToEnum.ParseEnum("UnaryOperator", str)
}

// ParseAggregationType parses an aggregation type string.
func ParseAggregationType(str string) (int, bool) {
	return defaultStringToEnum.ParseEnum("AggregationType", str)
}

// ParseJoinType parses a join type string.
func ParseJoinType(str string) (int, bool) {
	return defaultStringToEnum.ParseEnum("JoinType", str)
}

// ParseOrderDirection parses an order direction string.
func ParseOrderDirection(str string) (int, bool) {
	return defaultStringToEnum.ParseEnum("OrderDirection", str)
}

// ParseIntervalType parses an interval type string.
func ParseIntervalType(str string) (int, bool) {
	return defaultStringToEnum.ParseEnum("IntervalType", str)
}

// ParseEvaluationContext parses an evaluation context string.
func ParseEvaluationContext(str string) (int, bool) {
	return defaultStringToEnum.ParseEnum("EvaluationContext", str)
}
