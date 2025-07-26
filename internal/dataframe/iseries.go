package dataframe

import (
	"github.com/apache/arrow-go/v18/arrow"
)

// ISeries provides a type-erased interface for Series of any type
type ISeries interface {
	Name() string
	Len() int
	DataType() arrow.DataType
	IsNull(index int) bool
	String() string
	Array() arrow.Array
	Release()
	GetAsString(index int) string
}
