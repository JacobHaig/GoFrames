package dataframe

import (
	"fmt"
	"reflect"
	"slices"
	"time"
)

type Series struct {
	Name   string
	Values []interface{}
	Type   reflect.Type
}

// NewSeries returns a new Series.
//
// This should be used to create a new Series over the Series struct.
func NewSeries(name string, values []interface{}) *Series {
	realType := parseType(values[0])

	for _, value := range values {
		if parseType(value) != realType {
			realType = nil
			break
		}
	}

	return &Series{name, values, realType}
}

func NewSeriesWithType(name string, values []interface{}, valuesType string) *Series {
	realType := checkGivenType(valuesType)
	fmt.Println("Real type: ", realType)

	return &Series{name, values, realType}
}

func (s *Series) Rename(newName string) *Series {
	s.Name = newName
	return s
}

func checkGivenType(valueType string) reflect.Type {
	switch valueType {
	case "int":
		return reflect.TypeOf(0)
	case "int8":
		return reflect.TypeOf(int8(0))
	case "int16":
		return reflect.TypeOf(int16(0))
	case "int32":
		return reflect.TypeOf(int32(0))
	case "int64":
		return reflect.TypeOf(int64(0))
	case "float":
		return reflect.TypeOf(0.0)
	case "float32":
		return reflect.TypeOf(float32(0.0))
	case "float64":
		return reflect.TypeOf(float64(0.0))
	case "string":
		return reflect.TypeOf("")
	case "rune":
		return reflect.TypeOf(' ')
	case "byte":
		return reflect.TypeOf(byte(0))
	case "bool":
		return reflect.TypeOf(false)
	case "time":
		return reflect.TypeOf(time.Time{})
	case "datetime":
		return reflect.TypeOf(time.Time{})
	default:
		fmt.Println("Unknown type: ", valueType)
		return reflect.TypeOf(0)
	}
}

func parseType(value interface{}) reflect.Type {
	return reflect.TypeOf(value)
}

// Copy returns a new Series with the same values as the original Series.
//
// If deep is set to true, the function will create a deep copy of the Series.
func (s *Series) Copy(deep bool) *Series {
	if deep {
		newValues := make([]interface{}, len(s.Values))
		copy(newValues, s.Values)
		return NewSeries(s.Name, newValues)
	}
	return NewSeries(s.Name, s.Values)
}

func (s *Series) Len() int {
	return len(s.Values)
}

func (s *Series) DropRow(index int) *Series {
	s.Values = slices.Replace(s.Values, index, index+1)
	return s
}

func (s *Series) DropRows(indexes ...int) *Series {
	// Sort the indexes in reverse order.
	slices.Sort(indexes)
	slices.Reverse(indexes)

	for i := range indexes {
		s.DropRow(indexes[i])
	}
	return s
}
