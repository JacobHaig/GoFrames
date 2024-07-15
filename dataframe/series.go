package dataframe

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"time"
)

type Series struct {
	Name   string
	Values []any
	Type   reflect.Type
}

type Series2 struct {
	Name   string
	Values any // Contains a slice of strings, ints, floats, etc.
	Type   reflect.Type
}

// NewSeries returns a new Series.
//
// This should be used to create a new Series over the Series struct.
func NewSeries(name string, values []any) *Series {
	if len(values) == 0 {
		return &Series{name, values, nil}
	}

	realType := parseType(values[0])

	for _, value := range values {
		if parseType(value) != realType {
			realType = nil
			break
		}
	}

	return &Series{name, values, realType}
}

func NewSeriesWithType(name string, values []any, valueType string) *Series {
	series := NewSeries(name, values)
	series.AsType(valueType)
	return series
}

func parseType(value any) reflect.Type {
	return reflect.TypeOf(value)
}

func (s *Series) Rename(newName string) *Series {
	s.Name = newName
	return s
}

func (s *Series) AsType(valueType string) *Series {
	// Todo: Decide if we should panic or return an error if
	// the conversion fails. Currently, we are just printing the error
	// and returning nil which is not a good practice.

	// Convert the values to the new type.
	for i := range s.Values {
		value, err := convertValue(s.Values[i], valueType)
		// fmt.Printf("Underlying Type: %T\n", value)

		if err != nil {
			fmt.Println(err)
			return nil
		}

		s.Values[i] = value
	}

	// Set the new type.
	s.Type = parseType(s.Values[0])

	return s
}

func convertToType(value any, newType string) any {
	switch newType {
	case "int":
		i, err := strconv.Atoi(value.(string))
		if err != nil {
			fmt.Println(err)
			return nil
		}
		return i
	case "int8":
		return int8(value.(int8))
	case "int16":
		return int16(value.(int16))
	case "int32":
		return int32(value.(int32))
	case "int64":
		return int64(value.(int64))
	case "float":
		return float64(value.(float64))
	case "float32":
		return float32(value.(float32))
	case "float64":
		return float64(value.(float64))
	case "string":
		return string(value.(string))
	case "rune":
		return rune(value.(rune))
	case "byte":
		return byte(value.(byte))
	case "bool":
		return bool(value.(bool))
	case "time":
		return time.Time(value.(time.Time))
	case "datetime":
		return time.Time(value.(time.Time))
	default:
		fmt.Println("Unknown type: ", newType)
		return nil
	}
}

// InferType detects the type used in the Series.
//
// Returns the string label of the series type and sets it to the Series.Type field.
func (s *Series) InferType() reflect.Type {
	s.Type = parseType(s.Values[0])

	for _, value := range s.Values {
		if parseType(value) != s.Type {
			s.Type = nil
			break
		}
	}

	return s.Type
}

func (s *Series) Get(index int) any {
	return s.Values[index]
}

// Copy returns a new Series with the same values as the original Series.
//
// If deep is set to true, the function will create a deep copy of the Series.
func (s *Series) Copy(deep bool) *Series {
	if deep {
		newValues := make([]any, len(s.Values))
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
