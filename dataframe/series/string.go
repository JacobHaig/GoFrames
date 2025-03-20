package series

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
)

type StringSeries struct {
	name   string
	values []string
}

// Implementation for StringSeries
func NewStringSeries(name string, values []string) *StringSeries {
	return &StringSeries{name: name, values: values}
}

func (s *StringSeries) Name() string { return s.name }
func (s *StringSeries) Rename(newName string) SeriesInterface {
	s.name = newName
	return s
}
func (s *StringSeries) Type() reflect.Type { return reflect.TypeOf("") }
func (s *StringSeries) Get(index int) any  { return s.values[index] }
func (s *StringSeries) Len() int           { return len(s.values) }

func (s *StringSeries) Values() []any {
	result := make([]any, len(s.values))
	for i, v := range s.values {
		result[i] = v
	}
	return result
}

func (s *StringSeries) Copy(deep bool) SeriesInterface {
	if deep {
		newValues := make([]string, len(s.values))
		copy(newValues, s.values)
		return NewStringSeries(s.name, newValues)
	}
	return NewStringSeries(s.name, s.values)
}

func (s *StringSeries) DropRow(index int) SeriesInterface {
	if index < 0 || index >= len(s.values) {
		return s
	}
	s.values = slices.Delete(s.values, index, index+1)
	return s
}

func (s *StringSeries) DropRows(indexes ...int) SeriesInterface {
	slices.Sort(indexes)
	slices.Reverse(indexes)
	for _, i := range indexes {
		if i >= 0 && i < len(s.values) {
			s.DropRow(i)
		}
	}
	return s
}

func (s *StringSeries) ToGenericSeries() *GenericSeries {
	values := make([]any, len(s.values))
	for i, v := range s.values {
		values[i] = v
	}
	return NewGenericSeries(s.name, values)
}

func (s *StringSeries) AsType(valueType string) SeriesInterface {
	switch valueType {
	case "int":
		values, ok := StringSliceToIntSlice(s.values)
		if !ok {
			fmt.Println("Error converting string values to int")
			return s
		}
		return NewIntSeries(s.name, values)
	case "float", "float64":
		values, ok := StringSliceToFloat64Slice(s.values)
		if !ok {
			fmt.Println("Error converting string values to float64")
			return s
		}
		return NewFloat64Series(s.name, values)
	case "string":
		return s
	case "bool":
		values, ok := StringSliceToBoolSlice(s.values)
		if !ok {
			fmt.Println("Error converting string values to bool")
			return s
		}
		return NewBoolSeries(s.name, values)
	default:
		// Fall back to generic series for unsupported types
		return s.ToGenericSeries().AsType(valueType)
	}
}

// StringSliceToIntSlice converts a slice of strings to a slice of ints
func StringSliceToIntSlice(values []string) ([]int, bool) {
	result := make([]int, len(values))
	for i, v := range values {
		if v == "" {
			result[i] = 0
			continue
		}

		// Try to parse as int
		intVal, err := strconv.Atoi(v)
		if err != nil {
			// Try removing any formatting (commas, etc.)
			cleanVal := strings.ReplaceAll(v, ",", "")
			intVal, err = strconv.Atoi(cleanVal)
			if err != nil {
				return nil, false
			}
		}
		result[i] = intVal
	}
	return result, true
}

// StringSliceToFloat64Slice converts a slice of strings to a slice of float64s
func StringSliceToFloat64Slice(values []string) ([]float64, bool) {
	result := make([]float64, len(values))
	for i, v := range values {
		if v == "" {
			result[i] = 0
			continue
		}

		// Try to parse as float
		floatVal, err := strconv.ParseFloat(v, 64)
		if err != nil {
			// Try removing any formatting (commas, etc.)
			cleanVal := strings.ReplaceAll(v, ",", "")
			floatVal, err = strconv.ParseFloat(cleanVal, 64)
			if err != nil {
				return nil, false
			}
		}
		result[i] = floatVal
	}
	return result, true
}

// StringSliceToBoolSlice converts a slice of strings to a slice of bools
func StringSliceToBoolSlice(values []string) ([]bool, bool) {
	result := make([]bool, len(values))
	for i, v := range values {
		if v == "" {
			result[i] = false
			continue
		}

		// Try to parse as bool
		boolVal, err := strconv.ParseBool(v)
		if err != nil {
			// Check for common boolean representations
			switch strings.ToLower(v) {
			case "yes", "y", "1":
				result[i] = true
			case "no", "n", "0":
				result[i] = false
			default:
				return nil, false
			}
		} else {
			result[i] = boolVal
		}
	}
	return result, true
}
