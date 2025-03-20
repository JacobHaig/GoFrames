package series

import (
	"fmt"
	"reflect"
	"slices"
)

type Float64Series struct {
	name   string
	values []float64
}

// Implementation for Float64Series
func NewFloat64Series(name string, values []float64) *Float64Series {
	return &Float64Series{name: name, values: values}
}

func (s *Float64Series) Name() string { return s.name }
func (s *Float64Series) Rename(newName string) SeriesInterface {
	s.name = newName
	return s
}
func (s *Float64Series) Type() reflect.Type { return reflect.TypeOf(0.0) }
func (s *Float64Series) Get(index int) any  { return s.values[index] }
func (s *Float64Series) Len() int           { return len(s.values) }

func (s *Float64Series) Values() []any {
	result := make([]any, len(s.values))
	for i, v := range s.values {
		result[i] = v
	}
	return result
}

func (s *Float64Series) Copy(deep bool) SeriesInterface {
	if deep {
		newValues := make([]float64, len(s.values))
		copy(newValues, s.values)
		return NewFloat64Series(s.name, newValues)
	}
	return NewFloat64Series(s.name, s.values)
}

func (s *Float64Series) DropRow(index int) SeriesInterface {
	if index < 0 || index >= len(s.values) {
		return s
	}
	s.values = slices.Delete(s.values, index, index+1)
	return s
}

func (s *Float64Series) DropRows(indexes ...int) SeriesInterface {
	slices.Sort(indexes)
	slices.Reverse(indexes)
	for _, i := range indexes {
		if i >= 0 && i < len(s.values) {
			s.DropRow(i)
		}
	}
	return s
}

func (s *Float64Series) ToGenericSeries() *GenericSeries {
	values := make([]any, len(s.values))
	for i, v := range s.values {
		values[i] = v
	}
	return NewGenericSeries(s.name, values)
}

func (s *Float64Series) AsType(valueType string) SeriesInterface {
	switch valueType {
	case "int":
		values := make([]int, len(s.values))
		for i, v := range s.values {
			values[i] = int(v)
		}
		return NewIntSeries(s.name, values)
	case "float", "float64":
		return s
	case "string":
		values := make([]string, len(s.values))
		for i, v := range s.values {
			values[i] = fmt.Sprint(v)
		}
		return NewStringSeries(s.name, values)
	case "bool":
		values := make([]bool, len(s.values))
		for i, v := range s.values {
			values[i] = v != 0
		}
		return NewBoolSeries(s.name, values)
	default:
		// Fall back to generic series for unsupported types
		return s.ToGenericSeries().AsType(valueType)
	}
}
