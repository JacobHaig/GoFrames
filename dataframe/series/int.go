package series

import (
	"fmt"
	"reflect"
	"slices"
)

type IntSeries struct {
	name   string
	values []int
}

// Implementation for IntSeries
func NewIntSeries(name string, values []int) *IntSeries {
	return &IntSeries{name: name, values: values}
}

func (s *IntSeries) Name() string { return s.name }
func (s *IntSeries) Rename(newName string) SeriesInterface {
	s.name = newName
	return s
}
func (s *IntSeries) Type() reflect.Type { return reflect.TypeOf(0) }
func (s *IntSeries) Get(index int) any  { return s.values[index] }
func (s *IntSeries) Len() int           { return len(s.values) }

func (s *IntSeries) Values() []any {
	result := make([]any, len(s.values))
	for i, v := range s.values {
		result[i] = v
	}
	return result
}

func (s *IntSeries) Copy(deep bool) SeriesInterface {
	if deep {
		newValues := make([]int, len(s.values))
		copy(newValues, s.values)
		return NewIntSeries(s.name, newValues)
	}
	return NewIntSeries(s.name, s.values)
}

func (s *IntSeries) DropRow(index int) SeriesInterface {
	if index < 0 || index >= len(s.values) {
		return s
	}
	s.values = slices.Delete(s.values, index, index+1)
	return s
}

func (s *IntSeries) DropRows(indexes ...int) SeriesInterface {
	slices.Sort(indexes)
	slices.Reverse(indexes)
	for _, i := range indexes {
		if i >= 0 && i < len(s.values) {
			s.DropRow(i)
		}
	}
	return s
}

func (s *IntSeries) ToGenericSeries() *GenericSeries {
	values := make([]any, len(s.values))
	for i, v := range s.values {
		values[i] = v
	}
	return NewGenericSeries(s.name, values)
}

func (s *IntSeries) AsType(valueType string) SeriesInterface {
	switch valueType {
	case "int":
		return s
	case "float", "float64":
		values := make([]float64, len(s.values))
		for i, v := range s.values {
			values[i] = float64(v)
		}
		return NewFloat64Series(s.name, values)
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
