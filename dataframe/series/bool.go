package series

import (
	"fmt"
	"reflect"
	"slices"
)

type BoolSeries struct {
	name   string
	values []bool
}

// Implementation for BoolSeries
func NewBoolSeries(name string, values []bool) *BoolSeries {
	return &BoolSeries{name: name, values: values}
}

func (s *BoolSeries) Name() string { return s.name }
func (s *BoolSeries) Rename(newName string) SeriesInterface {
	s.name = newName
	return s
}
func (s *BoolSeries) Type() reflect.Type { return reflect.TypeOf(true) }
func (s *BoolSeries) Get(index int) any  { return s.values[index] }
func (s *BoolSeries) Len() int           { return len(s.values) }

func (s *BoolSeries) Values() []any {
	result := make([]any, len(s.values))
	for i, v := range s.values {
		result[i] = v
	}
	return result
}

func (s *BoolSeries) Copy(deep bool) SeriesInterface {
	if deep {
		newValues := make([]bool, len(s.values))
		copy(newValues, s.values)
		return NewBoolSeries(s.name, newValues)
	}
	return NewBoolSeries(s.name, s.values)
}

func (s *BoolSeries) DropRow(index int) SeriesInterface {
	if index < 0 || index >= len(s.values) {
		return s
	}
	s.values = slices.Delete(s.values, index, index+1)
	return s
}

func (s *BoolSeries) DropRows(indexes ...int) SeriesInterface {
	slices.Sort(indexes)
	slices.Reverse(indexes)
	for _, i := range indexes {
		if i >= 0 && i < len(s.values) {
			s.DropRow(i)
		}
	}
	return s
}

func (s *BoolSeries) ToGenericSeries() *GenericSeries {
	values := make([]any, len(s.values))
	for i, v := range s.values {
		values[i] = v
	}
	return NewGenericSeries(s.name, values)
}

func (s *BoolSeries) AsType(valueType string) SeriesInterface {
	switch valueType {
	case "int":
		values := make([]int, len(s.values))
		for i, v := range s.values {
			if v {
				values[i] = 1
			} else {
				values[i] = 0
			}
		}
		return NewIntSeries(s.name, values)
	case "float", "float64":
		values := make([]float64, len(s.values))
		for i, v := range s.values {
			if v {
				values[i] = 1.0
			} else {
				values[i] = 0.0
			}
		}
		return NewFloat64Series(s.name, values)
	case "string":
		values := make([]string, len(s.values))
		for i, v := range s.values {
			values[i] = fmt.Sprint(v)
		}
		return NewStringSeries(s.name, values)
	case "bool":
		return s
	default:
		// Fall back to generic series for unsupported types
		return s.ToGenericSeries().AsType(valueType)
	}
}
