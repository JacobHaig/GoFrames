package dataframe

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
)

// SeriesInterface defines common operations for all Series types
type SeriesInterface interface {
	// Get the Series name
	Name() string

	// Rename the Series
	Rename(newName string) SeriesInterface

	// Get the Go type of the Series values
	Type() reflect.Type

	// Get the value at the specified index as interface{}
	Get(index int) any

	// Create a copy of the Series
	Copy(deep bool) SeriesInterface

	// Drop a row from the Series
	DropRow(index int) SeriesInterface

	// Drop rows from the Series
	DropRows(indexes ...int) SeriesInterface

	// Get the length of the Series
	Len() int

	// Convert to a generic Series with []any
	ToGenericSeries() *GenericSeries

	// Change the type of values
	AsType(valueType string) SeriesInterface

	// Get all values as a slice of any
	Values() []any
}

// GenericSeries is equivalent to the original Series implementation
type GenericSeries struct {
	name   string
	values []any
	typ    reflect.Type
}

// Create specialized Series implementations for common types
type IntSeries struct {
	name   string
	values []int
}

type Float64Series struct {
	name   string
	values []float64
}

type StringSeries struct {
	name   string
	values []string
}

type BoolSeries struct {
	name   string
	values []bool
}

// Implementation for GenericSeries
func NewGenericSeries(name string, values []any) *GenericSeries {
	var typ reflect.Type
	if len(values) > 0 {
		typ = reflect.TypeOf(values[0])

		// Ensure all values have the same type
		for _, value := range values {
			if value != nil && reflect.TypeOf(value) != typ {
				typ = nil
				break
			}
		}
	}
	return &GenericSeries{name: name, values: values, typ: typ}
}

func (s *GenericSeries) Name() string { return s.name }
func (s *GenericSeries) Rename(newName string) SeriesInterface {
	s.name = newName
	return s
}
func (s *GenericSeries) Type() reflect.Type { return s.typ }
func (s *GenericSeries) Get(index int) any  { return s.values[index] }
func (s *GenericSeries) Len() int           { return len(s.values) }
func (s *GenericSeries) Values() []any      { return s.values }

func (s *GenericSeries) Copy(deep bool) SeriesInterface {
	if deep {
		newValues := make([]any, len(s.values))
		copy(newValues, s.values)
		return NewGenericSeries(s.name, newValues)
	}
	return NewGenericSeries(s.name, s.values)
}

func (s *GenericSeries) DropRow(index int) SeriesInterface {
	if index < 0 || index >= len(s.values) {
		return s
	}
	s.values = slices.Delete(s.values, index, index+1)
	return s
}

func (s *GenericSeries) DropRows(indexes ...int) SeriesInterface {
	slices.Sort(indexes)
	slices.Reverse(indexes)
	for _, i := range indexes {
		if i >= 0 && i < len(s.values) {
			s.DropRow(i)
		}
	}
	return s
}

func (s *GenericSeries) ToGenericSeries() *GenericSeries {
	return s
}

func (s *GenericSeries) AsType(valueType string) SeriesInterface {
	// Try to convert to a specialized series if possible
	switch valueType {
	case "int":
		values, ok := ToIntSlice(s.values)
		if ok {
			return NewIntSeries(s.name, values)
		}
	case "float", "float64":
		values, ok := ToFloat64Slice(s.values)
		if ok {
			return NewFloat64Series(s.name, values)
		}
	case "string":
		values := ToStringSlice(s.values)
		return NewStringSeries(s.name, values)
	case "bool":
		values, ok := ToBoolSlice(s.values)
		if ok {
			return NewBoolSeries(s.name, values)
		}
	}

	// Fall back to converting each value individually
	for i := range s.values {
		value, err := convertValue(s.values[i], valueType)
		if err != nil {
			fmt.Printf("Error converting value to type %s: %v\n", valueType, err)
			return s
		}
		s.values[i] = value
	}

	if len(s.values) > 0 {
		s.typ = reflect.TypeOf(s.values[0])
	}

	return s
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

// Factory function to create the appropriate Series type based on input data
func NewSeries(name string, values []any) SeriesInterface {
	if len(values) == 0 {
		return NewGenericSeries(name, values)
	}

	// Try to determine the type and convert to a specialized Series
	switch values[0].(type) {
	case int:
		intValues, ok := ToIntSlice(values)
		if ok {
			return NewIntSeries(name, intValues)
		}
	case float64:
		floatValues, ok := ToFloat64Slice(values)
		if ok {
			return NewFloat64Series(name, floatValues)
		}
	case string:
		stringValues := ToStringSlice(values)
		return NewStringSeries(name, stringValues)
	case bool:
		boolValues, ok := ToBoolSlice(values)
		if ok {
			return NewBoolSeries(name, boolValues)
		}
	}

	// Default to GenericSeries for mixed or unsupported types
	return NewGenericSeries(name, values)
}

func NewSeriesWithType(name string, values []any, valueType string) SeriesInterface {
	series := NewSeries(name, values)
	return series.AsType(valueType)
}

// Helper functions to convert between types
func ToIntSlice(values []any) ([]int, bool) {
	result := make([]int, len(values))
	for i, v := range values {
		switch val := v.(type) {
		case int:
			result[i] = val
		case int8:
			result[i] = int(val)
		case int16:
			result[i] = int(val)
		case int32:
			result[i] = int(val)
		case int64:
			result[i] = int(val)
		case uint:
			result[i] = int(val)
		case uint8:
			result[i] = int(val)
		case uint16:
			result[i] = int(val)
		case uint32:
			result[i] = int(val)
		case uint64:
			result[i] = int(val)
		case float32:
			result[i] = int(val)
		case float64:
			result[i] = int(val)
		case bool:
			if val {
				result[i] = 1
			} else {
				result[i] = 0
			}
		case string:
			intVal, err := strconv.Atoi(val)
			if err != nil {
				return nil, false
			}
			result[i] = intVal
		default:
			return nil, false
		}
	}
	return result, true
}

func ToFloat64Slice(values []any) ([]float64, bool) {
	result := make([]float64, len(values))
	for i, v := range values {
		switch val := v.(type) {
		case int:
			result[i] = float64(val)
		case int8:
			result[i] = float64(val)
		case int16:
			result[i] = float64(val)
		case int32:
			result[i] = float64(val)
		case int64:
			result[i] = float64(val)
		case uint:
			result[i] = float64(val)
		case uint8:
			result[i] = float64(val)
		case uint16:
			result[i] = float64(val)
		case uint32:
			result[i] = float64(val)
		case uint64:
			result[i] = float64(val)
		case float32:
			result[i] = float64(val)
		case float64:
			result[i] = val
		case bool:
			if val {
				result[i] = 1.0
			} else {
				result[i] = 0.0
			}
		case string:
			floatVal, err := strconv.ParseFloat(val, 64)
			if err != nil {
				return nil, false
			}
			result[i] = floatVal
		default:
			return nil, false
		}
	}
	return result, true
}

func ToStringSlice(values []any) []string {
	result := make([]string, len(values))
	for i, v := range values {
		result[i] = fmt.Sprint(v)
	}
	return result
}

func ToBoolSlice(values []any) ([]bool, bool) {
	result := make([]bool, len(values))
	for i, v := range values {
		switch val := v.(type) {
		case bool:
			result[i] = val
		case int:
			result[i] = val != 0
		case int8:
			result[i] = val != 0
		case int16:
			result[i] = val != 0
		case int32:
			result[i] = val != 0
		case int64:
			result[i] = val != 0
		case uint:
			result[i] = val != 0
		case uint8:
			result[i] = val != 0
		case uint16:
			result[i] = val != 0
		case uint32:
			result[i] = val != 0
		case uint64:
			result[i] = val != 0
		case float32:
			result[i] = val != 0
		case float64:
			result[i] = val != 0
		case string:
			boolVal, err := strconv.ParseBool(val)
			if err != nil {
				return nil, false
			}
			result[i] = boolVal
		default:
			return nil, false
		}
	}
	return result, true
}
