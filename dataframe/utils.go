package dataframe

import (
	"errors"
	"fmt"
	"log"
	"strings"
)

type OptionsMap map[string]any

func standardizeOptions(options ...OptionsMap) OptionsMap {
	if len(options) == 0 {
		return OptionsMap{}
	}
	// Lowercase all keys
	for k, v := range options[0] {
		delete(options[0], k)
		options[0][strings.ToLower(k)] = v
	}
	return options[0]
}

func (options OptionsMap) getOption(key string, defaultValue any) any {
	if val, ok := options[key]; ok {
		return val
	}
	return defaultValue
}

func allSameType(values []any) bool {
	if len(values) == 0 {
		return true
	}

	firstType := fmt.Sprintf("%T", values[0])

	for _, value := range values {
		if fmt.Sprintf("%T", value) != firstType {
			return false
		}
	}

	return true
}

// FlattenInterface flattens a slice of slices of interfaces into a single slice of T
// This can flatten [][]any into []T or []any into []T
func flattenInterface[T any](acc []T, arr any) ([]T, error) {
	var err error
	switch v := arr.(type) {
	case []T:
		acc = append(acc, v...)
	case T:
		acc = append(acc, v)
	case []any:
		for _, elem := range v {
			acc, err = flattenInterface(acc, elem)
			if err != nil {
				return nil, errors.New("Error flattening interface")
			}
		}
	case [][]any:
		for _, elem := range v {
			acc, err = flattenInterface(acc, elem)
			if err != nil {
				return nil, errors.New("Error flattening interface")
			}
		}
		return acc, nil
	default:
		return nil, errors.New("Could not flatten array of type " + fmt.Sprintf("%T", arr))
	}

	return acc, nil
}

// InterfaceToTypeSlice flattens a slice of slices of interfaces into a single slice of T
//
// This can flatten [][]any into []T or []any into []T
func InterfaceToTypeSlice[T any](values ...any) []T {
	result, err := flattenInterface([]T{}, values)

	if err != nil {
		log.Println("Error flattening interface")
		log.Fatal(err)
	}

	return result
}

// PadRight pads a string on the right with a pad string until it reaches a certain length
func PadRight(str, pad string, length int) string {
	for {
		str += pad
		if len(str) > length {
			return str[0:length]
		}
	}
}

// PadLeft pads a string on the left with a pad string until it reaches a certain length
func PadLeft(str, pad string, length int) string {
	for {
		str = pad + str
		if len(str) > length {
			return str[0:length]
		}
	}
}

func SprintfStringSlice(slice []string) string {
	list := []string{}
	for _, ele := range slice {
		list = append(list, fmt.Sprintf("\"%v\"", ele))
	}
	return "[" + strings.Join(list, ", ") + "]"
}

func TransposeRows(rows [][]string) [][]string {
	// Create a new 2D array
	transposed := make([][]string, len(rows[0]))
	for i := range transposed {
		transposed[i] = make([]string, len(rows))
	}

	// Transpose the 2D array
	for rowIndex, row := range rows {
		for colIndex, cell := range row {
			transposed[colIndex][rowIndex] = cell
		}
	}

	return transposed
}

func allTrue(values []bool) bool {
	for _, value := range values {
		if !value {
			return false
		}
	}
	return true
}
