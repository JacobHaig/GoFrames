package dataframe

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/rotisserie/eris"
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
				return nil, eris.Wrap(err, "Error flattening interface")
			}
		}
	case [][]any:
		for _, elem := range v {
			acc, err = flattenInterface(acc, elem)
			if err != nil {
				return nil, eris.Wrap(err, "Error flattening interface")
			}
		}
		return acc, nil
	default:
		return nil, eris.Wrap(errors.New("Could not flatten array of type "+fmt.Sprintf("%T", arr)), "Error flattening interface")
	}

	return acc, nil
}

// FlattenInterface flattens a slice of slices of interfaces into a single slice of T
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

func PrintTrace(err error) {
	// format := eris.NewDefaultStringFormat(eris.FormatOptions{
	// 	InvertOutput: true, // flag that inverts the error output (wrap errors shown first)
	// 	WithTrace:    true, // flag that enables stack trace output
	// 	InvertTrace:  true, // flag that inverts the stack trace output (top of call stack shown first)
	// 	WithExternal: true,
	// })
	// fmt.Println(eris.ToCustomString(err, format))

	upErr := eris.Unpack(err)

	var str string
	if upErr.ErrExternal != nil {
		str += fmt.Sprintf("%+v", upErr.ErrExternal) + "\n"
	}
	str += fmt.Sprintf("%+v", upErr.ErrRoot.Msg) + "\n"

	for _, frame := range upErr.ErrRoot.Stack {
		str += frame.Name + "\n"
		str += "\t" + removeParentFolder(frame.File) + ":" + strconv.Itoa(frame.Line) + "\n"
	}

	str += "\n"

	for _, eLink := range upErr.ErrChain {
		str += eLink.Msg + "\n"
		str += eLink.Frame.Name + "\n"
		str += "\t" + removeParentFolder(eLink.Frame.File) + ":" + strconv.Itoa(eLink.Frame.Line) + "\n"
	}

	fmt.Println(str)
}

// Function that removes the parent path from the file path
func removeParentFolder(parentfolder string) string {
	SplitLabel := "GoFrames" // Change this to the parent directory name
	SplitPath := strings.Split(parentfolder, SplitLabel)

	return SplitPath[1]
}
