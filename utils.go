package main

import (
	"errors"
	"fmt"
	"log"
	"strings"
)

type Options map[string]interface{}

func standardizeMapKeys(options ...Options) Options {
	if len(options) == 0 {
		return Options{}
	}
	// Lowercase all keys
	for k, v := range options[0] {
		delete(options[0], k)
		options[0][strings.ToLower(k)] = v
	}
	return options[0]
}

func allSameType(values []interface{}) bool {
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

func flattenInterface[T interface{}](acc []T, arr interface{}) ([]T, error) {
	// s := fmt.Sprintf("%T", arr)
	// fmt.Println(s)

	var err error
	switch v := arr.(type) {

	case []T:
		acc = append(acc, v...)
	case T:
		acc = append(acc, v)
	case []interface{}:
		for _, elem := range v {
			acc, err = flattenInterface(acc, elem)
			if err != nil {
				return nil, err
			}
		}
	case [][]interface{}:
		for _, elem := range v {
			acc, err = flattenInterface(acc, elem)
			if err != nil {
				return nil, err
			}
		}
		return acc, nil
	default:
		return nil, errors.New("Could not flatten array of type " + fmt.Sprintf("%T", arr))
	}

	return acc, nil
}

// FlattenInterface flattens a slice of slices of interfaces into a single slice of T
//
// This can flatten [][]interface{} into []T or []interface{} into []T
func InterfaceToTypeSlice[T interface{}](values ...interface{}) []T {
	result, err := flattenInterface([]T{}, values)

	if err != nil {
		log.Println("Error flattening interface")
		log.Fatal(err)
	}

	return result
}

func PadRight(str, pad string, length int) string {
	for {
		str += pad
		if len(str) > length {
			return str[0:length]
		}
	}
}
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
