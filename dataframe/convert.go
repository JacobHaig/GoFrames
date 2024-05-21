package dataframe

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// This file is used to do type conversions on a series.

// string
// bytes
// floating
// integer
// mixed-integer
// mixed-integer-float
// decimal
// complex
// categorical
// boolean
// datetime64
// datetime
// date
// timedelta64
// timedelta
// time
// period
// mixed

func convertValue(value interface{}, newType string) (interface{}, error) {
	switch newType {
	case "int":
		i, err := convertToInt(value)
		if err != nil {
			return nil, err
		}
		return i, nil
	case "float":
		f, err := convertToFloat(value)
		if err != nil {
			return nil, err
		}
		return f, nil
	case "string":
		return convertToString(value), nil
	case "bool":
		return convertToBool(value), nil
	}

	return nil, errors.New("error: unknown type")
}

func convertToInt(value interface{}) (int, error) {
	switch v := value.(type) {
	case int:
		return v, nil
	case float64:
		return int(v), nil
	case string:
		return convertStringToInt(v)
	}
	errorMessage := fmt.Sprintf("error: could not convert %T to int: %v", value, value)
	return 0, errors.New(errorMessage)
}

func convertStringToInt(value string) (int, error) {
	i, err := strconv.Atoi(value)
	if err != nil {
		// The value is not easily converted to an int
		// we should try to remove formatting characters and try again.
		value = strings.Replace(value, ",", "", -1)
		i, err = strconv.Atoi(value)
		if err != nil {
			return 0, err
		}
	}
	return i, nil
}

func convertToFloat(value interface{}) (float64, error) {
	switch v := value.(type) {
	case int:
		return float64(v), nil
	case float64:
		return v, nil
	case string:
		return convertStringToFloat(v)
	}
	errorMessage := fmt.Sprintf("error: could not convert %T to float: %v", value, value)
	return 0, errors.New(errorMessage)
}

func convertStringToFloat(value string) (float64, error) {
	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		// The value is not easily converted to a float
		// we should try to remove formatting characters and try again.
		value = strings.Replace(value, ",", "", -1)
		f, err = strconv.ParseFloat(value, 64)
		if err != nil {
			return 0, err
		}
	}
	return f, nil
}

func convertToString(value interface{}) string {
	return fmt.Sprint(value)
}

func convertToBool(value interface{}) bool {
	switch v := value.(type) {
	case bool:
		return v
	case int:
		if v == 1 {
			return true
		}
		if v == 0 {
			return false
		}
	case string:
		switch v {
		case "true":
			return true
		case "false":
			return false
		}
		return false
	}
	return false
}
