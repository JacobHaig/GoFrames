package dataframe

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// This file is used to do type conversions on a datatypes.

// Some of the possible datatypes are listed below:

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

// convertValue converts a value to a new type
// It's used for the older Series implementation for backward compatibility
func ConvertValue(value any, newType string) (any, error) {
	switch newType {
	case "int":
		i, err := convertToInt(value)
		if err != nil {
			return nil, fmt.Errorf("error converting value to type %s: %w", newType, err)
		}
		return i, nil
	case "float", "float64":
		f, err := convertToFloat(value)
		if err != nil {
			return nil, fmt.Errorf("error converting value to type %s: %w", newType, err)
		}
		return f, nil
	case "string":
		return ConvertToString(value), nil
	case "bool":
		return ConvertToBool(value), nil
	case "time", "datetime":
		t, err := convertToTime(value)
		if err != nil {
			return nil, fmt.Errorf("error converting value to type %s: %w", newType, err)
		}
		return t, nil
	}

	return nil, errors.New("error: unknown type")
}

func convertToInt(value any) (int, error) {
	switch v := value.(type) {
	case int:
		return v, nil
	case int8:
		return int(v), nil
	case int16:
		return int(v), nil
	case int32:
		return int(v), nil
	case int64:
		return int(v), nil
	case uint:
		return int(v), nil
	case uint8:
		return int(v), nil
	case uint16:
		return int(v), nil
	case uint32:
		return int(v), nil
	case uint64:
		return int(v), nil
	case float32:
		return int(v), nil
	case float64:
		return int(v), nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case string:
		return convertStringToInt(v)
	}
	errorMessage := fmt.Sprintf("error: could not convert value of type %T to int. The Value is %v", value, value)
	return 0, errors.New(errorMessage)
}

func convertStringToInt(value string) (int, error) {
	// Try direct conversion
	i, err := strconv.Atoi(value)
	if err == nil {
		return i, nil
	}

	// Try removing formatting characters and try again
	value = strings.ReplaceAll(value, ",", "")
	value = strings.TrimSpace(value)

	i, err = strconv.Atoi(value)
	if err != nil {
		// Try to parse as float and then convert to int
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return 0, fmt.Errorf("error converting string '%s' to int: %w", value, err)
		}
		return int(f), nil
	}
	return i, nil
}

func convertToFloat(value any) (float64, error) {
	switch v := value.(type) {
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case bool:
		if v {
			return 1.0, nil
		}
		return 0.0, nil
	case string:
		return convertStringToFloat(v)
	}
	errorMessage := fmt.Sprintf("error: could not convert value of type %T to float. The Value is %v", value, value)
	return 0, errors.New(errorMessage)
}

func convertStringToFloat(value string) (float64, error) {
	// Try direct conversion
	f, err := strconv.ParseFloat(value, 64)
	if err == nil {
		return f, nil
	}

	// Try removing formatting characters and try again
	value = strings.ReplaceAll(value, ",", "")
	value = strings.TrimSpace(value)

	f, err = strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("error converting string '%s' to float: %w", value, err)
	}
	return f, nil
}

func ConvertToString(value any) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}

func ConvertToBool(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return v != 0
	case float32, float64:
		return v != 0
	case string:
		v = strings.ToLower(strings.TrimSpace(v))
		switch v {
		case "true", "t", "yes", "y", "1":
			return true
		case "false", "f", "no", "n", "0":
			return false
		}
	}
	return false
}

func convertToTime(value any) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case string:
		return parseTime(v)
	case int, int64:
		// Interpret as Unix timestamp
		return time.Unix(int64(v.(int64)), 0), nil
	}
	errorMessage := fmt.Sprintf("error: could not convert value of type %T to time. The Value is %v", value, value)
	return time.Time{}, errors.New(errorMessage)
}

func parseTime(value string) (time.Time, error) {
	// Common date/time formats to try
	formats := []string{
		time.RFC3339,
		"2006-01-02",
		"2006-01-02 15:04:05",
		"01/02/2006",
		"01/02/2006 15:04:05",
		"2-Jan-2006",
		"2-Jan-2006 15:04:05",
		"02 Jan 2006",
		"02 Jan 2006 15:04:05",
	}

	for _, format := range formats {
		t, err := time.Parse(format, value)
		if err == nil {
			return t, nil
		}
	}

	return time.Time{}, errors.New("could not parse string as time")
}

// Helper function for max of two ints (Go < 1.21 compatibility)
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
