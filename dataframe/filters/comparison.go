package filters

import (
	"fmt"
	"strings"
)

// GreaterThan returns a filter that checks if a value is greater than the threshold
func GreaterThan(threshold any) Filter {
	return func(value any) bool {
		return compare(value, threshold) > 0
	}
}

// LessThan returns a filter that checks if a value is less than the threshold
func LessThan(threshold any) Filter {
	return func(value any) bool {
		return compare(value, threshold) < 0
	}
}

// GreaterEqual returns a filter that checks if a value is greater than or equal to the threshold
func GreaterEqual(threshold any) Filter {
	return func(value any) bool {
		return compare(value, threshold) >= 0
	}
}

// LessEqual returns a filter that checks if a value is less than or equal to the threshold
func LessEqual(threshold any) Filter {
	return func(value any) bool {
		return compare(value, threshold) <= 0
	}
}

// Equal returns a filter that checks if a value is equal to the target
func Equal(target any) Filter {
	return func(value any) bool {
		return compare(value, target) == 0
	}
}

// NotEqual returns a filter that checks if a value is not equal to the target
func NotEqual(target any) Filter {
	return func(value any) bool {
		return compare(value, target) != 0
	}
}

// Contains returns a filter that checks if a string value contains the substring
func Contains(substring string) Filter {
	return func(value any) bool {
		if str, ok := value.(string); ok {
			return strings.Contains(str, substring)
		}
		return false
	}
}

// StartsWith returns a filter that checks if a string value starts with the prefix
func StartsWith(prefix string) Filter {
	return func(value any) bool {
		if str, ok := value.(string); ok {
			return strings.HasPrefix(str, prefix)
		}
		return false
	}
}

// EndsWith returns a filter that checks if a string value ends with the suffix
func EndsWith(suffix string) Filter {
	return func(value any) bool {
		if str, ok := value.(string); ok {
			return strings.HasSuffix(str, suffix)
		}
		return false
	}
}

// In returns a filter that checks if a value is in the given set of values
func In(values ...any) Filter {
	return func(value any) bool {
		for _, v := range values {
			if compare(value, v) == 0 {
				return true
			}
		}
		return false
	}
}

// IsNull returns a filter that checks if a value is nil
func IsNull() Filter {
	return func(value any) bool {
		return value == nil
	}
}

// IsNotNull returns a filter that checks if a value is not nil
func IsNotNull() Filter {
	return func(value any) bool {
		return value != nil
	}
}

// compare compares two values and returns:
// -1 if a < b
//
//	0 if a == b
//	1 if a > b
func compare(a, b any) int {
	// Handle nil values
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Try to convert both values to float64 for numeric comparison
	aFloat, aOk := toFloat64(a)
	bFloat, bOk := toFloat64(b)

	if aOk && bOk {
		if aFloat < bFloat {
			return -1
		} else if aFloat > bFloat {
			return 1
		}
		return 0
	}

	// If not both numeric, compare as strings
	aStr := fmt.Sprint(a)
	bStr := fmt.Sprint(b)
	return strings.Compare(aStr, bStr)
}

// toFloat64 attempts to convert a value to float64
func toFloat64(v any) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	case bool:
		if val {
			return 1.0, true
		}
		return 0.0, true
	default:
		return 0, false
	}
}
