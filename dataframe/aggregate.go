package dataframe

import "teddy/dataframe/series"

type Number interface {
	int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64 | float32 | float64
}

// Sum takes a slice of values and returns their sum
func Sum(list ...any) any {
	if len(list) == 0 {
		return 0
	}

	// Optimize for typed values when possible
	switch list[0].(type) {
	case int:
		sum := 0
		allInts := true
		for _, val := range list {
			if intVal, ok := val.(int); ok {
				sum += intVal
			} else {
				allInts = false
				break
			}
		}
		if allInts {
			return sum
		}
	case float64:
		sum := 0.0
		allFloats := true
		for _, val := range list {
			if floatVal, ok := val.(float64); ok {
				sum += floatVal
			} else {
				allFloats = false
				break
			}
		}
		if allFloats {
			return sum
		}
	}

	// Fall back to generic handling
	// Try to convert all values to float64 for summation
	values, ok := series.ToFloat64Slice(list)
	if ok {
		var sum float64
		for _, val := range values {
			sum += val
		}
		return sum
	}

	// Can't sum non-numeric values
	return 0
}

// Mean takes a slice of values and returns their arithmetic mean
func Mean(list ...any) any {
	if len(list) == 0 {
		return 0.0
	}

	// Optimize for typed values when possible
	switch list[0].(type) {
	case int:
		sum := 0
		count := 0
		for _, val := range list {
			if intVal, ok := val.(int); ok {
				sum += intVal
				count++
			}
		}
		if count > 0 {
			return float64(sum) / float64(count)
		}
	case float64:
		sum := 0.0
		count := 0
		for _, val := range list {
			if floatVal, ok := val.(float64); ok {
				sum += floatVal
				count++
			}
		}
		if count > 0 {
			return sum / float64(count)
		}
	}

	// Fall back to generic handling
	// Try to convert all values to float64 for calculation
	values, ok := series.ToFloat64Slice(list)
	if ok && len(values) > 0 {
		var sum float64
		for _, val := range values {
			sum += val
		}
		return sum / float64(len(values))
	}

	// Can't calculate mean of non-numeric values
	return 0.0
}

// Min returns the minimum value in the list
func Min(list ...any) any {
	if len(list) == 0 {
		return nil
	}

	// Optimize for typed values when possible
	switch list[0].(type) {
	case int:
		minVal, ok := list[0].(int)
		if !ok {
			break
		}

		allInts := true
		for _, val := range list[1:] {
			if intVal, ok := val.(int); ok {
				if intVal < minVal {
					minVal = intVal
				}
			} else {
				allInts = false
				break
			}
		}

		if allInts {
			return minVal
		}
	case float64:
		minVal, ok := list[0].(float64)
		if !ok {
			break
		}

		allFloats := true
		for _, val := range list[1:] {
			if floatVal, ok := val.(float64); ok {
				if floatVal < minVal {
					minVal = floatVal
				}
			} else {
				allFloats = false
				break
			}
		}

		if allFloats {
			return minVal
		}
	}

	// Fall back to generic handling
	// Try to convert all values to float64 for comparison
	values, ok := series.ToFloat64Slice(list)
	if ok && len(values) > 0 {
		minVal := values[0]
		for _, val := range values[1:] {
			if val < minVal {
				minVal = val
			}
		}
		return minVal
	}

	// For non-numeric types, just return the first element
	return list[0]
}

// Max returns the maximum value in the list
func Max(list ...any) any {
	if len(list) == 0 {
		return nil
	}

	// Optimize for typed values when possible
	switch list[0].(type) {
	case int:
		maxVal, ok := list[0].(int)
		if !ok {
			break
		}

		allInts := true
		for _, val := range list[1:] {
			if intVal, ok := val.(int); ok {
				if intVal > maxVal {
					maxVal = intVal
				}
			} else {
				allInts = false
				break
			}
		}

		if allInts {
			return maxVal
		}
	case float64:
		maxVal, ok := list[0].(float64)
		if !ok {
			break
		}

		allFloats := true
		for _, val := range list[1:] {
			if floatVal, ok := val.(float64); ok {
				if floatVal > maxVal {
					maxVal = floatVal
				}
			} else {
				allFloats = false
				break
			}
		}

		if allFloats {
			return maxVal
		}
	}

	// Fall back to generic handling
	// Try to convert all values to float64 for comparison
	values, ok := series.ToFloat64Slice(list)
	if ok && len(values) > 0 {
		maxVal := values[0]
		for _, val := range values[1:] {
			if val > maxVal {
				maxVal = val
			}
		}
		return maxVal
	}

	// For non-numeric types, just return the first element
	return list[0]
}

// Count returns the number of elements in the list
func Count(list ...any) any {
	return len(list)
}

// First returns the first element in the list
func First(list ...any) any {
	if len(list) == 0 {
		return nil
	}
	return list[0]
}

// Last returns the last element in the list
func Last(list ...any) any {
	if len(list) == 0 {
		return nil
	}
	return list[len(list)-1]
}
