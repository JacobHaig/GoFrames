package aggregate

import (
	"teddy/dataframe/series"
)

// Sum returns an aggregator that sums all values
func Sum() Aggregator {
	return func(values ...any) any {
		if len(values) == 0 {
			return 0
		}

		// Optimize for typed values when possible
		switch values[0].(type) {
		case int:
			sum := 0
			allInts := true
			for _, val := range values {
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
			for _, val := range values {
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
		numValues, ok := series.ToFloat64Slice(values)
		if ok {
			var sum float64
			for _, val := range numValues {
				sum += val
			}
			return sum
		}

		// Can't sum non-numeric values
		return 0
	}
}

// Mean returns an aggregator that calculates the arithmetic mean
func Mean() Aggregator {
	return func(values ...any) any {
		if len(values) == 0 {
			return 0.0
		}

		// Optimize for typed values when possible
		switch values[0].(type) {
		case int:
			sum := 0
			count := 0
			for _, val := range values {
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
			for _, val := range values {
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
		numValues, ok := series.ToFloat64Slice(values)
		if ok && len(numValues) > 0 {
			var sum float64
			for _, val := range numValues {
				sum += val
			}
			return sum / float64(len(numValues))
		}

		// Can't calculate mean of non-numeric values
		return 0.0
	}
}

// Min returns an aggregator that finds the minimum value
func Min() Aggregator {
	return func(values ...any) any {
		if len(values) == 0 {
			return nil
		}

		// Optimize for typed values when possible
		switch values[0].(type) {
		case int:
			minVal, ok := values[0].(int)
			if !ok {
				break
			}

			allInts := true
			for _, val := range values[1:] {
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
			minVal, ok := values[0].(float64)
			if !ok {
				break
			}

			allFloats := true
			for _, val := range values[1:] {
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
		numValues, ok := series.ToFloat64Slice(values)
		if ok && len(numValues) > 0 {
			minVal := numValues[0]
			for _, val := range numValues[1:] {
				if val < minVal {
					minVal = val
				}
			}
			return minVal
		}

		// For non-numeric types, just return the first element
		return values[0]
	}
}

// Max returns an aggregator that finds the maximum value
func Max() Aggregator {
	return func(values ...any) any {
		if len(values) == 0 {
			return nil
		}

		// Optimize for typed values when possible
		switch values[0].(type) {
		case int:
			maxVal, ok := values[0].(int)
			if !ok {
				break
			}

			allInts := true
			for _, val := range values[1:] {
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
			maxVal, ok := values[0].(float64)
			if !ok {
				break
			}

			allFloats := true
			for _, val := range values[1:] {
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
		numValues, ok := series.ToFloat64Slice(values)
		if ok && len(numValues) > 0 {
			maxVal := numValues[0]
			for _, val := range numValues[1:] {
				if val > maxVal {
					maxVal = val
				}
			}
			return maxVal
		}

		// For non-numeric types, just return the first element
		return values[0]
	}
}

// Count returns an aggregator that counts the number of elements
func Count() Aggregator {
	return func(values ...any) any {
		return len(values)
	}
}

// First returns an aggregator that returns the first element
func First() Aggregator {
	return func(values ...any) any {
		if len(values) == 0 {
			return nil
		}
		return values[0]
	}
}

// Last returns an aggregator that returns the last element
func Last() Aggregator {
	return func(values ...any) any {
		if len(values) == 0 {
			return nil
		}
		return values[len(values)-1]
	}
}
