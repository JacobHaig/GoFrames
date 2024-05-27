package dataframe

type Number interface {
	int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64 | float32 | float64
}

// Takes an empty interface and returns a Number
func Sum(list ...any) any {

	if len(list) == 0 {
		return any(0)
	}

	switch list[0].(type) {
	case int:
		return sum(InterfaceToTypeSlice[int](list))
	case int8:
		return sum(InterfaceToTypeSlice[int8](list))
	case int16:
		return sum(InterfaceToTypeSlice[int16](list))
	case int32:
		return sum(InterfaceToTypeSlice[int32](list))
	case int64:
		return sum(InterfaceToTypeSlice[int64](list))
	case uint:
		return sum(InterfaceToTypeSlice[uint](list))
	case uint8:
		return sum(InterfaceToTypeSlice[uint8](list))
	case uint16:
		return sum(InterfaceToTypeSlice[uint16](list))
	case uint32:
		return sum(InterfaceToTypeSlice[uint32](list))
	case uint64:
		return sum(InterfaceToTypeSlice[uint64](list))
	case float32:
		return sum(InterfaceToTypeSlice[float32](list))
	case float64:
		return sum(InterfaceToTypeSlice[float64](list))
	}
	return 0
}

func sum[T Number](list []T) T {
	if len(list) == 0 {
		return T(0)
	}

	// Sum of values
	var sum T
	for _, value := range list {
		sum += value
	}
	return sum
}

func Mean(list ...any) any {
	if len(list) == 0 {
		return any(0)
	}

	switch list[0].(type) {
	case int:
		return mean(InterfaceToTypeSlice[int](list))
	case int8:
		return mean(InterfaceToTypeSlice[int8](list))
	case int16:
		return mean(InterfaceToTypeSlice[int16](list))
	case int32:
		return mean(InterfaceToTypeSlice[int32](list))
	case int64:
		return mean(InterfaceToTypeSlice[int64](list))
	case uint:
		return mean(InterfaceToTypeSlice[uint](list))
	case uint8:
		return mean(InterfaceToTypeSlice[uint8](list))
	case uint16:
		return mean(InterfaceToTypeSlice[uint16](list))
	case uint32:
		return mean(InterfaceToTypeSlice[uint32](list))
	case uint64:
		return mean(InterfaceToTypeSlice[uint64](list))
	case float32:
		return mean(InterfaceToTypeSlice[float32](list))
	case float64:
		return mean(InterfaceToTypeSlice[float64](list))
	}
	return 0
}

func mean[T Number](list []T) T {
	if len(list) == 0 {
		return T(0)
	}

	// Sum of values
	var sum T
	for _, value := range list {
		sum += value
	}

	// Mean
	return sum / T(len(list))
}
