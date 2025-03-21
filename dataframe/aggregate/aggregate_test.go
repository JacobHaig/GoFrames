package aggregate_test

import (
	"teddy/dataframe"
	"teddy/dataframe/aggregate"
	"teddy/dataframe/series"
	"testing"
)

func TestBasicAggregators(t *testing.T) {
	// Test Sum
	sumAgg := aggregate.Sum()
	result := sumAgg(1, 2, 3, 4, 5)
	if result != 15 {
		t.Errorf("Sum aggregator returned %v, expected 15", result)
	}

	// Test Mean
	meanAgg := aggregate.Mean()
	result = meanAgg(1, 2, 3, 4, 5)
	if result != 3.0 {
		t.Errorf("Mean aggregator returned %v, expected 3.0", result)
	}

	// Test Min
	minAgg := aggregate.Min()
	result = minAgg(5, 3, 8, 1, 10)
	if result != 1 {
		t.Errorf("Min aggregator returned %v, expected 1", result)
	}

	// Test Max
	maxAgg := aggregate.Max()
	result = maxAgg(5, 3, 8, 1, 10)
	if result != 10 {
		t.Errorf("Max aggregator returned %v, expected 10", result)
	}
}

func TestApply(t *testing.T) {
	// Test with IntSeries
	intValues := []int{1, 2, 3, 4, 5}
	intSeries := series.NewIntSeries("numbers", intValues)

	// Apply Sum
	result := aggregate.Apply(intSeries, aggregate.Sum())
	if result != 15 {
		t.Errorf("Apply with Sum returned %v, expected 15", result)
	}

	// Apply Mean
	result = aggregate.Apply(intSeries, aggregate.Mean())
	if result != 3.0 {
		t.Errorf("Apply with Mean returned %v, expected 3.0", result)
	}

	// Test with StringSeries
	stringValues := []string{"apple", "banana", "cherry"}
	stringSeries := series.NewStringSeries("fruits", stringValues)

	// Apply Count
	result = aggregate.Apply(stringSeries, aggregate.Count())
	if result != 3 {
		t.Errorf("Apply with Count returned %v, expected 3", result)
	}

	// Apply First
	result = aggregate.Apply(stringSeries, aggregate.First())
	if result != "apple" {
		t.Errorf("Apply with First returned %v, expected 'apple'", result)
	}

	// Apply Last
	result = aggregate.Apply(stringSeries, aggregate.Last())
	if result != "cherry" {
		t.Errorf("Apply with Last returned %v, expected 'cherry'", result)
	}
}

func TestApplyToDF(t *testing.T) {
	// Create test dataframe
	intValues := []int{1, 2, 3, 4, 5}
	intSeries := series.NewIntSeries("numbers", intValues)

	stringValues := []string{"apple", "banana", "cherry", "date", "elderberry"}
	stringSeries := series.NewStringSeries("fruits", stringValues)

	df := dataframe.NewDataFrame(intSeries, stringSeries)

	// Apply Sum to numbers column
	result := aggregate.ApplyToDF(df, "numbers", aggregate.Sum())
	if result != 15 {
		t.Errorf("ApplyToDF with Sum returned %v, expected 15", result)
	}

	// Apply First to fruits column
	result = aggregate.ApplyToDF(df, "fruits", aggregate.First())
	if result != "apple" {
		t.Errorf("ApplyToDF with First returned %v, expected 'apple'", result)
	}
}

func TestCombine(t *testing.T) {
	// Create a combined aggregator
	combined := aggregate.Combine(
		aggregate.Sum(),
		aggregate.Mean(),
		aggregate.Min(),
		aggregate.Max(),
	)

	// Apply to values
	result := combined(1, 2, 3, 4, 5)
	resultSlice, ok := result.([]any)

	if !ok {
		t.Fatalf("Combined aggregator result is not a slice")
	}

	if len(resultSlice) != 4 {
		t.Fatalf("Combined aggregator returned %d results, expected 4", len(resultSlice))
	}

	if resultSlice[0] != 15 {
		t.Errorf("Combined Sum returned %v, expected 15", resultSlice[0])
	}

	if resultSlice[1] != 3.0 {
		t.Errorf("Combined Mean returned %v, expected 3.0", resultSlice[1])
	}

	if resultSlice[2] != 1 {
		t.Errorf("Combined Min returned %v, expected 1", resultSlice[2])
	}

	if resultSlice[3] != 5 {
		t.Errorf("Combined Max returned %v, expected 5", resultSlice[3])
	}
}
