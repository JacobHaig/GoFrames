package filters_test

import (
	"teddy/dataframe"
	"teddy/dataframe/filters"
	"teddy/dataframe/series"
	"testing"
)

func TestBasicFilters(t *testing.T) {
	// Tests basic comparison filters (GreaterThan, LessThan, Equal)
	// Test GreaterThan
	gtFilter := filters.GreaterThan(5)
	if gtFilter(3) {
		t.Error("GreaterThan filter returned true for 3 > 5")
	}
	if !gtFilter(7) {
		t.Error("GreaterThan filter returned false for 7 > 5")
	}

	// Test LessThan
	ltFilter := filters.LessThan(5)
	if !ltFilter(3) {
		t.Error("LessThan filter returned false for 3 < 5")
	}
	if ltFilter(7) {
		t.Error("LessThan filter returned true for 7 < 5")
	}

	// Test Equal
	eqFilter := filters.Equal(5)
	if !eqFilter(5) {
		t.Error("Equal filter returned false for 5 == 5")
	}
	if eqFilter(7) {
		t.Error("Equal filter returned true for 7 == 5")
	}
}

func TestLogicalOperators(t *testing.T) {
	// Tests logical operators (And, Or, Not) with numeric comparisons
	// Test And
	andFilter := filters.And(
		filters.GreaterThan(5),
		filters.LessThan(10),
	)
	if andFilter(3) {
		t.Error("And filter returned true for 3 > 5 && 3 < 10")
	}
	if !andFilter(7) {
		t.Error("And filter returned false for 7 > 5 && 7 < 10")
	}
	if andFilter(12) {
		t.Error("And filter returned true for 12 > 5 && 12 < 10")
	}

	// Test Or
	orFilter := filters.Or(
		filters.LessThan(5),
		filters.GreaterThan(10),
	)
	if !orFilter(3) {
		t.Error("Or filter returned false for 3 < 5 || 3 > 10")
	}
	if orFilter(7) {
		t.Error("Or filter returned true for 7 < 5 || 7 > 10")
	}
	if !orFilter(12) {
		t.Error("Or filter returned false for 12 < 5 || 12 > 10")
	}

	// Test Not
	notFilter := filters.Not(filters.GreaterThan(5))
	if !notFilter(3) {
		t.Error("Not filter returned false for !(3 > 5)")
	}
	if notFilter(7) {
		t.Error("Not filter returned true for !(7 > 5)")
	}
}

func TestStringFilters(t *testing.T) {
	// Tests string-specific filters (Contains, StartsWith, EndsWith)
	// Test Contains
	containsFilter := filters.Contains("world")
	if !containsFilter("hello world") {
		t.Error("Contains filter returned false for 'hello world' contains 'world'")
	}
	if containsFilter("hello") {
		t.Error("Contains filter returned true for 'hello' contains 'world'")
	}

	// Test StartsWith
	startsWithFilter := filters.StartsWith("hello")
	if !startsWithFilter("hello world") {
		t.Error("StartsWith filter returned false for 'hello world' starts with 'hello'")
	}
	if startsWithFilter("world hello") {
		t.Error("StartsWith filter returned true for 'world hello' starts with 'hello'")
	}

	// Test EndsWith
	endsWithFilter := filters.EndsWith("world")
	if !endsWithFilter("hello world") {
		t.Error("EndsWith filter returned false for 'hello world' ends with 'world'")
	}
	if endsWithFilter("world hello") {
		t.Error("EndsWith filter returned true for 'world hello' ends with 'world'")
	}
}

func TestApply(t *testing.T) {
	// Tests applying filters to series and getting matching indices
	// Test with IntSeries
	intValues := []int{1, 5, 10, 15, 20}
	intSeries := series.NewIntSeries("numbers", intValues)

	indices := filters.Apply(intSeries, filters.GreaterThan(10))
	if len(indices) != 2 {
		t.Errorf("Expected 2 indices, got %d", len(indices))
	}
	if indices[0] != 3 || indices[1] != 4 {
		t.Errorf("Expected indices [3, 4], got %v", indices)
	}

	// Test with StringSeries
	stringValues := []string{"apple", "banana", "cherry", "date", "elderberry"}
	stringSeries := series.NewStringSeries("fruits", stringValues)

	indices = filters.Apply(stringSeries, filters.StartsWith("b"))
	if len(indices) != 1 {
		t.Errorf("Expected 1 index, got %d", len(indices))
	}
	if indices[0] != 1 {
		t.Errorf("Expected index [1], got %v", indices)
	}
}

func TestApplyToDF(t *testing.T) {
	// Tests filtering DataFrames by applying a filter to a specific column
	// Create test dataframe
	intSeries := series.NewIntSeries("numbers", []int{1, 5, 10, 15, 20})
	stringSeries := series.NewStringSeries("fruits", []string{"apple", "banana", "cherry", "date", "elderberry"})
	df := dataframe.NewDataFrame(intSeries, stringSeries)

	// Filter rows where numbers > 10
	filteredDF := filters.ApplyToDF(df, "numbers", filters.GreaterThan(10))

	height, _ := filteredDF.Shape()
	if height != 2 {
		t.Errorf("Expected 2 rows, got %d", height)
	}

	// Verify the filtered values
	numbersCol := filteredDF.GetSeries("numbers")
	if numbersCol.Get(0) != 15 || numbersCol.Get(1) != 20 {
		t.Errorf("Expected numbers [15, 20], got %v, %v", numbersCol.Get(0), numbersCol.Get(1))
	}
}

func TestAdvancedFilters(t *testing.T) {
	// Tests advanced filters (IsNull, IsNotNull, In) and complex filter combinations
	// Test IsNull/IsNotNull
	mixedValues := []any{1, 5.5, "test", nil, 10}
	mixedSeries := series.NewGenericSeries("mixed", mixedValues)

	nullIndices := filters.Apply(mixedSeries, filters.IsNull())
	if len(nullIndices) != 1 || nullIndices[0] != 3 {
		t.Errorf("Expected [3], got %v", nullIndices)
	}

	notNullIndices := filters.Apply(mixedSeries, filters.IsNotNull())
	if len(notNullIndices) != 4 {
		t.Errorf("Expected 4 indices, got %d", len(notNullIndices))
	}

	// Test In
	inFilter := filters.In(1, "test", 10)
	inIndices := filters.Apply(mixedSeries, inFilter)
	if len(inIndices) != 3 {
		t.Errorf("Expected 3 indices, got %d", len(inIndices))
	}

	// Test complex filter
	complexFilter := filters.And(
		filters.Or(
			filters.GreaterThan(5),
			filters.Equal("test"),
		),
		filters.Not(filters.IsNull()),
	)

	complexIndices := filters.Apply(mixedSeries, complexFilter)
	if len(complexIndices) != 3 {
		t.Errorf("Expected 3 indices, got %d", len(complexIndices))
	}
}
