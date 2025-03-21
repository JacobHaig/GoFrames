package aggregate_test

import (
	"teddy/dataframe"
	"teddy/dataframe/aggregate"
	"teddy/dataframe/series"
	"testing"
)

// createTestDataFrame creates a sample DataFrame for testing
func createTestDataFrame() *dataframe.DataFrame {
	// Creates a test DataFrame with categories, regions, and sales data
	categories := []string{"A", "B", "A", "B", "A", "C", "C"}
	regions := []string{"East", "East", "West", "West", "East", "West", "East"}
	sales := []int{100, 200, 150, 250, 120, 300, 180}

	return dataframe.NewDataFrame(
		series.NewStringSeries("category", categories),
		series.NewStringSeries("region", regions),
		series.NewIntSeries("sales", sales),
	)
}

func TestGroupBySingleColumnSingleAggregation(t *testing.T) {
	// Tests grouping by a single column with one aggregation function
	df := createTestDataFrame()

	// Group by a single column with one aggregation
	result := aggregate.GroupBy(
		df,
		[]string{"category"},
		map[string]aggregate.Aggregator{
			"sales": aggregate.Sum(),
		},
	)

	if result.Height() != 3 {
		t.Errorf("Expected 3 groups, got %d", result.Height())
	}

	// Verify sum aggregation
	salesSeries := result.GetSeries("sales")
	categorySeries := result.GetSeries("category")

	if categorySeries == nil || salesSeries == nil {
		t.Fatal("Expected category and sales series in the result")
	}

	// Create a map to verify each category's sum
	categoryToSales := make(map[string]int)
	for i := 0; i < result.Height(); i++ {
		category := categorySeries.Get(i).(string)
		sales := salesSeries.Get(i).(int)
		categoryToSales[category] = sales
	}

	if categoryToSales["A"] != 370 { // 100 + 150 + 120
		t.Errorf("Expected sum of category A to be 370, got %d", categoryToSales["A"])
	}
	if categoryToSales["B"] != 450 { // 200 + 250
		t.Errorf("Expected sum of category B to be 450, got %d", categoryToSales["B"])
	}
	if categoryToSales["C"] != 480 { // 300 + 180
		t.Errorf("Expected sum of category C to be 480, got %d", categoryToSales["C"])
	}
}

func TestGroupByMultipleColumnsSingleAggregation(t *testing.T) {
	// Tests grouping by multiple columns with one aggregation function
	df := createTestDataFrame()

	// Group by multiple columns with one aggregation
	result := aggregate.GroupBy(
		df,
		[]string{"category", "region"},
		map[string]aggregate.Aggregator{
			"sales": aggregate.Sum(),
		},
	)

	// There should be 6 groups: (A,East), (A,West), (B,East), (B,West), (C,East), (C,West)
	if result.Height() != 6 {
		t.Errorf("Expected 6 groups, got %d", result.Height())
	}

	// Verify the total sum of sales is preserved
	resultValues := result.GetSeries("sales").Values()
	sum := 0
	for _, v := range resultValues {
		sum += v.(int)
	}

	expectedTotal := 100 + 200 + 150 + 250 + 120 + 300 + 180 // 1300
	if sum != expectedTotal {
		t.Errorf("Expected total sales to be %d, got %d", expectedTotal, sum)
	}

	// Verify each group has the correct values
	categorySeries := result.GetSeries("category")
	regionSeries := result.GetSeries("region")
	salesSeries := result.GetSeries("sales")

	for i := 0; i < result.Height(); i++ {
		category := categorySeries.Get(i).(string)
		region := regionSeries.Get(i).(string)
		sales := salesSeries.Get(i).(int)

		// Check specific group values
		if category == "A" && region == "East" && sales != 220 { // 100 + 120
			t.Errorf("Expected sales for A,East to be 220, got %d", sales)
		}
		if category == "A" && region == "West" && sales != 150 {
			t.Errorf("Expected sales for A,West to be 150, got %d", sales)
		}
		if category == "B" && region == "East" && sales != 200 {
			t.Errorf("Expected sales for B,East to be 200, got %d", sales)
		}
		if category == "B" && region == "West" && sales != 250 {
			t.Errorf("Expected sales for B,West to be 250, got %d", sales)
		}
	}
}

func TestGroupBySingleColumnMultipleAggregations(t *testing.T) {
	// Tests grouping by a single column with multiple aggregation functions
	df := createTestDataFrame()

	// Group with multiple aggregations
	result := aggregate.GroupBy(
		df,
		[]string{"category"},
		map[string]aggregate.Aggregator{
			"sales": aggregate.Sum(),
			"count": aggregate.Count(),
		},
	)

	// Check that we have both aggregation columns
	if !result.HasColumn("sales") || !result.HasColumn("count") {
		t.Errorf("Expected both 'sales' and 'count' columns in result")
	}

	// Verify count aggregation
	countSeries := result.GetSeries("count")
	categorySeries := result.GetSeries("category")

	if categorySeries == nil || countSeries == nil {
		t.Fatal("Expected category and count series in the result")
	}

	// Create a map to verify each category's count
	categoryToCount := make(map[string]int)
	for i := 0; i < result.Height(); i++ {
		category := categorySeries.Get(i).(string)
		count := countSeries.Get(i).(int)
		categoryToCount[category] = count
	}

	if categoryToCount["A"] != 3 {
		t.Errorf("Expected count of category A to be 3, got %d", categoryToCount["A"])
	}
	if categoryToCount["B"] != 2 {
		t.Errorf("Expected count of category B to be 2, got %d", categoryToCount["B"])
	}
	if categoryToCount["C"] != 2 {
		t.Errorf("Expected count of category C to be 2, got %d", categoryToCount["C"])
	}
}

func TestGroupByWithDifferentAggregationTypes(t *testing.T) {
	// Tests grouping with different types of aggregation functions (Mean, Count)
	df := createTestDataFrame()

	// Group with different aggregation types
	result := aggregate.GroupBy(
		df,
		[]string{"category"},
		map[string]aggregate.Aggregator{
			"sales": aggregate.Mean(),
			"count": aggregate.Count(),
		},
	)

	// Verify mean aggregation
	salesSeries := result.GetSeries("sales")
	categorySeries := result.GetSeries("category")

	if categorySeries == nil || salesSeries == nil {
		t.Fatal("Expected category and sales series in the result")
	}

	// Create a map to verify each category's mean
	categoryToMean := make(map[string]float64)
	for i := 0; i < result.Height(); i++ {
		category := categorySeries.Get(i).(string)
		mean := salesSeries.Get(i).(float64)
		categoryToMean[category] = mean
	}

	// A: (100 + 150 + 120) / 3 = 123.33
	if categoryToMean["A"] < 123.0 || categoryToMean["A"] > 124.0 {
		t.Errorf("Expected mean of category A to be around 123.33, got %f", categoryToMean["A"])
	}

	// B: (200 + 250) / 2 = 225.0
	if categoryToMean["B"] != 225.0 {
		t.Errorf("Expected mean of category B to be 225.0, got %f", categoryToMean["B"])
	}

	// C: (300 + 180) / 2 = 240.0
	if categoryToMean["C"] != 240.0 {
		t.Errorf("Expected mean of category C to be 240.0, got %f", categoryToMean["C"])
	}
}
