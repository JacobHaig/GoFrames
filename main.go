package main

import (
	"fmt"
	"runtime"

	"teddy/dataframe"
)

func memUsage(m1, m2 *runtime.MemStats) {
	fmt.Printf("Alloc: %d MB, TotalAlloc: %d MB, HeapAlloc: %d MB\n",
		(m2.Alloc-m1.Alloc)/1024/1024,
		(m2.TotalAlloc-m1.TotalAlloc)/1024/1024,
		(m2.HeapAlloc-m1.HeapAlloc)/1024/1024)
}

func main() {
	// Memory usage tracking
	var m1, m2, m3, m4 runtime.MemStats
	runtime.ReadMemStats(&m1)

	// Create a DataFrame with untyped Series (original approach)
	untypedDF := dataframe.NewDataFrame()
	// Create a large series with 1 million integers
	intValues := make([]any, 1000000)
	for i := 0; i < 1000000; i++ {
		intValues[i] = i
	}
	untypedDF = untypedDF.AddSeries(dataframe.NewGenericSeries("integers", intValues))

	// Measure memory after creating untyped DataFrame
	runtime.ReadMemStats(&m2)
	fmt.Println("Memory used by untyped DataFrame:")
	memUsage(&m1, &m2)

	// Reset memory stats
	runtime.GC()
	runtime.ReadMemStats(&m3)

	// Create a DataFrame with typed Series (new approach)
	typedValues := make([]int, 1000000)
	for i := 0; i < 1000000; i++ {
		typedValues[i] = i
	}
	typedDF := dataframe.NewDataFrame()
	typedDF = typedDF.AddSeries(dataframe.NewIntSeries("integers", typedValues))

	// Measure memory after creating typed DataFrame
	runtime.ReadMemStats(&m4)
	fmt.Println("Memory used by typed DataFrame:")
	memUsage(&m3, &m4)

	// Compare total memory usage
	fmt.Println("\nMemory savings:")
	memSavings := float64(m2.Alloc-m1.Alloc) / float64(m4.Alloc-m3.Alloc)
	fmt.Printf("Typed Series uses %.2fx less memory than untyped Series\n", memSavings)

	// Basic operations demonstration
	fmt.Println("\nExample operations:")

	// Create a simple DataFrame for operations
	df := dataframe.NewDataFrame()
	df = df.AddSeries(dataframe.NewIntSeries("Age", []int{25, 30, 35, 40, 45}))
	df = df.AddSeries(dataframe.NewStringSeries("Name", []string{"Alice", "Bob", "Charlie", "David", "Eve"}))
	df = df.AddSeries(dataframe.NewFloat64Series("Salary", []float64{50000.0, 60000.0, 70000.0, 80000.0, 90000.0}))

	// Display the DataFrame
	fmt.Println("Original DataFrame:")
	df.PrintTable()

	// Filter rows
	filteredDF := df.FilterMap(func(m map[string]any) bool {
		return m["Age"].(int) > 30
	})

	fmt.Println("\nFiltered DataFrame (Age > 30):")
	filteredDF.PrintTable()

	// Add a new column
	df = df.ApplyMap("Bonus", func(m map[string]any) any {
		return m["Salary"].(float64) * 0.1
	})

	fmt.Println("\nDataFrame with Bonus column:")
	df.PrintTable()

	// Group by and aggregate
	groupedDF := df.GroupByIndex("Age", dataframe.Sum, "Salary", "Bonus")

	fmt.Println("\nGrouped DataFrame (Sum of Salary and Bonus by Age):")
	groupedDF.PrintTable()
}
