# GoFrames

A simple dataframe library in golang

## Overview

GoFrames is a lightweight and efficient dataframe library for Go, designed to handle structured data with ease. It provides a familiar interface for data manipulation, analysis, and transformation, similar to pyspark and pandas in Python but with Go's performance benefits.

## Installation

```bash
go get github.com/username/goframes
```

## Features

- Fast and memory-efficient data manipulation
- Support for various data types
- Column and row-based operations
- Data filtering and transformation
- Import/export data from/to CSV, JSON, and other formats
- Statistical functions and aggregations

## Usage Examples

### Creating DataFrames

```go
package main

import (
	"fmt"
	"github.com/username/goframes/dataframe"
)

func main() {
	// Create an empty DataFrame
	df := dataframe.NewDataFrame()
	
	// Create DataFrame with specific series
	nameSeries := dataframe.NewStringSeries("Name", []string{"Alice", "Bob", "Charlie"})
	ageSeries := dataframe.NewIntSeries("Age", []int{25, 30, 35})
	citySeries := dataframe.NewStringSeries("City", []string{"New York", "London", "Tokyo"})
	
	df = dataframe.NewDataFrame(nameSeries, ageSeries, citySeries)
	fmt.Println("DataFrame dimensions:", df.Shape())
	
	// Adding a new series to an existing DataFrame
	countrySeries := dataframe.NewStringSeries("Country", []string{"USA", "UK", "Japan"})
	df.AddSeries(countrySeries)
	
	// Adding a row to the DataFrame
	df.AddRow([]any{"Dave", 40, "Paris", "France"})
}
```

### Reading Data

```go
package main

import (
	"fmt"
	"github.com/username/goframes/dataframe"
)

func main() {
	// Reading from CSV file
	df, err := dataframe.Read().
		FileType("csv").
		FilePath("data.csv").
		Option("header", true).
		Option("inferDataTypes", true).
		Load()
	
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	
	// Reading from a string
	csvData := `name,age,city
Alice,25,New York
Bob,30,London
Charlie,35,Tokyo`
	
	dfFromString, err := dataframe.Read().
		FromString(csvData).
		Option("header", true).
		Option("inferDataTypes", true).
		Load()
		
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	
	// Reading from Parquet file
	parquetDF, err := dataframe.ReadParquet("data.parquet")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

    // Write to CSV file
	err := parquetDF.Write().
		FileType("csv").
		FilePath("output.csv").
		Option("header", true).
		Save()
}
```

### Manipulating DataFrames

```go
package main

import (
	"fmt"
	"github.com/username/goframes/dataframe"
)

func main() {
	// Create sample data
	df := dataframe.NewDataFrame(
		dataframe.NewStringSeries("Name", []string{"Alice", "Bob", "Charlie", "Dave"}),
		dataframe.NewIntSeries("Age", []int{25, 30, 35, 40}),
		dataframe.NewStringSeries("City", []string{"New York", "London", "Tokyo", "Paris"}),
	)
	
	// Select specific columns
	subset := df.Select("Name", "Age")
	
	// Filter rows using the FilterIndex method
	adults := df.FilterIndex(func(values ...any) bool {
		age := values[0].(int)
		return age >= 30
	}, "Age")
	
	// Filter rows using the FilterMap method
	europeans := df.FilterMap(func(row map[string]any) bool {
		city := row["City"].(string)
		return city == "London" || city == "Paris"
	})
	
	// Rename a column
	df.Rename("City", "Location")
	
	// Add a new calculated column
	df.ApplyIndex("AgeNextYear", func(values ...any) any {
		age := values[0].(int)
		return age + 1
	}, "Age")
	
	// Add a column calculated from multiple columns
	df.ApplyMap("NameAndCity", func(row map[string]any) any {
		return fmt.Sprintf("%s from %s", row["Name"], row["Location"])
	})
	
	// Drop a column
	df.DropColumn("Age")
	
	// Drop a row
	df.DropRow(1) // Drops the second row (index 1)
	
	// Change column data type
	df.AsType("Age", "string")
}
```

### Type Conversions

```go
package main

import (
	"fmt"
	"github.com/username/goframes/dataframe"
)

func main() {
	// Create a DataFrame with string values
	df := dataframe.NewDataFrame(
		dataframe.NewStringSeries("ID", []string{"1", "2", "3", "4"}),
		dataframe.NewStringSeries("Value", []string{"10.5", "15.2", "8.7", "20.1"}),
		dataframe.NewStringSeries("Active", []string{"true", "false", "true", "true"}),
	)
	
	// Convert string columns to appropriate types
	df.AsType("ID", "int")
	df.AsType("Value", "float")
	df.AsType("Active", "bool")
	
	// Now the columns are properly typed for calculations
	valueSeries := df.GetSeries("Value")
	fmt.Printf("Value column type: %v\n", valueSeries.Type())
	
	// Convert numeric columns to string for display
	df.AsType("ID", "string")
	df.AsType("Value", "string")
}
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
