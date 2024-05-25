package main

import (
	"fmt"
	dataframe "goframe/dataframe"
	"os"
)

func test2() {
	df, err := dataframe.ReadCSV("data/addresses.csv", dataframe.Options{
		"delimiter":        ',',
		"trimleadingspace": true,
		"header":           true,
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	df3 := df.ApplyIndex("Full Name", func(a ...interface{}) interface{} {
		return a[0].(string) + " " + a[1].(string)
	}, "First Name", "Last Name")

	df3.PrintTable()

	series := df3.GetSeries("Full Name", dataframe.Options{"copy": true})
	df3.DropColumn("Full Name")
	df3.PrintTable()

	for i := range series.Values {
		series.Values[i] = series.Values[i].(string) + "!"
	}

	df3.AddSeries(series)
	df3.PrintTable()
	df3.DropColumn("Full Name")
	df3.PrintTable()

	df5 := df3.FilterIndex(func(a ...interface{}) bool {
		return a[0].(string) != "Tyler"
	}, "Last Name")
	df5.PrintTable()

	df4 := df3.FilterMap(func(m map[string]interface{}) bool {
		return m["First Name"].(string) != "Jack"
	})
	df4.PrintTable()
}

func main() {
	// read csv
	df, err := dataframe.ReadCSV("data/addresses.csv", dataframe.Options{
		"delimiter":        ',',
		"trimleadingspace": true,
		"header":           true,
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println(df.ColumnNames())
	fmt.Println(df.Shape())

	df.DropColumn("First Name", "Last Name", "Address", "City", "State", "Zip")
	// df.PrintTable()
	df.GetSeries("Age").AsType("float")

	df2 := df.ApplyMap("Age2", func(m map[string]interface{}) interface{} {
		return m["Age"].(float64) * 2.56
	})
	df2.GetSeries("Age2").AsType("string")

	// df2.PrintTable()

	df3 := df2.ApplyMap("Age2", func(m map[string]interface{}) interface{} {
		return m["Age2"].(string) + " = idk"
	})
	df3.GetSeries("Age2").AsType("string")
	// df3.PrintTable()
}
