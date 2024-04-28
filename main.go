package main

import (
	"fmt"
	dataframe "goframe/dataframe"
	"os"
	"strconv"
)

func main() {

	df, err := dataframe.ReadCSV("data/addresses.csv", dataframe.Options{
		"delimiter":        ',',
		"trimleadingspace": true,
		"header":           true,
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// df1 := df.Select("First Name", "Last Name", "Age")
	// df1.PrintTable()

	// Iteration 1
	// df2 := df.Apply("Full Name",
	// 	"First Name", "Last Name",
	// 	func(a, b string) string {
	// 		return a + " " + b
	// 	})
	// df2.Select("Full Name", "First Name", "Last Name").PrintTable()

	// // This version takes a variadic number of columns
	// df3 := df.Apply2("Full Name",
	// 	func(a ...string) string {
	// 		return a[0] + " " + a[1]
	// 	},
	// 	"First Name", "Last Name",
	// )
	// df3.Select("Full Name", "First Name", "Last Name").PrintTable()

	// // This version takes a variadic number of columns.
	// // This is showing that you can pass any number of columns.
	// df4 := df.Apply2(
	// 	"Full Address",
	// 	func(a ...string) string {
	// 		return a[0] + " " + a[1] + " " + a[2] + " " + a[3]
	// 	},
	// 	"Address", "City", "State", "Zip",
	// )
	// df4.Select("Full Address", "Address", "City", "State", "Zip").PrintTable()

	// This version allows use to return a different type.
	df5 := df.Apply3("Age Int",
		func(a ...string) interface{} {
			i, _ := strconv.Atoi(a[0])
			return i
		},
		"Age",
	)
	df5.Select("Age Int", "Age").PrintTable()

	// This version allows use to use any type and return any type.
	// We are required to assert the type we are using.
	df6 := df5.Apply4("Age Squared",
		func(a ...interface{}) interface{} {
			return a[0].(int) * a[0].(int)
		},
		"Age Int",
	)
	df6.Select("Age Squared", "Age Int", "Age").PrintTable()

	// Test Droping Columns
	df7 := df6.Drop("Age Squared", "Age Int")
	df7.PrintTable()

	// This shows you can pass a struct of column names.
	df8 := df5.Apply4("Age Squared",
		func(a ...interface{}) interface{} {
			return a[0].(int) * a[0].(int)
		},
		[]string{"Age Int"},
	)
	df8.Select("Age Squared", "Age Int", "Age").PrintTable()

	// Finish by writing the DataFrame to a CSV file
	// err1 := df8.WriteCSV("data/addresses_out.csv")
	// if err1 != nil {
	// 	fmt.Print(err)
	// 	os.Exit(1)
	// }
}
