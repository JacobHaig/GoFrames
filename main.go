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

	df3 := df.Apply("Full Name", func(a ...interface{}) interface{} {
		return a[0].(string) + " " + a[1].(string)
	}, "First Name", "Last Name")
	df3.Select("Full Name", "First Name", "Last Name").PrintTable()

	df4 := df.Apply(
		"Full Address", func(a ...interface{}) interface{} {
			return a[0].(string) + " " + a[1].(string) + " " + a[2].(string) + " " + a[3].(string)
		}, "Address", "City", "State", "Zip")
	df4.Select("Full Address", "Address", "City", "State", "Zip").PrintTable()

	df4.Drop("Full Address")

	// This version allows use to return a different type.
	df5 := df.Apply("Age Int", func(a ...interface{}) interface{} {
		i, _ := strconv.Atoi(a[0].(string))
		return i
	}, "Age")
	df5.Select("Age Int", "Age").PrintTable()

	df5.Rename("First Name", "First")
	df5.PrintTable()
	df5.Rename("First", "First Name")

	// This version allows use to use any type and return any type.
	// We are required to assert the type we are using.
	df6 := df5.Apply("Age Squared", func(a ...interface{}) interface{} {
		return a[0].(int) * a[0].(int)
	}, "Age Int")
	df6.Select("Age Squared", "Age Int", "Age").PrintTable()

	df7 := df6.Drop("Age Squared")
	df7.PrintTable()

	// This version allows you to use a map to access the columns directly.
	df8 := df5.ApplyMap("Full Address", func(a map[string]interface{}) interface{} {
		return a["Address"].(string) + " " + a["City"].(string) + " " + a["State"].(string) + " " + a["Zip"].(string)
	})
	df8.Select("Full Address", "Address", "City", "State", "Zip").PrintTable()

	// Finish by writing the DataFrame to a CSV file
	// err1 := df8.WriteCSV("data/addresses_out.csv")
	// if err1 != nil {
	// 	fmt.Print(err)
	// 	os.Exit(1)
	// }
}
