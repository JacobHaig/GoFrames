package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

type Series struct {
	Values []interface{}
	Name   string
}

type DataFrame struct {
	Series []Series
}

func ReadCSVtoRows(path string, options ...Options) ([][]string, error) {
	// Standardize the keys
	optionsClean := standardizeMapKeys(options...)

	// Read the file
	data, err := os.ReadFile(path)
	if err != nil {
		log.SetFlags(log.LstdFlags | log.Lshortfile)
		log.Println("Error reading file:", path)
		log.Fatal(err)
		return [][]string{}, err
	}

	// Create a CSV Reader
	stringReader := strings.NewReader(string(data))
	csvReader := csv.NewReader(stringReader)

	// Get Delimiter
	if val, ok := optionsClean["delimiter"]; ok {
		csvReader.Comma = val.(rune)
	} else {
		csvReader.Comma = ','
	}

	if val, ok := optionsClean["trimleadingspace"]; ok {
		csvReader.TrimLeadingSpace = val.(bool) // Can cause issues if the delimiter is a space or tab
	}

	// Prevent incompatible options
	if csvReader.TrimLeadingSpace && (csvReader.Comma == ' ' || csvReader.Comma == '\t') {
		return nil, errors.New("error: trimleadingspace is true, but the delimiter is a space or tab. these are incompatible options")
	}

	if val, ok := optionsClean["debug"]; ok {
		if val.(bool) {
			fmt.Println("Delimiter:", "("+string(csvReader.Comma)+")")
			fmt.Println("TrimLeadingSpace:", csvReader.TrimLeadingSpace)
		}
	}

	// Read the CSV
	rows, err := csvReader.ReadAll()
	if err != nil {
		// ParseError
		if _, ok := err.(*csv.ParseError); ok {
			log.Println("Error: CSV file has parse error")
			log.Println("This occurred while parsing the following file:", path)
		}
		log.Fatal(err)
	}

	return rows, nil
}

func ReadCSV(path string, options ...Options) (*DataFrame, error) {
	// Standardize the keys
	optionsClean := standardizeMapKeys(options...)

	// Read the file
	rows, err := ReadCSVtoRows(path, optionsClean)
	if err != nil {
		return &DataFrame{}, err
	}

	// Create the DataFrame
	df := NewFromRows(rows, optionsClean)

	return df, nil
}

func TransposeRows(rows [][]string) [][]string {
	// Create a new 2D array
	transposed := make([][]string, len(rows[0]))
	for i := range transposed {
		transposed[i] = make([]string, len(rows))
	}

	// Transpose the 2D array
	for rowIndex, row := range rows {
		for colIndex, cell := range row {
			transposed[colIndex][rowIndex] = cell
		}
	}

	return transposed
}

func NewFromRows(rows [][]string, options ...Options) *DataFrame {
	// Standardize the keys
	optionsClean := standardizeMapKeys(options...)

	// Prefill header with default values
	header := []string{}
	for index := range len(rows[0]) {
		header = append(header, fmt.Sprintf("Column %d", index))
	}

	// Check if header is present
	if val, ok := optionsClean["header"]; ok {
		if val.(bool) {
			header = rows[0]
			rows = rows[1:]
		}
	}

	// Transpose the rows
	rows = TransposeRows(rows)

	// Create the Series
	series := []Series{}

	for index, row := range rows {
		newRow := []interface{}{}
		for _, cell := range row {
			newRow = append(newRow, cell)
		}
		series = append(series, Series{newRow, header[index]})
	}

	return &DataFrame{series}
}

// Select returns a new DataFrame with the selected columns.
//
// Select does not create a copy of the data, it only creates a new DataFrame
// with the referances to the original data.
// The columnNames can be a string, slice of strings, int, or slice of ints.
func (df *DataFrame) Select(selectedColumn ...interface{}) *DataFrame {

	// Variadic functions arguments are passed as a slice.
	// If a type T is passed in, we access it as a slice of T.
	// If a slice of T is passed in, we access it by taking
	// the first element, which is a slice of T.

	if len(df.Series) == 0 {
		return &DataFrame{}
	}

	// Check if all values are of the same type
	if !allSameType(selectedColumn) {
		fmt.Println("All values must be of the same type")
		return &DataFrame{}
	}

	// Check the type of the first value. We have to know the
	// inner type of the slice to be able to work with it.
	// If the inner type is a slice, we need to change it to
	// the correct type. If the inner type is a string, we
	// If the inner type is an interface{}, we fail.
	switch selectedColumn[0].(type) {

	case []string, string:
		columnNames := FlattenInterfaceToTypeSlice[string](selectedColumn)

		newSeries := []Series{}
		for _, columnName := range columnNames {
			for _, series := range df.Series {
				if series.Name == columnName {
					newSeries = append(newSeries, series)
				}
			}
		}
		return &DataFrame{newSeries}

	case []int, int:
		columnIndexes := FlattenInterfaceToTypeSlice[int](selectedColumn)

		newSeries := []Series{}
		for _, index := range columnIndexes {
			if index < 0 || index >= len(df.Series) {
				fmt.Println("Index out of range")
				return &DataFrame{}
			}
			newSeries = append(newSeries, df.Series[index])
		}
		return &DataFrame{newSeries}
	}

	return &DataFrame{}
}

func (df *DataFrame) PrintTable() {
	if len(df.Series) == 0 {
		fmt.Println("Empty DataFrame")
		return
	}

	// Calculate the max width of each column
	widths := make([]int, len(df.Series))

	// max header
	for seriesIndex, series := range df.Series {
		widths[seriesIndex] = max(widths[seriesIndex], len(series.Name))

		for rowIndex := 0; rowIndex < len(df.Series[0].Values); rowIndex++ {
			widths[seriesIndex] = max(widths[seriesIndex], len(fmt.Sprint(series.Values[rowIndex])))
		}
	}

	// Print the header separator
	fmt.Print("+-")
	for index := range df.Series {
		fmt.Print(PadRight("", "-", widths[index]))
		if index < len(df.Series)-1 {
			fmt.Print("-+-")
		}
	}
	fmt.Println("-+ ")

	// Print the header
	fmt.Print("| ")
	for index, series := range df.Series {
		fmt.Print(PadRight(series.Name, " ", widths[index]))
		if index < len(df.Series)-1 {
			fmt.Print(" | ")
		}
	}
	fmt.Println(" |")

	// Print the body separator
	fmt.Print("+-")
	for index, width := range widths {
		fmt.Print(PadRight("", "-", width))
		if index < len(df.Series)-1 {
			fmt.Print("-+-")
		}
	}
	fmt.Println("-+")

	// Print the DataFrame
	for rowIndex := 0; rowIndex < len(df.Series[0].Values); rowIndex++ {
		fmt.Print("| ")
		for colIndex, series := range df.Series {
			fmt.Print(PadRight(fmt.Sprint(series.Values[rowIndex]), " ", widths[colIndex]))
			if colIndex < len(df.Series)-1 {
				fmt.Print(" | ")
			}
		}
		fmt.Println(" |")
	}

	// Print the footer separator
	fmt.Print("+-")
	for index := range df.Series {
		fmt.Print(PadRight("", "-", widths[index]))
		if index < len(df.Series)-1 {
			fmt.Print("-+-")
		}
	}
	fmt.Println("-+")
}

func (df *DataFrame) Print() {
	if len(df.Series) == 0 {
		fmt.Println("Empty DataFrame")
		return
	}

	// Print the header
	for index, series := range df.Series {
		fmt.Print(series.Name)
		if index < len(df.Series)-1 {
			fmt.Print(", ")
		}
	}
	fmt.Println("")

	// Print the DataFrame
	for rowIndex := 0; rowIndex < len(df.Series[0].Values); rowIndex++ {
		for colIndex, series := range df.Series {
			fmt.Print(series.Values[rowIndex])
			if colIndex < len(df.Series)-1 {
				fmt.Print(", ")
			}
		}
		fmt.Println("")
	}
}

func (df *DataFrame) GetColumnNames() []string {
	columns := []string{}
	for _, series := range df.Series {
		columns = append(columns, series.Name)
	}
	return columns
}

func (df *DataFrame) GetColumnIndex(columnName string) (int, bool) {
	for index, series := range df.Series {
		if series.Name == columnName {
			return index, true
		}
	}
	return -1, false
}

// WriteCSV writes the DataFrame to a CSV file.
//
// The options can be used to control the output.
// The options are:
//   - header: bool (default: false)
//     Whether to include the header in the output.
func (df *DataFrame) WriteCSV(path string, options ...Options) error {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Standardize the keys
	optionsClean := standardizeMapKeys(options...)

	header := []string{}
	if val, ok := optionsClean["header"]; ok {
		if val.(bool) {
			header = df.GetColumnNames()
		}
	}

	columns := [][]string{} // Todo: Change to [][]interface{}
	for _, series := range df.Series {
		seriesValues := FlattenInterfaceToTypeSlice[string](series.Values)
		columns = append(columns, seriesValues)
	}

	// Transpose the columns
	columns = TransposeRows(columns)

	// Add the header
	if len(header) > 0 {
		columns = append([][]string{header}, columns...)
	}

	println("Columns:")
	for _, column := range columns {
		fmt.Println(column)
	}

	// Write the file
	file, err := os.Create(path)
	if err != nil {
		log.Println("Error creating file:", path)
		log.Fatal(err)
		return err
	}
	defer file.Close()

	csvWriter := csv.NewWriter(file)
	csvWriter.Comma = ','

	err1 := csvWriter.WriteAll(columns)
	if err1 != nil {
		log.Println("Error writing to file:", path)
		log.Fatal(err1)
		return err1
	}

	return nil
}

func addString(a, b string) string {
	return a + b
}

func (df *DataFrame) Apply(newColumnName string, col1, col2 interface{}, f func(string, string) string) *DataFrame {

	col1Index, col1Exists := -1, false
	col2Index, col2Exists := -1, false

	// Check if the columns exist as strings
	_, ok1 := col1.(string)
	_, ok2 := col2.(string)
	if ok1 && ok2 {
		col1Index, col1Exists = df.GetColumnIndex(col1.(string))
		col2Index, col2Exists = df.GetColumnIndex(col2.(string))
	}

	// Check if the columns exist as ints
	_, ok3 := col1.(int)
	_, ok4 := col2.(int)
	if ok3 && ok4 {
		col1Index = col1.(int)
		col2Index = col2.(int)

		// Check if the columns exist
		if col1Index < 0 || col1Index >= len(df.Series) {
			col1Exists = false
		}
		if col2Index < 0 || col2Index >= len(df.Series) {
			col2Exists = false
		}
	}

	if !col1Exists || !col2Exists {
		fmt.Println("One or more columns do not exist")
		return &DataFrame{}
	}

	// Create the new column
	newValues := []interface{}{}
	for i := 0; i < len(df.Series[0].Values); i++ {
		newValue := f(df.Series[col1Index].Values[i].(string), df.Series[col2Index].Values[i].(string))
		newValues = append(newValues, newValue)
	}

	// Add the new column to the DataFrame
	newSeries := []Series{}
	for i, series := range df.Series {
		if i == col1Index {
			newSeries = append(newSeries, Series{newValues, newColumnName})
		}
		newSeries = append(newSeries, series)
	}

	return &DataFrame{newSeries}
}

func allTrue(values []bool) bool {
	for _, value := range values {
		if !value {
			return false
		}
	}
	return true
}

func (df *DataFrame) Apply2(newColumnName string, f func(...string) string, cols ...interface{}) *DataFrame {

	// Check if all values are of the same type
	if !allSameType(cols) {
		fmt.Println("All values must be of the same type")
		return &DataFrame{}
	}

	columnIndexs := []int{}
	columnExists := []bool{}

	switch cols[0].(type) {
	case []string, string:
		columnNames := FlattenInterfaceToTypeSlice[string](cols)

		for _, columnName := range columnNames {
			columnIndex, columnExist := df.GetColumnIndex(columnName)
			columnIndexs = append(columnIndexs, columnIndex)
			columnExists = append(columnExists, columnExist)
		}

	case []int, int:
		columnIndexes := FlattenInterfaceToTypeSlice[int](cols)

		for _, columnIndex := range columnIndexes {
			columnIndexs = append(columnIndexs, columnIndex)
			if columnIndex >= 0 || columnIndex < len(df.Series) {
				columnExists = append(columnExists, true)
			} else {
				columnExists = append(columnExists, false)
			}
		}
	}

	// Check if all columns exist
	if len(columnExists) == 0 && len(columnIndexs) == 0 {
		fmt.Println("No columns provided")
		return &DataFrame{}
	}

	if !allTrue(columnExists) {
		fmt.Println("One or more columns do not exist")
		return &DataFrame{}
	}

	fmt.Println(columnIndexs)

	// Create the new column
	newValues := []interface{}{}
	for i := 0; i < len(df.Series[0].Values); i++ {

		// List of Values to be used
		values := []string{}
		for _, columnIndex := range columnIndexs {
			values = append(values, df.Series[columnIndex].Values[i].(string))
		}

		newValue := f(values...)
		newValues = append(newValues, newValue)
	}

	// Add the new column to the DataFrame
	newSeries := []Series{}
	for i, series := range df.Series {
		if i == columnIndexs[0] {
			newSeries = append(newSeries, Series{newValues, newColumnName})
		}
		newSeries = append(newSeries, series)
	}

	return &DataFrame{newSeries}
}

func (df *DataFrame) Apply3(newColumnName string, f func(...string) interface{}, cols ...interface{}) *DataFrame {

	// Check if all values are of the same type
	if !allSameType(cols) {
		fmt.Println("All values must be of the same type")
		return &DataFrame{}
	}

	columnIndexs := []int{}
	columnExists := []bool{}

	switch cols[0].(type) {
	case []string, string:
		columnNames := FlattenInterfaceToTypeSlice[string](cols)

		for _, columnName := range columnNames {
			columnIndex, columnExist := df.GetColumnIndex(columnName)
			columnIndexs = append(columnIndexs, columnIndex)
			columnExists = append(columnExists, columnExist)
		}

	case []int, int:
		columnIndexes := FlattenInterfaceToTypeSlice[int](cols)

		for _, columnIndex := range columnIndexes {
			columnIndexs = append(columnIndexs, columnIndex)
			if columnIndex >= 0 || columnIndex < len(df.Series) {
				columnExists = append(columnExists, true)
			} else {
				columnExists = append(columnExists, false)
			}
		}
	}

	// Check if all columns exist
	if len(columnExists) == 0 && len(columnIndexs) == 0 {
		fmt.Println("No columns provided")
		return &DataFrame{}
	}

	if !allTrue(columnExists) {
		fmt.Println("One or more columns do not exist")
		return &DataFrame{}
	}

	fmt.Println(columnIndexs)

	// Create the new column
	newValues := []interface{}{}
	for i := 0; i < len(df.Series[0].Values); i++ {

		// List of Values to be used
		values := []string{}
		for _, columnIndex := range columnIndexs {
			values = append(values, df.Series[columnIndex].Values[i].(string))
		}

		newValue := f(values...)
		newValues = append(newValues, newValue)
	}

	// Add the new column to the DataFrame
	newSeries := []Series{}
	for i, series := range df.Series {
		if i == columnIndexs[0] {
			newSeries = append(newSeries, Series{newValues, newColumnName})
		}
		newSeries = append(newSeries, series)
	}

	return &DataFrame{newSeries}
}

func (df *DataFrame) Apply4(newColumnName string, f func(...interface{}) interface{}, cols ...interface{}) *DataFrame {

	// Check if all values are of the same type
	if !allSameType(cols) {
		fmt.Println("All values must be of the same type")
		return &DataFrame{}
	}

	columnIndexs := []int{}
	columnExists := []bool{}

	switch cols[0].(type) {
	case []string, string:
		columnNames := FlattenInterfaceToTypeSlice[string](cols)

		for _, columnName := range columnNames {
			columnIndex, columnExist := df.GetColumnIndex(columnName)
			columnIndexs = append(columnIndexs, columnIndex)
			columnExists = append(columnExists, columnExist)
		}

	case []int, int:
		columnIndexes := FlattenInterfaceToTypeSlice[int](cols)

		for _, columnIndex := range columnIndexes {
			columnIndexs = append(columnIndexs, columnIndex)
			if columnIndex >= 0 || columnIndex < len(df.Series) {
				columnExists = append(columnExists, true)
			} else {
				columnExists = append(columnExists, false)
			}
		}
	}

	// Check if all columns exist
	if len(columnExists) == 0 && len(columnIndexs) == 0 {
		fmt.Println("No columns provided")
		return &DataFrame{}
	}

	if !allTrue(columnExists) {
		fmt.Println("One or more columns do not exist")
		return &DataFrame{}
	}

	fmt.Println(columnIndexs)

	// Create the new column
	newValues := []interface{}{}
	for i := 0; i < len(df.Series[0].Values); i++ {

		// List of Values to be used
		values := []interface{}{}
		for _, columnIndex := range columnIndexs {
			values = append(values, df.Series[columnIndex].Values[i])
		}

		newValue := f(values...)
		newValues = append(newValues, newValue)
	}

	// Add the new column to the DataFrame
	newSeries := []Series{}
	for i, series := range df.Series {
		if i == columnIndexs[0] {
			newSeries = append(newSeries, Series{newValues, newColumnName})
		}
		newSeries = append(newSeries, series)
	}

	return &DataFrame{newSeries}
}

func main() {

	// rows := ReadCSV("addresses.csv")
	// ReadCSV("addresses.csv", Options{"delimiter": ','})
	// rows, _ := ReadCSVtoRows("data/addresses.csv", Options{"delimiter": ',', "trimleadingspace": true})
	// ReadCSV("addresses.tsv", Options{"delimiter": '\t'})

	// df := NewFromRows(rows)
	// df := NewFromRows(rows, Options{"header": true})
	// df := NewFromRows(rows, Options{"header": false})
	// df.Print()

	df, err := ReadCSV("data/addresses.csv", Options{
		"delimiter":        ',',
		"trimleadingspace": true,
		"header":           true,
	})
	if err != nil {
		log.Fatal(err)
	}

	// df1 := df.Select("First Name", "Last Name", "Age")
	// df1.PrintTable()

	// Iteration 1
	df2 := df.Apply("Full Name",
		"First Name", "Last Name",
		func(a, b string) string {
			return a + " " + b
		})
	df2.Select("Full Name", "First Name", "Last Name").PrintTable()

	// This version takes a variadic number of columns
	df3 := df.Apply2("Full Name",
		func(a ...string) string {
			return a[0] + " " + a[1]
		},
		"First Name", "Last Name",
	)
	df3.Select("Full Name", "First Name", "Last Name").PrintTable()

	// This version takes a variadic number of columns.
	// This is showing that you can pass any number of columns.
	df4 := df.Apply2(
		"Full Address",
		func(a ...string) string {
			return a[0] + " " + a[1] + " " + a[2] + " " + a[3]
		},
		"Address", "City", "State", "Zip",
	)
	df4.Select("Full Address", "Address", "City", "State", "Zip").PrintTable()

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

	// This shows you can pass a struct of column names.
	df7 := df5.Apply4("Age Squared",
		func(a ...interface{}) interface{} {
			return a[0].(int) * a[0].(int)
		},
		[]string{"Age Int"},
	)
	df7.Select("Age Squared", "Age Int", "Age").PrintTable()

	// Finish by writing the DataFrame to a CSV file
	err1 := df7.WriteCSV("data/addresses_out.csv")
	if err1 != nil {
		log.Fatal(err)
	}
}
