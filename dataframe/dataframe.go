package dataframe

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
)

type DataFrame struct {
	Series []Series
}

type Series struct {
	Values []interface{}
	Name   string
}

func (df *DataFrame) allColumnsExist(columnNames []string) bool {
	for _, columnName := range columnNames {
		if _, ok := df.GetColumnIndex(columnName); !ok {
			return false
		}
	}
	return true
}

func (df *DataFrame) findColumnsThatDontExist(columnNames []string) []string {
	columns := []string{}
	for _, columnName := range columnNames {
		if _, ok := df.GetColumnIndex(columnName); !ok {
			columns = append(columns, columnName)
		}
	}
	return columns
}

// GetColumn returns the column names based on the selected columns.
//
// The selectedColumns can be a string, slice of strings, int, or slice of ints.
// If the selectedColumns are strings, the function will return the column names
// as strings. If the selectedColumns are ints, the function will return the
// column names as ints.
//
// The function returns an error if one of the columns does not exist.
func (df *DataFrame) GetColumn(selectedColumns ...interface{}) ([]string, error) {

	if len(selectedColumns) == 0 {
		return []string{}, nil
	}

	switch selectedColumns[0].(type) {
	case []string, string:
		columnNames := InterfaceToTypeSlice[string](selectedColumns)

		// Check if all columns exist
		allExist := df.allColumnsExist(columnNames)

		if allExist {
			return columnNames, nil
		} else {
			columns := df.findColumnsThatDontExist(columnNames)
			return nil, errors.New("One of these columns do not exist: " + SprintfStringSlice(columns))
		}

	case []int, int:
		columnIndexes := InterfaceToTypeSlice[int](selectedColumns)

		columnNames := []string{}
		for _, index := range columnIndexes {
			if index < 0 || index >= len(df.Series) {
				return nil, errors.New("Index out of range: " + fmt.Sprint(index))
			}
			columnNames = append(columnNames, df.Series[index].Name)
		}
		return columnNames, nil
	}

	return []string{}, nil
}

func (df *DataFrame) Drop(selectedColumn ...interface{}) *DataFrame {

	if len(df.Series) == 0 {
		return &DataFrame{}
	}

	// Check if all values are of the same type
	columns, err := df.GetColumn(selectedColumn...)
	if err != nil {
		fmt.Println(err)
		return &DataFrame{}
	}

	for _, columnName := range columns {
		for index, series := range df.Series {
			if series.Name == columnName {
				df.Series = append(df.Series[:index], df.Series[index+1:]...)
			}
		}
	}

	return df
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

	columnNames, err := df.GetColumn(selectedColumn...)
	if err != nil {
		fmt.Println(err)
		return &DataFrame{}
	}

	newSeries := []Series{}
	for _, columnName := range columnNames {
		for _, series := range df.Series {
			if series.Name == columnName {
				newSeries = append(newSeries, series)
			}
		}
	}
	return &DataFrame{newSeries}
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
		seriesValues := InterfaceToTypeSlice[string](series.Values)
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
		fmt.Println("Error creating file:", path)
		fmt.Println(err)
		os.Exit(1)
		return err
	}
	defer file.Close()

	csvWriter := csv.NewWriter(file)
	csvWriter.Comma = ','

	err1 := csvWriter.WriteAll(columns)
	if err1 != nil {
		fmt.Println("Error writing to file:", path)
		fmt.Println(err1)
		os.Exit(1)
	}

	return nil
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
		columnNames := InterfaceToTypeSlice[string](cols)

		for _, columnName := range columnNames {
			columnIndex, columnExist := df.GetColumnIndex(columnName)
			columnIndexs = append(columnIndexs, columnIndex)
			columnExists = append(columnExists, columnExist)
		}

	case []int, int:
		columnIndexes := InterfaceToTypeSlice[int](cols)

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
		columnNames := InterfaceToTypeSlice[string](cols)

		for _, columnName := range columnNames {
			columnIndex, columnExist := df.GetColumnIndex(columnName)
			columnIndexs = append(columnIndexs, columnIndex)
			columnExists = append(columnExists, columnExist)
		}

	case []int, int:
		columnIndexes := InterfaceToTypeSlice[int](cols)

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
		columnNames := InterfaceToTypeSlice[string](cols)

		for _, columnName := range columnNames {
			columnIndex, columnExist := df.GetColumnIndex(columnName)
			columnIndexs = append(columnIndexs, columnIndex)
			columnExists = append(columnExists, columnExist)
		}

	case []int, int:
		columnIndexes := InterfaceToTypeSlice[int](cols)

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

func (df *DataFrame) Apply5(newColumnName string, f func(...interface{}) interface{}, cols ...interface{}) *DataFrame {

	// Check if all values are of the same type
	if !allSameType(cols) {
		fmt.Println("All values must be of the same type")
		return &DataFrame{}
	}

	// Get the column names
	columns, err := df.GetColumn(cols...)
	if err != nil {
		fmt.Println(err)
		return &DataFrame{}
	}

	// Get the column indexes
	columnIndexs := []int{}
	for _, columnName := range columns {
		columnIndex, _ := df.GetColumnIndex(columnName)
		columnIndexs = append(columnIndexs, columnIndex)
	}

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
	df.Series = append(df.Series, Series{newValues, newColumnName})

	return df
}