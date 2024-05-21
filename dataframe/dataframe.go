package dataframe

import (
	"errors"
	"fmt"
	"slices"
)

type DataFrame struct {
	series []*Series
}

func NewDataFrame(series ...*Series) *DataFrame {
	return &DataFrame{series}
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

func (df *DataFrame) Width() int {
	return len(df.series)
}

func (df *DataFrame) Height() int {
	if len(df.series) == 0 {
		return 0
	}
	return len(df.series[0].Values)
}

// Shape returns the height and width of the DataFrame.
//
// The height is the number of rows in the DataFrame.
// The width is the number of columns in the DataFrame.
func (df *DataFrame) Shape() (int, int) {
	if len(df.series) == 0 {
		return 0, 0
	}
	return df.Height(), df.Width()
}

func (df *DataFrame) DropRow(index int) *DataFrame {
	for _, series := range df.series {
		series.DropRow(index)
	}
	return df
}

func (df *DataFrame) DropRows(indexes ...int) *DataFrame {
	// Sort the indexes in reverse order.
	slices.Sort(indexes)
	slices.Reverse(indexes)

	for i := range indexes {
		df.DropRow(indexes[i])
	}
	return df
}

func (df *DataFrame) DropRowsBySeries(series *Series) *DataFrame {
	// Convert the Series to a list of indexes
	indexes := []int{}
	for _, value := range series.Values {
		indexes = append(indexes, value.(int))
	}

	df = df.DropRows(indexes...)

	return df
}

func (df *DataFrame) DropColumn(selectedColumn ...interface{}) *DataFrame {

	if len(df.series) == 0 {
		return &DataFrame{}
	}

	// Check if all values are of the same type
	columns, err := df.GetColumn(selectedColumn...)
	if err != nil {
		fmt.Println(err)
		return &DataFrame{}
	}

	for _, columnName := range columns {
		for index, series := range df.series {
			if series.Name == columnName {
				df.series = slices.Delete(df.series, index, index+1)
				break
			}
		}
	}

	return df
}

func (df *DataFrame) AsType(columnName string, newType string) *DataFrame {
	for _, series := range df.series {
		if series.Name == columnName {
			series.AsType(newType)
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

	if len(df.series) == 0 {
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

	newSeries := []*Series{}
	for _, columnName := range columnNames {
		for _, series := range df.series {
			if series.Name == columnName {
				newSeries = append(newSeries, series)
			}
		}
	}
	return &DataFrame{newSeries}
}

// GetColumn returns the column names based on the selected columns.
//
// The selectedColumns can be a string, slice of strings, int, or slice of ints.
// If the selectedColumns are strings, the function will return the column names
// as strings. If the selectedColumns are ints, the function will also return the
// column names.
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
			if index < 0 || index >= len(df.series) {
				return nil, errors.New("Index out of range: " + fmt.Sprint(index))
			}
			columnNames = append(columnNames, df.series[index].Name)
		}
		return columnNames, nil
	}

	return []string{}, nil
}

// GetSeriesCopy returns a slice of Series based on the column name.
//
// The function returns a completely new slice of Series. This means that
// the original DataFrame is not affected by the function.
func (df *DataFrame) GetSeriesCopy(columnName string) *Series {
	series := &Series{}
	for _, s := range df.series {
		if s.Name == columnName {
			return s.Copy(true)
		}
	}
	return series
}

// GetSeriesCopy returns a slice of Series based on the column name.
//
// The function returns a referance to the original Series.
func (df *DataFrame) GetSeries(columnName string) *Series {
	series := &Series{}
	for _, s := range df.series {
		if s.Name == columnName {
			return s
		}
	}
	return series
}

func (df *DataFrame) AddSeries(series *Series) *DataFrame {
	// If the DataFrame is empty, add the Series
	if df.Width() == 0 {
		df.series = append(df.series, series)
		return df
	}

	// Check if the Series is the same length as the DataFrame
	if len(series.Values) != df.Height() {
		fmt.Println("Series must be the same length as the DataFrame")
		return df
	}

	df.series = append(df.series, series)
	return df
}

func (df *DataFrame) AddRow(row []interface{}) *DataFrame {
	if df.Width() == 0 {
		return df
	}

	if len(row) != df.Width() {
		fmt.Println("Row must be the same length as the DataFrame")
		return df
	}

	for index, value := range row {
		df.series[index].Values = append(df.series[index].Values, value)
	}

	return df
}

func (df *DataFrame) ColumnNames() []string {
	columns := []string{}
	for _, series := range df.series {
		columns = append(columns, series.Name)
	}
	return columns
}

func (df *DataFrame) GetColumnIndex(columnName string) (int, bool) {
	for index, series := range df.series {
		if series.Name == columnName {
			return index, true
		}
	}
	return -1, false
}

func (df *DataFrame) Rename(oldColumnName, newColumnName string) *DataFrame {
	for index, series := range df.series {
		if series.Name == oldColumnName {
			df.series[index].Name = newColumnName
		}
	}
	return df
}

func (df *DataFrame) ApplyIndex(newColumnName string, f func(...interface{}) interface{}, cols ...interface{}) *DataFrame {

	// // Check if all values are of the same type
	// if !allSameType(cols) {
	// 	fmt.Println("All values must be of the same type")
	// 	return &DataFrame{}
	// }

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
	for i := 0; i < df.Height(); i++ {

		// List of Values to be used
		values := []interface{}{}
		for _, columnIndex := range columnIndexs {
			values = append(values, df.series[columnIndex].Values[i])
		}

		newValue := f(values...)
		newValues = append(newValues, newValue)
	}

	df = df.DropColumn(newColumnName)
	// Add the new column to the DataFrame
	df.series = append(df.series, NewSeries(newColumnName, newValues))

	return df
}

func (df *DataFrame) ApplyMap(newColumnName string, f func(map[string]interface{}) interface{}) *DataFrame {

	columns := df.ColumnNames()

	// Get the column indexes
	columnIndexs := []int{}
	for _, columnName := range columns {
		columnIndex, _ := df.GetColumnIndex(columnName)
		columnIndexs = append(columnIndexs, columnIndex)
	}

	// Create the new column
	newValues := []interface{}{}
	for i := 0; i < df.Height(); i++ {

		// List of Values to be used
		valuemap := map[string]interface{}{}
		for _, columnIndex := range columnIndexs {
			valuemap[df.series[columnIndex].Name] = df.series[columnIndex].Values[i]
		}

		newValue := f(valuemap)
		newValues = append(newValues, newValue)
	}

	df = df.DropColumn(newColumnName)
	// Add the new column to the DataFrame
	df.series = append(df.series, NewSeries(newColumnName, newValues))

	return df
}

func (df *DataFrame) ApplySeries(newColumnName string, f func(...[]interface{}) []interface{}, cols ...interface{}) *DataFrame {

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
	newValue := []interface{}{}
	for i := 0; i < df.Height(); i++ {

		// List of Values to be used
		values := [][]interface{}{}
		for _, columnIndex := range columnIndexs {
			values = append(values, df.series[columnIndex].Values)
		}

		newValue = f(values...)
	}

	df = df.DropColumn(newColumnName)
	// Add the new column to the DataFrame
	df.series = append(df.series, NewSeries(newColumnName, newValue))

	return df
}

func (df *DataFrame) FilterIndex(f func(...interface{}) bool, cols ...interface{}) *DataFrame {
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
	for i := 0; i < df.Height(); i++ {

		// List of Values to be used
		values := []interface{}{}
		for _, columnIndex := range columnIndexs {
			values = append(values, df.series[columnIndex].Values[i])
		}

		boolValue := f(values...)
		newValues = append(newValues, boolValue)
	}

	// Remove the rows that are false
	for i := df.Height() - 1; i >= 0; i-- {
		if !newValues[i].(bool) {
			df.DropRow(i)
		}
	}

	return df
}

func (df *DataFrame) FilterMap(f func(map[string]interface{}) bool) *DataFrame {

	columns := df.ColumnNames()

	// Get the column indexes
	columnIndexs := []int{}
	for _, columnName := range columns {
		columnIndex, _ := df.GetColumnIndex(columnName)
		columnIndexs = append(columnIndexs, columnIndex)
	}

	// Create the new column
	newValues := []interface{}{}
	for i := 0; i < df.Height(); i++ {

		// List of Values to be used
		valuemap := map[string]interface{}{}
		for _, columnIndex := range columnIndexs {
			valuemap[df.series[columnIndex].Name] = df.series[columnIndex].Values[i]
		}

		boolValue := f(valuemap)
		newValues = append(newValues, boolValue)
	}

	// Get the indexes of the rows that are false
	indexes := []int{}
	for i := 0; i < df.Height(); i++ {
		if !newValues[i].(bool) {
			indexes = append(indexes, i)
		}
	}

	// Remove the rows that are false
	df = df.DropRows(indexes...)

	return df
}

func (df *DataFrame) FilterSeries(f func(...[]interface{}) bool, cols ...interface{}) *DataFrame {

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
	for i := 0; i < df.Height(); i++ {

		// List of Values to be used
		values := [][]interface{}{}
		for _, columnIndex := range columnIndexs {
			values = append(values, df.series[columnIndex].Values)
		}

		boolValue := f(values...)
		newValues = append(newValues, boolValue)
	}

	// Get the indexes of the rows that are false
	indexes := []int{}
	for i := 0; i < df.Height(); i++ {
		if !newValues[i].(bool) {
			indexes = append(indexes, i)
		}
	}

	// Remove the rows that are false
	df = df.DropRows(indexes...)

	return df
}
