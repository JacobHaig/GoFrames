package dataframe

import (
	"errors"
	"fmt"
)

type DataFrame struct {
	Series []Series
}

type Series struct {
	Values []interface{}
	Name   string
}

// type Any interface{}

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

func (df *DataFrame) Rename(oldColumnName, newColumnName string) *DataFrame {
	for index, series := range df.Series {
		if series.Name == oldColumnName {
			df.Series[index].Name = newColumnName
		}
	}
	return df
}

func (df *DataFrame) Apply(newColumnName string, f func(...interface{}) interface{}, cols ...interface{}) *DataFrame {

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

func (df *DataFrame) ApplyMap(newColumnName string, f func(map[string]interface{}) interface{}) *DataFrame {

	columns := df.GetColumnNames()

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
		valuemap := map[string]interface{}{}
		for _, columnIndex := range columnIndexs {
			valuemap[df.Series[columnIndex].Name] = df.Series[columnIndex].Values[i]
		}

		newValue := f(valuemap)
		newValues = append(newValues, newValue)
	}

	// Add the new column to the DataFrame
	df.Series = append(df.Series, Series{newValues, newColumnName})

	return df
}
