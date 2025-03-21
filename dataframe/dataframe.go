package dataframe

import (
	"errors"
	"fmt"
	"slices"
	"teddy/dataframe/series"
)

type DataFrame struct {
	series []series.SeriesInterface
}

func NewDataFrame(series ...series.SeriesInterface) *DataFrame {
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

// GetSeries returns a Series based on the column name.
//
// If the column doesn't exist, returns nil.
//
// Options:
//   - copy: bool (default: false) If true, the function will return a copy of the Series.
func (df *DataFrame) GetSeries(columnName string, options ...OptionsMap) series.SeriesInterface {
	optionsClean := standardizeOptions(options...)
	copy := optionsClean.getOption("copy", false).(bool)

	for _, s := range df.series {
		if s.Name() == columnName {
			if copy {
				return s.Copy(true)
			}
			return s
		}
	}

	return nil
}

func (df *DataFrame) Rename(oldColumnName, newColumnName string) *DataFrame {
	for i, series := range df.series {
		if series.Name() == oldColumnName {
			df.series[i] = series.Rename(newColumnName)
		}
	}
	return df
}

// ApplyIndex applies a function to each row of the specified columns.
//
// The function takes variable arguments of any type and returns a single value of any type.
func (df *DataFrame) ApplyIndex(newColumnName string, f func(...any) any, cols ...any) *DataFrame {
	// Get the column names
	columns, err := df.GetColumnNames(cols...)
	if err != nil {
		fmt.Println(err)
		return df
	}

	// Get the column indexes
	columnIndexs := []int{}
	for _, columnName := range columns {
		columnIndex, _ := df.GetColumnIndex(columnName)
		columnIndexs = append(columnIndexs, columnIndex)
	}

	// Create the new column
	newValues := make([]any, df.Height())
	for i := 0; i < df.Height(); i++ {
		// List of Values to be used
		values := make([]any, len(columnIndexs))
		for j, columnIndex := range columnIndexs {
			values[j] = df.series[columnIndex].Get(i)
		}

		newValues[i] = f(values...)
	}

	// If the column already exists, drop it
	if df.HasColumn(newColumnName) {
		df = df.DropColumn(newColumnName)
	}

	// Add the new column to the DataFrame
	df.series = append(df.series, series.NewSeries(newColumnName, newValues))

	return df
}

// ApplyMap applies a function to each row as a map of column name to value.
//
// The function takes a map of column names to values and returns a single value of any type.
func (df *DataFrame) ApplyMap(newColumnName string, f func(map[string]any) any) *DataFrame {
	columns := df.ColumnNames()

	// Create the new column
	newValues := make([]any, df.Height())
	for i := 0; i < df.Height(); i++ {
		// Create map of column name to value
		rowMap := make(map[string]any)
		for j, series := range df.series {
			rowMap[columns[j]] = series.Get(i)
		}

		newValues[i] = f(rowMap)
	}

	// If the column already exists, drop it
	if df.HasColumn(newColumnName) {
		df = df.DropColumn(newColumnName)
	}

	// Add the new column to the DataFrame
	df.series = append(df.series, series.NewSeries(newColumnName, newValues))

	return df
}

// ApplySeries applies a function to entire columns.
//
// The function takes variable arguments of slices of any and returns a slice of any.
func (df *DataFrame) ApplySeries(newColumnName string, f func(...[]any) []any, cols ...any) *DataFrame {
	// Get the column names
	columns, err := df.GetColumnNames(cols...)
	if err != nil {
		fmt.Println(err)
		return df
	}

	// Get the column values
	columnValues := make([][]any, len(columns))
	for i, columnName := range columns {
		series := df.GetSeries(columnName)
		columnValues[i] = series.Values()
	}

	// Apply the function to get new values
	newValues := f(columnValues...)

	// If the column already exists, drop it
	if df.HasColumn(newColumnName) {
		df = df.DropColumn(newColumnName)
	}

	// Add the new column to the DataFrame
	df.series = append(df.series, series.NewSeries(newColumnName, newValues))

	return df
}

// FilterIndex filters rows by applying a function to the values of specified columns.
//
// The function takes variable arguments of any type and returns a boolean.
func (df *DataFrame) FilterIndex(f func(...any) bool, cols ...any) *DataFrame {
	// Get the column names
	columns, err := df.GetColumnNames(cols...)
	if err != nil {
		fmt.Println(err)
		return df
	}

	// Get the column indexes
	columnIndexes := make([]int, len(columns))
	for i, columnName := range columns {
		index, _ := df.GetColumnIndex(columnName)
		columnIndexes[i] = index
	}

	// Apply the filter function to each row
	dropIndexes := make([]int, 0)
	for i := 0; i < df.Height(); i++ {
		// List of values to be used
		values := make([]any, len(columnIndexes))
		for j, columnIndex := range columnIndexes {
			values[j] = df.series[columnIndex].Get(i)
		}

		// If filter returns false, mark row for removal
		if !f(values...) {
			dropIndexes = append(dropIndexes, i)
		}
	}

	// Drop the rows that don't pass the filter
	df = df.DropRows(dropIndexes...)

	return df
}

// FilterMap filters rows by applying a function to a map of column name to value.
//
// The function takes a map of column names to values and returns a boolean.
func (df *DataFrame) FilterMap(f func(map[string]any) bool) *DataFrame {
	columns := df.ColumnNames()

	// Apply the filter function to each row
	dropIndexes := make([]int, 0)
	for i := 0; i < df.Height(); i++ {
		// Create map of column name to value
		rowMap := make(map[string]any)
		for j, series := range df.series {
			rowMap[columns[j]] = series.Get(i)
		}

		// If filter returns false, mark row for removal
		if !f(rowMap) {
			dropIndexes = append(dropIndexes, i)
		}
	}

	// Drop the rows that don't pass the filter
	df = df.DropRows(dropIndexes...)

	return df
}

// GroupByIndex groups rows by a column and applies an aggregation function to other columns.
//
// The function takes variable arguments of any type and returns a single value.
func (df *DataFrame) GroupByIndex(by string, f func(...any) any, cols ...any) *DataFrame {
	// Get the column names to aggregate
	columns, err := df.GetColumnNames(cols...)
	if err != nil {
		fmt.Println(err)
		return df
	}

	// Get the 'by' column
	byColumnIndex, exists := df.GetColumnIndex(by)
	if !exists {
		fmt.Println("Column does not exist:", by)
		return df
	}
	bySeries := df.series[byColumnIndex]

	// Create maps to collect values for each group
	groupedValues := make(map[any][][]any)

	// Group the values
	for i := 0; i < df.Height(); i++ {
		// Get the group key (value from 'by' column)
		key := bySeries.Get(i)

		// If this is the first value for this key, initialize the slice
		if _, ok := groupedValues[key]; !ok {
			groupedValues[key] = make([][]any, len(columns))
			for j := range columns {
				groupedValues[key][j] = make([]any, 0)
			}
		}

		// Add values from other columns to the group
		for j, columnName := range columns {
			series := df.GetSeries(columnName)
			groupedValues[key][j] = append(groupedValues[key][j], series.Get(i))
		}
	}

	// Create a new DataFrame with the results
	result := NewDataFrame()

	// Create deterministic list of keys to ensure consistent order
	keys := make([]any, 0, len(groupedValues))
	for key := range groupedValues {
		keys = append(keys, key)
	}

	// Add the 'by' column
	byValues := make([]any, len(keys))
	for i, key := range keys {
		byValues[i] = key
	}
	result.AddSeries(series.NewSeries(by, byValues))

	// Add aggregated columns
	for j, columnName := range columns {
		aggValues := make([]any, len(keys))
		for i, key := range keys {
			// Get all values for this group and column
			values := groupedValues[key][j]
			// Apply aggregation function to all values
			aggValues[i] = f(values...)
		}
		result.AddSeries(series.NewSeries(columnName, aggValues))
	}

	return result
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
	return df.series[0].Len()
}

func (df *DataFrame) HasColumn(columnName string) bool {
	for _, series := range df.series {
		if series.Name() == columnName {
			return true
		}
	}
	return false
}

func (df *DataFrame) ColumnNames() []string {
	columns := make([]string, len(df.series))
	for i, series := range df.series {
		columns[i] = series.Name()
	}
	return columns
}

func (df *DataFrame) GetColumnIndex(columnName string) (int, bool) {
	for index, series := range df.series {
		if series.Name() == columnName {
			return index, true
		}
	}
	return -1, false
}

// Shape returns the height and width of the DataFrame.
func (df *DataFrame) Shape() (int, int) {
	if len(df.series) == 0 {
		return 0, 0
	}
	return df.Height(), df.Width()
}

func (df *DataFrame) DropRow(index int) *DataFrame {
	for i, series := range df.series {
		df.series[i] = series.DropRow(index)
	}
	return df
}

func (df *DataFrame) DropRows(indexes ...int) *DataFrame {
	// Sort the indexes in reverse order
	slices.Sort(indexes)
	slices.Reverse(indexes)

	for i := range indexes {
		df.DropRow(indexes[i])
	}
	return df
}

func (df *DataFrame) DropRowsBySeries(series series.SeriesInterface) *DataFrame {
	// Convert the Series to a list of indexes
	indexes := []int{}
	for i := 0; i < series.Len(); i++ {
		if val, ok := series.Get(i).(int); ok {
			indexes = append(indexes, val)
		}
	}

	df = df.DropRows(indexes...)
	return df
}

func (df *DataFrame) DropColumn(selectedColumn ...any) *DataFrame {
	if len(df.series) == 0 {
		return &DataFrame{}
	}

	// Check if all values are of the same type
	columns, err := df.GetColumnNames(selectedColumn...)
	if err != nil {
		fmt.Println(err)
		return &DataFrame{}
	}

	for _, columnName := range columns {
		for index, series := range df.series {
			if series.Name() == columnName {
				df.series = slices.Delete(df.series, index, index+1)
				break
			}
		}
	}

	return df
}

func (df *DataFrame) AsType(columnName string, newType string) *DataFrame {
	for i, series := range df.series {
		if series.Name() == columnName {
			df.series[i] = series.AsType(newType)
		}
	}
	return df
}

func (df *DataFrame) AddSeries(series series.SeriesInterface) *DataFrame {
	// If the DataFrame is empty, add the Series
	if df.Width() == 0 {
		df.series = append(df.series, series)
		return df
	}

	// Check if the Series is the same length as the DataFrame
	if series.Len() != df.Height() {
		fmt.Println("Series must be the same length as the DataFrame")
		return df
	}

	df.series = append(df.series, series)
	return df
}

func (df *DataFrame) AddRow(row []any) *DataFrame {
	if df.Width() == 0 {
		return df
	}

	if len(row) != df.Width() {
		fmt.Println("Row must be the same length as the DataFrame")
		return df
	}

	// Create new rows for each series
	for i, value := range row {
		seriess := df.series[i]

		// For typed series, we need to handle type conversion
		switch s := seriess.(type) {
		case *series.IntSeries:
			if intVal, ok := value.(int); ok {
				newValues := append(s.Values(), intVal)
				intValues, _ := series.ToIntSlice(newValues)
				df.series[i] = series.NewIntSeries(s.Name(), intValues)
			} else {
				// Convert to int or fall back to generic
				genSeries := s.ToGenericSeries()
				genValues := append(genSeries.Values(), value)
				df.series[i] = series.NewGenericSeries(genSeries.Name(), genValues)
			}
		case *series.Float64Series:
			if floatVal, ok := value.(float64); ok {
				newValues := append(s.Values(), floatVal)
				floatValues, _ := series.ToFloat64Slice(newValues)
				df.series[i] = series.NewFloat64Series(s.Name(), floatValues)
			} else {
				// Convert to float64 or fall back to generic
				genSeries := s.ToGenericSeries()
				genValues := append(genSeries.Values(), value)
				df.series[i] = series.NewGenericSeries(genSeries.Name(), genValues)
			}
		case *series.StringSeries:
			if strVal, ok := value.(string); ok {
				newValues := append(s.Values(), strVal)
				stringValue := series.ToStringSlice(newValues)
				df.series[i] = series.NewStringSeries(s.Name(), stringValue)
			} else {
				// Convert to string or fall back to generic
				genSeries := s.ToGenericSeries()
				genValues := append(genSeries.Values(), value)
				df.series[i] = series.NewGenericSeries(genSeries.Name(), genValues)
			}
		case *series.BoolSeries:
			if boolVal, ok := value.(bool); ok {
				newValues := append(s.Values(), boolVal)
				boolValues, _ := series.ToBoolSlice(newValues)
				df.series[i] = series.NewBoolSeries(s.Name(), boolValues)
			} else {
				// Convert to bool or fall back to generic
				genSeries := s.ToGenericSeries()
				genValues := append(genSeries.Values(), value)
				df.series[i] = series.NewGenericSeries(genSeries.Name(), genValues)
			}
		case *series.GenericSeries:
			newValues := append(s.Values(), value)
			df.series[i] = series.NewGenericSeries(s.Name(), newValues)
		}
	}

	return df
}

// Select returns a new DataFrame with the selected columns.
//
// Select does not create a copy of the data, it only creates a new DataFrame
// with references to the original data.
// The columnNames can be a string, slice of strings, int, or slice of ints.
func (df *DataFrame) Select(selectedColumn ...any) *DataFrame {
	if len(df.series) == 0 {
		return &DataFrame{}
	}

	// Check if all values are of the same type
	if !allSameType(selectedColumn) {
		fmt.Println("All values must be of the same type")
		return &DataFrame{}
	}

	columnNames, err := df.GetColumnNames(selectedColumn...)
	if err != nil {
		fmt.Println(err)
		return &DataFrame{}
	}

	newSeries := []series.SeriesInterface{}
	for _, columnName := range columnNames {
		for _, series := range df.series {
			if series.Name() == columnName {
				newSeries = append(newSeries, series)
			}
		}
	}
	return &DataFrame{newSeries}
}

// GetColumnNames returns the column names based on the selected columns.
//
// The selectedColumns can be a string, slice of strings, int, or slice of ints.
//
// Returns a slice of strings with the column names.
// Error is returned if one of the columns do not exist.
func (df *DataFrame) GetColumnNames(selectedColumns ...any) ([]string, error) {
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
			columnNames = append(columnNames, df.series[index].Name())
		}
		return columnNames, nil
	}

	return []string{}, nil
}
