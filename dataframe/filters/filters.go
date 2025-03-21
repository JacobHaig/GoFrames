package filters

import (
	"teddy/dataframe"
	"teddy/dataframe/series"
)

// Filter represents a predicate that can be applied to a value
type Filter func(value any) bool

// Apply applies a filter to a series and returns the indices of matching elements
func Apply(s series.SeriesInterface, filter Filter) []int {
	indices := []int{}
	for i := 0; i < s.Len(); i++ {
		if filter(s.Get(i)) {
			indices = append(indices, i)
		}
	}
	return indices
}

// ApplyToDF applies a filter to a DataFrame based on a column and returns a new filtered DataFrame
func ApplyToDF(df *dataframe.DataFrame, colName string, filter Filter) *dataframe.DataFrame {
	// Find rows that match the filter
	s := df.GetSeries(colName)
	if s == nil {
		return df
	}

	// Find matching rows
	matchedRows := make([]int, 0)
	for i := 0; i < s.Len(); i++ {
		if filter(s.Get(i)) {
			matchedRows = append(matchedRows, i)
		}
	}

	// If no rows match, return empty DataFrame
	if len(matchedRows) == 0 {
		return dataframe.NewDataFrame()
	}

	// Create a new DataFrame with only the matching rows
	result := dataframe.NewDataFrame()

	// Add filtered series for each column
	for _, col := range df.ColumnNames() {
		s := df.GetSeries(col)

		filteredValues := make([]any, len(matchedRows))
		for i, row := range matchedRows {
			filteredValues[i] = s.Get(row)
		}

		result.AddSeries(series.NewSeries(col, filteredValues))
	}

	return result
}

// And returns a new filter that is the logical AND of the provided filters
func And(filters ...Filter) Filter {
	return func(value any) bool {
		for _, filter := range filters {
			if !filter(value) {
				return false
			}
		}
		return true
	}
}

// Or returns a new filter that is the logical OR of the provided filters
func Or(filters ...Filter) Filter {
	return func(value any) bool {
		for _, filter := range filters {
			if filter(value) {
				return true
			}
		}
		return false
	}
}

// Not returns a new filter that is the logical NOT of the provided filter
func Not(filter Filter) Filter {
	return func(value any) bool {
		return !filter(value)
	}
}
