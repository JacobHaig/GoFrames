package aggregate

import (
	"fmt"
	"sort"
	"teddy/dataframe"
	"teddy/dataframe/series"
)

// GroupBy groups data by one or more columns and applies aggregation functions to other columns
// Returns a new DataFrame with results
func GroupBy(df *dataframe.DataFrame, by []string, aggregations map[string]Aggregator) *dataframe.DataFrame {
	// Check if all groupby columns exist
	for _, col := range by {
		if !df.HasColumn(col) {
			return dataframe.NewDataFrame()
		}
	}

	// Get the values for groupby columns
	groupKeys := make([][]any, len(by))
	for i, col := range by {
		s := df.GetSeries(col)
		groupKeys[i] = s.Values()
	}

	// Group data by constructing composite keys
	groupData := make(map[string][]int)
	for i := 0; i < df.Height(); i++ {
		key := ""
		for j := range by {
			key += fmt.Sprintf("%v|", groupKeys[j][i])
		}

		groupData[key] = append(groupData[key], i)
	}

	// Create the result DataFrame with group columns
	result := dataframe.NewDataFrame()

	// Create series for the group columns
	groupValues := make(map[string][]any)
	for _, col := range by {
		groupValues[col] = make([]any, 0, len(groupData))
	}

	// Sort keys for consistent order
	keys := make([]string, 0, len(groupData))
	for key := range groupData {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Extract group values from the first row of each group
	for _, key := range keys {
		rows := groupData[key]
		if len(rows) == 0 {
			continue
		}

		firstRow := rows[0]
		for _, col := range by {
			s := df.GetSeries(col)
			groupValues[col] = append(groupValues[col], s.Get(firstRow))
		}
	}

	// Add group columns to result
	for _, col := range by {
		result.AddSeries(series.NewSeries(col, groupValues[col]))
	}

	// Process all aggregations first
	processedAggs := make(map[string][]any)

	for aggColName, agg := range aggregations {
		// Special case for "count" - don't try to look for a column
		if aggColName == "count" {
			countValues := make([]any, len(keys))
			for i, key := range keys {
				countValues[i] = len(groupData[key])
			}
			processedAggs["count"] = countValues
			continue
		}

		// Skip columns that don't exist
		if !df.HasColumn(aggColName) {
			continue
		}

		// Skip columns that are used for grouping
		isGroupCol := false
		for _, groupCol := range by {
			if aggColName == groupCol {
				isGroupCol = true
				break
			}
		}
		if isGroupCol {
			continue
		}

		s := df.GetSeries(aggColName)
		aggValues := make([]any, 0, len(keys))

		for _, key := range keys {
			rows := groupData[key]
			// Extract values for this group
			values := make([]any, 0, len(rows))
			for _, row := range rows {
				values = append(values, s.Get(row))
			}

			// Apply the aggregation function
			aggValues = append(aggValues, agg(values...))
		}

		processedAggs[aggColName] = aggValues
	}

	// Now add all processed aggregations to the result
	for colName, values := range processedAggs {
		result.AddSeries(series.NewSeries(colName, values))
	}

	return result
}
