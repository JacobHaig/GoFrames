package aggregate

import (
	"teddy/dataframe"
	"teddy/dataframe/series"
)

// Aggregator represents a function that can aggregate multiple values into a single value
type Aggregator func(values ...any) any

// Apply applies an aggregator to a series and returns the result
func Apply(s series.SeriesInterface, aggregator Aggregator) any {
	if s.Len() == 0 {
		return nil
	}
	return aggregator(s.Values()...)
}

// ApplyToDF applies an aggregator to a DataFrame column and returns the result
func ApplyToDF(df *dataframe.DataFrame, colName string, aggregator Aggregator) any {
	s := df.GetSeries(colName)
	if s == nil {
		return nil
	}
	return Apply(s, aggregator)
}

// Combine creates a new aggregator that applies each aggregator in sequence
// and returns a slice of results
func Combine(aggregators ...Aggregator) Aggregator {
	return func(values ...any) any {
		results := make([]any, len(aggregators))
		for i, agg := range aggregators {
			results[i] = agg(values...)
		}
		return results
	}
}
