package dataframe

import "slices"

type Series struct {
	Name   string
	Values []interface{}
}

// NewSeries returns a new Series.
//
// This should be used to create a new Series over the Series struct.
func NewSeries(name string, values []interface{}) *Series {
	return &Series{name, values}
}

func (s *Series) Rename(newName string) *Series {
	s.Name = newName
	return s
}

// Copy returns a new Series with the same values as the original Series.
//
// If deep is set to true, the function will create a deep copy of the Series.
func (s *Series) Copy(deep bool) *Series {
	if deep {
		newValues := make([]interface{}, len(s.Values))
		copy(newValues, s.Values)
		return &Series{s.Name, newValues}
	}
	return &Series{s.Name, s.Values}
}

func (s *Series) Len() int {
	return len(s.Values)
}

func (s *Series) DropRow(index int) *Series {
	s.Values = slices.Replace(s.Values, index, index+1)
	return s
}
