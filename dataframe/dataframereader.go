package dataframe

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"github.com/rotisserie/eris"
	// "github.com/pkg/errors"
)

type DataFrameReader struct {
	fileType string
	filePath string
	options  *Options
}

func Read() *DataFrameReader {
	return &DataFrameReader{
		options: &Options{
			delimiter:        ',',
			trimleadingspace: false,
			header:           false,
			inferdatatypes:   false,
		},
	}
}

func (dfr *DataFrameReader) FileType(fileType string) *DataFrameReader {
	dfr.fileType = fileType
	return dfr
}

func (dfr *DataFrameReader) FilePath(filePath string) *DataFrameReader {
	dfr.filePath = filePath
	return dfr
}

func (dfr *DataFrameReader) Option(key string, value any) *DataFrameReader {
	switch key {
	case "delimiter":
		dfr.options.delimiter = value.(rune)
	case "trimleadingspace":
		dfr.options.trimleadingspace = value.(bool)
	case "header":
		dfr.options.header = value.(bool)
	case "inferdatatypes":
		dfr.options.inferdatatypes = value.(bool)
	}
	return dfr
}

func (dfr *DataFrameReader) Load() (*DataFrame, error) {

	optionsStandard, err := dfr.options.standardizeOptions()
	if err != nil {
		return nil, eris.Wrap(err, "Error standardizing options")
	}

	switch dfr.fileType {
	case "csv":
		df, err := csvReader(dfr.filePath, optionsStandard)
		if err != nil {
			return &DataFrame{}, eris.Wrap(err, "Error reading CSV file")
		}
		return df, nil
	}

	return &DataFrame{}, eris.New("Unknown file type")
	// return &DataFrame{}, errors.New("Unknown file type")
	// return &DataFrame{}, eris.Wrap(errors.New("Unknown file type"), "Unknown file type2")

}

func csvReader(path string, options *Options) (*DataFrame, error) {
	// Read the file
	file, err := os.Open(path)
	if err != nil {
		return nil, eris.Wrap(err, "Error opening file")
	}

	// Create a CSV Reader
	buf := bufio.NewReader(file)
	csvReader := csv.NewReader(buf)

	csvReader.Comma = options.delimiter
	csvReader.TrimLeadingSpace = options.trimleadingspace

	columns := [][]any{}
	// Read the CSV
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}

		for index, item := range row {
			if len(columns) <= index {
				columns = append(columns, []any{})
			}
			columns[index] = append(columns[index], item)
		}

		if err != nil {
			return nil, eris.Wrap(err, "CSV read error")
		}
	}

	// Prefill header with default values
	header := []string{}
	for index := range len(columns) {
		header = append(header, fmt.Sprintf("Column %d", index))
	}

	// Check if header is present
	if options.header {
		for index, column := range columns {
			if len(column) > 0 {
				header[index] = column[0].(string)
				columns[index] = column[1:]
			}
		}
	}

	// Create the Series
	series := []*Series{}

	for index, column := range columns {
		newSeries := NewSeries(header[index], column)
		series = append(series, newSeries)
	}

	return &DataFrame{series}, nil
}
