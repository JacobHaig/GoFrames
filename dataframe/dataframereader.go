package dataframe

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
)

type OptionsRaw struct {
	delimiter        string
	trimleadingspace bool
	header           bool
}

type OptionsStandard struct {
	delimiter        rune
	trimleadingspace bool
	header           bool
}

func (options OptionsRaw) standardizeOptions() (*OptionsStandard, error) {
	if len(options.delimiter) > 1 {
		return nil, errors.New("error: delimiter must be a single character")
	}

	optionNew := &OptionsStandard{
		delimiter:        rune(options.delimiter[0]),
		trimleadingspace: options.trimleadingspace,
		header:           options.header,
	}

	// Report any errors to Prevent incompatible options
	if optionNew.trimleadingspace && (optionNew.delimiter == ' ' || optionNew.delimiter == '\t') {
		return nil, errors.New("error: trimleadingspace is true, but the delimiter is a space or tab. these are incompatible options")
	}

	return optionNew, nil
}

type DataFrameReader struct {
	fileType string
	filePath string
	options  *OptionsRaw
}

func Read() *DataFrameReader {
	return &DataFrameReader{
		options: &OptionsRaw{
			delimiter:        ",",
			trimleadingspace: false,
			header:           false,
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
		// If its a rune, convert it to a string
		if _, ok := value.(rune); ok {
			value = string(value.(rune))
		}
		dfr.options.delimiter = value.(string)
	case "trimleadingspace":
		dfr.options.trimleadingspace = value.(bool)
	case "header":
		dfr.options.header = value.(bool)
	}
	return dfr
}

func (dfr *DataFrameReader) Load() (*DataFrame, error) {

	optionsStandard, err := dfr.options.standardizeOptions()
	if err != nil {
		return nil, err
	}

	switch dfr.fileType {
	case "csv":
		df, err := ReadCSV2(dfr.filePath, optionsStandard)
		if err != nil {
			return &DataFrame{}, err
		}
		return df, nil
	}

	return &DataFrame{}, errors.New("FileType not supported")
}

func ReadCSV2(path string, options *OptionsStandard) (*DataFrame, error) {

	// Read the file
	file, err := os.Open(path)
	if err != nil {
		return nil, err
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
			fmt.Println(err)
			return nil, err
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
