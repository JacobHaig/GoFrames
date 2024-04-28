package dataframe

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"strings"
)

func ReadCSVtoRows(path string, options ...Options) ([][]string, error) {
	// Standardize the keys
	optionsClean := standardizeMapKeys(options...)

	// Read the file
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Println("Error reading file:", path)
		fmt.Println(err)
		os.Exit(1)
		return [][]string{}, err
	}

	// Create a CSV Reader
	stringReader := strings.NewReader(string(data))
	csvReader := csv.NewReader(stringReader)

	// Get Delimiter
	if val, ok := optionsClean["delimiter"]; ok {
		csvReader.Comma = val.(rune)
	} else {
		csvReader.Comma = ','
	}

	if val, ok := optionsClean["trimleadingspace"]; ok {
		csvReader.TrimLeadingSpace = val.(bool) // Can cause issues if the delimiter is a space or tab
	}

	// Prevent incompatible options
	if csvReader.TrimLeadingSpace && (csvReader.Comma == ' ' || csvReader.Comma == '\t') {
		return nil, errors.New("error: trimleadingspace is true, but the delimiter is a space or tab. these are incompatible options")
	}

	if val, ok := optionsClean["debug"]; ok {
		if val.(bool) {
			fmt.Println("Delimiter:", "("+string(csvReader.Comma)+")")
			fmt.Println("TrimLeadingSpace:", csvReader.TrimLeadingSpace)
		}
	}

	// Read the CSV
	rows, err := csvReader.ReadAll()
	if err != nil {
		// ParseError
		if _, ok := err.(*csv.ParseError); ok {
			fmt.Println("Error: CSV file has parse error")
			fmt.Println("This occurred while parsing the following file:", path)
		}
		fmt.Println(err)
		os.Exit(1)
	}

	return rows, nil
}

func ReadCSV(path string, options ...Options) (*DataFrame, error) {
	// Standardize the keys
	optionsClean := standardizeMapKeys(options...)

	// Read the file
	rows, err := ReadCSVtoRows(path, optionsClean)
	if err != nil {
		return &DataFrame{}, err
	}

	// Create the DataFrame
	df := NewFromRows(rows, optionsClean)

	return df, nil
}

func NewFromRows(rows [][]string, options ...Options) *DataFrame {
	// Standardize the keys
	optionsClean := standardizeMapKeys(options...)

	// Prefill header with default values
	header := []string{}
	for index := range len(rows[0]) {
		header = append(header, fmt.Sprintf("Column %d", index))
	}

	// Check if header is present
	if val, ok := optionsClean["header"]; ok {
		if val.(bool) {
			header = rows[0]
			rows = rows[1:]
		}
	}

	// Transpose the rows
	rows = TransposeRows(rows)

	// Create the Series
	series := []Series{}

	for index, row := range rows {
		newRow := []interface{}{}
		for _, cell := range row {
			newRow = append(newRow, cell)
		}
		series = append(series, Series{newRow, header[index]})
	}

	return &DataFrame{series}
}
