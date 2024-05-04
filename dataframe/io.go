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
	series := []*Series{}

	for index, row := range rows {
		newRow := []interface{}{}
		for _, cell := range row {
			newRow = append(newRow, cell)
		}
		series = append(series, &Series{header[index], newRow})
	}

	return &DataFrame{series}
}

// WriteCSV writes the DataFrame to a CSV file.
//
// The options can be used to control the output.
// The options are:
//   - header: bool (default: false)
//     Whether to include the header in the output.
func (df *DataFrame) WriteCSV(path string, options ...Options) error {

	// Standardize the keys
	optionsClean := standardizeMapKeys(options...)

	header := []string{}
	if val, ok := optionsClean["header"]; ok {
		if val.(bool) {
			header = df.GetColumnNames()
		}
	}

	columns := [][]string{} // Todo: Change to [][]interface{}
	for _, series := range df.Series {
		seriesValues := InterfaceToTypeSlice[string](series.Values)
		columns = append(columns, seriesValues)
	}

	// Transpose the columns
	columns = TransposeRows(columns)

	// Add the header
	if len(header) > 0 {
		columns = append([][]string{header}, columns...)
	}

	println("Columns:")
	for _, column := range columns {
		fmt.Println(column)
	}

	// Write the file
	file, err := os.Create(path)
	if err != nil {
		fmt.Println("Error creating file:", path)
		fmt.Println(err)
		os.Exit(1)
		return err
	}
	defer file.Close()

	csvWriter := csv.NewWriter(file)
	csvWriter.Comma = ','

	err1 := csvWriter.WriteAll(columns)
	if err1 != nil {
		fmt.Println("Error writing to file:", path)
		fmt.Println(err1)
		os.Exit(1)
	}

	return nil
}

func (df *DataFrame) PrintTable() {
	if df.Width() == 0 {
		fmt.Println("Empty DataFrame")
		return
	}

	// Calculate the max width of each column
	widths := make([]int, df.Width())

	// max header
	for seriesIndex, series := range df.Series {
		widths[seriesIndex] = max(widths[seriesIndex], len(series.Name))

		for rowIndex := 0; rowIndex < df.Height(); rowIndex++ {
			widths[seriesIndex] = max(widths[seriesIndex], len(fmt.Sprint(series.Values[rowIndex])))
		}
	}

	// Print the header separator
	fmt.Print("+-")
	for index := range df.Series {
		fmt.Print(PadRight("", "-", widths[index]))
		if index < df.Width()-1 {
			fmt.Print("-+-")
		}
	}
	fmt.Println("-+ ")

	// Print the header
	fmt.Print("| ")
	for index, series := range df.Series {
		fmt.Print(PadRight(series.Name, " ", widths[index]))
		if index < df.Width()-1 {
			fmt.Print(" | ")
		}
	}
	fmt.Println(" |")

	// Print the body separator
	fmt.Print("+-")
	for index, width := range widths {
		fmt.Print(PadRight("", "-", width))
		if index < df.Width()-1 {
			fmt.Print("-+-")
		}
	}
	fmt.Println("-+")

	// Print the DataFrame
	for rowIndex := 0; rowIndex < df.Height(); rowIndex++ {
		fmt.Print("| ")
		for colIndex, series := range df.Series {
			fmt.Print(PadRight(fmt.Sprint(series.Values[rowIndex]), " ", widths[colIndex]))
			if colIndex < df.Width()-1 {
				fmt.Print(" | ")
			}
		}
		fmt.Println(" |")
	}

	// Print the footer separator
	fmt.Print("+-")
	for index := range df.Series {
		fmt.Print(PadRight("", "-", widths[index]))
		if index < df.Width()-1 {
			fmt.Print("-+-")
		}
	}
	fmt.Println("-+")
}

func (df *DataFrame) Print() {
	if df.Width() == 0 {
		fmt.Println("Empty DataFrame")
		return
	}

	// Print the header
	for index, series := range df.Series {
		fmt.Print(series.Name)
		if index < df.Width()-1 {
			fmt.Print(", ")
		}
	}
	fmt.Println("")

	// Print the DataFrame
	for rowIndex := 0; rowIndex < df.Height(); rowIndex++ {
		for colIndex, series := range df.Series {
			fmt.Print(series.Values[rowIndex])
			if colIndex < df.Width()-1 {
				fmt.Print(", ")
			}
		}
		fmt.Println("")
	}
}
