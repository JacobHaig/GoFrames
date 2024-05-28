package dataframe

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"os"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

func ReadCSVtoRows(path string, options ...Options) ([][]string, error) {
	// Standardize the keys
	optionsClean := standardizeOptions(options...)
	delimiter := optionsClean.getOption("delimiter", ',').(rune)
	trimLeadingSpace := optionsClean.getOption("trimleadingspace", false).(bool)
	debug := optionsClean.getOption("debug", false).(bool)

	// Read the file
	file, err := os.Open(path)
	if err != nil {
		fmt.Println("Error reading file:", path)
		fmt.Println(err)
		return nil, err
	}

	// Create a CSV Reader
	buf := bufio.NewReader(file)
	csvReader := csv.NewReader(buf)

	csvReader.Comma = delimiter
	csvReader.TrimLeadingSpace = trimLeadingSpace

	// Prevent incompatible options
	if csvReader.TrimLeadingSpace && (csvReader.Comma == ' ' || csvReader.Comma == '\t') {
		return nil, errors.New("error: trimleadingspace is true, but the delimiter is a space or tab. these are incompatible options")
	}

	if debug {
		fmt.Println("Delimiter:", "("+string(csvReader.Comma)+")")
		fmt.Println("TrimLeadingSpace:", csvReader.TrimLeadingSpace)
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
		return nil, err
	}

	if rows == nil {
		return nil, errors.New("error: rows is nil")
	}

	return rows, nil
}

func ReadCSV(path string, options ...Options) (*DataFrame, error) {
	optionsClean := standardizeOptions(options...)

	// Read the file
	rows, err := ReadCSVtoRows(path, optionsClean)
	if err != nil {
		return nil, err
	}

	// Create the DataFrame
	df := NewFromRows(rows, optionsClean)

	return df, nil
}

func ReadParquet(filename string, options ...Options) (*DataFrame, error) {
	// optionsClean := standardizeOptions(options...)

	fr, err := local.NewLocalFileReader(filename)
	if err != nil {
		return nil, err
	}

	pr, err := reader.NewParquetColumnReader(fr, 4)
	if err != nil {
		return nil, err
	}
	// println(rowCounts)

	rowCount := pr.GetNumRows()
	colCount := pr.SchemaHandler.GetColumnNum()

	df := NewDataFrame()

	for i := range colCount {
		values, _, _, err := pr.ReadColumnByIndex(int64(i), rowCount)
		if err != nil {
			return nil, err
		}

		// fmt.Println(values)
		// fmt.Println(rls)
		// fmt.Println(dls)

		series := NewSeries(pr.SchemaHandler.GetExName(int(i)+1), values)
		df = df.AddSeries(series)
	}

	// fmt.Println(pr.SchemaHandler.GetColumnNum())
	// fmt.Println(pr.SchemaHandler.GetInName(1))
	// fmt.Println(pr.SchemaHandler.GetExName(1))

	// fmt.Println(pr.SchemaHandler.ValueColumns)
	// fmt.Println(pr.SchemaHandler.GetTypes())

	return df, nil
}

func NewFromRows(rows [][]string, options ...Options) *DataFrame {
	optionsClean := standardizeOptions(options...)
	headerOption := optionsClean.getOption("header", false).(bool)

	// Prefill header with default values
	header := []string{}
	for index := range len(rows[0]) {
		header = append(header, fmt.Sprintf("Column %d", index))
	}

	// Check if header is present
	if headerOption {
		header = rows[0]
		rows = rows[1:]
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
		series = append(series, NewSeries(header[index], newRow))
	}

	return &DataFrame{series}
}

// WriteCSV writes the DataFrame to a CSV file.
//
// The options can be used to control the output.
// The options are:
//   - header: bool (default: false) Whether to include the header in the output.
func (df *DataFrame) WriteCSV(path string, options ...Options) error {
	optionsClean := standardizeOptions(options...)
	headerOption := optionsClean.getOption("header", false).(bool)

	header := []string{}
	if headerOption {
		header = df.ColumnNames()
	}

	columns := [][]string{} // Todo: Change to [][]interface{}
	for _, series := range df.series {
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

func (df *DataFrame) PrintTable(options ...Options) {
	optionsClean := standardizeOptions(options...)
	displayRows := optionsClean.getOption("display_rows", 10).(int)

	if df.Width() == 0 {
		fmt.Println("Empty DataFrame")
		return
	}

	// Calculate the max width of each column
	widths := make([]int, df.Width())
	printTypes := false // If there is atleast one type, print the types in the header

	// max header
	for seriesIndex, series := range df.series {
		widths[seriesIndex] = max(widths[seriesIndex], len(series.Name))

		if series.Type != nil {
			widths[seriesIndex] = max(widths[seriesIndex], len(series.Type.Name()))
			printTypes = true
		}

		for rowIndex := 0; rowIndex < df.Height(); rowIndex++ {
			widths[seriesIndex] = max(widths[seriesIndex], len(fmt.Sprint(series.Values[rowIndex])))
		}
	}

	// Print the header separator
	fmt.Print("+-")
	for index := range df.series {
		fmt.Print(PadRight("", "-", widths[index]))
		if index < df.Width()-1 {
			fmt.Print("-+-")
		}
	}
	fmt.Println("-+ ")

	// Print the header
	fmt.Print("| ")
	for index, series := range df.series {
		fmt.Print(PadRight(series.Name, " ", widths[index]))
		if index < df.Width()-1 {
			fmt.Print(" | ")
		}
	}
	fmt.Println(" |")

	if printTypes {
		// Print the type
		fmt.Print("| ")
		for index, series := range df.series {
			if series.Type != nil {
				fmt.Print(PadRight(series.Type.Name(), " ", widths[index]))
			} else {
				fmt.Print(PadRight("", " ", widths[index]))
			}
			if index < df.Width()-1 {
				fmt.Print(" | ")
			}
		}
		fmt.Println(" |")
	}

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
out:
	for rowIndex := 0; rowIndex < df.Height(); rowIndex++ {
		fmt.Print("| ")
		for colIndex, series := range df.series {

			// This is the limit of rows to print. Use the "display_rows" option to change this.
			if rowIndex >= displayRows {
				fmt.Print(PadRight("...", " ", widths[colIndex]))
				if colIndex < df.Width()-1 {
					fmt.Print(" | ")
				}
				// After we fill the columns with ... , we break out of the loop
				if colIndex == df.Width()-1 {
					fmt.Println(" |")
					break out
				}
				continue
			}

			fmt.Print(PadRight(fmt.Sprint(series.Values[rowIndex]), " ", widths[colIndex]))
			if colIndex < df.Width()-1 {
				fmt.Print(" | ")
			}
		}
		fmt.Println(" |")
	}

	// Print the footer separator
	fmt.Print("+-")
	for index := range df.series {
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
	for index, series := range df.series {
		fmt.Print(series.Name)
		if index < df.Width()-1 {
			fmt.Print(", ")
		}
	}
	fmt.Println("")

	// Print the DataFrame
	for rowIndex := 0; rowIndex < df.Height(); rowIndex++ {
		for colIndex, series := range df.series {
			fmt.Print(series.Values[rowIndex])
			if colIndex < df.Width()-1 {
				fmt.Print(", ")
			}
		}
		fmt.Println("")
	}
}
