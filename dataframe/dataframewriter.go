package dataframe

import (
	"encoding/csv"
	"errors"
	"os"
)

type DataFrameWriter struct {
	df       *DataFrame
	fileType string
	filePath string
	options  *Options
}

func (df *DataFrame) Write() *DataFrameWriter {
	return &DataFrameWriter{
		df: df,
		options: &Options{
			delimiter:        ',',
			trimleadingspace: false,
			header:           false,
			inferdatatypes:   false,
		},
	}
}

func (dfw *DataFrameWriter) FileType(fileType string) *DataFrameWriter {
	dfw.fileType = fileType
	return dfw
}

func (dfw *DataFrameWriter) FilePath(filePath string) *DataFrameWriter {
	dfw.filePath = filePath
	return dfw
}

func (dfw *DataFrameWriter) Option(key string, value any) *DataFrameWriter {
	switch key {
	case "delimiter":
		dfw.options.delimiter = value.(rune)
	case "trimleadingspace":
		dfw.options.trimleadingspace = value.(bool)
	case "header":
		dfw.options.header = value.(bool)
	}
	return dfw
}

func (dfw *DataFrameWriter) Save() error {
	optionsStandard, err := dfw.options.standardizeOptions()
	if err != nil {
		return err
	}

	switch dfw.fileType {
	case "csv":
		err := WriteCSV(dfw.df, dfw.filePath, optionsStandard)
		if err != nil {
			return err
		}
		return nil
	}

	return errors.New("FileType not supported")
}

func WriteCSV(df *DataFrame, path string, options *Options) error {

	header := []string{}
	if options.header {
		header = df.ColumnNames()
	}

	columns := [][]string{} // Todo: Change to [][]any
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

	// println("Columns:")
	// for _, column := range columns {
	// 	fmt.Println(column)
	// }

	// Create the file
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	csvWriter := csv.NewWriter(file)
	csvWriter.Comma = options.delimiter

	// Write to the csv file
	err1 := csvWriter.WriteAll(columns)
	if err1 != nil {
		return err1
	}

	return nil
}
