package dataframe

import (
	"encoding/csv"
	"errors"
	"os"
)

type WriterOptionsRaw struct {
	delimiter        string
	trimleadingspace bool
	header           bool
}

func (wo *WriterOptionsRaw) standardizeOptions() (*WriterOptionsStandard, error) {

	optionNew := &WriterOptionsStandard{
		delimiter:        rune(wo.delimiter[0]),
		trimleadingspace: wo.trimleadingspace,
		header:           wo.header,
	}

	// Report any errors to Prevent incompatible options
	if optionNew.trimleadingspace && (optionNew.delimiter == ' ' || optionNew.delimiter == '\t') {
		return nil, errors.New("error: trimleadingspace is true, but the delimiter is a space or tab. These are incompatible options")
	}

	return optionNew, nil
}

type WriterOptionsStandard struct {
	delimiter        rune
	trimleadingspace bool
	header           bool
}

type DataFrameWriter struct {
	df       *DataFrame
	fileType string
	filePath string
	options  *WriterOptionsRaw
}

func (df *DataFrame) Write() *DataFrameWriter {
	return &DataFrameWriter{
		df: df,
		options: &WriterOptionsRaw{
			delimiter:        ",",
			trimleadingspace: false,
			header:           false,
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
		if _, ok := value.(rune); ok {
			value = string(value.(rune))
		}
		dfw.options.delimiter = value.(string)
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
		err := WriteCSV2(dfw.df, dfw.filePath, optionsStandard)
		if err != nil {
			return err
		}
		return nil
	}

	return errors.New("FileType not supported")
}

func WriteCSV2(df *DataFrame, path string, options *WriterOptionsStandard) error {

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
