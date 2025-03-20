package dataframe

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"

	convert "teddy/dataframe/convert"
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
			header:           true,
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
		return fmt.Errorf("error standardizing options: %w", err)
	}

	switch dfw.fileType {
	case "csv":
		err := WriteCSV(dfw.df, dfw.filePath, optionsStandard)
		if err != nil {
			return fmt.Errorf("error writing csv: %w", err)
		}
		return nil
	}

	return errors.New("unknown file type")
}

func WriteCSV(df *DataFrame, path string, options *Options) error {
	if df.Width() == 0 {
		return errors.New("cannot write empty DataFrame to CSV")
	}

	// Create the file
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("error creating file: %w", err)
	}
	defer file.Close()

	// Create the CSV writer
	csvWriter := csv.NewWriter(file)
	csvWriter.Comma = options.delimiter

	// Write header if requested
	if options.header {
		header := df.ColumnNames()
		err := csvWriter.Write(header)
		if err != nil {
			return fmt.Errorf("error writing header to CSV: %w", err)
		}
	}

	// Write data rows
	height := df.Height()
	width := df.Width()

	for i := 0; i < height; i++ {
		row := make([]string, width)
		for j, series := range df.series {
			// Convert any value to string
			row[j] = convert.ConvertToString(series.Get(i))
		}

		err := csvWriter.Write(row)
		if err != nil {
			return fmt.Errorf("error writing row to CSV: %w", err)
		}
	}

	// Flush the CSV writer
	csvWriter.Flush()

	if err := csvWriter.Error(); err != nil {
		return fmt.Errorf("error flushing CSV writer: %w", err)
	}

	return nil
}
