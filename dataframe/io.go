package dataframe

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"teddy/dataframe/series"

	"github.com/rotisserie/eris"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

// ReadCSVtoRows reads a CSV file and returns a 2D array of strings (rows)
func ReadCSVtoRows(path string, options ...OptionsMap) ([][]string, error) {
	// Standardize the keys
	optionsClean := standardizeOptions(options...)
	delimiter := optionsClean.getOption("delimiter", ',').(rune)
	trimLeadingSpace := optionsClean.getOption("trimleadingspace", false).(bool)
	debug := optionsClean.getOption("debug", false).(bool)

	// Read the file
	file, err := os.Open(path)
	if err != nil {
		return nil, eris.Wrapf(err, "Error reading file: %s", path)
	}
	defer file.Close()

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
			return nil, eris.Wrapf(err, "Error parsing CSV file: %s", path)
		}
		return nil, eris.Wrapf(err, "Error reading CSV file: %s", path)
	}

	if rows == nil {
		return nil, errors.New("error: rows is nil")
	}

	return rows, nil
}

// ReadCSVtoColumns reads a CSV file and returns a 2D array of strings (columns)
func ReadCSVtoColumns(path string, options ...OptionsMap) ([][]string, error) {
	// First read as rows
	rows, err := ReadCSVtoRows(path, options...)
	if err != nil {
		return nil, err
	}

	// Then transpose to columns
	return TransposeRows(rows), nil
}

// ReadParquet reads a Parquet file and returns a DataFrame
func ReadParquet(filename string, options ...OptionsMap) (*DataFrame, error) {
	fr, err := local.NewLocalFileReader(filename)
	if err != nil {
		return nil, eris.Wrapf(err, "Error reading parquet file: %s", filename)
	}
	defer fr.Close()

	pr, err := reader.NewParquetColumnReader(fr, 4)
	if err != nil {
		return nil, eris.Wrap(err, "Error creating parquet column reader")
	}
	defer pr.ReadStop()

	rowCount := pr.GetNumRows()
	colCount := pr.SchemaHandler.GetColumnNum()

	df := NewDataFrame()

	for i := int64(0); i < colCount; i++ {
		colName := pr.SchemaHandler.GetExName(int(i) + 1)
		values, _, _, err := pr.ReadColumnByIndex(i, rowCount)
		if err != nil {
			return nil, eris.Wrapf(err, "Error reading column %d", i)
		}

		// Detect type and create appropriate typed series
		if len(values) > 0 {
			var seriess series.SeriesInterface

			switch values[0].(type) {
			case int32, int64:
				// Convert to []int
				intValues := make([]int, len(values))
				for j, v := range values {
					switch vt := v.(type) {
					case int32:
						intValues[j] = int(vt)
					case int64:
						intValues[j] = int(vt)
					default:
						// Fallback to generic if conversion fails
						seriess = series.NewGenericSeries(colName, values)
					}
				}
				if seriess == nil {
					seriess = series.NewIntSeries(colName, intValues)
				}

			case float32, float64:
				// Convert to []float64
				floatValues := make([]float64, len(values))

				for j, v := range values {
					switch vt := v.(type) {
					case float32:
						floatValues[j] = float64(vt)
					case float64:
						floatValues[j] = vt
					default:
						// Fallback to generic if conversion fails
						seriess = series.NewGenericSeries(colName, values)
					}
				}

				if seriess == nil {
					seriess = series.NewFloat64Series(colName, floatValues)
				}

			case string:
				// Convert to []string
				stringValues := make([]string, len(values))
				for j, v := range values {
					if str, ok := v.(string); ok {
						stringValues[j] = str
					} else {
						// Fallback to generic if conversion fails
						seriess = series.NewGenericSeries(colName, values)
						break
					}
				}
				if seriess == nil {
					seriess = series.NewStringSeries(colName, stringValues)
				}

			case bool:
				// Convert to []bool
				boolValues := make([]bool, len(values))
				for j, v := range values {
					if b, ok := v.(bool); ok {
						boolValues[j] = b
					} else {
						// Fallback to generic if conversion fails
						seriess = series.NewGenericSeries(colName, values)
						break
					}
				}
				if seriess == nil {
					seriess = series.NewBoolSeries(colName, boolValues)
				}

			default:
				// Use generic series for unsupported or mixed types
				seriess = series.NewGenericSeries(colName, values)
			}

			df.AddSeries(seriess)
		} else {
			// Empty column - create an empty generic series
			df.AddSeries(series.NewGenericSeries(colName, []any{}))
		}
	}

	return df, nil
}

// NewFromRows creates a DataFrame from a 2D array of strings (rows)
func NewFromRows(rows [][]string, options ...OptionsMap) *DataFrame {
	optionsClean := standardizeOptions(options...)
	headerOption := optionsClean.getOption("header", false).(bool)
	inferTypes := optionsClean.getOption("inferdatatypes", false).(bool)

	// Prefill header with default values
	header := make([]string, len(rows[0]))
	for i := range header {
		header[i] = fmt.Sprintf("Column %d", i)
	}

	// Check if header is present
	var dataRows [][]string
	if headerOption {
		header = rows[0]
		dataRows = rows[1:]
	} else {
		dataRows = rows
	}

	// Transpose the rows to get columns
	columns := TransposeRows(dataRows)

	// Create series with type inference if requested
	df := NewDataFrame()
	for i, column := range columns {
		if inferTypes {
			// Try to infer types based on the column data
			switch inferColumnType(column) {
			case "int":
				intValues, ok := series.StringSliceToIntSlice(column)
				if ok {
					df.AddSeries(series.NewIntSeries(header[i], intValues))
				} else {
					df.AddSeries(series.NewStringSeries(header[i], column))
				}
			case "float":
				floatValues, ok := series.StringSliceToFloat64Slice(column)
				if ok {
					df.AddSeries(series.NewFloat64Series(header[i], floatValues))
				} else {
					df.AddSeries(series.NewStringSeries(header[i], column))
				}
			case "bool":
				boolValues, ok := series.StringSliceToBoolSlice(column)
				if ok {
					df.AddSeries(series.NewBoolSeries(header[i], boolValues))
				} else {
					df.AddSeries(series.NewStringSeries(header[i], column))
				}
			default:
				df.AddSeries(series.NewStringSeries(header[i], column))
			}
		} else {
			// Default to string series
			df.AddSeries(series.NewStringSeries(header[i], column))
		}
	}

	return df
}

// NewFromColumns creates a DataFrame from a 2D array of strings (columns)
func NewFromColumns(columns [][]string, options ...OptionsMap) *DataFrame {
	optionsClean := standardizeOptions(options...)
	headerOption := optionsClean.getOption("header", false).(bool)
	inferTypes := optionsClean.getOption("inferdatatypes", false).(bool)

	// Prefill header with default values
	header := make([]string, len(columns))
	for i := range header {
		header[i] = fmt.Sprintf("Column %d", i)
	}

	// Check if header is present
	var dataColumns [][]string
	if headerOption {
		for i, column := range columns {
			if len(column) > 0 {
				header[i] = column[0]
			}
		}

		// Remove header row from each column
		dataColumns = make([][]string, len(columns))
		for i, column := range columns {
			if len(column) > 1 {
				dataColumns[i] = column[1:]
			} else {
				dataColumns[i] = []string{}
			}
		}
	} else {
		dataColumns = columns
	}

	// Create series with type inference if requested
	df := NewDataFrame()
	for i, column := range dataColumns {
		if inferTypes {
			// Try to infer types based on the column data
			switch inferColumnType(column) {
			case "int":
				intValues, ok := series.StringSliceToIntSlice(column)
				if ok {
					df.AddSeries(series.NewIntSeries(header[i], intValues))
				} else {
					df.AddSeries(series.NewStringSeries(header[i], column))
				}
			case "float":
				floatValues, ok := series.StringSliceToFloat64Slice(column)
				if ok {
					df.AddSeries(series.NewFloat64Series(header[i], floatValues))
				} else {
					df.AddSeries(series.NewStringSeries(header[i], column))
				}
			case "bool":
				boolValues, ok := series.StringSliceToBoolSlice(column)
				if ok {
					df.AddSeries(series.NewBoolSeries(header[i], boolValues))
				} else {
					df.AddSeries(series.NewStringSeries(header[i], column))
				}
			default:
				df.AddSeries(series.NewStringSeries(header[i], column))
			}
		} else {
			// Default to string series
			df.AddSeries(series.NewStringSeries(header[i], column))
		}
	}

	return df
}

// inferColumnType tries to determine the data type of a column
func inferColumnType(column []string) string {
	if len(column) == 0 {
		return "string"
	}

	// Check if all values are boolean
	allBool := true
	for _, val := range column {
		if val == "" {
			continue // Skip empty values
		}
		if _, err := strconv.ParseBool(val); err != nil {
			allBool = false
			break
		}
	}
	if allBool {
		return "bool"
	}

	// Check if all values are integers
	allInt := true
	for _, val := range column {
		if val == "" {
			continue // Skip empty values
		}
		if _, err := strconv.Atoi(val); err != nil {
			allInt = false
			break
		}
	}
	if allInt {
		return "int"
	}

	// Check if all values are floats
	allFloat := true
	for _, val := range column {
		if val == "" {
			continue // Skip empty values
		}
		if _, err := strconv.ParseFloat(val, 64); err != nil {
			allFloat = false
			break
		}
	}
	if allFloat {
		return "float"
	}

	// Default to string
	return "string"
}

// Helper function to parse int values from strings
func parseInt(value string) (int, error) {
	// Remove any commas or other formatting
	cleanValue := strings.ReplaceAll(value, ",", "")
	return strconv.Atoi(cleanValue)
}

// Helper function to parse float values from strings
func parseFloat(value string) (float64, error) {
	// Remove any commas or other formatting
	cleanValue := strings.ReplaceAll(value, ",", "")
	return strconv.ParseFloat(cleanValue, 64)
}

// Helper function to parse bool values from strings
func parseBool(value string) (bool, error) {
	// Handle standard bool values
	switch strings.ToLower(value) {
	case "true", "t", "yes", "y", "1":
		return true, nil
	case "false", "f", "no", "n", "0":
		return false, nil
	default:
		return false, errors.New("cannot parse as bool")
	}
}
