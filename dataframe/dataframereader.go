package dataframe

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"teddy/dataframe/series"
)

// DataFrameReader provides a fluent API for reading data into a DataFrame
type DataFrameReader struct {
	fileType    string
	filePath    string
	stringValue string
	options     *Options
}

// Read returns a new DataFrameReader instance
func Read() *DataFrameReader {
	return &DataFrameReader{
		options: NewOptions(),
	}
}

// FileType sets the file type for the reader (e.g., "csv", "parquet")
func (dfr *DataFrameReader) FileType(fileType string) *DataFrameReader {
	dfr.fileType = strings.ToLower(fileType)
	return dfr
}

// FilePath sets the file path to read from
func (dfr *DataFrameReader) FilePath(filePath string) *DataFrameReader {
	dfr.filePath = filePath
	return dfr
}

// FromString sets a string value to read from instead of a file
func (dfr *DataFrameReader) FromString(value string) *DataFrameReader {
	dfr.stringValue = value
	return dfr
}

// Option sets a reader option
func (dfr *DataFrameReader) Option(key string, value any) *DataFrameReader {
	key = strings.ToLower(key)
	switch key {
	case "delimiter":
		dfr.options.SetDelimiter(value.(rune))
	case "trimleadingspace":
		dfr.options.SetTrimLeadingSpace(value.(bool))
	case "header":
		dfr.options.SetHeader(value.(bool))
	case "inferdatatypes":
		dfr.options.SetInferDataTypes(value.(bool))
	}
	return dfr
}

// Load reads the data source and returns a DataFrame
func (dfr *DataFrameReader) Load() (*DataFrame, error) {
	// Validate and standardize options
	optionsStandard, err := dfr.options.standardizeOptions()
	if err != nil {
		return nil, fmt.Errorf("Error standardizing options: %w", err)
	}

	// If reading from a string
	if dfr.stringValue != "" {
		// Auto-detect file type if not specified
		if dfr.fileType == "" {
			dfr.fileType = "csv" // Default to CSV for string input
		}

		switch dfr.fileType {
		case "csv":
			df, err := readCSVFromString(dfr.stringValue, optionsStandard)
			if err != nil {
				return nil, fmt.Errorf("Error reading CSV from string: %w", err)
			}
			return df, nil
		default:
			return nil, fmt.Errorf("Unsupported file type for string input: %s", dfr.fileType)
		}
	}

	// If reading from a file
	if dfr.filePath == "" && dfr.stringValue == "" {
		return nil, errors.New("no file path or string value provided")
	}

	// Auto-detect file type if not specified
	if dfr.fileType == "" {
		dfr.fileType = detectFileType(dfr.filePath)
	}

	// Read based on file type
	switch dfr.fileType {
	case "csv":
		df, err := readCSVFromFile(dfr.filePath, optionsStandard)
		if err != nil {
			return nil, fmt.Errorf("Error reading CSV file: %w", err)
		}
		return df, nil
	case "parquet":
		df, err := ReadParquet(dfr.filePath)
		if err != nil {
			return nil, fmt.Errorf("Error reading Parquet file: %w", err)
		}
		return df, nil
	default:
		return nil, fmt.Errorf("Unsupported file type: %s", dfr.fileType)
	}
}

// detectFileType attempts to detect the file type from the file extension
func detectFileType(filePath string) string {
	lowerPath := strings.ToLower(filePath)
	if strings.HasSuffix(lowerPath, ".csv") {
		return "csv"
	}
	if strings.HasSuffix(lowerPath, ".parquet") {
		return "parquet"
	}
	// Default to CSV if can't determine
	return "csv"
}

// readCSVFromFile reads a CSV file and returns a DataFrame
func readCSVFromFile(path string, options *Options) (*DataFrame, error) {
	// Open the file
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("Error opening file: %s, %w", path, err)
	}
	defer file.Close()

	// Create a buffered reader for efficiency
	reader := bufio.NewReader(file)

	// Use the common CSV reading function
	return readCSV(reader, options)
}

// readCSVFromString reads a CSV from a string and returns a DataFrame
func readCSVFromString(content string, options *Options) (*DataFrame, error) {
	// Create a reader from the string
	reader := bufio.NewReader(strings.NewReader(content))

	// Use the common CSV reading function
	return readCSV(reader, options)
}

// readCSV is a common function to read CSV data from any source
func readCSV(reader io.Reader, options *Options) (*DataFrame, error) {
	// Create a CSV reader
	csvReader := csv.NewReader(reader)
	csvReader.Comma = options.GetDelimiter()
	csvReader.TrimLeadingSpace = options.GetTrimLeadingSpace()

	// Read all records at once
	// This is faster than reading line by line, but uses more memory
	// If memory becomes an issue, we can implement a chunked reading approach
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("Error reading CSV data: %w", err)
	}

	// Handle empty file
	if len(records) == 0 {
		return NewDataFrame(), nil
	}

	// Process header row if present
	var headers []string
	var dataRows [][]string

	if options.GetHeader() {
		if len(records) > 0 {
			headers = records[0]
			dataRows = records[1:]
		} else {
			headers = []string{}
			dataRows = [][]string{}
		}
	} else {
		// Generate column names
		headers = make([]string, len(records[0]))
		for i := range headers {
			headers[i] = fmt.Sprintf("Column %d", i)
		}
		dataRows = records
	}

	// Create DataFrame with appropriate series types
	df := NewDataFrame()

	// Process each column
	for colIdx := range headers {
		// Extract column values
		colValues := make([]string, len(dataRows))
		for rowIdx, row := range dataRows {
			if colIdx < len(row) {
				colValues[rowIdx] = row[colIdx]
			} else {
				colValues[rowIdx] = "" // Handle missing values
			}
		}

		// Detect and create appropriate series type
		if options.GetInferDataTypes() {
			seriesType, err := inferType(colValues)
			if err != nil {
				// If type inference fails, default to string
				df.AddSeries(series.NewStringSeries(headers[colIdx], colValues))
				continue
			}

			switch seriesType {
			case "int":
				intValues, err := convertToIntSlice(colValues)
				if err != nil {
					df.AddSeries(series.NewStringSeries(headers[colIdx], colValues))
				} else {
					df.AddSeries(series.NewIntSeries(headers[colIdx], intValues))
				}
			case "float":
				floatValues, err := convertToFloatSlice(colValues)
				if err != nil {
					df.AddSeries(series.NewStringSeries(headers[colIdx], colValues))
				} else {
					df.AddSeries(series.NewFloat64Series(headers[colIdx], floatValues))
				}
			case "bool":
				boolValues, err := convertToBoolSlice(colValues)
				if err != nil {
					df.AddSeries(series.NewStringSeries(headers[colIdx], colValues))
				} else {
					df.AddSeries(series.NewBoolSeries(headers[colIdx], boolValues))
				}
			default:
				df.AddSeries(series.NewStringSeries(headers[colIdx], colValues))
			}
		} else {
			// No type inference, use string series
			df.AddSeries(series.NewStringSeries(headers[colIdx], colValues))
		}
	}

	return df, nil
}

// inferType detects the most appropriate type for a column
func inferType(values []string) (string, error) {
	if len(values) == 0 {
		return "string", nil
	}

	// Counters for type detection
	var (
		emptyCount  int
		intCount    int
		floatCount  int
		boolCount   int
		stringCount int
	)

	// Check each value
	for _, val := range values {
		val = strings.TrimSpace(val)

		// Skip empty values in type detection
		if val == "" {
			emptyCount++
			continue
		}

		// Try bool first (fastest check)
		if isBool(val) {
			boolCount++
			continue
		}

		// Try int
		if isInt(val) {
			intCount++
			continue
		}

		// Try float
		if isFloat(val) {
			floatCount++
			continue
		}

		// If none of the above, it's a string
		stringCount++
	}

	// Determine the dominant type
	nonEmptyCount := len(values) - emptyCount
	if nonEmptyCount == 0 {
		return "string", nil // All values are empty
	}

	// Rule: if any values are strings, the whole column is string
	if stringCount > 0 {
		return "string", nil
	}

	// Rules for other types - require at least 90% conformity
	if float64(boolCount)/float64(nonEmptyCount) >= 0.9 {
		return "bool", nil
	}

	if float64(intCount)/float64(nonEmptyCount) >= 0.9 {
		return "int", nil
	}

	if float64(intCount+floatCount)/float64(nonEmptyCount) >= 0.9 {
		return "float", nil
	}

	// Default to string
	return "string", nil
}

// isBool checks if a string represents a boolean value
func isBool(val string) bool {
	lower := strings.ToLower(val)
	return lower == "true" || lower == "false" ||
		lower == "yes" || lower == "no" ||
		lower == "t" || lower == "f" ||
		lower == "1" || lower == "0"
}

// isInt checks if a string represents an integer
func isInt(val string) bool {
	// Remove common formatting
	clean := strings.ReplaceAll(val, ",", "")
	clean = strings.ReplaceAll(clean, " ", "")

	_, err := strconv.Atoi(clean)
	return err == nil
}

// isFloat checks if a string represents a floating point number
func isFloat(val string) bool {
	// Remove common formatting
	clean := strings.ReplaceAll(val, ",", "")
	clean = strings.ReplaceAll(clean, " ", "")

	_, err := strconv.ParseFloat(clean, 64)
	return err == nil
}

// convertToIntSlice converts a slice of strings to a slice of ints
func convertToIntSlice(values []string) ([]int, error) {
	result := make([]int, len(values))
	for i, val := range values {
		if val == "" {
			result[i] = 0 // Default value for empty strings
			continue
		}

		// Clean the value
		clean := strings.ReplaceAll(val, ",", "")
		clean = strings.TrimSpace(clean)

		// Parse as int
		intVal, err := strconv.Atoi(clean)
		if err != nil {
			return nil, err
		}
		result[i] = intVal
	}
	return result, nil
}

// convertToFloatSlice converts a slice of strings to a slice of float64s
func convertToFloatSlice(values []string) ([]float64, error) {
	result := make([]float64, len(values))
	for i, val := range values {
		if val == "" {
			result[i] = 0.0 // Default value for empty strings
			continue
		}

		// Clean the value
		clean := strings.ReplaceAll(val, ",", "")
		clean = strings.TrimSpace(clean)

		// Parse as float
		floatVal, err := strconv.ParseFloat(clean, 64)
		if err != nil {
			return nil, err
		}
		result[i] = floatVal
	}
	return result, nil
}

// convertToBoolSlice converts a slice of strings to a slice of bools
func convertToBoolSlice(values []string) ([]bool, error) {
	result := make([]bool, len(values))
	for i, val := range values {
		if val == "" {
			result[i] = false // Default value for empty strings
			continue
		}

		// Clean and convert common boolean representations
		clean := strings.ToLower(strings.TrimSpace(val))

		switch clean {
		case "true", "t", "yes", "y", "1":
			result[i] = true
		case "false", "f", "no", "n", "0":
			result[i] = false
		default:
			return nil, fmt.Errorf("cannot convert %s to bool", val)
		}
	}
	return result, nil
}
