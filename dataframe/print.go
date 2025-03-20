package dataframe

import (
	"fmt"
	"teddy/dataframe/series"
)

// PrintTable prints a formatted table representation of the DataFrame
func (df *DataFrame) PrintTable(options ...OptionsMap) {
	optionsClean := standardizeOptions(options...)
	displayRows := optionsClean.getOption("display_rows", 10).(int)

	if df.Width() == 0 {
		fmt.Println("Empty DataFrame")
		return
	}

	// Calculate the max width of each column
	widths := make([]int, df.Width())
	printTypes := false // If there is at least one type, print the types in the header

	// max header
	for i, series := range df.series {
		// Column name width
		widths[i] = max(widths[i], len(series.Name()))

		// Column type width
		seriesType := series.Type()
		if seriesType != nil {
			typeName := seriesType.Name()
			widths[i] = max(widths[i], len(typeName))
			printTypes = true
		}

		// Maximum value width
		for j := 0; j < series.Len(); j++ {
			valueName := fmt.Sprint(series.Get(j))
			widths[i] = max(widths[i], len(valueName))
		}
	}

	// Print the header separator
	fmt.Print("+-")
	for i := range df.series {
		fmt.Print(PadRight("", "-", widths[i]))
		if i < df.Width()-1 {
			fmt.Print("-+-")
		}
	}
	fmt.Println("-+")

	// Print the header row (column names)
	fmt.Print("| ")
	for i, series := range df.series {
		fmt.Print(PadRight(series.Name(), " ", widths[i]))
		if i < df.Width()-1 {
			fmt.Print(" | ")
		}
	}
	fmt.Println(" |")

	// Print the type row if needed
	if printTypes {
		fmt.Print("| ")
		for i, series := range df.series {
			if seriesType := series.Type(); seriesType != nil {
				fmt.Print(PadRight(seriesType.Name(), " ", widths[i]))
			} else {
				fmt.Print(PadRight("", " ", widths[i]))
			}
			if i < df.Width()-1 {
				fmt.Print(" | ")
			}
		}
		fmt.Println(" |")
	}

	// Print the header/body separator
	fmt.Print("+-")
	for i := range df.series {
		fmt.Print(PadRight("", "-", widths[i]))
		if i < df.Width()-1 {
			fmt.Print("-+-")
		}
	}
	fmt.Println("-+")

	// Print data rows
	height := df.Height()
	if height > displayRows {
		printRows := displayRows

		// Print the first displayRows rows
		for i := 0; i < printRows; i++ {
			fmt.Print("| ")
			for j, series := range df.series {
				value := series.Get(i)
				fmt.Print(PadRight(fmt.Sprint(value), " ", widths[j]))
				if j < df.Width()-1 {
					fmt.Print(" | ")
				}
			}
			fmt.Println(" |")
		}

		// Print ellipsis row to indicate truncation
		fmt.Print("| ")
		for j, _ := range df.series {
			fmt.Print(PadRight("...", " ", widths[j]))
			if j < df.Width()-1 {
				fmt.Print(" | ")
			}
		}
		fmt.Println(" |")

		// Print row count info
		fmt.Printf("(%d rows total, showing first %d)\n", height, displayRows)
	} else {
		// Print all rows
		for i := 0; i < height; i++ {
			fmt.Print("| ")
			for j, series := range df.series {
				value := series.Get(i)
				fmt.Print(PadRight(fmt.Sprint(value), " ", widths[j]))
				if j < df.Width()-1 {
					fmt.Print(" | ")
				}
			}
			fmt.Println(" |")
		}
	}

	// Print the footer separator
	fmt.Print("+-")
	for i := range df.series {
		fmt.Print(PadRight("", "-", widths[i]))
		if i < df.Width()-1 {
			fmt.Print("-+-")
		}
	}
	fmt.Println("-+")
}

// Print prints a simpler representation of the DataFrame
func (df *DataFrame) Print() {
	if df.Width() == 0 {
		fmt.Println("Empty DataFrame")
		return
	}

	// Print column names
	for i, series := range df.series {
		fmt.Print(series.Name())
		if i < df.Width()-1 {
			fmt.Print(", ")
		}
	}
	fmt.Println()

	// Print data rows
	for i := 0; i < df.Height(); i++ {
		for j, series := range df.series {
			fmt.Print(series.Get(i))
			if j < df.Width()-1 {
				fmt.Print(", ")
			}
		}
		fmt.Println()
	}
}

// Summary prints a summary of the DataFrame
func (df *DataFrame) Summary() {
	if df.Width() == 0 {
		fmt.Println("Empty DataFrame")
		return
	}

	// Print shape
	rows, cols := df.Shape()
	fmt.Printf("DataFrame: %d rows × %d columns\n\n", rows, cols)

	// Print column information
	fmt.Println("Columns:")
	for i, seriess := range df.series {
		fmt.Printf("  %d: %s", i, seriess.Name())

		// Show column type
		if seriess.Type() != nil {
			fmt.Printf(" (Type: %s)", seriess.Type().Name())
		}

		// Show type-specific information
		switch s := seriess.(type) {
		case *series.IntSeries:
			if len(s.Values()) > 0 {
				values, _ := series.ToIntSlice(s.Values())
				min, max := findIntMinMax(values)
				fmt.Printf(" [Min: %d, Max: %d]", min, max)
			}
		case *series.Float64Series:
			if len(s.Values()) > 0 {
				values, _ := series.ToFloat64Slice(s.Values())
				min, max := findFloat64MinMax(values)
				fmt.Printf(" [Min: %.2f, Max: %.2f]", min, max)
			}
		case *series.StringSeries:
			if len(s.Values()) > 0 {
				values := series.ToStringSlice(s.Values())
				uniqueCount := countUniqueStrings(values)
				fmt.Printf(" [%d unique values]", uniqueCount)
			}
		case *series.BoolSeries:
			if len(s.Values()) > 0 {
				values, _ := series.ToBoolSlice(s.Values())
				trueCount := countBoolTrue(values)
				fmt.Printf(" [%d true, %d false]", trueCount, len(s.Values())-trueCount)
			}
		}

		fmt.Println()
	}

	// Print memory usage estimates
	fmt.Println("\nMemory Usage Estimate:")
	totalBytes := int64(0)

	for _, seriess := range df.series {
		seriesBytes := int64(0)
		switch s := seriess.(type) {
		case *series.IntSeries:
			seriesBytes = int64(s.Len() * 8) // 8 bytes per int
		case *series.Float64Series:
			seriesBytes = int64(s.Len() * 8) // 8 bytes per float64
		case *series.BoolSeries:
			seriesBytes = int64(s.Len() * 1) // 1 byte per bool
		case *series.StringSeries:
			// Estimate string size (rough approximation)
			stringSize := int64(0)
			for _, str := range series.ToStringSlice(s.Values()) {
				stringSize += int64(len(str))
			}
			seriesBytes = stringSize + int64(s.Len()*16) // String data + overhead
		case *series.GenericSeries:
			// Generic series is hard to estimate precisely
			seriesBytes = int64(s.Len() * 16) // Pointer size + type info
		}

		fmt.Printf("  %s: ~%s\n", seriess.Name(), formatBytes(seriesBytes))
		totalBytes += seriesBytes
	}

	fmt.Printf("  Total: ~%s\n", formatBytes(totalBytes))
}

// Helper function to find min and max values in an int slice
func findIntMinMax(values []int) (min, max int) {
	if len(values) == 0 {
		return 0, 0
	}

	min = values[0]
	max = values[0]

	for _, v := range values {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}

	return min, max
}

// Helper function to find min and max values in a float64 slice
func findFloat64MinMax(values []float64) (min, max float64) {
	if len(values) == 0 {
		return 0, 0
	}

	min = values[0]
	max = values[0]

	for _, v := range values {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}

	return min, max
}

// Helper function to count unique string values
func countUniqueStrings(values []string) int {
	uniqueMap := make(map[string]struct{})
	for _, v := range values {
		uniqueMap[v] = struct{}{}
	}
	return len(uniqueMap)
}

// Helper function to count true values in a bool slice
func countBoolTrue(values []bool) int {
	count := 0
	for _, v := range values {
		if v {
			count++
		}
	}
	return count
}

// Helper function to format bytes in a human-readable way
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
