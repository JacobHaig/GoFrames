package dataframe

import (
	"strconv"
	"testing"
)

func TestNewDataFrame(t *testing.T) {
	df := NewDataFrame()

	// Add typed series instead of generic series
	df = df.AddSeries(NewStringSeries("First Name", []string{"John", "Jack", "Tyler", "Jill", "Kenny", "Aaron"}))
	df = df.AddSeries(NewStringSeries("Last Name", []string{"Doe", "Smith", "Johnson", "Brown", "Peters", "Williams"}))
	df = df.AddSeries(NewIntSeries("Age", []int{35, 23, 48, 63, 28, 32}))

	// Add a row of mixed types
	df = df.AddRow([]any{"Jane", "Doe", 29})

	// Check DataFrame shape
	row, col := df.Shape()
	if row != 7 || col != 3 {
		t.Errorf("Expected 7 rows and 3 columns, got %d rows and %d columns", row, col)
	}

	// Test row and column operations
	df = df.DropRow(0)
	df = df.DropColumn("Last Name")

	row, col = df.Shape()
	if row != 6 || col != 2 {
		t.Errorf("Expected 6 rows and 2 columns, got %d rows and %d columns", row, col)
	}
}

func TestFilterMap(t *testing.T) {
	df := NewDataFrame()

	df = df.AddSeries(NewStringSeries("First Name", []string{"John", "Jack", "Tyler", "Jill", "Kenny", "Aaron"}))
	df = df.AddSeries(NewStringSeries("Last Name", []string{"Doe", "Smith", "Johnson", "Brown", "Peters", "Williams"}))
	df = df.AddSeries(NewIntSeries("Age", []int{35, 23, 48, 63, 28, 32}))

	// Filter rows where Age > 30
	df = df.FilterMap(func(m map[string]any) bool {
		age, ok := m["Age"].(int)
		if !ok {
			t.Errorf("Expected Age to be int, got %T", m["Age"])
			return false
		}
		return age > 30
	})

	row, col := df.Shape()
	if row != 4 || col != 3 {
		t.Errorf("Expected 4 rows and 3 columns, got %d rows and %d columns", row, col)
	}

	// Further filter by Last Name
	df = df.FilterMap(func(m map[string]any) bool {
		lastName, ok := m["Last Name"].(string)
		if !ok {
			t.Errorf("Expected Last Name to be string, got %T", m["Last Name"])
			return false
		}
		return lastName != "Smith"
	})

	row, col = df.Shape()
	if row != 4 || col != 3 {
		t.Errorf("Expected 4 rows and 3 columns after Smith filter, got %d rows and %d columns", row, col)
	}
}

func TestDeleteColumn(t *testing.T) {
	df := NewDataFrame()

	df = df.AddSeries(NewStringSeries("First Name", []string{"John", "Jack", "Tyler", "Jill", "Kenny", "Aaron"}))
	df = df.AddSeries(NewStringSeries("Last Name", []string{"Doe", "Smith", "Johnson", "Brown", "Peters", "Williams"}))
	df = df.AddSeries(NewIntSeries("Age", []int{35, 23, 48, 63, 28, 32}))

	df = df.DropColumn("Age")

	row, col := df.Shape()
	if row != 6 || col != 2 {
		t.Errorf("Expected 6 rows and 2 columns, got %d rows and %d columns", row, col)
	}

	df = df.AddSeries(NewIntSeries("Age", []int{35, 23, 48, 63, 28, 32}))

	row, col = df.Shape()
	if row != 6 || col != 3 {
		t.Errorf("Expected 6 rows and 3 columns, got %d rows and %d columns", row, col)
	}

	df = df.DropColumn("First Name", "Last Name")

	row, col = df.Shape()
	if row != 6 || col != 1 {
		t.Errorf("Expected 6 rows and 1 column, got %d rows and %d columns", row, col)
	}
}

func TestApplyIndex(t *testing.T) {
	df := NewDataFrame()

	df = df.AddSeries(NewStringSeries("First Name", []string{"John", "Jack", "Tyler", "Jill", "Kenny", "Aaron"}))
	df = df.AddSeries(NewStringSeries("Last Name", []string{"Doe", "Smith", "Johnson", "Brown", "Peters", "Williams"}))
	df = df.AddSeries(NewStringSeries("Age", []string{"35", "23", "48", "63", "28", "32"}))

	df1 := df.ApplyIndex("Full Name", func(a ...any) any {
		return a[0].(string) + " " + a[1].(string)
	}, "First Name", "Last Name")

	// Verify that the column was correctly populated
	expected := []any{"John Doe", "Jack Smith", "Tyler Johnson", "Jill Brown", "Kenny Peters", "Aaron Williams"}
	fullNameSeries := df1.GetSeries("Full Name")
	if fullNameSeries == nil {
		t.Errorf("Full Name series not found")
	} else {
		for i := 0; i < fullNameSeries.Len(); i++ {
			if fullNameSeries.Get(i) != expected[i] {
				t.Errorf("Expected value %s, got %s", expected[i], fullNameSeries.Get(i))
			}
		}
	}

	row, col := df1.Shape()
	if row != 6 || col != 4 {
		t.Errorf("Expected 6 rows and 4 columns, got %d rows and %d columns", row, col)
	}

	// This version allows us to return a different type
	df2 := df1.ApplyIndex("Age Int", func(a ...any) any {
		i, _ := strconv.Atoi(a[0].(string))
		return i
	}, "Age")

	// Verify that the column was correctly populated
	expectedInts := []any{35, 23, 48, 63, 28, 32}
	ageIntSeries := df2.GetSeries("Age Int")
	if ageIntSeries == nil {
		t.Errorf("Age Int series not found")
	} else {
		for i := 0; i < ageIntSeries.Len(); i++ {
			if ageIntSeries.Get(i) != expectedInts[i] {
				t.Errorf("Expected value %d, got %v", expectedInts[i], ageIntSeries.Get(i))
			}
		}
	}
	df2 = df2.DropColumn("Age Int")

	// Test ApplySeries - operating on entire columns
	df3 := df2.ApplySeries("Age Cubed", func(s ...[]any) []any {
		s1 := s[0] // The index refers to the "Age" column passed in below
		s2 := make([]any, len(s1))

		for index, val := range s1 {
			i, _ := strconv.Atoi(val.(string))
			s2[index] = i * i * i
		}
		return s2
	}, "Age")

	// Verify that the column was correctly populated
	expectedCubed := []any{35 * 35 * 35, 23 * 23 * 23, 48 * 48 * 48, 63 * 63 * 63, 28 * 28 * 28, 32 * 32 * 32}
	ageCubedSeries := df3.GetSeries("Age Cubed")
	if ageCubedSeries == nil {
		t.Errorf("Age Cubed series not found")
	} else {
		for i := 0; i < ageCubedSeries.Len(); i++ {
			if ageCubedSeries.Get(i) != expectedCubed[i] {
				t.Errorf("Expected value %d, got %v", expectedCubed[i], ageCubedSeries.Get(i))
			}
		}
	}
}

func TestTypeConversion(t *testing.T) {
	df := NewDataFrame()

	df = df.AddSeries(NewStringSeries("First Name", []string{"John", "Jack", "Tyler", "Jill", "Kenny", "Aaron"}))
	df = df.AddSeries(NewIntSeries("Age", []int{35, 23, 48, 63, 28, 32}))
	df = df.AddSeries(NewBoolSeries("Is Student", []bool{true, false, true, false, true, false}))
	df = df.AddSeries(NewFloat64Series("Height", []float64{5.8, 6.1, 5.9, 5.6, 6.0, 5.7}))

	// Test type conversion
	df = df.AsType("Age", "float")
	df = df.AsType("Is Student", "int")
	df = df.AsType("Height", "int")

	// Verify conversions
	ageSeries := df.GetSeries("Age")
	if ageSeries.Type().String() != "float64" {
		t.Errorf("Expected Age to be float64, got %s", ageSeries.Type().String())
	}

	isStudentSeries := df.GetSeries("Is Student")
	if isStudentSeries.Type().String() != "int" {
		t.Errorf("Expected Is Student to be int, got %s", isStudentSeries.Type().String())
	}

	heightSeries := df.GetSeries("Height")
	if heightSeries.Type().String() != "int" {
		t.Errorf("Expected Height to be int, got %s", heightSeries.Type().String())
	}

	// Apply transformations to the converted columns
	df2 := df.ApplyMap("Age", func(m map[string]any) any {
		return m["Age"].(float64) * 2.56
	})
	df2 = df2.ApplyMap("Is Student", func(m map[string]any) any {
		return m["Is Student"].(int) * 2
	})
	df2 = df2.ApplyMap("Height", func(m map[string]any) any {
		return m["Height"].(int) * 2
	})

	// Verify shape is unchanged
	row, col := df2.Shape()
	if row != 6 || col != 4 {
		t.Errorf("Expected 6 rows and 4 columns, got %d rows and %d columns", row, col)
	}

	// Verify the transformations were applied correctly
	ageSeries = df2.GetSeries("Age")
	expected := []float64{35 * 2.56, 23 * 2.56, 48 * 2.56, 63 * 2.56, 28 * 2.56, 32 * 2.56}
	for i := 0; i < ageSeries.Len(); i++ {
		if ageSeries.Get(i).(float64) != expected[i] {
			t.Errorf("Expected Age value %f, got %f", expected[i], ageSeries.Get(i).(float64))
		}
	}
}

func TestComplexTypeConversion(t *testing.T) {
	df := NewDataFrame()

	df = df.AddSeries(NewStringSeries("First Name", []string{"John", "Jack", "Tyler", "Jill", "Kenny", "Aaron"}))
	df = df.AddSeries(NewStringSeries("Last Name", []string{"Doe", "Smith", "Johnson", "Brown", "Peters", "Williams"}))
	df = df.AddSeries(NewIntSeries("Age", []int{35, 23, 48, 63, 28, 32}))

	// Convert Ages to float and apply a mathematical transformation
	df = df.AsType("Age", "float")
	df2 := df.ApplyMap("Age2", func(m map[string]any) any {
		return float64(int(m["Age"].(float64)*2.56*100.0)) / 100.0
	})

	// Convert the new column to string
	df2 = df2.AsType("Age2", "string")

	// Append text to the string column
	df3 := df2.ApplyMap("Age2", func(m map[string]any) any {
		return m["Age2"].(string) + " = idk"
	})

	// Verify the results
	expected := []string{"89.6 = idk", "58.88 = idk", "122.88 = idk", "161.28 = idk", "71.68 = idk", "81.92 = idk"}
	age2Series := df3.GetSeries("Age2")
	if age2Series == nil {
		t.Errorf("Age2 series not found")
	} else {
		for i := 0; i < age2Series.Len(); i++ {
			if age2Series.Get(i) != expected[i] {
				t.Errorf("Expected value %s, got %v", expected[i], age2Series.Get(i))
			}
		}
	}
}

func TestGroupByIndex(t *testing.T) {
	df := NewDataFrame()

	df = df.AddSeries(NewStringSeries("First Name", []string{"John", "Jack", "John", "Jill", "Jack", "Aaron"}))
	df = df.AddSeries(NewStringSeries("Last Name", []string{"Doe", "Smith", "Doe", "Brown", "Smith", "Williams"}))
	df = df.AddSeries(NewIntSeries("Age", []int{35, 23, 48, 63, 28, 32}))

	// Group by First Name and sum the Ages
	df = df.GroupByIndex("First Name", func(list ...any) any {
		// Sum of ages
		sum := 0
		for _, value := range list {
			sum += value.(int)
		}
		return sum
	}, "Age")

	// Verify the result
	row, col := df.Shape()
	if row != 4 || col != 2 {
		t.Errorf("Expected 4 rows and 2 columns, got %d rows and %d columns", row, col)
	}

	// Check specifically the Jack group
	df_filtered := df.FilterMap(func(m map[string]any) bool {
		return m["First Name"].(string) == "Jack"
	})

	// Jack's ages were 23 + 28 = 51
	expected := 51
	ageSeries := df_filtered.GetSeries("Age")
	if ageSeries == nil || ageSeries.Len() == 0 {
		t.Errorf("Age series not found or empty for Jack")
	} else {
		actual := ageSeries.Get(0).(int)
		if actual != expected {
			t.Errorf("Expected summed age for Jack to be %d, got %d", expected, actual)
		}
	}
}

func TestAggGroupByIndex(t *testing.T) {
	df := NewDataFrame()

	df = df.AddSeries(NewStringSeries("First Name", []string{"John", "Jack", "John", "Jill", "Jack", "Aaron"}))
	df = df.AddSeries(NewStringSeries("Last Name", []string{"Doe", "Smith", "Doe", "Brown", "Smith", "Williams"}))
	df = df.AddSeries(NewIntSeries("Age", []int{35, 23, 48, 63, 28, 32}))

	// Group by First Name and use the Sum aggregator
	df = df.GroupByIndex("First Name", Sum, "Age")

	// Verify the result
	row, col := df.Shape()
	if row != 4 || col != 2 {
		t.Errorf("Expected 4 rows and 2 columns, got %d rows and %d columns", row, col)
	}

	// Check specifically the Jack group
	df_filtered := df.FilterMap(func(m map[string]any) bool {
		return m["First Name"].(string) == "Jack"
	})

	// Jack's ages were 23 + 28 = 51
	expected := 51
	ageSeries := df_filtered.GetSeries("Age")
	if ageSeries == nil || ageSeries.Len() == 0 {
		t.Errorf("Age series not found or empty for Jack")
	} else {
		actual := ageSeries.Get(0).(int)
		if actual != expected {
			t.Errorf("Expected summed age for Jack to be %d, got %d", expected, actual)
		}
	}
}

func TestReadCSV(t *testing.T) {
	// This is a simple CSV content for testing
	csvContent := `Name,Age,IsStudent
John,25,true
Jane,30,false
Bob,22,true`

	// Create a DataFrame from the string
	df, err := Read().
		FromString(csvContent).
		Option("header", true).
		Option("inferdatatypes", true).
		Load()

	if err != nil {
		t.Errorf("Error reading CSV: %v", err)
	}

	// Check DataFrame dimensions
	row, col := df.Shape()
	if row != 3 || col != 3 {
		t.Errorf("Expected 3 rows and 3 columns, got %d rows and %d columns", row, col)
	}

	// Check column types
	nameSeries := df.GetSeries("Name")
	if nameSeries.Type().String() != "string" {
		t.Errorf("Expected Name to be string, got %s", nameSeries.Type().String())
	}

	ageSeries := df.GetSeries("Age")
	if ageSeries.Type().String() != "int" {
		t.Errorf("Expected Age to be int, got %s", ageSeries.Type().String())
	}

	isStudentSeries := df.GetSeries("IsStudent")
	if isStudentSeries.Type().String() != "bool" {
		t.Errorf("Expected IsStudent to be bool, got %s", isStudentSeries.Type().String())
	}

	// Verify values
	if ageSeries.Get(0).(int) != 25 {
		t.Errorf("Expected first age to be 25, got %v", ageSeries.Get(0))
	}

	if nameSeries.Get(1).(string) != "Jane" {
		t.Errorf("Expected second name to be Jane, got %v", nameSeries.Get(1))
	}

	if isStudentSeries.Get(2).(bool) != true {
		t.Errorf("Expected third IsStudent to be true, got %v", isStudentSeries.Get(2))
	}
}

func TestWriteAndReadCSV(t *testing.T) {
	// Create a simple DataFrame
	df := NewDataFrame()
	df = df.AddSeries(NewStringSeries("Name", []string{"John", "Jane", "Bob"}))
	df = df.AddSeries(NewIntSeries("Age", []int{25, 30, 22}))
	df = df.AddSeries(NewBoolSeries("IsStudent", []bool{true, false, true}))

	// Use a temporary file
	tempFile := "test_output.csv"

	// Write to CSV
	err := df.Write().
		FileType("csv").
		FilePath(tempFile).
		Option("header", true).
		Save()

	if err != nil {
		t.Errorf("Error writing CSV: %v", err)
	}

	// Read back from CSV
	df2, err := Read().
		FileType("csv").
		FilePath(tempFile).
		Option("header", true).
		Option("inferdatatypes", true).
		Load()

	if err != nil {
		t.Errorf("Error reading CSV: %v", err)
	}

	// Check DataFrame dimensions
	row, col := df2.Shape()
	if row != 3 || col != 3 {
		t.Errorf("Expected 3 rows and 3 columns, got %d rows and %d columns", row, col)
	}

	// Verify values
	ageSeries := df2.GetSeries("Age")
	if ageSeries.Get(0).(int) != 25 {
		t.Errorf("Expected first age to be 25, got %v", ageSeries.Get(0))
	}

	nameSeries := df2.GetSeries("Name")
	if nameSeries.Get(1).(string) != "Jane" {
		t.Errorf("Expected second name to be Jane, got %v", nameSeries.Get(1))
	}

	// Check if IsStudent is correctly read back as boolean
	isStudentSeries := df2.GetSeries("IsStudent")
	if isStudentSeries.Type().String() != "bool" {
		t.Errorf("Expected IsStudent to be bool, got %s", isStudentSeries.Type().String())
	}

	// Clean up
	// os.Remove(tempFile)
}

func TestTypedSeriesCreation(t *testing.T) {
	// Test creating different types of Series
	intSeries := NewIntSeries("Numbers", []int{1, 2, 3, 4, 5})
	if intSeries.Type().String() != "int" {
		t.Errorf("Expected int series, got %s", intSeries.Type().String())
	}
	if intSeries.Len() != 5 {
		t.Errorf("Expected length 5, got %d", intSeries.Len())
	}

	floatSeries := NewFloat64Series("Floats", []float64{1.1, 2.2, 3.3})
	if floatSeries.Type().String() != "float64" {
		t.Errorf("Expected float64 series, got %s", floatSeries.Type().String())
	}

	stringSeries := NewStringSeries("Strings", []string{"a", "b", "c"})
	if stringSeries.Type().String() != "string" {
		t.Errorf("Expected string series, got %s", stringSeries.Type().String())
	}

	boolSeries := NewBoolSeries("Bools", []bool{true, false, true})
	if boolSeries.Type().String() != "bool" {
		t.Errorf("Expected bool series, got %s", boolSeries.Type().String())
	}

	// Test auto-detection of types
	mixedSeries := NewSeries("Mixed", []any{1, "string", true})
	// This should remain a generic series since the types are mixed
	if _, ok := mixedSeries.(*GenericSeries); !ok {
		t.Errorf("Expected generic series for mixed types")
	}

	// This should be detected as an int series
	intOnlySeries := NewSeries("IntOnly", []any{1, 2, 3})
	if _, ok := intOnlySeries.(*IntSeries); !ok {
		t.Errorf("Expected int series for int-only values")
	}
}

func TestMemoryUsage(t *testing.T) {
	// This isn't a real test, but demonstrates memory usage
	// Create a DataFrame with typed and untyped series

	// Small size for quick test
	size := 1000

	// Create a DataFrame with a typed int series
	df1 := NewDataFrame()
	intValues := make([]int, size)
	for i := 0; i < size; i++ {
		intValues[i] = i
	}
	df1 = df1.AddSeries(NewIntSeries("Typed", intValues))

	// Create a DataFrame with an untyped series
	df2 := NewDataFrame()
	anyValues := make([]any, size)
	for i := 0; i < size; i++ {
		anyValues[i] = i
	}
	df2 = df2.AddSeries(NewGenericSeries("Untyped", anyValues))

	// Just check that they have the same shape - no actual memory test
	row1, col1 := df1.Shape()
	row2, col2 := df2.Shape()

	if row1 != row2 || col1 != col2 {
		t.Errorf("Expected same shape for both DataFrames")
	}
}
