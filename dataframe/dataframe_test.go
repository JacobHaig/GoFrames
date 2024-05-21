package dataframe

import (
	"testing"
)

func TestNewDataFrame(t *testing.T) {
	df := NewDataFrame()

	df = df.AddSeries(NewSeries("First Name", []interface{}{"John", "Jack", "Tyler", "Jill", "Kenny", "Aaron"}))
	df = df.AddSeries(NewSeries("Age", []interface{}{35, 23, 48, 63, 28, 32}))
	df = df.AddSeries(NewSeries("Last Name", []interface{}{"Doe", "Smith", "Johnson", "Brown", "Peters", "Williams"}))

	df = df.AddRow([]interface{}{"Jane", 29, "Doe"})
	// df.PrintTable()

	row, col := df.Shape()
	if row != 7 || col != 3 {
		t.Errorf("Expected 7 rows and 3 columns, got %d rows and %d columns", row, col)
	}

	df = df.DropRow(0)
	df = df.DropColumn("Last Name")
	// df.PrintTable()

	row, col = df.Shape()
	if row != 6 || col != 2 {
		t.Errorf("Expected 6 rows and 2 columns, got %d rows and %d columns", row, col)
	}
}

func TestFilterMap(t *testing.T) {
	df := NewDataFrame()

	df = df.AddSeries(NewSeries("First Name", []interface{}{"John", "Jack", "Tyler", "Jill", "Kenny", "Aaron"}))
	df = df.AddSeries(NewSeries("Age", []interface{}{35, 23, 48, 63, 28, 32}))
	df = df.AddSeries(NewSeries("Last Name", []interface{}{"Doe", "Smith", "Johnson", "Brown", "Peters", "Williams"}))

	df = df.FilterMap(func(m map[string]interface{}) bool {
		return m["Age"].(int) > 30
	})

	row, col := df.Shape()
	if row != 4 || col != 3 {
		t.Errorf("Expected 4 rows and 3 columns, got %d rows and %d columns", row, col)
	}

	df = df.FilterMap(func(m map[string]interface{}) bool {
		return m["Last Name"].(string) != "Smith"
	})

	row, col = df.Shape()
	if row != 4 || col != 3 {
		t.Errorf("Expected 3 rows and 3 columns, got %d rows and %d columns", row, col)
	}
}

func TestSimpleTypeConversion(t *testing.T) {
	df := NewDataFrame()

	df = df.AddSeries(NewSeries("First Name", []interface{}{"John", "Jack", "Tyler", "Jill", "Kenny", "Aaron"}))
	df = df.AddSeries(NewSeriesWithType("Age", []interface{}{35, 23, 48, 63, 28, 32}, "int"))
	df = df.AddSeries(NewSeries("Is Student", []interface{}{true, false, true, false, true, false}))
	df = df.AddSeries(NewSeries("Height", []interface{}{5.8, 6.1, 5.9, 5.6, 6.0, 5.7}))

	df.PrintTable()

	df = df.AsType("Age", "float")
	df = df.AsType("Is Student", "int")
	df = df.AsType("Height", "int")

	df2 := df.ApplyMap("Age", func(m map[string]interface{}) interface{} {
		return m["Age"].(float64) * 2.56
	})
	df2 = df2.ApplyMap("Is Student", func(m map[string]interface{}) interface{} {
		return m["Is Student"].(int) * 2
	})
	df2 = df2.ApplyMap("Height", func(m map[string]interface{}) interface{} {
		return m["Height"].(int) * 2
	})

	row, col := df2.Shape()
	if row != 6 || col != 4 {
		t.Errorf("Expected 6 rows and 4 columns, got %d rows and %d columns", row, col)
	}
	df2.PrintTable()
}
