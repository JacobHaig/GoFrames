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
