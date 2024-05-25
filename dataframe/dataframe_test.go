package dataframe

import (
	"strconv"
	"testing"
)

func TestNewDataFrame(t *testing.T) {
	df := NewDataFrame()

	df = df.AddSeries(NewSeries("First Name", []interface{}{"John", "Jack", "Tyler", "Jill", "Kenny", "Aaron"}))
	df = df.AddSeries(NewSeries("Last Name", []interface{}{"Doe", "Smith", "Johnson", "Brown", "Peters", "Williams"}))
	df = df.AddSeries(NewSeries("Age", []interface{}{35, 23, 48, 63, 28, 32}))

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
	df = df.AddSeries(NewSeries("Last Name", []interface{}{"Doe", "Smith", "Johnson", "Brown", "Peters", "Williams"}))
	df = df.AddSeries(NewSeries("Age", []interface{}{35, 23, 48, 63, 28, 32}))

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

func TestDeleteColumn(t *testing.T) {
	df := NewDataFrame()

	df = df.AddSeries(NewSeries("First Name", []interface{}{"John", "Jack", "Tyler", "Jill", "Kenny", "Aaron"}))
	df = df.AddSeries(NewSeries("Last Name", []interface{}{"Doe", "Smith", "Johnson", "Brown", "Peters", "Williams"}))
	df = df.AddSeries(NewSeries("Age", []interface{}{35, 23, 48, 63, 28, 32}))

	df = df.DropColumn("Age")
	// df.PrintTable()

	row, col := df.Shape()
	if row != 6 || col != 2 {
		t.Errorf("Expected 6 rows and 2 columns, got %d rows and %d columns", row, col)
	}

	df = df.AddSeries(NewSeries("Age", []interface{}{35, 23, 48, 63, 28, 32}))

	row, col = df.Shape()
	if row != 6 || col != 3 {
		t.Errorf("Expected 6 rows and 3 columns, got %d rows and %d columns", row, col)
	}

	df = df.DropColumn("First Name", "Last Name")

	row, col = df.Shape()
	if row != 6 || col != 1 {
		t.Errorf("Expected 6 rows and 1 columns, got %d rows and %d columns", row, col)
	}

}

func TestApplyIndex(t *testing.T) {
	df := NewDataFrame()

	df = df.AddSeries(NewSeries("First Name", []interface{}{"John", "Jack", "Tyler", "Jill", "Kenny", "Aaron"}))
	df = df.AddSeries(NewSeries("Last Name", []interface{}{"Doe", "Smith", "Johnson", "Brown", "Peters", "Williams"}))
	df = df.AddSeries(NewSeries("Age", []interface{}{"35", "23", "48", "63", "28", "32"}))

	df1 := df.ApplyIndex("Full Name", func(a ...interface{}) interface{} {
		return a[0].(string) + " " + a[1].(string)
	}, "First Name", "Last Name")

	// Verify that that the column was correctly populated.
	expected := []interface{}{"John Doe", "Jack Smith", "Tyler Johnson", "Jill Brown", "Kenny Peters", "Aaron Williams"}
	for i, val := range df1.GetSeries("Full Name").Values {
		if val != expected[i] {
			t.Errorf("Expected value %s, got %s", expected[i], val)
		}
	}

	row, col := df1.Shape()
	if row != 6 || col != 4 {
		t.Errorf("Expected 6 rows and 4 columns, got %d rows and %d columns", row, col)
	}
	// df1.PrintTable()

	// This version allows use to return a different type.
	df2 := df1.ApplyIndex("Age Int", func(a ...interface{}) interface{} {
		i, _ := strconv.Atoi(a[0].(string))
		return i
	}, "Age")

	// Verify that that the column was correctly populated.
	expected = []interface{}{35, 23, 48, 63, 28, 32}
	for i, val := range df2.GetSeries("Age Int").Values {
		if val != expected[i] {
			t.Errorf("Expected value %d, got %d", expected[i], val)
		}
	}
	df2.DropColumn("Age Int")

	// This version allows you to get the entire column as a slice. From
	// there you can do whatever you want with it.
	df3 := df2.ApplySeries("Age Cubed", func(s ...[]interface{}) []interface{} {
		s1 := s[0] // The index refers to the "Age" column passed in below.
		s2 := make([]interface{}, len(s1))

		for index, val := range s1 {
			i, _ := strconv.Atoi(val.(string))
			s2[index] = i * i * i
		}
		return s2
	}, "Age")

	// Verify that that the column was correctly populated.
	expected = []interface{}{35 * 35 * 35, 23 * 23 * 23, 48 * 48 * 48, 63 * 63 * 63, 28 * 28 * 28, 32 * 32 * 32}
	for i, val := range df3.GetSeries("Age Cubed").Values {
		if val != expected[i] {
			t.Errorf("Expected value %d, got %d", expected[i], val)
		}
	}
}

func TestSimpleTypeConversion(t *testing.T) {
	df := NewDataFrame()

	df = df.AddSeries(NewSeries("First Name", []interface{}{"John", "Jack", "Tyler", "Jill", "Kenny", "Aaron"}))
	df = df.AddSeries(NewSeriesWithType("Age", []interface{}{35, 23, 48, 63, 28, 32}, "int"))
	df = df.AddSeries(NewSeries("Is Student", []interface{}{true, false, true, false, true, false}))
	df = df.AddSeries(NewSeries("Height", []interface{}{5.8, 6.1, 5.9, 5.6, 6.0, 5.7}))

	// df.PrintTable()

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

	// Verify that that the column was correctly populated.
	expected := []float64{35 * 2.56, 23 * 2.56, 48 * 2.56, 63 * 2.56, 28 * 2.56, 32 * 2.56}
	for i, val := range InterfaceToTypeSlice[float64](df2.GetSeries("Age").Values) {
		if val-expected[i] > 0.00000001 {
			t.Errorf("Expected value %f, got %f. Diff %f", expected[i], val, val-expected[i])
		}
	}

	// df2.PrintTable()
}

func TestComplexTypeConversion(t *testing.T) {
	df := NewDataFrame()

	df = df.AddSeries(NewSeries("First Name", []interface{}{"John", "Jack", "Tyler", "Jill", "Kenny", "Aaron"}))
	df = df.AddSeries(NewSeries("Last Name", []interface{}{"Doe", "Smith", "Johnson", "Brown", "Peters", "Williams"}))
	df = df.AddSeries(NewSeries("Age", []interface{}{35, 23, 48, 63, 28, 32}))
	df.GetSeries("Age").AsType("float")

	df2 := df.ApplyMap("Age2", func(m map[string]interface{}) interface{} {
		return float64(int(m["Age"].(float64)*2.56*100.0)) / 100.0
	})
	df2.GetSeries("Age2").AsType("string")
	// df2.PrintTable()

	df3 := df2.ApplyMap("Age2", func(m map[string]interface{}) interface{} {
		return m["Age2"].(string) + " = idk"
	})
	df3.GetSeries("Age2").AsType("string")
	// df3.PrintTable()

	// Verify that that the column was correctly populated.
	expected := []interface{}{"89.6 = idk", "58.88 = idk", "122.88 = idk", "161.28 = idk", "71.68 = idk", "81.92 = idk"}
	for i, val := range df3.GetSeries("Age2").Values {
		if val != expected[i] {
			t.Errorf("Expected value %s, got %s", expected[i], val)
		}
	}

}
