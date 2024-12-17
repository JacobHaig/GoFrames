package dataframe

// I want to make a Streaming Dataframe library for Reading csv files in go. It should look like the spark  api. Can you please define the type StreamingFrame Struct? The Streamming Frame should have Options and a way to define the File type (csv parquet). Also the Streaming frame should have a Load Method when called will finish the execution plan. At the point of calling Load, it doesnt actually load, it returns a DataFramePlan. Which includes the entire plan. From there we can write to a file or print to the terminal to execute.

// Options struct holds configuration options for the StreamingFrame.
type Options2 struct {
	delimiter        rune
	trimLeadingSpace bool
	header           bool
}

// This is a DataFrameReader struct that holds the configuration for reading a file.
type DataFrameReader struct {
	filetype string
	filepath string
	options  Options2
}

// DataFramePlan represents the final execution plan with transformations.
type DataFramePlan struct {
	filetype        string
	filepath        string
	options         Options2
	columns         []string
	transformations []Transformation
}

// Transformation represents a transformation to be applied to a dataframe.
type Transformation interface {
	Apply(df *DataFramePlan)
}

type SelectTransformation struct {
	columns []string
}

func (st *SelectTransformation) Apply(df *DataFramePlan) {
	df.columns = append(df.columns, st.columns...)
}

type WithColumnTransformation struct {
	columnName string
	expression string
}

func (wc *WithColumnTransformation) Apply(df *DataFramePlan) {
	df.columns = append(df.columns, wc.columnName)
}

func NewStreamingFrame() *DataFrameReader {
	return &DataFrameReader{}
}
func (sf *DataFrameReader) FileType(filetype string) *DataFrameReader {
	sf.filetype = filetype
	return sf
}

func (sf *DataFrameReader) FilePath(filepath string) *DataFrameReader {
	sf.filepath = filepath
	return sf
}

func (sf *DataFrameReader) Option(key string, value interface{}) *DataFrameReader {
	// Set options like delimiter, header, trimLeadingSpace
	switch key {
	case "delimiter":
		sf.options.delimiter = value.(rune)
	case "trimleadingspace":
		sf.options.trimLeadingSpace = value.(bool)
	case "header":
		sf.options.header = value.(bool)
	}
	return sf
}

func (sf *DataFrameReader) Load() *DataFramePlan {
	// Create a DataFramePlan based on the StreamingFrame configuration
	plan := &DataFramePlan{
		filetype: sf.filetype,
		filepath: sf.filepath,
		options:  sf.options,
	}

	// Return the DataFramePlan, which can be modified with transformations
	return plan
}

// Select method applies a Select transformation to the DataFramePlan.
func (plan *DataFramePlan) Select(columns ...string) *DataFramePlan {
	selectTransformation := &SelectTransformation{
		columns: columns,
	}
	plan.transformations = append(plan.transformations, selectTransformation)
	return plan
}

// WithColumn method applies a WithColumn transformation to the DataFramePlan.
func (plan *DataFramePlan) WithColumn(columnName string, expression Expression) *DataFramePlan {
	withColumnTransformation := &WithColumnTransformation{
		columnName: columnName,
		expression: expression,
	}
	plan.transformations = append(plan.transformations, withColumnTransformation)
	return plan
}

func example() {
	dataframePlan := NewStreamingFrame().
		FileType("csv").
		FilePath("data/addresses.csv").
		Option("delimiter", ',').
		Option("trimleadingspace", true).
		Option("header", true).
		Load()

	dataframePlan = dataframePlan.
		Select("First Name", "Last Name").
		WithColumn("Age", Lit(30)).
		WithColumn("Age", Col("Age").Add(Lit(5)))

	dataframePlan.
		Write().
		FileType("csv").
		FilePath("output.csv").
		Execute()
}

type Col string

func (c Col) Add(other interface{}) string {
	return string(c) + " + " + other.(string)
}

// func (l Lit) Add(other Lit) Lit {

type DataFrameWriter struct {
	filetype string
	filepath string
}

func (plan *DataFramePlan) Write() *DataFrameWriter {
	return &DataFrameWriter{
		filetype: plan.filetype,
		filepath: plan.filepath,
	}
}

func (writer *DataFrameWriter) FileType(filetype string) *DataFrameWriter {
	writer.filetype = filetype
	return writer
}

func (writer *DataFrameWriter) FilePath(filepath string) *DataFrameWriter {
	writer.filepath = filepath
	return writer
}

func (writer *DataFrameWriter) Execute() {
	// Execute the write operation
}
