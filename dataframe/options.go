package dataframe

import "github.com/rotisserie/eris"

type Options struct {
	delimiter        rune
	trimleadingspace bool
	header           bool
	inferdatatypes   bool
}

func NewOptions() *Options {
	return &Options{
		delimiter:        ',',
		trimleadingspace: false,
		header:           false,
		inferdatatypes:   false,
	}
}

func (options *Options) standardizeOptions() (*Options, error) {
	// Report any errors to Prevent incompatible options
	if options.trimleadingspace && (options.delimiter == ' ' || options.delimiter == '\t') {
		return nil, eris.New("error: trimleadingspace is true, but the delimiter is a space or tab. these are incompatible options")
	}

	return options, nil
}

func (options *Options) SetDelimiter(delimiter rune) *Options {
	options.delimiter = delimiter
	return options
}

func (options *Options) SetTrimLeadingSpace(trimLeadingSpace bool) *Options {
	options.trimleadingspace = trimLeadingSpace
	return options
}

func (options *Options) SetHeader(header bool) *Options {
	options.header = header
	return options
}

func (options *Options) SetInferDataTypes(inferDataTypes bool) *Options {
	options.inferdatatypes = inferDataTypes
	return options
}

func (options *Options) GetDelimiter() rune {
	return options.delimiter
}

func (options *Options) GetTrimLeadingSpace() bool {
	return options.trimleadingspace
}

func (options *Options) GetHeader() bool {
	return options.header
}

func (options *Options) GetInferDataTypes() bool {
	return options.inferdatatypes
}
