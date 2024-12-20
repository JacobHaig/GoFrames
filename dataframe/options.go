package dataframe

import "github.com/rotisserie/eris"

type Options struct {
	delimiter        rune
	trimleadingspace bool
	header           bool
	inferdatatypes   bool
}

func (options *Options) standardizeOptions() (*Options, error) {
	// Report any errors to Prevent incompatible options
	if options.trimleadingspace && (options.delimiter == ' ' || options.delimiter == '\t') {
		return nil, eris.New("error: trimleadingspace is true, but the delimiter is a space or tab. these are incompatible options")
	}

	return options, nil
}
