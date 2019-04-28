package common

// Common is a common set of functions embedded by sinks & sources
type Common interface {
	Name() string
	Type() string

	Stats() string
	Status() error
	Close() error

	SetDebug(bool)
	SetVerbose(bool)
}
