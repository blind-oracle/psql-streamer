package common

import (
	"fmt"
	"log"
	"sync/atomic"

	"github.com/spf13/viper"
)

// Source is an interface implementing a source for logs
type Source interface {
	Name() string
	Type() string
}

// Logger is embedded by sinks & sources structs
type Logger struct {
	debug, verbose int32
	header         string
}

// Logf logs!
func (l *Logger) Logf(m string, f ...interface{}) {
	log.Printf(l.header+m, f...)
}

// Errorf logs error (generic currently)
func (l *Logger) Errorf(m string, f ...interface{}) {
	log.Printf(l.header+m, f...)
}

// Debugf logs if debug
func (l *Logger) Debugf(m string, f ...interface{}) {
	if atomic.LoadInt32(&l.debug) == 1 {
		l.Logf(m, f...)
	}
}

// LogEv logs event
func (l *Logger) LogEv(uuid string, m string, f ...interface{}) {
	l.Logf("["+uuid+"]: "+m, f...)
}

// LogDebugEv logs event if debug
func (l *Logger) LogDebugEv(uuid string, m string, f ...interface{}) {
	l.Debugf("["+uuid+"]: "+m, f...)
}

// LogVerboseEv logs event if verbose
func (l *Logger) LogVerboseEv(uuid string, m string, f ...interface{}) {
	if atomic.LoadInt32(&l.verbose) == 1 || atomic.LoadInt32(&l.debug) == 1 {
		l.LogEv(uuid, m, f...)
	}
}

// SetDebug toggles the debug mode
func (l *Logger) SetDebug(d bool) {
	var v int32
	if d {
		v = 1
	}
	atomic.StoreInt32(&l.debug, v)
}

// SetVerbose toggles the verbose mode
func (l *Logger) SetVerbose(d bool) {
	var v int32
	if d {
		v = 1
	}
	atomic.StoreInt32(&l.verbose, v)
}

// LoggerCreate creates a logger
func LoggerCreate(l Source, v *viper.Viper) *Logger {
	lg := &Logger{}

	if l != nil {
		lg.header = LoggerHeader(l)
	}

	if v != nil {
		lg.SetDebug(v.GetBool("debug"))
		lg.SetVerbose(v.GetBool("verbose"))
	}

	return lg
}

// LoggerHeader returns a log header for a particular source
func LoggerHeader(l Source) string {
	return fmt.Sprintf("%s (%s): ", l.Type(), l.Name())
}
