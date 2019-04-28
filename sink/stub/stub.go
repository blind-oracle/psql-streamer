// Package stub is used for testing
package stub

import (
	"fmt"
	"sync/atomic"

	"github.com/blind-oracle/psql-streamer/common"
	"github.com/blind-oracle/psql-streamer/event"
	"github.com/spf13/viper"
)

type evf func([]event.Event) error

// Stub is a stub sink
type Stub struct {
	name   string
	f      evf
	events uint64
	*common.Logger
}

// New creates a new Stub sink
func New(name string, v *viper.Viper, f evf) (*Stub, error) {
	return &Stub{name: name, f: f}, nil
}

// ProcessEventsBatch processess the events batch
func (k *Stub) ProcessEventsBatch(events []event.Event) (err error) {
	atomic.AddUint64(&k.events, uint64(len(events)))

	if k.f != nil {
		return k.f(events)
	} else {
		return nil
	}
}

// Name returns Sink's name
func (k *Stub) Name() string { return k.name }

// Type returns Sink's type
func (k *Stub) Type() string { return "Sink-Stub" }

// SetLogger sets a logger
func (k *Stub) SetLogger(l *common.Logger) {}

// Stats returns statistics
func (k *Stub) Stats() string { return fmt.Sprintf("events: %d", atomic.LoadUint64(&k.events)) }

// Status returns the current healthcheck state
func (k *Stub) Status() error { return nil }

// Close shuts down sink
func (k *Stub) Close() error { return nil }
