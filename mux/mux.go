package mux

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/blind-oracle/psql-streamer/common"
	"github.com/blind-oracle/psql-streamer/event"
	"github.com/spf13/viper"
)

// Subset of Sink's methods used by Mux
// Minified to make testing easier
type muxSink interface {
	ProcessEventsBatch([]event.Event) error
	Name() string
}

// Mux is an event dispatcher with buffer
type Mux struct {
	cfg Config

	count  int
	buffer []event.Event

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	sinks map[string]muxSink

	logger *common.Logger

	sync.Mutex
}

// Config is a Mux config
type Config struct {
	Callback     func()
	ErrorCounter *uint64
	Logger       *common.Logger

	batchSize          int
	batchFlushInterval time.Duration
	sendRetryInterval  time.Duration
}

// New creates a Mux with config
func New(ctx context.Context, v *viper.Viper, c Config) (*Mux, error) {
	// If no Viper provided - just go with defaults
	if v == nil {
		v = viper.New()
	}

	v.SetDefault("batchSize", 100)
	v.SetDefault("batchFlushInterval", time.Second)
	v.SetDefault("sendRetryInterval", 100*time.Millisecond)

	if c.batchSize = v.GetInt("batchSize"); c.batchSize <= 0 {
		return nil, fmt.Errorf("batchSize should be > 0")
	}

	if c.batchFlushInterval = v.GetDuration("batchFlushInterval"); c.batchFlushInterval <= 0 {
		return nil, fmt.Errorf("batchFlushInterval should be > 0")
	}

	if c.sendRetryInterval = v.GetDuration("sendRetryInterval"); c.sendRetryInterval <= 0 {
		return nil, fmt.Errorf("sendRetryInterval should be > 0")
	}

	if c.Logger == nil {
		return nil, fmt.Errorf("Logger should be provided")
	}

	m := &Mux{
		cfg:    c,
		logger: c.Logger,
		sinks:  map[string]muxSink{},
		buffer: make([]event.Event, c.batchSize),
	}

	m.ctx, m.cancel = context.WithCancel(ctx)
	return m, nil
}

// Start a Mux
func (m *Mux) Start() {
	m.wg.Add(1)
	go m.dispatch()
	m.logf("Mux: started (batchSize=%d flushInterval=%s)", m.cfg.batchSize, m.cfg.batchFlushInterval)
}

// Push the event into queue
func (m *Mux) Push(ev event.Event, f func()) {
	m.Lock()
	defer m.Unlock()

	// Execute provided func inside a lock (if any)
	if f != nil {
		f()
	}

	m.buffer[m.count] = ev
	m.count++

	// Flush the buffer if it's already full
	if m.count >= m.cfg.batchSize {
		m.debugf("Mux: Buffer is full (%d/%d), flushing", m.count, m.cfg.batchSize)
		m.flushQueue()
		return
	}

	m.debugf("Mux: Buffer is now %d/%d", m.count, m.cfg.batchSize)
}

// Subscribe adds a sink to the Mux
func (m *Mux) Subscribe(s muxSink) {
	n := s.Name()
	if _, ok := m.sinks[n]; ok {
		panic("Mux: already subscribed to " + n)
	}

	m.sinks[n] = s
}

// Close the mux
func (m *Mux) Close() {
	m.debugf("Mux: closing...")
	m.cancel()
	m.wg.Wait()
	m.logf("Mux: closed")
}

func (m *Mux) dispatch() {
	defer m.wg.Done()

	t := time.NewTicker(m.cfg.batchFlushInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			m.TryFlushQueue()

		// Flush the buffer on exit
		case <-m.ctx.Done():
			m.TryFlushQueue()
			return
		}
	}
}

// TryFlushQueue flush the queue if there's something in it
func (m *Mux) TryFlushQueue() {
	m.Lock()
	defer m.Unlock()

	if m.count == 0 {
		return
	}

	m.flushQueue()
}

// flush the queue to sinks
// assumes locked mutex
func (m *Mux) flushQueue() {
	t := time.Now()

	m.debugf("Mux: Flushing queue (%d events)", m.count)
	for _, snk := range m.sinks {
		err := common.RetryForever(m.ctx, common.RetryParams{
			F:            func() error { return snk.ProcessEventsBatch(m.buffer[:m.count]) },
			Interval:     m.cfg.sendRetryInterval,
			Logger:       m.logger,
			ErrorMsg:     fmt.Sprintf("Mux: Unable to send batch (%d events) to sink '%s')", m.count, snk.Name()) + " (retry %d): %s",
			ErrorCounter: m.cfg.ErrorCounter,
		})

		if err == context.Canceled {
			m.logf("Mux: Context canceled, exiting")
			break
		} else if err != nil {
			// Continue with other sinks if we've got a permanent error with this one
			continue
		}
	}

	m.debugf("Mux: Queue flushed in %.4f sec, running callback (if any)", time.Since(t).Seconds())

	// Run callback if configured
	if m.cfg.Callback != nil {
		t1 := time.Now()
		m.cfg.Callback()
		m.debugf("Mux: Callback is done in %.4f sec", time.Since(t1).Seconds())
	}

	// Reset the buffer
	m.count = 0
	m.debugf("Mux: Done in %.4f sec", time.Since(t).Seconds())
}

// BufferSize returns the size of the buffer
func (m *Mux) BufferSize() int {
	return m.cfg.batchSize
}

func (m *Mux) logf(msg string, f ...interface{}) {
	m.logger.Logf(msg, f...)
}

func (m *Mux) debugf(msg string, f ...interface{}) {
	m.logger.Debugf(msg, f...)
}
