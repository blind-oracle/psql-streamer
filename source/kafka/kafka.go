package kafkasrc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blind-oracle/psql-streamer/common"
	"github.com/blind-oracle/psql-streamer/event"
	"github.com/blind-oracle/psql-streamer/mux"
	"github.com/blind-oracle/psql-streamer/sink"
	kafka "github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
)

const (
	healthCheckErrorThreshold = 30
)

// Kfk is a Kafka source
type Kfk struct {
	name string

	ctx    context.Context
	cancel context.CancelFunc

	topics    []string
	sinkNames []string

	readers map[string]*reader

	wg sync.WaitGroup

	stats struct {
		events, errors, fetchErrors uint64
	}

	err error

	*common.Logger
	sync.RWMutex
}

type reader struct {
	topic string

	rdr *kafka.Reader
	mux *mux.Mux

	msgBuf   []kafka.Message
	msgCount int
}

func (r *reader) push(ev event.Event, m kafka.Message) {
	r.mux.Push(ev, func() {
		r.msgBuf[r.msgCount] = m
		r.msgCount++
	})
}

// New returns an instance of Kafka source based on the provided viper subtreee
func New(name string, v *viper.Viper) (k *Kfk, err error) {
	k = &Kfk{
		name:    name,
		readers: map[string]*reader{},
	}

	k.ctx, k.cancel = context.WithCancel(context.Background())
	k.Logger = common.LoggerCreate(k, v)

	rdrCfg := kafka.ReaderConfig{}
	if rdrCfg.Brokers = v.GetStringSlice("hosts"); len(rdrCfg.Brokers) == 0 {
		return nil, errors.New("You need to specify at least one Kafka host")
	}

	if rdrCfg.GroupID = v.GetString("groupID"); rdrCfg.GroupID == "" {
		return nil, errors.New("You need to specify groupID")
	}

	k.topics = v.GetStringSlice("topics")
	if len(k.topics) == 0 {
		return nil, errors.New("You need to specify at least one topic")
	}

	for _, t := range k.topics {
		if _, ok := k.readers[t]; ok {
			return nil, fmt.Errorf("Topic '%s' is specified more than once", t)
		}

		cfg := rdrCfg
		cfg.Topic = t

		r := &reader{
			topic: t,
		}

		// commit is executed inside a mux's mutex, so no lock needed here
		commit := func() {
			// Commit all messages from the buffer to Kafka
			k.commit(r, r.msgBuf[:r.msgCount]...)
			r.msgCount = 0
		}

		r.mux, err = mux.New(k.ctx, v,
			mux.Config{
				Callback:     commit,
				ErrorCounter: &k.stats.errors,
				Logger:       k.Logger,
			},
		)

		if err != nil {
			return nil, fmt.Errorf("Unable to create Mux: %s", err)
		}

		// Align Kafka library's queue size with Mux's
		cfg.QueueCapacity = r.mux.BufferSize()

		// Create reader and init buffers
		r.rdr = kafka.NewReader(cfg)
		r.msgBuf = make([]kafka.Message, cfg.QueueCapacity)

		k.readers[t] = r
	}

	return
}

// Start starts the worker goroutines
func (k *Kfk) Start() {
	for _, r := range k.readers {
		r.mux.Start()
		k.wg.Add(1)
		go k.fetch(r)
	}
}

// fetch gets messages from Kafka, decodes into events and pushes them into queue
func (k *Kfk) fetch(r *reader) {
	defer k.wg.Done()

	var (
		fetchErrors int
		err         error
	)

	k.Logf("Fetching from topic %s", r.topic)
	for {
		var m kafka.Message
		// EOF is issued on reader Close()
		if m, err = r.rdr.FetchMessage(k.ctx); err == context.Canceled || err == io.EOF {
			return
		}

		if err != nil {
			atomic.AddUint64(&k.stats.fetchErrors, 1)
			k.Errorf("Error fetching message: %s", err)

			// Increment the error counter and raise error if over threshold
			if fetchErrors++; fetchErrors > healthCheckErrorThreshold {
				k.setError(err)
			}

			time.Sleep(time.Second)
			continue
		}

		// As soon as we got a message we assume the reader is Ok and clear the error
		fetchErrors = 0
		k.setError(nil)

		atomic.AddUint64(&k.stats.events, 1)
		var ev event.Event
		if err = json.Unmarshal(m.Value, &ev); err != nil {
			atomic.AddUint64(&k.stats.errors, 1)
			k.Errorf("Unable to unmarshal event: %s", err)
			k.commit(r, m)
			continue
		}

		if ev.UUID == "" {
			atomic.AddUint64(&k.stats.errors, 1)
			k.Errorf("Malformed event: no UUID (%+v)", ev)
			k.commit(r, m)
			continue
		}

		k.LogVerboseEv(ev.UUID, "Event [partition %d offset %d]: %s", m.Partition, m.Offset, string(m.Value))
		r.push(ev, m)
	}
}

func (k *Kfk) commit(r *reader, m ...kafka.Message) {
	k.Debugf("Commiting %d messages to topic %s", len(m), r.topic)

	t := time.Now()
	// Try to commit indefinitely until success
	err := common.RetryForever(k.ctx, common.RetryParams{
		F:            func() error { return r.rdr.CommitMessages(context.Background(), m...) },
		Interval:     500 * time.Millisecond,
		Logger:       k.Logger,
		ErrorMsg:     "Unable to commit messages to Kafka (topic '" + r.topic + "') (retry %d): %s",
		ErrorCounter: &k.stats.errors,
	})

	if err == context.Canceled {
		return
	}

	if err != nil {
		k.Errorf("Unable to commit messages: %s", err)
	}

	k.Debugf("%d messages commited to Kafka in %.4f sec", len(m), time.Since(t).Seconds())
}

func (k *Kfk) setError(err error) {
	k.Lock()
	k.err = err
	k.Unlock()
}

// Status returns source's status
func (k *Kfk) Status() error {
	k.RLock()
	defer k.RUnlock()
	return k.err
}

// Subscribe a sink to events from this source
func (k *Kfk) Subscribe(s sink.Sink) {
	for _, r := range k.readers {
		k.sinkNames = append(k.sinkNames, s.Name())
		r.mux.Subscribe(s)
	}
}

// Name returns Source's name
func (k *Kfk) Name() string {
	return k.name
}

// Type returns Source's type
func (k *Kfk) Type() string {
	return "Source-Kafka"
}

// SetLogger sets a logger
func (k *Kfk) SetLogger(l *common.Logger) {
	k.Logger = l
}

// Stats returns statistics
func (k *Kfk) Stats() string {
	return fmt.Sprintf("events: %d errors: %d fetchErrors: %d sinks: %s, topics: %s",
		atomic.LoadUint64(&k.stats.events),
		atomic.LoadUint64(&k.stats.errors),
		atomic.LoadUint64(&k.stats.fetchErrors),
		strings.Join(k.sinkNames, ", "),
		strings.Join(k.topics, ", "),
	)
}

// Flush forces queue flush
func (k *Kfk) Flush() (err error) {
	for _, r := range k.readers {
		r.mux.TryFlushQueue()
	}

	return
}

// Close shuts down Kafka source
func (k *Kfk) Close() (err error) {
	k.cancel()
	// Wait for all goroutines to exit (buffers flushed etc)
	k.wg.Wait()

	k.Logf("Closing Kafka readers...")

	var errs []string
	for topic, r := range k.readers {
		if err = r.rdr.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("[%s: %s]", topic, err))
		}

		// Close the mux flushing anything that it has in buffers
		r.mux.Close()
	}

	if len(errs) > 0 {
		err = errors.New("Errors while closing readers: " + strings.Join(errs, " "))
	}

	return
}
