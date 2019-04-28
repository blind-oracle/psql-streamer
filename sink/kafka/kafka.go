package kafkasink

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blind-oracle/psql-streamer/common"
	"github.com/blind-oracle/psql-streamer/event"
	"github.com/blind-oracle/psql-streamer/sink/prom"
	kafka "github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

type eventHandler interface {
	Handle(event.Event) (kafka.Message, error)
	Stats() string
}

// Kfk is a Kafka sink
type Kfk struct {
	name string

	domain *grpc.ClientConn

	wrFallback *writer
	wr         map[string]*writer

	promTags []string

	ctx    context.Context
	cancel context.CancelFunc

	eventHandlers map[string]eventHandler

	wg sync.WaitGroup

	stats struct {
		total, errors, noTopic, skipped, kafkaMessages uint64
	}

	*common.Logger
}

type batch struct {
	w    *writer
	msgs []kafka.Message
}

type writer struct {
	topic string
	wr    *kafka.Writer
}

// New creates a new Kafka sink
func New(name string, v *viper.Viper) (s *Kfk, err error) {
	v.SetDefault("rebalanceInterval", 15*time.Second)
	v.SetDefault("timeout", 2*time.Second)
	v.SetDefault("requiredAcks", -1)
	v.SetDefault("maxAttempts", 3)
	v.SetDefault("batchSize", 100)
	v.SetDefault("batchTimeout", 5*time.Millisecond)
	v.SetDefault("async", false)

	cf := kafka.WriterConfig{
		Balancer:          &kafka.Hash{},
		Dialer:            &kafka.Dialer{Timeout: v.GetDuration("timeout")},
		Brokers:           v.GetStringSlice("hosts"),
		Topic:             v.GetString("topic"),
		BatchTimeout:      v.GetDuration("batchTimeout"),
		BatchSize:         v.GetInt("batchSize"),
		RequiredAcks:      v.GetInt("requiredAcks"),
		WriteTimeout:      v.GetDuration("timeout"),
		MaxAttempts:       v.GetInt("maxAttempts"),
		Async:             v.GetBool("async"),
		RebalanceInterval: v.GetDuration("rebalanceInterval"),
	}

	if len(cf.Brokers) == 0 {
		return nil, fmt.Errorf("Hosts should be defined")
	}

	if cf.RebalanceInterval <= 0 {
		return nil, fmt.Errorf("RebalanceInterval should be > 0")
	}

	if cf.WriteTimeout <= 0 {
		return nil, fmt.Errorf("Timeout should be > 0")
	}

	if cf.MaxAttempts <= 0 {
		return nil, fmt.Errorf("MaxAttempts should be > 0")
	}

	if cf.BatchSize <= 0 {
		return nil, fmt.Errorf("batchSize should be > 0")
	}

	if cf.BatchTimeout <= 0 {
		return nil, fmt.Errorf("BatchTimeout should be > 0")
	}

	s = &Kfk{
		name:          name,
		eventHandlers: map[string]eventHandler{},
		wr:            map[string]*writer{},
	}

	s.Logger = common.LoggerCreate(s, v)
	s.promTags = []string{s.Name(), s.Type()}

	topicFallback := v.GetString("topicFallback")
	topicMapping := v.GetStringMapString("tableTopicMapping")

	if topicFallback != "" {
		cff := cf
		cff.Topic = topicFallback
		s.wrFallback = &writer{topicFallback, kafka.NewWriter(cff)}
	} else if len(topicMapping) == 0 {
		return nil, fmt.Errorf("At least one of topicFallback or tableTopicMapping should be specified")
	}

	for tbl, topic := range topicMapping {
		if topic == topicFallback {
			return nil, fmt.Errorf("Topic '%s' should not be the same as topicFallback", topic)
		}

		cff := cf
		cff.Topic = topic
		s.wr[tbl] = &writer{topic, kafka.NewWriter(cff)}
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())

	grpcHost := v.GetString("domain")
	if grpcHost != "" {
		if s.domain, err = grpc.Dial(grpcHost, grpc.WithInsecure()); err != nil {
			return nil, fmt.Errorf("Unable to connect to Domain API: %s", err)
		}
	}

	handlers := v.GetStringSlice("handlers")
	if len(handlers) == 0 {
		return nil, fmt.Errorf("At least one handler should be defined")
	}

	for _, h := range handlers {
		if _, ok := s.eventHandlers[h]; ok {
			return nil, fmt.Errorf("Handler %s specified twice", h)
		}

		switch h {
		case "passthrough":
			s.eventHandlers[h] = newPassthroughEventHandler()

		default:
			return nil, fmt.Errorf("Unknown handler type: %s", h)
		}
	}

	return
}

// generateBatches generates a map of per-topic batches to send to Kafka
func (k *Kfk) generateBatches(events []event.Event) (batches map[string]*batch, err error) {
	batches = map[string]*batch{}

	for _, ev := range events {
		// Find a writer for the corresponding table
		wr, ok := k.wr[ev.Table]
		if ok {
			k.LogDebugEv(ev.UUID, "Found topic for table %s: %s", ev.Table, wr.topic)
			goto handle
		}

		// If there is none - check if the fallback topic was defined
		if k.wrFallback != nil {
			wr = k.wrFallback
			k.LogDebugEv(ev.UUID, "Using fallback topic: %s", wr.topic)
			goto handle
		}

		// Skip the event otherwise
		k.LogDebugEv(ev.UUID, "No topic found, skipping")
		atomic.AddUint64(&k.stats.noTopic, 1)
		continue

	handle:
		var msgs []kafka.Message
		if msgs, err = k.generateMessages(ev); err != nil {
			err = fmt.Errorf("Unable to handle event (%+v): %s", ev, err)
			return
		}

		if len(msgs) == 0 {
			k.LogDebugEv(ev.UUID, "No messages generated, skipping")
			atomic.AddUint64(&k.stats.skipped, 1)
			continue
		}

		k.LogDebugEv(ev.UUID, "Adding %d messages to a batch for topic %s", len(msgs), wr.topic)
		b, ok := batches[wr.topic]
		if ok {
			b.msgs = append(b.msgs, msgs...)
		} else {
			batches[wr.topic] = &batch{wr, msgs}
		}
	}

	return
}

// generateMessages generates Kafka messages from an event using handlers
func (k *Kfk) generateMessages(ev event.Event) (msgs []kafka.Message, err error) {
	t := time.Now()

	defer func() {
		dur := time.Since(t)
		k.LogVerboseEv(ev.UUID, "Event (%+v): %.4f", ev, dur.Seconds())
		prom.Observe(append(k.promTags, ev.Table), dur, err != nil)
	}()

	for n, h := range k.eventHandlers {
		m, err := h.Handle(ev)
		if err != nil {
			// If the error is permanent - don't report it back, just skip it and log.
			// Otherwise we might end up in an endless retry loop.
			if !common.IsErrorTemporary(err) {
				k.Errorf("Got permanent error from handler (skipping): %s", err)
				continue
			}

			return nil, fmt.Errorf("Event handler '%s' failed: %s", n, err)
		}

		// Skip empty messages
		if len(m.Value) == 0 {
			continue
		}

		// If the handler didn't set the key - make something up
		if len(m.Key) == 0 {
			m.Key = []byte(fmt.Sprintf("%s:%s:%s:%s", ev.Host, ev.Database, ev.Table, ev.Action))
		}

		msgs = append(msgs, m)
	}

	return
}

// ProcessEventsBatch processess the events batch
func (k *Kfk) ProcessEventsBatch(events []event.Event) (err error) {
	atomic.AddUint64(&k.stats.total, uint64(len(events)))

	batches, err := k.generateBatches(events)
	if err != nil {
		return fmt.Errorf("Unable to generate batches: %s", err)
	}

	if len(batches) == 0 {
		return
	}

	k.Debugf("Writing %d batches", len(batches))
	msgCount := 0
	start := time.Now()
	for _, b := range batches {
		t := time.Now()
		msgCount += len(b.msgs)
		k.Debugf("Writing batch of %d messages to topic '%s'", len(b.msgs), b.w.topic)

		if err = b.w.wr.WriteMessages(context.Background(), b.msgs...); err != nil {
			return fmt.Errorf("Unable to write a batch to topic '%s': %s", b.w.topic, err)
		}

		k.Debugf("Batch written successfully in %.4f sec", time.Since(t).Seconds())
	}
	k.Debugf("%d batches (%d messages) successfully written in %.4f sec", len(batches), msgCount, time.Since(start).Seconds())
	atomic.AddUint64(&k.stats.kafkaMessages, uint64(msgCount))

	return
}

// Name returns Sink's name
func (k *Kfk) Name() string {
	return k.name
}

// Type returns Sink's type
func (k *Kfk) Type() string {
	return "Sink-Kafka"
}

// SetLogger sets a logger
func (k *Kfk) SetLogger(l *common.Logger) {
	k.Logger = l
}

// Stats returns statistics
func (k *Kfk) Stats() string {
	t := fmt.Sprintf("total: %d, no topic: %d, skipped: %d, errors: %d, kafka msgs sent: %d, handlers:",
		atomic.LoadUint64(&k.stats.total),
		atomic.LoadUint64(&k.stats.noTopic),
		atomic.LoadUint64(&k.stats.skipped),
		atomic.LoadUint64(&k.stats.errors),
		atomic.LoadUint64(&k.stats.kafkaMessages),
	)

	for k, v := range k.eventHandlers {
		t += fmt.Sprintf(" [%s: %s]", k, v.Stats())
	}

	return t
}

// Status returns the current healthcheck state
// Currently No-op
func (k *Kfk) Status() error {
	return nil
}

// Close shuts down sink
func (k *Kfk) Close() (err error) {
	k.cancel()
	k.wg.Wait()

	var errs []string
	for tbl, w := range k.wr {
		if err = w.wr.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("[%s: %s]", tbl, err))
		}
	}

	if k.wrFallback != nil {
		if err = k.wrFallback.wr.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("[fallback: %s]", err))
		}
	}

	if len(errs) > 0 {
		err = errors.New("Errors while closing writers: " + strings.Join(errs, " "))
	}

	return
}
