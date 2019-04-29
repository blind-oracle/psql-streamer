package postgres

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blind-oracle/psql-streamer/mux"

	"github.com/spf13/viper"

	"github.com/blind-oracle/pgoutput"
	"github.com/blind-oracle/psql-streamer/common"
	"github.com/blind-oracle/psql-streamer/db"
	"github.com/blind-oracle/psql-streamer/event"
	"github.com/blind-oracle/psql-streamer/sink"
	"github.com/blind-oracle/psql-streamer/source/prom"
	"github.com/jackc/pgx"
	"github.com/satori/go.uuid"
)

// PSQL is a PostgreSQL source
type PSQL struct {
	name string

	cfg psqlConfig

	conn        *pgx.ReplicationConn
	connConfig  pgx.ConnConfig
	relationSet *pgoutput.RelationSet
	sub         *pgoutput.Subscription
	mux         *mux.Mux

	boltDB     *db.DB
	boltBucket string

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	walPosition          uint64
	walPositionPersisted uint64

	promTags []string

	sinks map[string]sink.Sink

	err error

	stats struct {
		events, replicationErrors, persistErrors, eventErrors uint64
	}

	*common.Logger
	sync.RWMutex
}

type psqlConfig struct {
	dsn             string
	publication     string
	replicationSlot string

	timeout time.Duration

	startRetryInterval time.Duration

	walPositionOverride uint64
	walRetain           uint64
}

// New creates a new PostgreSQL source
func New(name string, v *viper.Viper) (s *PSQL, err error) {
	v.SetDefault("startRetryInterval", 5*time.Second)
	v.SetDefault("timeout", 2*time.Second)

	cf := psqlConfig{}

	if cf.dsn = v.GetString("dsn"); cf.dsn == "" {
		return nil, fmt.Errorf("dsn should be defined")
	}

	if cf.publication = v.GetString("publication"); cf.publication == "" {
		return nil, fmt.Errorf("publication should be defined")
	}

	if cf.replicationSlot = v.GetString("replicationSlot"); cf.replicationSlot == "" {
		return nil, fmt.Errorf("replicationSlot should be defined")
	}

	if cf.startRetryInterval = v.GetDuration("startRetryInterval"); cf.startRetryInterval <= 0 {
		return nil, fmt.Errorf("startRetryInterval should be > 0")
	}

	if cf.timeout = v.GetDuration("timeout"); cf.timeout <= 0 {
		return nil, fmt.Errorf("timeout should be > 0")
	}

	if v.GetInt64("walPositionOverride") < 0 {
		return nil, fmt.Errorf("walPositionOverride should be >= 0")
	}
	cf.walPositionOverride = uint64(v.GetInt64("walPositionOverride"))

	if v.GetInt64("walRetain") < 0 {
		return nil, fmt.Errorf("walRetain should be >= 0")
	}
	cf.walRetain = uint64(v.GetInt64("walRetain"))

	s = &PSQL{
		name:        name,
		boltBucket:  "source_" + name,
		relationSet: pgoutput.NewRelationSet(nil),
		sinks:       map[string]sink.Sink{},
		cfg:         cf,
	}

	s.Logger = common.LoggerCreate(s, v)

	if s.boltDB, err = db.GetHandleFromViper(v); err != nil {
		return nil, err
	}

	s.promTags = []string{s.Name(), s.Type()}

	if err = s.boltDB.BucketInit(s.boltBucket); err != nil {
		return nil, fmt.Errorf("Unable to init Bolt bucket: %s", err)
	}

	// Check if we have a WAL position overridden from the config file
	if s.cfg.walPositionOverride == 0 {
		// Read WAL position if it's there
		if s.cfg.walPositionOverride, err = s.boltDB.CounterGet(s.boltBucket, db.CounterWALPos); err != nil {
			return nil, fmt.Errorf("Unable to read bolt counter: %s", err)
		}
	}

	s.walPosition = s.cfg.walPositionOverride
	s.walPositionPersisted = s.walPosition

	s.ctx, s.cancel = context.WithCancel(context.Background())

	if s.connConfig, err = pgx.ParseDSN(s.cfg.dsn); err != nil {
		return nil, fmt.Errorf("Unable to parse DSN: %s", err)
	}

	// Use custom dialer to be able to cancel the dial process with context
	s.connConfig.Dial = func(n, a string) (c net.Conn, err error) {
		dialer := &net.Dialer{
			Timeout:   s.cfg.timeout,
			KeepAlive: 30 * time.Second,
			DualStack: false,
		}

		return dialer.DialContext(s.ctx, n, a)
	}

	if err = s.setup(); err != nil {
		return nil, err
	}

	s.mux, err = mux.New(s.ctx, v,
		mux.Config{
			Callback:     s.persistWAL,
			ErrorCounter: &s.stats.eventErrors,
			Logger:       s.Logger,
		},
	)

	if err != nil {
		return nil, fmt.Errorf("Unable to create Mux: %s", err)
	}

	return
}

// Start instructs source to begin streaming the events
func (s *PSQL) Start() {
	s.mux.Start()
	s.wg.Add(1)
	go s.fetch()
}

// Close stops the replication and exits
func (s *PSQL) Close() error {
	s.cancel()
	s.Logf("Closing...")
	s.wg.Wait()

	s.mux.Close()

	if s.conn != nil {
		return s.conn.Close()
	}

	return nil
}

func (s *PSQL) setup() (err error) {
	// Close the connection if it was already set up
	if s.conn != nil {
		if err = s.conn.Close(); err != nil {
			s.Errorf("Unable to close connection: %s", err)
		}
	}

	if s.conn, err = pgx.ReplicationConnect(s.connConfig); err != nil {
		return fmt.Errorf("Unable to open replication connection: %s", err)
	}

	s.sub = pgoutput.NewSubscription(s.conn, s.cfg.replicationSlot, s.cfg.publication, s.cfg.walRetain, false)

	if err = s.sub.CreateSlot(); err != nil {
		return fmt.Errorf("Unable to create replication slot: %s", err)
	}

	return
}

// Name returns source name
func (s *PSQL) Name() string {
	return s.name
}

// Type returns source type
func (s *PSQL) Type() string {
	return "Source-PSQL"
}

// Subscribe registers a function to be called on event arrival
func (s *PSQL) Subscribe(sub sink.Sink) {
	s.mux.Subscribe(sub)
}

// SetLogger sets a logger
func (s *PSQL) SetLogger(l *common.Logger) {
	s.Logger = l
}

// Main execution loop
func (s *PSQL) fetch() {
	var err error

	defer s.wg.Done()

	first := true
	for {
		walPos := atomic.LoadUint64(&s.walPositionPersisted)

		select {
		// Return if the context is canceled
		// This should be caught by (err == nil) below, but just in case
		case <-s.ctx.Done():
			return

		// Try to start replication indefinitely
		default:
			if first {
				first = false
				goto start
			}

			// If it's not the first iteration - try to recreate the connection from scratch
			if err = s.setup(); err != nil {
				if err == context.Canceled {
					return
				}

				goto oops
			}

		start:
			s.Logf("Starting replication at WAL position %d", walPos)
			s.setError(nil)
			// err will be nil only in case of context cancellation (correct shutdown)
			if err = s.sub.Start(s.ctx, walPos, s.process); err == nil {
				return
			}

		oops:
			atomic.AddUint64(&s.stats.replicationErrors, 1)
			s.setError(err)
			walPos = atomic.LoadUint64(&s.walPositionPersisted)
			// Send the error to listeners
			s.Errorf("Replication error (walPositionPersisted: %d): %s", walPos, err)

			// Wait to retry
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(s.cfg.startRetryInterval):
			}
		}
	}
}

func (s *PSQL) process(m pgoutput.Message, walPos uint64) (err error) {
	// These come with walPos == 0
	switch v := m.(type) {
	// Relation is the metadata of the table - we cache it locally and then look up on the receipt of the row
	// Potential unbounded map growth, but in practice shouldn't happen as the table count is limited
	case pgoutput.Relation:
		s.relationSet.Add(v)
		return
	// This is not used now
	case pgoutput.Type:
		return
	}

	// Check just in case
	if walPos == 0 {
		return
	}

	atomic.StoreUint64(&s.walPosition, walPos)

	var ev event.Event
	t := time.Now()
	switch v := m.(type) {
	case pgoutput.Insert:
		ev, err = s.generateEvent(event.ActionInsert, v.RelationID, v.Row)
	case pgoutput.Update:
		ev, err = s.generateEvent(event.ActionUpdate, v.RelationID, v.Row)
	case pgoutput.Delete:
		ev, err = s.generateEvent(event.ActionDelete, v.RelationID, v.Row)
	default:
		// Ignore all other events for now (Begin, Commit etc)
		// Their WAL position is equal to an actual INSERT/UPDATE/DELETE event, so don't persist it
		return
	}

	promTags := append(s.promTags, ev.Table)

	// Report the error if any
	if err != nil {
		atomic.AddUint64(&s.stats.eventErrors, 1)
		s.Errorf("Error generating event: %s", err)
		return
	}

	defer func() {
		dur := time.Since(t)
		prom.Observe(promTags, dur)
		atomic.AddUint64(&s.stats.events, 1)
		s.LogVerboseEv(ev.UUID, "Event (%+v): %.4f sec", ev, dur.Seconds())
	}()

	ev.UUID = uuid.Must(uuid.NewV4()).String()
	ev.WALPosition = walPos

	s.LogDebugEv(ev.UUID, "Got message '%#v' (wal %d)", m, walPos)
	s.mux.Push(ev, nil)
	return
}

// Persist the WAL log position to Bolt
func (s *PSQL) persistWAL() {
	// For testing purposes moslty
	if s.boltDB == nil {
		return
	}

	walPos := atomic.LoadUint64(&s.walPosition)

	// Retry forever in case of some Bolt errors (out of disk space?)
	_ = common.RetryForever(s.ctx, common.RetryParams{
		F:            func() error { return s.boltDB.CounterSet(s.boltBucket, db.CounterWALPos, walPos) },
		Interval:     time.Second,
		Logger:       s.Logger,
		ErrorMsg:     "Unable to flush WAL position to Bolt (retry %d): %s",
		ErrorCounter: &s.stats.persistErrors,
	})

	atomic.StoreUint64(&s.walPositionPersisted, walPos)
	s.Debugf("WAL persisted at position %d", walPos)
}

func (s *PSQL) generateEvent(action string, relationID uint32, row []pgoutput.Tuple) (ev event.Event, err error) {
	rel, ok := s.relationSet.Get(relationID)
	if !ok {
		err = fmt.Errorf("Relation with ID '%d' not found in relationSet", relationID)
		return
	}

	ev = event.Event{
		Host:      s.connConfig.Host,
		Database:  s.connConfig.Database,
		Table:     rel.Name,
		Action:    action,
		Timestamp: time.Now(),
		Columns:   map[string]interface{}{},
	}

	if ev.Host == "" {
		ev.Host = "unknown"
	}

	vals, err := s.relationSet.Values(relationID, row)
	if err != nil {
		err = fmt.Errorf("Unable to decode values: %s", err)
		return
	}

	for n, v := range vals {
		// Process only some basic column types
		switch val := v.Get().(type) {
		case string, nil, time.Time, bool,
			uint, uint8, uint16, uint32, uint64,
			int, int8, int16, int32, int64,
			float32, float64:
			ev.Columns[n] = val

		case []byte:
			// It might contain unprintable chars, but JSON should handle them in hex notaion
			ev.Columns[n] = string(val)

		case *net.IPNet:
			ev.Columns[n] = val.String()

		default:
			// Just skip unsupported columns for now, while notifying the user
			s.Errorf("Table %s column %s: unsupported type (%T)", rel.Name, n, val)
		}
	}

	return
}

// Stats returns source's statistics
func (s *PSQL) Stats() string {
	return fmt.Sprintf("events: %d, eventErrors: %d, replicaErrors: %d, persistErrors: %d, walPos: %d, walPosPersist: %d",
		atomic.LoadUint64(&s.stats.events),
		atomic.LoadUint64(&s.stats.eventErrors),
		atomic.LoadUint64(&s.stats.replicationErrors),
		atomic.LoadUint64(&s.stats.persistErrors),
		atomic.LoadUint64(&s.walPosition),
		atomic.LoadUint64(&s.walPositionPersisted),
	)
}

// Status returns the status for the source
func (s *PSQL) Status() error {
	s.RLock()
	defer s.RUnlock()
	return s.err
}

// Flush reports to PostgreSQL that all events are consumed and applied
// This allows it to remove logs.
func (s *PSQL) Flush() (err error) {
	s.Lock()
	defer s.Unlock()

	if s.err != nil {
		return fmt.Errorf("Source is not in a stable state")
	}

	if err = s.sub.Flush(); err != nil {
		s.Errorf("Unable to Flush() subscription: %s", err)
	} else {
		s.Logf("Flushed: %s", s.Stats())
	}

	return
}

func (s *PSQL) setError(err error) {
	s.Lock()
	s.err = err
	s.Unlock()
}
