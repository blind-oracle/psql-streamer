package mux

import (
	"context"
	"log"
	"runtime"
	"testing"
	"time"

	"github.com/blind-oracle/psql-streamer/common"
	"github.com/blind-oracle/psql-streamer/event"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

type sink struct {
	name string
	f    func([]event.Event) error
}

func (s *sink) Name() string {
	return s.name
}

func (s *sink) Type() string {
	return ""
}

func (s *sink) ProcessEventsBatch(ev []event.Event) error {
	return s.f(ev)
}

func Test_New(t *testing.T) {
	ctx := context.Background()

	v := viper.New()
	v.Set("batchSize", -1)
	s := &sink{}
	_, err := New(ctx, v, Config{
		Logger: common.LoggerCreate(s, v),
	})
	assert.NotNil(t, err)

	v = viper.New()
	v.Set("batchFlushInterval", -1)
	_, err = New(ctx, v, Config{
		Logger: common.LoggerCreate(s, v),
	})
	assert.NotNil(t, err)

	v = viper.New()
	v.Set("sendRetryInterval", -1)
	_, err = New(ctx, v, Config{
		Logger: common.LoggerCreate(s, v),
	})
	assert.NotNil(t, err)

	v = viper.New()
	_, err = New(ctx, v, Config{})
	assert.NotNil(t, err)

	mux, err := New(ctx, nil, Config{
		Logger: common.LoggerCreate(s, v),
	})
	assert.Nil(t, err)
	assert.Equal(t, mux.cfg.batchSize, 100)
	assert.Equal(t, mux.cfg.batchFlushInterval, time.Second)
	assert.Equal(t, mux.cfg.sendRetryInterval, 100*time.Millisecond)
}

func Test_Mux(t *testing.T) {
	ctx := context.Background()
	gCount := runtime.NumGoroutine()

	v := viper.New()
	v.Set("batchSize", 10)
	v.Set("batchFlushInterval", 50*time.Millisecond)

	ev := event.Event{UUID: "lalala"}
	s := &sink{}

	mux, err := New(ctx, v, Config{
		Logger: common.LoggerCreate(s, v),
	})
	assert.Nil(t, err)
	assert.EqualValues(t, 10, mux.BufferSize())

	mux.Subscribe(s)
	assert.Panics(t, func() { mux.Subscribe(s) })

	canary := 0
	s.f = func(ev []event.Event) error {
		canary++
		assert.Len(t, ev, 10)
		return nil
	}

	// Push 10 events
	for i := 0; i < 10; i++ {
		mux.Push(ev, nil)
	}

	// Function ran and buffer should be empty after
	assert.Equal(t, 1, canary)
	assert.Equal(t, 0, mux.count)

	// Test flush by timer
	s.f = func(ev []event.Event) error {
		canary++
		assert.Len(t, ev, 5)
		return nil
	}

	mux.cfg.Callback = func() { log.Printf("callback %d", canary); canary++ }
	testFunc := func() { log.Printf("push %d", canary); canary++ }
	// Push 5 events
	for i := 0; i < 5; i++ {
		mux.Push(ev, testFunc)
	}

	// Start dispatcher
	mux.Start()

	time.Sleep(110 * time.Millisecond)

	// Functions ran and buffer should be empty after
	assert.Equal(t, 8, canary)
	assert.Equal(t, 0, mux.count)

	// Push 5 events
	for i := 0; i < 5; i++ {
		mux.Push(ev, testFunc)
	}

	mux.Close()
	// Check goroutines are closed
	assert.Equal(t, gCount, runtime.NumGoroutine())

	// Functions ran and buffer should be empty after Close
	assert.Equal(t, 15, canary)
	assert.Equal(t, 0, mux.count)
}

func Test_Mux_Cancel(t *testing.T) {
	v := viper.New()
	v.Set("batchSize", 10)
	v.Set("batchFlushInterval", 50*time.Millisecond)

	ev := event.Event{UUID: "lalala"}
	s1 := &sink{name: "s1"}
	s2 := &sink{name: "s2"}

	ctx, cancel := context.WithCancel(context.Background())
	mux, err := New(ctx, v, Config{
		Logger: common.LoggerCreate(s1, v),
	})
	assert.Nil(t, err)

	mux.Subscribe(s1)
	mux.Subscribe(s2)

	s1.f = func(ev []event.Event) error {
		return common.ErrorPermf("boo")
	}

	s2.f = func(ev []event.Event) error {
		return nil
	}

	// Test Permerror
	// Push 10 events
	for i := 0; i < 10; i++ {
		mux.Push(ev, nil)
	}

	// Should just fallthrough and not get stuck, buffer empty
	assert.EqualValues(t, 0, mux.count)

	// Test cancellation with retries on temperror
	s1.f = func(ev []event.Event) error {
		return common.ErrorTempf("boo")
	}

	t1 := time.Now()
	go func() { time.Sleep(10 * time.Millisecond); cancel() }()

	// Push 10 events
	for i := 0; i < 10; i++ {
		mux.Push(ev, nil)
	}

	// It should fall through after >= 10ms
	if time.Since(t1) < 10*time.Millisecond {
		t.Errorf("Delay < 10ms (%s)", time.Since(t1))
	}
}
