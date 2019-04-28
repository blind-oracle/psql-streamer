package common

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_RetryForever(t *testing.T) {
	c := 0
	retryLimit := 5
	var cnt uint64
	p := RetryParams{
		F: func() error {
			if c++; c > retryLimit {
				return nil
			}

			return fmt.Errorf("Boo")
		},

		Interval:     1 * time.Millisecond,
		ErrorCounter: &cnt,
	}

	// Check execution after some failures
	ctx, cancel := context.WithCancel(context.Background())
	err := RetryForever(ctx, p)
	assert.Nil(t, err)
	assert.EqualValues(t, 5, cnt)

	// Check success on first try
	cnt = 0
	p.F = func() error { return nil }
	err = RetryForever(ctx, p)
	assert.Nil(t, err)
	assert.EqualValues(t, 0, cnt)

	// Check context cancellation
	cnt = 0
	p.F = func() error { return fmt.Errorf("Boo") }
	t1 := time.Now()
	go func() { time.AfterFunc(50*time.Millisecond, cancel) }()
	err = RetryForever(ctx, p)
	assert.NotNil(t, err)

	if time.Since(t1) < 50*time.Millisecond {
		t.Errorf("Delay should be > 50ms")
	}
}
