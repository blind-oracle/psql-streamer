package common

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// RetryParams are params for RetryForever
type RetryParams struct {
	F        func() error
	Interval time.Duration
	Logger   *Logger

	// errorMsg should have %d and %s placeholders for retry count and error
	ErrorMsg            string
	ErrorCounter        *uint64
	PromErrorCounter    prometheus.Counter
	RetryPermanentError bool
}

func (p *RetryParams) logErr(err error, retry int) {
	if p.Logger != nil {
		p.Logger.Errorf(p.ErrorMsg, retry, err)
	}

	if p.ErrorCounter != nil {
		atomic.AddUint64(p.ErrorCounter, 1)
	}

	if p.PromErrorCounter != nil {
		p.PromErrorCounter.Add(1)
	}
}

// RetryForever executes the provided function F() from RetryParams until succeeded
// It logs to logger, increments error counter and reports to prometheus (if any of these were provided)
func RetryForever(ctx context.Context, p RetryParams) (err error) {
	retry := 0

	if err = p.F(); err == nil {
		return
	}

	retry++
	p.logErr(err, retry)

	if !IsErrorTemporary(err) && !p.RetryPermanentError {
		return fmt.Errorf("Permanent error detected: %s", err)
	}

	ticker := time.NewTicker(p.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return context.Canceled

		case <-ticker.C:
			if err = p.F(); err == nil {
				return
			}

			retry++
			p.logErr(err, retry)

			if !IsErrorTemporary(err) && !p.RetryPermanentError {
				return fmt.Errorf("Permanent error detected: %s", err)
			}
		}
	}
}
