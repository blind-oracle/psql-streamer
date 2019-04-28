package kafkasink

import (
	"encoding/json"
	"fmt"
	"sync/atomic"

	"github.com/segmentio/kafka-go"

	"github.com/blind-oracle/psql-streamer/common"
	"github.com/blind-oracle/psql-streamer/event"
)

type passthroughEventHandler struct {
	events uint64
}

func newPassthroughEventHandler() *passthroughEventHandler {
	return &passthroughEventHandler{}
}

func (h *passthroughEventHandler) Handle(ev event.Event) (m kafka.Message, err error) {
	atomic.AddUint64(&h.events, 1)

	js, err := json.Marshal(ev)
	if err != nil {
		err = common.ErrorPermf("Unable to marshal event to JSON: %s", err)
		return
	}

	return kafka.Message{Value: js}, nil
}

func (h *passthroughEventHandler) Stats() string {
	return fmt.Sprintf("events: %d", atomic.LoadUint64(&h.events))
}
