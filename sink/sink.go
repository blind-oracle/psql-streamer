package sink

import (
	"fmt"
	"log"

	"github.com/blind-oracle/psql-streamer/common"
	"github.com/blind-oracle/psql-streamer/event"
	"github.com/blind-oracle/psql-streamer/sink/kafka"
	"github.com/blind-oracle/psql-streamer/sink/stub"
	"github.com/spf13/viper"
)

// ProcessEventBatchFunc processess event batches
type ProcessEventBatchFunc func([]event.Event) error

// Sink represent a generic sink that handles events
type Sink interface {
	ProcessEventsBatch([]event.Event) error
	common.Common
}

// Init initializes a sink from a viper subtree
func Init(name string, v *viper.Viper) (s Sink, err error) {
	t := v.GetString("type")
	if t == "" {
		return nil, fmt.Errorf("Sink type not specified")
	}

	switch t {
	case "kafka":
		return kafkasink.New(name, v)
	case "stub_log":
		return stub.New(name, v, func(ev []event.Event) error { log.Printf("%d events dumped", len(ev)); return nil })
	case "stub_discard":
		return stub.New(name, v, nil)
	default:
		return nil, fmt.Errorf("Unknown sink type: %s", t)
	}
}
