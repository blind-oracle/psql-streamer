package event

import (
	"time"
)

// Event actions
const (
	ActionInsert = "insert"
	ActionUpdate = "update"
	ActionDelete = "delete"
)

var (
	// ActionsMap is map of all supported actions
	ActionsMap = map[string]bool{
		ActionInsert: true,
		ActionUpdate: true,
		ActionDelete: true,
	}
)

// Event represents a database event
type Event struct {
	Host        string
	Database    string
	Table       string
	Action      string
	WALPosition uint64
	Timestamp   time.Time
	UUID        string

	Columns map[string]interface{}
}
