package pipeline

import (
	"time"

	"github.com/jackc/pglogrepl"
)

type EventType string

const (
	EventTypeBegin    EventType = "begin"
	EventTypeRelation EventType = "relation"
	EventTypeInsert   EventType = "insert"
	EventTypeUpdate   EventType = "update"
	EventTypeDelete   EventType = "delete"
	EventTypeCommit   EventType = "commit"
)

// Modelo principal del conector que representa un operacion en la base de datos por ejemplo: insert, update, delete
type ChangeEvent struct {
	Operation   EventType              `json:"operation"`
	Schema      string                 `json:"schema"`
	Table       string                 `json:"table"`
	ConsumeTime time.Time              `json:"consume_time,omitempty"`
	OldData     map[string]interface{} `json:"old_data,omitempty"`
	NewData     map[string]interface{} `json:"new_data,omitempty"`
}

func (ce *ChangeEvent) ToChangeEventSink(xid uint32,
	lsn pglogrepl.LSN) *ChangeEventSink {
	return &ChangeEventSink{
		Xid:         xid,
		Lsn:         lsn,
		Operation:   ce.Operation,
		Schema:      ce.Schema,
		Table:       ce.Table,
		ConsumeTime: ce.ConsumeTime,
		OldData:     ce.OldData,
		NewData:     ce.NewData,
	}
}

type ChangeEventSink struct {
	Xid         uint32                 `json:"xid,omitempty"`
	Lsn         pglogrepl.LSN          `json:"lsn,omitempty"`
	Operation   EventType              `json:"operation"`
	Schema      string                 `json:"schema"`
	Table       string                 `json:"table"`
	ConsumeTime time.Time              `json:"consume_time,omitempty"`
	OldData     map[string]interface{} `json:"old_data,omitempty"`
	NewData     map[string]interface{} `json:"new_data,omitempty"`
}

type TransactionEvent struct {
	Xid        uint32        `json:"tx_id,omitempty"`
	Timestamp  time.Time     `json:"timestamp,omitempty"`
	LSN        pglogrepl.LSN `json:"-"`
	BeginLSN   string        `json:"begin_lsn,omitempty"`
	Operations []ChangeEvent `json:"operations,omitempty"`
	CommitLSN  string        `json:"commit_lsn,omitempty"`
	IsCommit   bool          `json:"-"`
	CommitTime time.Time     `json:"commit_time,omitempty"`
}
