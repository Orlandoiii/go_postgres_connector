package pipeline

import "context"

type EventSink interface {
	PersistSingleEvent(ctx context.Context,
		changeEvent *ChangeEventSink) error

	PersistTransaction(ctx context.Context,
		txEvent *TransactionEvent) error

	Close() error
}

type SinkFactory interface {
	CreateSink(tableKey string) (EventSink, error)
}
