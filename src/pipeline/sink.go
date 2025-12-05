package pipeline

import "context"

// EventSink es la interfaz que debe implementar un sink para persistir los eventos de origen en el destino
type EventSink interface {
	PersistEvent(ctx context.Context,
		changeEvent *ChangeEventSink,
		txEvent *TransactionEvent) error

	Close() error
}

// SinkFactory es la interfaz que debe implementar un factory para crear sinks
type SinkFactory interface {
	CreateSink(tableKey string) (EventSink, error)
}
