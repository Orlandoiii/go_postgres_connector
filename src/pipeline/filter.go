package pipeline

import (
	"context"

	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/config"
)

type EventFilter interface {
	ShouldProcess(ctx context.Context, event *ChangeEvent, txEvent *TransactionEvent) bool
}

type EventFilterFactory interface {
	CreateFilter(config config.FilterConfig) EventFilter
}
