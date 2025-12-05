package pipeline

import (
	"context"

	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/config"
)

type EventFilter interface {
	ShouldProcessSingleEvent(ctx context.Context, event *ChangeEventSink) bool
}

type EventFilterFactory interface {
	CreateFilter(config config.FilterConfig) EventFilter
}
