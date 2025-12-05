package expressions

import (
	"context"
	"slices"
	"strings"

	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/config"
	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/observability"
	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/pipeline"
)

type ExpressionFilter struct {
	*Evaluator
	observability.Logger
}

func NewExpressionFilter(config config.FilterConfig, logger observability.Logger) *ExpressionFilter {
	evaluator := NewEvaluator(config, logger)

	return &ExpressionFilter{Evaluator: evaluator, Logger: logger}
}

func (f *ExpressionFilter) ShouldProcessSingleEvent(ctx context.Context,
	event *pipeline.ChangeEventSink) bool {

	if len(f.FilterConfig.Actions) == 0 {
		return false
	}

	eventAction := strings.ToLower(string(event.Operation))

	if !slices.Contains(f.FilterConfig.Actions, eventAction) {

		return false
	}

	if len(f.FilterConfig.Conditions) == 0 {
		return true
	}

	result, err := f.Evaluator.Evaluate(event)

	if err != nil {

		f.Error(ctx, "Error evaluating expression", err,
			"event", event)

		return false
	}

	return result
}

type ExpressionFilterFactory struct {
	Logger observability.Logger
}

func NewExpressionFilterFactory(logger observability.Logger) *ExpressionFilterFactory {

	return &ExpressionFilterFactory{Logger: logger}
}

func (f *ExpressionFilterFactory) CreateFilter(config config.FilterConfig) pipeline.EventFilter {

	evaluator := NewEvaluator(config, f.Logger)

	return &ExpressionFilter{Evaluator: evaluator, Logger: f.Logger}
}
