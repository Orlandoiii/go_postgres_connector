package observability

import (
	"context"

	"github.com/SOLUCIONESSYCOM/scribe"
)

type ScribeLogger struct {
	*scribe.Scribe
}

func NewScribeLogger(l *scribe.Scribe) *ScribeLogger {
	return &ScribeLogger{Scribe: l}
}

func (l *ScribeLogger) Trace(ctx context.Context, message string,
	fields ...interface{}) Logger {
	l.Scribe.TraceCtx(ctx).Fields(fields).Msg(message)
	return l
}

func (l *ScribeLogger) Debug(ctx context.Context, message string,
	fields ...interface{}) Logger {
	l.Scribe.DebugCtx(ctx).Fields(fields).Msg(message)
	return l
}

func (l *ScribeLogger) Info(ctx context.Context, message string,
	fields ...interface{}) Logger {
	l.Scribe.InfoCtx(ctx).Fields(fields).Msg(message)
	return l
}

func (l *ScribeLogger) Warn(ctx context.Context, message string,
	err error, fields ...interface{}) Logger {
	l.Scribe.WarnCtx(ctx).Err(err).Fields(fields).Msg(message)
	return l
}

func (l *ScribeLogger) Error(ctx context.Context, message string,
	err error, fields ...interface{}) Logger {

	l.Scribe.ErrorCtx(ctx).Err(err).Fields(fields).Msg(message)
	return l
}

func (l *ScribeLogger) Fatal(ctx context.Context, message string,
	err error, fields ...interface{}) Logger {
	l.Scribe.FatalCtx(ctx).Err(err).Fields(fields).Msg(message)
	return l
}

func (l *ScribeLogger) AddFieldsToContext(ctx context.Context,
	fields map[string]string) context.Context {
	sc := scribe.GetLogContext(ctx)

	for k, v := range fields {
		sc.Set(k, v)
	}

	return sc.WithCtx(ctx)
}
