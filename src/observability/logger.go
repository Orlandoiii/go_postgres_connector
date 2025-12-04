package observability

import "context"

type Logger interface {
	Trace(ctx context.Context, message string, fields ...interface{}) Logger

	Debug(ctx context.Context, message string, fields ...interface{}) Logger

	Info(ctx context.Context, message string, fields ...interface{}) Logger

	Warn(ctx context.Context, message string, err error, fields ...interface{}) Logger

	Error(ctx context.Context, message string, err error, fields ...interface{}) Logger

	Fatal(ctx context.Context, message string, err error, fields ...interface{}) Logger

	AddFieldsToContext(ctx context.Context, fields map[string]string) context.Context
}
