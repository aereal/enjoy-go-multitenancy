package logging

import (
	"context"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel/trace"
)

func Init() {
	opts := &slog.HandlerOptions{AddSource: true}
	if os.Getenv("DEBUG") != "" {
		opts.Level = levelerFunc(func() slog.Level { return slog.LevelDebug })
	}
	handler := &otelTraceIDHandler{Handler: slog.NewJSONHandler(os.Stdout, opts)}
	slog.SetDefault(slog.New(handler))
}

type otelTraceIDHandler struct {
	slog.Handler
}

var _ slog.Handler = (*otelTraceIDHandler)(nil)

func (h *otelTraceIDHandler) Handle(ctx context.Context, record slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		record.AddAttrs(slog.String("otel.trace_id", sc.TraceID().String()), slog.String("otel.span_id", sc.SpanID().String()))
	}
	return h.Handler.Handle(ctx, record)
}

type levelerFunc func() slog.Level

var _ slog.Leveler = (levelerFunc)(nil)

func (f levelerFunc) Level() slog.Level { return f() }
