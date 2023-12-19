package main

import (
	"context"
	"enjoymultitenancy/logging"
	"enjoymultitenancy/web"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.uber.org/zap"
)

func main() {
	os.Exit(run())
}

func run() int {
	logger := logging.New()
	defer func() { _ = logger.Sync() }()
	ctx := logging.WithLogger(context.Background(), logger)
	tp, err := setupOtel(ctx)
	if err != nil {
		logger.Error("failed to setup OpenTelemetry instrumentation", zap.Error(err))
		return 1
	}
	defer func() {
		ctx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()
		if err := tp.Shutdown(ctx); err != nil {
			logger.Warn("failed to shutdown TracerProvider", zap.Error(err))
		}
	}()
	otel.SetTracerProvider(tp)
	srv := web.NewServer(web.WithPort(os.Getenv("PORT")))
	if err := srv.Start(ctx); err != nil {
		logger.Error("failed to start server", zap.Error(err))
		return 1
	}
	return 0
}

func setupOtel(ctx context.Context) (*sdktrace.TracerProvider, error) {
	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("otlptracegrpc.New: %w", err)
	}
	res, err := resource.New(
		ctx,
		resource.WithHost(),
		resource.WithOS(),
		resource.WithProcess(),
		resource.WithTelemetrySDK(),
		resource.WithAttributes(
			semconv.ServiceName("enjoy-multitenancy"),
			semconv.DeploymentEnvironment("local"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("resource.New: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	return tp, nil
}
