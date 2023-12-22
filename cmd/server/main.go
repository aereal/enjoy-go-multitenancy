package main

import (
	"context"
	"enjoymultitenancy/adapters"
	"enjoymultitenancy/apartment"
	"enjoymultitenancy/logging"
	"enjoymultitenancy/repos"
	"enjoymultitenancy/web"
	"fmt"
	"os"
	"time"

	"github.com/jmoiron/sqlx"
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
	db, err := adapters.OpenDB(os.Getenv("DSN"))
	if err != nil {
		logger.Error("failed to create DB", zap.Error(err))
		return 1
	}
	defer func() {
		if err := db.Close(); err != nil {
			logger.Warn("failed to gracefully close DB connection", zap.Error(err))
		}
	}()
	ap := apartment.New[*sqlx.DB, *sqlx.Conn](db, func(ctx context.Context, db *sqlx.DB) (*sqlx.Conn, error) { return db.Connx(ctx) })
	userRepo := repos.NewUserRepo(repos.WithApartment(ap))
	apMiddleware := ap.Middleware(apartment.GetTenantFromHeader("tenant-id"))
	srv := web.NewServer(web.WithUserRepo(userRepo), web.WithPort(os.Getenv("PORT")), web.WithApartmentMiddleware(apMiddleware))
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
