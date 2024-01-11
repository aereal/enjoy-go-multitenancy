package main

import (
	"context"
	"enjoymultitenancy/adapters"
	"enjoymultitenancy/logging"
	"enjoymultitenancy/repos"
	"enjoymultitenancy/web"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/aereal/nagaya"
	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

func main() {
	os.Exit(run())
}

func run() int {
	logging.Init()
	ctx := context.Background()
	tp, err := setupOtel(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "failed to setup OpenTelemetry instrumentation", slog.String("error", err.Error()))
		return 1
	}
	defer func() {
		ctx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()
		if err := tp.Shutdown(ctx); err != nil {
			slog.WarnContext(ctx, "failed to shutdown TracerProvider", slog.String("error", err.Error()))
		}
	}()
	otel.SetTracerProvider(tp)
	db, err := adapters.OpenDB(os.Getenv("DSN"))
	if err != nil {
		slog.ErrorContext(ctx, "failed to create DB", slog.String("error", err.Error()))
		return 1
	}
	defer func() {
		if err := db.Close(); err != nil {
			slog.WarnContext(ctx, "failed to gracefully close DB connection", slog.String("error", err.Error()))
		}
	}()
	eventsDB, err := adapters.OpenEventsDB(os.Getenv("EVENTS_DB_DSN"))
	if err != nil {
		slog.ErrorContext(ctx, "failed to open events DB", slog.String("error", err.Error()))
		return 1
	}
	defer func() {
		if err := eventsDB.Close(); err != nil {
			slog.WarnContext(ctx, "failed to gracefully close events DB connection", slog.String("error", err.Error()))
		}
	}()
	ngy := nagaya.New[*sqlx.DB, *sqlx.Conn](db, func(ctx context.Context, db *sqlx.DB) (*sqlx.Conn, error) { return db.Connx(ctx) })
	userRepo := repos.NewUserRepo(repos.WithNagaya(ngy))
	mw := nagaya.Middleware[*sqlx.DB, *sqlx.Conn](ngy, nagaya.GetTenantFromHeader("tenant-id"))
	blogRepo := repos.NewBlogRepo(repos.WithDB(db))
	eventsRepo := repos.NewEventsRepo(repos.WithEventDB(eventsDB))
	srv := web.NewServer(web.WithUserRepo(userRepo), web.WithPort(os.Getenv("PORT")), web.WithApartmentMiddleware(mw), web.WithBlogRepo(blogRepo), web.WithEventsRepo(eventsRepo))
	if err := srv.Start(ctx); err != nil {
		slog.ErrorContext(ctx, "failed to start server", slog.String("error", err.Error()))
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
