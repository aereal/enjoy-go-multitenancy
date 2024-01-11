package repos

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var (
	ErrNoRecords = errors.New("no records")
)

type NewEventsRepoOption func(r *EventsRepo)

func WithEventDB(db *sqlx.DB) NewEventsRepoOption { return func(r *EventsRepo) { r.db = db } }

func NewEventsRepo(opts ...NewEventsRepoOption) *EventsRepo {
	r := &EventsRepo{tracer: otel.GetTracerProvider().Tracer("repos.EventsRepo")}
	for _, o := range opts {
		o(r)
	}
	r.tables.events = goqu.Dialect("postgresql").From("events")
	return r
}

type EventsRepo struct {
	tracer trace.Tracer
	db     *sqlx.DB
	tables struct {
		events *goqu.SelectDataset
	}
}

type EventKind string

const (
	EventKindRead EventKind = "read"
)

type Record struct {
	Kind       EventKind `db:"kind" json:"kind"`
	Message    string    `db:"message" json:"message"`
	OccurredAt time.Time `db:"occurred_at" json:"occurred_at"`
}

func (r *EventsRepo) FindRecords(ctx context.Context) (_ []*Record, err error) {
	ctx, span := r.tracer.Start(ctx, "FindRecords")
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		}
		span.End()
	}()

	query, args, err := r.tables.events.
		Select("kind", "message", "occurred_at").
		Prepared(true).
		Order(goqu.C("occurred_at").Desc()).
		ToSQL()
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}
	var records []*Record
	if err := r.db.SelectContext(ctx, &records, query, args...); err != nil {
		return nil, fmt.Errorf("SelectContext: %w", err)
	}
	return records, nil
}

func (r *EventsRepo) RecordEvent(ctx context.Context, records ...*Record) (err error) {
	ctx, span := r.tracer.Start(ctx, "RecordEvent", trace.WithAttributes(attribute.Int("record_num", len(records))))
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		}
		span.End()
	}()

	if len(records) == 0 {
		return ErrNoRecords
	}

	ds := r.tables.events.Insert().Prepared(true)
	for _, record := range records {
		ds = ds.Vals([]any{record})
	}
	query, args, err := ds.ToSQL()
	if err != nil {
		return fmt.Errorf("failed to build query: %w", err)
	}
	if _, err := r.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("ExecContext: %w", err)
	}

	return nil
}
