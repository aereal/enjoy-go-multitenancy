package repos

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/rs/xid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var (
	ErrEmptyBlogs     = errors.New("empty blogs")
	ErrBlogNotFound   = errors.New("blog not found")
	ErrNoUpdateClause = errors.New("no update clause found")
)

func WithDB(db *sqlx.DB) NewBlogRepoOption { return func(br *BlogRepo) { br.db = db } }

type NewBlogRepoOption func(br *BlogRepo)

func NewBlogRepo(opts ...NewBlogRepoOption) *BlogRepo {
	r := &BlogRepo{
		tracer: otel.GetTracerProvider().Tracer("repos.BlogRepo"),
	}
	for _, o := range opts {
		o(r)
	}
	r.tables.blogs = goqu.Dialect("mysql").From("blogs")
	return r
}

type BlogRepo struct {
	tracer trace.Tracer
	db     *sqlx.DB
	tables struct {
		blogs *goqu.SelectDataset
	}
}

type Blog struct {
	ID   string `db:"id"`
	Name string `db:"name"`
	URL  string `db:"url"`
}

type BlogToCreate struct {
	Name string `db:"name"`
	URL  string `db:"url"`
}

type blogToCreate struct {
	*BlogToCreate
	ID string `db:"id"`
}

func (r *BlogRepo) create(ctx context.Context, execer sqlx.ExecerContext, blog *blogToCreate) (err error) {
	ctx, span := r.tracer.Start(ctx, "create")
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		}
		span.End()
	}()

	query, args, err := r.tables.blogs.Insert().Prepared(true).Rows(blog).ToSQL()
	if err != nil {
		return fmt.Errorf("failed to build query: %w", err)
	}
	if _, err := execer.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("ExecContext: %w", err)
	}
	return nil
}

func (r *BlogRepo) CreateBlogs(ctx context.Context, blogs []*BlogToCreate) (err error) {
	ctx, span := r.tracer.Start(ctx, "CreateBlogs", trace.WithAttributes(attribute.Int("blogs.count", len(blogs))))
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		}
		span.End()
	}()

	if len(blogs) < 1 {
		return ErrEmptyBlogs
	}

	tx, err := r.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return fmt.Errorf("BeginTxx: %w", err)
	}
	for _, blog := range blogs {
		blog := blog
		toCreate := &blogToCreate{BlogToCreate: blog, ID: xid.New().String()}
		if err := r.create(ctx, tx, toCreate); err != nil {
			if txErr := tx.Rollback(); txErr != nil {
				slog.WarnContext(ctx, "failed to rollback", slog.String("error", txErr.Error()))
			}
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("Commit: %w", err)
	}
	return nil
}

type UpdateBlogOption func(c *updateBlogConfig)

func WithBlogName(name string) UpdateBlogOption { return func(c *updateBlogConfig) { c.name = name } }

type updateBlogConfig struct {
	name string
}

func (r *BlogRepo) UpdateBlog(ctx context.Context, blogID string, opts ...UpdateBlogOption) (err error) {
	ctx, span := r.tracer.Start(ctx, "UpdateBlog", trace.WithAttributes(attribute.String("blog.id", blogID)))
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		}
		span.End()
	}()

	cfg := new(updateBlogConfig)
	for _, o := range opts {
		o(cfg)
	}
	if cfg.name == "" {
		return ErrNoUpdateClause
	}

	record := goqu.Record{}
	if cfg.name != "" {
		span.SetAttributes(attribute.String("blog.name", cfg.name))
		record["name"] = cfg.name
	}
	query, args, err := r.tables.blogs.
		Update().
		Prepared(true).
		Where(goqu.C("id").Eq(blogID)).
		Set(record).
		Limit(1).
		ToSQL()
	if err != nil {
		return fmt.Errorf("failed to build query: %w", err)
	}

	if _, err := r.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("ExecContext: %w", err)
	}

	return nil
}

func (r *BlogRepo) FindBlogByID(ctx context.Context, blogID string) (_ *Blog, err error) {
	ctx, span := r.tracer.Start(ctx, "FindBlogByBlogID", trace.WithAttributes(attribute.String("blog.id", blogID)))
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		}
		span.End()
	}()

	query, args, err := r.tables.blogs.Prepared(true).Where(goqu.C("id").Eq(blogID)).Limit(1).ToSQL()
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}
	blog := new(Blog)
	if err := r.db.GetContext(ctx, blog, query, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrBlogNotFound
		}
		return nil, fmt.Errorf("GetContext: %w", err)
	}
	return blog, nil
}
