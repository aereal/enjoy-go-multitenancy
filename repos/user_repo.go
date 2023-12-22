package repos

import (
	"context"
	"database/sql"
	"enjoymultitenancy/apartment"
	"errors"
	"fmt"

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
	ErrUserNameRequired = errors.New("user.name is required")
	ErrNotFound         = errors.New("not found")
)

type NewUserRepoOption func(r *UserRepo)

func WithApartment(mng *apartment.Apartment[*sqlx.DB, *sqlx.Conn]) NewUserRepoOption {
	return func(r *UserRepo) { r.manager = mng }
}

func NewUserRepo(optFns ...NewUserRepoOption) *UserRepo {
	r := &UserRepo{
		tracer: otel.GetTracerProvider().Tracer("repos.UserRepo"),
	}
	for _, f := range optFns {
		f(r)
	}
	r.tables.users = goqu.Dialect("mysql").From("users")
	return r
}

type UserRepo struct {
	tracer  trace.Tracer
	manager *apartment.Apartment[*sqlx.DB, *sqlx.Conn]
	tables  struct {
		users *goqu.SelectDataset
	}
}

type UserToRegister struct {
	Name string `json:"name" db:"name"`
}

type userToRegisterDTO struct {
	*UserToRegister
	ID string `db:"id"`
}

type User struct {
	ID   string `db:"id"`
	Name string `db:"name"`
}

func (r *UserRepo) RegisterUser(ctx context.Context, user *UserToRegister) (err error) {
	ctx, span := r.tracer.Start(ctx, "RegisterUser")
	defer span.End()
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "")
		}
	}()

	if user == nil || user.Name == "" {
		return ErrUserNameRequired
	}

	query, args, err := r.tables.users.Insert().
		Prepared(true).
		Rows(&userToRegisterDTO{UserToRegister: user, ID: xid.New().String()}).
		ToSQL()
	if err != nil {
		return fmt.Errorf("failed to build query: %w", err)
	}

	conn, err := r.manager.ExtractConnection(ctx)
	if err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("ExecContext: %w", err)
	}

	return nil
}

func (r *UserRepo) FetchUserByName(ctx context.Context, name string) (_ *User, err error) {
	ctx, span := r.tracer.Start(ctx, "FetchUserByName", trace.WithAttributes(attribute.String("user.name", name)))
	defer span.End()
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "")
		}
	}()

	if name == "" {
		return nil, ErrUserNameRequired
	}

	query, args, err := r.tables.users.
		Where(goqu.C("name").Eq(name)).
		Limit(1).
		ToSQL()
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	conn, err := r.manager.ExtractConnection(ctx)
	if err != nil {
		return nil, err
	}
	user := new(User)
	if err := conn.GetContext(ctx, user, query, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return user, nil
}
