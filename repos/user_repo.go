package repos

import (
	"context"
	"enjoymultitenancy/apartment"
	"errors"
	"fmt"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
	"github.com/rs/xid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var (
	ErrUserNameRequired = errors.New("user.name is required")
)

type NewUserRepoOption func(r *UserRepo)

func WithApartment(mng *apartment.Manager) NewUserRepoOption {
	return func(r *UserRepo) { r.manager = mng }
}

func NewUserRepo(optFns ...NewUserRepoOption) *UserRepo {
	r := &UserRepo{
		tracer: otel.GetTracerProvider().Tracer("repos.UserRepo"),
	}
	for _, f := range optFns {
		f(r)
	}
	return r
}

type UserRepo struct {
	tracer  trace.Tracer
	manager *apartment.Manager
}

type UserToRegister struct {
	Name string `json:"name" db:"name"`
}

type userToRegisterDTO struct {
	*UserToRegister
	ID string `db:"id"`
}

func (r *UserRepo) RegisterUser(ctx context.Context, user *UserToRegister) (err error) {
	ctx, span := r.tracer.Start(ctx, "RegisterUser")
	defer span.End()
	defer func() {
		span.SetStatus(codes.Ok, "")
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
	}()

	if user == nil || user.Name == "" {
		return ErrUserNameRequired
	}

	query, args, err := goqu.Dialect("mysql").Insert("users").
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
