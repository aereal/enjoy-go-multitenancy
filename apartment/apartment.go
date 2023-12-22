package apartment

import (
	"context"
	"database/sql"
	"encoding/json"
	"enjoymultitenancy/logging"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/rs/xid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type tenantCtxKey struct{}
type reqIDCtxKey struct{}

var (
	// ErrNoTenantBound is an error that represents no tenant bound state.
	ErrNoTenantBound = errors.New("no tenant bound for the context")

	// ErrNoConnectionBound is an error that represents no DB connection bound state.
	ErrNoConnectionBound = errors.New("no connection bound for the context")
)

// Tenant is an identifier of each tenants.
type Tenant string

// WithTenant returns new context that contains given tenant.
func WithTenant(ctx context.Context, tenant Tenant) context.Context {
	return context.WithValue(ctx, tenantCtxKey{}, tenant)
}

// ContextWithRequestID returns new context that contains current request ID.
func ContextWithRequestID(ctx context.Context, id xid.ID) context.Context {
	return context.WithValue(ctx, reqIDCtxKey{}, id)
}

// RequestIDFromContext extracts request ID from the context.
func RequestIDFromContext(ctx context.Context) (xid.ID, bool) {
	id, ok := ctx.Value(reqIDCtxKey{}).(xid.ID)
	return id, ok
}

func defaultGetTenant(ctx context.Context) (Tenant, bool) {
	tenant, ok := ctx.Value(tenantCtxKey{}).(Tenant)
	return tenant, ok
}

type GetConnFn[DB DBish, Conn Connish] func(ctx context.Context, db DB) (Conn, error)

// New returns new Apartment.
func New[DB DBish, Conn Connish](db DB, getConn GetConnFn[DB, Conn]) *Apartment[DB, Conn] {
	a := &Apartment[DB, Conn]{
		db:      db,
		conns:   make(map[xid.ID]Conn),
		getConn: getConn,
	}
	if a.getTenant == nil {
		a.getTenant = defaultGetTenant
	}
	return a
}

type Apartment[DB DBish, Conn Connish] struct {
	db        DB
	getTenant func(ctx context.Context) (Tenant, bool)
	mux       sync.Mutex
	conns     map[xid.ID]Conn
	getConn   GetConnFn[DB, Conn]
}

func (h *Apartment[DB, Conn]) ExtractConnection(ctx context.Context) (conn Conn, err error) {
	reqID, ok := RequestIDFromContext(ctx)
	if !ok {
		err = ErrNoConnectionBound
		return
	}
	h.mux.Lock()
	defer h.mux.Unlock()
	conn, ok = h.conns[reqID]
	if !ok {
		err = ErrNoConnectionBound
		return
	}
	return
}

func InjectTenantFromHeader() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tenant := Tenant(r.Header.Get("tenant-id"))
			if tenant != "" {
				trace.SpanFromContext(r.Context()).SetAttributes(attribute.String("tenant.name", string(tenant)))
			}
			next.ServeHTTP(w, r.WithContext(WithTenant(r.Context(), tenant)))
		})
	}
}

func (h *Apartment[DB, Conn]) Middleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			logger := logging.FromContext(ctx)
			tenant, ok := h.getTenant(ctx)
			if !ok {
				logger.Warn("no tenant found")
				respondError(w, http.StatusBadRequest, "no tenant found")
				return
			}
			ctx = WithTenant(ctx, tenant)
			logger = logger.With(zap.String("tenant", string(tenant)))
			logger.Info("open new connection")
			conn, err := h.getConn(ctx, h.db)
			if err != nil {
				logger.Warn("failed to open connection", zap.Error(err))
				respondError(w, http.StatusInternalServerError, "failed to open new connection")
				return
			}
			defer func() {
				logger.Info("close the connection")
				if err := conn.Close(); err != nil {
					logger.Warn("failed to properly close the connection", zap.Error(err))
				}
			}()
			logger.Info("change the tenant")
			exCtx, cancel := context.WithTimeout(ctx, time.Second*3)
			defer cancel()
			if _, err := conn.ExecContext(exCtx, fmt.Sprintf("use %s", tenant)); err != nil {
				logger.Warn("failed to change the tenant", zap.Error(err))
				respondError(w, http.StatusInternalServerError, "failed to change the tenant")
				return
			}
			logger.Info("put the connection to the pool")
			h.mux.Lock()
			reqID := xid.New()
			h.conns[reqID] = conn
			h.mux.Unlock()
			defer func() {
				logger.Info("delete the request ID", zap.Stringer("request_id", reqID))
				h.mux.Lock()
				delete(h.conns, reqID)
				h.mux.Unlock()
			}()
			next.ServeHTTP(w, r.WithContext(ContextWithRequestID(ctx, reqID)))
		})
	}
}

func respondError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": msg})
}

type Beginner interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

type Closer interface {
	Close() error
}

type Common interface {
	PingContext(context.Context) error
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	PrepareContext(context.Context, string) (*sql.Stmt, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type DBish interface {
	Beginner
	Closer
	Common
	Stats() sql.DBStats
	SetConnMaxIdleTime(time.Duration)
	SetConnMaxLifetime(time.Duration)
	SetMaxIdleConns(int)
	SetMaxOpenConns(int)
}

type Connish interface {
	Beginner
	Closer
	Common
}
