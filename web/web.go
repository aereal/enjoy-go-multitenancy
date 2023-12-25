package web

import (
	"context"
	"encoding/json"
	"enjoymultitenancy/repos"
	"errors"
	"fmt"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dimfeld/httptreemux/v5"
	"go.opentelemetry.io/contrib/instrumentation/net/http/httptrace/otelhttptrace"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

var (
	defaultShutdownGrace = time.Second * 5
	defaultPort          = "8080"
	mediaTypeJSON        = "application/json"
)

func NewServer(optFns ...NewServerOption) *Server {
	s := &Server{}
	for _, o := range optFns {
		o(s)
	}
	if s.port == "" {
		s.port = defaultPort
	}
	if s.shutdownGrace == 0 {
		s.shutdownGrace = defaultShutdownGrace
	}
	return s
}

type NewServerOption func(s *Server)

func WithPort(port string) NewServerOption {
	return func(s *Server) { s.port = port }
}

func WithShutdownGrace(grace time.Duration) NewServerOption {
	return func(s *Server) { s.shutdownGrace = grace }
}

func WithUserRepo(ur *repos.UserRepo) NewServerOption {
	return func(s *Server) { s.userRepo = ur }
}

func WithApartmentMiddleware(mw func(http.Handler) http.Handler) NewServerOption {
	return func(s *Server) { s.apartmentMiddleware = mw }
}

type Server struct {
	shutdownGrace       time.Duration
	port                string
	userRepo            *repos.UserRepo
	apartmentMiddleware func(http.Handler) http.Handler
}

type errorResponse struct {
	Error string `json:"error"`
}

func (s *Server) handlePostUsers() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		slog.InfoContext(ctx, "handle POST /users")
		w.Header().Set("content-type", mediaTypeJSON)
		if mt, _, _ := mime.ParseMediaType(r.Header.Get("content-type")); mt != mediaTypeJSON {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(errorResponse{Error: fmt.Sprintf("invalid request content type: %s", mt)})
			return
		}
		defer r.Body.Close()
		userToRegister := new(repos.UserToRegister)
		if err := json.NewDecoder(r.Body).Decode(userToRegister); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(errorResponse{Error: fmt.Sprintf("failed to decode request body: %s", err)})
			return
		}
		if err := s.userRepo.RegisterUser(ctx, userToRegister); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(errorResponse{Error: fmt.Sprintf("failed to register user: %s", err)})
			return
		}
	})
}

func (s *Server) handleGetUser() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		params := httptreemux.ContextParams(r.Context())
		user, err := s.userRepo.FetchUserByName(r.Context(), params["name"])
		w.Header().Set("content-type", "application/json")
		switch {
		case errors.Is(err, repos.ErrUserNameRequired):
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintln(w, `{"error":"user name required"}`)
			return
		case errors.Is(err, repos.ErrNotFound):
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintln(w, `{"error":"not found"}`)
			return
		case err != nil:
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintln(w, `{"error":"failed to fetch the user"}`)
			return
		}
		_ = json.NewEncoder(w).Encode(user)
	})
}

func (s *Server) handler() http.Handler {
	m := httptreemux.NewContextMux()
	m.UseHandler(withOtel)
	m.UseHandler(injectRouteAttrs)
	m.UseHandler(s.apartmentMiddleware)
	m.Handler(http.MethodPost, "/users", s.handlePostUsers())
	m.Handler(http.MethodGet, "/users/:name", s.handleGetUser())
	return m
}

func withOtel(next http.Handler) http.Handler {
	return otelhttp.NewHandler(next, "server",
		otelhttp.WithPublicEndpoint(),
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string { return fmt.Sprintf("%s %s", r.Method, r.URL.Path) }),
		otelhttp.WithClientTrace(func(ctx context.Context) *httptrace.ClientTrace { return otelhttptrace.NewClientTrace(ctx) }))
}

func injectRouteAttrs(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if span := trace.SpanFromContext(ctx); span.IsRecording() {
			if data := httptreemux.ContextData(ctx); data != nil {
				params := data.Params()
				attrs := make([]attribute.KeyValue, 0, len(params)+1)
				attrs = append(attrs, semconv.HTTPRoute(data.Route()))
				for k, v := range data.Params() {
					attrs = append(attrs, attribute.String(fmt.Sprintf("http.path_parameters.%s", k), v))
				}
				span.SetAttributes(attrs...)
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) Start(ctx context.Context) error {
	hs := &http.Server{
		Handler: s.handler(),
		Addr:    net.JoinHostPort("localhost", s.port),
	}
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()
	go func() {
		<-ctx.Done()
		slog.InfoContext(ctx, "shutting down server", slog.Duration("grace", s.shutdownGrace))
		ctx, cancel := context.WithTimeout(ctx, s.shutdownGrace)
		defer cancel()
		if err := hs.Shutdown(ctx); err != nil {
			slog.WarnContext(ctx, "cannot shut down server gracefully", slog.String("error", err.Error()))
		}
	}()
	slog.InfoContext(ctx, "start server", slog.String("port", s.port))
	if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
