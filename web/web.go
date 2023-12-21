package web

import (
	"context"
	"encoding/json"
	"enjoymultitenancy/apartment"
	"enjoymultitenancy/logging"
	"enjoymultitenancy/repos"
	"errors"
	"fmt"
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
	"go.uber.org/zap"
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

func WithApartmentHandler(h *apartment.Manager) NewServerOption {
	return func(s *Server) { s.apHandler = h }
}

type Server struct {
	shutdownGrace time.Duration
	port          string
	userRepo      *repos.UserRepo
	apHandler     *apartment.Manager
}

type errorResponse struct {
	Error string `json:"error"`
}

func (s *Server) handlePostUsers() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		logger := logging.FromContext(ctx)
		logger.Info("handle POST /users")
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
	m.UseHandler(logging.Middleware())
	m.UseHandler(apartment.InjectTenantFromHeader())
	m.UseHandler(apartment.Middleware(s.apHandler))
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

func (s *Server) Start(ctx context.Context) error {
	logger := logging.FromContext(ctx)
	reqLogger := logger.Named("http")
	hs := &http.Server{
		Handler: s.handler(),
		Addr:    net.JoinHostPort("localhost", s.port),
		BaseContext: func(l net.Listener) context.Context {
			return logging.WithLogger(context.Background(), reqLogger)
		},
	}
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()
	go func() {
		<-ctx.Done()
		logger.Info("shutting down server", zap.Duration("grace", s.shutdownGrace))
		ctx, cancel := context.WithTimeout(ctx, s.shutdownGrace)
		defer cancel()
		if err := hs.Shutdown(ctx); err != nil {
			logger.Warn("cannot shut down server gracefully", zap.Error(err))
		}
	}()
	logger.Info("start server", zap.String("port", s.port))
	if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
