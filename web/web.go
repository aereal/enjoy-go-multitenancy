package web

import (
	"context"
	"enjoymultitenancy/logging"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/httptrace/otelhttptrace"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"
)

var (
	defaultShutdownGrace = time.Second * 5
	defaultPort          = "8080"
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

type Server struct {
	shutdownGrace time.Duration
	port          string
}

func (s *Server) handler() http.Handler {
	mux := http.NewServeMux()
	return otelhttp.NewHandler(mux, "server",
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
