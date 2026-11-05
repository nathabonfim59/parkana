// Package server is Parkana's single HTTP server. Roles register their routes
// on it; it adds tenant resolution, request metrics, panic recovery, /ready
// and /metrics.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

// Options configures a Server.
type Options struct {
	Addr        string
	Logger      *slog.Logger
	Registry    *prometheus.Registry
	MultiTenant bool
	// ShutdownTimeout bounds graceful shutdown; default 10s.
	ShutdownTimeout time.Duration
}

// Server is an http.ServeMux with Parkana's middleware. Register routes with
// Handle before calling Run.
type Server struct {
	opts     Options
	mux      *http.ServeMux
	mu       sync.Mutex
	patterns map[string]bool
	checks   []readyCheck
	serving  atomic.Bool
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

type readyCheck struct {
	name string
	fn   func(context.Context) error
}

// New returns a Server with /ready and /metrics registered.
func New(opts Options) *Server {
	if opts.ShutdownTimeout <= 0 {
		opts.ShutdownTimeout = 10 * time.Second
	}
	s := &Server{
		opts:     opts,
		mux:      http.NewServeMux(),
		patterns: make(map[string]bool),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "parkana_http_requests_total", Help: "HTTP requests by route pattern and status code.",
		}, []string{"route", "code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "parkana_http_request_duration_seconds", Help: "HTTP request latency by route pattern.",
			Buckets: prometheus.ExponentialBuckets(0.001, 4, 8),
		}, []string{"route"}),
	}
	opts.Registry.MustRegister(s.requests, s.duration)
	s.Handle("GET /ready", http.HandlerFunc(s.ready))
	s.Handle("GET /metrics", promhttp.HandlerFor(opts.Registry, promhttp.HandlerOpts{Registry: opts.Registry}))
	return s
}

// Handle registers h for a ServeMux pattern such as "GET /loki/api/v1/labels".
// It panics on a duplicate pattern, like http.ServeMux.
func (s *Server) Handle(pattern string, h http.Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mux.Handle(pattern, h)
	s.patterns[pattern] = true
}

// Registered reports whether pattern was registered with Handle.
func (s *Server) Registered(pattern string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.patterns[pattern]
}

// AddReadyCheck adds a check that /ready runs; /ready returns 503 while any
// check fails.
func (s *Server) AddReadyCheck(name string, fn func(context.Context) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checks = append(s.checks, readyCheck{name, fn})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if !s.serving.Load() {
		http.Error(w, "not serving yet", http.StatusServiceUnavailable)
		return
	}
	s.mu.Lock()
	checks := s.checks
	s.mu.Unlock()
	for _, c := range checks {
		if err := c.fn(r.Context()); err != nil {
			http.Error(w, fmt.Sprintf("%s: %v", c.name, err), http.StatusServiceUnavailable)
			return
		}
	}
	w.Write([]byte("ready\n"))
}

// Handler returns the full middleware stack around the mux.
func (s *Server) Handler() http.Handler {
	return s.recover(tenant.Middleware(s.opts.MultiTenant, s.observe(s.mux)))
}

// Run listens on opts.Addr and serves until ctx is done, then shuts down
// gracefully.
func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.opts.Addr)
	if err != nil {
		return fmt.Errorf("server: %w", err)
	}
	return s.Serve(ctx, ln)
}

// Serve is Run on an existing listener (tests use port 0).
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	srv := &http.Server{
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return context.WithoutCancel(ctx) },
		ErrorLog:          slog.NewLogLogger(s.opts.Logger.Handler(), slog.LevelWarn),
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	s.serving.Store(true)
	s.opts.Logger.Info("http server listening", "addr", ln.Addr().String())

	select {
	case err := <-errc:
		s.serving.Store(false)
		return fmt.Errorf("server: %w", err)
	case <-ctx.Done():
	}
	s.serving.Store(false)
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.opts.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("server: shutdown: %w", err)
	}
	if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("server: %w", err)
	}
	return nil
}

// observe records metrics and logs per request. It must wrap the mux
// directly: the mux sets r.Pattern on the request it receives.
func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(sw, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		elapsed := time.Since(start)
		s.requests.WithLabelValues(route, strconv.Itoa(sw.code)).Inc()
		s.duration.WithLabelValues(route).Observe(elapsed.Seconds())
		level := slog.LevelDebug
		if sw.code >= 500 {
			level = slog.LevelWarn
		}
		s.opts.Logger.Log(r.Context(), level, "http request",
			"method", r.Method, "path", r.URL.Path, "route", route, "code", sw.code,
			"duration", elapsed, "tenant", tenant.From(r.Context()))
	})
}

func (s *Server) recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler {
					panic(v)
				}
				s.opts.Logger.Error("http handler panic", "path", r.URL.Path, "panic", v)
				http.Error(w, "internal error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// statusWriter captures the status code. Unwrap keeps
// http.ResponseController (flush, hijack for WebSocket tail) working.
type statusWriter struct {
	http.ResponseWriter
	code        int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.code, w.wroteHeader = code, true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	w.wroteHeader = true
	return w.ResponseWriter.Write(b)
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
