package serverless

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"strings"
	"time"

	capi "xata/internal/api"
	"xata/internal/o11y"
	"xata/services/gateway/metrics"
	"xata/services/gateway/serverless/spec"
	"xata/services/gateway/session"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	proxyproto "github.com/pires/go-proxyproto"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/trace"
)

type Config struct {
	ListenAddress string
	// TrustProxyHeader sets the PROXY protocol policy of ListenAddress.
	TrustProxyHeader bool
	// ProxyListenAddress, when set, is a second address that always uses the
	// PROXY protocol header.
	ProxyListenAddress string
	TLSCert            *tls.Certificate
}

type Server struct {
	echo   *echo.Echo
	config Config
}

type handler struct {
	resolver session.BranchResolver
	dialer   *session.ClusterDialer
	tracer   trace.Tracer
	metrics  *metrics.GatewayMetrics
	ipFilter session.IPFilter
}

func NewServer(
	o *o11y.O,
	resolver session.BranchResolver,
	dialer *session.ClusterDialer,
	gwMetrics *metrics.GatewayMetrics,
	ipFilter session.IPFilter,
	config Config,
) (*Server, error) {
	tracer := o.Tracer("gateway.http")

	h := &handler{
		resolver: resolver,
		dialer:   dialer,
		tracer:   tracer,
		metrics:  gwMetrics,
		ipFilter: ipFilter,
	}

	s := &Server{config: config}

	e := capi.SetupRouter(o, capi.WithCORS(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowHeaders: corsAllowHeaders(),
		MaxAge:       86400,
	}))
	e.Use(middleware.BodyLimit(maxRequestSize))
	e.Use(normalizeHeadersMiddleware)
	spec.RegisterHandlers(e, h)

	s.echo = e
	return s, nil
}

var headerSuffixes = []string{
	"Connection-String",
	"Array-Mode",
	"Raw-Text-Output",
	"Batch-Isolation-Level",
	"Batch-Read-Only",
	"Batch-Deferrable",
}

func corsAllowHeaders() []string {
	headers := append([]string{}, capi.DefaultAllowHeaders...)
	for _, suffix := range headerSuffixes {
		headers = append(headers, suffix, "Neon-"+suffix)
	}
	return headers
}

func normalizeHeadersMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		h := c.Request().Header
		for _, suffix := range headerSuffixes {
			if h.Get(suffix) != "" {
				continue
			}
			for key := range h {
				if strings.HasSuffix(key, "-"+suffix) {
					h.Set(suffix, h.Get(key))
					break
				}
			}
		}
		return next(c)
	}
}

func (s *Server) Run(ctx context.Context) error {
	logger := log.Ctx(ctx)

	listeners, err := s.listen()
	if err != nil {
		return err
	}

	server := &http.Server{
		Handler:           s.echo,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		ErrorLog: o11y.StdLoggerFunc(logger, func(msg string) zerolog.Level {
			if strings.HasPrefix(msg, "http: TLS handshake error") {
				return zerolog.DebugLevel
			}
			return zerolog.WarnLevel
		}),
	}

	errChan := make(chan error, len(listeners))
	for _, listener := range listeners {
		go func() {
			logger.Info().Str("address", listener.Addr().String()).Msg("starting HTTP/WebSocket server")
			if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
				errChan <- err
			}
		}()
	}

	select {
	case <-ctx.Done():
		logger.Info().Msg("shutting down HTTP/WebSocket server")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error().Err(err).Msg("HTTP server shutdown error")
			return err
		}
		return ctx.Err()
	case err := <-errChan:
		server.Close()
		return err
	}
}

// listen binds ListenAddress and, when set, ProxyListenAddress. With USE,
// RemoteAddr() returns the address from a PROXY protocol header when one is
// present. With REQUIRE, a connection without a header fails on its first
// read. With IGNORE, it always returns the socket address.
func (s *Server) listen() ([]net.Listener, error) {
	type listenerConfig struct {
		address string
		policy  proxyproto.Policy
	}
	configs := []listenerConfig{{address: s.config.ListenAddress, policy: proxyproto.IGNORE}}
	if s.config.TrustProxyHeader {
		configs[0].policy = proxyproto.USE
	}
	if s.config.ProxyListenAddress != "" {
		configs = append(configs, listenerConfig{address: s.config.ProxyListenAddress, policy: proxyproto.REQUIRE})
	}

	// Turn off Multipath TCP, which Go enables for listeners by default since
	// Go 1.24. It brings no benefit behind a load balancer and exposes the
	// listener to MPTCP-only kernel bugs (CVE-2026-98432).
	var lc net.ListenConfig
	lc.SetMultipathTCP(false)

	listeners := make([]net.Listener, 0, len(configs))
	for _, cfg := range configs {
		baseListener, err := lc.Listen(context.Background(), "tcp", cfg.address)
		if err != nil {
			for _, l := range listeners {
				l.Close()
			}
			return nil, err
		}

		var listener net.Listener = &proxyproto.Listener{
			Listener: baseListener,
			ConnPolicy: func(opts proxyproto.ConnPolicyOptions) (proxyproto.Policy, error) {
				return cfg.policy, nil
			},
		}
		if s.config.TLSCert != nil {
			listener = tls.NewListener(listener, &tls.Config{
				Certificates: []tls.Certificate{*s.config.TLSCert},
				MinVersion:   tls.VersionTLS12,
			})
		}
		listeners = append(listeners, listener)
	}
	return listeners, nil
}
