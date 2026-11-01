package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"time"

	"xata/internal/o11y"
	"xata/services/gateway/initiator"
	"xata/services/gateway/metrics"
	"xata/services/gateway/session"

	"github.com/elastic/go-concert/ctxtool"
	"github.com/google/uuid"
	proxyproto "github.com/pires/go-proxyproto"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Server interface {
	Run(context.Context) error
}

// Server implements the SQL wire protocol server. The server uses the
// `Initiator` to accept and configure a new `Session` when a client connects.
type server struct {
	initiator            sessionInitiator
	drainer              *timedWaitGroup
	listeners            []listenerConfig
	metrics              *metrics.GatewayMetrics
	shutdownSignal       context.Context
	clientTCPUserTimeout time.Duration
}

// Initiator creates and configures a new session. The Initiator should handle
// authentication before creating an actual session.
type sessionInitiator interface {
	InitSession(ctx context.Context, sessionID string, conn net.Conn) (session.Session, error)
}

// listenerConfig is one address the server accepts connections on, with the
// PROXY protocol policy for that address.
type listenerConfig struct {
	address string
	policy  proxyproto.Policy
}

const sessionIDKey = "session_id"

// NewServer creates a new SQL gateway server.
func NewServer(si sessionInitiator, cfg ServerConfig, m *metrics.GatewayMetrics) Server {
	if si == nil {
		si = initiator.NewRejectInitiator()
	}

	listeners := []listenerConfig{{address: cfg.Listen, policy: proxyHeaderPolicy(cfg.TrustProxyHeader)}}
	if cfg.ProxyListen != "" {
		listeners = append(listeners, listenerConfig{address: cfg.ProxyListen, policy: proxyproto.REQUIRE})
	}

	return &server{
		listeners:            listeners,
		drainer:              newTimedWaitGroup(cfg.DrainingTime),
		initiator:            si,
		metrics:              m,
		shutdownSignal:       cfg.ShutdownSignal,
		clientTCPUserTimeout: cfg.ClientTCPUserTimeout,
	}
}

func (s *server) Run(ctx context.Context) error {
	listeners, err := s.listen(ctx)
	if err != nil {
		return err
	}
	return s.runWithListeners(ctx, listeners)
}

// listen binds every configured address and wraps it with its PROXY protocol
// policy. The listeners are returned in configuration order.
func (s *server) listen(ctx context.Context) ([]net.Listener, error) {
	lc := newListenConfig()
	listeners := make([]net.Listener, 0, len(s.listeners))
	for _, cfg := range s.listeners {
		baseListener, err := lc.Listen(ctx, "tcp", cfg.address)
		if err != nil {
			for _, l := range listeners {
				l.Close()
			}
			return nil, err
		}
		listeners = append(listeners, newProxyListener(baseListener, cfg.policy))
	}
	return listeners, nil
}

// newListenConfig returns the ListenConfig for client-facing listeners.
//
// Multipath TCP is turned off explicitly. Since Go 1.24, listeners accept
// MPTCP by default on Linux, which exposes the gateway to kernel bugs reachable
// only through MPTCP listeners (CVE-2026-98432). The gateway sits behind a load
// balancer, so a client cannot open extra subflows and MPTCP brings no benefit.
// Setting it in code, rather than through GODEBUG, keeps it off regardless of
// the deployment environment.
func newListenConfig() net.ListenConfig {
	var lc net.ListenConfig
	lc.SetMultipathTCP(false)
	return lc
}

// proxyHeaderPolicy maps the trust setting of a listener to its PROXY
// protocol policy. IGNORE still reads and drops a header, so a header sent
// by the client does not reach the TLS handshake.
func proxyHeaderPolicy(trust bool) proxyproto.Policy {
	if trust {
		return proxyproto.USE
	}
	return proxyproto.IGNORE
}

// newProxyListener wraps a listener with PROXY protocol version 1 and 2
// support. With USE, RemoteAddr() returns the address from the header when a
// header is present. With REQUIRE, a connection without a header fails on its
// first read. With IGNORE, it always returns the socket address.
func newProxyListener(l net.Listener, policy proxyproto.Policy) *proxyproto.Listener {
	trusted := policy == proxyproto.USE || policy == proxyproto.REQUIRE
	log.Info().Msgf("listening on %s, trusting the PROXY protocol header: %t", l.Addr(), trusted)
	return &proxyproto.Listener{
		Listener: l,
		ConnPolicy: func(opts proxyproto.ConnPolicyOptions) (proxyproto.Policy, error) {
			return policy, nil
		},
	}
}

// runWithListeners runs the server on already bound listeners and takes
// ownership of them.
func (s *server) runWithListeners(ctx context.Context, listeners []net.Listener) error {
	var ac ctxtool.AutoCancel
	defer ac.Cancel()

	// On shutdown: close the listeners, then give connections time to finish.
	// The drainer.Wait call must run inside this hook: ctx.Done() does not
	// fire until the hook returns, which is what keeps session contexts (and
	// their client connections) alive during the draining period.
	drainDone := make(chan struct{})
	serveCtx := ac.With(ctxtool.WithFunc(ctx, func() {
		defer close(drainDone)
		// Stop accepting connections right away
		for _, l := range listeners {
			l.Close()
		}

		if !s.shuttingDown(ctx) {
			// Not a graceful shutdown (e.g. sibling failure): skip draining
			// so session contexts are released promptly.
			return
		}

		log.Info().Msgf("draining %d active connections, waiting up to %s",
			s.drainer.GetCount(), s.drainer.drainingTimeout)

		// Wait on a fresh context: the shutdown context is already cancelled.
		if err := s.drainer.Wait(context.Background()); err != nil {
			log.Info().Msgf("draining period completed, closing %v active connections after draining period",
				s.drainer.GetCount())
		} else {
			log.Info().Msg("no active connections left")
		}
	}))

	serveErrs := make(chan error, len(listeners))
	for _, l := range listeners {
		go func() { serveErrs <- s.serve(serveCtx, l) }()
	}

	err := <-serveErrs
	if !s.shuttingDown(ctx) {
		// serve failed on its own, not via a graceful shutdown: fail fast
		// instead of draining. The deferred cancel closes the other listeners.
		return err
	}

	// Shutting down: serve returned because the hook above closed the
	// listeners. Block until draining completes so active connections are not
	// killed by process exit.
	<-drainDone
	return nil
}

// shuttingDown reports whether a graceful shutdown was requested. The
// dedicated shutdown signal takes precedence over the run context, which can
// also be cancelled by sibling failures (see ServerConfig.ShutdownSignal).
func (s *server) shuttingDown(ctx context.Context) bool {
	if s.shutdownSignal != nil {
		return s.shutdownSignal.Err() != nil
	}
	return ctx.Err() != nil
}

func (s *server) serve(ctx context.Context, l net.Listener) error {
	o := o11y.Ctx(ctx)
	logger := o.Logger()

	for ctx.Err() == nil {
		conn, err := l.Accept()
		if err != nil {
			return err
		}

		// Bound how long a copy loop may block writing to a client that has
		// stopped reading. That stall otherwise wedges the whole session,
		// because the client leg is TLS-terminated and crypto/tls serialises
		// a connection's reads and writes through one lock.
		if s.clientTCPUserTimeout > 0 {
			if err := session.SetConnTCPUserTimeout(rawClientConn(conn), s.clientTCPUserTimeout); err != nil {
				logger.Warn().Err(err).Msg("set client TCP_USER_TIMEOUT")
			}
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					event := logger.Error().Bytes("error.stack", debug.Stack())
					if err, ok := r.(error); ok {
						event = event.Err(err)
					} else {
						event = event.Err(fmt.Errorf("%v", r))
					}
					event.Msg("session panic")
				}
			}()

			defer conn.Close()

			startTime := time.Now()
			s.metrics.ConnectionStart(ctx, metrics.ProtocolWire)
			var branchID string
			defer func() {
				s.metrics.ConnectionEnd(ctx, metrics.ProtocolWire, time.Since(startTime))
			}()

			sessionID := uuid.New().String()
			sessionLogger := logger.With().Str(sessionIDKey, sessionID).Logger()

			sessionLogger.Debug().Msg("new client session")
			defer func() { sessionLogger.Debug().Msg("client session ended") }()

			var err error
			branchID, err = s.startSession(sessionLogger.WithContext(ctx), sessionID, conn)

			// RemoteAddr()/ProxyHeader() on a proxyproto.Conn must not be
			// called before startSession has parsed the header (see initiator).
			sessionLogger = enrichSessionLogger(sessionLogger, conn, branchID)

			if err != nil {
				if isIgnorableError(err) {
					sessionLogger.Debug().Err(err).Msg("ignorable error during gw session")
				} else {
					sessionLogger.Error().Err(err).Msg("error during gw session")
				}
			}
		}()
	}

	return ctx.Err()
}

// rawClientConn unwraps the PROXY protocol wrapper to reach the raw socket,
// which is what exposes syscall.Conn for socket options. Raw() returns the
// underlying connection without consuming the PROXY header, so it is safe to
// call before the header is parsed.
func rawClientConn(conn net.Conn) net.Conn {
	if proxyConn, ok := conn.(*proxyproto.Conn); ok {
		return proxyConn.Raw()
	}
	return conn
}

func isProxyProtocolConn(conn net.Conn) bool {
	proxyConn, ok := conn.(*proxyproto.Conn)
	if !ok {
		return false
	}
	return proxyConn.ProxyHeader() != nil
}

// enrichSessionLogger adds available connection identity to session logs.
// Call it only after the PROXY protocol header has been parsed.
func enrichSessionLogger(logger zerolog.Logger, conn net.Conn, branchID string) zerolog.Logger {
	logger = logger.With().Str("branchID", branchID).Logger()
	return enrichConnectionLogger(logger, conn)
}

// enrichConnectionLogger adds available network identity to session logs.
// Call it only after the PROXY protocol header has been parsed.
func enrichConnectionLogger(logger zerolog.Logger, conn net.Conn) zerolog.Logger {
	context := logger.With().
		Bool("external_connection", isProxyProtocolConn(conn))

	if addr := conn.RemoteAddr(); addr != nil {
		context = context.Str("client_addr", addr.String())
	}
	if addr := conn.LocalAddr(); addr != nil {
		context = context.Str("destination_addr", addr.String())
	}

	if proxyConn, ok := conn.(*proxyproto.Conn); ok {
		if rawAddr := proxyConn.Raw().LocalAddr(); rawAddr != nil {
			context = context.Str("gateway_addr", rawAddr.String())
		}
		if header := proxyConn.ProxyHeader(); header != nil {
			context = context.Int("proxy_protocol_version", int(header.Version))
		}
	}

	return context.Logger()
}

func isIgnorableError(err error) bool {
	return errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, initiator.ErrorSSLRequired) ||
		errors.Is(err, initiator.ErrorStartupMsgCode) ||
		errors.Is(err, initiator.ErrorStartupMsgLength)
}

func branchIDFromError(err error) string {
	type branchIDError interface {
		error
		BranchID() string
	}
	if branchErr, ok := errors.AsType[branchIDError](err); ok {
		return branchErr.BranchID()
	}
	return ""
}

func (s *server) startSession(ctx context.Context, sessionID string, clientConn net.Conn) (string, error) {
	if err := s.drainer.Add(1); err != nil {
		clientConn.Close()
		log.Info().Msg("connection attempt rejected while in draining mode")
		return "", nil
	}

	ctx, cancel := ctxtool.WithFunc(ctx, func() {
		defer s.drainer.Done()
		if err := clientConn.Close(); err != nil {
			if errors.Is(err, net.ErrClosed) {
				log.Debug().Msgf("connection was already closed")
			} else {
				log.Err(err).Msgf("close client connection")
			}
		}
	})
	defer cancel()

	session, err := s.initiator.InitSession(ctx, sessionID, clientConn)
	if err != nil {
		return branchIDFromError(err), err
	}

	branchID := session.BranchID()
	// InitSession has consumed the PROXY protocol header. Enrich the context
	// before ServeSQLSession so all lifecycle and copy-loop logs carry the
	// client and destination connection identity.
	sessionLogger := enrichConnectionLogger(*log.Ctx(ctx), clientConn)
	return branchID, session.ServeSQLSession(sessionLogger.WithContext(ctx))
}
