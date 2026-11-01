package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"xata/services/gateway/metrics"
	"xata/services/gateway/session"

	proxyproto "github.com/pires/go-proxyproto"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

// stubSession blocks in ServeSQLSession until release is closed or the
// session context is cancelled.
type stubSession struct {
	release chan struct{}
}

func (s *stubSession) BranchID() string { return "test-branch" }

func (s *stubSession) ServeSQLSession(ctx context.Context) error {
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// stubInitiator hands each accepted session to the test via the sessions
// channel.
type stubInitiator struct {
	sessions chan *stubSession
}

func (si *stubInitiator) InitSession(ctx context.Context, sessionID string, conn net.Conn) (session.Session, error) {
	s := &stubSession{release: make(chan struct{})}
	si.sessions <- s
	return s, nil
}

type branchError struct {
	err error
}

func (e *branchError) Error() string    { return e.err.Error() }
func (e *branchError) Unwrap() error    { return e.err }
func (e *branchError) BranchID() string { return "test-branch" }

type errorInitiator struct{}

func (errorInitiator) InitSession(context.Context, string, net.Conn) (session.Session, error) {
	return nil, &branchError{err: errors.New("session failed")}
}

type loggingSession struct{}

func (loggingSession) BranchID() string { return "test-branch" }

func (loggingSession) ServeSQLSession(ctx context.Context) error {
	logger := log.Ctx(ctx).With().Str("branchID", "test-branch").Logger()
	logger.Info().Msg("serving session")
	return nil
}

type loggingInitiator struct{}

func (loggingInitiator) InitSession(context.Context, string, net.Conn) (session.Session, error) {
	return loggingSession{}, nil
}

func startTestServer(t *testing.T, drainingTime time.Duration) (addr string, sessions chan *stubSession, runErr chan error, cancel context.CancelFunc) {
	return startTestServerWithSignal(t, drainingTime, nil)
}

// startTestServerWithSignal starts a test server. The optional signal
// function derives the ServerConfig.ShutdownSignal context from the run
// context; when nil, no shutdown signal is configured.
func startTestServerWithSignal(t *testing.T, drainingTime time.Duration, signal func(runCtx context.Context) context.Context) (addr string, sessions chan *stubSession, runErr chan error, cancel context.CancelFunc) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr = l.Addr().String()

	m, err := metrics.New(noop.NewMeterProvider().Meter("test"))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cfg := ServerConfig{Listen: addr, DrainingTime: drainingTime}
	if signal != nil {
		cfg.ShutdownSignal = signal(ctx)
	}

	si := &stubInitiator{sessions: make(chan *stubSession, 1)}
	srv, ok := NewServer(si, cfg, m).(*server)
	require.True(t, ok)

	runErr = make(chan error, 1)
	go func() { runErr <- srv.runWithListeners(ctx, []net.Listener{newProxyListener(l, proxyproto.USE)}) }()

	return addr, si.sessions, runErr, cancel
}

// dial connects to the server; the listener is already bound when the server
// starts, so no retry is needed.
func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	// Production initiators read the first client byte while determining
	// whether the connection uses PROXY protocol. The stub initiator does not,
	// so provide a byte that lets the listener make the same determination.
	_, err = conn.Write([]byte{0})
	require.NoError(t, err)
	return conn
}

func requireRunning(t *testing.T, runErr chan error, wait time.Duration) {
	t.Helper()
	select {
	case err := <-runErr:
		t.Fatalf("Run returned early: %v", err)
	case <-time.After(wait):
	}
}

func requireStopped(t *testing.T, runErr chan error, wait time.Duration) {
	t.Helper()
	select {
	case err := <-runErr:
		require.NoError(t, err)
	case <-time.After(wait):
		t.Fatal("Run did not return")
	}
}

func TestServerShutdownDrainsActiveConnections(t *testing.T) {
	addr, sessions, runErr, cancel := startTestServer(t, 30*time.Second)

	conn := dial(t, addr)
	defer conn.Close()
	sess := <-sessions

	cancel()

	// Run must block while the session is still active.
	requireRunning(t, runErr, 300*time.Millisecond)

	// New connections are refused while draining.
	require.Eventually(t, func() bool {
		c, err := net.Dial("tcp", addr)
		if err == nil {
			c.Close()
			return false
		}
		return true
	}, 5*time.Second, 10*time.Millisecond)

	// Once the session ends, Run returns without error.
	close(sess.release)
	requireStopped(t, runErr, 5*time.Second)
}

func TestServerShutdownDrainingTimeout(t *testing.T) {
	addr, sessions, runErr, cancel := startTestServer(t, 300*time.Millisecond)

	conn := dial(t, addr)
	defer conn.Close()
	<-sessions // session is never released

	start := time.Now()
	cancel()

	requireStopped(t, runErr, 5*time.Second)
	require.GreaterOrEqual(t, time.Since(start), 300*time.Millisecond)
}

func TestServerShutdownNoActiveConnections(t *testing.T) {
	addr, sessions, runErr, cancel := startTestServer(t, 30*time.Second)

	// Make sure the server is up before shutting it down, and end the
	// session so nothing is left to drain.
	conn := dial(t, addr)
	sess := <-sessions
	close(sess.release)
	require.NoError(t, conn.Close())

	cancel()

	// Without active connections Run must return promptly, well before the
	// draining timeout.
	requireStopped(t, runErr, 5*time.Second)
}

func TestServerShutdownSignalDrains(t *testing.T) {
	// The shutdown signal is the run context itself, as in the non-HTTP
	// production wiring: cancelling it is a graceful shutdown and drains.
	addr, sessions, runErr, cancel := startTestServerWithSignal(t, 30*time.Second,
		func(runCtx context.Context) context.Context { return runCtx })

	conn := dial(t, addr)
	defer conn.Close()
	sess := <-sessions

	cancel()

	requireRunning(t, runErr, 300*time.Millisecond)

	close(sess.release)
	requireStopped(t, runErr, 5*time.Second)
}

func TestServerSiblingFailureSkipsDraining(t *testing.T) {
	// The shutdown signal stays uncancelled while the run context is
	// cancelled, as happens when a sibling failure cancels the shared
	// errgroup context: the server must fail fast instead of draining.
	addr, sessions, runErr, cancel := startTestServerWithSignal(t, 30*time.Second,
		func(runCtx context.Context) context.Context { return context.Background() })

	conn := dial(t, addr)
	defer conn.Close()
	sess := <-sessions
	t.Cleanup(func() { close(sess.release) })

	cancel()

	select {
	case err := <-runErr:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return despite sibling failure")
	}

	// Sessions must be released promptly instead of draining: the client
	// connection gets closed once the shutdown hook returns.
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, err := conn.Read(make([]byte, 1))
	require.Error(t, err)
	require.NotErrorIs(t, err, os.ErrDeadlineExceeded)
}

func TestStartSessionReturnsBranchIDFromError(t *testing.T) {
	client, serverConn := net.Pipe()
	t.Cleanup(func() {
		client.Close()
		serverConn.Close()
	})

	srv := &server{
		initiator: errorInitiator{},
		drainer:   newTimedWaitGroup(time.Second),
	}

	branchID, err := srv.startSession(context.Background(), "session-id", serverConn)
	require.EqualError(t, err, "session failed")
	require.Equal(t, "test-branch", branchID)
}

func TestStartSessionEnrichesSessionLogger(t *testing.T) {
	client, serverConn := net.Pipe()
	t.Cleanup(func() {
		client.Close()
		serverConn.Close()
	})

	proxyConn := proxyproto.NewConn(serverConn, proxyproto.WithPolicy(proxyproto.USE))
	header := &proxyproto.Header{
		Version:           1,
		Command:           proxyproto.PROXY,
		TransportProtocol: proxyproto.TCPv4,
		SourceAddr: &net.TCPAddr{
			IP:   net.ParseIP("198.51.100.10"),
			Port: 43210,
		},
		DestinationAddr: &net.TCPAddr{
			IP:   net.ParseIP("203.0.113.20"),
			Port: 5432,
		},
	}

	writeErr := make(chan error, 1)
	go func() {
		_, err := header.WriteTo(client)
		writeErr <- err
	}()

	var output bytes.Buffer
	logger := zerolog.New(&output)
	ctx := logger.WithContext(context.Background())
	srv := &server{
		initiator: loggingInitiator{},
		drainer:   newTimedWaitGroup(time.Second),
	}

	branchID, err := srv.startSession(ctx, "session-id", proxyConn)
	require.NoError(t, err)
	require.Equal(t, "test-branch", branchID)
	require.NoError(t, <-writeErr)

	rawEntry := bytes.TrimSpace(output.Bytes())
	require.Equal(t, 1, bytes.Count(rawEntry, []byte(`"branchID"`)))

	var entry map[string]any
	require.NoError(t, json.Unmarshal(rawEntry, &entry))
	require.Equal(t, "test-branch", entry["branchID"])
	require.Equal(t, "198.51.100.10:43210", entry["client_addr"])
	require.Equal(t, "203.0.113.20:5432", entry["destination_addr"])
	require.Equal(t, "pipe", entry["gateway_addr"])
	require.Equal(t, true, entry["external_connection"])
	require.Equal(t, float64(1), entry["proxy_protocol_version"])
}

// addrInitiator reports the client address that the session sees, then
// ends the session.
type addrInitiator struct {
	addrs chan net.Addr
}

func (ai addrInitiator) InitSession(_ context.Context, _ string, conn net.Conn) (session.Session, error) {
	ai.addrs <- conn.RemoteAddr()
	return nil, errors.New("session ended by test")
}

// TestServerProxyHeaderPolicy checks which client address each listener
// reports when the client sends its own PROXY protocol header.
func TestServerProxyHeaderPolicy(t *testing.T) {
	headerAddr := &net.TCPAddr{IP: net.ParseIP("203.0.113.10"), Port: 54321}

	tests := map[string]struct {
		trustProxyHeader bool
		proxyListener    bool
		wantHeaderAddr   bool
	}{
		"listen address trusts the header": {
			trustProxyHeader: true,
			wantHeaderAddr:   true,
		},
		"listen address ignores the header": {
			trustProxyHeader: false,
			wantHeaderAddr:   false,
		},
		"proxy listen address trusts the header when the listen address does not": {
			trustProxyHeader: false,
			proxyListener:    true,
			wantHeaderAddr:   true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			m, err := metrics.New(noop.NewMeterProvider().Meter("test"))
			require.NoError(t, err)

			ai := addrInitiator{addrs: make(chan net.Addr, 1)}
			srv, ok := NewServer(ai, ServerConfig{
				Listen:           "127.0.0.1:0",
				TrustProxyHeader: tt.trustProxyHeader,
				ProxyListen:      "127.0.0.1:0",
			}, m).(*server)
			require.True(t, ok)

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			listeners, err := srv.listen(ctx)
			require.NoError(t, err)
			require.Len(t, listeners, 2)
			go func() { _ = srv.runWithListeners(ctx, listeners) }()

			target := listeners[0]
			if tt.proxyListener {
				target = listeners[1]
			}
			conn, err := net.Dial("tcp", target.Addr().String())
			require.NoError(t, err)
			defer conn.Close()

			header := proxyproto.HeaderProxyFromAddrs(2, headerAddr, target.Addr())
			_, err = header.WriteTo(conn)
			require.NoError(t, err)
			_, err = conn.Write([]byte{0})
			require.NoError(t, err)

			var got net.Addr
			select {
			case got = <-ai.addrs:
			case <-time.After(5 * time.Second):
				t.Fatal("no session started")
			}

			if tt.wantHeaderAddr {
				require.Equal(t, headerAddr.String(), got.String())
			} else {
				require.Equal(t, conn.LocalAddr().String(), got.String())
			}
		})
	}
}

// TestServerProxyListenRequiresHeader checks that the proxy listen address
// rejects a connection that sends no PROXY protocol header.
func TestServerProxyListenRequiresHeader(t *testing.T) {
	m, err := metrics.New(noop.NewMeterProvider().Meter("test"))
	require.NoError(t, err)

	srv, ok := NewServer(nil, ServerConfig{
		Listen:      "127.0.0.1:0",
		ProxyListen: "127.0.0.1:0",
	}, m).(*server)
	require.True(t, ok)

	listeners, err := srv.listen(context.Background())
	require.NoError(t, err)
	require.Len(t, listeners, 2)
	for _, l := range listeners {
		t.Cleanup(func() { l.Close() })
	}

	conn, err := net.Dial("tcp", listeners[1].Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.Write([]byte{0})
	require.NoError(t, err)

	accepted, err := listeners[1].Accept()
	require.NoError(t, err)
	defer accepted.Close()

	_, err = accepted.Read(make([]byte, 1))
	require.ErrorIs(t, err, proxyproto.ErrNoProxyProtocol)
}

// TestServerListenRejectsMultipathTCP checks that the listeners do not accept
// Multipath TCP: a client asking for MPTCP falls back to plain TCP. Where the
// kernel has no MPTCP support, the client never uses it and the test passes
// trivially.
func TestServerListenRejectsMultipathTCP(t *testing.T) {
	m, err := metrics.New(noop.NewMeterProvider().Meter("test"))
	require.NoError(t, err)

	srv, ok := NewServer(nil, ServerConfig{
		Listen:      "127.0.0.1:0",
		ProxyListen: "127.0.0.1:0",
	}, m).(*server)
	require.True(t, ok)

	listeners, err := srv.listen(context.Background())
	require.NoError(t, err)
	require.Len(t, listeners, 2)
	for _, l := range listeners {
		t.Cleanup(func() { l.Close() })
	}

	var d net.Dialer
	d.SetMultipathTCP(true)
	for _, l := range listeners {
		conn, err := d.Dial("tcp", l.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		tcpConn, ok := conn.(*net.TCPConn)
		require.True(t, ok)
		got, err := tcpConn.MultipathTCP()
		require.NoError(t, err)
		require.False(t, got)
	}
}

// TestServerRunCancelRace cancels the run context concurrently with server
// startup: the shutdown hook may run before runWithListener finishes setting
// up and must not race it (caught by the race detector).
func TestServerRunCancelRace(t *testing.T) {
	m, err := metrics.New(noop.NewMeterProvider().Meter("test"))
	require.NoError(t, err)

	for i := range 1000 {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		si := &stubInitiator{sessions: make(chan *stubSession, 1)}
		srv, ok := NewServer(si, ServerConfig{Listen: l.Addr().String()}, m).(*server)
		require.True(t, ok)

		delay := time.Duration(i%2000) * time.Nanosecond
		go func() {
			time.Sleep(delay)
			cancel()
		}()
		_ = srv.runWithListeners(ctx, []net.Listener{newProxyListener(l, proxyproto.USE)})
		cancel()
	}
}

// TestRawClientConn guards the PROXY-protocol unwrap that the client
// TCP_USER_TIMEOUT relies on: a proxyproto.Conn does not expose the raw
// socket, so the option has to be set on the connection underneath it.
func TestRawClientConn(t *testing.T) {
	t.Run("unwraps a proxyproto conn to the raw socket", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		tcpConn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer tcpConn.Close()

		proxyConn := proxyproto.NewConn(tcpConn, proxyproto.WithPolicy(proxyproto.USE))
		_, wrapped := any(proxyConn).(syscall.Conn)
		require.False(t, wrapped, "proxyproto.Conn must not expose the raw socket directly")

		raw := rawClientConn(proxyConn)
		_, unwrapped := raw.(syscall.Conn)
		require.True(t, unwrapped, "unwrapped conn must expose the raw socket")

		// The whole inbound path applies cleanly on the unwrapped socket.
		require.NoError(t, session.SetConnTCPUserTimeout(raw, 30*time.Second))
	})

	t.Run("returns a non-proxy conn unchanged", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()
		require.Same(t, client, rawClientConn(client))
	})
}
