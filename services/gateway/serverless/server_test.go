package serverless

import (
	"net"
	"testing"

	proxyproto "github.com/pires/go-proxyproto"
	"github.com/stretchr/testify/require"
)

// TestListenProxyHeaderPolicy checks which client address each HTTP listener
// reports when the client sends its own PROXY protocol header.
func TestListenProxyHeaderPolicy(t *testing.T) {
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
			s := &Server{config: Config{
				ListenAddress:      "127.0.0.1:0",
				TrustProxyHeader:   tt.trustProxyHeader,
				ProxyListenAddress: "127.0.0.1:0",
			}}
			listeners, err := s.listen()
			require.NoError(t, err)
			require.Len(t, listeners, 2)
			for _, l := range listeners {
				t.Cleanup(func() { l.Close() })
			}

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

			accepted, err := target.Accept()
			require.NoError(t, err)
			defer accepted.Close()

			if tt.wantHeaderAddr {
				require.Equal(t, headerAddr.String(), accepted.RemoteAddr().String())
			} else {
				require.Equal(t, conn.LocalAddr().String(), accepted.RemoteAddr().String())
			}
		})
	}
}

// TestListenRejectsMultipathTCP checks that the listeners do not accept
// Multipath TCP: a client asking for MPTCP falls back to plain TCP. Where the
// kernel has no MPTCP support, the client never uses it and the test passes
// trivially.
func TestListenRejectsMultipathTCP(t *testing.T) {
	s := &Server{config: Config{
		ListenAddress:      "127.0.0.1:0",
		ProxyListenAddress: "127.0.0.1:0",
	}}
	listeners, err := s.listen()
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

// TestListenProxyListenRequiresHeader checks that the proxy listen address
// rejects a connection that sends no PROXY protocol header.
func TestListenProxyListenRequiresHeader(t *testing.T) {
	s := &Server{config: Config{
		ListenAddress:      "127.0.0.1:0",
		ProxyListenAddress: "127.0.0.1:0",
	}}
	listeners, err := s.listen()
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
