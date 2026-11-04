package initiator

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"

	"xata/internal/o11y"
	"xata/services/gateway/session"

	"github.com/jackc/pgx/v5/pgproto3"
	proxyproto "github.com/pires/go-proxyproto"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/trace"
)

type Initiator interface {
	InitSession(ctx context.Context, sessionID string, conn net.Conn) (session.Session, error)
}

// TLSInitiator is an postgres protocol connection initiator that requires clients to use TLS.
type TLSInitiator struct {
	certificate *tls.Certificate
	handler     SessionHandler
	tracer      trace.Tracer
}

type SessionHandler interface {
	CreateBackendSession(ctx context.Context, serverName string, inboundConn net.Conn, startupMessage *pgproto3.StartupMessage) (session.Session, error)
	CancelSession(ctx context.Context, serverName string, inboundConn net.Conn, req *pgproto3.CancelRequest) error
}

func New(tracer trace.Tracer, handler SessionHandler, certificate *tls.Certificate) (*TLSInitiator, error) {
	return &TLSInitiator{
		certificate: certificate,
		handler:     handler,
		tracer:      tracer,
	}, nil
}

func (si *TLSInitiator) InitSession(ctx context.Context, sessionID string, conn net.Conn) (sess session.Session, err error) {
	ctx, span := si.tracer.Start(ctx, "init_session")
	defer o11y.CloseSpan(span, &err)

	inbound := pgproto3.NewBackend(conn, conn)
	sess, err = si.processStartupPacket(ctx, inbound, conn)
	if err != nil {
		if !isClientFacingError(err) {
			inbound.Send(&pgproto3.ErrorResponse{Message: "unable to authenticate"})
			inbound.Flush()
		}
		return nil, err
	}

	return sess, nil
}

func isClientFacingError(err error) bool {
	return errors.Is(err, session.ErrIPNotAllowed) ||
		errors.Is(err, session.ErrBranchHibernated) ||
		errors.Is(err, session.ErrNoReadReplica)
}

// processStartupPacket handles the connection setup.
// In postrgres source code BackendInitialize in `backend_startup.c` handles the connection setup:
//   - optionally upgrade TPC connection to TLS (via ProcessSSLRequest):
//   - handle startup messages in `ProcessStartupMessage`
func (si *TLSInitiator) processStartupPacket(
	ctx context.Context,
	inbound *pgproto3.Backend,
	conn net.Conn,
) (session.Session, error) {
	var err error
	ctx, span := si.tracer.Start(ctx, "process_startup_packet")
	defer o11y.CloseSpan(span, &err)

	sslConn, serverName, err := si.processTLSStartup(ctx, inbound, conn)
	if err != nil {
		return nil, err
	}

	sslBackend := pgproto3.NewBackend(sslConn, sslConn)
	*inbound = *sslBackend

	var msg pgproto3.FrontendMessage
	for {
		msg, err = si.receiveStartupMessage(ctx, inbound)
		if err != nil {
			return nil, err
		}

		// See `ProcessStartupPacket` in `backend_startup.c` in PostgreSQL source code.
		// As we already have established TLS connection we can ignore SSL and
		// GSS messages (assuming sslmode and gssmode = true in Postgres source
		// code)
		if _, ok := msg.(*pgproto3.SSLRequest); ok {
			continue
		}
		if _, ok := msg.(*pgproto3.GSSEncRequest); ok {
			continue
		}
		break
	}

	var startupMessage *pgproto3.StartupMessage
	switch msg := msg.(type) {
	case *pgproto3.StartupMessage:
		log.Ctx(ctx).Debug().Msg("StartupMessage received")
		startupMessage = msg
	case *pgproto3.CancelRequest:
		log.Ctx(ctx).Debug().Msg("CancelRequest received")
		// In `ProcessStartupPacket` the `CancelRequest` is handled directly.
		// After handling it returns early forcing the connection to be closed.
		// We do not want to create a session, as the CancelRequest just acts
		// as a signal that Postgres can handle or ignore (there is no proper validation or error response).
		if err = si.handler.CancelSession(ctx, serverName, sslConn, msg); err != nil {
			// Not a critical error. There is no guarantee that the cancel
			// request will be handled.
			log.Ctx(ctx).Warn().Err(err).Msg("Error when cancelling session")
		}
		return session.NoopSession(), nil
	case *pgproto3.Terminate:
		return session.NoopSession(), nil
	default:
		return nil, fmt.Errorf("unexpected message: %T", msg)
	}

	sess, err := si.handler.CreateBackendSession(ctx, serverName, sslConn, startupMessage)
	if err != nil {
		switch {
		case errors.Is(err, session.ErrIPNotAllowed):
			inbound.Send(&pgproto3.ErrorResponse{Message: "forbidden"})
		case errors.Is(err, session.ErrBranchHibernated):
			inbound.Send(&pgproto3.ErrorResponse{
				Severity: "FATAL",
				Code:     "57P01",
				Message:  "branch is hibernated, reactivate it to continue",
			})
		case errors.Is(err, session.ErrNoReadReplica):
			inbound.Send(&pgproto3.ErrorResponse{
				Severity: "FATAL",
				Code:     "57P03",
				Message:  "branch has no read replica, use the primary endpoint or add a replica",
			})
		}
		inbound.Flush()
		return nil, err
	}
	return sess, nil
}

// processTLSStartup handles the TLS startup process.
// See `ProcessSSLRequest` (in `backend_startup.c`) in PostgreSQL source code.
//
// If first byte is 0x16, then it's a direct TLS handshake. In this case we assume the client is a direct TLS connection.
// Otherwise we might have to wait for SSLRequest startup message. As we
// require all connections to be TLS we will disallow any other startup
// messages.
func (si *TLSInitiator) processTLSStartup(ctx context.Context, inbound *pgproto3.Backend, conn net.Conn) (sslConn net.Conn, serverName string, err error) {
	ctx, span := si.tracer.Start(ctx, "process_tls_startup")
	defer o11y.CloseSpan(span, &err)

	b, err := peekByte(conn)
	if err != nil {
		return nil, "", fmt.Errorf("unable to peek at connection: %w", err)
	}

	if b == 0x16 {
		// User is attempting directl SSL handshake. Directly secure the connection before continuing.
		sslConn, serverName, err = si.processTLSHandshake(ctx, conn)
		if err != nil {
			return nil, "", fmt.Errorf("secure connection: %w", err)
		}
		return sslConn, serverName, nil
	}

	var msg pgproto3.FrontendMessage
	var gssDone bool
	for {
		msg, err = si.receiveStartupMessage(ctx, inbound)
		if err != nil {
			return nil, "", err
		}

		switch msg.(type) {
		case *pgproto3.SSLRequest:
			break
		case *pgproto3.GSSEncRequest:
			if !gssDone {
				inbound.Send(&NoResponse{})
				if err = inbound.Flush(); err != nil {
					return nil, "", fmt.Errorf("send gss response: %w", err)
				}
			}
			continue
		default:
			if _, ok := msg.(*pgproto3.SSLRequest); !ok {
				inbound.Send(&pgproto3.ErrorResponse{Message: ErrorSSLRequired.Error()})
				inbound.Flush()
				return nil, "", ErrorSSLRequired
			}
		}
		break
	}

	inbound.Send(&SSLYesResponse{})
	if err = inbound.Flush(); err != nil {
		return nil, "", fmt.Errorf("send SSLYesResponse: %w", err)
	}

	sslConn, serverName, err = si.processTLSHandshake(ctx, conn)
	if err != nil {
		return nil, "", fmt.Errorf("establish TLS connection: %w", err)
	}

	return sslConn, serverName, nil
}

func (si *TLSInitiator) receiveStartupMessage(ctx context.Context, inbound *pgproto3.Backend) (message pgproto3.FrontendMessage, err error) {
	_, span := si.tracer.Start(ctx, "receive_startup_message")
	defer o11y.CloseSpan(span, &err)

	message, err = inbound.ReceiveStartupMessage()
	if err != nil {
		if strings.Contains(err.Error(), "unknown startup message code") {
			return message, ErrorStartupMsgCode
		}
		if strings.Contains(err.Error(), "invalid length of startup packet") {
			return message, ErrorStartupMsgLength
		}
		return message, fmt.Errorf("receive startup message: %w", err)
	}

	return message, nil
}

func (si *TLSInitiator) processTLSHandshake(ctx context.Context, conn net.Conn) (tlsConn *tls.Conn, serverName string, err error) {
	ctx, span := si.tracer.Start(ctx, "process_tls_handshake")
	defer o11y.CloseSpan(span, &err)

	config := tls.Config{
		GetCertificate: func(info *tls.ClientHelloInfo) (*tls.Certificate, error) {
			log.Ctx(ctx).Debug().Msgf("GetCertificate: %s", info.ServerName)
			serverName = info.ServerName
			return si.certificate, nil
		},
		Certificates: []tls.Certificate{*si.certificate},
		MinVersion:   tls.VersionTLS12,
		VerifyConnection: func(cs tls.ConnectionState) error {
			log.Ctx(ctx).Debug().Msgf("VerifyConnection: %s", cs.ServerName)
			serverName = cs.ServerName
			return nil
		},
	}

	tlsConn = tls.Server(conn, &config)
	err = tlsConn.HandshakeContext(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("TLS handshake failed: %w", err)
	}
	return tlsConn, serverName, err
}

func peekByte(conn net.Conn) (byte, error) {
	// Unwrap PROXY protocol connections to get the underlying TCP connection
	// The PROXY protocol wrapper doesn't implement syscall.Conn, but the underlying connection does
	if proxyConn, ok := conn.(*proxyproto.Conn); ok {
		conn = proxyConn.Raw()
	}

	sysConn, ok := conn.(syscall.Conn)
	if !ok {
		return 0, fmt.Errorf("connection is not a TCP connection")
	}

	rawConn, err := sysConn.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("unable to get raw connection: %w", err)
	}

	var buf [1]byte
	var readErr error
	err = rawConn.Read(func(fd uintptr) bool {
		n, _, err := syscall.Recvfrom(int(fd), buf[:], syscall.MSG_PEEK)
		if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
			// returning 'false' will instruct `Read` to use go runtime polling to wait for data to be available
			return false
		}

		if err != nil {
			readErr = err
		}
		if n == 0 {
			readErr = io.EOF
		}
		return true
	})

	if err = errors.Join(err, readErr); err != nil {
		return 0, fmt.Errorf("unable to peek at connection: %w", err)
	}

	return buf[0], nil
}
