package serverless

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"time"

	"xata/services/gateway/metrics"
	"xata/services/gateway/session"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

const (
	sslRequestCode    = 80877103
	gssEncRequestCode = 80877104

	// maxStartupReadBytes bounds the opening client message, which with
	// pipelineConnect carries the first query. Must exceed coder/websocket's
	// 32 KiB default; matches the HTTP /sql ceiling (maxResponseSizeBytes).
	maxStartupReadBytes = 10 * 1024 * 1024
)

func (h *handler) Websocket(c echo.Context) error {
	ctx := c.Request().Context()
	startTime := time.Now()

	ctx, span := h.tracer.Start(ctx, "websocket_proxy")
	defer span.End()

	ws, err := websocket.Accept(c.Response(), c.Request(), &websocket.AcceptOptions{
		OriginPatterns: []string{"*"},
	})
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("websocket upgrade")
		span.RecordError(err)
		return nil
	}
	defer ws.CloseNow()

	// Lift coder/websocket's 32 KiB default before reading the (possibly
	// pipelined and large) opening message. NetConn disables it below anyway.
	ws.SetReadLimit(maxStartupReadBytes)

	// Read startup message, rejecting SSL/GSS encryption requests
	var startupData []byte
	for {
		_, data, err := ws.Read(ctx)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("read startup")
			span.RecordError(err)
			return nil
		}

		if len(data) == 8 {
			code := binary.BigEndian.Uint32(data[4:8])
			if code == sslRequestCode || code == gssEncRequestCode {
				if err := ws.Write(ctx, websocket.MessageBinary, []byte("N")); err != nil {
					log.Ctx(ctx).Error().Err(err).Msg("reject ssl/gss")
					span.RecordError(err)
					return nil
				}
				continue
			}
		}

		startupData = data
		break
	}

	// Extract hostname from startup message parameters, fall back to HTTP Host header
	serverName := extractStartupParam(startupData, "host")
	if serverName == "" {
		serverName = c.Request().Host
	}
	if serverName == "" {
		ws.Close(websocket.StatusPolicyViolation, "missing host")
		return nil
	}

	span.SetAttributes(metrics.AttrHost.String(serverName))

	branch, err := h.resolver.Resolve(ctx, serverName, session.EndpointPooler)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Str("host", serverName).Msg("resolve branch")
		span.RecordError(err)
		ws.Close(websocket.StatusInternalError, "resolve branch failed")
		return nil
	}

	if err := session.CheckIPAllowed(h.ipFilter, branch.ID, c.Request().RemoteAddr); err != nil {
		ws.Close(websocket.StatusPolicyViolation, "forbidden")
		return nil
	}

	span.SetAttributes(metrics.AttrBranchID.String(branch.ID))
	logger := log.Ctx(ctx).With().Str("branch_id", branch.ID).Logger()

	h.metrics.ConnectionStart(ctx, metrics.ProtocolWebSocket)
	defer func() {
		h.metrics.ConnectionEnd(ctx, metrics.ProtocolWebSocket, time.Since(startTime))
	}()

	startup, pipelinedData, err := parseStartupPipeline(startupData)
	if err != nil {
		logger.Error().Err(err).Msg("parse startup")
		span.RecordError(err)
		return nil
	}

	wsConn := websocket.NetConn(ctx, ws, websocket.MessageBinary)

	var pgConn net.Conn
	if pipelinedData != nil {
		logger.Debug().Msg("pipelined connect detected")
		dialFunc := func(dialCtx context.Context, _, _ string) (net.Conn, error) {
			return h.dialer.Dial(dialCtx, "tcp", branch)
		}
		pgConn, err = pipelineAuth(ctx, dialFunc, wsConn, startup, pipelinedData)
		if err != nil {
			logger.Error().Err(err).Msg("relay auth")
			span.RecordError(err)
			// PgErrors are already forwarded inside pipelineAuth. For everything
			// else, surface a FATAL wire error so the client sees a reason
			// instead of an opaque "Connection terminated".
			var pgErr *pgconn.PgError
			switch {
			case errors.As(err, &pgErr):
			case errors.Is(err, session.ErrBranchHibernated):
				sendPgWireError(wsConn, "57P01", "branch is hibernated, reactivate it to continue")
			case errors.Is(err, session.ErrNoReadReplica):
				sendPgWireError(wsConn, "57P03", "branch has no read replica, use the primary endpoint or add a replica")
			case errors.Is(err, context.DeadlineExceeded):
				sendPgWireError(wsConn, "57P03", "branch is reactivating, please retry")
			default:
				sendPgWireError(wsConn, "28000", "authentication failed")
			}
			return nil
		}
	} else {
		pgConn, err = h.dialer.Dial(ctx, "tcp", branch)
		if err != nil {
			logger.Error().Err(err).Msg("dial postgres")
			span.RecordError(err)
			switch {
			case errors.Is(err, session.ErrBranchHibernated):
				sendPgWireError(wsConn, "57P01", "branch is hibernated, reactivate it to continue")
			case errors.Is(err, session.ErrNoReadReplica):
				sendPgWireError(wsConn, "57P03", "branch has no read replica, use the primary endpoint or add a replica")
			}
			return nil
		}
		if _, err := pgConn.Write(startup); err != nil {
			pgConn.Close()
			logger.Error().Err(err).Msg("send startup")
			span.RecordError(err)
			return nil
		}
	}
	defer pgConn.Close()

	logger.Debug().Msg("websocket connection established")

	// Bidirectional proxy using errgroup for proper goroutine lifecycle
	g := new(errgroup.Group)

	g.Go(func() error {
		defer ws.Close(websocket.StatusNormalClosure, "")
		_, err := io.Copy(wsConn, pgConn)
		return err
	})

	g.Go(func() error {
		defer pgConn.Close()
		_, err := io.Copy(pgConn, wsConn)
		return err
	})

	g.Wait()

	logger.Debug().Dur("duration", time.Since(startTime)).Msg("websocket closed")
	return nil
}

// sendPgWireError writes a FATAL ErrorResponse with the given SQLSTATE code.
func sendPgWireError(conn net.Conn, code, msg string) {
	resp := &pgproto3.ErrorResponse{
		Severity: "FATAL",
		Code:     code,
		Message:  msg,
	}
	var buf [256]byte
	encoded, _ := resp.Encode(buf[:0])
	conn.Write(encoded)
}

func extractStartupParam(data []byte, key string) string {
	if len(data) < 8 {
		return ""
	}
	var msg pgproto3.StartupMessage
	if err := msg.Decode(data[4:]); err != nil {
		return ""
	}
	return msg.Parameters[key]
}
