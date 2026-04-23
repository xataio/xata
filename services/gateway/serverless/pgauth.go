package serverless

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog/log"
)

const (
	// maxStartupPacketLen matches PostgreSQL's MAX_STARTUP_PACKET_LENGTH
	maxStartupPacketLen = 10000

	// authTimeout bounds the entire authentication phase via pgconn.
	authTimeout = 15 * time.Second
)

// parseStartupPipeline splits coalesced WebSocket data into the startup message
// and any trailing pipelined bytes (e.g. a PasswordMessage). It uses the
// startup message's 4-byte length prefix to determine the boundary.
// Returns an error if the startup message exceeds maxStartupPacketLen.
func parseStartupPipeline(data []byte) (startup, pipelined []byte, err error) {
	if len(data) < 8 {
		return nil, nil, fmt.Errorf("startup message too short: %d bytes", len(data))
	}
	startupLen := binary.BigEndian.Uint32(data[:4])
	if startupLen > maxStartupPacketLen {
		return nil, nil, fmt.Errorf("startup message too large: %d bytes (max %d)", startupLen, maxStartupPacketLen)
	}
	if startupLen < 8 || int(startupLen) > len(data) {
		return nil, nil, fmt.Errorf("invalid startup message length: %d (data %d bytes)", startupLen, len(data))
	}
	if int(startupLen) == len(data) {
		return data, nil, nil
	}
	return data[:startupLen], data[startupLen:], nil
}

// extractPassword extracts the password string from a PasswordMessage
// ('p' + int32 length + password + \0) and returns any remaining bytes
// that follow (e.g. pipelined query messages).
func extractPassword(msg []byte) (string, []byte, error) {
	if len(msg) < 5 || msg[0] != 'p' {
		return "", nil, fmt.Errorf("not a password message")
	}
	msgLen := int(binary.BigEndian.Uint32(msg[1:5]))
	totalLen := 1 + msgLen // type byte + length field value
	if totalLen > len(msg) {
		return "", nil, fmt.Errorf("password message truncated: need %d, have %d", totalLen, len(msg))
	}
	var pm pgproto3.PasswordMessage
	if err := pm.Decode(msg[5:totalLen]); err != nil {
		return "", nil, fmt.Errorf("decode password message: %w", err)
	}
	var remaining []byte
	if totalLen < len(msg) {
		remaining = msg[totalLen:]
	}
	return pm.Password, remaining, nil
}

// parseStartupParams decodes a PostgreSQL startup message and returns the
// user, database, and all other parameters as a map suitable for
// pgconn.Config.RuntimeParams.
func parseStartupParams(data []byte) (user, database string, params map[string]string, err error) {
	if len(data) < 8 {
		return "", "", nil, errors.New("startup message too short")
	}

	var msg pgproto3.StartupMessage
	if err := msg.Decode(data[4:]); err != nil {
		return "", "", nil, fmt.Errorf("decode startup message: %w", err)
	}

	user = msg.Parameters["user"]
	database = msg.Parameters["database"]

	params = make(map[string]string, len(msg.Parameters))
	for k, v := range msg.Parameters {
		if k == "user" || k == "database" {
			continue
		}
		params[k] = v
	}
	return user, database, params, nil
}

func pipelineAuth(ctx context.Context, dialFunc pgconn.DialFunc, wsConn net.Conn, startupData, pipelinedData []byte) (net.Conn, error) {
	password, queryData, err := extractPassword(pipelinedData)
	if err != nil {
		return nil, fmt.Errorf("extract password: %w", err)
	}

	user, database, params, err := parseStartupParams(startupData)
	if err != nil {
		return nil, fmt.Errorf("parse startup params: %w", err)
	}

	cfg, err := newServerlessConfig(pgxConnParams{
		User:          user,
		Password:      password,
		Database:      database,
		RuntimeParams: params,
		DialFunc:      dialFunc,
	})
	if err != nil {
		return nil, err
	}

	authCtx, cancel := context.WithTimeout(ctx, authTimeout)
	defer cancel()

	conn, err := pgconn.ConnectConfig(authCtx, &cfg.Config)
	if err != nil {
		if conn != nil {
			conn.Close(ctx)
		}
		if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
			buf, _ := pgWireError(pgErr).Encode(nil)
			if _, writeErr := wsConn.Write(buf); writeErr != nil {
				return nil, fmt.Errorf("forward error to client: %w (original: %w)", writeErr, err)
			}
		}
		return nil, fmt.Errorf("pgconn auth: %w", err)
	}

	hj, err := conn.Hijack()
	if err != nil {
		conn.Close(ctx)
		return nil, fmt.Errorf("hijack connection: %w", err)
	}

	if hj.Frontend.ReadBufferLen() > 0 {
		log.Ctx(ctx).Warn().Int("buffered_bytes", hj.Frontend.ReadBufferLen()).
			Msg("unexpected buffered data after hijack")
	}

	// Synthesize auth response from hijacked connection state
	var buf []byte
	buf, _ = (&pgproto3.AuthenticationOk{}).Encode(buf)
	for name, value := range hj.ParameterStatuses {
		buf, _ = (&pgproto3.ParameterStatus{Name: name, Value: value}).Encode(buf)
	}
	buf, _ = (&pgproto3.BackendKeyData{ProcessID: hj.PID, SecretKey: hj.SecretKey}).Encode(buf)
	buf, _ = (&pgproto3.ReadyForQuery{TxStatus: hj.TxStatus}).Encode(buf)

	if _, err := wsConn.Write(buf); err != nil {
		hj.Conn.Close()
		return nil, fmt.Errorf("write auth response: %w", err)
	}

	if len(queryData) > 0 {
		if _, err := hj.Conn.Write(queryData); err != nil {
			hj.Conn.Close()
			return nil, fmt.Errorf("flush pipelined query data: %w", err)
		}
	}

	return hj.Conn, nil
}
