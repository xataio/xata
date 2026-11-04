package serverless

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"syscall"

	"xata/services/gateway/serverless/spec"
	"xata/services/gateway/session"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/labstack/echo/v4"
)

var (
	ErrInvalidRequest          = errors.New("invalid request")
	ErrMissingQuery            = errors.New("missing query")
	ErrMissingConnectionString = errors.New("missing Connection-String header")
	ErrInvalidConnectionString = errors.New("invalid connection string format")
	ErrResponseTooLarge        = errors.New("response too large")
	ErrQueryExecTimeout        = errors.New("query execution exceeded timeout")
)

// Error classifications returned by classifyError. They are used as a metric
// attribute, a structured log field, and to select the HTTP response.
const (
	errTypeHibernated       = "hibernated"
	errTypeBranchNotFound   = "branch_not_found"
	errTypeNoReadReplica    = "no_read_replica"
	errTypeQueryTimeout     = "query_timeout"
	errTypeTimeout          = "timeout"
	errTypeCanceled         = "canceled"
	errTypeResponseTooLarge = "response_too_large"
	errTypePgErrorRequest   = "pg_error_request"
	errTypePgErrorAccess    = "pg_error_access"
	errTypePgErrorConflict  = "pg_error_conflict"
	errTypePgErrorAvailable = "pg_error_unavailable"
	errTypePgErrorUpstream  = "pg_error_upstream"
	errTypePgErrorInternal  = "pg_error_internal"
	errTypeClient           = "client"
	errTypeConnection       = "connection"
	errTypeOther            = "other"
)

// connectionEstablishmentError identifies failures before a database connection
// is available. Unwrap preserves PostgreSQL errors and lifecycle sentinels.
type connectionEstablishmentError struct{ err error }

func (e *connectionEstablishmentError) Error() string { return "connect: " + e.err.Error() }
func (e *connectionEstablishmentError) Unwrap() error { return e.err }

// pgErrorClassification maps SQLSTATE to a metric and logging category.
func pgErrorClassification(code string) string {
	// Validate the SQLSTATE shape before applying class defaults. Otherwise a
	// malformed value such as "08!01" would be treated as a connection failure.
	if len(code) != 5 {
		return errTypePgErrorInternal
	}
	for _, ch := range code {
		if (ch < '0' || ch > '9') && (ch < 'A' || ch > 'Z') {
			return errTypePgErrorInternal
		}
	}
	switch code {
	case "08P01": // The gateway's upstream PostgreSQL protocol failed.
		return errTypePgErrorUpstream
	case "08007", "40003": // Execution outcome is unknown; do not imply safe replay.
		return errTypePgErrorInternal
	case "42501":
		// The database denied permission to execute the operation.
		return errTypePgErrorAccess
	case "23502", "23514", "57014", "P0001", "P0002", "P0003":
		// Invalid row values, canceled queries, and user routine exceptions
		// remain request errors rather than data conflicts or internal failures.
		return errTypePgErrorRequest
	case "57P01", "57P02", "57P03", "57P04", "57P05":
		// Shutdown, recovery, a dropped database, or an expired session makes
		// the database connection unavailable.
		return errTypePgErrorAvailable
	}
	switch code[:2] {
	case "08", "53":
		// Connection failures and resource shortages prevent database service.
		return errTypePgErrorAvailable
	case "28":
		// Database authentication was rejected; HTTP authentication is separate.
		return errTypePgErrorAccess
	case "23", "40", "55":
		// Integrity constraints, transaction rollbacks, and object state can
		// conflict with an otherwise valid operation.
		return errTypePgErrorConflict
	case "03", "09", "0A", "0B", "0F", "0L", "0P", "0Z", "10", "20",
		"21", "22", "24", "25", "26", "27", "2B", "2D", "2F", "34",
		"38", "39", "3B", "3D", "3F", "42", "44", "54":
		// SQL syntax, data, execution context, routine usage, and program-limit
		// errors require a change to the submitted SQL or its execution context.
		return errTypePgErrorRequest
	default:
		// System, configuration, internal, and unrecognized errors do not
		// provide enough evidence to classify the request as invalid.
		return errTypePgErrorInternal
	}
}

func errorResponse(code, message string) *spec.ErrorResponse {
	return &spec.ErrorResponse{Message: message, Code: &code}
}

func pgWireError(pgErr *pgconn.PgError) *pgproto3.ErrorResponse {
	return &pgproto3.ErrorResponse{
		Severity:         pgErr.Severity,
		Code:             pgErr.Code,
		Message:          pgErr.Message,
		Detail:           pgErr.Detail,
		Hint:             pgErr.Hint,
		Position:         pgErr.Position,
		InternalPosition: pgErr.InternalPosition,
		InternalQuery:    pgErr.InternalQuery,
		Where:            pgErr.Where,
		SchemaName:       pgErr.SchemaName,
		TableName:        pgErr.TableName,
		ColumnName:       pgErr.ColumnName,
		DataTypeName:     pgErr.DataTypeName,
		ConstraintName:   pgErr.ConstraintName,
		File:             pgErr.File,
		Line:             pgErr.Line,
		Routine:          pgErr.Routine,
	}
}

func pgErrorResponse(pgErr *pgconn.PgError) *spec.ErrorResponse {
	resp := &spec.ErrorResponse{
		Message: pgErr.Message,
	}
	if pgErr.Code != "" {
		resp.Code = &pgErr.Code
	}
	if pgErr.Severity != "" {
		resp.Severity = &pgErr.Severity
	}
	if pgErr.Detail != "" {
		resp.Detail = &pgErr.Detail
	}
	if pgErr.Hint != "" {
		resp.Hint = &pgErr.Hint
	}
	if pgErr.InternalQuery != "" {
		resp.InternalQuery = &pgErr.InternalQuery
	}
	if pgErr.Where != "" {
		resp.Where = &pgErr.Where
	}
	if pgErr.SchemaName != "" {
		resp.Schema = &pgErr.SchemaName
	}
	if pgErr.TableName != "" {
		resp.Table = &pgErr.TableName
	}
	if pgErr.ColumnName != "" {
		resp.Column = &pgErr.ColumnName
	}
	if pgErr.DataTypeName != "" {
		resp.DataType = &pgErr.DataTypeName
	}
	if pgErr.ConstraintName != "" {
		resp.Constraint = &pgErr.ConstraintName
	}
	if pgErr.File != "" {
		resp.File = &pgErr.File
	}
	if pgErr.Routine != "" {
		resp.Routine = &pgErr.Routine
	}
	if pgErr.Position > 0 {
		resp.Position = new(fmt.Sprintf("%d", pgErr.Position))
	}
	if pgErr.InternalPosition > 0 {
		resp.InternalPosition = new(fmt.Sprintf("%d", pgErr.InternalPosition))
	}
	if pgErr.Line > 0 {
		resp.Line = new(fmt.Sprintf("%d", pgErr.Line))
	}
	return resp
}

// handlePgError maps the error classification to an HTTP response.
func handlePgError(c echo.Context, err error) error {
	errorType := classifyError(err)
	switch errorType {
	case errTypeResponseTooLarge:
		return c.JSON(http.StatusBadRequest, errorResponse("RESPONSE_TOO_LARGE",
			fmt.Sprintf("query result exceeds the %dMB response limit, narrow it with LIMIT/OFFSET or use a direct Postgres connection", maxResponseSizeBytes/(1024*1024))))
	case errTypeQueryTimeout:
		return c.JSON(http.StatusBadRequest, errorResponse("QUERY_TIMEOUT", "query exceeded the timeout limit"))
	case errTypeTimeout:
		return c.JSON(http.StatusGatewayTimeout, errorResponse("GATEWAY_TIMEOUT", "timed out connecting to the database"))
	case errTypeCanceled:
		return nil
	case errTypeHibernated:
		return c.JSON(http.StatusConflict, errorResponse("BRANCH_HIBERNATED", "branch is hibernated, reactivate it to continue"))
	case errTypeBranchNotFound:
		return c.JSON(http.StatusNotFound, errorResponse("BRANCH_NOT_FOUND", "branch not found"))
	case errTypeNoReadReplica:
		return c.JSON(http.StatusBadRequest, errorResponse("NO_READ_REPLICA", "branch has no read replica, use the primary endpoint or add a replica"))
	case errTypePgErrorRequest, errTypePgErrorAccess, errTypePgErrorConflict,
		errTypePgErrorAvailable, errTypePgErrorUpstream, errTypePgErrorInternal:
		var pgErr *pgconn.PgError
		errors.As(err, &pgErr)
		return c.JSON(http.StatusBadRequest, pgErrorResponse(pgErr))
	case errTypeClient:
		return c.JSON(http.StatusBadRequest, errorResponse("08P01", err.Error()))
	case errTypeConnection:
		code := "08006"
		if _, ok := errors.AsType[*connectionEstablishmentError](err); ok {
			code = "08001"
		}
		return c.JSON(http.StatusServiceUnavailable, errorResponse(code, sanitizeError(err)))
	default:
		return c.JSON(http.StatusInternalServerError, errorResponse("XX000", sanitizeError(err)))
	}
}

// isPgxClientError detects parameter binding errors raised by pgx before the
// query reaches PostgreSQL. These are definitively client errors (wrong number
// of parameters, a value that cannot be encoded as the inferred type, etc.)
// that should return 400. pgx wraps encode failures as
// "failed to encode args[N]: ..." in extended_query_builder.go.
func isPgxClientError(err error) bool {
	msg := err.Error()
	return strings.HasPrefix(msg, "expected ") ||
		strings.HasPrefix(msg, "unused argument") ||
		strings.HasPrefix(msg, "failed to encode args[")
}

// classifyError returns a short category string for an error, suitable for
// use as a metric attribute or structured log field.
func classifyError(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, session.ErrBranchHibernated) {
		return errTypeHibernated
	}
	if errors.Is(err, session.ErrBranchNotFound) {
		return errTypeBranchNotFound
	}
	if errors.Is(err, session.ErrNoReadReplica) {
		return errTypeNoReadReplica
	}
	if errors.Is(err, ErrQueryExecTimeout) {
		return errTypeQueryTimeout
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errTypeTimeout
	}
	if errors.Is(err, context.Canceled) {
		return errTypeCanceled
	}
	if errors.Is(err, ErrResponseTooLarge) {
		return errTypeResponseTooLarge
	}
	if _, ok := errors.AsType[*pgconn.PgError](err); ok {
		var pgErr *pgconn.PgError
		errors.As(err, &pgErr)
		return pgErrorClassification(pgErr.Code)
	}
	if isPgxClientError(err) {
		return errTypeClient
	}
	if isConnectionError(err) {
		return errTypeConnection
	}
	return errTypeOther
}

func isPgErrorServerFault(errorType string) bool {
	return errorType == errTypePgErrorAvailable ||
		errorType == errTypePgErrorUpstream ||
		errorType == errTypePgErrorInternal
}

func isConnectionError(err error) bool {
	if _, ok := errors.AsType[*connectionEstablishmentError](err); ok {
		return true
	}
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	if _, ok := errors.AsType[*net.OpError](err); ok {
		return true
	}
	return strings.Contains(err.Error(), "connect:")
}

// sanitizeError strips credentials from error messages to prevent leaking
// passwords in logs or HTTP responses. Connection errors from pgx can contain
// the full connection URI including user:password.
func sanitizeError(err error) string {
	msg := err.Error()
	i := strings.Index(msg, "://")
	if i == -1 {
		return msg
	}
	at := strings.Index(msg[i:], "@")
	if at == -1 {
		return msg
	}
	return msg[:i+3] + "REDACTED" + msg[i+at:]
}
