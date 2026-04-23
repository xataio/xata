package serverless

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"xata/services/gateway/metrics"
	"xata/services/gateway/serverless/spec"
	"xata/services/gateway/session"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/utils/ptr"
)

const (
	maxRequestSize = "64M"

	maxQueriesPerBatch   = 100
	queryTimeout         = 30 * time.Second
	maxResponseSizeBytes = 10 * 1024 * 1024 // 10MB response limit
)

func (h *handler) Query(c echo.Context, params spec.QueryParams) error {
	ctx := c.Request().Context()

	connInfo, err := parseConnectionString(ptr.Deref(params.ConnectionString, ""))
	if err != nil {
		return c.JSON(http.StatusUnauthorized, errorResponse("UNAUTHORIZED", err.Error()))
	}

	req, err := parseRequest(c.Request().Body)
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BAD_REQUEST", err.Error()))
	}

	if err := validateRequest(req); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BAD_REQUEST", err.Error()))
	}

	ctx, span := h.tracer.Start(ctx, "sql_query",
		trace.WithAttributes(
			metrics.AttrHost.String(connInfo.Host),
			metrics.AttrDatabase.String(connInfo.Database),
			metrics.AttrBatch.Bool(isBatch(req)),
		))
	defer span.End()

	branch, err := h.resolver.Resolve(ctx, connInfo.Host)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Str("host", connInfo.Host).Msg("resolve branch")
		span.RecordError(err)
		return c.JSON(http.StatusBadRequest, errorResponse("INVALID_HOST", err.Error()))
	}

	if err := session.CheckIPAllowed(h.ipFilter, branch.ID, c.Request().RemoteAddr); err != nil {
		return c.JSON(http.StatusForbidden, errorResponse("FORBIDDEN", "forbidden"))
	}

	span.SetAttributes(metrics.AttrBranchID.String(branch.ID))

	start := time.Now()

	opts := queryOptions{
		arrayMode:     ptr.Deref(params.ArrayMode, "") == spec.QueryParamsArrayModeTrue,
		rawTextOutput: ptr.Deref(params.RawTextOutput, "") == spec.QueryParamsRawTextOutputTrue,
	}

	var response any
	if isBatch(req) {
		txOpts, txErr := parseTxOptions(params)
		if txErr != nil {
			return c.JSON(http.StatusBadRequest, errorResponse("BAD_REQUEST", txErr.Error()))
		}
		response, err = h.executeBatch(ctx, branch, connInfo, *req.Queries, opts, txOpts)
	} else {
		response, err = h.executeQuery(ctx, branch, connInfo,
			ptr.Deref(req.Query, ""),
			ptr.Deref(req.Params, nil),
			opts.withArrayMode(req.ArrayMode))
	}

	errorType := classifyError(err)
	h.metrics.RecordRequest(ctx, metrics.ProtocolHTTP, err == nil, time.Since(start),
		metrics.AttrBranchID.String(branch.ID),
		metrics.AttrErrorType.String(errorType))

	if err != nil {
		log.Ctx(ctx).Error().
			Str("error", sanitizeError(err)).
			Str("error_type", errorType).
			Str("branch_id", branch.ID).
			Dur("duration", time.Since(start)).
			Msg("execute query")
		return handlePgError(c, err)
	}

	return c.JSON(http.StatusOK, response)
}

// pgxConnParams carries the per-request fields for newServerlessConfig.
// Host/Port are optional — leave them zero when DialFunc ignores the target.
type pgxConnParams struct {
	Host, User, Password, Database string
	Port                           uint16
	RuntimeParams                  map[string]string
	DialFunc                       pgconn.DialFunc
}

// newServerlessConfig returns a pgx ConnConfig wired for a custom DialFunc.
// pgx's built-in resolver and host fallbacks are disabled because every
// serverless handler delegates network I/O to its own DialFunc — otherwise a
// host like "localhost" (which pgx.ParseConfig("") falls through to on a
// scratch image) would cause one DialFunc call per resolved IP.
func newServerlessConfig(p pgxConnParams) (*pgx.ConnConfig, error) {
	cfg, err := pgx.ParseConfig("host=unused sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	cfg.Fallbacks = nil
	cfg.LookupFunc = func(context.Context, string) ([]string, error) {
		return []string{"127.0.0.1"}, nil // dummy — DialFunc ignores it
	}

	if p.Host != "" {
		cfg.Host = p.Host
	}
	if p.Port != 0 {
		cfg.Port = p.Port
	}
	cfg.User = p.User
	cfg.Password = p.Password
	cfg.Database = p.Database
	if p.RuntimeParams != nil {
		cfg.RuntimeParams = p.RuntimeParams
	}
	cfg.DialFunc = p.DialFunc

	return cfg, nil
}

// connect opens a pgx connection to branch via the handler's ClusterDialer.
func (h *handler) connect(ctx context.Context, branch *session.Branch, connInfo *connectionInfo) (*pgx.Conn, error) {
	host, portStr, err := net.SplitHostPort(branch.Address)
	if err != nil {
		return nil, fmt.Errorf("split branch address %q: %w", branch.Address, err)
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return nil, fmt.Errorf("parse branch port %q: %w", portStr, err)
	}

	cfg, err := newServerlessConfig(pgxConnParams{
		Host:     host,
		Port:     uint16(port),
		User:     connInfo.User,
		Password: connInfo.Password,
		Database: connInfo.Database,
		DialFunc: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return h.dialer.Dial(ctx, "tcp", branch)
		},
	})
	if err != nil {
		return nil, err
	}
	return pgx.ConnectConfig(ctx, cfg)
}

type queryOptions struct {
	arrayMode     bool
	rawTextOutput bool
}

func (o queryOptions) withArrayMode(override *bool) queryOptions {
	if override != nil {
		o.arrayMode = *override
	}
	return o
}

var textResultFormats = pgx.QueryResultFormats{pgx.TextFormatCode}

func queryArgs(params []any) []any {
	return append([]any{textResultFormats}, convertParams(params)...)
}

func (h *handler) connectTraced(ctx context.Context, branch *session.Branch, connInfo *connectionInfo) (*pgx.Conn, error) {
	ctx, span := h.tracer.Start(ctx, "sql_connect")
	defer span.End()

	conn, err := h.connect(ctx, branch, connInfo)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	return conn, nil
}

func (h *handler) executeQuery(ctx context.Context, branch *session.Branch, connInfo *connectionInfo, query string, params []any, opts queryOptions) (*spec.QueryResult, error) {
	conn, err := h.connectTraced(ctx, branch, connInfo)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())

	execCtx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	execCtx, execSpan := h.tracer.Start(execCtx, "sql_execute")
	defer execSpan.End()

	rows, err := conn.Query(execCtx, query, queryArgs(params)...)
	if err != nil {
		return nil, err
	}
	return processRows(rows, opts)
}

func (h *handler) executeBatch(ctx context.Context, branch *session.Branch, connInfo *connectionInfo, queries []spec.QueryItem, opts queryOptions, txOpts pgx.TxOptions) (*spec.BatchResponse, error) {
	conn, err := h.connectTraced(ctx, branch, connInfo)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())

	execCtx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	_, execSpan := h.tracer.Start(execCtx, "sql_execute")
	defer execSpan.End()

	tx, err := conn.BeginTx(execCtx, txOpts)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	results := make([]spec.QueryResult, 0, len(queries))
	for _, q := range queries {
		rows, err := tx.Query(execCtx, q.Query, queryArgs(ptr.Deref(q.Params, nil))...)
		if err != nil {
			return nil, err
		}
		result, err := processRows(rows, opts.withArrayMode(q.ArrayMode))
		if err != nil {
			return nil, err
		}
		results = append(results, *result)
	}

	if err := tx.Commit(execCtx); err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
	}

	return &spec.BatchResponse{Results: results}, nil
}

func parseRequest(body io.Reader) (*spec.SQLRequest, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, ErrInvalidRequest
	}

	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil, ErrInvalidRequest
	}

	// Check if it's an array (batch as array format per serverless driver spec)
	if data[0] == '[' {
		var queries []spec.QueryItem
		if err := json.Unmarshal(data, &queries); err != nil {
			return nil, ErrInvalidRequest
		}
		return &spec.SQLRequest{Queries: &queries}, nil
	}

	var req spec.SQLRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, ErrInvalidRequest
	}
	return &req, nil
}

func isBatch(r *spec.SQLRequest) bool { return r.Queries != nil }

func validateRequest(r *spec.SQLRequest) error {
	if isBatch(r) {
		if len(*r.Queries) > maxQueriesPerBatch {
			return fmt.Errorf("too many queries: maximum %d allowed", maxQueriesPerBatch)
		}
		for i, q := range *r.Queries {
			if q.Query == "" {
				return fmt.Errorf("query at index %d is empty", i)
			}
		}
		return nil
	}
	if r.Query == nil || *r.Query == "" {
		return ErrMissingQuery
	}
	return nil
}

type connectionInfo struct {
	Host, User, Password, Database string
}

func parseConnectionString(connStr string) (*connectionInfo, error) {
	if connStr == "" {
		return nil, ErrMissingConnectionString
	}
	u, err := url.Parse(connStr)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		return nil, ErrInvalidConnectionString
	}
	if u.User == nil {
		return nil, ErrInvalidConnectionString
	}
	user := u.User.Username()
	password, hasPassword := u.User.Password()
	database := strings.TrimPrefix(u.Path, "/")
	if user == "" || !hasPassword || password == "" || u.Host == "" {
		return nil, ErrInvalidConnectionString
	}
	return &connectionInfo{Host: u.Hostname(), User: user, Password: password, Database: database}, nil
}

func parseTxOptions(params spec.QueryParams) (pgx.TxOptions, error) {
	var isoLevel pgx.TxIsoLevel
	switch ptr.Deref(params.BatchIsolationLevel, spec.ReadCommitted) {
	case spec.ReadCommitted:
		isoLevel = pgx.ReadCommitted
	case spec.ReadUncommitted:
		isoLevel = pgx.ReadUncommitted
	case spec.RepeatableRead:
		isoLevel = pgx.RepeatableRead
	case spec.Serializable:
		isoLevel = pgx.Serializable
	default:
		return pgx.TxOptions{}, fmt.Errorf("invalid isolation level: %s", *params.BatchIsolationLevel)
	}

	accessMode := pgx.ReadWrite
	if ptr.Deref(params.BatchReadOnly, "") == spec.QueryParamsBatchReadOnlyTrue {
		accessMode = pgx.ReadOnly
	}

	deferrable := pgx.NotDeferrable
	if ptr.Deref(params.BatchDeferrable, "") == spec.QueryParamsBatchDeferrableTrue {
		deferrable = pgx.Deferrable
	}

	return pgx.TxOptions{IsoLevel: isoLevel, AccessMode: accessMode, DeferrableMode: deferrable}, nil
}

func processRows(rows pgx.Rows, opts queryOptions) (*spec.QueryResult, error) {
	defer rows.Close()

	fields := rows.FieldDescriptions()
	fieldDefs := make([]spec.FieldDefinition, len(fields))
	for i, f := range fields {
		fieldDefs[i] = spec.FieldDefinition{
			Name:             f.Name,
			TableID:          f.TableOID,
			ColumnID:         uint16(f.TableAttributeNumber),
			DataTypeID:       f.DataTypeOID,
			DataTypeSize:     f.DataTypeSize,
			DataTypeModifier: f.TypeModifier,
			Format:           "text",
		}
	}

	resultRows := make([]any, 0)
	responseSize := 0
	for rows.Next() {
		rawValues := rows.RawValues()
		values := make([]any, len(rawValues))
		for i, rv := range rawValues {
			if opts.rawTextOutput {
				if rv == nil {
					values[i] = nil
				} else {
					values[i] = string(rv)
				}
			} else {
				values[i] = convertTextValue(rv, fields[i].DataTypeOID)
			}
		}

		var entry any
		if opts.arrayMode {
			entry = values
		} else {
			row := make(map[string]any)
			for i, f := range fields {
				row[f.Name] = values[i]
			}
			entry = row
		}
		resultRows = append(resultRows, entry)

		if rowData, err := json.Marshal(entry); err == nil {
			responseSize += len(rowData)
		}
		if responseSize > maxResponseSizeBytes {
			return nil, ErrResponseTooLarge
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	command, rowCount := parseCommandTag(rows.CommandTag().String())

	return &spec.QueryResult{
		Fields:     fieldDefs,
		Command:    command,
		RowCount:   rowCount,
		Rows:       resultRows,
		RowAsArray: opts.arrayMode,
	}, nil
}

// parseCommandTag extracts the command name and affected row count from a PostgreSQL command tag.
// Command tag formats:
// - "SELECT 5" -> command="SELECT", count=5
// - "INSERT 0 3" -> command="INSERT", count=3 (OID is skipped)
// - "UPDATE 3" -> command="UPDATE", count=3
// - "DELETE 2" -> command="DELETE", count=2
func parseCommandTag(tag string) (command string, rowCount *int) {
	parts := strings.Split(tag, " ")
	if len(parts) == 0 {
		return "", nil
	}

	command = parts[0]

	// INSERT has format "INSERT oid count", others have "COMMAND count"
	var countStr string
	if command == "INSERT" && len(parts) >= 3 {
		countStr = parts[2]
	} else if len(parts) >= 2 {
		countStr = parts[1]
	}

	if countStr != "" {
		if n, err := strconv.Atoi(countStr); err == nil {
			rowCount = &n
		}
	}

	return command, rowCount
}
