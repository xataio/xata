package serverless

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"

	"xata/services/gateway/metrics"
	"xata/services/gateway/serverless/spec"
	"xata/services/gateway/session"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"k8s.io/utils/ptr"
)

func TestParseConnectionString(t *testing.T) {
	tests := map[string]struct {
		input   string
		want    *connectionInfo
		wantErr error
	}{
		"valid postgres URL": {
			input: "postgres://user:pass@host:5432/mydb",
			want:  &connectionInfo{Host: "host", User: "user", Password: "pass", Database: "mydb"},
		},
		"valid postgresql URL": {
			input: "postgresql://user:pass@host:5432/mydb",
			want:  &connectionInfo{Host: "host", User: "user", Password: "pass", Database: "mydb"},
		},
		"valid URL without port": {
			input: "postgres://user:pass@host/mydb",
			want:  &connectionInfo{Host: "host", User: "user", Password: "pass", Database: "mydb"},
		},
		"valid URL with empty database": {
			input: "postgres://user:pass@host/",
			want:  &connectionInfo{Host: "host", User: "user", Password: "pass", Database: ""},
		},
		"empty string": {
			input:   "",
			wantErr: ErrMissingConnectionString,
		},
		"invalid scheme": {
			input:   "mysql://user:pass@host/db",
			wantErr: ErrInvalidConnectionString,
		},
		"missing password": {
			input:   "postgres://user@host/db",
			wantErr: ErrInvalidConnectionString,
		},
		"missing user": {
			input:   "postgres://:pass@host/db",
			wantErr: ErrInvalidConnectionString,
		},
		"missing host": {
			input:   "postgres://user:pass@/db",
			wantErr: ErrInvalidConnectionString,
		},
		"not a URL": {
			input:   "not-a-url",
			wantErr: ErrInvalidConnectionString,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := parseConnectionString(tc.input)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestParseRequest(t *testing.T) {
	tests := map[string]struct {
		input   string
		want    *spec.SQLRequest
		wantErr error
	}{
		"single query": {
			input: `{"query": "SELECT 1"}`,
			want:  &spec.SQLRequest{Query: new("SELECT 1")},
		},
		"single query with params": {
			input: `{"query": "SELECT $1", "params": [42]}`,
			want:  &spec.SQLRequest{Query: new("SELECT $1"), Params: new([]any{float64(42)})},
		},
		"single query with arrayMode true": {
			input: `{"query": "SELECT 1", "arrayMode": true}`,
			want:  &spec.SQLRequest{Query: new("SELECT 1"), ArrayMode: new(true)},
		},
		"single query with arrayMode false": {
			input: `{"query": "SELECT 1", "arrayMode": false}`,
			want:  &spec.SQLRequest{Query: new("SELECT 1"), ArrayMode: new(false)},
		},
		"single query without arrayMode": {
			input: `{"query": "SELECT 1"}`,
			want:  &spec.SQLRequest{Query: new("SELECT 1")},
		},
		"batch with queries key": {
			input: `{"queries": [{"query": "SELECT 1"}, {"query": "SELECT 2"}]}`,
			want: &spec.SQLRequest{
				Queries: new([]spec.QueryItem{
					{Query: "SELECT 1"},
					{Query: "SELECT 2"},
				}),
			},
		},
		"batch as array": {
			input: `[{"query": "SELECT 1"}, {"query": "SELECT 2", "params": [1]}]`,
			want: &spec.SQLRequest{
				Queries: new([]spec.QueryItem{
					{Query: "SELECT 1"},
					{Query: "SELECT 2", Params: new([]any{float64(1)})},
				}),
			},
		},
		"batch with per-query arrayMode": {
			input: `[{"query": "SELECT 1", "arrayMode": true}, {"query": "SELECT 2", "arrayMode": false}]`,
			want: &spec.SQLRequest{
				Queries: new([]spec.QueryItem{
					{Query: "SELECT 1", ArrayMode: new(true)},
					{Query: "SELECT 2", ArrayMode: new(false)},
				}),
			},
		},
		"batch with mixed arrayMode": {
			input: `[{"query": "SELECT 1", "arrayMode": true}, {"query": "SELECT 2"}]`,
			want: &spec.SQLRequest{
				Queries: new([]spec.QueryItem{
					{Query: "SELECT 1", ArrayMode: new(true)},
					{Query: "SELECT 2"},
				}),
			},
		},
		"empty body": {
			input:   "",
			wantErr: ErrInvalidRequest,
		},
		"whitespace only": {
			input:   "   ",
			wantErr: ErrInvalidRequest,
		},
		"invalid JSON": {
			input:   "{invalid",
			wantErr: ErrInvalidRequest,
		},
		"invalid array JSON": {
			input:   "[invalid",
			wantErr: ErrInvalidRequest,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := parseRequest(strings.NewReader(tc.input))
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestValidateRequest(t *testing.T) {
	tests := map[string]struct {
		req     *spec.SQLRequest
		wantErr string
	}{
		"valid single query": {
			req: &spec.SQLRequest{Query: new("SELECT 1")},
		},
		"empty single query": {
			req:     &spec.SQLRequest{Query: new("")},
			wantErr: "missing query",
		},
		"nil query": {
			req:     &spec.SQLRequest{},
			wantErr: "missing query",
		},
		"valid batch": {
			req: &spec.SQLRequest{Queries: new([]spec.QueryItem{{Query: "SELECT 1"}})},
		},
		"empty batch query": {
			req:     &spec.SQLRequest{Queries: new([]spec.QueryItem{{Query: ""}})},
			wantErr: "query at index 0 is empty",
		},
		"too many queries": {
			req: &spec.SQLRequest{Queries: new(func() []spec.QueryItem {
				queries := make([]spec.QueryItem, maxQueriesPerBatch+1)
				for i := range queries {
					queries[i] = spec.QueryItem{Query: "SELECT 1"}
				}
				return queries
			}())},
			wantErr: "too many queries",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := validateRequest(tc.req)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestIsBatch(t *testing.T) {
	tests := map[string]struct {
		req  *spec.SQLRequest
		want bool
	}{
		"single query": {
			req:  &spec.SQLRequest{Query: new("SELECT 1")},
			want: false,
		},
		"batch with queries": {
			req:  &spec.SQLRequest{Queries: new([]spec.QueryItem{{Query: "SELECT 1"}})},
			want: true,
		},
		"empty batch": {
			req:  &spec.SQLRequest{Queries: new([]spec.QueryItem{})},
			want: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := isBatch(tc.req)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestParseCommandTag(t *testing.T) {
	tests := map[string]struct {
		input       string
		wantCommand string
		wantCount   *int
	}{
		"SELECT with count": {
			input:       "SELECT 5",
			wantCommand: "SELECT",
			wantCount:   new(5),
		},
		"INSERT": {
			input:       "INSERT 0 1",
			wantCommand: "INSERT",
			wantCount:   new(1),
		},
		"INSERT multiple rows": {
			input:       "INSERT 0 3",
			wantCommand: "INSERT",
			wantCount:   new(3),
		},
		"UPDATE": {
			input:       "UPDATE 3",
			wantCommand: "UPDATE",
			wantCount:   new(3),
		},
		"DELETE": {
			input:       "DELETE 2",
			wantCommand: "DELETE",
			wantCount:   new(2),
		},
		"command only": {
			input:       "BEGIN",
			wantCommand: "BEGIN",
			wantCount:   nil,
		},
		"empty": {
			input:       "",
			wantCommand: "",
			wantCount:   nil,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			gotCommand, gotCount := parseCommandTag(tc.input)
			require.Equal(t, tc.wantCommand, gotCommand)
			require.Equal(t, tc.wantCount, gotCount)
		})
	}
}

func TestParseTxOptions(t *testing.T) {
	tests := map[string]struct {
		params  spec.QueryParams
		want    pgx.TxOptions
		wantErr string
	}{
		"default options": {
			params: spec.QueryParams{},
			want: pgx.TxOptions{
				IsoLevel:       pgx.ReadCommitted,
				AccessMode:     pgx.ReadWrite,
				DeferrableMode: pgx.NotDeferrable,
			},
		},
		"explicit read committed": {
			params: spec.QueryParams{BatchIsolationLevel: ptr.To(spec.ReadCommitted)},
			want: pgx.TxOptions{
				IsoLevel:       pgx.ReadCommitted,
				AccessMode:     pgx.ReadWrite,
				DeferrableMode: pgx.NotDeferrable,
			},
		},
		"serializable isolation": {
			params: spec.QueryParams{BatchIsolationLevel: ptr.To(spec.Serializable)},
			want: pgx.TxOptions{
				IsoLevel:       pgx.Serializable,
				AccessMode:     pgx.ReadWrite,
				DeferrableMode: pgx.NotDeferrable,
			},
		},
		"repeatable read isolation": {
			params: spec.QueryParams{BatchIsolationLevel: ptr.To(spec.RepeatableRead)},
			want: pgx.TxOptions{
				IsoLevel:       pgx.RepeatableRead,
				AccessMode:     pgx.ReadWrite,
				DeferrableMode: pgx.NotDeferrable,
			},
		},
		"read uncommitted isolation": {
			params: spec.QueryParams{BatchIsolationLevel: ptr.To(spec.ReadUncommitted)},
			want: pgx.TxOptions{
				IsoLevel:       pgx.ReadUncommitted,
				AccessMode:     pgx.ReadWrite,
				DeferrableMode: pgx.NotDeferrable,
			},
		},
		"read only": {
			params: spec.QueryParams{BatchReadOnly: ptr.To(spec.QueryParamsBatchReadOnlyTrue)},
			want: pgx.TxOptions{
				IsoLevel:       pgx.ReadCommitted,
				AccessMode:     pgx.ReadOnly,
				DeferrableMode: pgx.NotDeferrable,
			},
		},
		"deferrable": {
			params: spec.QueryParams{BatchDeferrable: ptr.To(spec.QueryParamsBatchDeferrableTrue)},
			want: pgx.TxOptions{
				IsoLevel:       pgx.ReadCommitted,
				AccessMode:     pgx.ReadWrite,
				DeferrableMode: pgx.Deferrable,
			},
		},
		"all options": {
			params: spec.QueryParams{
				BatchIsolationLevel: ptr.To(spec.Serializable),
				BatchReadOnly:       ptr.To(spec.QueryParamsBatchReadOnlyTrue),
				BatchDeferrable:     ptr.To(spec.QueryParamsBatchDeferrableTrue),
			},
			want: pgx.TxOptions{
				IsoLevel:       pgx.Serializable,
				AccessMode:     pgx.ReadOnly,
				DeferrableMode: pgx.Deferrable,
			},
		},
		"unknown isolation level": {
			params:  spec.QueryParams{BatchIsolationLevel: ptr.To[spec.QueryParamsBatchIsolationLevel]("Snapshot")},
			wantErr: "invalid isolation level: Snapshot",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := parseTxOptions(tc.params)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestQueryOptionsWithArrayMode(t *testing.T) {
	tests := map[string]struct {
		base     queryOptions
		override *bool
		want     queryOptions
	}{
		"nil override keeps base true": {
			base:     queryOptions{arrayMode: true, rawTextOutput: false},
			override: nil,
			want:     queryOptions{arrayMode: true, rawTextOutput: false},
		},
		"nil override keeps base false": {
			base:     queryOptions{arrayMode: false, rawTextOutput: true},
			override: nil,
			want:     queryOptions{arrayMode: false, rawTextOutput: true},
		},
		"override true": {
			base:     queryOptions{arrayMode: false},
			override: new(true),
			want:     queryOptions{arrayMode: true},
		},
		"override false": {
			base:     queryOptions{arrayMode: true},
			override: new(false),
			want:     queryOptions{arrayMode: false},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := tc.base.withArrayMode(tc.override)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestErrorResponseJSON(t *testing.T) {
	tests := map[string]struct {
		code        string
		message     string
		wantCode    string
		wantMessage string
		absentKeys  []string
	}{
		"basic error response": {
			code:        "TEST_CODE",
			message:     "test message",
			wantCode:    "TEST_CODE",
			wantMessage: "test message",
			absentKeys: []string{
				"severity", "detail", "hint", "position", "internalPosition",
				"internalQuery", "where", "schema", "table", "column",
				"dataType", "constraint", "file", "line", "routine",
			},
		},
		"BAD_REQUEST error": {
			code:        "BAD_REQUEST",
			message:     "something went wrong",
			wantCode:    "BAD_REQUEST",
			wantMessage: "something went wrong",
			absentKeys: []string{
				"severity", "detail", "hint", "position", "internalPosition",
				"internalQuery", "where", "schema", "table", "column",
				"dataType", "constraint", "file", "line", "routine",
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			resp := errorResponse(tc.code, tc.message)
			require.Equal(t, tc.wantCode, *resp.Code)
			require.Equal(t, tc.wantMessage, resp.Message)

			data, err := json.Marshal(resp)
			require.NoError(t, err)

			var m map[string]any
			require.NoError(t, json.Unmarshal(data, &m))
			require.Equal(t, tc.wantMessage, m["message"])
			require.Equal(t, tc.wantCode, m["code"])

			for _, field := range tc.absentKeys {
				require.NotContains(t, m, field, "unset field %q must be omitted from JSON", field)
			}
		})
	}
}

func TestPgErrorResponseJSON(t *testing.T) {
	tests := map[string]struct {
		pgErr      *pgconn.PgError
		wantKeys   map[string]any
		absentKeys []string
	}{
		"minimal pg error": {
			pgErr: &pgconn.PgError{
				Code:    "42601",
				Message: "syntax error",
			},
			wantKeys: map[string]any{
				"message": "syntax error",
				"code":    "42601",
			},
			absentKeys: []string{"severity", "detail", "hint", "position"},
		},
		"pg error with all fields": {
			pgErr: &pgconn.PgError{
				Severity:         "ERROR",
				Code:             "23505",
				Message:          "duplicate key",
				Detail:           "Key (id)=(1) already exists.",
				Hint:             "try a different id",
				Position:         10,
				InternalPosition: 5,
				InternalQuery:    "SELECT ...",
				Where:            "trigger",
				SchemaName:       "public",
				TableName:        "users",
				ColumnName:       "id",
				DataTypeName:     "integer",
				ConstraintName:   "users_pkey",
				File:             "nbtinsert.c",
				Line:             666,
				Routine:          "_bt_check_unique",
			},
			wantKeys: map[string]any{
				"message":          "duplicate key",
				"code":             "23505",
				"severity":         "ERROR",
				"detail":           "Key (id)=(1) already exists.",
				"hint":             "try a different id",
				"position":         "10",
				"internalPosition": "5",
				"internalQuery":    "SELECT ...",
				"where":            "trigger",
				"schema":           "public",
				"table":            "users",
				"column":           "id",
				"dataType":         "integer",
				"constraint":       "users_pkey",
				"file":             "nbtinsert.c",
				"line":             "666",
				"routine":          "_bt_check_unique",
			},
			absentKeys: nil,
		},
		"pg error with empty code omits code": {
			pgErr: &pgconn.PgError{
				Message: "unknown error",
			},
			wantKeys: map[string]any{
				"message": "unknown error",
			},
			absentKeys: []string{"code", "severity", "detail"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			resp := pgErrorResponse(tc.pgErr)
			data, err := json.Marshal(resp)
			require.NoError(t, err)

			var m map[string]any
			require.NoError(t, json.Unmarshal(data, &m))

			for key, want := range tc.wantKeys {
				require.Equal(t, want, m[key], "key %q", key)
			}
			for _, key := range tc.absentKeys {
				require.NotContains(t, m, key, "unset field %q must be omitted from JSON", key)
			}
		})
	}
}

func TestIsPgxClientError(t *testing.T) {
	tests := map[string]struct {
		msg  string
		want bool
	}{
		"unused argument": {
			msg:  "unused argument: 1",
			want: true,
		},
		"expected arguments": {
			msg:  "expected 1 arguments, got 2",
			want: true,
		},
		"connection error": {
			msg:  "connection refused",
			want: false,
		},
		"internal error": {
			msg:  "internal server error",
			want: false,
		},
		"contains expected but not prefix": {
			msg:  "something expected to work",
			want: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := isPgxClientError(errors.New(tc.msg))
			require.Equal(t, tc.want, got)
		})
	}
}

func TestHandlePgError(t *testing.T) {
	tests := map[string]struct {
		err            error
		wantStatusCode int
		wantCode       string
	}{
		"pgx client error - unused argument": {
			err:            errors.New("unused argument: 1"),
			wantStatusCode: http.StatusBadRequest,
			wantCode:       "08P01",
		},
		"pgx client error - expected arguments": {
			err:            errors.New("expected 1 arguments, got 2"),
			wantStatusCode: http.StatusBadRequest,
			wantCode:       "08P01",
		},
		"unknown error - connection refused": {
			err:            errors.New("connection refused"),
			wantStatusCode: http.StatusInternalServerError,
			wantCode:       "XX000",
		},
		"response too large": {
			err:            ErrResponseTooLarge,
			wantStatusCode: http.StatusInsufficientStorage,
			wantCode:       "RESPONSE_TOO_LARGE",
		},
		"plain error": {
			err:            errors.New("begin transaction: connection reset"),
			wantStatusCode: http.StatusInternalServerError,
			wantCode:       "XX000",
		},
		"PgError - syntax error": {
			err:            &pgconn.PgError{Code: "42601", Message: "syntax error"},
			wantStatusCode: http.StatusBadRequest,
			wantCode:       "42601",
		},
		"PgError - server class code": {
			err:            &pgconn.PgError{Code: "53200", Message: "out of memory"},
			wantStatusCode: http.StatusBadRequest,
			wantCode:       "53200",
		},
		"wrapped PgError": {
			err:            fmt.Errorf("begin transaction: %w", &pgconn.PgError{Code: "53200", Message: "out of memory"}),
			wantStatusCode: http.StatusBadRequest,
			wantCode:       "53200",
		},
		"deadline exceeded": {
			err:            fmt.Errorf("connect: %w", context.DeadlineExceeded),
			wantStatusCode: http.StatusGatewayTimeout,
			wantCode:       "QUERY_TIMEOUT",
		},
		"bare deadline exceeded": {
			err:            context.DeadlineExceeded,
			wantStatusCode: http.StatusGatewayTimeout,
			wantCode:       "QUERY_TIMEOUT",
		},
		"branch hibernated": {
			err:            session.ErrBranchHibernated,
			wantStatusCode: http.StatusConflict,
			wantCode:       "BRANCH_HIBERNATED",
		},
		"wrapped branch hibernated": {
			err:            fmt.Errorf("connect: %w", session.ErrBranchHibernated),
			wantStatusCode: http.StatusConflict,
			wantCode:       "BRANCH_HIBERNATED",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := handlePgError(c, tc.err)
			require.NoError(t, err)

			require.Equal(t, tc.wantStatusCode, rec.Code)

			var resp spec.ErrorResponse
			err = json.Unmarshal(rec.Body.Bytes(), &resp)
			require.NoError(t, err)
			require.NotNil(t, resp.Code)
			require.Equal(t, tc.wantCode, *resp.Code)
		})
	}
}

func TestHandlePgError_Canceled(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := handlePgError(c, context.Canceled)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, rec.Code) // no JSON written, default 200
	require.Empty(t, rec.Body.String())
}

func TestClassifyError(t *testing.T) {
	tests := map[string]struct {
		err  error
		want string
	}{
		"nil": {
			err:  nil,
			want: "",
		},
		"deadline exceeded": {
			err:  context.DeadlineExceeded,
			want: "timeout",
		},
		"wrapped deadline exceeded": {
			err:  fmt.Errorf("connect: %w", context.DeadlineExceeded),
			want: "timeout",
		},
		"canceled": {
			err:  context.Canceled,
			want: "canceled",
		},
		"response too large": {
			err:  ErrResponseTooLarge,
			want: "response_too_large",
		},
		"pg error": {
			err:  &pgconn.PgError{Code: "42601", Message: "syntax error"},
			want: "pg_error",
		},
		"wrapped pg error": {
			err:  fmt.Errorf("query: %w", &pgconn.PgError{Code: "42601"}),
			want: "pg_error",
		},
		"client error - unused argument": {
			err:  errors.New("unused argument: 1"),
			want: "client",
		},
		"client error - expected arguments": {
			err:  errors.New("expected 1 arguments, got 2"),
			want: "client",
		},
		"connection refused": {
			err:  fmt.Errorf("connect: %w", syscall.ECONNREFUSED),
			want: "connection",
		},
		"connection reset": {
			err:  fmt.Errorf("read: %w", syscall.ECONNRESET),
			want: "connection",
		},
		"net.OpError": {
			err:  &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("refused")},
			want: "connection",
		},
		"connect prefix in message": {
			err:  errors.New("connect: connection refused"),
			want: "connection",
		},
		"branch hibernated": {
			err:  session.ErrBranchHibernated,
			want: "hibernated",
		},
		"wrapped branch hibernated": {
			err:  fmt.Errorf("connect: %w", session.ErrBranchHibernated),
			want: "hibernated",
		},
		"other error": {
			err:  errors.New("something unexpected"),
			want: "other",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := classifyError(tc.err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestSanitizeError(t *testing.T) {
	tests := map[string]struct {
		input string
		want  string
	}{
		"no URI": {
			input: "connection refused",
			want:  "connection refused",
		},
		"postgres URI with credentials": {
			input: "failed to connect to `host=localhost user=admin password=secret database=mydb`: postgresql://admin:secret@localhost:5432/mydb connection refused",
			want:  "failed to connect to `host=localhost user=admin password=secret database=mydb`: postgresql://REDACTED@localhost:5432/mydb connection refused",
		},
		"URI without at sign": {
			input: "invalid uri: postgres://localhost",
			want:  "invalid uri: postgres://localhost",
		},
		"error with no credentials": {
			input: "syntax error at position 5",
			want:  "syntax error at position 5",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := sanitizeError(errors.New(tc.input))
			require.Equal(t, tc.want, got)
		})
	}
}

func TestNormalizeHeadersMiddleware(t *testing.T) {
	tests := map[string]struct {
		headers    map[string]string
		wantHeader string
		wantValue  string
	}{
		"bare header passes through": {
			headers:    map[string]string{"Connection-String": "postgres://u:p@h/d"},
			wantHeader: "Connection-String",
			wantValue:  "postgres://u:p@h/d",
		},
		"Xata-prefixed header normalized": {
			headers:    map[string]string{"Xata-Connection-String": "postgres://u:p@h/d"},
			wantHeader: "Connection-String",
			wantValue:  "postgres://u:p@h/d",
		},
		"Neon-prefixed header normalized": {
			headers:    map[string]string{"Neon-Connection-String": "postgres://u:p@h/d"},
			wantHeader: "Connection-String",
			wantValue:  "postgres://u:p@h/d",
		},
		"bare header takes precedence over prefixed": {
			headers:    map[string]string{"Connection-String": "bare-val", "Xata-Connection-String": "prefixed-val"},
			wantHeader: "Connection-String",
			wantValue:  "bare-val",
		},
		"Xata-prefixed array mode": {
			headers:    map[string]string{"Xata-Array-Mode": "true"},
			wantHeader: "Array-Mode",
			wantValue:  "true",
		},
		"Xata-prefixed batch isolation level": {
			headers:    map[string]string{"Xata-Batch-Isolation-Level": "Serializable"},
			wantHeader: "Batch-Isolation-Level",
			wantValue:  "Serializable",
		},
		"Neon-prefixed raw text output": {
			headers:    map[string]string{"Neon-Raw-Text-Output": "true"},
			wantHeader: "Raw-Text-Output",
			wantValue:  "true",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			rec := httptest.NewRecorder()

			handler := normalizeHeadersMiddleware(func(c echo.Context) error {
				return c.String(http.StatusOK, c.Request().Header.Get(tc.wantHeader))
			})

			c := e.NewContext(req, rec)
			err := handler(c)
			require.NoError(t, err)
			require.Equal(t, tc.wantValue, rec.Body.String())
		})
	}
}

func TestQuery_IPFilter(t *testing.T) {
	tracer := tracenoop.NewTracerProvider().Tracer("")
	gwMetrics, err := metrics.New(noop.NewMeterProvider().Meter(""))
	require.NoError(t, err)

	resolver := session.ResolverFunc(func(_ context.Context, _ string) (*session.Branch, error) {
		return &session.Branch{ID: "test-branch", Address: "127.0.0.1:5432"}, nil
	})

	// Stub dialer: always fails so we can verify that connection errors are
	// surfaced as HTTP 500 without needing a real Postgres backend.
	dialer := session.NewClusterDialer(
		session.ClusterDialerConfiguration{
			ReactivateTimeout:   time.Second,
			StatusCheckInterval: 100 * time.Millisecond,
		},
		session.WithDialer(func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("stub dialer: no backend")
		}),
	)

	connStr := "postgres://user:pass@host/db"

	tests := map[string]struct {
		filter     session.IPFilter
		wantStatus int
	}{
		"denied": {
			filter:     &mockIPFilter{allowed: false},
			wantStatus: http.StatusForbidden,
		},
		"nil filter allows": {
			filter:     nil,
			wantStatus: http.StatusInternalServerError, // allowed through filter, fails at connect
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			h := &handler{
				resolver: resolver,
				dialer:   dialer,
				tracer:   tracer,
				metrics:  gwMetrics,
				ipFilter: tc.filter,
			}

			e := echo.New()
			body := strings.NewReader(`{"query": "SELECT 1"}`)
			req := httptest.NewRequest(http.MethodPost, "/sql", body)
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Query(c, spec.QueryParams{ConnectionString: new(connStr)})
			require.NoError(t, err)
			require.Equal(t, tc.wantStatus, rec.Code)

			if tc.wantStatus == http.StatusForbidden {
				var resp spec.ErrorResponse
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				require.Equal(t, "FORBIDDEN", *resp.Code)
			}
		})
	}
}

func TestConnect(t *testing.T) {
	const branchAddr = "branch-test-rw.xata-clusters.svc.cluster.local:5432"
	info := &connectionInfo{User: "bob", Password: "hunter2", Database: "mydb"}

	t.Run("dials exactly once via ClusterDialer", func(t *testing.T) {
		var dialed []string
		dialer := session.NewClusterDialer(
			session.ClusterDialerConfiguration{
				ReactivateTimeout:   time.Second,
				StatusCheckInterval: 100 * time.Millisecond,
			},
			session.WithDialer(func(_ context.Context, _, address string) (net.Conn, error) {
				dialed = append(dialed, address)
				return nil, errors.New("stub: no backend")
			}),
		)
		h := &handler{dialer: dialer}

		_, err := h.connect(context.Background(), &session.Branch{ID: "b", Address: branchAddr}, info)
		require.Error(t, err)
		// Regression: pgx used to call DialFunc once per IP a default
		// "localhost" host resolved to. The dialer must see exactly one
		// attempt per connect, targeting branch.Address.
		require.Equal(t, []string{branchAddr}, dialed)
	})

	errCases := map[string]struct{ address, wantErr string }{
		"missing port":      {"host-only", "split branch address"},
		"non-numeric port":  {"host:abc", "parse branch port"},
		"port out of range": {"host:70000", "parse branch port"},
	}
	for name, tc := range errCases {
		t.Run(name, func(t *testing.T) {
			h := &handler{dialer: session.NewClusterDialer(session.ClusterDialerConfiguration{})}
			_, err := h.connect(context.Background(), &session.Branch{ID: "b", Address: tc.address}, info)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestNewServerlessConfig(t *testing.T) {
	var dialCalls int
	params := pgxConnParams{
		Host: "pg.internal", Port: 5433,
		User: "alice", Password: "hunter2", Database: "mydb",
		RuntimeParams: map[string]string{"application_name": "gateway-test"},
		DialFunc: func(context.Context, string, string) (net.Conn, error) {
			dialCalls++
			return nil, errors.New("stub")
		},
	}

	cfg, err := newServerlessConfig(params)
	require.NoError(t, err)

	require.Equal(t, "pg.internal", cfg.Host)
	require.Equal(t, uint16(5433), cfg.Port)
	require.Equal(t, "alice", cfg.User)
	require.Equal(t, "hunter2", cfg.Password)
	require.Equal(t, "mydb", cfg.Database)
	require.Equal(t, params.RuntimeParams, cfg.RuntimeParams)

	// Invariants that guard against reintroducing the multi-dial bug.
	require.Empty(t, cfg.Fallbacks)
	require.NotNil(t, cfg.LookupFunc)
	ips, err := cfg.LookupFunc(context.Background(), "ignored.invalid")
	require.NoError(t, err)
	require.NotEmpty(t, ips)

	_, _ = cfg.DialFunc(context.Background(), "tcp", "ignored:5432")
	require.Equal(t, 1, dialCalls)

	t.Run("zero Host/Port keep sentinel defaults", func(t *testing.T) {
		cfg, err := newServerlessConfig(pgxConnParams{
			User: "u", Database: "d",
			DialFunc: func(context.Context, string, string) (net.Conn, error) { return nil, errors.New("stub") },
		})
		require.NoError(t, err)
		require.Equal(t, "unused", cfg.Host)
		require.Equal(t, uint16(5432), cfg.Port)
	})
}
