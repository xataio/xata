package observability

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeLogsBackend struct {
	queries []string
	rows    []LogRow
	err     error
}

func (f *fakeLogsBackend) Query(_ context.Context, query string, _, _ time.Time, _ int) ([]LogRow, error) {
	f.queries = append(f.queries, query)
	return f.rows, f.err
}

// boundedLogsBackend honours the cursor's `_time:<=` clause and the row limit,
// like VictoriaLogs, so the querier's pagination can be exercised end to end.
// The byte-budget tests scale both production limits down by the same factor,
// so the page/message arithmetic they exercise is identical while the messages
// stay kilobytes rather than megabytes.
const (
	budgetScale      = 1024
	testPageBytes    = maxLogPageBytes / budgetScale
	testMessageBytes = maxLogMessageBytes / budgetScale
)

type boundedLogsBackend struct {
	rows []LogRow
}

func (b *boundedLogsBackend) Query(_ context.Context, query string, _, _ time.Time, limit int) ([]LogRow, error) {
	bound, hasBound := time.Time{}, false
	if _, after, ok := strings.Cut(query, "_time:<="); ok {
		ts := after
		if before, _, found := strings.Cut(after, " "); found {
			ts = before
		}
		t, err := time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			return nil, err
		}
		bound, hasBound = t, true
	}

	var filtered []LogRow
	for _, r := range b.rows {
		if hasBound && r.Timestamp.After(bound) {
			continue
		}
		filtered = append(filtered, r)
	}
	// Newest first, like VictoriaLogs; same-timestamp ties keep input order.
	sort.SliceStable(filtered, func(i, j int) bool {
		return filtered[i].Timestamp.After(filtered[j].Timestamp)
	})
	if limit > 0 && len(filtered) > limit {
		filtered = filtered[:limit]
	}
	return filtered, nil
}

func TestBuildLogsQL_AppendsBranchScope(t *testing.T) {
	q, err := buildLogsQL("xata-clusters", "br-1", nil, 0)
	require.NoError(t, err)
	require.Contains(t, q, `kubernetes.namespace_name:="xata-clusters"`)
	require.Contains(t, q, `kubernetes.container_name:="postgres"`)
	require.Contains(t, q, `logger:in ("postgres","pgaudit")`, "scopes to postgres and pgaudit, drops instance-manager / backup")
	require.Contains(t, q, `branch_id:="br-1"`)
	require.NotContains(t, q, "_time:<", "no resume clause when cursor is empty")
}

func TestBuildLogsQL_ResumeCursorClause(t *testing.T) {
	q, err := buildLogsQL("xata-clusters", "br-1", nil, 1730000000000000000)
	require.NoError(t, err)
	require.Contains(t, q, "_time:<=2024-10-27T03:33:20Z")
}

func TestCompileLogFilter(t *testing.T) {
	tests := map[string]struct {
		filter LogFilter
		want   string
	}{
		"instance in": {
			filter: LogFilter{Field: "instance", Op: "in", Values: []string{"br-1-0", "br-1-1"}},
			want:   `kubernetes.pod_name:in ("br-1-0","br-1-1")`,
		},
		"level error expands to postgres severities": {
			filter: LogFilter{Field: "level", Op: "in", Values: []string{"error"}},
			want:   `severity_text:in ("ERROR","FATAL","PANIC","CRITICAL")`,
		},
		"level info expands": {
			filter: LogFilter{Field: "level", Op: "in", Values: []string{"info"}},
			want:   `severity_text:in ("INFO","LOG","NOTICE")`,
		},
		"process in": {
			filter: LogFilter{Field: "process", Op: "in", Values: []string{"client backend"}},
			want:   `backend_type:in ("client backend")`,
		},
		"logger in": {
			filter: LogFilter{Field: "logger", Op: "in", Values: []string{"postgres"}},
			want:   `logger:in ("postgres")`,
		},
		"body contains is a literal substring regex on _msg": {
			filter: LogFilter{Field: "body", Op: "contains", Value: "slow"},
			want:   `_msg:~"slow"`,
		},
		"body icontains prepends the inline case-insensitive flag": {
			filter: LogFilter{Field: "body", Op: "icontains", Value: "checkpoint"},
			want:   `_msg:~"(?i)checkpoint"`,
		},
		"body contains escapes regex metacharacters": {
			filter: LogFilter{Field: "body", Op: "contains", Value: "a.b*c"},
			want:   `_msg:~"a\\.b\\*c"`,
		},
		"body icontains escapes regex metacharacters": {
			filter: LogFilter{Field: "body", Op: "icontains", Value: "1.5s"},
			want:   `_msg:~"(?i)1\\.5s"`,
		},
		"body regex passes the pattern through verbatim": {
			filter: LogFilter{Field: "body", Op: "regex", Value: "^conn.* established$"},
			want:   `_msg:~"^conn.* established$"`,
		},
		"body iregex passes the pattern through with inline flag": {
			filter: LogFilter{Field: "body", Op: "iregex", Value: "^conn"},
			want:   `_msg:~"(?i)^conn"`,
		},
		"value with embedded quotes is escaped for the LogsQL literal": {
			filter: LogFilter{Field: "body", Op: "contains", Value: `say "hi"`},
			want:   `_msg:~"say \"hi\""`,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := compileLogFilter(tt.filter)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCompileLogFilter_Rejected(t *testing.T) {
	tests := map[string]LogFilter{
		"instance only supports in":        {Field: "instance", Op: "contains", Value: "x"},
		"level only supports in":           {Field: "level", Op: "regex", Value: "x"},
		"process only supports in":         {Field: "process", Op: "icontains", Value: "x"},
		"logger only supports in":          {Field: "logger", Op: "contains", Value: "x"},
		"body rejects in":                  {Field: "body", Op: "in", Values: []string{"x"}},
		"body rejects unknown op":          {Field: "body", Op: "eq", Value: "x"},
		"unknown field":                    {Field: "trace_id", Op: "contains", Value: "x"},
		"level rejects unknown level name": {Field: "level", Op: "in", Values: []string{"bogus"}},
	}
	for name, f := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := compileLogFilter(f)
			require.Error(t, err)
		})
	}
}

func TestBuildLogsQL_FullQueryLocksScopeAndFields(t *testing.T) {
	filters := []LogFilter{
		{Field: "instance", Op: "in", Values: []string{"br-1-0"}},
		{Field: "level", Op: "in", Values: []string{"error"}},
		{Field: "process", Op: "in", Values: []string{"checkpointer"}},
		{Field: "body", Op: "icontains", Value: "checkpoint"},
	}
	got, err := buildLogsQL("xata-clusters", "br-1", filters, 1730000000000000000)
	require.NoError(t, err)

	want := `kubernetes.namespace_name:="xata-clusters" AND kubernetes.container_name:="postgres"` +
		` AND logger:in ("postgres","pgaudit")` +
		` AND branch_id:="br-1"` +
		` AND _time:<=2024-10-27T03:33:20Z` +
		` AND kubernetes.pod_name:in ("br-1-0")` +
		` AND severity_text:in ("ERROR","FATAL","PANIC","CRITICAL")` +
		` AND backend_type:in ("checkpointer")` +
		` AND _msg:~"(?i)checkpoint"`
	require.Equal(t, want, got)

	require.NotContains(t, got, "body:", "the message lives in _msg; body: matches nothing in VictoriaLogs")
}

func TestLogsQuerier_DecodesEntriesAndSetsCursor(t *testing.T) {
	t1 := time.Date(2025, 5, 1, 12, 0, 0, 0, time.UTC)
	t2 := t1.Add(-time.Minute)
	backend := &fakeLogsBackend{
		rows: []LogRow{
			{Timestamp: t1, Pod: "br-1-0", Severity: "ERROR", Process: "client backend", Message: `{"record":{"message":"boom"}}`},
			{Timestamp: t2, Pod: "br-1-0", Severity: "INFO", Message: "hello"},
		},
	}
	q := NewLogsQuerier(backend, "xata-clusters")

	res, err := q.Query(context.Background(), "br-1", t2.Add(-time.Hour), t1, nil, 2, "")
	require.NoError(t, err)

	require.Len(t, res.Entries, 2)
	require.Equal(t, "boom", res.Entries[0].Message, "CNPG record body should be unwrapped")
	require.Equal(t, "error", res.Entries[0].Level)
	require.Equal(t, "client backend", res.Entries[0].Process)
	require.Equal(t, "info", res.Entries[1].Level)

	require.NotEmpty(t, res.NextCursor, "cursor should be set when page is full")
	resumeNanos, resumeSeen, err := decodeCursor(res.NextCursor)
	require.NoError(t, err)
	require.Equal(t, t2.UnixNano(), resumeNanos, "cursor anchors at oldest entry")
	require.NotEmpty(t, resumeSeen, "cursor carries the keys returned at the oldest timestamp")
}

func TestLogsQuerier_Query(t *testing.T) {
	base := time.Date(2025, 5, 1, 12, 0, 0, 0, time.UTC)

	// One testMessageBytes message each; the 9th would exceed testPageBytes.
	budgetRows := make([]LogRow, 10)
	for i := range budgetRows {
		budgetRows[i] = LogRow{Timestamp: base.Add(-time.Duration(i) * time.Minute), Pod: "br-1-0", Message: strings.Repeat("x", testMessageBytes)}
	}

	tests := map[string]struct {
		rows        []LogRow
		limit       int
		pageBytes   int
		wantEntries int
		wantCursor  bool
		check       func(t *testing.T, res *LogsResult)
	}{
		"no cursor when partial page": {
			rows:        []LogRow{{Timestamp: base, Pod: "br-1-0", Message: "a"}},
			limit:       100,
			wantEntries: 1,
			wantCursor:  false,
		},
		"stops on byte budget": {
			rows:        budgetRows,
			limit:       1000,
			pageBytes:   testPageBytes,
			wantEntries: 8,
			wantCursor:  true,
			check: func(t *testing.T, res *LogsResult) {
				resumeNanos, _, err := decodeCursor(res.NextCursor)
				require.NoError(t, err)
				require.Equal(t, res.Entries[len(res.Entries)-1].Timestamp.UnixNano(), resumeNanos)
			},
		},
		"truncates oversized message": {
			rows:        []LogRow{{Timestamp: base, Pod: "br-1-0", Message: strings.Repeat("y", maxLogMessageBytes+1024)}},
			limit:       1000,
			wantEntries: 1,
			wantCursor:  false,
			check: func(t *testing.T, res *LogsResult) {
				require.True(t, strings.HasSuffix(res.Entries[0].Message, logTruncationMarker), "oversized message should be truncated")
				require.LessOrEqual(t, len(res.Entries[0].Message), maxLogMessageBytes)
			},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			backend := &fakeLogsBackend{rows: tt.rows}
			q := NewLogsQuerier(backend, "xata-clusters")
			if tt.pageBytes != 0 {
				q.pageBytes = tt.pageBytes
			}

			res, err := q.Query(context.Background(), "br-1", base.Add(-time.Hour), base, nil, tt.limit, "")
			require.NoError(t, err)

			require.Len(t, res.Entries, tt.wantEntries)
			if tt.wantCursor {
				require.NotEmpty(t, res.NextCursor, "an early-stopped or full page must hand back a cursor")
			} else {
				require.Empty(t, res.NextCursor)
			}
			if tt.check != nil {
				tt.check(t, res)
			}
		})
	}
}

func TestLogsQuerier_PaginatesWithoutGaps(t *testing.T) {
	base := time.Date(2025, 5, 1, 12, 0, 0, 0, time.UTC)

	tests := map[string]struct {
		rows  []LogRow
		limit int
	}{
		// Messages just under testMessageBytes, so testPageBytes cuts the page
		// inside the timestamp shared by rows 7..9.
		"collision straddles byte-budget boundary": {
			rows:  collidingRows(base, 12, 7, 9, testMessageBytes-16),
			limit: 1000,
		},
		// Small messages, so the row limit cuts the page inside the timestamp
		// shared by rows 3..5 before the byte budget bites.
		"collision straddles row-limit boundary": {
			rows:  collidingRows(base, 12, 3, 5, 32),
			limit: 4,
		},
		"no collisions": {
			rows:  collidingRows(base, 10, -1, -1, 32),
			limit: 3,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			q := NewLogsQuerier(&boundedLogsBackend{rows: tt.rows}, "xata-clusters")
			q.pageBytes = testPageBytes

			got := map[string]int{}
			cursor := ""
			for range len(tt.rows) + 1 {
				res, err := q.Query(context.Background(), "br-1", base.Add(-time.Hour), base, nil, tt.limit, cursor)
				require.NoError(t, err)
				for _, e := range res.Entries {
					got[e.Message]++
				}
				if res.NextCursor == "" {
					break
				}
				cursor = res.NextCursor
			}

			require.Len(t, got, len(tt.rows), "every row is returned across pages, none dropped at a boundary")
			for msg, n := range got {
				require.Equal(t, 1, n, "row %q returned exactly once, no duplicate at a boundary", msg)
			}
		})
	}
}

func TestLogsQuerier_PaginatesPastFullTimestampGroups(t *testing.T) {
	base := time.Date(2025, 5, 1, 12, 0, 0, 0, time.UTC)

	tests := map[string]struct {
		rows  []LogRow
		limit int
	}{
		// limit 1 refetches and skips the resume timestamp every page, so the
		// cursor must step past it or pagination stalls after the first row.
		"limit 1, distinct timestamps": {rows: collidingRows(base, 6, -1, -1, 32), limit: 1},
		// A collision group exactly the size of the limit fills a page with the
		// resume timestamp's rows — the case that used to stall.
		"collision group equals limit": {rows: collidingRows(base, 6, 1, 2, 32), limit: 2},
		// A collision group larger than the limit can't fit on one page; the
		// surplus rows at that timestamp used to be silently dropped.
		"collision group exceeds limit":        {rows: collidingRows(base, 6, 1, 3, 32), limit: 2},
		"large collision group, small limit":   {rows: collidingRows(base, 9, 1, 6, 32), limit: 2},
		"collision group exceeds limit at one": {rows: collidingRows(base, 6, 1, 3, 32), limit: 1},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			q := NewLogsQuerier(&boundedLogsBackend{rows: tt.rows}, "xata-clusters")

			got := map[string]int{}
			cursor := ""
			for i := 0; ; i++ {
				require.Less(t, i, 4*len(tt.rows)+10, "pagination must terminate, not stall")
				res, err := q.Query(context.Background(), "br-1", base.Add(-time.Hour), base, nil, tt.limit, cursor)
				require.NoError(t, err)
				for _, e := range res.Entries {
					got[e.Message]++
				}
				if res.NextCursor == "" {
					break
				}
				cursor = res.NextCursor
			}

			require.Len(t, got, len(tt.rows), "every row returned, none dropped")
			for msg, n := range got {
				require.Equal(t, 1, n, "row %q returned exactly once", msg)
			}
		})
	}
}

// collidingRows builds count rows newest-first, one per minute, forcing rows
// [collideStart, collideEnd] onto a single timestamp so a page boundary can
// fall inside that group. A negative range disables the collision.
func collidingRows(base time.Time, count, collideStart, collideEnd, msgSize int) []LogRow {
	body := strings.Repeat("x", msgSize)
	rows := make([]LogRow, count)
	for i := range rows {
		ts := base.Add(-time.Duration(i) * time.Minute)
		if i >= collideStart && i <= collideEnd {
			ts = base.Add(-time.Duration(collideStart) * time.Minute)
		}
		// The per-row marker leads the message so rows stay distinct within the
		// prefix rowKey hashes, mirroring real logs where distinct lines diverge
		// early rather than only after a megabyte of identical text.
		rows[i] = LogRow{Timestamp: ts, Pod: "br-1-0", Message: fmt.Sprintf("%02d-%s", i, body)}
	}
	return rows
}

func TestUnwrapCNPGBody(t *testing.T) {
	type cnpgBody struct {
		msg    string
		logger string
	}
	tests := map[string]struct {
		body string
		want cnpgBody
	}{
		"postgres record.message": {
			body: `{"record":{"message":"boom"}}`,
			want: cnpgBody{msg: "boom"},
		},
		"instance-manager msg": {
			body: `{"msg":"lifecycle"}`,
			want: cnpgBody{msg: "lifecycle"},
		},
		"plain text passes through": {
			body: "plain text",
			want: cnpgBody{msg: "plain text"},
		},
		"unrecognised JSON shape passes through": {
			body: `{"foo":"bar"}`,
			want: cnpgBody{msg: `{"foo":"bar"}`},
		},
		"pgaudit SESSION rendered as native CSV with logger stamp": {
			body: `{"logger":"pgaudit","record":{"message":"","audit":{"audit_type":"SESSION","statement_id":"1","substatement_id":"1","class":"READ","command":"SELECT","object_type":"","object_name":"","statement":"select * from accounts","parameter":"<not logged>"}}}`,
			want: cnpgBody{
				msg:    `AUDIT: SESSION,1,1,READ,SELECT,,,select * from accounts,<not logged>`,
				logger: "pgaudit",
			},
		},
		"pgaudit statement with commas is CSV-quoted": {
			body: `{"logger":"pgaudit","record":{"audit":{"audit_type":"SESSION","statement_id":"2","substatement_id":"1","class":"WRITE","command":"INSERT","object_type":"TABLE","object_name":"public.t","statement":"insert into t values (1, 2)","parameter":"<not logged>","rows":"1"}}}`,
			want: cnpgBody{
				msg:    `AUDIT: SESSION,2,1,WRITE,INSERT,TABLE,public.t,"insert into t values (1, 2)",<not logged>,1`,
				logger: "pgaudit",
			},
		},
		"pgaudit envelope with non-map audit falls through to record.message but still stamps logger": {
			body: `{"logger":"pgaudit","record":{"message":"fallback","audit":"not-a-map"}}`,
			want: cnpgBody{msg: "fallback", logger: "pgaudit"},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			msg, logger := unwrapCNPGBody(tt.body)
			got := cnpgBody{msg: msg, logger: logger}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDecodeRow(t *testing.T) {
	row := decodeRow(map[string]any{
		"_time":               "2025-05-01T12:00:00Z",
		"_msg":                "checkpoint complete",
		"kubernetes.pod_name": "br-1-0",
		"severity_text":       "LOG",
		"backend_type":        "checkpointer",
	})
	require.False(t, row.Timestamp.IsZero())
	require.Equal(t, "checkpoint complete", row.Message)
	require.Equal(t, "br-1-0", row.Pod)
	require.Equal(t, "LOG", row.Severity)
	require.Equal(t, "checkpointer", row.Process)
}

func TestExpandLevels(t *testing.T) {
	got := expandLevels([]string{"error", "warning"})
	joined := strings.Join(got, ",")
	require.Contains(t, joined, "ERROR")
	require.Contains(t, joined, "FATAL")
	require.Contains(t, joined, "WARNING")
	require.NotContains(t, joined, "INFO")
}

func TestRedactPassword(t *testing.T) {
	tests := map[string]struct {
		in   string
		want string
	}{
		"alter role password": {
			in:   `statement: ALTER ROLE "xata" LOGIN PASSWORD 'aSR20yzeuGek'`,
			want: `statement: ALTER ROLE "xata" LOGIN PASSWORD <REDACTED>`,
		},
		"create role encrypted password": {
			in:   `statement: CREATE ROLE r LOGIN ENCRYPTED PASSWORD 'topsecret'`,
			want: `statement: CREATE ROLE r LOGIN ENCRYPTED PASSWORD <REDACTED>`,
		},
		"trailing clauses are dropped with the secret": {
			in:   `ALTER ROLE "postgres" WITH PASSWORD 'j21BPewd' VALID UNTIL 'infinity'`,
			want: `ALTER ROLE "postgres" WITH PASSWORD <REDACTED>`,
		},
		"statement split across lines": {
			in:   "statement: ALTER ROLE \"xata\" LOGIN\nPASSWORD 'topsecret'",
			want: "statement: ALTER ROLE \"xata\" LOGIN\nPASSWORD <REDACTED>",
		},
		"user mapping options password": {
			in:   `CREATE USER MAPPING FOR xata SERVER s OPTIONS (user 'u', password 'topsecret')`,
			want: `CREATE USER MAPPING FOR xata SERVER s OPTIONS (user 'u', password <REDACTED>`,
		},
		"password substring in role name is preserved": {
			in:   `CREATE ROLE password_admin LOGIN PASSWORD 'topsecret'`,
			want: `CREATE ROLE password_admin LOGIN PASSWORD <REDACTED>`,
		},
		"non-role statement mentioning password is unchanged": {
			in:   `statement: SELECT * FROM notes WHERE body = 'my password is secret'`,
			want: `statement: SELECT * FROM notes WHERE body = 'my password is secret'`,
		},
		"role set without password is unchanged": {
			in:   `statement: ALTER ROLE postgres SET log_statement = 'all'`,
			want: `statement: ALTER ROLE postgres SET log_statement = 'all'`,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := redactPassword(tc.in)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestQuery_RedactsManagedCredentialFromCNPGRecord(t *testing.T) {
	const secret = "vODquEQTYRrviVSRacwf"
	backend := &fakeLogsBackend{rows: []LogRow{{
		Timestamp: time.Unix(1730000000, 0),
		Pod:       "br-1-1",
		Severity:  "LOG",
		Message:   `{"logger":"postgres","record":{"message":"statement: ALTER ROLE \"xata\" LOGIN PASSWORD '` + secret + `'"}}`,
	}}}
	q := NewLogsQuerier(backend, "xata-clusters")
	res, err := q.Query(context.Background(), "br-1", time.Unix(0, 0), time.Now(), nil, 10, "")
	require.NoError(t, err)
	require.Len(t, res.Entries, 1)
	require.NotContains(t, res.Entries[0].Message, secret, "managed credential must never reach the customer")
	require.Contains(t, res.Entries[0].Message, "PASSWORD <REDACTED>")
}
