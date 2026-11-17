package observability

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"xata/internal/grpc/pagination"
)

const (
	// maxLogPageBytes bounds a page by message bytes, kept under the gRPC
	// receive cap; the rest is paginated.
	maxLogPageBytes = 8 * 1024 * 1024
	// maxLogMessageBytes mirrors VictoriaLogs' per-line cap (insert.maxLineSizeBytes
	// in cell-observability), so this only bites if that upstream cap changes.
	maxLogMessageBytes = 1024 * 1024

	logTruncationMarker = "… [truncated]"
)

// LogFilter mirrors the user-facing filter shape passed across the gRPC
// boundary. It's intentionally a copy of clustersv1.LogFilter so this package
// remains transport-agnostic.
type LogFilter struct {
	Field  string
	Op     string
	Values []string
	Value  string
}

// LogsBackend is the minimum surface LogsQuerier needs from VictoriaLogs.
type LogsBackend interface {
	Query(ctx context.Context, lqlQuery string, start, end time.Time, limit int) ([]LogRow, error)
}

// LogRow is one parsed entry from VictoriaLogs.
type LogRow struct {
	Timestamp time.Time
	Pod       string
	Severity  string
	Process   string
	Message   string
}

// LogEntry is the cleaned-up entry returned to the RPC.
type LogEntry struct {
	Timestamp  time.Time
	InstanceID string
	Level      string
	Message    string
	Process    string
}

// LogsResult contains everything the RPC needs to serialize.
type LogsResult struct {
	Entries    []LogEntry
	NextCursor string
}

// LogsQuerier resolves a branch logs query into a single LogsQL expression,
// runs it, and decodes the result. The branch-scope predicate on branch_id
// is always added server-side as defense in depth.
type LogsQuerier struct {
	backend   LogsBackend
	namespace string
	// pageBytes bounds a page by message bytes. Tests scale it down so they can
	// cross the budget without building megabyte messages to do it.
	pageBytes int
}

func NewLogsQuerier(backend LogsBackend, namespace string) *LogsQuerier {
	return &LogsQuerier{backend: backend, namespace: namespace, pageBytes: maxLogPageBytes}
}

// schemaLevelToSeverities is the user-facing → CNPG/Postgres severity mapping.
var schemaLevelToSeverities = map[string][]string{
	"debug":   {"DEBUG", "DEBUG1", "DEBUG2", "DEBUG3", "DEBUG4", "DEBUG5"},
	"info":    {"INFO", "LOG", "NOTICE"},
	"warning": {"WARN", "WARNING"},
	"error":   {"ERROR", "FATAL", "PANIC", "CRITICAL"},
}

var severityToLevel = func() map[string]string {
	out := make(map[string]string, 32)
	for level, severities := range schemaLevelToSeverities {
		for _, s := range severities {
			out[s] = level
		}
	}
	return out
}()

// expandLevels resolves one or more user levels to the underlying severities.
func expandLevels(levels []string) []string {
	if len(levels) == 0 {
		return nil
	}
	out := make([]string, 0, len(levels)*4)
	for _, lvl := range levels {
		out = append(out, schemaLevelToSeverities[lvl]...)
	}
	return out
}

// Query runs the LogsQL query and returns up to limit entries plus an opaque
// cursor. The cursor pairs the oldest returned timestamp with the keys seen at
// it, so resuming neither drops nor repeats rows sharing that timestamp.
func (q *LogsQuerier) Query(ctx context.Context, branchID string, start, end time.Time, filters []LogFilter, limit int, cursor string) (*LogsResult, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be positive")
	}
	cursorNanos, cursorSeen, err := decodeCursor(cursor)
	if err != nil {
		return nil, fmt.Errorf("decode cursor: %w", err)
	}

	lql, err := buildLogsQL(q.namespace, branchID, filters, cursorNanos)
	if err != nil {
		return nil, err
	}

	fetchLimit := limit + len(cursorSeen)
	rows, err := q.backend.Query(ctx, lql, start, end, fetchLimit)
	if err != nil {
		return nil, fmt.Errorf("query backend: %w", err)
	}

	out := &LogsResult{Entries: make([]LogEntry, 0, len(rows))}
	page := pagination.New(q.pageBytes)
	// Rows are _time DESC, so the oldest emitted timestamp's rows sit at the
	// tail; collect their keys for the resume cursor.
	var boundaryNanos int64
	var boundaryKeys []string
	capped := false
	for _, r := range rows {
		key := rowKey(r)
		nanos := r.Timestamp.UnixNano()
		// Inclusive resume refetches the cursor timestamp; skip what the
		// previous page already returned there.
		if nanos == cursorNanos && cursorSeen[key] {
			continue
		}
		// The overlap fetch can yield more than limit unseen rows; stop at the
		// caller's limit and let the cursor carry the remainder forward.
		if len(out.Entries) == limit {
			capped = true
			break
		}

		message, logger := unwrapCNPGBody(r.Message)
		message = redactPassword(message)
		message = pagination.Truncate(message, logTruncationMarker, maxLogMessageBytes)
		if !page.Add(len(message)) {
			break
		}

		entry := LogEntry{
			Timestamp:  r.Timestamp,
			InstanceID: r.Pod,
			Message:    message,
		}
		if mapped, ok := severityToLevel[strings.ToUpper(r.Severity)]; ok {
			entry.Level = mapped
		}
		switch {
		case logger == "pgaudit":
			entry.Process = "pgaudit"
		case r.Process != "":
			entry.Process = r.Process
		}
		out.Entries = append(out.Entries, entry)

		if nanos != boundaryNanos {
			boundaryNanos, boundaryKeys = nanos, boundaryKeys[:0]
		}
		boundaryKeys = append(boundaryKeys, key)
	}

	// More rows wait if we capped, hit the byte budget, or filled the fetch.
	moreRemain := capped || page.Stopped() || len(rows) >= fetchLimit
	if !moreRemain {
		return out, nil
	}
	if len(out.Entries) == 0 {
		// Whole fetch was already-seen rows at the cursor timestamp (identical
		// lines, or a backend ignoring _time order). _time:<= is inclusive, so
		// stepping one ns past it unsticks pagination on the next fetch.
		out.NextCursor = encodeCursor(cursorNanos-1, nil)
		return out, nil
	}
	// A timestamp spanning pages must carry forward every key already returned
	// at it, not just this page's, or the next fetch would re-emit them.
	if boundaryNanos == cursorNanos {
		for k := range cursorSeen {
			boundaryKeys = append(boundaryKeys, k)
		}
	}
	out.NextCursor = encodeCursor(boundaryNanos, boundaryKeys)
	return out, nil
}

// rowKeyMessageBytes caps how much of the message feeds the hash. A line can
// reach maxLogMessageBytes (1 MiB); hashing only the prefix bounds the per-row
// work without hurting uniqueness, since distinct rows at one nanosecond
// effectively always differ within the first few KB.
const rowKeyMessageBytes = 2048

// rowKey is a content hash standing in for the per-row id VictoriaLogs lacks.
// Lines sharing their hashed prefix at one timestamp collide, which is fine:
// in practice they are the same content.
func rowKey(r LogRow) string {
	msg := r.Message
	if len(msg) > rowKeyMessageBytes {
		msg = msg[:rowKeyMessageBytes]
	}
	h := fnv.New64a()
	for _, s := range []string{r.Pod, r.Severity, r.Process, msg} {
		io.WriteString(h, s)
		h.Write([]byte{0})
	}
	return strconv.FormatUint(h.Sum64(), 16)
}

// buildLogsQL renders the LogsQL expression. It starts with the namespace
// and container scopes, then appends an exact-match branch_id predicate
// (defense in depth) and finally each user-supplied filter. We use the
// LogsQL field-filter syntax (`field:value`, `field:in (a,b)`,
// `field:~"regex"`) which is the stable subset supported by VictoriaLogs.
//
// resumeOnOrBeforeNanos > 0 adds `_time:<={ts}` (RFC3339Nano) so paginated
// queries pick up the previous page's oldest timestamp again. The caller skips
// the rows it already returned at that timestamp, so same-timestamp rows
// straddling the page boundary are neither dropped nor duplicated.
// VictoriaLogs's `_time:` filter parses bare integers as durations, so the
// cursor must be serialized as an ISO timestamp.
func buildLogsQL(namespace, branchID string, filters []LogFilter, resumeOnOrBeforeNanos int64) (string, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "kubernetes.namespace_name:=%q AND kubernetes.container_name:=%q", namespace, "postgres")
	// The postgres container streams output from CNPG's instance manager,
	// which mixes its own logs (logger="instance-manager"), backup lines
	// (logger="backup") and actual postgres records (logger="postgres").
	b.WriteString(` AND logger:in ("postgres","pgaudit")`)
	fmt.Fprintf(&b, ` AND branch_id:=%q`, branchID)
	if resumeOnOrBeforeNanos > 0 {
		fmt.Fprintf(&b, " AND _time:<=%s", time.Unix(0, resumeOnOrBeforeNanos).UTC().Format(time.RFC3339Nano))
	}

	for _, f := range filters {
		clause, err := compileLogFilter(f)
		if err != nil {
			return "", err
		}
		b.WriteString(" AND ")
		b.WriteString(clause)
	}
	return b.String(), nil
}

func compileLogFilter(f LogFilter) (string, error) {
	switch f.Field {
	case "instance":
		if f.Op != "in" {
			return "", fmt.Errorf("op [%s] not allowed for field [instance]", f.Op)
		}
		return inClause("kubernetes.pod_name", f.Values), nil
	case "level":
		if f.Op != "in" {
			return "", fmt.Errorf("op [%s] not allowed for field [level]", f.Op)
		}
		expanded := expandLevels(f.Values)
		if len(expanded) == 0 {
			return "", fmt.Errorf("invalid log level set")
		}
		return inClause("severity_text", expanded), nil
	case "process":
		if f.Op != "in" {
			return "", fmt.Errorf("op [%s] not allowed for field [process]", f.Op)
		}
		return inClause("backend_type", f.Values), nil
	case "logger":
		if f.Op != "in" {
			return "", fmt.Errorf("op [%s] not allowed for field [logger]", f.Op)
		}
		return inClause("logger", f.Values), nil
	case "body":
		// The message lives in _msg, not body (Vector renames it on ingest).
		switch f.Op {
		case "contains":
			return fmt.Sprintf("_msg:~%s", quoteLQL(regexp.QuoteMeta(f.Value))), nil
		case "icontains":
			return fmt.Sprintf("_msg:~%s", quoteLQL("(?i)"+regexp.QuoteMeta(f.Value))), nil
		case "regex":
			return fmt.Sprintf("_msg:~%s", quoteLQL(f.Value)), nil
		case "iregex":
			return fmt.Sprintf("_msg:~%s", quoteLQL("(?i)"+f.Value)), nil
		default:
			return "", fmt.Errorf("op [%s] not allowed for field [body]", f.Op)
		}
	}
	return "", fmt.Errorf("unknown field [%s]", f.Field)
}

func inClause(field string, values []string) string {
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = quoteLQL(v)
	}
	return fmt.Sprintf("%s:in (%s)", field, strings.Join(quoted, ","))
}

// quoteLQL renders a string literal for LogsQL. Go-style quoting matches
// LogsQL's understanding of double-quoted strings with backslash escapes for
// ", \, and control characters.
func quoteLQL(v string) string {
	return strconv.Quote(v)
}

// CNPG wraps Postgres CSV records as `{...,"record":{"message":"..."}}`,
// pgaudit records as `{...,"record":{"audit":{...}}}` (with record.message
// blanked), and its lifecycle logs as `{...,"msg":"..."}`. Returns
// (message, logger); logger is the outer envelope's `logger` field when
// present so the caller can stamp Process from it without trusting the
// storage layer's derived backend_type.
func unwrapCNPGBody(body string) (string, string) {
	if !strings.HasPrefix(body, "{") || !strings.HasSuffix(body, "}") {
		return body, ""
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		return body, ""
	}
	logger, _ := parsed["logger"].(string)
	if record, ok := parsed["record"].(map[string]any); ok {
		if audit, ok := record["audit"].(map[string]any); ok {
			return renderPgAuditRecord(audit, body), logger
		}
		if msg, ok := record["message"].(string); ok && msg != "" {
			return msg, logger
		}
	}
	if msg, ok := parsed["msg"].(string); ok && msg != "" {
		return msg, logger
	}
	return body, logger
}

var pgauditRecordFields = []string{
	"audit_type", "statement_id", "substatement_id", "class", "command",
	"object_type", "object_name", "statement", "parameter",
}

// renderPgAuditRecord reconstructs pgaudit's CSV line
// ("AUDIT: SESSION,1,1,...,statement,parameter") from CNPG's structured envelope.
// Falls back to body if the CSV writer fails (in practice unreachable with strings.Builder).
func renderPgAuditRecord(audit map[string]any, body string) string {
	fields := make([]string, 0, len(pgauditRecordFields)+1)
	for _, key := range pgauditRecordFields {
		s, _ := audit[key].(string)
		fields = append(fields, s)
	}
	if rows, _ := audit["rows"].(string); rows != "" {
		fields = append(fields, rows)
	}

	var buf strings.Builder
	w := csv.NewWriter(&buf)
	if err := w.Write(fields); err != nil {
		return body
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return body
	}

	return "AUDIT: " + strings.TrimRight(buf.String(), "\n")
}

// rolePassword matches a CREATE/ALTER ROLE|USER|GROUP or USER MAPPING
// statement up to its `password` keyword, capturing everything before the
// secret. Native Postgres logging does not redact passwords, so CNPG's
// managed-role sync (`ALTER ROLE ... PASSWORD '...'`, run as superuser) can
// otherwise reach the customer through the always-on postgres logger. The `s`
// flag matters: Vector reassembles multi-line records, so the statement and its
// password may span newlines in one message.
var rolePassword = regexp.MustCompile(`(?is)((?:create|alter)\s+(?:role|user|group)\b.*?\bpassword\b).*`)

// redactPassword mirrors pgaudit: it truncates a role/user-mapping statement at
// the `password` keyword and appends a redaction token, rather than parsing out
// the literal. The command-type guard avoids redacting unrelated lines that
// merely mention "password".
func redactPassword(s string) string {
	return rolePassword.ReplaceAllString(s, "$1 <REDACTED>")
}

// The cursor is opaque: "{unixNano}:{key,key,...}". The timestamp drives the
// resume clause; the keys let the next page skip rows already returned there.
func encodeCursor(nanos int64, keys []string) string {
	return strconv.FormatInt(nanos, 10) + ":" + strings.Join(keys, ",")
}

func decodeCursor(c string) (int64, map[string]bool, error) {
	if c == "" {
		return 0, nil, nil
	}
	nanos, keys, ok := strings.Cut(c, ":")
	if !ok {
		return 0, nil, fmt.Errorf("cut cursor")
	}
	n, err := strconv.ParseInt(nanos, 10, 64)
	if err != nil || n <= 0 {
		return 0, nil, fmt.Errorf("parse cursor timestamp")
	}
	seen := make(map[string]bool)
	if keys != "" {
		for k := range strings.SplitSeq(keys, ",") {
			seen[k] = true
		}
	}
	return n, seen, nil
}
