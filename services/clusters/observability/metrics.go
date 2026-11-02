// Package observability backs the per-cell branch metric and log RPCs on the
// clusters service. Metrics are queried from a local VictoriaMetrics instance
// using PromQL/MetricsQL; logs from a local VictoriaLogs instance using LogsQL.
package observability

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

var ErrInvalidMetricsRequest = errors.New("invalid metrics request")

// MetricKind is the underlying Prometheus metric semantic.
type MetricKind string

const (
	Counter MetricKind = "counter"
	Gauge   MetricKind = "gauge"
)

// scrapeInterval is the configured vmagent scrape_interval (see
// kustomize/overlays/local/cell-observability/vmagent-values.yaml). PromQL
// rate windows must cover at least four scrapes so a single missed scrape
// doesn't blank the cell.
const scrapeInterval = 30 * time.Second

// MetricInfo describes how a Xata-facing metric maps onto a Prometheus series
// scraped by Vector inside the cell.
type MetricInfo struct {
	// PromName is the metric name as exposed to VictoriaMetrics.
	PromName string
	// Unit is the user-visible unit label.
	Unit string
	Kind MetricKind
	// SpaceAggDefault is the space aggregation for gauges (user agg becomes the time agg).
	SpaceAggDefault string
	// TemporalAggDefault is the time aggregation for counters (user agg becomes the space agg).
	TemporalAggDefault string
	// AdditionalLabels are added as label matchers (label="value").
	AdditionalLabels map[string]string
	// ExcludeLabels are added as label exclusion matchers (label!="value") to drop unreliable series.
	ExcludeLabels map[string]string
	// WithinPodCombiner folds per-container series into one series per pod before the user aggregation.
	WithinPodCombiner string
}

// metricCatalog maps Xata metric names to their PromQL definitions. Names use
// the Prometheus underscore form preserved by Vector's remote_write sink.
var metricCatalog = map[string]MetricInfo{
	"cpu":                  {PromName: "container_cpu_usage_seconds_total", Unit: "percentage", Kind: Counter, TemporalAggDefault: "rate", WithinPodCombiner: "sum", ExcludeLabels: map[string]string{"container": ""}},
	"memory":               {PromName: "container_memory_working_set_bytes", Unit: "bytes", Kind: Gauge, SpaceAggDefault: "avg"},
	"disk":                 {PromName: "cnpg_pg_database_size_bytes", Unit: "bytes", Kind: Gauge, SpaceAggDefault: "sum"},
	"connections_active":   {PromName: "cnpg_pg_stat_activity_connections_active", Unit: "connections", Kind: Gauge, SpaceAggDefault: "sum"},
	"connections_idle":     {PromName: "cnpg_pg_stat_activity_connections_idle", Unit: "connections", Kind: Gauge, SpaceAggDefault: "sum"},
	"network_ingress":      {PromName: "container_network_receive_bytes_total", Unit: "bytes", Kind: Counter, TemporalAggDefault: "increase"},
	"network_egress":       {PromName: "container_network_transmit_bytes_total", Unit: "bytes", Kind: Counter, TemporalAggDefault: "increase"},
	"iops_read":            {PromName: "cnpg_pg_stat_io_total_reads", Unit: "iops", Kind: Counter, TemporalAggDefault: "rate"},
	"iops_write":           {PromName: "cnpg_pg_stat_io_total_writes", Unit: "iops", Kind: Counter, TemporalAggDefault: "rate"},
	"latency_read":         {PromName: "cnpg_pg_stat_io_total_read_time_ms", Unit: "ms", Kind: Counter, TemporalAggDefault: "rate"},
	"latency_write":        {PromName: "cnpg_pg_stat_io_total_write_time_ms", Unit: "ms", Kind: Counter, TemporalAggDefault: "rate"},
	"throughput_read":      {PromName: "container_fs_reads_bytes_total", Unit: "bytes", Kind: Counter, TemporalAggDefault: "rate"},
	"throughput_write":     {PromName: "container_fs_writes_bytes_total", Unit: "bytes", Kind: Counter, TemporalAggDefault: "rate"},
	"wal_sync_time":        {PromName: "cnpg_collector_wal_sync_time", Unit: "ms", Kind: Counter, TemporalAggDefault: "rate"},
	"replication_lag_time": {PromName: "cnpg_pg_replication_lag", Unit: "s", Kind: Gauge, SpaceAggDefault: "avg"},
}

// LookupMetric returns the catalog entry for a Xata metric name. Returns
// false when the metric is not part of the public surface.
func LookupMetric(name string) (MetricInfo, bool) {
	info, ok := metricCatalog[name]
	return info, ok
}

// validAggregations is the user-facing aggregation set, mirroring the OpenAPI
// enum on BranchMetricsRequest.aggregations.
var validAggregations = map[string]struct{}{
	"avg": {},
	"min": {},
	"max": {},
	"sum": {},
}

// CalculateStep picks a step interval based on the queried time range.
func CalculateStep(start, end time.Time) time.Duration {
	diff := end.Sub(start)
	switch {
	case diff < 24*time.Hour:
		return time.Minute
	case diff < 3*24*time.Hour:
		return 15 * time.Minute
	case diff < 7*24*time.Hour:
		return 30 * time.Minute
	case diff < 30*24*time.Hour:
		return time.Hour
	default:
		return 6 * time.Hour
	}
}

// MetricsBackend is the minimum surface MetricsQuerier needs from the
// VictoriaMetrics HTTP API. It exists so tests can inject a stub.
type MetricsBackend interface {
	QueryRange(ctx context.Context, query string, start, end time.Time, step time.Duration) ([]PromSeries, error)
}

// PromSeries is a parsed time-series result from VictoriaMetrics
// /api/v1/query_range.
type PromSeries struct {
	Labels map[string]string
	Values []PromSample
}

// PromSample is a single time-series point.
type PromSample struct {
	Timestamp time.Time
	Value     float64
}

// MetricsQuerier resolves a branch metric query into one PromQL expression per
// requested aggregation, executes them, and returns parsed time-series.
type MetricsQuerier struct {
	backend   MetricsBackend
	namespace string
}

// NewMetricsQuerier wires a querier against the given VictoriaMetrics-backed
// HTTP client. namespace scopes results to pods in the clusters namespace.
func NewMetricsQuerier(backend MetricsBackend, namespace string) *MetricsQuerier {
	return &MetricsQuerier{backend: backend, namespace: namespace}
}

// MetricSeries is a single aggregated time-series ready to be marshalled into
// the gRPC response.
type MetricSeries struct {
	Aggregation string
	InstanceID  string
	Values      []MetricValue
}

// MetricValue is a single aggregated point.
type MetricValue struct {
	Timestamp time.Time
	Value     float32
}

// Result captures the time-series for a single metric.
type Result struct {
	Metric string
	Unit   string
	Series []MetricSeries
}

// Query fans out one VictoriaMetrics request per (metric, aggregation) pair
// in parallel and returns one Result per requested metric in request order.
// branchID is enforced as an anchored regex on the pod label so results can
// never include other branches' samples even if validation is bypassed
// upstream.
func (q *MetricsQuerier) Query(ctx context.Context, branchID string, metrics, instances, aggregations []string, start, end time.Time) ([]Result, error) {
	if len(metrics) == 0 {
		return nil, fmt.Errorf("%w: at least one metric is required", ErrInvalidMetricsRequest)
	}
	if len(aggregations) == 0 {
		return nil, fmt.Errorf("%w: at least one aggregation is required", ErrInvalidMetricsRequest)
	}
	for _, agg := range aggregations {
		if _, ok := validAggregations[agg]; !ok {
			return nil, fmt.Errorf("%w: unsupported aggregation: %s", ErrInvalidMetricsRequest, agg)
		}
	}

	infos := make([]MetricInfo, len(metrics))
	for i, m := range metrics {
		info, ok := LookupMetric(m)
		if !ok {
			return nil, fmt.Errorf("%w: unknown metric: %s", ErrInvalidMetricsRequest, m)
		}
		infos[i] = info
	}

	step := CalculateStep(start, end)

	// Per-metric, per-aggregation slots so backend fan-out can fill them
	// independently without locking.
	perMetric := make([][][]MetricSeries, len(metrics))
	for i := range perMetric {
		perMetric[i] = make([][]MetricSeries, len(aggregations))
	}

	g, gctx := errgroup.WithContext(ctx)
	for mi, m := range metrics {
		info := infos[mi]
		matchers := buildMatchers(q.namespace, branchID, instances, info.AdditionalLabels, info.ExcludeLabels)
		for ai, agg := range aggregations {
			g.Go(func() error {
				query, err := buildPromQL(info, agg, matchers, step)
				if err != nil {
					return fmt.Errorf("build promql (metric=%s agg=%s): %w", m, agg, err)
				}
				series, err := q.backend.QueryRange(gctx, query, start, end, step)
				if err != nil {
					return fmt.Errorf("query backend (metric=%s agg=%s): %w", m, agg, err)
				}
				ms := make([]MetricSeries, len(series))
				for j, s := range series {
					ms[j] = MetricSeries{
						Aggregation: agg,
						InstanceID:  s.Labels["pod"],
						Values:      toMetricValues(s.Values),
					}
				}
				perMetric[mi][ai] = ms
				return nil
			})
		}
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	out := make([]Result, len(metrics))
	for i, m := range metrics {
		out[i] = Result{Metric: m, Unit: infos[i].Unit}
		for _, series := range perMetric[i] {
			out[i].Series = append(out[i].Series, series...)
		}
	}
	return out, nil
}

// buildPromQL assembles the PromQL expression for a metric/aggregation pair.
// Counters get `<spaceAgg> by (pod) (<temporalAgg>(<metric>{matchers}[window]))`;
// gauges get `<spaceAgg> by (pod) (<metric>{matchers})` with the user-supplied
// agg as the space aggregation when the metric has no fixed default.
func buildPromQL(info MetricInfo, userAgg, matchers string, step time.Duration) (string, error) {
	switch info.Kind {
	case Counter:
		// Counter rate windows must clamp to >= 4 * scrape_interval so a
		// single missed scrape doesn't leave only one sample in the window
		// and blank the cell.
		inner := fmt.Sprintf("%s(%s{%s}[%s])", info.TemporalAggDefault, info.PromName, matchers, formatRange(rateWindow(step)))
		if info.WithinPodCombiner != "" {
			// Fold per-container series into one per pod before the user
			// aggregation runs across pods.
			inner = fmt.Sprintf("%s by (pod) (%s)", info.WithinPodCombiner, inner)
		}
		return fmt.Sprintf("%s by (pod) (%s)", userAgg, inner), nil
	case Gauge:
		spaceAgg := info.SpaceAggDefault
		if spaceAgg == "" {
			// No fixed space aggregation: the user agg IS the space agg, no
			// _over_time wrapping needed.
			return fmt.Sprintf("%s by (pod) (%s{%s})", userAgg, info.PromName, matchers), nil
		}
		// Fixed space agg (e.g. memory:avg): user agg becomes the time
		// aggregation via avg_over_time / max_over_time / min_over_time.
		// Window stays at `step` — clamping it like rate() would smear gauge
		// values across step boundaries (e.g. connections_active=0.5).
		return fmt.Sprintf("%s by (pod) (%s_over_time(%s{%s}[%s]))", spaceAgg, userAgg, info.PromName, matchers, formatRange(step)), nil
	default:
		return "", fmt.Errorf("unhandled metric kind %q", info.Kind)
	}
}

// rateWindow is the lookback window for rate()/increase(). A bare `step`
// window is dangerous: with scrape_interval=30s and step=1m, missing a
// single scrape leaves only one sample in the window and the cell goes
// blank. The Prometheus convention is at least 4× scrape_interval.
func rateWindow(step time.Duration) time.Duration {
	if min := 4 * scrapeInterval; step < min {
		return min
	}
	return step
}

// buildMatchers renders the PromQL label matcher string. The branch scope
// is always added so a buggy or malicious caller cannot read another
// branch's samples by omitting the instance list.
func buildMatchers(namespace, branchID string, instances []string, extra, exclude map[string]string) string {
	parts := []string{
		fmt.Sprintf(`namespace=%q`, namespace),
		fmt.Sprintf(`branch_id=%q`, branchID),
	}
	if len(instances) > 0 {
		parts = append(parts, fmt.Sprintf(`pod=~%q`, "^("+strings.Join(quotedRegexAlts(instances), "|")+")$"))
	}
	for k, v := range extra {
		parts = append(parts, fmt.Sprintf(`%s=%q`, k, v))
	}
	for k, v := range exclude {
		parts = append(parts, fmt.Sprintf(`%s!=%q`, k, v))
	}
	return strings.Join(parts, ",")
}

func quotedRegexAlts(values []string) []string {
	out := make([]string, len(values))
	for i, v := range values {
		out[i] = regexp.QuoteMeta(v)
	}
	return out
}

func formatRange(step time.Duration) string {
	// VictoriaMetrics accepts "1m", "15m", "30m", "1h", "6h" — these match
	// the buckets returned by CalculateStep. Fallback to seconds otherwise.
	if step >= time.Hour && step%time.Hour == 0 {
		return fmt.Sprintf("%dh", int(step/time.Hour))
	}
	if step%time.Minute == 0 {
		return fmt.Sprintf("%dm", int(step/time.Minute))
	}
	return fmt.Sprintf("%ds", int(step/time.Second))
}

func toMetricValues(samples []PromSample) []MetricValue {
	out := make([]MetricValue, 0, len(samples))
	for _, s := range samples {
		out = append(out, MetricValue{Timestamp: s.Timestamp, Value: float32(s.Value)})
	}
	return out
}
