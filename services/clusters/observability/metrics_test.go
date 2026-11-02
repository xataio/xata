package observability

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeMetricsBackend struct {
	queries []string
	result  []PromSeries
	err     error
}

func (f *fakeMetricsBackend) QueryRange(_ context.Context, query string, _, _ time.Time, _ time.Duration) ([]PromSeries, error) {
	f.queries = append(f.queries, query)
	return f.result, f.err
}

func TestMetricsQuerier_BuildsPromQLPerMetric(t *testing.T) {
	branchID := "br-123"
	start := time.Now().Add(-1 * time.Hour)
	end := time.Now()

	tests := map[string]struct {
		metric        string
		instances     []string
		aggregations  []string
		wantContains  []string
		wantInstances []string // optional: assert the resolved series InstanceID
	}{
		"counter cpu rate avg": {
			metric:       "cpu",
			instances:    []string{"br-123-1"},
			aggregations: []string{"avg"},
			wantContains: []string{
				"avg by (pod) (sum by (pod) (rate(container_cpu_usage_seconds_total{",
				`namespace="xata-clusters"`,
				`branch_id="br-123"`,
				`container!=""`,
				`pod=~"^(br-123-1)$"`,
			},
		},
		"gauge memory avg over time": {
			metric:       "memory",
			instances:    nil,
			aggregations: []string{"max"},
			wantContains: []string{
				"avg by (pod) (max_over_time(container_memory_working_set_bytes{",
				`branch_id="br-123"`,
			},
		},
		"counter network_ingress carries direction-equivalent metric name": {
			metric:       "network_ingress",
			instances:    []string{"br-123-0"},
			aggregations: []string{"avg"},
			wantContains: []string{
				"increase(container_network_receive_bytes_total{",
			},
		},
		"gauge connections_active sum aggregation": {
			metric:       "connections_active",
			aggregations: []string{"max"},
			wantContains: []string{
				"sum by (pod) (max_over_time(cnpg_pg_stat_activity_connections_active{",
			},
		},
		"counter wal_sync_time rate": {
			// cnpg_collector_wal_sync_time is cumulative (pg_stat_wal), so it
			// must be rated rather than shown as a raw ever-growing total.
			metric:       "wal_sync_time",
			aggregations: []string{"avg"},
			wantContains: []string{
				"avg by (pod) (rate(cnpg_collector_wal_sync_time{",
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			backend := &fakeMetricsBackend{result: []PromSeries{
				{Labels: map[string]string{"pod": "br-123-1"}, Values: []PromSample{{Timestamp: start, Value: 1.5}}},
			}}
			q := NewMetricsQuerier(backend, "xata-clusters")
			res, err := q.Query(context.Background(), branchID, []string{tt.metric}, tt.instances, tt.aggregations, start, end)
			require.NoError(t, err)
			require.Len(t, backend.queries, len(tt.aggregations))
			for _, fragment := range tt.wantContains {
				require.Contains(t, backend.queries[0], fragment, "missing fragment in %s", backend.queries[0])
			}
			require.Len(t, res, 1)
			require.Len(t, res[0].Series, 1)
			require.Equal(t, "br-123-1", res[0].Series[0].InstanceID)
		})
	}
}

// 1h window → step=1m. Counter rate windows clamp up to 4*scrape_interval
// (2m); gauge *_over_time windows must NOT clamp — clamping would smear
// gauge values across step boundaries (e.g. connections_active=0.5).
func TestBuildPromQL_RateWindowAppliesOnlyToCounters(t *testing.T) {
	step := time.Minute
	cpu, _ := LookupMetric("cpu")                  // counter
	conns, _ := LookupMetric("connections_active") // gauge with fixed space agg
	cpuQuery, err := buildPromQL(cpu, "avg", "ns=\"x\"", step)
	require.NoError(t, err)
	require.Contains(t, cpuQuery, "[2m]", "counter rate window clamps up")
	connsQuery, err := buildPromQL(conns, "avg", "ns=\"x\"", step)
	require.NoError(t, err)
	require.Contains(t, connsQuery, "[1m]", "gauge over_time stays at step")
}

func TestMetricsQuerier_RejectsUnknownMetric(t *testing.T) {
	q := NewMetricsQuerier(&fakeMetricsBackend{}, "xata-clusters")
	_, err := q.Query(context.Background(), "br-1", []string{"unknown-metric"}, nil, []string{"avg"}, time.Now().Add(-time.Hour), time.Now())
	require.ErrorIs(t, err, ErrInvalidMetricsRequest)
}

func TestMetricsQuerier_RejectsUnsupportedAggregation(t *testing.T) {
	q := NewMetricsQuerier(&fakeMetricsBackend{}, "xata-clusters")
	_, err := q.Query(context.Background(), "br-1", []string{"cpu"}, nil, []string{"p99"}, time.Now().Add(-time.Hour), time.Now())
	require.ErrorIs(t, err, ErrInvalidMetricsRequest)
}

func TestMetricsQuerier_RejectsMissingAggregations(t *testing.T) {
	q := NewMetricsQuerier(&fakeMetricsBackend{}, "xata-clusters")
	_, err := q.Query(context.Background(), "br-1", []string{"cpu"}, nil, nil, time.Now().Add(-time.Hour), time.Now())
	require.ErrorIs(t, err, ErrInvalidMetricsRequest)
}

func TestCalculateStep(t *testing.T) {
	now := time.Now()
	tests := map[string]struct {
		span time.Duration
		want time.Duration
	}{
		"1h":  {time.Hour, time.Minute},
		"36h": {36 * time.Hour, 15 * time.Minute},
		"5d":  {5 * 24 * time.Hour, 30 * time.Minute},
		"15d": {15 * 24 * time.Hour, time.Hour},
		"60d": {60 * 24 * time.Hour, 6 * time.Hour},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tt.want, CalculateStep(now.Add(-tt.span), now))
		})
	}
}
