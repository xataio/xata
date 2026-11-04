package session

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clustersv1 "xata/gen/proto/clusters/v1"
	"xata/internal/coalesce"
	"xata/internal/o11y"
	"xata/services/gateway/metrics"
)

const instrumentationName = "xata/services/gateway/session"

// ErrBranchHibernated is returned when the branch is manually hibernated
// (scale-to-zero disabled) and cannot be auto-reactivated.
var ErrBranchHibernated = errors.New("branch is hibernated")

// ErrBranchNotFound is returned when the branch has been terminated and the
// clusters service no longer knows it.
var ErrBranchNotFound = errors.New("branch not found")

// ErrNoReadReplica is returned when the read-only endpoint is dialed on a
// branch configured with a single instance.
var ErrNoReadReplica = errors.New("branch has no read replica")

// ErrReactivateTimeout is returned when a held connection gives up waiting
// for its cluster to become reachable.
var ErrReactivateTimeout = errors.New("timed out waiting for cluster to be reactivated")

// ClusterDialer is responsible for dialing into a Postgres cluster, handling
// reactivation when the cluster is hibernated.
type ClusterDialer struct {
	dialer dialerFn

	clustersService     clustersService
	reactivateFn        reactivateClusterFn
	reactivateTimeout   time.Duration
	statusCheckInterval time.Duration
	tracer              trace.Tracer

	// statusPoll shares one status poll among every caller waiting on the
	// same cluster, keyed by cluster ID. See waitUntilAvailable.
	statusPoll *coalesce.Coalescer[string, struct{}]
}

// reactivateClusterFn wakes a hibernated cluster and returns a live connection
// to it. cluster is the description that classified it as hibernated, so an
// instrumented implementation can label the sample without another lookup.
type reactivateClusterFn func(ctx context.Context, svc clustersService, clusterID, network, address string, cluster *clustersv1.DescribePostgresClusterResponse) (net.Conn, error)

type dialerFn func(ctx context.Context, network, address string) (net.Conn, error)

type ClusterDialerOption func(*ClusterDialer)

type ClusterDialerConfiguration struct {
	ReactivateTimeout   time.Duration
	StatusCheckInterval time.Duration
	// BackendTCPUserTimeout bounds how long data written to a backend
	// connection may stay unacknowledged before the kernel fails it. Zero
	// leaves the system default, under which an unacknowledged write is
	// retried for many minutes without surfacing an error. Linux only.
	BackendTCPUserTimeout time.Duration
}

type clustersService interface {
	DescribePostgresCluster(ctx context.Context, request *clustersv1.DescribePostgresClusterRequest, opts ...grpc.CallOption) (*clustersv1.DescribePostgresClusterResponse, error)
	UpdatePostgresCluster(ctx context.Context, request *clustersv1.UpdatePostgresClusterRequest, opts ...grpc.CallOption) (*clustersv1.UpdatePostgresClusterResponse, error)
}

// newNetDialer builds the dialer used for backend connections. A non-zero
// userTimeout applies TCP_USER_TIMEOUT to each socket, so a write that is
// never acknowledged fails the connection instead of hanging silently.
func newNetDialer(userTimeout time.Duration) dialerFn {
	var d net.Dialer
	if userTimeout > 0 {
		d.Control = func(_, _ string, c syscall.RawConn) error {
			return setTCPUserTimeout(c, userTimeout)
		}
	}
	return d.DialContext
}

// NewClusterDialer creates a dialer for connecting to Postgres clusters.
// clusters is the cell-local clusters service used to describe and reactivate
// a cluster when a dial fails.
func NewClusterDialer(cfg ClusterDialerConfiguration, clusters clustersService, opts ...ClusterDialerOption) *ClusterDialer {
	d := &ClusterDialer{
		dialer:              newNetDialer(cfg.BackendTCPUserTimeout),
		clustersService:     clusters,
		reactivateTimeout:   cfg.ReactivateTimeout,
		statusCheckInterval: cfg.StatusCheckInterval,
		tracer:              noop.NewTracerProvider().Tracer(instrumentationName),
	}

	d.reactivateFn = func(ctx context.Context, svc clustersService, clusterID, network, address string, _ *clustersv1.DescribePostgresClusterResponse) (net.Conn, error) {
		return d.reactivateCluster(ctx, svc, clusterID, network, address)
	}

	for _, opt := range opts {
		opt(d)
	}

	// statusPoll is a shared poll for every caller waiting on the same cluster.
	// It runs waitUntilAvailable, which polls the clusters service until the
	// cluster reports an available instance.
	d.statusPoll = coalesce.New(
		d.waitUntilAvailable,
		coalesce.WithTracer(d.tracer, "cluster_status_poll"),
	)

	return d
}

// WithTracing configures the tracer used to instrument the dialer.
func WithTracing(tracer trace.Tracer) ClusterDialerOption {
	return func(d *ClusterDialer) {
		d.tracer = tracer
	}
}

func WithInstrumentation(gwMetrics *metrics.GatewayMetrics) ClusterDialerOption {
	return func(d *ClusterDialer) {
		reactivate := d.reactivateCluster
		d.reactivateFn = func(ctx context.Context, svc clustersService, clusterID, network, address string, cluster *clustersv1.DescribePostgresClusterResponse) (net.Conn, error) {
			startTime := time.Now()
			conn, err := reactivate(ctx, svc, clusterID, network, address)
			gwMetrics.RecordClusterReactivation(ctx, time.Since(startTime), cluster.GetUsesWakeupPool(), instanceSize(cluster.GetConfiguration()), err == nil, waitErrorType(err))
			return conn, err
		}
	}
}

// instanceSize formats the cluster's vCPU request and memory as a single
// metric label, e.g. "500m/1GB" or "2/8GB". The clusters service only reports
// the resource values, not the instance type name they were derived from, so
// this is the closest the gateway can get to the size a user picked. Returns
// "" when the configuration carries neither value.
func instanceSize(cfg *clustersv1.ClusterConfiguration) string {
	vcpu, memory := cfg.GetVcpuRequest(), cfg.GetMemory()
	if vcpu == "" && memory == "" {
		return ""
	}
	return fmt.Sprintf("%s/%sGB", vcpu, memory)
}

func WithDialer(dialer dialerFn) ClusterDialerOption {
	return func(d *ClusterDialer) {
		d.dialer = dialer
	}
}

// Dial connects to the specified branch, handling reactivation if the cluster
// is hibernated and the configuration allows it.
func (d *ClusterDialer) Dial(ctx context.Context, network string, branch *Branch) (net.Conn, error) {
	dialLogger := log.Ctx(ctx).With().Str("cluster", branch.ID).Str("address", branch.Address).Logger()
	conn, dialErr := d.dialer(ctx, network, branch.Address)
	if !shouldAttemptReactivation(dialErr) {
		return conn, dialErr
	}

	// Dial failed with refused or DNS-not-found. Look up the cluster to
	// decide between: reactivating (hibernated + scale-to-zero), holding the
	// connection until the target is reachable (cluster is or will be
	// healthy), or surfacing the error (genuinely unavailable).
	svc := d.clustersService
	cluster, err := svc.DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
		Id: branch.ID,
	})
	if err != nil {
		// A terminated branch has no Branch resource, which the clusters
		// service reports as NotFound. Hibernation keeps the Branch, so this
		// never collides with the scale-to-zero reactivation path.
		if status.Code(err) == codes.NotFound {
			return nil, ErrBranchNotFound
		}
		dialLogger.Error().Err(err).Msg("failed to describe cluster")
		return nil, dialErr
	}

	// The configured instance count is the desired state, so a branch that
	// was just scaled up still holds while its replica comes up. A count of
	// one means no replica is coming, and zero means the count is unknown.
	if branch.Endpoint == EndpointRO && cluster.GetConfiguration().GetNumInstances() == 1 {
		dialLogger.Warn().Msg("read-only endpoint dialed on a single-instance branch, not holding the connection")
		return nil, ErrNoReadReplica
	}

	switch {
	case d.isClusterHibernated(cluster.Status) && d.isScaleToZeroEnabled(cluster.Configuration):
		dialLogger.Info().Msg("cluster is hibernated, reactivating...")

		conn, err := d.reactivateFn(ctx, svc, branch.ID, network, branch.Address, cluster)
		if err != nil {
			dialLogger.Error().Err(err).Msg("failed to reactivate cluster")
			return nil, dialErr
		}

		dialLogger.Info().Msg("cluster reactivated successfully")
		return conn, nil

	case d.isClusterHibernated(cluster.Status):
		return nil, ErrBranchHibernated

	case d.isClusterStartingOrHealthy(cluster.Status):
		// The cluster is not hibernated but the dial still failed. Two very
		// different situations reach this branch, and only one of them is worth
		// holding a client connection for:
		//
		//   - An instance is serving and only the dial target lags behind: a
		//     scale-to-zero wake in flight, a postgres pod roll, a pooler
		//     Service endpoint flap. Endpoint propagation is quick, so holding
		//     hides a blip that would otherwise surface as a spurious error.
		//   - No instance is serving at all, because the primary crashed and is
		//     in recovery. StatusType is derived from the Cluster resource and
		//     lags the instances, so it still reads Healthy or Transient while
		//     nothing can accept connections. Holding here blocks the client
		//     for the full reactivate timeout and makes waitUntilReachable poll
		//     the clusters service for the whole duration, which turns one
		//     unhealthy branch into load on a shared service.
		//
		// isClusterAvailable is the same predicate that ends the wait inside
		// waitUntilReachable, so checking it here keeps the decision to hold
		// and the condition that releases the hold consistent.
		// isStartingFromZero covers the cases where no instance is available
		// yet but waiting is still right: a branch that is being created or
		// woken and has no serving instance yet, so there is nothing that could
		// be crash-looping.
		if !d.isClusterAvailable(cluster.Status) && !d.isStartingFromZero(cluster.Status) {
			dialLogger.Warn().
				Str("status", cluster.Status.Status).
				Stringer("status_type", cluster.Status.StatusType).
				Int64("instance_count", cluster.Status.InstanceCount).
				Int64("instance_ready_count", cluster.Status.InstanceReadyCount).
				Msg("dial failed and no cluster instance is available, not holding the connection")
			return nil, dialErr
		}

		dialLogger.Info().Msg("cluster is unreachable but reported available, waiting...")
		conn, err := d.waitUntilReachable(ctx, branch.ID, network, branch.Address)
		if err != nil {
			dialLogger.Error().Err(err).Msg("failed to wait for cluster to be available")
			return nil, dialErr
		}

		dialLogger.Info().Msg("cluster is now available after waiting for instances to become active")
		return conn, nil
	default:
		dialLogger.Warn().Stringer("status_type", cluster.Status.StatusType).Msg("dial failed but cluster is not hibernated or reactivating")
		return nil, dialErr
	}
}

func (d *ClusterDialer) isScaleToZeroEnabled(cfg *clustersv1.ClusterConfiguration) bool {
	return cfg.ScaleToZero != nil && cfg.ScaleToZero.Enabled
}

func (d *ClusterDialer) isClusterHibernated(status *clustersv1.ClusterStatus) bool {
	return status.StatusType == clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED
}

// isClusterStartingOrHealthy reports whether the cluster is healthy or still
// transitioning toward healthy (any transient phase), as opposed to faulted or
// in an unknown state. A failed dial against such a cluster means the target
// endpoint hasn't caught up yet, so the connection should be held rather than
// dropped.
func (d *ClusterDialer) isClusterStartingOrHealthy(status *clustersv1.ClusterStatus) bool {
	return status.StatusType == clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY ||
		status.StatusType == clustersv1.ClusterStatus_STATUS_TYPE_TRANSIENT
}

// waitErrorType classifies a failed cluster wait into one of the bounded
// metrics.WaitError* values.
func waitErrorType(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrReactivateTimeout):
		return metrics.WaitErrorTimeout
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return metrics.WaitErrorCanceled
	case status.Code(err) == codes.Canceled, status.Code(err) == codes.DeadlineExceeded:
		return metrics.WaitErrorCanceled
	case isGRPCStatusError(err):
		return metrics.WaitErrorRPC
	default:
		return metrics.WaitErrorDial
	}
}

// isGRPCStatusError reports whether err wraps a gRPC status error, which is
// what the clusters service calls return on failure.
func isGRPCStatusError(err error) bool {
	_, ok := status.FromError(err)
	return ok
}

func (d *ClusterDialer) reactivateCluster(ctx context.Context, svc clustersService, clusterID, network, address string) (net.Conn, error) {
	_, err := svc.UpdatePostgresCluster(ctx, &clustersv1.UpdatePostgresClusterRequest{
		Id: clusterID,
		UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
			Hibernate: new(false),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("reactivating hibernated cluster %s: %w", clusterID, err)
	}

	return d.waitUntilReachable(ctx, clusterID, network, address)
}

// waitUntilReachable waits until the cluster is reported available AND a TCP
// connection to the dial target succeeds. The status wait is shared by every
// caller waiting on the same cluster (see waitUntilAvailable); the dial is per
// caller, since the resulting connection cannot be shared. The cluster status
// only reflects the Postgres instances; the dial target may be a separate
// component (e.g. the pooler Service) whose endpoints lag behind the cluster
// becoming healthy. Returns the live connection on success so the caller
// doesn't have to redial.
func (d *ClusterDialer) waitUntilReachable(ctx context.Context, clusterID, network, address string) (conn net.Conn, err error) {
	logger := log.Ctx(ctx).With().Str("cluster", clusterID).Str("address", address).Logger()

	// The reactivate timeout is the wait context's deadline so that the shared
	// status wait releases this caller on time, and cancels the shared poll if
	// this was its last waiter.
	waitCtx, cancel := context.WithTimeoutCause(ctx, d.reactivateTimeout, ErrReactivateTimeout)
	defer cancel()

	if _, err = d.statusPoll.Do(waitCtx, clusterID); err != nil {
		return nil, d.waitErr(waitCtx, clusterID, err)
	}

	// Start a span for the wait until target is reachable
	ctx, span := d.tracer.Start(ctx, "target_reachable_wait", trace.WithAttributes(
		metrics.AttrBranchID.String(clusterID),
	))
	defer o11y.CloseSpan(span, &err)

	// Created before the first dial so retries keep the status-check cadence
	// from the moment the cluster became available.
	ticker := time.NewTicker(d.statusCheckInterval)
	defer ticker.Stop()
	for {
		conn, err = d.dialer(ctx, network, address)
		if err == nil {
			return conn, nil
		}
		if !shouldAttemptReactivation(err) {
			return nil, fmt.Errorf("dialing %s: %w", address, err)
		}
		logger.Debug().Err(err).Msgf("cluster ready but target not yet reachable, next check: %s", d.statusCheckInterval)

		select {
		case <-waitCtx.Done():
			return nil, d.waitErr(waitCtx, clusterID, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

// waitErr maps a wait context error back to ErrReactivateTimeout when the
// reactivate timeout is what ended the wait, so callers and metrics can tell
// a timeout from the client giving up.
func (d *ClusterDialer) waitErr(waitCtx context.Context, clusterID string, err error) error {
	if errors.Is(context.Cause(waitCtx), ErrReactivateTimeout) {
		return fmt.Errorf("%w: cluster %s after %s", ErrReactivateTimeout, clusterID, d.reactivateTimeout)
	}
	return err
}

// waitUntilAvailable polls the clusters service until the cluster reports an
// available instance. It runs through d.statusPoll, so concurrent callers
// waiting on the same cluster share one poll: ctx carries the poll's own
// span and the first caller's logger, is cancelled when the last caller
// gives up, and has no deadline of its own. A Describe error ends the poll
// for every caller.
func (d *ClusterDialer) waitUntilAvailable(ctx context.Context, clusterID string) (struct{}, error) {
	// Label whichever span the poll runs under with the branch it is for
	trace.SpanFromContext(ctx).SetAttributes(metrics.AttrBranchID.String(clusterID))

	logger := log.Ctx(ctx).With().Str("cluster", clusterID).Logger()
	waitStarted := time.Now()
	statusInterval := d.statusCheckInterval
	statusChecker := time.NewTicker(statusInterval)
	defer statusChecker.Stop()

	for {
		select {
		case <-ctx.Done():
			return struct{}{}, ctx.Err()
		case <-statusChecker.C:
		}

		cluster, err := d.clustersService.DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{
			Id: clusterID,
		})
		if err != nil {
			return struct{}{}, fmt.Errorf("checking cluster status: %w", err)
		}
		if d.isClusterAvailable(cluster.Status) {
			return struct{}{}, nil
		}

		next := statusPollInterval(d.statusCheckInterval, time.Since(waitStarted))
		if next != statusInterval {
			statusInterval = next
			statusChecker.Reset(statusInterval)
		}
		logger.Debug().Msgf("waiting for cluster to be available, current status: %s, next check: %s", cluster.Status.StatusType, statusInterval)
	}
}

// isClusterAvailable checks if the cluster is available for connections.
// It first checks if the cluster primary instance is healthy, and returns true if so.
// If no healthy primary is found, it then checks if the cluster is healthy and all expected
// instances are ready. Note: The cluster status may briefly appear as healthy after reactivation,
// before starting the instances and switching to transient status. Therefore, this function
// prioritizes primary instance health, then falls back to full cluster health and instance readiness.
func (d *ClusterDialer) isClusterAvailable(status *clustersv1.ClusterStatus) bool {
	if status == nil {
		return false
	}
	for _, instance := range status.Instances {
		if instance.Primary && instance.Status == apiv1.PodHealthy {
			return true
		}
	}
	return status.StatusType == clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY &&
		status.InstanceCount == status.InstanceReadyCount
}

// startingFromZeroPhases are the cluster phases that mean the cluster is on its
// way up from having no running instance. The important one is a scale-to-zero
// wake that a concurrent connection already triggered: this connection did not
// start the wake, so it arrives here rather than on the hibernated branch, and
// an instance is expected shortly.
//
// Phases where an already-running cluster is being disrupted (switchover,
// failover, in-place restart) are deliberately excluded. Those are not
// distinguishable from a primary that is crash-looping, and holding a client
// connection for the full reactivate timeout is the wrong trade there: the
// client is better served by a prompt error it can retry.
var startingFromZeroPhases = map[string]struct{}{
	apiv1.PhaseWaitingForInstancesToBeActive: {},
	apiv1.PhaseFirstPrimary:                  {},
	apiv1.PhaseCreatingReplica:               {},
}

// isStartingFromZero reports whether the cluster is coming up from having no
// running instance, in which case holding the connection is worthwhile even
// though isClusterAvailable is still false.
func (d *ClusterDialer) isStartingFromZero(status *clustersv1.ClusterStatus) bool {
	if status == nil {
		return false
	}

	// InstanceCount is CNPG's Cluster.Status.Instances, which counts bound PVC
	// groups rather than pods ("an instance has no identity of its own, is a
	// reflection of the available PVCs"). Zero means the cluster has no
	// instance identity at all, so there is nothing that could be crash-
	// looping: a primary being OOM-killed keeps its bound PVC and so keeps a
	// non-zero count, and lands on the phase check below instead.
	//
	// Two shapes report zero, and neither carries a CNPG phase that the check
	// below would match:
	//
	//   - A branch on a wakeup pool that has not been assigned a cluster yet.
	//     Its Branch has no cluster name, so the clusters service has no
	//     Cluster resource to derive a status from and synthesizes one, which
	//     reports the healthy phase with a Transient status type. This is the
	//     window a freshly created branch connects in.
	//   - A Cluster that exists but whose status the operator has not populated
	//     yet, reported as the unknown phase.
	if status.InstanceCount == 0 {
		return true
	}

	if _, ok := startingFromZeroPhases[status.Status]; ok {
		return true
	}

	return noInstanceHasPod(status)
}

// noInstanceHasPod reports whether the cluster has instances but none of them
// has a pod. This is the first window of a scale-to-zero wake: hibernation
// deletes the pod and keeps the PVC, and CNPG derives the instance list from
// PVCs, so the count stays at one. When the hibernation annotation is removed
// the controller sees no pod to wait for and moves straight to creating one
// without leaving the healthy phase, so neither the count check nor the phase
// check above recognises the wake. The clusters service fills each such
// instance with a status that is not a CNPG pod status because the Cluster
// reports no pod for it.
//
// A crash-looping primary keeps its pod, which CNPG reports as replicating
// or failed. An empty map carries no evidence either way, so it is not held for.
func noInstanceHasPod(status *clustersv1.ClusterStatus) bool {
	if len(status.Instances) == 0 {
		return false
	}
	for _, instance := range status.Instances {
		switch instance.GetStatus() {
		case apiv1.PodHealthy, apiv1.PodReplicating, apiv1.PodFailed:
			return false
		}
	}
	return true
}

// shouldAttemptReactivation returns true for dial errors that warrant a
// cluster status lookup: connection refused (pod or Service not listening)
// or DNS not found (Service deleted during scale-to-zero).
func shouldAttemptReactivation(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	var dnsErr *net.DNSError
	return errors.As(err, &dnsErr) && dnsErr.IsNotFound
}
