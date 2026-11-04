package session

import (
	"context"
	"errors"
	"net"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	clustersv1 "xata/gen/proto/clusters/v1"
	"xata/gen/protomocks"
	"xata/services/gateway/metrics"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClusterDialer_Dial(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	errTest := errors.New("oh noes")
	errDNS := &net.DNSError{Err: "server misbehaving", Name: "branch-rw.svc", IsTemporary: true}

	tests := map[string]struct {
		dialer     *mockDialer
		endpoint   string
		setupMocks func(*protomocks.ClustersServiceClient)

		wantDialCalls    uint // exact expected dial count; ignored if wantMinDialCalls is set
		wantMinDialCalls uint // for timeout-driven tests where the exact count depends on tick timing
		wantErr          error
	}{
		"ok - no dial error": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, _ uint, network, address string) (net.Conn, error) {
					return &net.TCPConn{}, nil
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {},

			wantDialCalls: 1,
			wantErr:       nil,
		},
		"ok - hibernated cluster with scale to zero reactivates on dial": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					case 2:
						return &net.TCPConn{}, nil
					default:
						return nil, errors.New("unexpected dial call")
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType: clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: true},
					},
				}, nil).Once()

				mockClusters.EXPECT().UpdatePostgresCluster(ctx, &clustersv1.UpdatePostgresClusterRequest{
					Id: "test-branch",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						Hibernate: new(false),
					},
				}).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()

				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
						InstanceCount:      1,
						InstanceReadyCount: 1,
					},
				}, nil).Once()
			},

			wantDialCalls: 2,
			wantErr:       nil,
		},
		"ok - DNS not found triggers reactivation for hibernated cluster": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, &net.DNSError{Err: "no such host", Name: "branch-rw.xata-clusters.svc", IsNotFound: true}
					case 2:
						return &net.TCPConn{}, nil
					default:
						return nil, errors.New("unexpected dial call")
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType: clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: true},
					},
				}, nil).Once()

				mockClusters.EXPECT().UpdatePostgresCluster(ctx, &clustersv1.UpdatePostgresClusterRequest{
					Id: "test-branch",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						Hibernate: new(false),
					},
				}).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()

				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
						InstanceCount:      1,
						InstanceReadyCount: 1,
					},
				}, nil).Once()
			},

			wantDialCalls: 2,
			wantErr:       nil,
		},
		"ok - connection refused scale to zero enabled and hibernated cluster, reactivation ongoing": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					case 2:
						return &net.TCPConn{}, nil
					default:
						return nil, errors.New("unexpected dial call")
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType: clustersv1.ClusterStatus_STATUS_TYPE_TRANSIENT,
						Status:     apiv1.PhaseWaitingForInstancesToBeActive,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: true},
					},
				}, nil).Once()

				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_TRANSIENT,
						InstanceCount:      2,
						InstanceReadyCount: 1,
						Instances: map[string]*clustersv1.InstanceStatus{
							"instance-1": {
								Primary: true,
								Status:  apiv1.PodHealthy,
							},
							"instance-2": {
								Primary: false,
								Status:  apiv1.PodFailed,
							},
						},
					},
				}, nil).Once()
			},

			wantDialCalls: 2,
			wantErr:       nil,
		},
		"ok - connection refused scale to zero enabled and hibernated cluster, reactivates cluster with only primary": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					case 2:
						return &net.TCPConn{}, nil
					default:
						return nil, errors.New("unexpected dial call")
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType: clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: true},
					},
				}, nil).Once()

				mockClusters.EXPECT().UpdatePostgresCluster(ctx, &clustersv1.UpdatePostgresClusterRequest{
					Id: "test-branch",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						Hibernate: new(false),
					},
				}).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()

				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_TRANSIENT,
						InstanceCount:      2,
						InstanceReadyCount: 1,
						Instances: map[string]*clustersv1.InstanceStatus{
							"instance-1": {
								Primary: true,
								Status:  apiv1.PodHealthy,
							},
							"instance-2": {
								Primary: false,
								Status:  apiv1.PodFailed,
							},
						},
					},
				}, nil).Once()
			},

			wantDialCalls: 2,
			wantErr:       nil,
		},
		"ok - connection refused scale to zero enabled and hibernated cluster, reactivates cluster waiting for instances to be ready": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					case 2:
						return &net.TCPConn{}, nil
					default:
						return nil, errors.New("unexpected dial call")
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType: clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: true},
					},
				}, nil).Once()

				mockClusters.EXPECT().UpdatePostgresCluster(ctx, &clustersv1.UpdatePostgresClusterRequest{
					Id: "test-branch",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						Hibernate: new(false),
					},
				}).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()

				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
						InstanceCount:      1,
						InstanceReadyCount: 0,
					},
				}, nil).Once()
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
						InstanceCount:      1,
						InstanceReadyCount: 1,
					},
				}, nil).Once()
			},

			wantDialCalls: 2,
			wantErr:       nil,
		},
		"ok - connection refused with scale to zero disabled, cluster healthy, waits then connects": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					default:
						return &net.TCPConn{}, nil
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
						InstanceCount:      1,
						InstanceReadyCount: 1,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: false},
					},
				}, nil)
			},

			wantDialCalls: 2,
			wantErr:       nil,
		},
		"error - manually hibernated cluster returns ErrBranchHibernated": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					default:
						return nil, errors.New("unexpected dial call")
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType: clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: false},
					},
				}, nil).Once()
			},

			wantDialCalls: 1,
			wantErr:       ErrBranchHibernated,
		},
		// A concurrent request already reactivated the cluster (so it reports
		// HEALTHY, not HIBERNATED) but the dial target — typically the pooler
		// Service — isn't routable yet. The connection must be held and the
		// target re-probed until it succeeds, rather than failing fast.
		"ok - scale to zero enabled, cluster healthy but target briefly unreachable, waits then connects": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					default:
						return &net.TCPConn{}, nil
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
						InstanceCount:      1,
						InstanceReadyCount: 1,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: true},
					},
				}, nil)
			},

			wantDialCalls: 2,
			wantErr:       nil,
		},
		// Same race as above, but the target never recovers: the dialer keeps
		// re-probing until reactivateTimeout, then surfaces the dial error.
		"error - scale to zero enabled, cluster healthy but target stays unreachable, returns dial error": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, _ uint, network, address string) (net.Conn, error) {
					return nil, syscall.ECONNREFUSED
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
						InstanceCount:      1,
						InstanceReadyCount: 1,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: true},
					},
				}, nil)
			},

			wantMinDialCalls: 2,
			wantErr:          syscall.ECONNREFUSED,
		},
		// A branch on a wakeup pool that has not been assigned a cluster yet.
		// The clusters service has no Cluster resource to derive a status from,
		// so it synthesizes one: the healthy phase with a Transient status type
		// and no instances. A freshly created branch connects in exactly this
		// window, so the connection has to be held until the assigned cluster
		// comes up. The phase is not one of startingFromZeroPhases, so the
		// no-instance-reported check is what keeps this waiting.
		"ok - branch awaiting a wakeup pool assignment, waits then connects": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					default:
						return &net.TCPConn{}, nil
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						Status:     apiv1.PhaseHealthy,
						StatusType: clustersv1.ClusterStatus_STATUS_TYPE_TRANSIENT,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: true},
					},
				}, nil).Once()

				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						Status:             apiv1.PhaseHealthy,
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
						InstanceCount:      1,
						InstanceReadyCount: 1,
					},
				}, nil).Once()
			},

			wantDialCalls: 2,
			wantErr:       nil,
		},
		// A Cluster that exists but whose status the operator has not populated
		// yet: the clusters service reports the unknown phase with a Transient
		// status type. Nothing is running and nothing can be crash-looping, so
		// the connection is held.
		"ok - cluster status not populated yet, waits then connects": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					default:
						return &net.TCPConn{}, nil
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						Status:     "unknown",
						StatusType: clustersv1.ClusterStatus_STATUS_TYPE_TRANSIENT,
					},
					Configuration: &clustersv1.ClusterConfiguration{},
				}, nil).Once()

				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						Status:             apiv1.PhaseHealthy,
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
						InstanceCount:      1,
						InstanceReadyCount: 1,
					},
				}, nil).Once()
			},

			wantDialCalls: 2,
			wantErr:       nil,
		},
		// StatusType is derived from the Cluster resource and lags the instances,
		// so it can still read Healthy while the primary is crashed or in
		// recovery and nothing can accept connections. There is nothing to wait
		// for, so the dial error is surfaced immediately rather than holding the
		// client connection for the full reactivate timeout. The single dial
		// call is the assertion that matters: it proves waitUntilReachable was
		// never entered, so the clusters service is not polled either.
		"error - cluster reports healthy but no instance is ready, fails fast without holding": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, _ uint, network, address string) (net.Conn, error) {
					return nil, syscall.ECONNREFUSED
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
						InstanceCount:      1,
						InstanceReadyCount: 0,
					},
					Configuration: &clustersv1.ClusterConfiguration{},
				}, nil)
			},

			wantDialCalls: 1,
			wantErr:       syscall.ECONNREFUSED,
		},
		// Same fail-fast path, reached with a Transient cluster whose primary is
		// restarting in place and reported unhealthy. This is the shape seen
		// when a primary is being OOM-killed repeatedly. A restart phase is not
		// distinguishable from a crash loop, so it is not held for: an in-place
		// restart of a running cluster is excluded from startingFromZeroPhases.
		"error - cluster restarting primary in place, fails fast without holding": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, _ uint, network, address string) (net.Conn, error) {
					return nil, syscall.ECONNREFUSED
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						Status:             apiv1.PhaseInplacePrimaryRestart,
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_TRANSIENT,
						InstanceCount:      1,
						InstanceReadyCount: 0,
						Instances: map[string]*clustersv1.InstanceStatus{
							"test-branch-1": {
								Primary: true,
								Status:  apiv1.PodFailed,
							},
						},
					},
					Configuration: &clustersv1.ClusterConfiguration{},
				}, nil)
			},

			wantDialCalls: 1,
			wantErr:       syscall.ECONNREFUSED,
		},
		// A scale-to-zero wake that another connection triggered moments ago.
		// Hibernation deleted the pod but kept the PVC, so CNPG still counts one
		// instance, and the controller has not yet created the new pod, so the
		// phase still reads healthy. The clusters service reports the instance
		// with a placeholder status because the Cluster has no pod for it. This
		// is the shape a concurrent request sees in the first reconcile of a
		// wake, and it must be held rather than surfaced as a dial error.
		"ok - wake in flight with PVC but no pod yet, waits then connects": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					case 2:
						return &net.TCPConn{}, nil
					default:
						return nil, errors.New("unexpected dial call")
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						Status:             apiv1.PhaseHealthy,
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_TRANSIENT,
						InstanceCount:      1,
						InstanceReadyCount: 0,
						Instances: map[string]*clustersv1.InstanceStatus{
							"test-branch-1": {
								Primary: true,
								Status:  "Unknown",
							},
						},
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: true},
					},
				}, nil).Once()

				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						Status:             apiv1.PhaseHealthy,
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
						InstanceCount:      1,
						InstanceReadyCount: 1,
						Instances: map[string]*clustersv1.InstanceStatus{
							"test-branch-1": {
								Primary: true,
								Status:  apiv1.PodHealthy,
							},
						},
					},
				}, nil).Once()
			},

			wantDialCalls: 2,
			wantErr:       nil,
		},
		// The same counts and phase as the wake above, but the primary has a
		// pod and CNPG classifies it as replicating: a running pod that is not
		// ready, which is what a crash-looping primary looks like. The pod is
		// evidence that nothing is being created, so this still fails fast.
		"error - cluster healthy phase with crash-looping primary, fails fast without holding": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, _ uint, network, address string) (net.Conn, error) {
					return nil, syscall.ECONNREFUSED
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						Status:             apiv1.PhaseHealthy,
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_TRANSIENT,
						InstanceCount:      1,
						InstanceReadyCount: 0,
						Instances: map[string]*clustersv1.InstanceStatus{
							"test-branch-1": {
								Primary: true,
								Status:  apiv1.PodReplicating,
							},
						},
					},
					Configuration: &clustersv1.ClusterConfiguration{},
				}, nil)
			},

			wantDialCalls: 1,
			wantErr:       syscall.ECONNREFUSED,
		},
		// Simulates a hibernated cluster that reactivates successfully (Postgres
		// instances come back) but the dial target (e.g. the pooler Service)
		// stays unreachable. waitUntilReachable retries the probe-dial until
		// reactivateTimeout, then surfaces the original dial error.
		"error - hibernated cluster reactivates but target stays unreachable, returns dial error": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, _ uint, network, address string) (net.Conn, error) {
					return nil, syscall.ECONNREFUSED
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType: clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: true},
					},
				}, nil).Once()

				mockClusters.EXPECT().UpdatePostgresCluster(ctx, &clustersv1.UpdatePostgresClusterRequest{
					Id: "test-branch",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						Hibernate: new(false),
					},
				}).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()

				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
						InstanceCount:      1,
						InstanceReadyCount: 1,
					},
				}, nil).Once()
			},

			wantMinDialCalls: 2,
			wantErr:          syscall.ECONNREFUSED,
		},
		"error - read-only endpoint on single-instance branch fails fast": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, _ uint, network, address string) (net.Conn, error) {
					return nil, syscall.ECONNREFUSED
				},
			},
			endpoint: EndpointRO,
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						Status:             apiv1.PhaseHealthy,
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
						InstanceCount:      1,
						InstanceReadyCount: 1,
					},
					Configuration: &clustersv1.ClusterConfiguration{NumInstances: 1},
				}, nil).Once()
			},

			wantDialCalls: 1,
			wantErr:       ErrNoReadReplica,
		},
		"ok - read-only endpoint on branch scaling up to a replica waits": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					case 2:
						return &net.TCPConn{}, nil
					default:
						return nil, errors.New("unexpected dial call")
					}
				},
			},
			endpoint: EndpointRO,
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						Status:             apiv1.PhaseHealthy,
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_TRANSIENT,
						InstanceCount:      2,
						InstanceReadyCount: 1,
						Instances: map[string]*clustersv1.InstanceStatus{
							"test-branch-1": {Primary: true, Status: apiv1.PodHealthy},
							"test-branch-2": {Primary: false, Status: apiv1.PodReplicating},
						},
					},
					Configuration: &clustersv1.ClusterConfiguration{NumInstances: 2},
				}, nil).Twice()
			},

			wantDialCalls: 2,
			wantErr:       nil,
		},
		"error - connection refused, error describing cluster, returns dial error": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					default:
						return nil, errors.New("unexpected dial call")
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(nil, errTest).Once()
			},

			wantDialCalls: 1,
			wantErr:       syscall.ECONNREFUSED,
		},
		"error - connection refused, error updating cluster, returns dial error": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					default:
						return nil, errors.New("unexpected dial call")
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType: clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: true},
					},
				}, nil).Once()

				mockClusters.EXPECT().UpdatePostgresCluster(ctx, &clustersv1.UpdatePostgresClusterRequest{
					Id: "test-branch",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						Hibernate: new(false),
					},
				}).Return(nil, errTest).Once()
			},

			wantDialCalls: 1,
			wantErr:       syscall.ECONNREFUSED,
		},
		"error - connection refused, error describing cluster after update, returns dial error": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					default:
						return nil, errors.New("unexpected dial call")
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType: clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: true},
					},
				}, nil).Once()

				mockClusters.EXPECT().UpdatePostgresCluster(ctx, &clustersv1.UpdatePostgresClusterRequest{
					Id: "test-branch",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						Hibernate: new(false),
					},
				}).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()

				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(nil, errTest).Once()
			},

			wantDialCalls: 1,
			wantErr:       syscall.ECONNREFUSED,
		},
		"error - connection refused with hibernated cluster and scale to zero enabled, timeout on reactivation returns dial error": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
					switch i {
					case 1:
						return nil, syscall.ECONNREFUSED
					default:
						return nil, errors.New("unexpected dial call")
					}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(&clustersv1.DescribePostgresClusterResponse{
					Status: &clustersv1.ClusterStatus{
						StatusType: clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED,
					},
					Configuration: &clustersv1.ClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{Enabled: true},
					},
				}, nil)

				mockClusters.EXPECT().UpdatePostgresCluster(ctx, &clustersv1.UpdatePostgresClusterRequest{
					Id: "test-branch",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						Hibernate: new(false),
					},
				}).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()
			},

			wantDialCalls: 1,
			wantErr:       syscall.ECONNREFUSED,
		},
		"error - other dial error": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, _ uint, network, address string) (net.Conn, error) {
					return nil, errTest
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
			},

			wantDialCalls: 1,
			wantErr:       errTest,
		},
		"error - temporary DNS error does not trigger reactivation": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, _ uint, network, address string) (net.Conn, error) {
					return nil, errDNS
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {},

			wantDialCalls: 1,
			wantErr:       errDNS,
		},
		"error - transient clusters service unavailable returns dial error": {
			dialer: &mockDialer{
				dialFn: func(ctx context.Context, _ uint, network, address string) (net.Conn, error) {
					return nil, syscall.ECONNREFUSED
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(nil, status.Error(codes.Unavailable, "connection refused")).Once()
			},

			wantDialCalls: 1,
			wantErr:       syscall.ECONNREFUSED,
		},
		"error - terminated branch unknown to clusters service returns ErrBranchNotFound": {
			dialer: &mockDialer{
				dialFn: func(_ context.Context, _ uint, _, address string) (net.Conn, error) {
					return nil, &net.DNSError{Err: "no such host", Name: address, IsNotFound: true}
				},
			},
			setupMocks: func(mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{
					Id: "test-branch",
				}).Return(nil, status.Error(codes.NotFound, "resource not found")).Once()
			},

			wantDialCalls: 1,
			wantErr:       ErrBranchNotFound,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			mockClusters := protomocks.NewClustersServiceClient(t)
			tc.setupMocks(mockClusters)

			d := NewClusterDialer(ClusterDialerConfiguration{
				ReactivateTimeout:   time.Second,
				StatusCheckInterval: time.Millisecond * 100,
			}, mockClusters, WithDialer(tc.dialer.Dial))

			_, err := d.Dial(ctx, "tcp", &Branch{
				ID:       "test-branch",
				Address:  "test-branch-address",
				Endpoint: tc.endpoint,
			})
			require.ErrorIs(t, err, tc.wantErr)
			if tc.wantMinDialCalls > 0 {
				require.GreaterOrEqual(t, tc.dialer.DialCalls(), tc.wantMinDialCalls, "unexpected number of dial calls")
			} else {
				require.Equal(t, tc.wantDialCalls, tc.dialer.DialCalls(), "unexpected number of dial calls")
			}

			mockClusters.AssertExpectations(t)
		})
	}
}

// TestClusterDialer_ReactivationMetrics checks pool and instance size labels
// and outcomes for reactivation attempts. Connections that only wait do not
// record a sample.
func TestClusterDialer_ReactivationMetrics(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	errRPC := status.Error(codes.Internal, "boom")

	hibernated := &clustersv1.DescribePostgresClusterResponse{
		UsesWakeupPool: new(true),
		Status: &clustersv1.ClusterStatus{
			StatusType: clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED,
		},
		Configuration: &clustersv1.ClusterConfiguration{
			ScaleToZero: &clustersv1.ScaleToZero{Enabled: true},
			VcpuRequest: "500m",
			Memory:      "1",
		},
	}
	sized := metrics.AttrInstanceSize.String("500m/1GB")
	healthy := &clustersv1.DescribePostgresClusterResponse{
		Status: &clustersv1.ClusterStatus{
			Status:             apiv1.PhaseHealthy,
			StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
			InstanceCount:      1,
			InstanceReadyCount: 1,
		},
		Configuration: &clustersv1.ClusterConfiguration{},
	}

	// Build fresh RPC messages in each test rather than sharing across
	// parallel subtests
	reactivate := func() *clustersv1.UpdatePostgresClusterRequest {
		return &clustersv1.UpdatePostgresClusterRequest{
			Id: "test-branch",
			UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
				Hibernate: new(false),
			},
		}
	}
	describe := func() *clustersv1.DescribePostgresClusterRequest {
		return &clustersv1.DescribePostgresClusterRequest{Id: "test-branch"}
	}

	refusedThenOK := func(ctx context.Context, i uint, network, address string) (net.Conn, error) {
		if i == 1 {
			return nil, syscall.ECONNREFUSED
		}
		return &net.TCPConn{}, nil
	}
	alwaysRefused := func(ctx context.Context, _ uint, network, address string) (net.Conn, error) {
		return nil, syscall.ECONNREFUSED
	}

	type testCase struct {
		dialFn     func(ctx context.Context, i uint, network, address string) (net.Conn, error)
		setupMocks func(*protomocks.ClustersServiceClient)

		wantErr      error
		wantAttrs    attribute.Set
		wantNoMetric bool
	}

	tests := map[string]testCase{
		"reactivated - this connection triggered the wake": {
			dialFn: refusedThenOK,
			setupMocks: func(m *protomocks.ClustersServiceClient) {
				m.EXPECT().DescribePostgresCluster(mock.Anything, describe()).Return(hibernated, nil).Once()
				m.EXPECT().UpdatePostgresCluster(ctx, reactivate()).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()
				m.EXPECT().DescribePostgresCluster(mock.Anything, describe()).Return(healthy, nil).Once()
			},
			wantAttrs: attribute.NewSet(
				metrics.AttrPool.Bool(true),
				metrics.AttrSuccess.Bool(true),
				sized,
			),
		},
		"waited - wake already in flight": {
			dialFn: refusedThenOK,
			setupMocks: func(m *protomocks.ClustersServiceClient) {
				m.EXPECT().DescribePostgresCluster(mock.Anything, describe()).Return(healthy, nil).Twice()
			},
			wantNoMetric: true,
		},
		"reactivated - timed out": {
			dialFn: alwaysRefused,
			setupMocks: func(m *protomocks.ClustersServiceClient) {
				m.EXPECT().DescribePostgresCluster(mock.Anything, describe()).Return(hibernated, nil).Once()
				m.EXPECT().UpdatePostgresCluster(ctx, reactivate()).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()
				m.EXPECT().DescribePostgresCluster(mock.Anything, describe()).Return(healthy, nil)
			},
			wantErr:   syscall.ECONNREFUSED,
			wantAttrs: attribute.NewSet(metrics.AttrPool.Bool(true), metrics.AttrSuccess.Bool(false), metrics.AttrErrorType.String(metrics.WaitErrorTimeout), sized),
		},
		"reactivated - clusters service rpc failed": {
			dialFn: alwaysRefused,
			setupMocks: func(m *protomocks.ClustersServiceClient) {
				m.EXPECT().DescribePostgresCluster(mock.Anything, describe()).Return(hibernated, nil).Once()
				m.EXPECT().UpdatePostgresCluster(ctx, reactivate()).Return(nil, errRPC).Once()
			},
			wantErr: syscall.ECONNREFUSED,
			wantAttrs: attribute.NewSet(
				metrics.AttrPool.Bool(true),
				metrics.AttrSuccess.Bool(false),
				metrics.AttrErrorType.String(metrics.WaitErrorRPC),
				sized,
			),
		},
	}

	for name, pool := range map[string]*bool{"non-pooled": new(false), "older server": nil} {
		cluster := &clustersv1.DescribePostgresClusterResponse{
			Configuration:  hibernated.Configuration,
			Status:         hibernated.Status,
			UsesWakeupPool: pool,
		}
		attrs := []attribute.KeyValue{metrics.AttrSuccess.Bool(true), metrics.AttrPool.Bool(false), sized}
		tests[name] = testCase{
			dialFn: refusedThenOK,
			setupMocks: func(m *protomocks.ClustersServiceClient) {
				m.EXPECT().DescribePostgresCluster(mock.Anything, describe()).Return(cluster, nil).Once()
				m.EXPECT().UpdatePostgresCluster(ctx, reactivate()).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()
				m.EXPECT().DescribePostgresCluster(mock.Anything, describe()).Return(healthy, nil).Once()
			},
			wantAttrs: attribute.NewSet(attrs...),
		}
	}

	// A configuration without resource values (e.g. an older clusters
	// service) records the sample without an instance size label rather
	// than an empty one.
	unsized := &clustersv1.DescribePostgresClusterResponse{
		UsesWakeupPool: new(true),
		Status:         hibernated.Status,
		Configuration: &clustersv1.ClusterConfiguration{
			ScaleToZero: &clustersv1.ScaleToZero{Enabled: true},
		},
	}
	tests["reactivated - unknown instance size"] = testCase{
		dialFn: refusedThenOK,
		setupMocks: func(m *protomocks.ClustersServiceClient) {
			m.EXPECT().DescribePostgresCluster(ctx, describe()).Return(unsized, nil).Once()
			m.EXPECT().UpdatePostgresCluster(ctx, reactivate()).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()
			// The post-wake status poll is shared between callers, so it runs
			// on the coalescer's context rather than this caller's
			m.EXPECT().DescribePostgresCluster(mock.Anything, describe()).Return(healthy, nil).Once()
		},
		wantAttrs: attribute.NewSet(metrics.AttrPool.Bool(true), metrics.AttrSuccess.Bool(true)),
	}

	for name, cause := range map[string]error{
		"canceled":          context.Canceled,
		"deadline exceeded": context.DeadlineExceeded,
	} {
		err := status.FromContextError(cause).Err()
		tests["reactivated - "+name+" during update"] = testCase{
			dialFn: alwaysRefused,
			setupMocks: func(m *protomocks.ClustersServiceClient) {
				m.EXPECT().DescribePostgresCluster(mock.Anything, describe()).Return(hibernated, nil).Once()
				m.EXPECT().UpdatePostgresCluster(ctx, reactivate()).Return(nil, err).Once()
			},
			wantErr: syscall.ECONNREFUSED,
			wantAttrs: attribute.NewSet(
				metrics.AttrPool.Bool(true),
				metrics.AttrSuccess.Bool(false),
				metrics.AttrErrorType.String(metrics.WaitErrorCanceled),
				sized,
			),
		}
		tests["reactivated - "+name+" during describe"] = testCase{
			dialFn: alwaysRefused,
			setupMocks: func(m *protomocks.ClustersServiceClient) {
				m.EXPECT().DescribePostgresCluster(mock.Anything, describe()).Return(hibernated, nil).Once()
				m.EXPECT().UpdatePostgresCluster(ctx, reactivate()).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()
				m.EXPECT().DescribePostgresCluster(mock.Anything, describe()).Return(nil, err).Once()
			},
			wantErr: syscall.ECONNREFUSED,
			wantAttrs: attribute.NewSet(
				metrics.AttrPool.Bool(true),
				metrics.AttrSuccess.Bool(false),
				metrics.AttrErrorType.String(metrics.WaitErrorCanceled),
				sized,
			),
		}
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
			gwMetrics, err := metrics.New(mp.Meter("test"))
			require.NoError(t, err)

			mockClusters := protomocks.NewClustersServiceClient(t)
			tc.setupMocks(mockClusters)

			d := NewClusterDialer(ClusterDialerConfiguration{
				ReactivateTimeout:   200 * time.Millisecond,
				StatusCheckInterval: 20 * time.Millisecond,
			}, mockClusters, WithDialer((&mockDialer{dialFn: tc.dialFn}).Dial),
				WithInstrumentation(gwMetrics))

			_, err = d.Dial(ctx, "tcp", &Branch{ID: "test-branch", Address: "test-branch-address"})
			require.ErrorIs(t, err, tc.wantErr)

			var rm metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(ctx, &rm))
			var hist metricdata.Histogram[float64]
			found := false
			for _, sm := range rm.ScopeMetrics {
				for _, m := range sm.Metrics {
					if m.Name == "xata.gateway.cluster.reactivation_duration_seconds" {
						hist, found = m.Data.(metricdata.Histogram[float64])
					}
				}
			}
			if tc.wantNoMetric {
				require.False(t, found, "wait-only connection recorded a reactivation")
				return
			}
			require.True(t, found, "reactivation histogram not collected")
			require.Len(t, hist.DataPoints, 1)
			dp := hist.DataPoints[0]
			require.Equal(t, uint64(1), dp.Count)
			require.True(t, dp.Attributes.Equals(&tc.wantAttrs), "got attributes %v", dp.Attributes.ToSlice())
		})
	}
}

// TestClusterDialer_ConcurrentDialsShareStatusPoll checks that connections
// held for the same cluster share one status poll rather than each polling
// the clusters service on their own.
func TestClusterDialer_ConcurrentDialsShareStatusPoll(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()

		var mu sync.Mutex
		describes := 0
		svc := pollingStatusService{describe: func() (*clustersv1.DescribePostgresClusterResponse, error) {
			mu.Lock()
			describes++
			mu.Unlock()
			return &clustersv1.DescribePostgresClusterResponse{Status: &clustersv1.ClusterStatus{
				StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
				InstanceCount:      1,
				InstanceReadyCount: 1,
			}}, nil
		}}

		// The target refuses connections for the first 250ms, so every dial
		// fails, looks the cluster up, and is held until it becomes reachable
		d := NewClusterDialer(ClusterDialerConfiguration{
			StatusCheckInterval: 100 * time.Millisecond,
			ReactivateTimeout:   50 * time.Second,
		}, svc, WithDialer(func(context.Context, string, string) (net.Conn, error) {
			if time.Since(start) < 250*time.Millisecond {
				return nil, syscall.ECONNREFUSED
			}
			return &net.TCPConn{}, nil
		}))

		const dials = 10
		errs := make([]error, dials)
		var wg sync.WaitGroup
		for i := range dials {
			wg.Go(func() {
				_, errs[i] = d.Dial(context.Background(), "tcp", &Branch{ID: "test-branch", Address: "test-branch-address"})
			})
		}
		wg.Wait()

		for i, err := range errs {
			require.NoError(t, err, "dial %d", i)
		}
		// Each dial describes the cluster once on its own to decide whether to
		// hold, then all of them share a single poll that sees the cluster
		// available on its first tick
		require.Equal(t, dials+1, describes, "held connections should share one status poll")
	})
}

type mockDialer struct {
	dialCalls uint
	dialFn    func(ctx context.Context, i uint, network, address string) (net.Conn, error)
}

func (m *mockDialer) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	m.dialCalls++
	return m.dialFn(ctx, m.dialCalls, network, address)
}

func (m *mockDialer) DialCalls() uint {
	return m.dialCalls
}

func TestNewNetDialer(t *testing.T) {
	tests := map[string]struct {
		userTimeout time.Duration
	}{
		"system default": {
			userTimeout: 0,
		},
		"with user timeout": {
			userTimeout: 30 * time.Second,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer listener.Close()

			accepted := make(chan struct{})
			go func() {
				defer close(accepted)
				conn, err := listener.Accept()
				if err == nil {
					conn.Close()
				}
			}()

			// The Control hook runs during dial, so a successful connection
			// means the socket option was accepted by the kernel. On non-Linux
			// platforms setTCPUserTimeout is a no-op and this just checks the
			// dialer is still usable.
			conn, err := newNetDialer(test.userTimeout)(t.Context(), "tcp", listener.Addr().String())
			require.NoError(t, err)
			require.NoError(t, conn.Close())
			<-accepted
		})
	}
}
