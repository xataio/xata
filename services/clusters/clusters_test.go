package clusters

import (
	"context"
	"fmt"
	"testing"
	"time"

	barmanPluginApi "github.com/cloudnative-pg/plugin-barman-cloud/api/v1"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clustersv1 "xata/gen/proto/clusters/v1"
	cpv1alpha1 "xata/proto/clusterpool-operator/api/v1alpha1"
	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/clusters/internal/connectors/cnpg"
	cnpgmocks "xata/services/clusters/internal/connectors/cnpg/mocks"
	openebsmocks "xata/services/clusters/internal/connectors/openebs/mocks"
)

var pitrTimestamp = time.Date(2024, 6, 15, 14, 30, 0, 0, time.UTC)

const testImage = "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5"

var expectedPrivilegedExtensions = MandatoryPostgresParameters(testImage)["xatautils.privileged_extensions"]

func TestCreatePostgresCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		nodeSelector       map[string]string
		parentBranch       *v1alpha1.Branch
		sourceCluster      *apiv1.Cluster
		objectStore        *barmanPluginApi.ObjectStore
		extraObjects       []client.Object
		requestFn          func(r *clustersv1.CreatePostgresClusterRequest)
		expectedBranchFn   func(b *v1alpha1.Branch)
		expectedStatusCode codes.Code
	}{
		{
			name:             "main branch - with backups and scale to zero",
			parentBranch:     nil,
			requestFn:        nil,
			expectedBranchFn: nil,
		},
		{
			name:         "main branch - backups disabled",
			parentBranch: nil,
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.BackupConfiguration.BackupsEnabled = false
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.BackupSpec = nil
			},
		},
		{
			name:         "main branch - scale to zero disabled",
			parentBranch: nil,
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.Configuration.ScaleToZero.Enabled = false
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.ScaleToZero = &v1alpha1.ScaleToZeroConfiguration{
					Enabled:                 false,
					InactivityPeriodMinutes: 30,
				}
			},
		},
		{
			name:         "main branch - storage size is defaulted to service default",
			parentBranch: nil,
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.Configuration.StorageSize = 0
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Storage.Size = "256Gi"
			},
		},
		{
			name:         "main branch - node selector is set to service default",
			nodeSelector: map[string]string{"karpenter.sh/nodepool": "default"},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Affinity = &v1alpha1.AffinitySpec{
					NodeSelector: map[string]string{"karpenter.sh/nodepool": "default"},
				}
			},
		},
		{
			name: "child branch - settings overridden by parent branch",
			parentBranch: parentBranch(
				withStorageClass("some-other-storage-class"),
				withVolumeSnapshotClass("some-other-snapshot-class"),
				withBackupDisabled(),
			),
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.ParentId = new("gmnfj6042d3qd09dcc8a7le0eo")
				r.DataSource = &clustersv1.CreatePostgresClusterRequest_ClusterSnapshot{
					ClusterSnapshot: &clustersv1.ClusterSnapshot{
						ClusterId: "gmnfj6042d3qd09dcc8a7le0eo",
					},
				}
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.Restore = &v1alpha1.RestoreSpec{
					Type: v1alpha1.RestoreTypeVolumeSnapshot,
					Name: "gmnfj6042d3qd09dcc8a7le0eo",
				}
				b.Spec.ClusterSpec.Instances = 1
				b.Spec.ClusterSpec.Image = "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:16.3"
				b.Spec.ClusterSpec.Storage.Size = "200Gi"
				b.Spec.ClusterSpec.Storage.StorageClass = new("some-other-storage-class")
				b.Spec.ClusterSpec.Storage.VolumeSnapshotClass = new("some-other-snapshot-class")
				b.Spec.ClusterSpec.Resources = corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("1"),
						corev1.ResourceMemory: resource.MustParse("1948Mi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("2"),
						corev1.ResourceMemory: resource.MustParse("1948Mi"),
					},
				}
				b.Spec.ClusterSpec.Postgres.Parameters = updatePostgresParam(b.Spec.ClusterSpec.Postgres.Parameters, "max_connections", "100")
				b.Spec.ClusterSpec.Postgres.Parameters = updatePostgresParam(b.Spec.ClusterSpec.Postgres.Parameters, "shared_buffers", "128MB")
				b.Spec.ClusterSpec.Postgres.SharedPreloadLibraries = []string{"xatautils", "pg_stat_statements"}
				b.Spec.BackupSpec = nil
			},
		},
		{
			name: "child branch - deduplicates xatautils from parent",
			parentBranch: parentBranch(
				withSharedPreloadLibraries([]string{"xatautils", "pg_stat_statements", "xatautils"}),
			),
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.ParentId = new("gmnfj6042d3qd09dcc8a7le0eo")
				r.DataSource = &clustersv1.CreatePostgresClusterRequest_ClusterSnapshot{
					ClusterSnapshot: &clustersv1.ClusterSnapshot{
						ClusterId: "gmnfj6042d3qd09dcc8a7le0eo",
					},
				}
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.Restore = &v1alpha1.RestoreSpec{
					Type: v1alpha1.RestoreTypeVolumeSnapshot,
					Name: "gmnfj6042d3qd09dcc8a7le0eo",
				}
				b.Spec.ClusterSpec.Instances = 1
				b.Spec.ClusterSpec.Image = "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:16.3"
				b.Spec.ClusterSpec.Storage.Size = "200Gi"
				b.Spec.ClusterSpec.Resources = corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("1"),
						corev1.ResourceMemory: resource.MustParse("1948Mi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("2"),
						corev1.ResourceMemory: resource.MustParse("1948Mi"),
					},
				}
				b.Spec.ClusterSpec.Postgres.Parameters = updatePostgresParam(b.Spec.ClusterSpec.Postgres.Parameters, "max_connections", "100")
				b.Spec.ClusterSpec.Postgres.Parameters = updatePostgresParam(b.Spec.ClusterSpec.Postgres.Parameters, "shared_buffers", "128MB")
				b.Spec.ClusterSpec.Postgres.SharedPreloadLibraries = []string{"xatautils", "pg_stat_statements"}
			},
		},
		{
			name: "error - child branch with non-existent parent cluster",
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.ParentId = new("non-existent-cluster")
				r.DataSource = &clustersv1.CreatePostgresClusterRequest_ClusterSnapshot{
					ClusterSnapshot: &clustersv1.ClusterSnapshot{
						ClusterId: "non-existent-cluster",
					},
				}
			},
			expectedStatusCode: codes.NotFound,
		},
		{
			name:         "restore from BaseBackup",
			parentBranch: nil,
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.DataSource = &clustersv1.CreatePostgresClusterRequest_BaseBackup{
					BaseBackup: &clustersv1.BaseBackup{
						BackupId: "backup-20240615-143000",
					},
				}
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.Restore = &v1alpha1.RestoreSpec{
					Type: v1alpha1.RestoreTypeBaseBackup,
					Name: "backup-20240615-143000",
				}
			},
		},
		{
			name:          "restore from ContinuousBackup without timestamp (latest)",
			parentBranch:  nil,
			sourceCluster: sourceClusterForPITR(),
			objectStore:   objectStoreForPITR(),
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.DataSource = &clustersv1.CreatePostgresClusterRequest_ContinuousBackup{
					ContinuousBackup: &clustersv1.ContinuousBackup{
						ClusterId: "source-cluster-for-pitr",
					},
				}
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.Restore = &v1alpha1.RestoreSpec{
					Type: v1alpha1.RestoreTypeObjectStore,
					Name: "source-cluster-for-pitr",
				}
			},
		},
		{
			name:          "restore from ContinuousBackup with timestamp (PITR)",
			parentBranch:  nil,
			sourceCluster: sourceClusterForPITR(),
			objectStore:   objectStoreForPITR(),
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.DataSource = &clustersv1.CreatePostgresClusterRequest_ContinuousBackup{
					ContinuousBackup: &clustersv1.ContinuousBackup{
						ClusterId: "source-cluster-for-pitr",
						Timestamp: timestamppb.New(pitrTimestamp),
					},
				}
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.Restore = &v1alpha1.RestoreSpec{
					Type:      v1alpha1.RestoreTypeObjectStore,
					Name:      "source-cluster-for-pitr",
					Timestamp: &metav1.Time{Time: pitrTimestamp},
				}
			},
		},
		{
			name:          "error - restore from ContinuousBackup with non-existent source cluster",
			parentBranch:  nil,
			sourceCluster: nil,
			objectStore:   nil,
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.DataSource = &clustersv1.CreatePostgresClusterRequest_ContinuousBackup{
					ContinuousBackup: &clustersv1.ContinuousBackup{
						ClusterId: "non-existent-source-cluster",
					},
				}
			},
			expectedStatusCode: codes.NotFound,
		},
		{
			name:          "error - restore from ContinuousBackup with non-existent ObjectStore",
			parentBranch:  nil,
			sourceCluster: sourceClusterForPITR(),
			objectStore:   nil,
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.DataSource = &clustersv1.CreatePostgresClusterRequest_ContinuousBackup{
					ContinuousBackup: &clustersv1.ContinuousBackup{
						ClusterId: "source-cluster-for-pitr",
					},
				}
			},
			expectedStatusCode: codes.NotFound,
		},
		{
			name:          "error - restore from ContinuousBackup with ObjectStore missing recovery window",
			parentBranch:  nil,
			sourceCluster: sourceClusterForPITR(),
			objectStore: &barmanPluginApi.ObjectStore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "source-cluster-for-pitr",
					Namespace: "xata-clusters",
				},
				Status: barmanPluginApi.ObjectStoreStatus{
					ServerRecoveryWindow: map[string]barmanPluginApi.RecoveryWindow{},
				},
			},
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.DataSource = &clustersv1.CreatePostgresClusterRequest_ContinuousBackup{
					ContinuousBackup: &clustersv1.ContinuousBackup{
						ClusterId: "source-cluster-for-pitr",
					},
				}
			},
			expectedStatusCode: codes.NotFound,
		},
		{
			name:          "error - restore from ContinuousBackup with ObjectStore having nil FirstRecoverabilityPoint",
			parentBranch:  nil,
			sourceCluster: sourceClusterForPITR(),
			objectStore: &barmanPluginApi.ObjectStore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "source-cluster-for-pitr",
					Namespace: "xata-clusters",
				},
				Status: barmanPluginApi.ObjectStoreStatus{
					ServerRecoveryWindow: map[string]barmanPluginApi.RecoveryWindow{
						"source-cluster-for-pitr": {
							FirstRecoverabilityPoint: nil,
						},
					},
				},
			},
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.DataSource = &clustersv1.CreatePostgresClusterRequest_ContinuousBackup{
					ContinuousBackup: &clustersv1.ContinuousBackup{
						ClusterId: "source-cluster-for-pitr",
					},
				}
			},
			expectedStatusCode: codes.NotFound,
		},
		{
			name:          "error - restore from ContinuousBackup with empty ClusterId",
			parentBranch:  nil,
			sourceCluster: nil,
			objectStore:   nil,
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.DataSource = &clustersv1.CreatePostgresClusterRequest_ContinuousBackup{
					ContinuousBackup: &clustersv1.ContinuousBackup{
						ClusterId: "",
					},
				}
			},
			expectedStatusCode: codes.InvalidArgument,
		},
		{
			name: "use_pool with matching pool - adopts pool cluster",
			extraObjects: []client.Object{
				poolForTest("default-storage-class", testImage, "2", "4Gi"),
				poolClusterForTest(),
			},
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.UsePool = new(true)
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Name = new("pool-cluster-1")
				b.Spec.BackupSpec = nil
			},
		},
		{
			name: "use_pool with no matching pool - falls back to normal creation",
			extraObjects: []client.Object{
				poolForTest("other-storage-class", testImage, "2", "4Gi"),
			},
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.UsePool = new(true)
			},
		},
		{
			name: "use_pool false - normal creation unchanged",
			extraObjects: []client.Object{
				poolForTest("default-storage-class", testImage, "2", "4Gi"),
				poolClusterForTest(),
			},
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.UsePool = new(false)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			var existingObjs []client.Object
			if tt.parentBranch != nil {
				existingObjs = append(existingObjs, tt.parentBranch)
			}
			if tt.sourceCluster != nil {
				existingObjs = append(existingObjs, tt.sourceCluster)
			}
			if tt.objectStore != nil {
				existingObjs = append(existingObjs, tt.objectStore)
			}
			existingObjs = append(existingObjs, tt.extraObjects...)
			svc, k8sClient := setupTestClustersService(t,
				withExistingObjects(existingObjs...),
				withNodeSelector(tt.nodeSelector))

			req, expectedBranch, _, _, _ := exampleRequestsAndBranches()
			if tt.requestFn != nil {
				tt.requestFn(req)
			}
			if tt.expectedBranchFn != nil {
				tt.expectedBranchFn(expectedBranch)
			}

			resp, err := svc.CreatePostgresCluster(ctx, req)
			st, _ := status.FromError(err)
			require.Equal(t, tt.expectedStatusCode, st.Code())

			if tt.expectedStatusCode == codes.OK {
				branch, err := getBranchFromK8s(ctx, k8sClient, resp.GetId())
				require.NoError(t, err)

				if expectedBranch.Spec.Restore != nil && expectedBranch.Spec.Restore.Timestamp != nil {
					require.NotNil(t, branch.Spec.Restore)
					require.NotNil(t, branch.Spec.Restore.Timestamp)
					require.True(t, expectedBranch.Spec.Restore.Timestamp.Time.Equal(branch.Spec.Restore.Timestamp.Time))
					expectedBranch.Spec.Restore.Timestamp = nil
					branch.Spec.Restore.Timestamp = nil
				}

				require.Equal(t, expectedBranch.Spec, branch.Spec)
				require.Equal(t, expectedBranch.Labels, branch.Labels)
				require.Equal(t, resp.GetId(), branch.Name)
			}
		})
	}
}

func TestUpdatePostgresCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		extraObjects       []client.Object
		inputBranchFn      func(b *v1alpha1.Branch)
		requestFn          func(r *clustersv1.UpdatePostgresClusterRequest)
		expectedBranchFn   func(b *v1alpha1.Branch)
		expectedStatusCode codes.Code
	}{
		{
			name:             "full update of all fields with hibernate disabled",
			requestFn:        nil,
			expectedBranchFn: nil,
		},
		{
			name: "full update of all fields with hibernate enabled",
			requestFn: func(r *clustersv1.UpdatePostgresClusterRequest) {
				r.UpdateConfiguration.Hibernate = new(true)
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Hibernation = ptr.To(v1alpha1.HibernationModeEnabled)
			},
		},
		{
			name: "hibernate with wakeup-pool annotation uses pool-style hibernation",
			inputBranchFn: func(b *v1alpha1.Branch) {
				b.Annotations = map[string]string{
					v1alpha1.WakeupPoolAnnotation: "test-pool",
				}
			},
			requestFn: func(r *clustersv1.UpdatePostgresClusterRequest) {
				r.UpdateConfiguration.Hibernate = new(true)
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Name = nil
				b.Spec.ClusterSpec.Hibernation = nil
			},
		},
		{
			name: "update storage size increase",
			requestFn: func(r *clustersv1.UpdatePostgresClusterRequest) {
				r.UpdateConfiguration.StorageSize = new(int32(200))
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Storage.Size = "200Gi"
			},
		},
		{
			name: "update storage size same value",
			requestFn: func(r *clustersv1.UpdatePostgresClusterRequest) {
				r.UpdateConfiguration.StorageSize = new(int32(100))
			},
		},
		{
			name: "error - storage size decrease",
			requestFn: func(r *clustersv1.UpdatePostgresClusterRequest) {
				r.UpdateConfiguration.StorageSize = new(int32(50))
			},
			expectedStatusCode: codes.InvalidArgument,
		},
		{
			name: "error - storage size exceeds maximum",
			requestFn: func(r *clustersv1.UpdatePostgresClusterRequest) {
				r.UpdateConfiguration.StorageSize = new(int32(2048))
			},
			expectedStatusCode: codes.InvalidArgument,
		},
		{
			name: "update non-existent branch",
			requestFn: func(r *clustersv1.UpdatePostgresClusterRequest) {
				r.Id = "non-existent-branch"
			},
			expectedStatusCode: codes.NotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			_, branchToUpdate, _, req, expectedBranch := exampleRequestsAndBranches()

			if tt.inputBranchFn != nil {
				tt.inputBranchFn(branchToUpdate)
			}

			existingObjs := append([]client.Object{branchToUpdate}, tt.extraObjects...)
			svc, k8sClient := setupTestClustersService(t, withExistingObjects(existingObjs...))

			if tt.requestFn != nil {
				tt.requestFn(req)
			}
			if tt.expectedBranchFn != nil {
				tt.expectedBranchFn(expectedBranch)
			}

			_, err := svc.UpdatePostgresCluster(ctx, req)
			st, _ := status.FromError(err)
			require.Equal(t, tt.expectedStatusCode, st.Code())

			if tt.expectedStatusCode == codes.OK {
				br, err := getBranchFromK8s(ctx, k8sClient, req.GetId())
				require.NoError(t, err)

				require.Equal(t, expectedBranch.Spec, br.Spec)
				require.Equal(t, expectedBranch.Labels, br.Labels)
			}
		})
	}
}

func TestDeletePostgresCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		requestFn          func(r *clustersv1.DeletePostgresClusterRequest)
		expectedStatusCode codes.Code
	}{
		{
			name:      "delete existing branch",
			requestFn: nil,
		},
		{
			name: "delete non-existent branch",
			requestFn: func(r *clustersv1.DeletePostgresClusterRequest) {
				r.Id = "non-existent-branch"
			},
			expectedStatusCode: codes.NotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			_, branch, _, _, _ := exampleRequestsAndBranches()
			existingObjs := []client.Object{branch}
			svc, k8sClient := setupTestClustersService(t, withExistingObjects(existingObjs...))

			req := &clustersv1.DeletePostgresClusterRequest{
				Id: branch.Name,
			}
			if tt.requestFn != nil {
				tt.requestFn(req)
			}

			_, err := svc.DeletePostgresCluster(ctx, req)
			st, _ := status.FromError(err)
			require.Equal(t, tt.expectedStatusCode, st.Code())

			if tt.expectedStatusCode == codes.OK {
				_, err := getBranchFromK8s(ctx, k8sClient, req.GetId())
				require.True(t, errors.IsNotFound(err))
			}
		})
	}
}

func TestDescribePostgresCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		existingObjects func() []client.Object
		requestID       string
		wantResp        *clustersv1.DescribePostgresClusterResponse
		wantErr         string
	}{
		{
			name: "healthy branch with backup configuration",
			existingObjects: func() []client.Object {
				return []client.Object{
					&v1alpha1.Branch{
						ObjectMeta: metav1.ObjectMeta{Name: "test-branch"},
						Spec: v1alpha1.BranchSpec{
							ClusterSpec: v1alpha1.ClusterSpec{
								Name:      new("test-branch-cluster"),
								Instances: 2,
								Image:     testImage,
								Storage: v1alpha1.StorageSpec{
									Size: "100Gi",
								},
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceCPU:    resource.MustParse("2"),
										corev1.ResourceMemory: resource.MustParse("3996Mi"),
									},
									Limits: corev1.ResourceList{
										corev1.ResourceCPU:    resource.MustParse("4"),
										corev1.ResourceMemory: resource.MustParse("3996Mi"),
									},
								},
								Postgres: &v1alpha1.PostgresConfiguration{
									Parameters: []v1alpha1.PostgresParameter{
										{Name: "max_connections", Value: "200"},
										{Name: "shared_buffers", Value: "256MB"},
									},
									SharedPreloadLibraries: []string{"pg_stat_statements"},
								},
								ScaleToZero: &v1alpha1.ScaleToZeroConfiguration{
									Enabled:                 true,
									InactivityPeriodMinutes: 15,
								},
							},
							BackupSpec: &v1alpha1.BackupSpec{
								ScheduledBackup: &v1alpha1.ScheduledBackupSpec{
									Schedule: "0 2 * * *",
								},
								Retention: "7d",
							},
						},
					},
					&apiv1.Cluster{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-branch-cluster",
							Namespace: "xata-clusters",
						},
						Spec: apiv1.ClusterSpec{
							StorageConfiguration: apiv1.StorageConfiguration{
								Size: "100Gi",
							},
						},
						Status: apiv1.ClusterStatus{
							Phase:          apiv1.PhaseHealthy,
							Instances:      2,
							ReadyInstances: 2,
							CurrentPrimary: "test-branch-cluster-1",
							InstancesStatus: map[apiv1.PodStatus][]string{
								apiv1.PodHealthy: {"test-branch-cluster-1", "test-branch-cluster-2"},
							},
						},
					},
				}
			},
			requestID: "test-branch",
			wantResp: &clustersv1.DescribePostgresClusterResponse{
				Id: "test-branch",
				Configuration: &clustersv1.ClusterConfiguration{
					NumInstances: 2,
					StorageSize:  100,
					ImageName:    testImage,
					VcpuRequest:  "2",
					VcpuLimit:    "4",
					Memory:       "4",
					ScaleToZero: &clustersv1.ScaleToZero{
						Enabled:                 true,
						InactivityPeriodMinutes: 15,
					},
					PostgresConfigurationParameters: map[string]string{
						"max_connections": "200",
						"shared_buffers":  "256MB",
					},
					PreloadLibraries: []string{"pg_stat_statements"},
				},
				Status: &clustersv1.ClusterStatus{
					Status:             apiv1.PhaseHealthy,
					StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
					InstanceCount:      2,
					InstanceReadyCount: 2,
					Instances: map[string]*clustersv1.InstanceStatus{
						"test-branch-cluster-1": {Status: string(apiv1.PodHealthy), Primary: true},
						"test-branch-cluster-2": {Status: string(apiv1.PodHealthy)},
					},
				},
				BackupConfiguration: &clustersv1.BackupConfiguration{
					BackupsEnabled:  true,
					BackupSchedule:  "0 2 * * *",
					BackupRetention: "7d",
				},
			},
		},
		{
			name: "hibernated branch",
			existingObjects: func() []client.Object {
				return []client.Object{
					&v1alpha1.Branch{
						ObjectMeta: metav1.ObjectMeta{Name: "test-branch"},
						Spec: v1alpha1.BranchSpec{
							ClusterSpec: v1alpha1.ClusterSpec{
								Name:        new("test-branch-cluster"),
								Instances:   1,
								Hibernation: ptr.To(v1alpha1.HibernationModeEnabled),
								Image:       testImage,
								Storage: v1alpha1.StorageSpec{
									Size: "100Gi",
								},
								Postgres: &v1alpha1.PostgresConfiguration{},
							},
						},
					},
					&apiv1.Cluster{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-branch-cluster",
							Namespace: "xata-clusters",
							Annotations: map[string]string{
								"cnpg.io/hibernation": "on",
							},
						},
						Spec: apiv1.ClusterSpec{
							StorageConfiguration: apiv1.StorageConfiguration{
								Size: "100Gi",
							},
						},
						Status: apiv1.ClusterStatus{
							Phase: apiv1.PhaseHealthy,
						},
					},
				}
			},
			requestID: "test-branch",
			wantResp: &clustersv1.DescribePostgresClusterResponse{
				Id: "test-branch",
				Configuration: &clustersv1.ClusterConfiguration{
					NumInstances:                    1,
					StorageSize:                     100,
					ImageName:                       testImage,
					VcpuRequest:                     "0m",
					VcpuLimit:                       "0m",
					Memory:                          "0",
					Hibernate:                       true,
					PostgresConfigurationParameters: map[string]string{},
					ScaleToZero: &clustersv1.ScaleToZero{
						Enabled: false,
					},
				},
				Status: &clustersv1.ClusterStatus{
					Status:     apiv1.PhaseHealthy,
					StatusType: clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED,
					Instances:  map[string]*clustersv1.InstanceStatus{},
				},
				BackupConfiguration: &clustersv1.BackupConfiguration{
					BackupsEnabled: false,
				},
			},
		},
		{
			name: "branch in fault state",
			existingObjects: func() []client.Object {
				return []client.Object{
					&v1alpha1.Branch{
						ObjectMeta: metav1.ObjectMeta{Name: "test-branch"},
						Spec: v1alpha1.BranchSpec{
							ClusterSpec: v1alpha1.ClusterSpec{
								Name:      new("test-branch-cluster"),
								Instances: 2,
								Image:     testImage,
								Storage: v1alpha1.StorageSpec{
									Size: "100Gi",
								},
								Postgres: &v1alpha1.PostgresConfiguration{},
							},
						},
					},
					&apiv1.Cluster{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-branch-cluster",
							Namespace: "xata-clusters",
						},
						Spec: apiv1.ClusterSpec{
							StorageConfiguration: apiv1.StorageConfiguration{
								Size: "100Gi",
							},
						},
						Status: apiv1.ClusterStatus{
							Phase:          apiv1.PhaseUnrecoverable,
							Instances:      2,
							ReadyInstances: 0,
						},
					},
				}
			},
			requestID: "test-branch",
			wantResp: &clustersv1.DescribePostgresClusterResponse{
				Id: "test-branch",
				Configuration: &clustersv1.ClusterConfiguration{
					NumInstances:                    2,
					StorageSize:                     100,
					ImageName:                       testImage,
					VcpuRequest:                     "0m",
					VcpuLimit:                       "0m",
					Memory:                          "0",
					PostgresConfigurationParameters: map[string]string{},
					ScaleToZero: &clustersv1.ScaleToZero{
						Enabled: false,
					},
				},
				Status: &clustersv1.ClusterStatus{
					Status:             apiv1.PhaseUnrecoverable,
					StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_FAULT,
					InstanceCount:      2,
					InstanceReadyCount: 0,
					Instances:          map[string]*clustersv1.InstanceStatus{},
				},
				BackupConfiguration: &clustersv1.BackupConfiguration{
					BackupsEnabled: false,
				},
			},
		},
		{
			name: "branch CR with no associated cluster",
			existingObjects: func() []client.Object {
				_, branch, _, _, _ := exampleRequestsAndBranches()
				branch.Spec.ClusterSpec.Name = nil
				return []client.Object{branch}
			},
			requestID: "lsmevenv7t3l56euo1v9bh3b74",
			wantResp: func() *clustersv1.DescribePostgresClusterResponse {
				_, _, resp, _, _ := exampleRequestsAndBranches()
				resp.Status = &clustersv1.ClusterStatus{
					Status:     apiv1.PhaseHealthy,
					StatusType: clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED,
				}
				return resp
			}(),
		},
		{
			name: "branch with cluster name but cluster not yet created",
			existingObjects: func() []client.Object {
				_, branch, _, _, _ := exampleRequestsAndBranches()
				return []client.Object{branch}
			},
			requestID: "lsmevenv7t3l56euo1v9bh3b74",
			wantResp: func() *clustersv1.DescribePostgresClusterResponse {
				_, _, resp, _, _ := exampleRequestsAndBranches()
				resp.Status = &clustersv1.ClusterStatus{
					Status:     apiv1.PhaseHealthy,
					StatusType: clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED,
				}
				return resp
			}(),
		},
		{
			name:      "no branch CR returns not found",
			requestID: "test-branch",
			wantErr:   "not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var objs []client.Object
			if tt.existingObjects != nil {
				objs = tt.existingObjects()
			}

			svc, _ := setupTestClustersService(t,
				withExistingObjects(objs...))

			resp, err := svc.DescribePostgresCluster(context.Background(),
				&clustersv1.DescribePostgresClusterRequest{Id: tt.requestID})

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantResp, resp)
		})
	}
}

func TestGetPostgresClusterCredentials(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		setupMock func(m *cnpgmocks.Connector)
		wantUser  string
		wantErr   string
	}{
		"success": {
			setupMock: func(m *cnpgmocks.Connector) {
				m.EXPECT().GetClusterCredentials(mock.Anything, "cluster-1", "xata-clusters", "app").
					Return(&cnpg.Credentials{Username: "app", Password: "secret"}, nil)
			},
			wantUser: "app",
		},
		"secret not found": {
			setupMock: func(m *cnpgmocks.Connector) {
				m.EXPECT().GetClusterCredentials(mock.Anything, "cluster-1", "xata-clusters", "app").
					Return(nil, fmt.Errorf(`secrets "cluster-1-app" not found`))
			},
			wantErr: "secret not found",
		},
		"other error": {
			setupMock: func(m *cnpgmocks.Connector) {
				m.EXPECT().GetClusterCredentials(mock.Anything, "cluster-1", "xata-clusters", "app").
					Return(nil, fmt.Errorf("connection refused"))
			},
			wantErr: "get credentials",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			cnpgMock := cnpgmocks.NewConnector(t)
			tt.setupMock(cnpgMock)

			svc, _ := setupTestClustersService(t, withCNPGConnector(cnpgMock))

			resp, err := svc.GetPostgresClusterCredentials(context.Background(),
				&clustersv1.GetPostgresClusterCredentialsRequest{Id: "cluster-1", Username: "app"})

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantUser, resp.Username)
			require.Equal(t, "secret", resp.Password)
		})
	}
}

func TestRegisterPostgresCluster(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		setupMock func(m *cnpgmocks.Connector)
		wantErr   string
	}{
		"success": {
			setupMock: func(m *cnpgmocks.Connector) {
				m.EXPECT().RegisterCluster(mock.Anything, "cluster-1", "xata-clusters", "xata").
					Return(nil)
			},
		},
		"error": {
			setupMock: func(m *cnpgmocks.Connector) {
				m.EXPECT().RegisterCluster(mock.Anything, "cluster-1", "xata-clusters", "xata").
					Return(fmt.Errorf("service already exists"))
			},
			wantErr: "register:",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			cnpgMock := cnpgmocks.NewConnector(t)
			tt.setupMock(cnpgMock)

			svc, _ := setupTestClustersService(t, withCNPGConnector(cnpgMock))

			resp, err := svc.RegisterPostgresCluster(context.Background(),
				&clustersv1.RegisterPostgresClusterRequest{Id: "cluster-1"})

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, resp)
		})
	}
}

func TestDeregisterPostgresCluster(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		setupMock func(m *cnpgmocks.Connector)
		wantErr   string
	}{
		"success": {
			setupMock: func(m *cnpgmocks.Connector) {
				m.EXPECT().DeregisterCluster(mock.Anything, "cluster-1", "xata-clusters", "xata").
					Return(nil)
			},
		},
		"error": {
			setupMock: func(m *cnpgmocks.Connector) {
				m.EXPECT().DeregisterCluster(mock.Anything, "cluster-1", "xata-clusters", "xata").
					Return(fmt.Errorf("service not found"))
			},
			wantErr: "deregister:",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			cnpgMock := cnpgmocks.NewConnector(t)
			tt.setupMock(cnpgMock)

			svc, _ := setupTestClustersService(t, withCNPGConnector(cnpgMock))

			resp, err := svc.DeregisterPostgresCluster(context.Background(),
				&clustersv1.DeregisterPostgresClusterRequest{Id: "cluster-1"})

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, resp)
		})
	}
}

func TestGetCellUtilization(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		setupMock func(m *openebsmocks.Connector)
		wantBytes uint64
		wantErr   string
	}{
		"success": {
			setupMock: func(m *openebsmocks.Connector) {
				bytes := uint64(1024 * 1024 * 1024 * 100)
				m.EXPECT().AvailableSpaceBytes(mock.Anything).
					Return(&bytes, nil)
			},
			wantBytes: 1024 * 1024 * 1024 * 100,
		},
		"error": {
			setupMock: func(m *openebsmocks.Connector) {
				m.EXPECT().AvailableSpaceBytes(mock.Anything).
					Return(nil, fmt.Errorf("lvm not available"))
			},
			wantErr: "get cell utilization",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			openebsMock := openebsmocks.NewConnector(t)
			tt.setupMock(openebsMock)

			svc, _ := setupTestClustersService(t, withOpenEBSConnector(openebsMock))

			resp, err := svc.GetCellUtilization(context.Background(),
				&clustersv1.GetCellUtilizationRequest{})

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, &tt.wantBytes, resp.AvailableBytes)
		})
	}
}

func TestGetObjectStore(t *testing.T) {
	t.Parallel()

	recoveryTime := metav1.Now()

	tests := map[string]struct {
		setupMock func(m *cnpgmocks.Connector)
		wantErr   string
		validate  func(t *testing.T, resp *clustersv1.GetObjectStoreResponse)
	}{
		"success with recovery windows": {
			setupMock: func(m *cnpgmocks.Connector) {
				m.EXPECT().GetObjectStore(mock.Anything, "cluster-1", "xata-clusters").
					Return(&barmanPluginApi.ObjectStore{
						Status: barmanPluginApi.ObjectStoreStatus{
							ServerRecoveryWindow: map[string]barmanPluginApi.RecoveryWindow{
								"cluster-1": {
									FirstRecoverabilityPoint: &recoveryTime,
									LastSuccessfulBackupTime: &recoveryTime,
									LastFailedBackupTime:     &recoveryTime,
								},
							},
						},
					}, nil)
			},
			validate: func(t *testing.T, resp *clustersv1.GetObjectStoreResponse) {
				require.Contains(t, resp.Status.ServerRecoveryWindow, "cluster-1")
				w := resp.Status.ServerRecoveryWindow["cluster-1"]
				require.NotEmpty(t, w.FirstRecoverabilityPoint)
				require.NotEmpty(t, w.LastSuccessfulBackupTime)
				require.NotEmpty(t, w.LastFailedBackupTime)
			},
		},
		"not found": {
			setupMock: func(m *cnpgmocks.Connector) {
				m.EXPECT().GetObjectStore(mock.Anything, "cluster-1", "xata-clusters").
					Return(nil, fmt.Errorf(`objectstores.barman.io "cluster-1" not found`))
			},
			wantErr: "object store not found",
		},
		"other error": {
			setupMock: func(m *cnpgmocks.Connector) {
				m.EXPECT().GetObjectStore(mock.Anything, "cluster-1", "xata-clusters").
					Return(nil, fmt.Errorf("connection refused"))
			},
			wantErr: "get object store status",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			cnpgMock := cnpgmocks.NewConnector(t)
			tt.setupMock(cnpgMock)

			svc, _ := setupTestClustersService(t, withCNPGConnector(cnpgMock))

			resp, err := svc.GetObjectStore(context.Background(),
				&clustersv1.GetObjectStoreRequest{Id: "cluster-1"})

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			if tt.validate != nil {
				tt.validate(t, resp)
			}
		})
	}
}

func TestSetBranchIPFiltering(t *testing.T) {
	tests := []struct {
		name    string
		request *clustersv1.SetBranchIPFilteringRequest
		wantErr string
	}{
		{
			name: "sets IP filtering for a branch",
			request: &clustersv1.SetBranchIPFilteringRequest{
				BranchId: "branch-1",
				IpFiltering: &clustersv1.IPFilteringConfig{
					Enabled: true,
					Allowed: []string{"10.0.0.0/8"},
				},
			},
		},
		{
			name: "missing branch_id returns error",
			request: &clustersv1.SetBranchIPFilteringRequest{
				IpFiltering: &clustersv1.IPFilteringConfig{Enabled: true},
			},
			wantErr: "branch_id is required",
		},
		{
			name: "nil ip_filtering returns error",
			request: &clustersv1.SetBranchIPFilteringRequest{
				BranchId: "branch-1",
			},
			wantErr: "ip_filtering is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service, _ := setupTestClustersService(t)

			resp, err := service.SetBranchIPFiltering(context.Background(), tt.request)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Nil(t, resp)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, resp)

			getResp, err := service.GetBranchIPFiltering(context.Background(), &clustersv1.GetBranchIPFilteringRequest{
				BranchId: tt.request.BranchId,
			})
			require.NoError(t, err)
			require.Equal(t, tt.request.IpFiltering.Enabled, getResp.IpFiltering.Enabled)
			require.Equal(t, tt.request.IpFiltering.Allowed, getResp.IpFiltering.Allowed)
		})
	}
}

func TestSetBranchesIPFiltering(t *testing.T) {
	tests := []struct {
		name    string
		request *clustersv1.SetBranchesIPFilteringRequest
		wantErr string
	}{
		{
			name: "sets IP filtering for multiple branches",
			request: &clustersv1.SetBranchesIPFilteringRequest{
				BranchIds: []string{"branch-1", "branch-2"},
				IpFiltering: &clustersv1.IPFilteringConfig{
					Enabled: true,
					Allowed: []string{"10.0.0.0/8"},
				},
			},
		},
		{
			name: "empty branch_ids returns error",
			request: &clustersv1.SetBranchesIPFilteringRequest{
				BranchIds:   []string{},
				IpFiltering: &clustersv1.IPFilteringConfig{Enabled: true},
			},
			wantErr: "at least one branch_id is required",
		},
		{
			name: "nil ip_filtering returns error",
			request: &clustersv1.SetBranchesIPFilteringRequest{
				BranchIds: []string{"branch-1"},
			},
			wantErr: "ip_filtering is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service, _ := setupTestClustersService(t)

			resp, err := service.SetBranchesIPFiltering(context.Background(), tt.request)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Nil(t, resp)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, resp)

			for _, branchID := range tt.request.BranchIds {
				getResp, err := service.GetBranchIPFiltering(context.Background(), &clustersv1.GetBranchIPFilteringRequest{
					BranchId: branchID,
				})
				require.NoError(t, err)
				require.Equal(t, tt.request.IpFiltering.Enabled, getResp.IpFiltering.Enabled)
				require.Equal(t, tt.request.IpFiltering.Allowed, getResp.IpFiltering.Allowed)
			}
		})
	}
}

func TestGetBranchIPFiltering(t *testing.T) {
	tests := []struct {
		name    string
		request *clustersv1.GetBranchIPFilteringRequest
		setup   func(t *testing.T, svc *ClustersService)
		want    *clustersv1.IPFilteringConfig
		wantErr string
	}{
		{
			name:    "missing branch_id returns error",
			request: &clustersv1.GetBranchIPFilteringRequest{},
			wantErr: "branch_id is required",
		},
		{
			name:    "non-existent branch returns default (disabled)",
			request: &clustersv1.GetBranchIPFilteringRequest{BranchId: "branch-1"},
			want:    &clustersv1.IPFilteringConfig{Enabled: false, Allowed: []string{}},
		},
		{
			name:    "returns stored config",
			request: &clustersv1.GetBranchIPFilteringRequest{BranchId: "branch-1"},
			setup: func(t *testing.T, svc *ClustersService) {
				_, err := svc.SetBranchIPFiltering(context.Background(), &clustersv1.SetBranchIPFilteringRequest{
					BranchId: "branch-1",
					IpFiltering: &clustersv1.IPFilteringConfig{
						Enabled: true,
						Allowed: []string{"192.168.0.0/16"},
					},
				})
				require.NoError(t, err)
			},
			want: &clustersv1.IPFilteringConfig{Enabled: true, Allowed: []string{"192.168.0.0/16"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service, _ := setupTestClustersService(t)

			if tt.setup != nil {
				tt.setup(t, service)
			}

			resp, err := service.GetBranchIPFiltering(context.Background(), tt.request)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Nil(t, resp)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want.Enabled, resp.IpFiltering.Enabled)
			require.Equal(t, tt.want.Allowed, resp.IpFiltering.Allowed)
		})
	}
}

func TestDeleteBranchIPFiltering(t *testing.T) {
	tests := []struct {
		name    string
		request *clustersv1.DeleteBranchIPFilteringRequest
		setup   func(t *testing.T, svc *ClustersService)
		wantErr string
	}{
		{
			name:    "missing branch_id returns error",
			request: &clustersv1.DeleteBranchIPFilteringRequest{},
			wantErr: "branch_id is required",
		},
		{
			name:    "deleting non-existent branch is no-op",
			request: &clustersv1.DeleteBranchIPFilteringRequest{BranchId: "branch-1"},
		},
		{
			name:    "deletes existing branch config",
			request: &clustersv1.DeleteBranchIPFilteringRequest{BranchId: "branch-1"},
			setup: func(t *testing.T, svc *ClustersService) {
				_, err := svc.SetBranchIPFiltering(context.Background(), &clustersv1.SetBranchIPFilteringRequest{
					BranchId: "branch-1",
					IpFiltering: &clustersv1.IPFilteringConfig{
						Enabled: true,
						Allowed: []string{"10.0.0.0/8"},
					},
				})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service, _ := setupTestClustersService(t)

			if tt.setup != nil {
				tt.setup(t, service)
			}

			resp, err := service.DeleteBranchIPFiltering(context.Background(), tt.request)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Nil(t, resp)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, resp)

			getResp, err := service.GetBranchIPFiltering(context.Background(), &clustersv1.GetBranchIPFilteringRequest{
				BranchId: tt.request.BranchId,
			})
			require.NoError(t, err)
			require.False(t, getResp.IpFiltering.Enabled)
		})
	}
}

type testServiceConfig struct {
	existingObjects  []client.Object
	nodeSelector     map[string]string
	cnpgConnector    *cnpgmocks.Connector
	openebsConnector *openebsmocks.Connector
}

type testServiceOption func(*testServiceConfig)

func withExistingObjects(objects ...client.Object) testServiceOption {
	return func(c *testServiceConfig) {
		c.existingObjects = objects
	}
}

func withNodeSelector(nodeSelector map[string]string) testServiceOption {
	return func(c *testServiceConfig) {
		c.nodeSelector = nodeSelector
	}
}

func withCNPGConnector(m *cnpgmocks.Connector) testServiceOption {
	return func(c *testServiceConfig) {
		c.cnpgConnector = m
	}
}

func withOpenEBSConnector(m *openebsmocks.Connector) testServiceOption {
	return func(c *testServiceConfig) {
		c.openebsConnector = m
	}
}

func setupTestClustersService(t *testing.T, opts ...testServiceOption) (*ClustersService, client.Client) {
	cfg := &testServiceConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, apiv1.AddToScheme(scheme))
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	require.NoError(t, barmanPluginApi.AddToScheme(scheme))
	require.NoError(t, cpv1alpha1.AddToScheme(scheme))

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cfg.existingObjects...).
		WithIndex(&apiv1.Cluster{}, clusterOwnerKey, func(obj client.Object) []string {
			owner := metav1.GetControllerOf(obj)
			if owner == nil {
				return nil
			}
			if owner.APIVersion != cpv1alpha1.GroupVersion.String() || owner.Kind != "ClusterPool" {
				return nil
			}
			return []string{owner.Name}
		}).
		Build()

	cacheReady := make(chan struct{})
	close(cacheReady)

	svc := &ClustersService{
		config: Config{
			ClustersNamespace:           "xata-clusters",
			XataNamespace:               "xata",
			ClustersStorageRequest:      256,
			ClustersStorageClass:        "default-storage-class",
			ClustersVolumeSnapshotClass: "default-snapshot-class",
			ClustersNodeSelector:        cfg.nodeSelector,
		},
		kubeClient:       fakeClient,
		clusterReader:    fakeClient,
		clusterCacheOk:   cacheReady,
		cnpgConnector:    cfg.cnpgConnector,
		openebsConnector: cfg.openebsConnector,
	}

	return svc, fakeClient
}

func getBranchFromK8s(ctx context.Context, k8sClient client.Client, name string) (*v1alpha1.Branch, error) {
	br := &v1alpha1.Branch{}
	err := k8sClient.Get(ctx, client.ObjectKey{Name: name}, br)
	if err != nil {
		return nil, err
	}
	return br, err
}

func exampleRequestsAndBranches() (*clustersv1.CreatePostgresClusterRequest, *v1alpha1.Branch, *clustersv1.DescribePostgresClusterResponse, *clustersv1.UpdatePostgresClusterRequest, *v1alpha1.Branch) {
	return &clustersv1.CreatePostgresClusterRequest{
			Id:             "lsmevenv7t3l56euo1v9bh3b74",
			OrganizationId: "jf1tpn",
			ProjectId:      "prj_9tmrorf02l0gv9ioptqat1uc6k",
			Configuration: &clustersv1.ClusterConfiguration{
				ImageName:    testImage,
				NumInstances: 2,
				VcpuRequest:  "2",
				VcpuLimit:    "4",
				Memory:       "4Gi",
				StorageSize:  100,
				PostgresConfigurationParameters: map[string]string{
					"max_connections": "200",
					"shared_buffers":  "256MB",
				},
				PreloadLibraries: []string{
					"xatautils",
					"pg_stat_statements",
					"auto_explain",
				},
				ScaleToZero: &clustersv1.ScaleToZero{
					Enabled:                 true,
					InactivityPeriodMinutes: 30,
				},
			},
			BackupConfiguration: &clustersv1.BackupConfiguration{
				BackupsEnabled:  true,
				BackupSchedule:  "0 2 * * *",
				BackupRetention: "7d",
			},
		},
		&v1alpha1.Branch{
			ObjectMeta: metav1.ObjectMeta{
				Name: "lsmevenv7t3l56euo1v9bh3b74",
				Labels: map[string]string{
					LabelOrgID:     "jf1tpn",
					LabelProjectID: "prj_9tmrorf02l0gv9ioptqat1uc6k",
				},
			},
			Spec: v1alpha1.BranchSpec{
				ClusterSpec: v1alpha1.ClusterSpec{
					Name:      new("lsmevenv7t3l56euo1v9bh3b74"),
					Image:     testImage,
					Instances: 2,
					Storage: v1alpha1.StorageSpec{
						Size:                "100Gi",
						StorageClass:        new("default-storage-class"),
						VolumeSnapshotClass: new("default-snapshot-class"),
						MountPropagation:    new(string(corev1.MountPropagationHostToContainer)),
					},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("2"),
							corev1.ResourceMemory: resource.MustParse("3996Mi"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("4"),
							corev1.ResourceMemory: resource.MustParse("3996Mi"),
						},
					},
					Postgres: &v1alpha1.PostgresConfiguration{
						Parameters: []v1alpha1.PostgresParameter{
							{Name: "lc_messages", Value: "C.utf8"},
							{Name: "lc_monetary", Value: "C.utf8"},
							{Name: "lc_numeric", Value: "C.utf8"},
							{Name: "lc_time", Value: "C.utf8"},
							{Name: "max_connections", Value: "200"},
							{Name: "shared_buffers", Value: "256MB"},
							{Name: "xatautils.extension_custom_scripts_path", Value: "/etc/xatautils/extensions"},
							{Name: "xatautils.privileged_extensions", Value: expectedPrivilegedExtensions},
							{Name: "xatautils.privileged_role", Value: "xata"},
							{Name: "xatautils.privileged_role_allowed_configs", Value: "auth_delay.*, auto_explain.*, log_lock_waits, log_min_duration_statement, log_min_messages, log_replication_commands, log_statement, log_temp_files, pg_net.batch_size, pg_net.ttl, pg_stat_statements.*, pgaudit.log, pgaudit.log_catalog, pgaudit.log_client, pgaudit.log_level, pgaudit.log_relation, pgaudit.log_rows, pgaudit.log_statement, pgaudit.log_statement_once, pgaudit.role, pgrst.*, plan_filter.*, safeupdate.enabled, session_replication_role, track_io_timing, wal_compression"},
							{Name: "xatautils.reserved_memberships", Value: "xata_superuser, pg_read_server_files, pg_write_server_files, pg_execute_server_program"},
						},
						SharedPreloadLibraries: []string{
							"xatautils",
							"pg_stat_statements",
							"auto_explain",
						},
					},
					ScaleToZero: &v1alpha1.ScaleToZeroConfiguration{
						Enabled:                 true,
						InactivityPeriodMinutes: 30,
					},
				},
				BackupSpec: &v1alpha1.BackupSpec{
					Retention: "7d",
					ScheduledBackup: &v1alpha1.ScheduledBackupSpec{
						Schedule: "0 2 * * *",
					},
				},
				InheritedMetadata: &v1alpha1.InheritedMetadata{
					Labels: map[string]string{
						LabelOrgID:     "jf1tpn",
						LabelProjectID: "prj_9tmrorf02l0gv9ioptqat1uc6k",
					},
				},
			},
		},
		&clustersv1.DescribePostgresClusterResponse{
			Id: "lsmevenv7t3l56euo1v9bh3b74",
			Configuration: &clustersv1.ClusterConfiguration{
				NumInstances: 2,
				StorageSize:  100,
				ImageName:    testImage,
				VcpuRequest:  "2",
				VcpuLimit:    "4",
				Memory:       "4",
				Hibernate:    false,
				ScaleToZero: &clustersv1.ScaleToZero{
					Enabled:                 true,
					InactivityPeriodMinutes: 30,
				},
				PostgresConfigurationParameters: map[string]string{
					"lc_messages":     "C.utf8",
					"lc_monetary":     "C.utf8",
					"lc_numeric":      "C.utf8",
					"lc_time":         "C.utf8",
					"max_connections": "200",
					"shared_buffers":  "256MB",
					"xatautils.extension_custom_scripts_path":   "/etc/xatautils/extensions",
					"xatautils.privileged_extensions":           expectedPrivilegedExtensions,
					"xatautils.privileged_role":                 "xata",
					"xatautils.privileged_role_allowed_configs": "auth_delay.*, auto_explain.*, log_lock_waits, log_min_duration_statement, log_min_messages, log_replication_commands, log_statement, log_temp_files, pg_net.batch_size, pg_net.ttl, pg_stat_statements.*, pgaudit.log, pgaudit.log_catalog, pgaudit.log_client, pgaudit.log_level, pgaudit.log_relation, pgaudit.log_rows, pgaudit.log_statement, pgaudit.log_statement_once, pgaudit.role, pgrst.*, plan_filter.*, safeupdate.enabled, session_replication_role, track_io_timing, wal_compression",
					"xatautils.reserved_memberships":            "xata_superuser, pg_read_server_files, pg_write_server_files, pg_execute_server_program",
				},
				PreloadLibraries: []string{
					"xatautils",
					"pg_stat_statements",
					"auto_explain",
				},
			},
			Status: &clustersv1.ClusterStatus{
				Status:             "",
				InstanceCount:      0,
				InstanceReadyCount: 0,
				Instances:          map[string]*clustersv1.InstanceStatus{},
				StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_UNSPECIFIED,
			},
			BackupConfiguration: &clustersv1.BackupConfiguration{
				BackupsEnabled:  true,
				BackupSchedule:  "0 2 * * *",
				BackupRetention: "7d",
			},
		},
		&clustersv1.UpdatePostgresClusterRequest{
			Id: "lsmevenv7t3l56euo1v9bh3b74",
			UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
				NumInstances: new(int32(5)),
				ImageName:    new(testImage),
				Hibernate:    new(false),
				Memory:       new("8Gi"),
				VcpuRequest:  new("3"),
				VcpuLimit:    new("6"),
				PostgresConfigurationParameters: map[string]string{
					"max_connections":      "500",
					"shared_buffers":       "512MB",
					"work_mem":             "64MB",
					"maintenance_work_mem": "256MB",
				},
				PreloadLibraries: []string{
					"pg_stat_statements",
					"pgaudit",
					"pg_cron",
				},
				BackupConfiguration: &clustersv1.BackupConfiguration{
					BackupsEnabled:  true,
					BackupSchedule:  "0 7 * * *",
					BackupRetention: "14d",
				},
				ScaleToZero: &clustersv1.ScaleToZero{
					Enabled:                 true,
					InactivityPeriodMinutes: 60,
				},
			},
		},
		&v1alpha1.Branch{
			ObjectMeta: metav1.ObjectMeta{
				Name: "lsmevenv7t3l56euo1v9bh3b74",
				Labels: map[string]string{
					LabelOrgID:     "jf1tpn",
					LabelProjectID: "prj_9tmrorf02l0gv9ioptqat1uc6k",
				},
			},
			Spec: v1alpha1.BranchSpec{
				ClusterSpec: v1alpha1.ClusterSpec{
					Name:      new("lsmevenv7t3l56euo1v9bh3b74"),
					Image:     testImage,
					Instances: 5,
					Storage: v1alpha1.StorageSpec{
						Size:                "100Gi",
						StorageClass:        new("default-storage-class"),
						VolumeSnapshotClass: new("default-snapshot-class"),
						MountPropagation:    new(string(corev1.MountPropagationHostToContainer)),
					},
					Hibernation: ptr.To(v1alpha1.HibernationModeDisabled),
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("3"),
							corev1.ResourceMemory: resource.MustParse("8092Mi"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("6"),
							corev1.ResourceMemory: resource.MustParse("8092Mi"),
						},
					},
					Postgres: &v1alpha1.PostgresConfiguration{
						Parameters: []v1alpha1.PostgresParameter{
							{Name: "lc_messages", Value: "C.utf8"},
							{Name: "lc_monetary", Value: "C.utf8"},
							{Name: "lc_numeric", Value: "C.utf8"},
							{Name: "lc_time", Value: "C.utf8"},
							{Name: "maintenance_work_mem", Value: "256MB"},
							{Name: "max_connections", Value: "500"},
							{Name: "shared_buffers", Value: "512MB"},
							{Name: "work_mem", Value: "64MB"},
							{Name: "xatautils.extension_custom_scripts_path", Value: "/etc/xatautils/extensions"},
							{Name: "xatautils.privileged_extensions", Value: expectedPrivilegedExtensions},
							{Name: "xatautils.privileged_role", Value: "xata"},
							{Name: "xatautils.privileged_role_allowed_configs", Value: "auth_delay.*, auto_explain.*, log_lock_waits, log_min_duration_statement, log_min_messages, log_replication_commands, log_statement, log_temp_files, pg_net.batch_size, pg_net.ttl, pg_stat_statements.*, pgaudit.log, pgaudit.log_catalog, pgaudit.log_client, pgaudit.log_level, pgaudit.log_relation, pgaudit.log_rows, pgaudit.log_statement, pgaudit.log_statement_once, pgaudit.role, pgrst.*, plan_filter.*, safeupdate.enabled, session_replication_role, track_io_timing, wal_compression"},
							{Name: "xatautils.reserved_memberships", Value: "xata_superuser, pg_read_server_files, pg_write_server_files, pg_execute_server_program"},
						},
						SharedPreloadLibraries: []string{
							"xatautils",
							"pg_stat_statements",
							"pgaudit",
							"pg_cron",
						},
					},
					ScaleToZero: &v1alpha1.ScaleToZeroConfiguration{
						Enabled:                 true,
						InactivityPeriodMinutes: 60,
					},
				},
				BackupSpec: &v1alpha1.BackupSpec{
					Retention: "14d",
					ScheduledBackup: &v1alpha1.ScheduledBackupSpec{
						Schedule: "0 7 * * *",
					},
				},
				InheritedMetadata: &v1alpha1.InheritedMetadata{
					Labels: map[string]string{
						LabelOrgID:     "jf1tpn",
						LabelProjectID: "prj_9tmrorf02l0gv9ioptqat1uc6k",
					},
				},
			},
		}
}

func parentBranch(opts ...parentBranchOption) *v1alpha1.Branch {
	b := &v1alpha1.Branch{
		ObjectMeta: metav1.ObjectMeta{
			Name: "gmnfj6042d3qd09dcc8a7le0eo",
		},
		Spec: v1alpha1.BranchSpec{
			ClusterSpec: v1alpha1.ClusterSpec{
				Image: "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:16.3",
				Storage: v1alpha1.StorageSpec{
					Size:             "200Gi",
					MountPropagation: new(string(corev1.MountPropagationHostToContainer)),
				},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("1"),
						corev1.ResourceMemory: resource.MustParse("1948Mi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("2"),
						corev1.ResourceMemory: resource.MustParse("1948Mi"),
					},
				},
				Postgres: &v1alpha1.PostgresConfiguration{
					Parameters: []v1alpha1.PostgresParameter{
						{Name: "max_connections", Value: "100"},
						{Name: "shared_buffers", Value: "128MB"},
					},
					SharedPreloadLibraries: []string{
						"pg_stat_statements",
					},
				},
			},
			BackupSpec: &v1alpha1.BackupSpec{
				Retention:    "2d",
				WALArchiving: v1alpha1.WALArchivingModeEnabled,
			},
		},
	}

	for _, opt := range opts {
		opt(b)
	}

	return b
}

type parentBranchOption func(*v1alpha1.Branch)

func withStorageClass(storageClass string) parentBranchOption {
	return func(b *v1alpha1.Branch) {
		b.Spec.ClusterSpec.Storage.StorageClass = new(storageClass)
	}
}

func withVolumeSnapshotClass(className string) parentBranchOption {
	return func(b *v1alpha1.Branch) {
		b.Spec.ClusterSpec.Storage.VolumeSnapshotClass = new(className)
	}
}

func withBackupDisabled() parentBranchOption {
	return func(b *v1alpha1.Branch) {
		b.Spec.BackupSpec = nil
	}
}

func withSharedPreloadLibraries(libs []string) parentBranchOption {
	return func(b *v1alpha1.Branch) {
		b.Spec.ClusterSpec.Postgres.SharedPreloadLibraries = libs
	}
}

func sourceClusterForPITR() *apiv1.Cluster {
	return &apiv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "source-cluster-for-pitr",
			Namespace: "xata-clusters",
		},
		Spec: apiv1.ClusterSpec{
			ImageName: "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:16.3",
			StorageConfiguration: apiv1.StorageConfiguration{
				Size: "100Gi",
			},
		},
	}
}

func objectStoreForPITR() *barmanPluginApi.ObjectStore {
	return &barmanPluginApi.ObjectStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "source-cluster-for-pitr",
			Namespace: "xata-clusters",
		},
		Status: barmanPluginApi.ObjectStoreStatus{
			ServerRecoveryWindow: map[string]barmanPluginApi.RecoveryWindow{
				"source-cluster-for-pitr": {
					FirstRecoverabilityPoint: &metav1.Time{Time: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)},
				},
			},
		},
	}
}

func poolForTest(storageClass, image, cpu, memory string) *cpv1alpha1.ClusterPool {
	return &cpv1alpha1.ClusterPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pool",
			Namespace: "xata-clusters",
			UID:       "pool-uid-123",
		},
		Spec: cpv1alpha1.ClusterPoolSpec{
			Clusters: 2,
			ClusterSpec: apiv1.ClusterSpec{
				ImageName: image,
				StorageConfiguration: apiv1.StorageConfiguration{
					StorageClass: &storageClass,
				},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse(cpu),
						corev1.ResourceMemory: resource.MustParse(memory),
					},
				},
			},
		},
	}
}

func poolClusterForTest() *apiv1.Cluster {
	return &apiv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-cluster-1",
			Namespace: "xata-clusters",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: cpv1alpha1.GroupVersion.String(),
					Kind:       "ClusterPool",
					UID:        "pool-uid-123",
					Name:       "test-pool",
					Controller: ptr.To(true),
				},
			},
		},
		Status: apiv1.ClusterStatus{
			Phase: apiv1.PhaseHealthy,
		},
	}
}

func TestCreateWakeupRequest(t *testing.T) {
	t.Parallel()

	branchName := "lsmevenv7t3l56euo1v9bh3b74"
	namespace := "xata-clusters"

	baseBranch := func() *v1alpha1.Branch {
		_, b, _, _, _ := exampleRequestsAndBranches()
		b.Annotations = map[string]string{
			v1alpha1.WakeupPoolAnnotation: "test-pool",
		}
		return b
	}

	baseRequest := func() *clustersv1.UpdatePostgresClusterRequest {
		_, _, _, req, _ := exampleRequestsAndBranches()
		req.UpdateConfiguration.Hibernate = new(false)
		return req
	}

	// existingWUR creates a WakeupRequest to be used in those testcases that
	// need to assert on the behaviour when a WUR already exists for the branch
	existingWUR := func(condStatus metav1.ConditionStatus, reason string) *v1alpha1.WakeupRequest {
		return &v1alpha1.WakeupRequest{
			ObjectMeta: metav1.ObjectMeta{
				Name:      branchName,
				Namespace: namespace,
				Labels:    map[string]string{"initialWUR": "true"},
			},
			Spec: v1alpha1.WakeupRequestSpec{
				BranchName: branchName,
			},
			Status: v1alpha1.WakeupRequestStatus{
				Conditions: []metav1.Condition{
					{
						Type:   v1alpha1.WakeupSucceededConditionType,
						Status: condStatus,
						Reason: reason,
					},
				},
			},
		}
	}

	tests := []struct {
		name          string
		inputBranchFn func(b *v1alpha1.Branch)
		requestFn     func(r *clustersv1.UpdatePostgresClusterRequest)
		extraObjects  []client.Object
		wantWUR       bool
		wantNew       bool
		wantErr       bool
	}{
		{
			name: "no-op when hibernate field of the update request is nil",
			requestFn: func(r *clustersv1.UpdatePostgresClusterRequest) {
				r.UpdateConfiguration.Hibernate = nil
			},
			wantWUR: false,
		},
		{
			name: "no-op when update is setting branch to hibernate",
			requestFn: func(r *clustersv1.UpdatePostgresClusterRequest) {
				r.UpdateConfiguration.Hibernate = new(true)
			},
			wantWUR: false,
		},
		{
			name: "no-op when branch has no pool annotation",
			inputBranchFn: func(b *v1alpha1.Branch) {
				delete(b.Annotations, v1alpha1.WakeupPoolAnnotation)
			},
			wantWUR: false,
		},
		{
			name:    "creates WakeupRequest when none exists",
			wantWUR: true,
			wantNew: true,
		},
		{
			name: "no-op when existing WakeupRequest is in progress",
			extraObjects: []client.Object{
				existingWUR(metav1.ConditionUnknown, v1alpha1.WakeupInProgressReason),
			},
			wantWUR: true,
		},
		{
			name: "replaces succeeded WakeupRequest",
			extraObjects: []client.Object{
				existingWUR(metav1.ConditionTrue, v1alpha1.WakeupSucceededReason),
			},
			wantWUR: true,
			wantNew: true,
		},
		{
			name: "replaces failed WakeupRequest",
			extraObjects: []client.Object{
				existingWUR(metav1.ConditionFalse, v1alpha1.BranchHasNoXVolReason),
			},
			wantWUR: true,
			wantNew: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			branch := baseBranch()
			if tt.inputBranchFn != nil {
				tt.inputBranchFn(branch)
			}

			req := baseRequest()
			if tt.requestFn != nil {
				tt.requestFn(req)
			}

			existingObjs := append([]client.Object{branch}, tt.extraObjects...)
			svc, k8sClient := setupTestClustersService(t, withExistingObjects(existingObjs...))

			err := svc.createWakeupRequest(ctx, branch, req)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			wur := &v1alpha1.WakeupRequest{}
			getErr := k8sClient.Get(ctx, client.ObjectKey{
				Name:      branchName,
				Namespace: namespace,
			}, wur)

			if tt.wantWUR {
				require.NoError(t, getErr)
				require.Equal(t, branchName, wur.Spec.BranchName)
				if tt.wantNew {
					require.Empty(t, wur.Labels["initialWUR"])
				} else {
					require.Equal(t, "true", wur.Labels["initialWUR"])
				}
			} else {
				require.True(t, errors.IsNotFound(getErr))
			}
		})
	}
}

func updatePostgresParam(params []v1alpha1.PostgresParameter, name, value string) []v1alpha1.PostgresParameter {
	for i := range params {
		if params[i].Name == name {
			params[i].Value = value
			return params
		}
	}
	return append(params, v1alpha1.PostgresParameter{Name: name, Value: value})
}
