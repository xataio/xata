package clusters

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	clustersv1 "xata/gen/proto/clusters/v1"
	"xata/internal/pgbackrest"
	"xata/internal/storageqos"
	cpv1alpha1 "xata/proto/clusterpool-operator/api/v1alpha1"
	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/clusters/internal/connectors/cnpg"
	cnpgmocks "xata/services/clusters/internal/connectors/cnpg/mocks"
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
		extraObjects       []client.Object
		xatastorEnabled    bool
		serviceOpts        []testServiceOption
		requestFn          func(r *clustersv1.CreatePostgresClusterRequest)
		expectedBranchFn   func(b *v1alpha1.Branch)
		expectedStatusCode codes.Code
		wantWUR            bool
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
			name: "child branch of parent with wakeup pool annotation uses XVolClone restore",
			parentBranch: parentBranch(
				withStorageClass("xatastor"),
				withWakeupPool("test-pool"),
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
					Type: v1alpha1.RestoreTypeXVolClone,
					Name: "gmnfj6042d3qd09dcc8a7le0eo",
				}
				b.Spec.ClusterSpec.Name = nil
				b.Spec.ClusterSpec.Instances = 1
				b.Spec.ClusterSpec.Image = "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:16.3"
				b.Spec.ClusterSpec.Storage.Size = "200Gi"
				b.Spec.ClusterSpec.Storage.StorageClass = new("xatastor")
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
				b.Annotations = map[string]string{
					v1alpha1.WakeupPoolAnnotation:     "test-pool",
					v1alpha1.AwaitingWakeupAnnotation: "true",
				}
			},
			wantWUR: true,
		},
		{
			name:         "restore from ContinuousBackup with wakeup pool parent keeps ObjectStore restore",
			parentBranch: poolParentBranch(),
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.ParentId = new("gmnfj6042d3qd09dcc8a7le0eo")
				r.BackupConfiguration.BackupMethod = string(v1alpha1.BackupMethodPgBackRest)
				r.DataSource = &clustersv1.CreatePostgresClusterRequest_ContinuousBackup{
					ContinuousBackup: &clustersv1.ContinuousBackup{
						ClusterId: "gmnfj6042d3qd09dcc8a7le0eo",
						Timestamp: timestamppb.New(pitrTimestamp),
					},
				}
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				expectPoolParentInheritance(b)
				expectPgBackRestBackupSpec(b)
				// The restore reads the backup, so the branch keeps its own
				// cluster name and is not adopted into the parent's pool
				b.Spec.Restore = &v1alpha1.RestoreSpec{
					Type:      v1alpha1.RestoreTypeObjectStore,
					Name:      "gmnfj6042d3qd09dcc8a7le0eo",
					Timestamp: &metav1.Time{Time: pitrTimestamp},
					PgBackRest: &v1alpha1.RestorePgBackRestSpec{
						RepoPath:   "gmnfj6042d3qd09dcc8a7le0eo",
						StanzaName: "gmnfj6042d3qd09dcc8a7le0eo",
						S3:         &v1alpha1.PgBackRestS3Spec{Bucket: "test-pgbackrest-bucket", Region: "us-east-1", KeyType: webIdentityKeyType},
					},
				}
			},
		},
		{
			name:         "restore from BaseBackup with wakeup pool parent keeps BaseBackup restore",
			parentBranch: poolParentBranch(),
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.ParentId = new("gmnfj6042d3qd09dcc8a7le0eo")
				r.BackupConfiguration.BackupMethod = string(v1alpha1.BackupMethodPgBackRest)
				r.DataSource = &clustersv1.CreatePostgresClusterRequest_BaseBackup{
					BaseBackup: &clustersv1.BaseBackup{
						BackupId: "backup-20240615-143000",
					},
				}
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				expectPoolParentInheritance(b)
				expectPgBackRestBackupSpec(b)
				b.Spec.Restore = &v1alpha1.RestoreSpec{
					Type: v1alpha1.RestoreTypeBaseBackup,
					Name: "backup-20240615-143000",
				}
			},
		},
		{
			name:         "wakeup pool parent without a data source is not pool adopted",
			parentBranch: poolParentBranch(),
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.ParentId = new("gmnfj6042d3qd09dcc8a7le0eo")
				r.BackupConfiguration.BackupMethod = string(v1alpha1.BackupMethodPgBackRest)
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				expectPoolParentInheritance(b)
				expectPgBackRestBackupSpec(b)
				b.Spec.Restore = nil
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
			extraObjects:  []client.Object{sourceBranchForPITR()},
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
			extraObjects:  []client.Object{sourceBranchForPITR()},
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
			name: "restore from pgbackrest AWS web-id source reads the source repository",
			extraObjects: []client.Object{pgBackRestRestoreSource(&v1alpha1.PgBackRestSpec{
				S3:       &v1alpha1.PgBackRestS3Spec{Bucket: "source-bucket", Region: "us-east-1", KeyType: webIdentityKeyType},
				RepoPath: webIdentityRepoPath("restore-source"),
			})},
			requestFn: pgBackRestRestoreRequest,
			expectedBranchFn: func(b *v1alpha1.Branch) {
				expectPgBackRestBackupSpec(b)
				b.Spec.Restore = &v1alpha1.RestoreSpec{
					Type: v1alpha1.RestoreTypeObjectStore,
					Name: "restore-source",
					PgBackRest: &v1alpha1.RestorePgBackRestSpec{
						RepoPath:   webIdentityRepoPath("restore-source"),
						StanzaName: "restore-source",
						S3:         &v1alpha1.PgBackRestS3Spec{Bucket: "source-bucket", Region: "us-east-1", KeyType: webIdentityKeyType},
					},
				}
			},
		},
		{
			name: "restore from pgbackrest GCS web-id source reads the source repository",
			extraObjects: []client.Object{pgBackRestRestoreSource(&v1alpha1.PgBackRestSpec{
				GCS:      &v1alpha1.PgBackRestGCSSpec{Bucket: "source-bucket", KeyType: webIdentityKeyType},
				RepoPath: "restore-source",
			})},
			requestFn: pgBackRestRestoreRequest,
			expectedBranchFn: func(b *v1alpha1.Branch) {
				expectPgBackRestBackupSpec(b)
				b.Spec.Restore = &v1alpha1.RestoreSpec{
					Type: v1alpha1.RestoreTypeObjectStore,
					Name: "restore-source",
					PgBackRest: &v1alpha1.RestorePgBackRestSpec{
						RepoPath:   "restore-source",
						StanzaName: "restore-source",
						GCS:        &v1alpha1.PgBackRestGCSSpec{Bucket: "source-bucket", KeyType: webIdentityKeyType},
					},
				}
			},
		},
		{
			name: "restore from pgbackrest static-key S3 source reads the source stanza",
			extraObjects: []client.Object{pgBackRestRestoreSource(&v1alpha1.PgBackRestSpec{
				S3:         &v1alpha1.PgBackRestS3Spec{Bucket: "source-bucket", Region: "us-east-1", Endpoint: "http://rustfs:9000", CredentialsSecretName: "source-creds"},
				StanzaName: "restore-source-1",
			})},
			requestFn: pgBackRestRestoreRequest,
			expectedBranchFn: func(b *v1alpha1.Branch) {
				expectPgBackRestBackupSpec(b)
				b.Spec.Restore = &v1alpha1.RestoreSpec{
					Type: v1alpha1.RestoreTypeObjectStore,
					Name: "restore-source",
					PgBackRest: &v1alpha1.RestorePgBackRestSpec{
						RepoPath:   "restore-source-1",
						StanzaName: "restore-source-1",
						S3:         &v1alpha1.PgBackRestS3Spec{Bucket: "source-bucket", Region: "us-east-1", Endpoint: "http://rustfs:9000", CredentialsSecretName: "source-creds"},
					},
				}
			},
		},
		{
			name: "restore from pgbackrest Azure source reads the source repository",
			extraObjects: []client.Object{pgBackRestRestoreSource(&v1alpha1.PgBackRestSpec{
				Azure: &v1alpha1.PgBackRestAzureSpec{Account: "account", Container: "backups"},
			})},
			requestFn: pgBackRestRestoreRequest,
			expectedBranchFn: func(b *v1alpha1.Branch) {
				expectPgBackRestBackupSpec(b)
				b.Spec.Restore = &v1alpha1.RestoreSpec{
					Type: v1alpha1.RestoreTypeObjectStore,
					Name: "restore-source",
					PgBackRest: &v1alpha1.RestorePgBackRestSpec{
						RepoPath:   "restore-source",
						StanzaName: "restore-source",
						Azure:      &v1alpha1.PgBackRestAzureSpec{Account: "account", Container: "backups"},
					},
				}
			},
		},
		{
			name: "restore from encrypted pgbackrest source sets the restore cipher and the source repository",
			extraObjects: []client.Object{pgBackRestRestoreSource(&v1alpha1.PgBackRestSpec{
				S3:                        &v1alpha1.PgBackRestS3Spec{Bucket: "source-bucket", Region: "us-east-1", KeyType: webIdentityKeyType},
				RepoPath:                  webIdentityRepoPath("restore-source"),
				CipherPassphraseSecretRef: &corev1.SecretKeySelector{Name: "restore-source-pgbackrest", Key: pgbackrest.CipherPassphraseKey},
			})},
			requestFn: pgBackRestRestoreRequest,
			expectedBranchFn: func(b *v1alpha1.Branch) {
				expectPgBackRestBackupSpec(b)
				b.Spec.Restore = &v1alpha1.RestoreSpec{
					Type: v1alpha1.RestoreTypeObjectStore,
					Name: "restore-source",
					PgBackRestCipherPassphraseSecretRef: &corev1.SecretKeySelector{
						Name: pgbackrest.CipherSecretName(b.Name),
						Key:  pgbackrest.RestoreCipherPassphraseKey,
					},
					PgBackRest: &v1alpha1.RestorePgBackRestSpec{
						RepoPath:   webIdentityRepoPath("restore-source"),
						StanzaName: "restore-source",
						S3:         &v1alpha1.PgBackRestS3Spec{Bucket: "source-bucket", Region: "us-east-1", KeyType: webIdentityKeyType},
					},
				}
			},
		},
		{
			name:               "error - restore from pgbackrest source without a backend",
			extraObjects:       []client.Object{pgBackRestRestoreSource(&v1alpha1.PgBackRestSpec{})},
			requestFn:          pgBackRestRestoreRequest,
			expectedStatusCode: codes.FailedPrecondition,
		},
		{
			name: "restore from legacy pgbackrest AWS source keeps its key type and path",
			extraObjects: []client.Object{pgBackRestRestoreSource(&v1alpha1.PgBackRestSpec{
				S3: &v1alpha1.PgBackRestS3Spec{Bucket: "source-bucket", Region: "us-east-1", KeyType: "auto"},
			})},
			requestFn: pgBackRestRestoreRequest,
			expectedBranchFn: func(b *v1alpha1.Branch) {
				expectPgBackRestBackupSpec(b)
				b.Spec.Restore = &v1alpha1.RestoreSpec{
					Type: v1alpha1.RestoreTypeObjectStore,
					Name: "restore-source",
					PgBackRest: &v1alpha1.RestorePgBackRestSpec{
						RepoPath:   "restore-source",
						StanzaName: "restore-source",
						S3:         &v1alpha1.PgBackRestS3Spec{Bucket: "source-bucket", Region: "us-east-1", KeyType: "auto"},
					},
				}
			},
		},
		{
			name:          "error - restore from ContinuousBackup with non-existent source cluster",
			parentBranch:  nil,
			sourceCluster: nil,
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
			name:          "error - restore from ContinuousBackup with empty ClusterId",
			parentBranch:  nil,
			sourceCluster: nil,
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
			name: "use_pool with matching pool - adopts pool cluster and keeps the branch backup spec",
			extraObjects: []client.Object{
				poolForTest("default-storage-class", testImage, "2", "3996Mi"),
				poolClusterForTest(),
				poolClusterAppSecretForTest("pool-password"),
			},
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.UsePool = new(true)
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Name = new("pool-cluster-1")
				b.Annotations = map[string]string{v1alpha1.WakeupPoolAnnotation: "test-pool-slot"}
			},
		},
		{
			name: "use_pool with no matching pool - falls back to normal creation",
			extraObjects: []client.Object{
				poolForTest("other-storage-class", testImage, "2", "3996Mi"),
			},
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.UsePool = new(true)
			},
		},
		{
			name: "use_pool with a pool disk larger than requested - falls back to normal creation",
			extraObjects: []client.Object{
				func() client.Object {
					pool := poolForTest("default-storage-class", testImage, "2", "3996Mi")
					pool.Spec.ClusterSpec.StorageConfiguration.Size = "1Ti"
					return pool
				}(),
				poolClusterForTest(),
			},
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.UsePool = new(true)
			},
		},
		{
			name: "use_pool false with matching pool - field is ignored and the pool cluster is adopted",
			extraObjects: []client.Object{
				poolForTest("default-storage-class", testImage, "2", "3996Mi"),
				poolClusterForTest(),
				poolClusterAppSecretForTest("pool-password"),
			},
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.UsePool = new(false)
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Name = new("pool-cluster-1")
				b.Annotations = map[string]string{v1alpha1.WakeupPoolAnnotation: "test-pool-slot"}
			},
		},
		{
			name: "ClusterPool CRD not installed - falls back to normal creation",
			serviceOpts: []testServiceOption{
				withInterceptorFuncs(interceptor.Funcs{
					List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
						if _, ok := list.(*cpv1alpha1.ClusterPoolList); ok {
							return &meta.NoKindMatchError{
								GroupKind:        schema.GroupKind{Group: "xata.io", Kind: "ClusterPool"},
								SearchedVersions: []string{"v1alpha1"},
							}
						}
						return c.List(ctx, list, opts...)
					},
				}),
			},
		},
		{
			name:            "use_xatastor with cell enabled - root cluster uses xatastor",
			xatastorEnabled: true,
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.UseXatastor = new(true)
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Storage.StorageClass = new("xatastor")
				b.Spec.ClusterSpec.Storage.VolumeSnapshotClass = new("xatastor")
			},
		},
		{
			name:            "use_xatastor with cell disabled - silent fallback to default",
			xatastorEnabled: false,
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.UseXatastor = new(true)
			},
		},
		{
			name:            "use_xatastor with use_pool - adopts matching xatastor pool cluster",
			xatastorEnabled: true,
			extraObjects: []client.Object{
				poolForTest("xatastor", testImage, "2", "3996Mi"),
				poolClusterForTest(),
				poolClusterAppSecretForTest("pool-password"),
			},
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.UseXatastor = new(true)
				r.UsePool = new(true)
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Storage.StorageClass = new("xatastor")
				b.Spec.ClusterSpec.Storage.VolumeSnapshotClass = new("xatastor")
				b.Spec.ClusterSpec.Name = new("pool-cluster-1")
				b.Annotations = map[string]string{v1alpha1.WakeupPoolAnnotation: "test-pool-slot"}
			},
		},
		{
			name:            "use_xatastor with use_pool - no xatastor pool, falls back to normal xatastor creation",
			xatastorEnabled: true,
			extraObjects: []client.Object{
				poolForTest("default-storage-class", testImage, "2", "3996Mi"),
				poolClusterForTest(),
			},
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.UseXatastor = new(true)
				r.UsePool = new(true)
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Storage.StorageClass = new("xatastor")
				b.Spec.ClusterSpec.Storage.VolumeSnapshotClass = new("xatastor")
			},
		},
		{
			name: "use_xatastor with child branch - inherits parent storage class",
			parentBranch: parentBranch(
				withStorageClass("some-other-storage-class"),
				withVolumeSnapshotClass("some-other-snapshot-class"),
				withBackupDisabled(),
			),
			xatastorEnabled: true,
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.ParentId = new("gmnfj6042d3qd09dcc8a7le0eo")
				r.DataSource = &clustersv1.CreatePostgresClusterRequest_ClusterSnapshot{
					ClusterSnapshot: &clustersv1.ClusterSnapshot{
						ClusterId: "gmnfj6042d3qd09dcc8a7le0eo",
					},
				}
				r.UseXatastor = new(true)
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
			name:            "storage QoS class - resolves to VolumeAttributesClass on xatastor",
			xatastorEnabled: true,
			serviceOpts:     []testServiceOption{withStorageQoSClasses(true)},
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.UseXatastor = new(true)
				r.Configuration.StorageQosClass = new(storageqos.ClassMicro)
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Storage.StorageClass = new("xatastor")
				b.Spec.ClusterSpec.Storage.VolumeSnapshotClass = new("xatastor")
				b.Spec.ClusterSpec.Storage.VolumeAttributesClass = new("xatastor-micro")
			},
		},
		{
			name:            "storage QoS class - ignored when cell QoS support is disabled",
			xatastorEnabled: true,
			serviceOpts:     []testServiceOption{withStorageQoSClasses(false)},
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.UseXatastor = new(true)
				r.Configuration.StorageQosClass = new(storageqos.ClassMicro)
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Storage.StorageClass = new("xatastor")
				b.Spec.ClusterSpec.Storage.VolumeSnapshotClass = new("xatastor")
			},
		},
		{
			name:        "storage QoS class - ignored on a storage class without VolumeAttributesClasses",
			serviceOpts: []testServiceOption{withStorageQoSClasses(true)},
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.UseXatastor = new(false)
				r.Configuration.StorageQosClass = new(storageqos.ClassMicro)
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Storage.StorageClass = new("default-storage-class")
				b.Spec.ClusterSpec.Storage.VolumeAttributesClass = nil
			},
		},
		{
			name: "child branch - inherits parent VolumeAttributesClass",
			parentBranch: parentBranch(
				withStorageClass("xatastor"),
				withVolumeSnapshotClass("xatastor"),
				withVolumeAttributesClass("xatastor-large"),
				withBackupDisabled(),
			),
			serviceOpts: []testServiceOption{withStorageQoSClasses(true)},
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
				b.Spec.ClusterSpec.Storage.StorageClass = new("xatastor")
				b.Spec.ClusterSpec.Storage.VolumeSnapshotClass = new("xatastor")
				b.Spec.ClusterSpec.Storage.VolumeAttributesClass = new("xatastor-large")
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
			name:         "main branch - pgbackrest backup method",
			parentBranch: nil,
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.BackupConfiguration.BackupMethod = string(v1alpha1.BackupMethodPgBackRest)
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.BackupSpec.Method = v1alpha1.BackupMethodPgBackRest
				b.Spec.BackupSpec.PgBackRest = &v1alpha1.PgBackRestSpec{
					S3: &v1alpha1.PgBackRestS3Spec{
						Bucket:  "test-pgbackrest-bucket",
						Region:  "us-east-1",
						KeyType: webIdentityKeyType,
					},
					RepoPath:            webIdentityRepoPath(b.Name),
					RetentionFullDays:   7,
					CompressType:        DefaultPgBackRestCompressType,
					ArchiveAsync:        DefaultPgBackRestArchiveAsync,
					ArchivePushQueueMax: DefaultPgBackRestPushQueueMax,
					ArchiveGetQueueMax:  DefaultPgBackRestGetQueueMax,
				}
			},
		},
		{
			name:         "main branch - pgbackrest parses retention from proto",
			parentBranch: nil,
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.BackupConfiguration.BackupMethod = string(v1alpha1.BackupMethodPgBackRest)
				r.BackupConfiguration.BackupRetention = "14d"
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.BackupSpec.Retention = "14d"
				b.Spec.BackupSpec.Method = v1alpha1.BackupMethodPgBackRest
				b.Spec.BackupSpec.PgBackRest = &v1alpha1.PgBackRestSpec{
					S3: &v1alpha1.PgBackRestS3Spec{
						Bucket:  "test-pgbackrest-bucket",
						Region:  "us-east-1",
						KeyType: webIdentityKeyType,
					},
					RepoPath:            webIdentityRepoPath(b.Name),
					RetentionFullDays:   14,
					CompressType:        DefaultPgBackRestCompressType,
					ArchiveAsync:        DefaultPgBackRestArchiveAsync,
					ArchivePushQueueMax: DefaultPgBackRestPushQueueMax,
					ArchiveGetQueueMax:  DefaultPgBackRestGetQueueMax,
				}
			},
		},
		{
			name: "main branch - pgbackrest gcs backend",
			serviceOpts: []testServiceOption{
				withCloudProvider(CloudProviderGCP),
				withPgBackRestGCSServiceAccount("backups@project.iam.gserviceaccount.com"),
			},
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.BackupConfiguration.BackupMethod = string(v1alpha1.BackupMethodPgBackRest)
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.BackupSpec.Method = v1alpha1.BackupMethodPgBackRest
				b.Spec.BackupSpec.PgBackRest = &v1alpha1.PgBackRestSpec{
					GCS: &v1alpha1.PgBackRestGCSSpec{
						Bucket:              "test-pgbackrest-bucket",
						ServiceAccountEmail: "backups@project.iam.gserviceaccount.com",
						KeyType:             webIdentityKeyType,
					},
					RepoPath:            b.Name,
					RetentionFullDays:   7,
					CompressType:        DefaultPgBackRestCompressType,
					ArchiveAsync:        DefaultPgBackRestArchiveAsync,
					ArchivePushQueueMax: DefaultPgBackRestPushQueueMax,
					ArchiveGetQueueMax:  DefaultPgBackRestGetQueueMax,
				}
			},
		},
		{
			name:        "main branch - pgbackrest gcs backend without service account",
			serviceOpts: []testServiceOption{withCloudProvider(CloudProviderGCP)},
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.BackupConfiguration.BackupMethod = string(v1alpha1.BackupMethodPgBackRest)
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.BackupSpec.Method = v1alpha1.BackupMethodPgBackRest
				b.Spec.BackupSpec.PgBackRest = &v1alpha1.PgBackRestSpec{
					GCS: &v1alpha1.PgBackRestGCSSpec{
						Bucket:  "test-pgbackrest-bucket",
						KeyType: webIdentityKeyType,
					},
					RepoPath:            b.Name,
					RetentionFullDays:   7,
					CompressType:        DefaultPgBackRestCompressType,
					ArchiveAsync:        DefaultPgBackRestArchiveAsync,
					ArchivePushQueueMax: DefaultPgBackRestPushQueueMax,
					ArchiveGetQueueMax:  DefaultPgBackRestGetQueueMax,
				}
			},
		},
		{
			name: "main branch - pgbackrest azure backend",
			serviceOpts: []testServiceOption{
				withCloudProvider(CloudProviderAzure),
				withPgBackRestAzure("testaccount", "backups"),
			},
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.BackupConfiguration.BackupMethod = string(v1alpha1.BackupMethodPgBackRest)
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.BackupSpec.Method = v1alpha1.BackupMethodPgBackRest
				b.Spec.BackupSpec.PgBackRest = &v1alpha1.PgBackRestSpec{
					Azure: &v1alpha1.PgBackRestAzureSpec{
						Account:   "testaccount",
						Container: "backups",
					},
					RetentionFullDays:   7,
					CompressType:        DefaultPgBackRestCompressType,
					ArchiveAsync:        DefaultPgBackRestArchiveAsync,
					ArchivePushQueueMax: DefaultPgBackRestPushQueueMax,
					ArchiveGetQueueMax:  DefaultPgBackRestGetQueueMax,
				}
			},
		},
		{
			name:        "error - pgbackrest without cell config",
			serviceOpts: []testServiceOption{withPgBackRestBucket("")},
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.BackupConfiguration.BackupMethod = "pgbackrest"
			},
			expectedStatusCode: codes.FailedPrecondition,
		},
		{
			name:        "error - pgbackrest azure without account and container",
			serviceOpts: []testServiceOption{withCloudProvider(CloudProviderAzure)},
			requestFn: func(r *clustersv1.CreatePostgresClusterRequest) {
				r.BackupConfiguration.BackupMethod = "pgbackrest"
			},
			expectedStatusCode: codes.FailedPrecondition,
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
			existingObjs = append(existingObjs, tt.extraObjects...)
			svc, k8sClient := setupTestClustersService(t,
				append([]testServiceOption{
					withExistingObjects(existingObjs...),
					withNodeSelector(tt.nodeSelector),
					withXatastorEnabled(tt.xatastorEnabled),
				}, tt.serviceOpts...)...)

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
				require.Equal(t, expectedBranch.Annotations, branch.Annotations)
				require.Equal(t, resp.GetId(), branch.Name)

				wur := &v1alpha1.WakeupRequest{}
				wurErr := k8sClient.Get(ctx, client.ObjectKey{
					Name:      branch.Name,
					Namespace: "xata-clusters",
				}, wur)
				if tt.wantWUR {
					require.NoError(t, wurErr)
					require.Equal(t, branch.Name, wur.Spec.BranchName)
					require.Equal(t, v1alpha1.XVolCloneName(tt.parentBranch.Name, branch.Name), wur.Spec.XVolName)
				} else {
					require.True(t, errors.IsNotFound(wurErr))
				}
			}
		})
	}
}

func TestCreatePostgresClusterIdempotent(t *testing.T) {
	t.Parallel()

	t.Run("same idempotency key returns existing branch", func(t *testing.T) {
		ctx := context.Background()
		svc, k8sClient := setupTestClustersService(t)
		req, _, _, _, _ := exampleRequestsAndBranches()
		req.IdempotencyKey = "create-request-id"

		first, err := svc.CreatePostgresCluster(ctx, req)
		require.NoError(t, err)

		secretKey := client.ObjectKey{Name: req.GetId() + "-app", Namespace: "xata-clusters"}
		firstSecret := &corev1.Secret{}
		require.NoError(t, k8sClient.Get(ctx, secretKey, firstSecret))

		second, err := svc.CreatePostgresCluster(ctx, req)
		require.NoError(t, err)
		require.Equal(t, first, second)

		secondSecret := &corev1.Secret{}
		require.NoError(t, k8sClient.Get(ctx, secretKey, secondSecret))
		require.Equal(t,
			firstSecret.Data[corev1.BasicAuthPasswordKey],
			secondSecret.Data[corev1.BasicAuthPasswordKey])
	})

	t.Run("different idempotency key returns already exists", func(t *testing.T) {
		ctx := context.Background()
		req, existing, _, _, _ := exampleRequestsAndBranches()
		req.IdempotencyKey = "create-request-id"
		existing.Annotations = map[string]string{
			AnnotationIdempotencyKey: "another-key",
		}
		svc, k8sClient := setupTestClustersService(t, withExistingObjects(existing))

		resp, err := svc.CreatePostgresCluster(ctx, req)
		require.Nil(t, resp)
		require.Equal(t, codes.AlreadyExists, status.Code(err))

		secret := &corev1.Secret{}
		err = k8sClient.Get(ctx, client.ObjectKey{
			Name:      req.GetId() + "-app",
			Namespace: "xata-clusters",
		}, secret)
		require.True(t, errors.IsNotFound(err))
	})
}

func TestCreatePostgresClusterAppSecret(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	appSecretKey := func(name string) client.ObjectKey {
		return client.ObjectKey{Name: name, Namespace: "xata-clusters"}
	}

	t.Run("fresh root branch - secret created before the Branch", func(t *testing.T) {
		svc, k8sClient := setupTestClustersService(t)
		req, _, _, _, _ := exampleRequestsAndBranches()

		_, err := svc.CreatePostgresCluster(ctx, req)
		require.NoError(t, err)

		secret := &corev1.Secret{}
		require.NoError(t, k8sClient.Get(ctx, appSecretKey(req.GetId()+"-app"), secret))
		require.Equal(t, corev1.SecretTypeBasicAuth, secret.Type)
		require.Equal(t, "true", secret.Labels["cnpg.io/reload"])
		require.Equal(t, req.GetOrganizationId(), secret.Labels[LabelOrgID])
		require.Equal(t, req.GetProjectId(), secret.Labels[LabelProjectID])
		require.Equal(t, req.GetId(), secret.Labels[LabelBranchID])
		require.Equal(t, "xata", string(secret.Data[corev1.BasicAuthUsernameKey]))
		require.Len(t, string(secret.Data[corev1.BasicAuthPasswordKey]), 64)
	})

	t.Run("use_pool adoption - branch secret copies the pool cluster password", func(t *testing.T) {
		svc, k8sClient := setupTestClustersService(t,
			withExistingObjects(
				poolForTest("default-storage-class", testImage, "2", "3996Mi"),
				poolClusterForTest(),
				poolClusterAppSecretForTest("pool-password"),
			))
		req, _, _, _, _ := exampleRequestsAndBranches()
		req.UsePool = new(true)

		_, err := svc.CreatePostgresCluster(ctx, req)
		require.NoError(t, err)

		branch, err := getBranchFromK8s(ctx, k8sClient, req.GetId())
		require.NoError(t, err)
		require.Equal(t, new("pool-cluster-1"), branch.Spec.ClusterSpec.Name)

		// The branch gets its own <id>-app secret seeded with the password
		// the pool cluster already has, so the managed-role sync on adoption
		// changes nothing. Only username and password are copied.
		secret := &corev1.Secret{}
		require.NoError(t, k8sClient.Get(ctx, appSecretKey(req.GetId()+"-app"), secret))
		require.Equal(t, corev1.SecretTypeBasicAuth, secret.Type)
		require.Equal(t, "true", secret.Labels["cnpg.io/reload"])
		require.Equal(t, req.GetId(), secret.Labels[LabelBranchID])
		require.Equal(t, map[string][]byte{
			corev1.BasicAuthUsernameKey: []byte("xata"),
			corev1.BasicAuthPasswordKey: []byte("pool-password"),
		}, secret.Data)
	})

	t.Run("child branch - fresh secret created", func(t *testing.T) {
		parent := parentBranch(withWakeupPool("test-pool"))
		svc, k8sClient := setupTestClustersService(t,
			withExistingObjects(parent))
		req, _, _, _, _ := exampleRequestsAndBranches()
		req.ParentId = new(parent.Name)
		req.DataSource = &clustersv1.CreatePostgresClusterRequest_ClusterSnapshot{
			ClusterSnapshot: &clustersv1.ClusterSnapshot{ClusterId: parent.Name},
		}

		_, err := svc.CreatePostgresCluster(ctx, req)
		require.NoError(t, err)

		secret := &corev1.Secret{}
		require.NoError(t, k8sClient.Get(ctx, appSecretKey(req.GetId()+"-app"), secret))
		require.NotEmpty(t, secret.Data[corev1.BasicAuthPasswordKey])
	})

	t.Run("use_pool adoption - pool secret missing fails the request", func(t *testing.T) {
		svc, k8sClient := setupTestClustersService(t,
			withExistingObjects(
				poolForTest("default-storage-class", testImage, "2", "3996Mi"),
				poolClusterForTest(),
			))
		req, _, _, _, _ := exampleRequestsAndBranches()
		req.UsePool = new(true)

		_, err := svc.CreatePostgresCluster(ctx, req)
		require.Error(t, err)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))

		// Neither the branch secret nor the Branch is created
		err = k8sClient.Get(ctx, appSecretKey(req.GetId()+"-app"), &corev1.Secret{})
		require.True(t, errors.IsNotFound(err))
		_, err = getBranchFromK8s(ctx, k8sClient, req.GetId())
		require.True(t, errors.IsNotFound(err))
	})

	t.Run("pre-existing secret - data preserved", func(t *testing.T) {
		req, _, _, _, _ := exampleRequestsAndBranches()
		existing := &corev1.Secret{
			Name:      req.GetId() + "-app",
			Namespace: "xata-clusters",
			Type:      corev1.SecretTypeBasicAuth,
			Data: map[string][]byte{
				corev1.BasicAuthUsernameKey: []byte("xata"),
				corev1.BasicAuthPasswordKey: []byte("existing-password"),
			},
		}
		svc, k8sClient := setupTestClustersService(t,
			withExistingObjects(existing))

		_, err := svc.CreatePostgresCluster(ctx, req)
		require.NoError(t, err)

		secret := &corev1.Secret{}
		require.NoError(t, k8sClient.Get(ctx, appSecretKey(req.GetId()+"-app"), secret))
		require.Equal(t, "existing-password", string(secret.Data[corev1.BasicAuthPasswordKey]))
	})

	t.Run("branch create failure - created secret cleaned up", func(t *testing.T) {
		svc, k8sClient := setupTestClustersService(t,
			withInterceptorFuncs(interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if _, ok := obj.(*v1alpha1.Branch); ok {
						return fmt.Errorf("branch create failed")
					}
					return c.Create(ctx, obj, opts...)
				},
			}))
		req, _, _, _, _ := exampleRequestsAndBranches()

		_, err := svc.CreatePostgresCluster(ctx, req)
		require.Error(t, err)

		err = k8sClient.Get(ctx, appSecretKey(req.GetId()+"-app"), &corev1.Secret{})
		require.True(t, errors.IsNotFound(err))
	})
}

func TestUpdatePostgresCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		extraObjects       []client.Object
		serviceOpts        []testServiceOption
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
				b.Annotations = map[string]string{
					v1alpha1.WakeupPoolAnnotation:     "test-pool",
					v1alpha1.AwaitingWakeupAnnotation: "false",
				}
			},
		},
		{
			name: "hibernate with wakeup-pool annotation updates a stale awaiting-wakeup annotation",
			inputBranchFn: func(b *v1alpha1.Branch) {
				b.Annotations = map[string]string{
					v1alpha1.WakeupPoolAnnotation:     "test-pool",
					v1alpha1.AwaitingWakeupAnnotation: "true",
				}
			},
			requestFn: func(r *clustersv1.UpdatePostgresClusterRequest) {
				r.UpdateConfiguration.Hibernate = new(true)
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Name = nil
				b.Spec.ClusterSpec.Hibernation = nil
				b.Annotations = map[string]string{
					v1alpha1.WakeupPoolAnnotation:     "test-pool",
					v1alpha1.AwaitingWakeupAnnotation: "false",
				}
			},
		},
		{
			name: "wake with wakeup-pool annotation adds the awaiting-wakeup annotation",
			inputBranchFn: func(b *v1alpha1.Branch) {
				b.Annotations = map[string]string{
					v1alpha1.WakeupPoolAnnotation: "test-pool",
				}
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Hibernation = nil
				b.Annotations = map[string]string{
					v1alpha1.WakeupPoolAnnotation:     "test-pool",
					v1alpha1.AwaitingWakeupAnnotation: "true",
				}
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
			name: "error - storage size exceeds maximum",
			requestFn: func(r *clustersv1.UpdatePostgresClusterRequest) {
				r.UpdateConfiguration.StorageSize = new(int32(MaxStorageSizeGi + 1))
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
		{
			name: "update backup retention preserves pgbackrest method",
			inputBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.BackupSpec.Method = v1alpha1.BackupMethodPgBackRest
				b.Spec.BackupSpec.PgBackRest = &v1alpha1.PgBackRestSpec{
					S3: &v1alpha1.PgBackRestS3Spec{
						Bucket: "test-bucket",
						Region: "us-east-1",
					},
					RetentionFullDays:   7,
					CompressType:        DefaultPgBackRestCompressType,
					ArchiveAsync:        DefaultPgBackRestArchiveAsync,
					ArchivePushQueueMax: DefaultPgBackRestPushQueueMax,
					ArchiveGetQueueMax:  DefaultPgBackRestGetQueueMax,
				}
			},
			requestFn: func(r *clustersv1.UpdatePostgresClusterRequest) {
				r.UpdateConfiguration.BackupConfiguration = &clustersv1.BackupConfiguration{
					BackupsEnabled:  true,
					BackupRetention: "14d",
				}
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.BackupSpec.Method = v1alpha1.BackupMethodPgBackRest
				b.Spec.BackupSpec.Retention = "14d"
				b.Spec.BackupSpec.ScheduledBackup.Schedule = "0 2 * * *"
				b.Spec.BackupSpec.PgBackRest = &v1alpha1.PgBackRestSpec{
					S3: &v1alpha1.PgBackRestS3Spec{
						Bucket: "test-bucket",
						Region: "us-east-1",
					},
					RetentionFullDays:   14,
					CompressType:        DefaultPgBackRestCompressType,
					ArchiveAsync:        DefaultPgBackRestArchiveAsync,
					ArchivePushQueueMax: DefaultPgBackRestPushQueueMax,
					ArchiveGetQueueMax:  DefaultPgBackRestGetQueueMax,
				}
			},
		},
		{
			name: "update backup config is rejected when BackupSpec is nil",
			inputBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.BackupSpec = nil
			},
			requestFn: func(r *clustersv1.UpdatePostgresClusterRequest) {
				r.UpdateConfiguration.BackupConfiguration = &clustersv1.BackupConfiguration{
					BackupsEnabled:  true,
					BackupRetention: "14d",
					BackupSchedule:  "0 7 * * *",
				}
			},
			expectedStatusCode: codes.InvalidArgument,
		},
		{
			name:        "storage QoS class update - replaces the VolumeAttributesClass",
			serviceOpts: []testServiceOption{withStorageQoSClasses(true)},
			inputBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Storage.StorageClass = new("xatastor")
				b.Spec.ClusterSpec.Storage.VolumeAttributesClass = new("xatastor-micro")
			},
			requestFn: func(r *clustersv1.UpdatePostgresClusterRequest) {
				r.UpdateConfiguration.StorageQosClass = new(storageqos.ClassXLarge)
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Storage.StorageClass = new("xatastor")
				b.Spec.ClusterSpec.Storage.VolumeAttributesClass = new("xatastor-xlarge")
			},
		},
		{
			name:        "storage QoS class update - unset class preserves the VolumeAttributesClass",
			serviceOpts: []testServiceOption{withStorageQoSClasses(true)},
			inputBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Storage.StorageClass = new("xatastor")
				b.Spec.ClusterSpec.Storage.VolumeAttributesClass = new("xatastor-micro")
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Storage.StorageClass = new("xatastor")
				b.Spec.ClusterSpec.Storage.VolumeAttributesClass = new("xatastor-micro")
			},
		},
		{
			name:        "storage QoS class update - ignored when cell QoS support is disabled",
			serviceOpts: []testServiceOption{withStorageQoSClasses(false)},
			inputBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Storage.StorageClass = new("xatastor")
				b.Spec.ClusterSpec.Storage.VolumeAttributesClass = new("xatastor-micro")
			},
			requestFn: func(r *clustersv1.UpdatePostgresClusterRequest) {
				r.UpdateConfiguration.StorageQosClass = new(storageqos.ClassXLarge)
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Storage.StorageClass = new("xatastor")
				b.Spec.ClusterSpec.Storage.VolumeAttributesClass = new("xatastor-micro")
			},
		},
		{
			name:        "storage QoS class update - unknown class preserves the VolumeAttributesClass",
			serviceOpts: []testServiceOption{withStorageQoSClasses(true)},
			inputBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Storage.StorageClass = new("xatastor")
				b.Spec.ClusterSpec.Storage.VolumeAttributesClass = new("xatastor-micro")
			},
			requestFn: func(r *clustersv1.UpdatePostgresClusterRequest) {
				r.UpdateConfiguration.StorageQosClass = new("no-such-class")
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Storage.StorageClass = new("xatastor")
				b.Spec.ClusterSpec.Storage.VolumeAttributesClass = new("xatastor-micro")
			},
		},
		{
			name:        "storage QoS class update - ignored on a storage class without VolumeAttributesClasses",
			serviceOpts: []testServiceOption{withStorageQoSClasses(true)},
			inputBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Storage.StorageClass = new("default-storage-class")
				b.Spec.ClusterSpec.Storage.VolumeAttributesClass = nil
			},
			requestFn: func(r *clustersv1.UpdatePostgresClusterRequest) {
				r.UpdateConfiguration.StorageQosClass = new(storageqos.ClassXLarge)
			},
			expectedBranchFn: func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Storage.StorageClass = new("default-storage-class")
				b.Spec.ClusterSpec.Storage.VolumeAttributesClass = nil
			},
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
			svc, k8sClient := setupTestClustersService(t,
				append([]testServiceOption{withExistingObjects(existingObjs...)}, tt.serviceOpts...)...)

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
				require.Equal(t, expectedBranch.Annotations, br.Annotations)

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
						Name: "test-branch",
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
						Name:      "test-branch-cluster",
						Namespace: "xata-clusters",
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
				UsesWakeupPool: new(false),
				Id:             "test-branch",
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
						Name: "test-branch",
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
						Name:      "test-branch-cluster",
						Namespace: "xata-clusters",
						Annotations: map[string]string{
							"cnpg.io/hibernation": "on",
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
				UsesWakeupPool: new(false),
				Id:             "test-branch",
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
						Name: "test-branch",
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
						Name:      "test-branch-cluster",
						Namespace: "xata-clusters",
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
				UsesWakeupPool: new(false),
				Id:             "test-branch",
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
			name: "branch awaiting wakeup",
			existingObjects: func() []client.Object {
				_, branch, _, _, _ := exampleRequestsAndBranches()
				branch.Annotations = map[string]string{
					v1alpha1.AwaitingWakeupAnnotation: "true",
				}
				branch.Spec.ClusterSpec.Name = nil
				return []client.Object{branch}
			},
			requestID: "lsmevenv7t3l56euo1v9bh3b74",
			wantResp: func() *clustersv1.DescribePostgresClusterResponse {
				_, _, resp, _, _ := exampleRequestsAndBranches()
				resp.Status = &clustersv1.ClusterStatus{
					Status:     apiv1.PhaseHealthy,
					StatusType: clustersv1.ClusterStatus_STATUS_TYPE_TRANSIENT,
				}
				return resp
			}(),
		},
		{
			name: "branch with a VolumeAttributesClass reports its storage QoS class",
			existingObjects: func() []client.Object {
				_, branch, _, _, _ := exampleRequestsAndBranches()
				branch.Spec.ClusterSpec.Storage.StorageClass = new("xatastor")
				branch.Spec.ClusterSpec.Storage.VolumeAttributesClass = new("xatastor-micro")
				return []client.Object{branch}
			},
			requestID: "lsmevenv7t3l56euo1v9bh3b74",
			wantResp: func() *clustersv1.DescribePostgresClusterResponse {
				_, _, resp, _, _ := exampleRequestsAndBranches()
				resp.Status = &clustersv1.ClusterStatus{
					Status:     apiv1.PhaseHealthy,
					StatusType: clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED,
				}
				resp.Configuration.StorageQosClass = new(storageqos.ClassMicro)
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
		username  string
		setupMock func(m *cnpgmocks.Connector)
		wantUser  string
		wantErr   string
	}{
		"success": {
			username: "app",
			setupMock: func(m *cnpgmocks.Connector) {
				m.EXPECT().GetClusterCredentials(mock.Anything, "cluster-1", "xata-clusters", "app").
					Return(&cnpg.Credentials{Username: "xata", Password: "secret"}, nil)
			},
			wantUser: "xata",
		},
		"success with superuser": {
			username: "superuser",
			setupMock: func(m *cnpgmocks.Connector) {
				m.EXPECT().GetClusterCredentials(mock.Anything, "cluster-1", "xata-clusters", "superuser").
					Return(&cnpg.Credentials{Username: "postgres", Password: "secret"}, nil)
			},
			wantUser: "postgres",
		},
		"secret not found": {
			username: "app",
			setupMock: func(m *cnpgmocks.Connector) {
				m.EXPECT().GetClusterCredentials(mock.Anything, "cluster-1", "xata-clusters", "app").
					Return(nil, errors.NewNotFound(schema.GroupResource{Resource: "secrets"}, "cluster-1-app"))
			},
			wantErr: "secret not found",
		},
		"other error": {
			username: "app",
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
				&clustersv1.GetPostgresClusterCredentialsRequest{Id: "cluster-1", Username: tt.username})

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

func TestRotatePostgresClusterCredentials(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		user               string
		branchExists       bool
		createSecret       bool
		expectedStatusCode codes.Code
		secretSuffix       string
	}{
		"success with xata user": {
			user:         "xata",
			branchExists: true,
			createSecret: true,
			secretSuffix: "app",
		},
		"success with postgres user": {
			user:         "postgres",
			branchExists: true,
			createSecret: true,
			secretSuffix: "superuser",
		},
		"unknown user": {
			user:               "unknown",
			branchExists:       true,
			expectedStatusCode: codes.InvalidArgument,
		},
		"branch not found": {
			user:               "xata",
			branchExists:       false,
			expectedStatusCode: codes.NotFound,
		},
		"secret not found": {
			user:               "xata",
			branchExists:       true,
			createSecret:       false,
			expectedStatusCode: codes.NotFound,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			_, branchToUpdate, _, _, _ := exampleRequestsAndBranches()

			secretName := branchToUpdate.Name + "-" + tt.secretSuffix

			var existingObjs []client.Object
			if tt.branchExists {
				existingObjs = append(existingObjs, branchToUpdate)
			}
			if tt.createSecret {
				existingObjs = append(existingObjs, &corev1.Secret{
					Name:      secretName,
					Namespace: "xata-clusters",
					Type:      corev1.SecretTypeBasicAuth,
					Data: map[string][]byte{
						corev1.BasicAuthUsernameKey: []byte("user"),
						corev1.BasicAuthPasswordKey: []byte("pass"),
					},
				})
			}

			svc, k8sClient := setupTestClustersService(t, withExistingObjects(existingObjs...))

			req := &clustersv1.RotatePostgresClusterCredentialsRequest{
				Id:   branchToUpdate.Name,
				User: tt.user,
			}

			_, err := svc.RotatePostgresClusterCredentials(ctx, req)
			st, _ := status.FromError(err)
			require.Equal(t, tt.expectedStatusCode, st.Code())

			if tt.expectedStatusCode == codes.OK {
				// Verify the secret was updated in place with a new password
				secret := &corev1.Secret{}
				err := k8sClient.Get(ctx, client.ObjectKey{
					Name:      secretName,
					Namespace: "xata-clusters",
				}, secret)
				require.NoError(t, err)
				require.Equal(t, corev1.SecretTypeBasicAuth, secret.Type)
				require.Equal(t, "user", string(secret.Data[corev1.BasicAuthUsernameKey]))
				require.NotEqual(t, "pass", string(secret.Data[corev1.BasicAuthPasswordKey]))
				require.Len(t, string(secret.Data[corev1.BasicAuthPasswordKey]), 64)
			}
		})
	}
}

func TestGetRecoveryWindow(t *testing.T) {
	t.Parallel()

	pgbackrestBranch := &v1alpha1.Branch{
		Name: "branch-1",
		Spec: v1alpha1.BranchSpec{
			BackupSpec: &v1alpha1.BackupSpec{
				Method:     v1alpha1.BackupMethodPgBackRest,
				PgBackRest: &v1alpha1.PgBackRestSpec{},
			},
		},
		Status: v1alpha1.BranchStatus{
			FirstRecoverabilityPoint: "2023-01-01T10:00:00Z",
			LastRecoverabilityPoint:  "2023-01-02T16:00:00Z",
			LastSuccessfulBackup:     "2023-01-02T15:30:00Z",
			LastFailedBackup:         "2023-01-02T09:00:00Z",
		},
	}

	branchWithoutBackups := &v1alpha1.Branch{
		Name: "branch-1",
	}

	tests := map[string]struct {
		branch   *v1alpha1.Branch
		wantErr  string
		wantCode codes.Code
		validate func(t *testing.T, resp *clustersv1.GetRecoveryWindowResponse)
	}{
		"branch with backups reads the Branch status": {
			branch: pgbackrestBranch,
			validate: func(t *testing.T, resp *clustersv1.GetRecoveryWindowResponse) {
				require.Equal(t, "2023-01-01T10:00:00Z", resp.FirstRecoverabilityPoint)
				require.Equal(t, "2023-01-02T16:00:00Z", resp.LastRecoverabilityPoint)
				require.Equal(t, "2023-01-02T15:30:00Z", resp.LastSuccessfulBackup)
				require.Equal(t, "2023-01-02T09:00:00Z", resp.LastFailedBackup)
			},
		},
		"branch without backups": {
			branch:   branchWithoutBackups,
			wantErr:  "has no backups",
			wantCode: codes.NotFound,
		},
		"branch not found": {
			branch:   nil,
			wantErr:  "not found",
			wantCode: codes.NotFound,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var opts []testServiceOption
			if tt.branch != nil {
				opts = append(opts, withExistingObjects(tt.branch.DeepCopy()))
			}
			svc, _ := setupTestClustersService(t, opts...)

			resp, err := svc.GetRecoveryWindow(context.Background(),
				&clustersv1.GetRecoveryWindowRequest{Id: "branch-1"})

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				if tt.wantCode != codes.OK {
					require.Equal(t, tt.wantCode, status.Code(err))
				}
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
	existingObjects      []client.Object
	nodeSelector         map[string]string
	cnpgConnector        *cnpgmocks.Connector
	xatastorEnabled      bool
	useStorageQoSClasses bool
	pgBackRestBucket     string
	pgBackRestEncryption bool
	cloudProvider        string
	pgBackRestGCSService string
	pgBackRestAzureAcct  string
	pgBackRestAzureCont  string
	interceptorFuncs     interceptor.Funcs
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

func withXatastorEnabled(enabled bool) testServiceOption {
	return func(c *testServiceConfig) {
		c.xatastorEnabled = enabled
	}
}

func withStorageQoSClasses(enabled bool) testServiceOption {
	return func(c *testServiceConfig) {
		c.useStorageQoSClasses = enabled
	}
}

func withPgBackRestBucket(bucket string) testServiceOption {
	return func(c *testServiceConfig) {
		c.pgBackRestBucket = bucket
	}
}

func withCloudProvider(provider string) testServiceOption {
	return func(c *testServiceConfig) {
		c.cloudProvider = provider
	}
}

func withPgBackRestGCSServiceAccount(serviceAccount string) testServiceOption {
	return func(c *testServiceConfig) {
		c.pgBackRestGCSService = serviceAccount
	}
}

func withPgBackRestAzure(account, container string) testServiceOption {
	return func(c *testServiceConfig) {
		c.pgBackRestAzureAcct = account
		c.pgBackRestAzureCont = container
	}
}

func withInterceptorFuncs(funcs interceptor.Funcs) testServiceOption {
	return func(c *testServiceConfig) {
		c.interceptorFuncs = funcs
	}
}

func setupTestClustersService(t *testing.T, opts ...testServiceOption) (*ClustersService, client.Client) {
	cfg := &testServiceConfig{
		pgBackRestBucket: "test-pgbackrest-bucket",
		cloudProvider:    CloudProviderAWS,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, apiv1.AddToScheme(scheme))
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	require.NoError(t, cpv1alpha1.AddToScheme(scheme))

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cfg.existingObjects...).
		WithIndex(&apiv1.Cluster{}, clusterOwnerKey, func(obj client.Object) []string {
			owner := metav1.GetControllerOf(obj)
			if owner == nil {
				return nil
			}
			if owner.APIVersion != cpv1alpha1.GroupVersion.String() || owner.Kind != cpv1alpha1.ClusterPoolKind {
				return nil
			}
			return []string{owner.Name}
		}).
		WithInterceptorFuncs(cfg.interceptorFuncs).
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
			XatastorEnabled:             cfg.xatastorEnabled,
			UseStorageQoSClasses:        cfg.useStorageQoSClasses,
			CloudProvider:               cfg.cloudProvider,
			PgBackRestBucket:            cfg.pgBackRestBucket,
			PgBackRestRegion:            "us-east-1",
			PgBackRestGCSServiceAccount: cfg.pgBackRestGCSService,
			PgBackRestAzureAccount:      cfg.pgBackRestAzureAcct,
			PgBackRestAzureContainer:    cfg.pgBackRestAzureCont,
			PgBackRestEncryptionEnabled: cfg.pgBackRestEncryption,
		},
		kubeClient:     fakeClient,
		scheme:         scheme,
		clusterReader:  fakeClient,
		clusterCacheOk: cacheReady,
		cnpgConnector:  cfg.cnpgConnector,
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
			Name: "lsmevenv7t3l56euo1v9bh3b74",
			Labels: map[string]string{
				LabelOrgID:     "jf1tpn",
				LabelProjectID: "prj_9tmrorf02l0gv9ioptqat1uc6k",
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
							{Name: "pg_stat_statements.track_utility", Value: "off"},
							{Name: "recovery_init_sync_method", Value: "syncfs"},
							{Name: "shared_buffers", Value: "256MB"},
							{Name: "xatautils.extension_custom_scripts_path", Value: "/etc/xatautils/extensions"},
							{Name: "xatautils.privileged_extensions", Value: expectedPrivilegedExtensions},
							{Name: "xatautils.privileged_role", Value: "xata"},
							{Name: "xatautils.privileged_role_allowed_configs", Value: "auth_delay.*, auto_explain.*, log_lock_waits, log_min_duration_statement, log_min_messages, log_replication_commands, log_statement, log_temp_files, pg_net.batch_size, pg_net.ttl, pg_stat_statements.*, pgaudit.log, pgaudit.log_catalog, pgaudit.log_client, pgaudit.log_level, pgaudit.log_relation, pgaudit.log_rows, pgaudit.log_statement, pgaudit.log_statement_once, pgaudit.role, pgrst.*, plan_filter.*, safeupdate.enabled, session_replication_role, track_io_timing, wal_compression"},
							{Name: "xatautils.reserved_memberships", Value: "xata_superuser, pg_read_server_files, pg_write_server_files, pg_execute_server_program"},
							{Name: "xatautils.reserved_roles", Value: "cnpg_pooler_pgbouncer, pgbouncer, postgres, streaming_replica, xata_superuser"},
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
					Method:    v1alpha1.BackupMethodPgBackRest,
					Retention: "7d",
					ScheduledBackup: &v1alpha1.ScheduledBackupSpec{
						Schedule: "0 2 * * *",
					},
					PgBackRest: &v1alpha1.PgBackRestSpec{
						S3: &v1alpha1.PgBackRestS3Spec{
							Bucket:  "test-pgbackrest-bucket",
							Region:  "us-east-1",
							KeyType: webIdentityKeyType,
						},
						RepoPath:            webIdentityRepoPath("lsmevenv7t3l56euo1v9bh3b74"),
						RetentionFullDays:   7,
						CompressType:        DefaultPgBackRestCompressType,
						ArchiveAsync:        DefaultPgBackRestArchiveAsync,
						ArchivePushQueueMax: DefaultPgBackRestPushQueueMax,
						ArchiveGetQueueMax:  DefaultPgBackRestGetQueueMax,
					},
				},
				InheritedMetadata: &v1alpha1.InheritedMetadata{
					Labels: map[string]string{
						LabelOrgID:                     "jf1tpn",
						LabelProjectID:                 "prj_9tmrorf02l0gv9ioptqat1uc6k",
						LabelBranchID:                  "lsmevenv7t3l56euo1v9bh3b74",
						LabelXatastorAntiAffinityGroup: "lsmevenv7t3l56euo1v9bh3b74",
					},
				},
			},
		},
		&clustersv1.DescribePostgresClusterResponse{
			UsesWakeupPool: new(false),
			Id:             "lsmevenv7t3l56euo1v9bh3b74",
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
					"lc_messages":                      "C.utf8",
					"lc_monetary":                      "C.utf8",
					"lc_numeric":                       "C.utf8",
					"lc_time":                          "C.utf8",
					"max_connections":                  "200",
					"pg_stat_statements.track_utility": "off",
					"recovery_init_sync_method":        "syncfs",
					"shared_buffers":                   "256MB",
					"xatautils.extension_custom_scripts_path":   "/etc/xatautils/extensions",
					"xatautils.privileged_extensions":           expectedPrivilegedExtensions,
					"xatautils.privileged_role":                 "xata",
					"xatautils.privileged_role_allowed_configs": "auth_delay.*, auto_explain.*, log_lock_waits, log_min_duration_statement, log_min_messages, log_replication_commands, log_statement, log_temp_files, pg_net.batch_size, pg_net.ttl, pg_stat_statements.*, pgaudit.log, pgaudit.log_catalog, pgaudit.log_client, pgaudit.log_level, pgaudit.log_relation, pgaudit.log_rows, pgaudit.log_statement, pgaudit.log_statement_once, pgaudit.role, pgrst.*, plan_filter.*, safeupdate.enabled, session_replication_role, track_io_timing, wal_compression",
					"xatautils.reserved_memberships":            "xata_superuser, pg_read_server_files, pg_write_server_files, pg_execute_server_program",
					"xatautils.reserved_roles":                  "cnpg_pooler_pgbouncer, pgbouncer, postgres, streaming_replica, xata_superuser",
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
			Name: "lsmevenv7t3l56euo1v9bh3b74",
			Labels: map[string]string{
				LabelOrgID:     "jf1tpn",
				LabelProjectID: "prj_9tmrorf02l0gv9ioptqat1uc6k",
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
							{Name: "pg_stat_statements.track_utility", Value: "off"},
							{Name: "recovery_init_sync_method", Value: "syncfs"},
							{Name: "shared_buffers", Value: "512MB"},
							{Name: "work_mem", Value: "64MB"},
							{Name: "xatautils.extension_custom_scripts_path", Value: "/etc/xatautils/extensions"},
							{Name: "xatautils.privileged_extensions", Value: expectedPrivilegedExtensions},
							{Name: "xatautils.privileged_role", Value: "xata"},
							{Name: "xatautils.privileged_role_allowed_configs", Value: "auth_delay.*, auto_explain.*, log_lock_waits, log_min_duration_statement, log_min_messages, log_replication_commands, log_statement, log_temp_files, pg_net.batch_size, pg_net.ttl, pg_stat_statements.*, pgaudit.log, pgaudit.log_catalog, pgaudit.log_client, pgaudit.log_level, pgaudit.log_relation, pgaudit.log_rows, pgaudit.log_statement, pgaudit.log_statement_once, pgaudit.role, pgrst.*, plan_filter.*, safeupdate.enabled, session_replication_role, track_io_timing, wal_compression"},
							{Name: "xatautils.reserved_memberships", Value: "xata_superuser, pg_read_server_files, pg_write_server_files, pg_execute_server_program"},
							{Name: "xatautils.reserved_roles", Value: "cnpg_pooler_pgbouncer, pgbouncer, postgres, streaming_replica, xata_superuser"},
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
					Method:    v1alpha1.BackupMethodPgBackRest,
					Retention: "14d",
					ScheduledBackup: &v1alpha1.ScheduledBackupSpec{
						Schedule: "0 7 * * *",
					},
					PgBackRest: &v1alpha1.PgBackRestSpec{
						S3: &v1alpha1.PgBackRestS3Spec{
							Bucket:  "test-pgbackrest-bucket",
							Region:  "us-east-1",
							KeyType: webIdentityKeyType,
						},
						RepoPath:            webIdentityRepoPath("lsmevenv7t3l56euo1v9bh3b74"),
						RetentionFullDays:   14,
						CompressType:        DefaultPgBackRestCompressType,
						ArchiveAsync:        DefaultPgBackRestArchiveAsync,
						ArchivePushQueueMax: DefaultPgBackRestPushQueueMax,
						ArchiveGetQueueMax:  DefaultPgBackRestGetQueueMax,
					},
				},
				InheritedMetadata: &v1alpha1.InheritedMetadata{
					Labels: map[string]string{
						LabelOrgID:                     "jf1tpn",
						LabelProjectID:                 "prj_9tmrorf02l0gv9ioptqat1uc6k",
						LabelBranchID:                  "lsmevenv7t3l56euo1v9bh3b74",
						LabelXatastorAntiAffinityGroup: "lsmevenv7t3l56euo1v9bh3b74",
					},
				},
			},
		}
}

func parentBranch(opts ...parentBranchOption) *v1alpha1.Branch {
	b := &v1alpha1.Branch{
		Name:   "gmnfj6042d3qd09dcc8a7le0eo",
		Labels: testProjectLabels(),
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

func withVolumeAttributesClass(className string) parentBranchOption {
	return func(b *v1alpha1.Branch) {
		b.Spec.ClusterSpec.Storage.VolumeAttributesClass = new(className)
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

func withPgBackRestBackups() parentBranchOption {
	return func(b *v1alpha1.Branch) {
		b.Spec.BackupSpec = &v1alpha1.BackupSpec{
			Retention: "2d",
			Method:    v1alpha1.BackupMethodPgBackRest,
			PgBackRest: &v1alpha1.PgBackRestSpec{
				S3: &v1alpha1.PgBackRestS3Spec{Bucket: "test-pgbackrest-bucket", Region: "us-east-1", KeyType: webIdentityKeyType},
			},
		}
	}
}

func withWakeupPool(pool string) parentBranchOption {
	return func(b *v1alpha1.Branch) {
		if b.Annotations == nil {
			b.Annotations = map[string]string{}
		}
		b.Annotations[v1alpha1.WakeupPoolAnnotation] = pool
	}
}

func sourceClusterForPITR() *apiv1.Cluster {
	return &apiv1.Cluster{
		Name:      "source-cluster-for-pitr",
		Namespace: "xata-clusters",
		Spec: apiv1.ClusterSpec{
			ImageName: "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:16.3",
			StorageConfiguration: apiv1.StorageConfiguration{
				Size: "100Gi",
			},
		},
	}
}

// sourceBranchForPITR returns the Branch of the continuous backup source that
// sourceClusterForPITR runs. The service reads it to decide whether a restore
// needs the source's repository cipher.
func sourceBranchForPITR() *v1alpha1.Branch {
	return &v1alpha1.Branch{
		Name:   "source-cluster-for-pitr",
		Labels: testProjectLabels(),
	}
}

// testProjectLabels returns the organization and project labels of the
// branches that exampleRequestsAndBranches creates.
func testProjectLabels() map[string]string {
	return map[string]string{
		LabelOrgID:     "jf1tpn",
		LabelProjectID: "prj_9tmrorf02l0gv9ioptqat1uc6k",
	}
}

// pgBackRestRestoreSource returns a source Branch named restore-source that
// backs up to pgb.
func pgBackRestRestoreSource(pgb *v1alpha1.PgBackRestSpec) *v1alpha1.Branch {
	return &v1alpha1.Branch{
		Name:   "restore-source",
		Labels: testProjectLabels(),
		Spec: v1alpha1.BranchSpec{
			BackupSpec: &v1alpha1.BackupSpec{
				Method:     v1alpha1.BackupMethodPgBackRest,
				PgBackRest: pgb,
			},
		},
	}
}

// pgBackRestRestoreRequest restores a pgbackrest branch from restore-source.
func pgBackRestRestoreRequest(r *clustersv1.CreatePostgresClusterRequest) {
	r.BackupConfiguration.BackupMethod = string(v1alpha1.BackupMethodPgBackRest)
	r.DataSource = &clustersv1.CreatePostgresClusterRequest_ContinuousBackup{
		ContinuousBackup: &clustersv1.ContinuousBackup{
			ClusterId: "restore-source",
		},
	}
}

// expectPoolParentInheritance applies the settings a child branch inherits from
// the parent built by parentBranch(withStorageClass("xatastor"),
// withWakeupPool(...)), leaving out the pool adoption itself (restore type,
// cluster name and annotations), which depends on how the child is created.
func expectPoolParentInheritance(b *v1alpha1.Branch) {
	b.Spec.ClusterSpec.Instances = 1
	b.Spec.ClusterSpec.Image = "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:16.3"
	b.Spec.ClusterSpec.Storage.Size = "200Gi"
	b.Spec.ClusterSpec.Storage.StorageClass = new("xatastor")
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
}

// poolParentBranch returns a parent branch that runs on a cluster from a wakeup
// pool. Such a branch always uses pgbackrest, because WithClusterFromPool drops
// any other backup method when it adopts a pool cluster.
func poolParentBranch() *v1alpha1.Branch {
	return parentBranch(
		withStorageClass("xatastor"),
		withWakeupPool("test-pool"),
		withPgBackRestBackups(),
	)
}

// expectPgBackRestBackupSpec applies the BackupSpec that the service builds for
// a branch that requests the pgbackrest backup method.
func expectPgBackRestBackupSpec(b *v1alpha1.Branch) {
	b.Spec.BackupSpec.Method = v1alpha1.BackupMethodPgBackRest
	b.Spec.BackupSpec.PgBackRest = &v1alpha1.PgBackRestSpec{
		S3: &v1alpha1.PgBackRestS3Spec{
			Bucket:  "test-pgbackrest-bucket",
			Region:  "us-east-1",
			KeyType: webIdentityKeyType,
		},
		RepoPath:            webIdentityRepoPath(b.Name),
		RetentionFullDays:   7,
		CompressType:        DefaultPgBackRestCompressType,
		ArchiveAsync:        DefaultPgBackRestArchiveAsync,
		ArchivePushQueueMax: DefaultPgBackRestPushQueueMax,
		ArchiveGetQueueMax:  DefaultPgBackRestGetQueueMax,
	}
}

func poolForTest(storageClass, image, cpu, memory string) *cpv1alpha1.ClusterPool {
	return &cpv1alpha1.ClusterPool{
		Name:      "test-pool",
		Namespace: "xata-clusters",
		UID:       "pool-uid-123",
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
		Name:      "pool-cluster-1",
		Namespace: "xata-clusters",
		OwnerReferences: []metav1.OwnerReference{
			{
				APIVersion: cpv1alpha1.GroupVersion.String(),
				Kind:       cpv1alpha1.ClusterPoolKind,
				UID:        "pool-uid-123",
				Name:       "test-pool",
				Controller: new(true),
			},
		},
		Status: apiv1.ClusterStatus{
			Phase:          apiv1.PhaseHealthy,
			ReadyInstances: 1,
		},
	}
}

// poolClusterAppSecretForTest returns the CNPG-generated app secret of the
// pool cluster from poolClusterForTest. It carries the extra keys CNPG writes
// so tests can check that only the password is copied to the branch secret.
func poolClusterAppSecretForTest(password string) *corev1.Secret {
	return &corev1.Secret{
		Name:      "pool-cluster-1-app",
		Namespace: "xata-clusters",
		Labels: map[string]string{
			"cnpg.io/cluster": "pool-cluster-1",
		},
		Type: corev1.SecretTypeBasicAuth,
		Data: map[string][]byte{
			corev1.BasicAuthUsernameKey: []byte("xata"),
			corev1.BasicAuthPasswordKey: []byte(password),
			"dbname":                    []byte("xata"),
			"uri":                       []byte("postgresql://xata:" + password + "@pool-cluster-1-rw.xata-clusters:5432/xata"),
		},
	}
}

func TestCreateWakeupRequestFromUpdate(t *testing.T) {
	t.Parallel()

	branchName := "lsmevenv7t3l56euo1v9bh3b74"
	namespace := "xata-clusters"

	baseBranch := func() *v1alpha1.Branch {
		_, b, _, _, _ := exampleRequestsAndBranches()
		b.Annotations = map[string]string{
			v1alpha1.WakeupPoolAnnotation: "test-pool",
		}
		b.Status.PrimaryXVolName = "branch-xvol"
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
			Name:      branchName,
			Namespace: namespace,
			Labels:    map[string]string{"initialWUR": "true"},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: v1alpha1.GroupVersion.String(),
					Kind:       v1alpha1.BranchKind,
					Name:       branchName,
				},
			},
			Spec: v1alpha1.WakeupRequestSpec{
				BranchName: branchName,
				XVolName:   "some-xvol",
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
		interceptors  interceptor.Funcs
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
				existingWUR(metav1.ConditionFalse, v1alpha1.CSINodePodNotFoundReason),
			},
			wantWUR: true,
			wantNew: true,
		},
		{
			// Concurrent wakers all see no WakeupRequest at the Get and race to
			// create one; the losers get AlreadyExists, which must be swallowed.
			name: "no error when create races and returns AlreadyExists",
			interceptors: interceptor.Funcs{
				Create: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.CreateOption) error {
					return errors.NewAlreadyExists(schema.GroupResource{Resource: "wakeuprequests"}, obj.GetName())
				},
			},
			wantWUR: false,
		},
		{
			// Concurrent wakers both delete a finished WakeupRequest; the loser
			// gets NotFound on delete, which must be swallowed.
			name: "no error when delete races and returns NotFound",
			extraObjects: []client.Object{
				existingWUR(metav1.ConditionTrue, v1alpha1.WakeupSucceededReason),
			},
			interceptors: interceptor.Funcs{
				Delete: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.DeleteOption) error {
					return errors.NewNotFound(schema.GroupResource{Resource: "wakeuprequests"}, obj.GetName())
				},
			},
			wantWUR: true,
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
			svc, k8sClient := setupTestClustersService(t,
				withExistingObjects(existingObjs...),
				withInterceptorFuncs(tt.interceptors),
			)

			err := svc.createWakeupRequestFromUpdateClusterRequest(ctx, branch, req)
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
				require.Len(t, wur.GetOwnerReferences(), 1)
				ref := wur.GetOwnerReferences()[0]
				require.Equal(t, branchName, ref.Name)
				require.Equal(t, v1alpha1.BranchKind, ref.Kind)
				if tt.wantNew {
					require.Empty(t, wur.Labels["initialWUR"])
					require.Equal(t, "branch-xvol", wur.Spec.XVolName)
					require.Equal(t, v1alpha1.PasswordSyncModeSkip, wur.Spec.PasswordSync)
				} else {
					require.Equal(t, "true", wur.Labels["initialWUR"])
					require.Equal(t, "some-xvol", wur.Spec.XVolName)
				}
			} else {
				require.True(t, errors.IsNotFound(getErr))
			}
		})
	}
}

func TestCreateWakeupRequestForNewBranch(t *testing.T) {
	t.Parallel()

	const (
		parentName = "parent-branch"
		childName  = "child-branch"
		namespace  = "xata-clusters"
	)

	newChild := func(restore *v1alpha1.RestoreSpec) *v1alpha1.Branch {
		return &v1alpha1.Branch{
			Name:      childName,
			Namespace: namespace,
			Spec: v1alpha1.BranchSpec{
				Restore: restore,
			},
		}
	}

	newParent := func() *v1alpha1.Branch {
		return &v1alpha1.Branch{
			Name:      parentName,
			Namespace: namespace,
		}
	}

	tests := map[string]struct {
		branch  *v1alpha1.Branch
		parent  *v1alpha1.Branch
		wantWUR bool
	}{
		"no-op when restore type is not XVolClone": {
			branch: newChild(&v1alpha1.RestoreSpec{
				Type: v1alpha1.RestoreTypeVolumeSnapshot,
				Name: parentName,
			}),
			parent:  newParent(),
			wantWUR: false,
		},
		"no-op when XVolClone restore but parent is nil": {
			branch: newChild(&v1alpha1.RestoreSpec{
				Type: v1alpha1.RestoreTypeXVolClone,
				Name: parentName,
			}),
			parent:  nil,
			wantWUR: false,
		},
		"creates WakeupRequest with derived XVolName when XVolClone restore and parent present": {
			branch: newChild(&v1alpha1.RestoreSpec{
				Type: v1alpha1.RestoreTypeXVolClone,
				Name: parentName,
			}),
			parent:  newParent(),
			wantWUR: true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			svc, k8sClient := setupTestClustersService(t, withExistingObjects(tt.branch))

			err := svc.createWakeupRequestForNewBranch(ctx, tt.branch, tt.parent)
			require.NoError(t, err)

			wur := &v1alpha1.WakeupRequest{}
			getErr := k8sClient.Get(ctx, client.ObjectKey{
				Name:      childName,
				Namespace: namespace,
			}, wur)

			if !tt.wantWUR {
				require.True(t, errors.IsNotFound(getErr))
				return
			}

			require.NoError(t, getErr)
			require.Equal(t, childName, wur.Spec.BranchName)
			require.Equal(t, v1alpha1.XVolCloneName(parentName, childName), wur.Spec.XVolName)
			require.Equal(t, v1alpha1.PasswordSyncModeWait, wur.Spec.PasswordSync)
			require.Len(t, wur.GetOwnerReferences(), 1)
			ref := wur.GetOwnerReferences()[0]
			require.Equal(t, tt.branch.GetName(), ref.Name)
			require.Equal(t, v1alpha1.BranchKind, ref.Kind)
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

func TestDescribePostgresClusterWakeupPool(t *testing.T) {
	tests := map[string]struct{ pooled, clusterExists, requestWake bool }{
		"pooled hibernation":                             {pooled: true},
		"non-pooled cluster not created yet":             {},
		"non-pooled wake before operator reconciliation": {clusterExists: true, requestWake: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, branch, _, _, _ := exampleRequestsAndBranches()
			branch.Annotations = nil
			branch.Spec.ClusterSpec.Hibernation = nil
			if tc.pooled {
				branch.Annotations = map[string]string{v1alpha1.WakeupPoolAnnotation: "test-pool"}
				branch.Spec.ClusterSpec.Name = nil
			}
			objs := []client.Object{branch}
			if tc.clusterExists {
				branch.Spec.ClusterSpec.Hibernation = ptr.To(v1alpha1.HibernationModeEnabled)
				objs = append(objs, &apiv1.Cluster{
					Name:        branch.ClusterName(),
					Namespace:   "xata-clusters",
					Annotations: map[string]string{"cnpg.io/hibernation": "on"},
					Status:      apiv1.ClusterStatus{Phase: apiv1.PhaseHealthy},
				})
			}
			svc, _ := setupTestClustersService(t, withExistingObjects(objs...))
			if tc.requestWake {
				_, err := svc.UpdatePostgresCluster(context.Background(), &clustersv1.UpdatePostgresClusterRequest{
					Id: branch.Name, UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{Hibernate: new(false)},
				})
				require.NoError(t, err)
			}
			got, err := svc.DescribePostgresCluster(context.Background(), &clustersv1.DescribePostgresClusterRequest{Id: branch.Name})
			require.NoError(t, err)
			require.Equal(t, clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED, got.Status.StatusType)
			require.False(t, got.Configuration.Hibernate)
			require.True(t, got.Configuration.ScaleToZero.Enabled)
			require.NotNil(t, got.UsesWakeupPool)
			require.Equal(t, tc.pooled, *got.UsesWakeupPool)
		})
	}
}
