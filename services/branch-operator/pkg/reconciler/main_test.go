package reconciler_test

import (
	"context"
	"os"
	"testing"
	"time"

	"xata/internal/envtestutil"
	"xata/internal/pgbackrest"
	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/reconciler"

	barmanPluginApi "github.com/cloudnative-pg/plugin-barman-cloud/api/v1"
	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	CRDDirectoryPath         = "../../../../charts/branch-operator/crds/"
	ThirdPartyCRDsPath       = "./testutils/crds/"
	SharedThirdPartyCRDsPath = "../../testutils/crds/"
	XataClustersNamespace    = "xata-clusters"
)

// k8sClient is a live Kubernetes client connected to the test environment.
// It does not use the controller cache for either reads or writes.
var k8sClient client.Client

// Make envtestutil functions available in this package for convenience
var (
	randomString           = envtestutil.RandomString
	requireEventuallyTrue  = envtestutil.RequireEventuallyTrue
	requireEventuallyNoErr = envtestutil.RequireEventuallyNoErr
)

// TestMain sets up the envtest integration test environment using the
// `envtestutil` package and runs the tests.
func TestMain(m *testing.M) {
	env := envtestutil.Setup(
		envtestutil.Options{
			CRDDirectoryPaths: []string{CRDDirectoryPath, ThirdPartyCRDsPath, SharedThirdPartyCRDsPath},
			SchemeAdders: []func(*runtime.Scheme) error{
				corev1.AddToScheme,
				v1alpha1.AddToScheme,
				networkingv1.AddToScheme,
				rbacv1.AddToScheme,
				apiv1.AddToScheme,
				barmanPluginApi.AddToScheme,
				snapshotv1.AddToScheme,
			},
			Namespaces: []string{XataClustersNamespace},
			ReconcilerSetup: func(ctx context.Context, mgr ctrl.Manager) error {
				r := &reconciler.BranchReconciler{
					Client:            mgr.GetClient(),
					Scheme:            mgr.GetScheme(),
					ClustersNamespace: XataClustersNamespace,
					BackupsBucket:     "s3://some-bucket",
					BackupsEndpoint:   "",
					CloudProvider:     "aws",
					// Each Cluster gets the web identity volume and environment variables.
					PgBackRestWebIdentityRoleARN: "arn:aws:iam::123456789012:role/test-cnpg-backups",
					BarmanRegionSecretName:       "barman-dummy-secret",
					BarmanRegionSecretKey:        "dummy",
					Tolerations:                  nil,
					EnforceZone:                  false,
					ImagePullSecrets:             nil,
				}
				return r.SetupWithManager(ctx, mgr)
			},
		})
	k8sClient = env.Client

	// Run the tests
	code := m.Run()

	env.Teardown()

	// Exit with the test return code
	os.Exit(code)
}

// withBranch creates the given branch and invokes the provided test function
func withBranch(ctx context.Context, t *testing.T, branch v1alpha1.Branch,
	fn func(t *testing.T, br *v1alpha1.Branch),
) {
	t.Helper()

	err := k8sClient.Create(ctx, &branch)
	require.NoError(t, err)

	b := v1alpha1.Branch{}
	err = k8sClient.Get(ctx, client.ObjectKey{Name: branch.Name}, &b)
	require.NoError(t, err)

	fn(t, &b)
}

// getK8SObject retrieves a Kubernetes object by name in the XataClustersNamespace.
func getK8SObject(ctx context.Context, name string, obj client.Object) error {
	return envtestutil.GetObject(ctx, k8sClient, name, XataClustersNamespace, obj)
}

// retryOnConflict tries to update the given object using the provided mutate function,
// retrying on conflict errors.
func retryOnConflict[T client.Object](ctx context.Context, obj T, mutateFn func(obj T)) error {
	return envtestutil.RetryOnConflict(ctx, k8sClient, obj, mutateFn)
}

// retryStatusOnConflict tries to update the status of the given object using
// the provided mutate function, retrying on conflict errors.
func retryStatusOnConflict[T client.Object](ctx context.Context, obj T, mutateFn func(obj T)) error {
	return envtestutil.RetryStatusOnConflict(ctx, k8sClient, obj, mutateFn)
}

// BranchBuilder is a builder for v1alpha1.Branch structs for use in tests
type BranchBuilder struct {
	branch v1alpha1.Branch
}

// NewBranchBuilder creates a new BranchBuilder with default values
func NewBranchBuilder() *BranchBuilder {
	branchName := randomString(10)

	return &BranchBuilder{
		branch: v1alpha1.Branch{
			ObjectMeta: metav1.ObjectMeta{
				Name: branchName,
			},
			Spec: v1alpha1.BranchSpec{
				ClusterSpec: v1alpha1.ClusterSpec{
					Name:      new(branchName),
					Instances: 1,
					Storage: v1alpha1.StorageSpec{
						Size: "1Gi",
					},
					Image: "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.7",
				},
			},
		},
	}
}

// WithName sets the Name and cluster name fields of the Branch
func (b *BranchBuilder) WithName(name string) *BranchBuilder {
	b.branch.Name = name
	return b
}

// WithClusterName sets the ClusterSpec.Name field of the Branch
func (b *BranchBuilder) WithClusterName(clusterName *string) *BranchBuilder {
	b.branch.Spec.ClusterSpec.Name = clusterName
	return b
}

// WithBackupRetention sets the BackupConfiguration.Retention field
func (b *BranchBuilder) WithBackupRetention(retention string) *BranchBuilder {
	if b.branch.Spec.BackupSpec == nil {
		b.branch.Spec.BackupSpec = &v1alpha1.BackupSpec{}
	}
	b.branch.Spec.BackupSpec.Retention = retention
	return b
}

func (b *BranchBuilder) WithWALArchiving(enabled bool) *BranchBuilder {
	if b.branch.Spec.BackupSpec == nil {
		b.branch.Spec.BackupSpec = &v1alpha1.BackupSpec{}
	}
	if enabled {
		b.branch.Spec.BackupSpec.WALArchiving = v1alpha1.WALArchivingModeEnabled
	} else {
		b.branch.Spec.BackupSpec.WALArchiving = v1alpha1.WALArchivingModeDisabled
	}
	return b
}

// WithPgBackRest sets the backup method to pgbackrest with the given S3 config
func (b *BranchBuilder) WithPgBackRest(bucket, region string) *BranchBuilder {
	if b.branch.Spec.BackupSpec == nil {
		b.branch.Spec.BackupSpec = &v1alpha1.BackupSpec{}
	}
	b.branch.Spec.BackupSpec.Method = v1alpha1.BackupMethodPgBackRest
	b.branch.Spec.BackupSpec.PgBackRest = &v1alpha1.PgBackRestSpec{
		S3: &v1alpha1.PgBackRestS3Spec{
			Bucket: bucket,
			Region: region,
		},
	}
	return b
}

// WithPgBackRestWebIdentity sets an S3 pgBackRest backend that gets
// credentials with the web identity token of the Branch ServiceAccount.
func (b *BranchBuilder) WithPgBackRestWebIdentity(bucket, region string) *BranchBuilder {
	if b.branch.Spec.BackupSpec == nil {
		b.branch.Spec.BackupSpec = &v1alpha1.BackupSpec{}
	}
	b.branch.Spec.BackupSpec.Method = v1alpha1.BackupMethodPgBackRest
	b.branch.Spec.BackupSpec.PgBackRest = &v1alpha1.PgBackRestSpec{
		S3: &v1alpha1.PgBackRestS3Spec{
			Bucket:  bucket,
			Region:  region,
			KeyType: pgbackrest.KeyTypeWebID,
		},
		RepoPath: pgbackrest.RepoPath(XataClustersNamespace, b.branch.Name),
	}
	return b
}

// WithPgBackRestCipher encrypts the pgBackRest repository of the Branch with
// the passphrase in its cipher Secret. Call after WithPgBackRest.
func (b *BranchBuilder) WithPgBackRestCipher() *BranchBuilder {
	b.branch.Spec.BackupSpec.PgBackRest.CipherPassphraseSecretRef = &corev1.SecretKeySelector{
		Name: pgbackrest.CipherSecretName(b.branch.Name),
		Key:  pgbackrest.CipherPassphraseKey,
	}
	return b
}

// WithPgBackRestRestoreCipher restores the Branch from the encrypted pgBackRest
// repository of the source Branch, reading its passphrase from the restore key
// of the cipher Secret of this Branch.
func (b *BranchBuilder) WithPgBackRestRestoreCipher(source string) *BranchBuilder {
	b.branch.Spec.Restore = &v1alpha1.RestoreSpec{
		Type: v1alpha1.RestoreTypeObjectStore,
		Name: source,
		PgBackRestCipherPassphraseSecretRef: &corev1.SecretKeySelector{
			Name: pgbackrest.CipherSecretName(b.branch.Name),
			Key:  pgbackrest.RestoreCipherPassphraseKey,
		},
	}
	return b
}

// WithBackupSchedule sets the BackupConfiguration.ScheduledBackup field
func (b *BranchBuilder) WithBackupSchedule(schedule string) *BranchBuilder {
	if b.branch.Spec.BackupSpec == nil {
		b.branch.Spec.BackupSpec = &v1alpha1.BackupSpec{}
	}
	b.branch.Spec.BackupSpec.ScheduledBackup = &v1alpha1.ScheduledBackupSpec{
		Schedule: schedule,
	}
	return b
}

// WithScaleToZeroInactivityPeriod sets the ScaleToZero configuration
func (b *BranchBuilder) WithScaleToZeroInactivityPeriod(m int32) *BranchBuilder {
	b.branch.Spec.ClusterSpec.ScaleToZero = &v1alpha1.ScaleToZeroConfiguration{
		Enabled:                 true,
		InactivityPeriodMinutes: m,
	}
	return b
}

// WithHibernationMode sets the Hibernation mode
func (b *BranchBuilder) WithHibernationMode(mode v1alpha1.HibernationMode) *BranchBuilder {
	b.branch.Spec.ClusterSpec.Hibernation = &mode
	return b
}

// WithRestore sets the Restore field
func (b *BranchBuilder) WithRestore(restoreType v1alpha1.RestoreType, name string) *BranchBuilder {
	b.branch.Spec.Restore = &v1alpha1.RestoreSpec{
		Type: restoreType,
		Name: name,
	}
	return b
}

// WithRestoreTimestamp sets the Restore field with a timestamp for PITR
func (b *BranchBuilder) WithRestoreTimestamp(restoreType v1alpha1.RestoreType, name string, timestamp time.Time) *BranchBuilder {
	b.branch.Spec.Restore = &v1alpha1.RestoreSpec{
		Type:      restoreType,
		Name:      name,
		Timestamp: &metav1.Time{Time: timestamp},
	}
	return b
}

// WithPooler enables a PgBouncer connection pooler
func (b *BranchBuilder) WithPooler() *BranchBuilder {
	b.branch.Spec.Pooler = &v1alpha1.PoolerSpec{
		Instances:     1,
		Mode:          v1alpha1.PoolModeSession,
		MaxClientConn: "100",
	}
	return b
}

// WithInheritedMetadata sets the InheritedMetadata field
func (b *BranchBuilder) WithInheritedMetadata(im *v1alpha1.InheritedMetadata) *BranchBuilder {
	b.branch.Spec.InheritedMetadata = im
	return b
}

// WithWakeupPool marks the Branch as a pool branch by setting the wakeup pool
// annotation
func (b *BranchBuilder) WithWakeupPool(pool string) *BranchBuilder {
	if b.branch.Annotations == nil {
		b.branch.Annotations = map[string]string{}
	}
	b.branch.Annotations[v1alpha1.WakeupPoolAnnotation] = pool
	return b
}

// Build builds the v1alpha1.BranchSpec struct
func (b *BranchBuilder) Build() v1alpha1.Branch {
	return b.branch
}
