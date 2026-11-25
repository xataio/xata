package reconciler_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	walgv1beta1 "github.com/wal-g/cnpg-plugin-wal-g/api/v1beta1"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"xata/internal/envtestutil"
	"xata/services/branching-replica-operator/api/v1alpha1"
	"xata/services/branching-replica-operator/pkg/reconciler"
)

const (
	CRDDirectoryPath   = "../../../../charts/branching-replica-operator/crds/"
	ThirdPartyCRDsPath = "./testutils/crds/"
	TestNamespace      = "test-replicas"

	testImage           = "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:18.7"
	testImagePullSecret = "test-pull-secret"
)

// testTolerations are the tolerations the reconciler under test is
// configured with
var testTolerations = []corev1.Toleration{
	{
		Key:    "xata.io/workload",
		Value:  "dataplane",
		Effect: corev1.TaintEffectNoSchedule,
	},
}

var k8sClient client.Client

func TestMain(m *testing.M) {
	env := envtestutil.Setup(envtestutil.Options{
		CRDDirectoryPaths: []string{CRDDirectoryPath, ThirdPartyCRDsPath},
		SchemeAdders: []func(*runtime.Scheme) error{
			corev1.AddToScheme,
			v1alpha1.AddToScheme,
			apiv1.AddToScheme,
			walgv1beta1.AddToScheme,
		},
		Namespaces: []string{TestNamespace},
		ReconcilerSetup: func(_ context.Context, mgr ctrl.Manager) error {
			r := &reconciler.BranchingReplicaReconciler{
				Client:           mgr.GetClient(),
				Tolerations:      testTolerations,
				ImagePullSecrets: []string{testImagePullSecret},
			}
			return r.SetupWithManager(mgr)
		},
	})
	k8sClient = env.Client

	code := m.Run()

	env.Teardown()

	os.Exit(code)
}

// withBranchingReplica creates a BranchingReplica with the given spec and a
// random name, and calls fn with the created resource
func withBranchingReplica(ctx context.Context, t *testing.T, spec v1alpha1.BranchingReplicaSpec,
	fn func(t *testing.T, replica *v1alpha1.BranchingReplica),
) {
	t.Helper()

	replica := &v1alpha1.BranchingReplica{
		Name:      envtestutil.RandomString(10),
		Namespace: TestNamespace,
		Spec:      spec,
	}

	err := k8sClient.Create(ctx, replica)
	require.NoError(t, err)

	created := v1alpha1.BranchingReplica{}
	err = k8sClient.Get(ctx, client.ObjectKeyFromObject(replica), &created)
	require.NoError(t, err)

	fn(t, &created)
}

// baseReplicaSpec returns a BranchingReplica spec with only the required
// fields set
func baseReplicaSpec() v1alpha1.BranchingReplicaSpec {
	return v1alpha1.BranchingReplicaSpec{
		Source: v1alpha1.SourceSpec{
			S3: v1alpha1.S3Spec{
				Prefix: "s3://test-bucket/archive",
				AccessKeyIDRef: corev1.SecretKeySelector{
					Name: "test-credentials",
					Key:  "AWS_ACCESS_KEY_ID",
				},
				AccessKeySecretRef: corev1.SecretKeySelector{
					Name: "test-credentials",
					Key:  "AWS_SECRET_ACCESS_KEY",
				},
			},
		},
		Postgres: v1alpha1.PostgresSpec{
			Image: testImage,
		},
		Storage: v1alpha1.StorageSpec{
			Size: "10Gi",
		},
	}
}

// getK8SObject retrieves a Kubernetes object by name in the test namespace
func getK8SObject(ctx context.Context, name string, obj client.Object) error {
	return envtestutil.GetObject(ctx, k8sClient, name, TestNamespace, obj)
}

// requireControlledBy requires that the controller owner reference of obj
// points at the given BranchingReplica
func requireControlledBy(t *testing.T, obj client.Object, replica *v1alpha1.BranchingReplica) {
	t.Helper()

	ref := metav1.GetControllerOf(obj)
	require.NotNil(t, ref)
	require.Equal(t, "BranchingReplica", ref.Kind)
	require.Equal(t, replica.Name, ref.Name)
	require.Equal(t, replica.UID, ref.UID)
}
