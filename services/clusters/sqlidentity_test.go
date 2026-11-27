package clusters

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	clustersv1 "xata/gen/proto/clusters/v1"
	"xata/services/branch-operator/api/v1alpha1"
)

func withSQLIdentityProfile(p v1alpha1.SQLIdentityProfile) testServiceOption {
	return func(c *testServiceConfig) {
		c.sqlIdentityProfile = string(p)
	}
}

func withParentSQLIdentity(p v1alpha1.SQLIdentityProfile) parentBranchOption {
	return func(b *v1alpha1.Branch) {
		b.Spec.SQLIdentity = p
	}
}

func appSecretUsername(ctx context.Context, t *testing.T, k8sClient client.Client, branchID string) string {
	t.Helper()
	secret := &corev1.Secret{}
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKey{Name: branchID + "-app", Namespace: "xata-clusters"}, secret))
	return string(secret.Data[corev1.BasicAuthUsernameKey])
}

func TestCreatePostgresClusterSQLIdentity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := map[string]struct {
		cellProfile   v1alpha1.SQLIdentityProfile
		parentProfile *v1alpha1.SQLIdentityProfile
		dataSource    string
		wantProfile   v1alpha1.SQLIdentityProfile
		wantUsername  string
	}{
		"legacy root without a cell profile": {
			wantProfile:  v1alpha1.SQLIdentityLegacy,
			wantUsername: "xata",
		},
		"root takes the cell profile": {
			cellProfile:  v1alpha1.SQLIdentityWhiteLabel,
			wantProfile:  v1alpha1.SQLIdentityWhiteLabel,
			wantUsername: "dbadmin",
		},
		"snapshot child inherits the parent profile": {
			cellProfile:   v1alpha1.SQLIdentityWhiteLabel,
			parentProfile: new(v1alpha1.SQLIdentityWhiteLabel),
			dataSource:    "snapshot",
			wantProfile:   v1alpha1.SQLIdentityWhiteLabel,
			wantUsername:  "dbadmin",
		},
		"snapshot child keeps the parent profile after the cell profile is removed": {
			cellProfile:   v1alpha1.SQLIdentityLegacy,
			parentProfile: new(v1alpha1.SQLIdentityWhiteLabel),
			dataSource:    "snapshot",
			wantProfile:   v1alpha1.SQLIdentityWhiteLabel,
			wantUsername:  "dbadmin",
		},
		"snapshot child of a legacy parent stays legacy in a white label cell": {
			cellProfile:   v1alpha1.SQLIdentityWhiteLabel,
			parentProfile: new(v1alpha1.SQLIdentityLegacy),
			dataSource:    "snapshot",
			wantProfile:   v1alpha1.SQLIdentityLegacy,
			wantUsername:  "xata",
		},
		"restore takes the source profile": {
			cellProfile:   v1alpha1.SQLIdentityLegacy,
			parentProfile: new(v1alpha1.SQLIdentityWhiteLabel),
			dataSource:    "continuous-backup",
			wantProfile:   v1alpha1.SQLIdentityWhiteLabel,
			wantUsername:  "dbadmin",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			opts := []testServiceOption{withSQLIdentityProfile(tc.cellProfile)}
			req, _, _, _, _ := exampleRequestsAndBranches()
			if tc.parentProfile != nil {
				parent := parentBranch(withParentSQLIdentity(*tc.parentProfile))
				opts = append(opts, withExistingObjects(parent))
				switch tc.dataSource {
				case "snapshot":
					req.ParentId = new(parent.Name)
					req.DataSource = &clustersv1.CreatePostgresClusterRequest_ClusterSnapshot{
						ClusterSnapshot: &clustersv1.ClusterSnapshot{ClusterId: parent.Name},
					}
				case "continuous-backup":
					req.DataSource = &clustersv1.CreatePostgresClusterRequest_ContinuousBackup{
						ContinuousBackup: &clustersv1.ContinuousBackup{ClusterId: parent.Name},
					}
				}
			}
			svc, k8sClient := setupTestClustersService(t, opts...)

			_, err := svc.CreatePostgresCluster(ctx, req)
			require.NoError(t, err)

			branch, err := getBranchFromK8s(ctx, k8sClient, req.GetId())
			require.NoError(t, err)
			require.Equal(t, tc.wantProfile, branch.Spec.SQLIdentity)
			require.Equal(t, tc.wantUsername, appSecretUsername(ctx, t, k8sClient, req.GetId()))
		})
	}
}

// An older Branch CRD removes the SQL identity. Strict validation makes the
// API server refuse such a Branch.
func TestCreatePostgresClusterSQLIdentityStrictValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := map[string]struct {
		cellProfile v1alpha1.SQLIdentityProfile
		want        string
	}{
		"legacy root":      {cellProfile: v1alpha1.SQLIdentityLegacy, want: ""},
		"white label root": {cellProfile: v1alpha1.SQLIdentityWhiteLabel, want: metav1.FieldValidationStrict},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var got client.CreateOptions
			svc, _ := setupTestClustersService(t,
				withSQLIdentityProfile(tc.cellProfile),
				withInterceptorFuncs(interceptor.Funcs{
					Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
						if _, ok := obj.(*v1alpha1.Branch); ok {
							got.ApplyOptions(opts)
						}
						return c.Create(ctx, obj, opts...)
					},
				}))
			req, _, _, _, _ := exampleRequestsAndBranches()

			_, err := svc.CreatePostgresCluster(ctx, req)
			require.NoError(t, err)
			require.Equal(t, tc.want, got.FieldValidation)
		})
	}
}

// A Branch takes a cluster only from a create pool that declares its SQL
// identity. The legacy rows are the behaviour without SQL identity profiles.
func TestCreatePostgresClusterSQLIdentityCreatePool(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := map[string]struct {
		cellProfile  v1alpha1.SQLIdentityProfile
		poolProfile  v1alpha1.SQLIdentityProfile
		wantPool     bool
		wantUsername string
	}{
		"legacy branch, legacy pool":           {wantPool: true, wantUsername: "xata"},
		"legacy branch, white label pool":      {poolProfile: v1alpha1.SQLIdentityWhiteLabel, wantUsername: "xata"},
		"white label branch, legacy pool":      {cellProfile: v1alpha1.SQLIdentityWhiteLabel, wantUsername: "dbadmin"},
		"white label branch, white label pool": {cellProfile: v1alpha1.SQLIdentityWhiteLabel, poolProfile: v1alpha1.SQLIdentityWhiteLabel, wantPool: true, wantUsername: "dbadmin"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			pool := poolForTest("default-storage-class", testImage, "2", "3996Mi")
			pool.Spec.SQLIdentity = string(tc.poolProfile)
			if tc.poolProfile == v1alpha1.SQLIdentityWhiteLabel {
				pool.Spec.ClusterSpec.Bootstrap = &apiv1.BootstrapConfiguration{
					InitDB: &apiv1.BootstrapInitDB{Database: "defaultdb", Owner: "dbadmin"},
				}
			}
			svc, k8sClient := setupTestClustersService(t,
				withSQLIdentityProfile(tc.cellProfile),
				withExistingObjects(pool, poolClusterForTest(), poolClusterAppSecretForTest("pool-password")))
			req, _, _, _, _ := exampleRequestsAndBranches()

			_, err := svc.CreatePostgresCluster(ctx, req)
			require.NoError(t, err)

			branch, err := getBranchFromK8s(ctx, k8sClient, req.GetId())
			require.NoError(t, err)
			require.Equal(t, tc.cellProfile, branch.Spec.SQLIdentity)

			secret := &corev1.Secret{}
			require.NoError(t, k8sClient.Get(ctx, client.ObjectKey{Name: req.GetId() + "-app", Namespace: "xata-clusters"}, secret))
			require.Equal(t, tc.wantUsername, string(secret.Data[corev1.BasicAuthUsernameKey]))

			cluster := &apiv1.Cluster{}
			require.NoError(t, k8sClient.Get(ctx, client.ObjectKey{Name: "pool-cluster-1", Namespace: "xata-clusters"}, cluster))

			if !tc.wantPool {
				// The Branch gets a cold cluster and the pool keeps its own
				require.Equal(t, new(req.GetId()), branch.Spec.ClusterSpec.Name)
				require.False(t, branch.HasWakeupPoolAnnotation())
				require.NotEqual(t, "pool-password", string(secret.Data[corev1.BasicAuthPasswordKey]))
				require.Len(t, cluster.OwnerReferences, 1)
				return
			}
			require.Equal(t, new("pool-cluster-1"), branch.Spec.ClusterSpec.Name)
			require.Equal(t, "test-pool-slot", branch.WakeupPoolName())
			require.Equal(t, "pool-password", string(secret.Data[corev1.BasicAuthPasswordKey]))
			require.Empty(t, cluster.OwnerReferences)
		})
	}
}

func TestUpdatePostgresClusterKeepsSQLIdentity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for _, cellProfile := range []v1alpha1.SQLIdentityProfile{v1alpha1.SQLIdentityLegacy, v1alpha1.SQLIdentityWhiteLabel} {
		for _, stored := range []v1alpha1.SQLIdentityProfile{v1alpha1.SQLIdentityLegacy, v1alpha1.SQLIdentityWhiteLabel} {
			t.Run("cell "+profileName(cellProfile)+"/stored "+profileName(stored), func(t *testing.T) {
				t.Parallel()

				existing := parentBranch(withParentSQLIdentity(stored))
				svc, k8sClient := setupTestClustersService(t,
					withSQLIdentityProfile(cellProfile),
					withExistingObjects(existing))

				_, err := svc.UpdatePostgresCluster(ctx, &clustersv1.UpdatePostgresClusterRequest{
					Id:                  existing.Name,
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{Hibernate: new(true)},
				})
				require.NoError(t, err)

				branch, err := getBranchFromK8s(ctx, k8sClient, existing.Name)
				require.NoError(t, err)
				require.Equal(t, stored, branch.Spec.SQLIdentity)
			})
		}
	}
}

func profileName(p v1alpha1.SQLIdentityProfile) string {
	if p == v1alpha1.SQLIdentityLegacy {
		return "legacy"
	}
	return string(p)
}
