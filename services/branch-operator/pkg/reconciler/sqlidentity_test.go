package reconciler_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/shared"
)

// requireAppRole checks that the Cluster manages exactly one role: the login
// of id, in its privilege group, with its password from the app Secret.
func requireAppRole(t *testing.T, cluster *apiv1.Cluster, branchName string, id shared.SQLIdentity) {
	t.Helper()
	require.NotNil(t, cluster.Spec.Managed)
	require.Len(t, cluster.Spec.Managed.Roles, 1)
	role := cluster.Spec.Managed.Roles[0]
	require.Equal(t, id.Login, role.Name)
	require.Equal(t, []string{id.PrivilegeGroup}, role.InRoles)
	require.Equal(t, branchName+"-app", role.PasswordSecret.Name)
	require.True(t, role.Login)
	require.False(t, role.Superuser)
	require.True(t, role.CreateDB)
	require.True(t, role.CreateRole)
	require.True(t, role.BypassRLS)
	require.True(t, role.Replication)
}

func TestBranchSQLIdentity(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		profile v1alpha1.SQLIdentityProfile
		want    shared.SQLIdentity
	}{
		"legacy":      {profile: v1alpha1.SQLIdentityLegacy, want: shared.LegacySQLIdentity()},
		"white label": {profile: v1alpha1.SQLIdentityWhiteLabel, want: shared.WhiteLabelSQLIdentity()},
	}

	for name, tc := range tests {
		t.Run(name+" root", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			branch := NewBranchBuilder().Build()
			branch.Spec.SQLIdentity = tc.profile

			withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
				secret := corev1.Secret{}
				requireEventuallyNoErr(t, func() error {
					return getK8SObject(ctx, br.Name+"-app", &secret)
				})
				require.Equal(t, tc.want.Login, string(secret.Data[corev1.BasicAuthUsernameKey]))

				cluster := apiv1.Cluster{}
				requireEventuallyNoErr(t, func() error {
					return getK8SObject(ctx, br.ClusterName(), &cluster)
				})

				initdb := cluster.Spec.Bootstrap.InitDB
				require.NotNil(t, initdb)
				require.Equal(t, tc.want.Database, initdb.Database)
				require.Equal(t, tc.want.Login, initdb.Owner)
				require.Equal(t, br.Name+"-app", initdb.Secret.Name)
				require.Contains(t, initdb.PostInitSQL, "CREATE ROLE "+tc.want.PrivilegeGroup+" NOLOGIN;")
				require.Contains(t, initdb.PostInitSQL, "GRANT "+tc.want.PrivilegeGroup+" TO "+tc.want.Login+";")

				requireAppRole(t, &cluster, br.Name, tc.want)
			})
		})

		t.Run(name+" snapshot child", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			parent := NewBranchBuilder().Build()
			parent.Spec.SQLIdentity = tc.profile
			require.NoError(t, k8sClient.Create(ctx, &parent))
			parentCluster := apiv1.Cluster{}
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, parent.ClusterName(), &parentCluster)
			})
			setClusterStatus(ctx, t, &parentCluster, apiv1.ClusterStatus{
				Phase:          apiv1.PhaseHealthy,
				CurrentPrimary: parent.Name + "-1",
			})

			branch := NewBranchBuilder().
				WithRestore(v1alpha1.RestoreTypeVolumeSnapshot, parent.Name).
				Build()
			branch.Spec.SQLIdentity = tc.profile

			withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
				secret := corev1.Secret{}
				requireEventuallyNoErr(t, func() error {
					return getK8SObject(ctx, br.Name+"-app", &secret)
				})
				require.Equal(t, tc.want.Login, string(secret.Data[corev1.BasicAuthUsernameKey]))

				cluster := apiv1.Cluster{}
				requireEventuallyNoErr(t, func() error {
					return getK8SObject(ctx, br.ClusterName(), &cluster)
				})

				recovery := cluster.Spec.Bootstrap.Recovery
				require.NotNil(t, recovery)
				require.Nil(t, cluster.Spec.Bootstrap.InitDB)
				require.Equal(t, tc.want.Database, recovery.Database)
				require.Equal(t, tc.want.Login, recovery.Owner)
				require.Equal(t, br.Name+"-app", recovery.Secret.Name)

				requireAppRole(t, &cluster, br.Name, tc.want)
			})
		})
	}
}

func TestBranchSQLIdentityIsImmutable(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		from, to v1alpha1.SQLIdentityProfile
	}{
		"cannot be set":   {from: v1alpha1.SQLIdentityLegacy, to: v1alpha1.SQLIdentityWhiteLabel},
		"cannot be unset": {from: v1alpha1.SQLIdentityWhiteLabel, to: v1alpha1.SQLIdentityLegacy},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			branch := NewBranchBuilder().Build()
			branch.Spec.SQLIdentity = tc.from

			withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
				err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
					b.Spec.SQLIdentity = tc.to
				})
				require.Error(t, err)
				require.True(t, apierrors.IsInvalid(err), err)
			})
		})
	}

	t.Run("rejects an unknown profile", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().Build()
		branch.Spec.SQLIdentity = "whitelabel"

		err := k8sClient.Create(ctx, &branch)
		require.Error(t, err)
		require.True(t, apierrors.IsInvalid(err), err)
	})
}
