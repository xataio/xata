package wakeup_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	poolv1alpha1 "xata/proto/clusterpool-operator/api/v1alpha1"
	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/shared"
)

func TestWakeupWhiteLabelBranch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	poolName := "pool-" + randomString(10)
	clusterName := "cluster-" + randomString(10)
	branchName := "branch-" + randomString(10)
	wrName := "wur-" + randomString(10)
	id := shared.WhiteLabelSQLIdentity

	pool := &poolv1alpha1.ClusterPool{
		Name:      poolName,
		Namespace: TestNamespace,
		Spec: poolv1alpha1.ClusterPoolSpec{
			Clusters:    1,
			ClusterSpec: apiv1.ClusterSpec{Instances: 1},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, pool))

	cluster, err := setupPoolCluster(ctx, pool, clusterName, TestNamespace, 1)
	require.NoError(t, err)

	branch := &v1alpha1.Branch{
		Name: branchName,
		Annotations: map[string]string{
			v1alpha1.WakeupPoolAnnotation:     poolName,
			v1alpha1.AwaitingWakeupAnnotation: "true",
		},
		Spec: v1alpha1.BranchSpec{
			SQLIdentity: v1alpha1.SQLIdentityWhiteLabel,
			ClusterSpec: v1alpha1.ClusterSpec{
				Instances: 1,
				Storage:   v1alpha1.StorageSpec{Size: "1Gi"},
				Image:     "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.7",
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, branch))
	require.NoError(t, createXVol(ctx, "xvol-"+branchName, "Available"))

	// Report the login role as synced, as CNPG would
	secret, err := createAppSecret(ctx, branch, cluster.Namespace)
	require.NoError(t, err)
	cluster.Status.ManagedRolesStatus.PasswordStatus = map[string]apiv1.PasswordState{
		id.Login: {SecretResourceVersion: secret.ResourceVersion},
	}
	require.NoError(t, k8sClient.Status().Update(ctx, cluster))

	wr, err := createWakeupRequest(ctx, wrName, branch.Name)
	require.NoError(t, err)
	requireWakeupSucceededCondition(t, ctx, wr, metav1.ConditionTrue, v1alpha1.WakeupSucceededReason)

	// The Cluster manages only the white label login, in its privilege group
	c := &apiv1.Cluster{}
	requireEventuallyTrue(t, func() bool {
		return k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), c) == nil &&
			c.Spec.Managed != nil && len(c.Spec.Managed.Roles) > 0
	})
	require.Len(t, c.Spec.Managed.Roles, 1)
	require.Equal(t, id.Login, c.Spec.Managed.Roles[0].Name)
	require.Equal(t, []string{id.PrivilegeGroup}, c.Spec.Managed.Roles[0].InRoles)
	require.Equal(t, branchName+"-app", c.Spec.Managed.Roles[0].PasswordSecret.Name)
	require.False(t, c.Spec.Managed.Roles[0].Superuser)
}
