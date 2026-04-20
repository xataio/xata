package reconciler_test

import (
	"context"
	"testing"

	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/reconciler"

	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/utils/ptr"
)

func TestClusterReconciliation(t *testing.T) {
	t.Parallel()

	t.Run("cluster is created on branch creation", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			// Expect the Cluster to be created
			requireEventuallyNoErr(t, func() error {
				cluster := apiv1.Cluster{}
				return getK8SObject(ctx, br.Name, &cluster)
			})
		})
	})

	t.Run("cluster is owned by the Branch", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			cluster := apiv1.Cluster{}

			// Expect the Cluster to be created with the correct owner reference
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name, &cluster)
			})
			require.Len(t, cluster.GetOwnerReferences(), 1)
			require.Equal(t, br.Name, cluster.GetOwnerReferences()[0].Name)
		})
	})

	t.Run("cluster is updated when Branch spec is updated", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			cluster := apiv1.Cluster{}

			// Expect the Cluster to be created
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name, &cluster)
			})

			// Expect the Cluster to have 1 instance
			require.Equal(t, 1, cluster.Spec.Instances)

			// Update the Branch's instance count
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Instances = 3
			})
			require.NoError(t, err)

			// Expect the Cluster to be updated with the new instance count
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, br.Name, &cluster)
				if err != nil {
					return false
				}
				return cluster.Spec.Instances == 3
			})
		})
	})

	t.Run("cluster direct changes are reverted", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			cluster := apiv1.Cluster{}

			// Expect the Cluster to be created
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name, &cluster)
			})

			// Directly modify the Cluster's instance count
			err := retryOnConflict(ctx, &cluster, func(c *apiv1.Cluster) {
				c.Spec.Instances = 5
			})
			require.NoError(t, err)

			// Expect the Cluster spec to be reverted to the correct instance count
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, br.Name, &cluster)
				if err != nil {
					return false
				}
				return cluster.Spec.Instances == 1
			})
		})
	})

	t.Run("cluster scale to zero annotations are reconciled", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithScaleToZeroInactivityPeriod(10).
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			cluster := apiv1.Cluster{}

			// Expect the Cluster to be created
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name, &cluster)
			})

			// Expect the scale to zero annotations to be set on the Cluster
			require.Equal(t, "true", cluster.Annotations[reconciler.ScaleToZeroEnabledAnnotation])
			require.Equal(t, "10", cluster.Annotations[reconciler.ScaleToZeroInactivityAnnotation])

			// Disable scale to zero on the Branch
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.ScaleToZero.Enabled = false
			})
			require.NoError(t, err)

			// Expect the scale to zero annotations to be removed from the Cluster
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, br.Name, &cluster)
				if err != nil {
					return false
				}
				enabled := cluster.Annotations[reconciler.ScaleToZeroEnabledAnnotation]
				_, inactivityExists := cluster.Annotations[reconciler.ScaleToZeroInactivityAnnotation]

				return enabled == "false" && !inactivityExists
			})

			// InactivityPeriodMinutes is preserved on the branch spec after disabling
			err = getK8SObject(ctx, br.Name, br)
			require.NoError(t, err)
			require.Equal(t, int32(10), br.Spec.ClusterSpec.ScaleToZero.InactivityPeriodMinutes)
		})
	})

	t.Run("cluster hibernation annotation is reconciled", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithHibernationMode(v1alpha1.HibernationModeDisabled).
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			cluster := apiv1.Cluster{}

			// Expect the Cluster to be created
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name, &cluster)
			})

			// Expect the hibernation annotation to be set on the Cluster
			require.Equal(t, "off", cluster.Annotations[reconciler.HibernationAnnotation])

			// Enable hibernation on the Branch
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Hibernation = ptr.To(v1alpha1.HibernationModeEnabled)
			})
			require.NoError(t, err)

			// Expect the hibernation annotation to be updated on the Cluster
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, br.Name, &cluster)
				if err != nil {
					return false
				}
				return cluster.Annotations[reconciler.HibernationAnnotation] == "on"
			})
		})
	})

	t.Run("wal archiving annotation is reconciled", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithWALArchiving(true).
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			cluster := apiv1.Cluster{}

			// Expect the Cluster to be created
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name, &cluster)
			})

			// WAL archiving is enabled, so the skip annotation should be "disabled"
			require.Equal(t, "disabled", cluster.Annotations[reconciler.SkipWALArchivingAnnotation])

			// Disable WAL archiving on the Branch
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.BackupSpec.WALArchiving = v1alpha1.WALArchivingModeDisabled
			})
			require.NoError(t, err)

			// Expect the WAL archiving annotation to be set on the Cluster
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, br.Name, &cluster)
				if err != nil {
					return false
				}
				return cluster.Annotations[reconciler.SkipWALArchivingAnnotation] == "enabled"
			})
		})
	})

	t.Run("cluster has branch name annotation", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			cluster := apiv1.Cluster{}

			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name, &cluster)
			})
			require.Equal(t, br.Name, cluster.Annotations[reconciler.BranchAnnotation])
		})
	})

	t.Run("cluster has pod patch annotation", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			cluster := apiv1.Cluster{}

			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name, &cluster)
			})
			require.Equal(t,
				`[{"op": "add", "path": "/spec/enableServiceLinks", "value": false}]`,
				cluster.Annotations[reconciler.PodPatchAnnotation],
			)
		})
	})

	t.Run("old cluster is deleted when cluster name changes", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			oldClusterName := br.Name

			// Expect the Cluster to be created
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, oldClusterName, &apiv1.Cluster{})
			})

			// Change the cluster name on the Branch
			newClusterName := randomString(10)
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Name = new(newClusterName)
			})
			require.NoError(t, err)

			// Expect a new Cluster with the new name to be created
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, newClusterName, &apiv1.Cluster{})
			})

			// Expect the old Cluster to be deleted
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, oldClusterName, &apiv1.Cluster{})
				return k8serrors.IsNotFound(err)
			})
		})
	})

	t.Run("all owned clusters are deleted when cluster name is unset", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			oldClusterName := br.Name

			// Expect the Cluster to be created
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, oldClusterName, &apiv1.Cluster{})
			})

			// Unset the cluster name on the Branch
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Name = nil
			})
			require.NoError(t, err)

			// Expect the old Cluster to be deleted
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, oldClusterName, &apiv1.Cluster{})
				return k8serrors.IsNotFound(err)
			})
		})
	})
}
