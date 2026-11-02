package reconciler_test

import (
	"context"
	"testing"

	"xata/services/branch-operator/api/v1alpha1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"

	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestVolumeSnapshotReconciliation(t *testing.T) {
	t.Parallel()

	t.Run("volumesnapshot is created when parent is in a snapshotable phase", func(t *testing.T) {
		t.Parallel()

		for _, phase := range []string{
			apiv1.PhaseHealthy,
			apiv1.PhaseUpgradeDelayed,
			apiv1.PhaseFailurePlugin,
		} {
			t.Run(phase, func(t *testing.T) {
				t.Parallel()
				ctx := context.Background()

				// Create the parent Branch with a random Cluster name to ensure that
				// no assumptions are made about the parent Cluster having the same
				// name as the parent Branch.
				parentBranch := NewBranchBuilder().
					WithClusterName(new(randomString(10))).
					Build()

				withBranch(ctx, t, parentBranch, func(t *testing.T, parentBr *v1alpha1.Branch) {
					// Expect the parent Cluster to be created
					parentCluster := apiv1.Cluster{}
					requireEventuallyNoErr(t, func() error {
						return getK8SObject(ctx, parentBr.ClusterName(), &parentCluster)
					})

					// Move the parent cluster to a snapshotable state with a designated primary
					setClusterStatus(ctx, t, &parentCluster, apiv1.ClusterStatus{
						Phase:          phase,
						CurrentPrimary: parentBr.Name + "-1",
					})

					// Expect the parent Cluster to be in the expected phase
					requireEventuallyTrue(t, func() bool {
						getK8SObject(ctx, parentBr.Name, &parentCluster)

						return parentCluster.Status.Phase == phase
					})

					// Create a child Branch that restores via VolumeSnapshot from the
					// parent. Give the child Branch a random Cluster name to ensure that
					// no assumptions are made about the child Cluster having the same
					// name as the child Branch.
					childClusterName := randomString(10)
					childBranch := NewBranchBuilder().
						WithClusterName(new(childClusterName)).
						WithRestore(v1alpha1.RestoreTypeVolumeSnapshot, parentBranch.Name).
						Build()

					withBranch(ctx, t, childBranch, func(t *testing.T, childBr *v1alpha1.Branch) {
						// Expect the VolumeSnapshot to be created
						requireEventuallyNoErr(t, func() error {
							vs := snapshotv1.VolumeSnapshot{}
							vsName := VolumeSnapshotName(parentBr.Name, childBranch.Name)

							return getK8SObject(ctx, vsName, &vs)
						})
					})
				})
			})
		}
	})

	t.Run("volumesnapshot is created using dangling PVC when no primary exists", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		parentBranch := NewBranchBuilder().Build()

		withBranch(ctx, t, parentBranch, func(t *testing.T, parentBr *v1alpha1.Branch) {
			// Expect the parent Cluster to be created
			parentCluster := apiv1.Cluster{}
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, parentBr.Name, &parentCluster)
			})

			// Move the parent cluster to a Healthy state with no designated primary
			// but a dangling PVC
			setClusterStatus(ctx, t, &parentCluster, apiv1.ClusterStatus{
				Phase:       apiv1.PhaseHealthy,
				DanglingPVC: []string{"dangling-pvc"},
			})

			// Expect the parent Cluster to be Healthy
			requireEventuallyTrue(t, func() bool {
				getK8SObject(ctx, parentBr.Name, &parentCluster)

				return parentCluster.Status.Phase == apiv1.PhaseHealthy
			})

			// Create a child Branch that restores via VolumeSnapshot from the parent
			childBranch := NewBranchBuilder().
				WithRestore(v1alpha1.RestoreTypeVolumeSnapshot, parentCluster.Name).
				Build()

			withBranch(ctx, t, childBranch, func(t *testing.T, childBr *v1alpha1.Branch) {
				// Expect the VolumeSnapshot to be created
				requireEventuallyNoErr(t, func() error {
					vs := snapshotv1.VolumeSnapshot{}
					vsName := VolumeSnapshotName(parentBr.Name, childBr.Name)

					return getK8SObject(ctx, vsName, &vs)
				})
			})
		})
	})

	t.Run("volumesnapshot is removed when child cluster is healthy", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		parentBranch := NewBranchBuilder().Build()

		withBranch(ctx, t, parentBranch, func(t *testing.T, parentBr *v1alpha1.Branch) {
			// Expect the parent Cluster to be created
			parentCluster := apiv1.Cluster{}
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, parentBr.Name, &parentCluster)
			})

			// Move the parent cluster to a Healthy state with a designated primary
			setClusterStatus(ctx, t, &parentCluster, apiv1.ClusterStatus{
				Phase:          apiv1.PhaseHealthy,
				CurrentPrimary: parentBr.Name + "-1",
			})

			// Expect the parent Cluster to be Healthy
			requireEventuallyTrue(t, func() bool {
				getK8SObject(ctx, parentBr.Name, &parentCluster)

				return parentCluster.Status.Phase == apiv1.PhaseHealthy
			})

			// Create a child Branch that restores via VolumeSnapshot from the parent
			childBranch := NewBranchBuilder().
				WithRestore(v1alpha1.RestoreTypeVolumeSnapshot, parentCluster.Name).
				Build()

			withBranch(ctx, t, childBranch, func(t *testing.T, childBr *v1alpha1.Branch) {
				// Expect the VolumeSnapshot to be created
				requireEventuallyNoErr(t, func() error {
					vs := snapshotv1.VolumeSnapshot{}
					vsName := VolumeSnapshotName(parentBr.Name, childBr.Name)

					return getK8SObject(ctx, vsName, &vs)
				})

				// Expect the child Cluster to be created
				childCluster := apiv1.Cluster{}
				requireEventuallyNoErr(t, func() error {
					return getK8SObject(ctx, childBr.Name, &childCluster)
				})

				// Move the child cluster to a Healthy state
				setClusterStatus(ctx, t, &childCluster, apiv1.ClusterStatus{
					Phase: apiv1.PhaseHealthy,
				})

				// Expect the VolumeSnapshot to be deleted
				requireEventuallyTrue(t, func() bool {
					vs := snapshotv1.VolumeSnapshot{}
					vsName := VolumeSnapshotName(parentBr.Name, childBr.Name)

					err := getK8SObject(ctx, vsName, &vs)
					return apierrors.IsNotFound(err)
				})
			})
		})
	})

	t.Run("volumesnapshot is not created when child cluster has no cluster name", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		parentBranch := NewBranchBuilder().Build()

		withBranch(ctx, t, parentBranch, func(t *testing.T, parentBr *v1alpha1.Branch) {
			// Expect the parent Cluster to be created
			parentCluster := apiv1.Cluster{}
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, parentBr.Name, &parentCluster)
			})

			// Move the parent cluster to a Healthy state with a designated primary
			setClusterStatus(ctx, t, &parentCluster, apiv1.ClusterStatus{
				Phase:          apiv1.PhaseHealthy,
				CurrentPrimary: parentBr.Name + "-1",
			})

			// Expect the parent Cluster to be Healthy
			requireEventuallyTrue(t, func() bool {
				getK8SObject(ctx, parentBr.Name, &parentCluster)

				return parentCluster.Status.Phase == apiv1.PhaseHealthy
			})

			// Create a child Branch that restores via VolumeSnapshot from the parent
			// but has no cluster name
			childBranch := NewBranchBuilder().
				WithClusterName(nil).
				WithRestore(v1alpha1.RestoreTypeVolumeSnapshot, parentCluster.Name).
				Build()

			withBranch(ctx, t, childBranch, func(t *testing.T, childBr *v1alpha1.Branch) {
				// Wait for reconciliation to complete by checking the Branch's
				// observed generation
				requireEventuallyTrue(t, func() bool {
					branch := v1alpha1.Branch{}

					err := k8sClient.Get(ctx, client.ObjectKey{Name: childBranch.Name}, &branch)
					if err != nil {
						return false
					}
					return branch.Status.ObservedGeneration == branch.Generation
				})

				// Expect no VolumeSnapshot to be created
				vs := snapshotv1.VolumeSnapshot{}
				vsName := VolumeSnapshotName(parentBr.Name, "")

				err := getK8SObject(ctx, vsName, &vs)
				require.True(t, apierrors.IsNotFound(err))
			})
		})
	})

	t.Run("volumesnapshot reconciliation fails when parent is unhealthy", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		parentBranch := NewBranchBuilder().Build()

		withBranch(ctx, t, parentBranch, func(t *testing.T, parentBr *v1alpha1.Branch) {
			// Expect the parent Cluster to be created
			parentCluster := apiv1.Cluster{}
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, parentBr.Name, &parentCluster)
			})

			// Parent remains in an unhealthy state

			// Create a child Branch that restores via VolumeSnapshot from the parent
			childBranch := NewBranchBuilder().
				WithRestore(v1alpha1.RestoreTypeVolumeSnapshot, parentCluster.Name).
				Build()

			withBranch(ctx, t, childBranch, func(t *testing.T, childBr *v1alpha1.Branch) {
				// Expect the branch condition to reflect the unhealthy parent
				requireEventuallyTrue(t, func() bool {
					br := v1alpha1.Branch{}
					err := getK8SObject(ctx, childBr.Name, &br)
					if err != nil {
						return false
					}
					return isReadyConditionFalseWithReason(br, v1alpha1.ParentClusterUnhealthyReason)
				})
			})
		})
	})

	t.Run("volumesnapshot reconciliation fails when parent branch has no cluster", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		// Define a parent Branch that has no Cluster associated with it
		parentBranch := NewBranchBuilder().
			WithClusterName(nil).
			Build()

		withBranch(ctx, t, parentBranch, func(t *testing.T, parentBr *v1alpha1.Branch) {
			// Create a child Branch that restores via VolumeSnapshot from the parent
			childBranch := NewBranchBuilder().
				WithRestore(v1alpha1.RestoreTypeVolumeSnapshot, parentBranch.Name).
				Build()

			withBranch(ctx, t, childBranch, func(t *testing.T, childBr *v1alpha1.Branch) {
				// Expect the branch condition to reflect the unhealthy parent
				requireEventuallyTrue(t, func() bool {
					br := v1alpha1.Branch{}
					err := getK8SObject(ctx, childBr.Name, &br)
					if err != nil {
						return false
					}
					return isReadyConditionFalseWithReason(br, v1alpha1.ParentBranchHasNoClusterReason)
				})
			})
		})
	})

	t.Run("volumesnapshot reconciliation fails when parent is missing", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		// Create a child Branch that restores via VolumeSnapshot from the parent
		childBranch := NewBranchBuilder().
			WithRestore(v1alpha1.RestoreTypeVolumeSnapshot, "non-existent-parent-cluster").
			Build()

		withBranch(ctx, t, childBranch, func(t *testing.T, childBr *v1alpha1.Branch) {
			// Expect the branch condition to reflect the missing parent
			requireEventuallyTrue(t, func() bool {
				br := v1alpha1.Branch{}
				err := getK8SObject(ctx, childBr.Name, &br)
				if err != nil {
					return false
				}
				return isReadyConditionFalseWithReason(br, v1alpha1.ParentClusterNotFoundReason)
			})
		})
	})

	t.Run("volumesnapshot reconciliation fails when parent has no PVC", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		parentBranch := NewBranchBuilder().Build()

		withBranch(ctx, t, parentBranch, func(t *testing.T, parentBr *v1alpha1.Branch) {
			// Expect the parent Cluster to be created
			parentCluster := apiv1.Cluster{}
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, parentBr.Name, &parentCluster)
			})

			// Move the parent cluster to a Healthy state but with no primary (hence
			// no PVC)
			setClusterStatus(ctx, t, &parentCluster, apiv1.ClusterStatus{
				Phase: apiv1.PhaseHealthy,
			})

			// Expect the parent Cluster to be Healthy
			requireEventuallyTrue(t, func() bool {
				getK8SObject(ctx, parentBr.Name, &parentCluster)

				return parentCluster.Status.Phase == apiv1.PhaseHealthy
			})

			// Create a child Branch that restores via VolumeSnapshot from the parent
			childBranch := NewBranchBuilder().
				WithRestore(v1alpha1.RestoreTypeVolumeSnapshot, parentCluster.Name).
				Build()

			withBranch(ctx, t, childBranch, func(t *testing.T, childBr *v1alpha1.Branch) {
				// Expect the VolumeSnapshot to be created
				requireEventuallyTrue(t, func() bool {
					br := v1alpha1.Branch{}
					err := getK8SObject(ctx, childBr.Name, &br)
					if err != nil {
						return false
					}
					return isReadyConditionFalseWithReason(br, v1alpha1.ParentClusterPVCNotFoundReason)
				})
			})
		})
	})
}

// isConditionFalseWithReason checks if the specified condition on the Branch
// is set to False with the given reason.
func isReadyConditionFalseWithReason(br v1alpha1.Branch, reason string) bool {
	c := meta.FindStatusCondition(br.Status.Conditions, v1alpha1.BranchReadyConditionType)
	if c == nil {
		return false
	}
	return c.Status == metav1.ConditionFalse && c.Reason == reason
}

// setClusterStatus sets the status of the given Cluster to the provided status.
// In the `envtest` environment there is no CNPG controller to reconcile
// Clusters, so status updates must be done manually.
func setClusterStatus(ctx context.Context, t *testing.T, c *apiv1.Cluster, s apiv1.ClusterStatus) {
	t.Helper()

	err := retryStatusOnConflict(ctx, c, func(c *apiv1.Cluster) {
		c.Status = s
	})
	require.NoError(t, err)
}

// VolumeSnapshotName returns the expected name of the VolumeSnapshot for the
// given parent and child branch names.
func VolumeSnapshotName(parentBranchName, childBranchName string) string {
	return parentBranchName + "-" + childBranchName
}
