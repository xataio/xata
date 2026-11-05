package reconciler_test

import (
	"context"
	"testing"

	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/reconciler"
	"xata/services/branch-operator/pkg/reconciler/resources"

	"github.com/stretchr/testify/require"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestScaleToZeroRBACReconciliation(t *testing.T) {
	t.Parallel()

	t.Run("cluster-scoped RBAC is created on branch creation", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			requireEventuallyNoErr(t, func() error {
				return getScaleToZeroRBAC(ctx, br, &rbacv1.ClusterRole{})
			})
			requireEventuallyNoErr(t, func() error {
				return getScaleToZeroRBAC(ctx, br, &rbacv1.ClusterRoleBinding{})
			})
		})
	})

	t.Run("cluster-scoped RBAC is owned by the Branch", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			objs := []client.Object{
				&rbacv1.ClusterRole{},
				&rbacv1.ClusterRoleBinding{},
			}

			for _, obj := range objs {
				requireEventuallyNoErr(t, func() error {
					return getScaleToZeroRBAC(ctx, br, obj)
				})
				require.Len(t, obj.GetOwnerReferences(), 1, "%T", obj)
				require.Equal(t, br.Name, obj.GetOwnerReferences()[0].Name, "%T", obj)
			}
		})
	})

	t.Run("the ClusterRole grants branch access and binds to the cluster ServiceAccount", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		// A cluster name that differs from the branch name, as after a pool claim
		branch := NewBranchBuilder().WithClusterName(new(randomString(10))).Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			clusterRole := rbacv1.ClusterRole{}
			clusterRoleBinding := rbacv1.ClusterRoleBinding{}

			requireEventuallyNoErr(t, func() error {
				return getScaleToZeroRBAC(ctx, br, &clusterRole)
			})
			requireEventuallyNoErr(t, func() error {
				return getScaleToZeroRBAC(ctx, br, &clusterRoleBinding)
			})

			require.Equal(t, resources.ScaleToZeroClusterRoleRules(br.Name), clusterRole.Rules)

			require.Equal(t, rbacv1.RoleRef{
				APIGroup: rbacv1.GroupName,
				Kind:     "ClusterRole",
				Name:     scaleToZeroRBACName(br),
			}, clusterRoleBinding.RoleRef)
			require.Equal(t, []rbacv1.Subject{{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      br.ClusterName(),
				Namespace: XataClustersNamespace,
			}}, clusterRoleBinding.Subjects)
		})
	})

	t.Run("the binding follows the cluster name when it changes", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			clusterRole := rbacv1.ClusterRole{}
			clusterRoleBinding := rbacv1.ClusterRoleBinding{}

			requireEventuallyNoErr(t, func() error {
				return getScaleToZeroRBAC(ctx, br, &clusterRoleBinding)
			})

			newClusterName := randomString(10)
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Name = new(newClusterName)
			})
			require.NoError(t, err)

			requireEventuallyTrue(t, func() bool {
				if err := getScaleToZeroRBAC(ctx, br, &clusterRoleBinding); err != nil {
					return false
				}
				return clusterRoleBinding.Subjects[0].Name == newClusterName
			})

			// The ClusterRole stays scoped to the Branch, which did not change
			require.NoError(t, getScaleToZeroRBAC(ctx, br, &clusterRole))
			require.Equal(t, resources.ScaleToZeroClusterRoleRules(br.Name), clusterRole.Rules)
		})
	})

	t.Run("cluster-scoped RBAC is deleted when cluster name is unset", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			objs := []client.Object{
				&rbacv1.ClusterRole{},
				&rbacv1.ClusterRoleBinding{},
			}

			for _, obj := range objs {
				requireEventuallyNoErr(t, func() error {
					return getScaleToZeroRBAC(ctx, br, obj)
				})
			}

			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Name = nil
			})
			require.NoError(t, err)

			for _, obj := range objs {
				requireEventuallyTrue(t, func() bool {
					return apierrors.IsNotFound(getScaleToZeroRBAC(ctx, br, obj))
				})
			}
		})
	})
}

// scaleToZeroRBACName returns the name of the Branch's scale-to-zero RBAC
// resources
func scaleToZeroRBACName(br *v1alpha1.Branch) string {
	return br.Name + reconciler.ScaleToZeroRBACSuffix
}

// getScaleToZeroRBAC retrieves one of the Branch's scale-to-zero RBAC resources
func getScaleToZeroRBAC(ctx context.Context, br *v1alpha1.Branch, obj client.Object) error {
	return getK8SObject(ctx, scaleToZeroRBACName(br), obj)
}
