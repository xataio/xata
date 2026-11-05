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

	t.Run("RBAC resources are created on branch creation", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			// Expect the Role, RoleBinding, ClusterRole and ClusterRoleBinding
			// to be created
			requireEventuallyNoErr(t, func() error {
				return getScaleToZeroRBAC(ctx, br, &rbacv1.Role{})
			})
			requireEventuallyNoErr(t, func() error {
				return getScaleToZeroRBAC(ctx, br, &rbacv1.RoleBinding{})
			})
			requireEventuallyNoErr(t, func() error {
				return getScaleToZeroRBAC(ctx, br, &rbacv1.ClusterRole{})
			})
			requireEventuallyNoErr(t, func() error {
				return getScaleToZeroRBAC(ctx, br, &rbacv1.ClusterRoleBinding{})
			})
		})
	})

	t.Run("RBAC resources are owned by the Branch", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			objs := []client.Object{
				&rbacv1.Role{},
				&rbacv1.RoleBinding{},
				&rbacv1.ClusterRole{},
				&rbacv1.ClusterRoleBinding{},
			}

			// Expect each resource to be created with the Branch as its owner
			for _, obj := range objs {
				requireEventuallyNoErr(t, func() error {
					return getScaleToZeroRBAC(ctx, br, obj)
				})
				require.Len(t, obj.GetOwnerReferences(), 1, "%T", obj)
				require.Equal(t, br.Name, obj.GetOwnerReferences()[0].Name, "%T", obj)
			}
		})
	})

	t.Run("roles grant access to the branch's resources and bind to its cluster ServiceAccount", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			role := rbacv1.Role{}
			roleBinding := rbacv1.RoleBinding{}
			clusterRole := rbacv1.ClusterRole{}
			clusterRoleBinding := rbacv1.ClusterRoleBinding{}

			requireEventuallyNoErr(t, func() error {
				return getScaleToZeroRBAC(ctx, br, &role)
			})
			requireEventuallyNoErr(t, func() error {
				return getScaleToZeroRBAC(ctx, br, &roleBinding)
			})
			requireEventuallyNoErr(t, func() error {
				return getScaleToZeroRBAC(ctx, br, &clusterRole)
			})
			requireEventuallyNoErr(t, func() error {
				return getScaleToZeroRBAC(ctx, br, &clusterRoleBinding)
			})

			// Expect the roles to be built from the Branch's cluster name and
			// the Branch name
			require.Equal(t, resources.ScaleToZeroRoleRules(br.ClusterName(), br.Name), role.Rules)
			require.Equal(t, resources.ScaleToZeroClusterRoleRules(br.Name), clusterRole.Rules)

			// Expect both bindings to grant their role to the ServiceAccount of
			// the Branch's cluster
			subjects := []rbacv1.Subject{{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      br.ClusterName(),
				Namespace: XataClustersNamespace,
			}}

			require.Equal(t, rbacv1.RoleRef{
				APIGroup: rbacv1.GroupName,
				Kind:     "Role",
				Name:     scaleToZeroRBACName(br),
			}, roleBinding.RoleRef)
			require.Equal(t, subjects, roleBinding.Subjects)

			require.Equal(t, rbacv1.RoleRef{
				APIGroup: rbacv1.GroupName,
				Kind:     "ClusterRole",
				Name:     scaleToZeroRBACName(br),
			}, clusterRoleBinding.RoleRef)
			require.Equal(t, subjects, clusterRoleBinding.Subjects)
		})
	})

	t.Run("RBAC resources follow the cluster name when it changes", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			role := rbacv1.Role{}
			roleBinding := rbacv1.RoleBinding{}
			clusterRoleBinding := rbacv1.ClusterRoleBinding{}

			// Expect the Role to be created
			requireEventuallyNoErr(t, func() error {
				return getScaleToZeroRBAC(ctx, br, &role)
			})

			// Change the cluster name on the Branch
			newClusterName := randomString(10)
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Name = new(newClusterName)
			})
			require.NoError(t, err)

			// Expect the Role to grant access to the new Cluster
			requireEventuallyTrue(t, func() bool {
				err := getScaleToZeroRBAC(ctx, br, &role)
				if err != nil {
					return false
				}
				return role.Rules[0].ResourceNames[0] == newClusterName
			})

			// Expect both bindings to bind to the new Cluster's ServiceAccount
			requireEventuallyTrue(t, func() bool {
				err := getScaleToZeroRBAC(ctx, br, &roleBinding)
				if err != nil {
					return false
				}
				return roleBinding.Subjects[0].Name == newClusterName
			})
			requireEventuallyTrue(t, func() bool {
				err := getScaleToZeroRBAC(ctx, br, &clusterRoleBinding)
				if err != nil {
					return false
				}
				return clusterRoleBinding.Subjects[0].Name == newClusterName
			})
		})
	})

	t.Run("RBAC resources are deleted when cluster name is unset", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			objs := []client.Object{
				&rbacv1.Role{},
				&rbacv1.RoleBinding{},
				&rbacv1.ClusterRole{},
				&rbacv1.ClusterRoleBinding{},
			}

			// Expect each resource to be created
			for _, obj := range objs {
				requireEventuallyNoErr(t, func() error {
					return getScaleToZeroRBAC(ctx, br, obj)
				})
			}

			// Remove the cluster name from the Branch
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Name = nil
			})
			require.NoError(t, err)

			// Expect each resource to be deleted
			for _, obj := range objs {
				requireEventuallyTrue(t, func() bool {
					err := getScaleToZeroRBAC(ctx, br, obj)
					return apierrors.IsNotFound(err)
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
