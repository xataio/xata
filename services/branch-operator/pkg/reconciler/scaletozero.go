package reconciler

import (
	"context"
	"fmt"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/reconciler/resources"
)

// ScaleToZeroRBACSuffix is appended to the branch name to name its scale-to-zero
// RBAC resources.
const ScaleToZeroRBACSuffix = "-scale-to-zero"

// reconcileScaleToZeroRBAC reconciles the RBAC resources that let the
// scale-to-zero sidecar in the branch's Cluster pods hibernate the branch. The
// sidecar runs under the ServiceAccount CNPG creates for the Cluster, which is
// named after the Cluster.
func (r *BranchReconciler) reconcileScaleToZeroRBAC(ctx context.Context, branch *v1alpha1.Branch) error {
	// If the branch has no cluster defined, ensure its RBAC resources don't
	// exist
	if !branch.HasClusterName() {
		return r.deleteScaleToZeroRBAC(ctx, branch)
	}

	// Reconcile the ClusterRole that allows the sidecar to update the Branch
	if err := r.reconcileScaleToZeroClusterRole(ctx, branch); err != nil {
		return fmt.Errorf("reconcile ClusterRole: %w", err)
	}

	// Reconcile the ClusterRoleBinding that binds the ClusterRole to the
	// ServiceAccount for the Cluster
	if err := r.reconcileScaleToZeroClusterRoleBinding(ctx, branch); err != nil {
		return fmt.Errorf("reconcile ClusterRoleBinding: %w", err)
	}

	return nil
}

// reconcileScaleToZeroClusterRole reconciles the ClusterRole that allows the
// scale-to-zero sidecar in the branch's Cluster pods to update the Branch resource.
func (r *BranchReconciler) reconcileScaleToZeroClusterRole(ctx context.Context, branch *v1alpha1.Branch) error {
	cr := scaleToZeroClusterRole(branch)

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cr, func() error {
		// Ensure the owner reference is set on the ClusterRole
		if err := controllerutil.SetControllerReference(branch, cr, r.Scheme); err != nil {
			return err
		}

		// Ensure labels are set on the ClusterRole
		ensureLabels(cr, branch.Spec.InheritedMetadata)

		// Set the rules for the ClusterRole
		cr.Rules = resources.ScaleToZeroClusterRoleRules(branch.Name)

		return nil
	})

	return err
}

// reconcileScaleToZeroClusterRoleBinding reconciles the ClusterRoleBinding that
// binds the ClusterRole to the ServiceAccount for the branch's Cluster.
func (r *BranchReconciler) reconcileScaleToZeroClusterRoleBinding(ctx context.Context, branch *v1alpha1.Branch) error {
	crb := scaleToZeroClusterRoleBinding(branch)

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, crb, func() error {
		// Ensure the owner reference is set on the ClusterRoleBinding
		if err := controllerutil.SetControllerReference(branch, crb, r.Scheme); err != nil {
			return err
		}

		// Ensure labels are set on the ClusterRoleBinding
		ensureLabels(crb, branch.Spec.InheritedMetadata)

		// Set the role reference and subjects for the ClusterRoleBinding
		crb.RoleRef = rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     scaleToZeroClusterRole(branch).Name,
		}
		crb.Subjects = r.scaleToZeroSubjects(branch)

		return nil
	})

	return err
}

// deleteScaleToZeroRBAC deletes the branch's scale-to-zero RBAC resources,
// binding first so that no binding is left referring to a missing role.
func (r *BranchReconciler) deleteScaleToZeroRBAC(ctx context.Context, branch *v1alpha1.Branch) error {
	objs := []client.Object{
		scaleToZeroClusterRoleBinding(branch),
		scaleToZeroClusterRole(branch),
	}
	for _, obj := range objs {
		if err := client.IgnoreNotFound(r.Delete(ctx, obj)); err != nil {
			return fmt.Errorf("delete %T %s: %w", obj, obj.GetName(), err)
		}
	}
	return nil
}

// scaleToZeroSubjects returns the single subject of the branch's scale-to-zero
// binding: the ServiceAccount CNPG creates for the branch's current Cluster.
func (r *BranchReconciler) scaleToZeroSubjects(branch *v1alpha1.Branch) []rbacv1.Subject {
	return []rbacv1.Subject{{
		Kind:      rbacv1.ServiceAccountKind,
		Name:      branch.ClusterName(),
		Namespace: r.ClustersNamespace,
	}}
}

// scaleToZeroRBACName returns the name of the branch's scale-to-zero RBAC
// resources
func scaleToZeroRBACName(branch *v1alpha1.Branch) string {
	return branch.Name + ScaleToZeroRBACSuffix
}

//
// The constructors below return the RBAC objects with only their identity set,
// for use as the target of CreateOrUpdate or Delete.
//

func scaleToZeroClusterRole(branch *v1alpha1.Branch) *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		Name: scaleToZeroRBACName(branch),
	}
}

func scaleToZeroClusterRoleBinding(branch *v1alpha1.Branch) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		Name: scaleToZeroRBACName(branch),
	}
}
