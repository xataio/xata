package reconciler

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/reconciler/resources"
)

// reconcileAdditionalServiceRW ensures that the read-write additional service
// exists for the given Branch.
func (r *BranchReconciler) reconcileAdditionalServiceRW(
	ctx context.Context,
	branch *v1alpha1.Branch,
) (controllerutil.OperationResult, error) {
	return r.reconcileAdditionalService(ctx, branch, "branch-"+branch.Name+"-rw", "rw")
}

// reconcileAdditionalServiceR ensures that the read additional service exists
// for the given Branch.
func (r *BranchReconciler) reconcileAdditionalServiceR(
	ctx context.Context,
	branch *v1alpha1.Branch,
) (controllerutil.OperationResult, error) {
	return r.reconcileAdditionalService(ctx, branch, "branch-"+branch.Name+"-r", "r")
}

// reconcileAdditionalServiceRO ensures that the read-only additional service
// exists for the given Branch only if the Branch has replicas
func (r *BranchReconciler) reconcileAdditionalServiceRO(
	ctx context.Context,
	branch *v1alpha1.Branch,
) (controllerutil.OperationResult, error) {
	// If the branch has no replicas, ensure that the read-only service does not
	// exist
	if branch.Spec.ClusterSpec.Instances == 1 {
		return controllerutil.OperationResultNone, client.IgnoreNotFound(
			r.Delete(ctx, &v1.Service{
				Name:      "branch-" + branch.Name + "-ro",
				Namespace: r.ClustersNamespace,
			}))
	}

	return r.reconcileAdditionalService(ctx, branch, "branch-"+branch.Name+"-ro", "ro")
}

// reconcileAdditionalServicePooler ensures that the pooler additional service
// exists for the given Branch if the pooler has been enabled.
func (r *BranchReconciler) reconcileAdditionalServicePooler(
	ctx context.Context,
	branch *v1alpha1.Branch,
) (controllerutil.OperationResult, error) {
	if !branch.Spec.Pooler.IsEnabled() || !branch.HasClusterName() {
		return controllerutil.OperationResultNone, nil
	}

	name := "branch-" + branch.Name + "-pooler"
	svc := &v1.Service{
		Name:      name,
		Namespace: r.ClustersNamespace,
	}

	// The Service keeps its branch-based name so callers addressing it are
	// unaffected, but its selector must target the cluster-named pooler's pods.
	poolerName := branch.ClusterName() + PoolerSuffix

	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		if err := controllerutil.SetControllerReference(branch, svc, r.Scheme); err != nil {
			return err
		}

		ensureLabels(svc, branch.Spec.InheritedMetadata)

		svc.Spec = resources.PoolerServiceSpec(poolerName)

		return nil
	})

	return result, err
}

// reconcileAdditionalService ensures that the given additional service exists
// for the Branch. These services route directly to CNPG PostgreSQL pods and are
// owned by the Branch so they survive CNPG Cluster deletion/recreation.
func (r *BranchReconciler) reconcileAdditionalService(
	ctx context.Context,
	branch *v1alpha1.Branch, name, selectorType string,
) (controllerutil.OperationResult, error) {
	svc := &v1.Service{
		Name:      name,
		Namespace: r.ClustersNamespace,
	}

	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		// Ensure the owner reference is set on the Service
		if err := controllerutil.SetControllerReference(branch, svc, r.Scheme); err != nil {
			return err
		}

		// Ensure labels are set on the Service
		ensureLabels(svc, branch.Spec.InheritedMetadata)

		// Set the spec for the Service
		svc.Spec = resources.AdditionalServiceSpec(branch.ClusterName(), selectorType)

		return nil
	})

	return result, err
}
