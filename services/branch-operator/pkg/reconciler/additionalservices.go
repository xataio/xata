package reconciler

import (
	"context"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/reconciler/resources"
)

const ciliumGlobalAnnotation = "service.cilium.io/global"

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
// exists for the given Branch.
func (r *BranchReconciler) reconcileAdditionalServiceRO(
	ctx context.Context,
	branch *v1alpha1.Branch,
) (controllerutil.OperationResult, error) {
	return r.reconcileAdditionalService(ctx, branch, "branch-"+branch.Name+"-ro", "ro")
}

// reconcileAdditionalServicePooler ensures that the pooler additional service
// exists for the given Branch once the pooler has been enabled. When the pooler
// is not enabled, the service is left untouched: if one already exists from a
// previous enabled state it persists with a selector that matches no pods, and
// if none exists it is not created. Like the rw/r/ro additional services, this
// runs on every cell — on cells without local pooler pods, the selector matches
// nothing and Cilium routes to the remote cell.
func (r *BranchReconciler) reconcileAdditionalServicePooler(
	ctx context.Context,
	branch *v1alpha1.Branch,
) (controllerutil.OperationResult, error) {
	if !branch.Spec.Pooler.IsEnabled() {
		return controllerutil.OperationResultNone, nil
	}

	name := "branch-" + branch.Name + "-pooler"
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: r.ClustersNamespace,
		},
	}

	poolerName := branch.Name + PoolerSuffix

	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		if err := controllerutil.SetControllerReference(branch, svc, r.Scheme); err != nil {
			return err
		}

		ensureLabels(&svc.ObjectMeta, branch.Spec.InheritedMetadata)

		if svc.Annotations == nil {
			svc.Annotations = make(map[string]string)
		}
		svc.Annotations[ciliumGlobalAnnotation] = kubeTrue

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
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: r.ClustersNamespace,
		},
	}

	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		// Ensure the owner reference is set on the Service
		if err := controllerutil.SetControllerReference(branch, svc, r.Scheme); err != nil {
			return err
		}

		// Ensure labels are set on the Service
		ensureLabels(&svc.ObjectMeta, branch.Spec.InheritedMetadata)

		// Ensure the Cilium global annotation is set on the Service
		if svc.Annotations == nil {
			svc.Annotations = make(map[string]string)
		}
		svc.Annotations[ciliumGlobalAnnotation] = kubeTrue

		// Set the spec for the Service
		svc.Spec = resources.AdditionalServiceSpec(branch.ClusterName(), selectorType)

		return nil
	})

	return result, err
}
