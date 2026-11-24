package reconciler

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"xata/services/branching-replica-operator/api/v1alpha1"
)

// ReconcilerName is the controller name used for events and metrics.
const ReconcilerName = "branchingreplica"

// +kubebuilder:rbac:groups=xata.io,resources=branchingreplicas,verbs=get;list;watch,namespace=xata-clusters
// +kubebuilder:rbac:groups=xata.io,resources=branchingreplicas/status,verbs=get;update;patch,namespace=xata-clusters

// BranchingReplicaReconciler reconciles BranchingReplica resources.
type BranchingReplicaReconciler struct {
	client.Client
}

// Reconcile does nothing yet. It exists so the controller is registered and
// the deployment, RBAC and CRD wiring can be exercised end to end before the
// BranchingReplica spec is designed.
func (r *BranchingReplicaReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log.FromContext(ctx).V(1).Info("reconciling BranchingReplica", "name", req.Name, "namespace", req.Namespace)
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager
func (r *BranchingReplicaReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named(ReconcilerName).
		For(&v1alpha1.BranchingReplica{}).
		Complete(r)
}
