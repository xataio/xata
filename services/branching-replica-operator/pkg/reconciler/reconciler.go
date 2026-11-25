package reconciler

import (
	"context"
	"fmt"

	walgv1beta1 "github.com/wal-g/cnpg-plugin-wal-g/api/v1beta1"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"xata/services/branching-replica-operator/api/v1alpha1"
)

const (
	ReconcilerName = "branchingreplica"
	OperatorName   = "branching-replica-operator"
)

// +kubebuilder:rbac:groups=xata.io,resources=branchingreplicas,verbs=get;list;watch,namespace=xata-clusters
// +kubebuilder:rbac:groups=xata.io,resources=branchingreplicas/status,verbs=get;update;patch,namespace=xata-clusters
// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=clusters,verbs=get;list;watch;create;patch,namespace=xata-clusters
// +kubebuilder:rbac:groups=cnpg-extensions.yandex.cloud,resources=backupconfigs,verbs=get;list;watch;create;update,namespace=xata-clusters

// BranchingReplicaReconciler reconciles BranchingReplica resources
type BranchingReplicaReconciler struct {
	client.Client
	Tolerations      []corev1.Toleration
	ImagePullSecrets []string
}

// Reconcile reconciles a BranchingReplica resource
func (r *BranchingReplicaReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var replica v1alpha1.BranchingReplica
	if err := r.Get(ctx, req.NamespacedName, &replica); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !replica.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Reconcile the BackupConfig resource for the BranchingReplica
	if err := r.reconcileBackupConfig(ctx, &replica); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile backup config: %w", err)
	}

	// Reconcile the Cluster resource for the BranchingReplica
	if err := r.reconcileCluster(ctx, &replica); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile cluster: %w", err)
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager
func (r *BranchingReplicaReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named(ReconcilerName).
		For(&v1alpha1.BranchingReplica{}).
		Owns(&apiv1.Cluster{}).
		Owns(&walgv1beta1.BackupConfig{}).
		Complete(r)
}
