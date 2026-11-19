package reconciler

import (
	"context"

	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	apiv1ac "github.com/xataio/xata-cnpg/pkg/client/applyconfiguration/api/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/reconciler/resources"
)

// reconcileScheduledBackup ensures that the correct ScheduledBackup exists for the
// given Branch when backups are configured, using server-side apply. When
// BackupConfiguration is nil, it ensures no ScheduledBackup exists.
//
// Both paths go straight to the apiserver. A read of the informer cache can lag
// a Branch create or delete, which makes the decision to create, update or
// delete act on stale state.
func (r *BranchReconciler) reconcileScheduledBackup(
	ctx context.Context,
	branch *v1alpha1.Branch,
) error {
	// If scheduled backup is not configured, ensure ScheduledBackup doesn't exist
	if !branch.Spec.BackupSpec.IsScheduledBackupEnabled() {
		sb := &apiv1.ScheduledBackup{
			Name:      branch.Name,
			Namespace: r.ClustersNamespace,
		}

		return client.IgnoreNotFound(r.Delete(ctx, sb))
	}

	ac := apiv1ac.ScheduledBackup(branch.Name, r.ClustersNamespace).
		WithLabels(clusterLabels(branch.Spec.InheritedMetadata)).
		WithOwnerReferences(metav1ac.OwnerReference().
			WithAPIVersion(v1alpha1.GroupVersion.String()).
			WithKind(v1alpha1.BranchKind).
			WithName(branch.Name).
			WithUID(branch.UID).
			WithBlockOwnerDeletion(true).
			WithController(true)).
		WithSpec(resources.ScheduledBackupSpec(
			branch.ClusterName(),
			branch.Spec.BackupSpec.ScheduledBackup.Schedule,
			!branch.HasClusterName() || branch.Spec.ClusterSpec.Hibernation.IsEnabled(),
		))

	return r.Apply(ctx, ac, client.FieldOwner(OperatorName), client.ForceOwnership)
}
