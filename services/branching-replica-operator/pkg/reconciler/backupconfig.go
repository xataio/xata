package reconciler

import (
	"context"

	walgv1beta1 "github.com/wal-g/cnpg-plugin-wal-g/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"xata/services/branching-replica-operator/api/v1alpha1"
	"xata/services/branching-replica-operator/pkg/reconciler/resources"
)

// reconcileBackupConfig creates or updates the BackupConfig for the given
// BranchingReplica
func (r *BranchingReplicaReconciler) reconcileBackupConfig(
	ctx context.Context,
	replica *v1alpha1.BranchingReplica,
) error {
	backupConfig := &walgv1beta1.BackupConfig{
		Name:      replica.Name,
		Namespace: replica.Namespace,
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, backupConfig, func() error {
		// Ensure the owner reference is set on the BackupConfig
		if err := controllerutil.SetControllerReference(replica, backupConfig, r.Scheme()); err != nil {
			return err
		}

		// Ensure labels are set on the BackupConfig
		if backupConfig.Labels == nil {
			backupConfig.Labels = map[string]string{}
		}
		backupConfig.Labels[resources.BranchingReplicaLabel] = replica.Name

		// Set the spec for the BackupConfig
		backupConfig.Spec = resources.BackupConfigSpec(replica)

		return nil
	})

	return err
}
