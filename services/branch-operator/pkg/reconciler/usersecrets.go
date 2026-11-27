package reconciler

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"xata/internal/passwords"
	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/shared"
)

// reconcileSuperuserSecret ensures that the superuser secret exists for the
// given Branch.
func (r *BranchReconciler) reconcileSuperuserSecret(ctx context.Context,
	branch *v1alpha1.Branch,
) (controllerutil.OperationResult, error) {
	return r.reconcileSecret(ctx, branch, branch.Name+"-superuser", "postgres")
}

// reconcileAppSecret ensures that the app user secret exists for the given
// Branch. The username is the login of the Branch's SQL identity.
func (r *BranchReconciler) reconcileAppSecret(ctx context.Context,
	branch *v1alpha1.Branch,
) (controllerutil.OperationResult, error) {
	return r.reconcileSecret(ctx, branch, branch.Name+"-app", shared.BranchSQLIdentity(branch).Login)
}

func (r *BranchReconciler) reconcileSecret(ctx context.Context,
	branch *v1alpha1.Branch, name, username string,
) (controllerutil.OperationResult, error) {
	secret := &corev1.Secret{
		Name:      name,
		Namespace: r.ClustersNamespace,
	}

	return controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		// TODO: Remove once all secrets have been migrated to Branch ownership.
		// Clear any existing owner references (e.g. from the CNPG Cluster)
		// so the Branch can safely take controller ownership.
		secret.OwnerReferences = nil

		// Set the controller reference on the Secret to the Branch.
		if err := controllerutil.SetControllerReference(branch, secret, r.Scheme); err != nil {
			return err
		}

		// Ensure that labels are set on the Secret
		ensureLabels(secret, branch.Spec.InheritedMetadata)

		// Set the CNPG reload annotatation on the Secret to trigger reloads of the
		// CNPG Cluster when the Secret changes.
		secret.Labels["cnpg.io/reload"] = kubeTrue

		// Only populate secret data on initial creation. If the secret
		// already has data, preserve it to avoid overwriting passwords.
		if len(secret.Data) > 0 {
			return nil
		}

		// Generate the password using the logic as CNPG (1.28)
		pw, err := passwords.Generate()
		if err != nil {
			return fmt.Errorf("generate password: %w", err)
		}

		// Set the secret type and data
		secret.Type = corev1.SecretTypeBasicAuth
		secret.Data = map[string][]byte{
			corev1.BasicAuthUsernameKey: []byte(username),
			corev1.BasicAuthPasswordKey: []byte(pw),
		}

		return nil
	})
}
