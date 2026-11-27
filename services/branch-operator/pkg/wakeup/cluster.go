package wakeup

import (
	"context"
	"fmt"
	"time"

	"xata/internal/o11y"
	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/shared"
	"xata/services/branch-operator/pkg/wakeup/tracing"

	"github.com/go-logr/logr"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	apiv1ac "github.com/xataio/xata-cnpg/pkg/client/applyconfiguration/api/v1"
	"go.opentelemetry.io/otel/trace"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const rolePasswordSyncInterval = 100 * time.Millisecond

// setUserPasswordSecret updates the Cluster resource to set the user password
// secret for the Branch's login role to the secret associated with the Branch
func (r *WakeupReconciler) setUserPasswordSecret(ctx context.Context, branch *v1alpha1.Branch, cluster *apiv1.Cluster) error {
	ac := apiv1ac.Cluster(cluster.Name, cluster.Namespace).
		WithSpec(apiv1ac.ClusterSpec().
			WithManaged(apiv1ac.ManagedConfiguration().
				WithRoles(shared.AppRoleConfiguration(branch.Name, shared.BranchSQLIdentity(branch)))))

	return r.Apply(ctx, ac, client.FieldOwner(ReconcilerName), client.ForceOwnership)
}

// waitForRolePasswordSync polls the Cluster's status until it reports that the
// Branch's login role is using a password derived from the current version of
// the Branch's user password secret.
//
// The wait is best-effort; the Branch reconciler re-applies the same
// configuration after adoption, so the password sync will eventually occur. If
// the password sync has not landed within r.PasswordSyncTimeout (which can
// happen when the cloned volume needs a long WAL redo), the wakeup proceeds
// anyway.
func (r *WakeupReconciler) waitForRolePasswordSync(
	ctx context.Context,
	log logr.Logger,
	branchName string,
	login string,
	cluster *apiv1.Cluster,
) (outcome string, err error) {
	// Start a span for the password sync wait operation
	ctx, span := tracing.Tracer(ctx).
		Start(ctx, tracing.SpanPasswordSyncWait,
			trace.WithAttributes(
				tracing.AttrBranch.String(branchName),
				tracing.AttrCluster.String(cluster.Name),
			))
	defer o11y.CloseSpan(span, &err)

	// Get the branch password secret for the login role
	secret := &corev1.Secret{}
	err = r.Get(ctx, client.ObjectKey{
		Name:      branchName + "-app",
		Namespace: cluster.Namespace,
	}, secret)
	if err != nil {
		return "", fmt.Errorf("get branch secret %q: %w", branchName+"-app", err)
	}

	// Poll the Cluster's status until it reports that the login role is
	// using the password from the current version of the Branch secret, or until
	// the timeout passes. The timeout is a condition result rather than a poll
	// deadline so that it ends the wait successfully, while a cancelled reconcile
	// context still ends it with an error.
	deadline := time.Now().Add(r.PasswordSyncTimeout)
	err = wait.PollUntilContextCancel(ctx, rolePasswordSyncInterval, true,
		func(ctx context.Context) (bool, error) {
			if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), cluster); err != nil {
				return false, err
			}

			// Check if the cluster has synced the user password secret
			if clusterUsesCredsFromSecretVersion(cluster, login, secret.ResourceVersion) {
				outcome = tracing.PasswordSyncOutcomeSynced
				return true, nil
			}

			// Check if the branch has been deleted mid-poll
			branch := &v1alpha1.Branch{}
			if err = r.Get(ctx, client.ObjectKey{Name: branchName}, branch); err != nil {
				if errors.IsNotFound(err) {
					log.Info("branch deleted while waiting for cluster to sync user password secret", "branchName", branchName)
					outcome = tracing.PasswordSyncOutcomeBranchDeleted
				}
				return true, err
			}

			// Check if the timeout has passed; if so, log a warning and proceed anyway
			if time.Now().After(deadline) {
				log.Info("timed out waiting for cluster to sync user password secret, proceeding anyway",
					"cluster", cluster.Name, "secret", secret.Name, "timeout", r.PasswordSyncTimeout)
				outcome = tracing.PasswordSyncOutcomeTimedOut
				return true, nil
			}

			log.Info("waiting for cluster to sync user password secret", "cluster", cluster.Name, "secret", secret.Name)
			return false, nil
		})
	span.SetAttributes(tracing.AttrPasswordSyncOutcome.String(outcome))
	return outcome, err
}

// clusterUsesCredsFromSecretVersion checks if the given Cluster's status
// reports that the specified user role is using a password derived from the
// given secret version
func clusterUsesCredsFromSecretVersion(cluster *apiv1.Cluster, username, secretVersion string) bool {
	st, ok := cluster.Status.ManagedRolesStatus.PasswordStatus[username]
	return ok && st.SecretResourceVersion == secretVersion
}
