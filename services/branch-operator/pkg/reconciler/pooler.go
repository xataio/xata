package reconciler

import (
	"context"
	"strconv"

	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/reconciler/resources"
)

const PoolerSuffix = "-pooler"

// reconcilePooler ensures that the correct Pooler exists for the given Branch
// when a pooler is configured. When Pooler is nil, it ensures no Pooler exists.
func (r *BranchReconciler) reconcilePooler(
	ctx context.Context,
	branch *v1alpha1.Branch,
) (controllerutil.OperationResult, error) {
	pooler := &apiv1.Pooler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      branch.Name + PoolerSuffix,
			Namespace: r.ClustersNamespace,
		},
	}

	// If pooler is not configured, ensure Pooler doesn't exist
	if !branch.Spec.Pooler.IsEnabled() {
		err := r.Get(ctx, types.NamespacedName{
			Name:      branch.Name + PoolerSuffix,
			Namespace: r.ClustersNamespace,
		}, pooler)
		if err != nil {
			return controllerutil.OperationResultNone, client.IgnoreNotFound(err)
		}

		if err := r.Delete(ctx, pooler); err != nil {
			return controllerutil.OperationResultNone, err
		}
		return controllerutil.OperationResultUpdated, nil
	}

	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, pooler, func() error {
		if err := controllerutil.SetControllerReference(branch, pooler, r.Scheme); err != nil {
			return err
		}

		ensureLabels(&pooler.ObjectMeta, branch.Spec.InheritedMetadata)

		pooler.Spec = resources.PoolerSpec(
			branch.ClusterName(),
			branch.Spec.Pooler.Instances,
			!branch.HasClusterName() || branch.Spec.ClusterSpec.Hibernation.IsEnabled(),
			apiv1.PgBouncerPoolMode(branch.Spec.Pooler.Mode),
			branch.Spec.Pooler.MaxClientConn,
			defaultPoolSize(branch),
			branch.Spec.InheritedMetadata.GetLabels(),
			r.ImagePullSecrets,
			r.Tolerations,
		)

		return nil
	})

	return result, err
}

// defaultPoolSize returns the PgBouncer default_pool_size to set for the
// branch. An operator-supplied override on the PoolerSpec wins; otherwise
// the value is derived as floor(0.9 * max_connections) from the branch's
// Postgres parameters. Returns "" when neither is available, which leaves
// the PgBouncer default in place.
func defaultPoolSize(branch *v1alpha1.Branch) string {
	if v := branch.Spec.Pooler.DefaultPoolSize; v != "" {
		// CRD validation enforces ^[1-9][0-9]*$; re-check in case an
		// older CRD let a malformed value through.
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return v
		}
	}
	maxConns := maxConnectionsFromBranch(branch)
	if maxConns <= 0 {
		return ""
	}
	return strconv.Itoa(maxConns * 9 / 10)
}

// maxConnectionsFromBranch extracts the max_connections value from the
// Branch's Postgres parameters. Returns 0 when unset or unparseable.
func maxConnectionsFromBranch(branch *v1alpha1.Branch) int {
	if branch.Spec.ClusterSpec.Postgres == nil {
		return 0
	}
	for _, p := range branch.Spec.ClusterSpec.Postgres.Parameters {
		if p.Name != "max_connections" {
			continue
		}
		v, err := strconv.Atoi(p.Value)
		if err != nil || v <= 0 {
			return 0
		}
		return v
	}
	return 0
}
