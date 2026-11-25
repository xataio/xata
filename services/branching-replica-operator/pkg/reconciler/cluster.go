package reconciler

import (
	"context"

	apiv1ac "github.com/xataio/xata-cnpg/pkg/client/applyconfiguration/api/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"xata/services/branching-replica-operator/api/v1alpha1"
	"xata/services/branching-replica-operator/pkg/reconciler/resources"
)

// reconcileCluster creates or updates the CNPG Cluster for the given
// BranchingReplica using server-side apply
func (r *BranchingReplicaReconciler) reconcileCluster(
	ctx context.Context,
	replica *v1alpha1.BranchingReplica,
) error {
	cfg := resources.ClusterConfig{
		BranchingReplicaSpec: replica.Spec,
		Tolerations:          r.Tolerations,
		ImagePullSecrets:     r.ImagePullSecrets,
	}

	ac := apiv1ac.Cluster(replica.Name, replica.Namespace).
		WithLabels(resources.ClusterLabels(replica.Name)).
		WithAnnotations(resources.ClusterAnnotations()).
		WithOwnerReferences(metav1ac.OwnerReference().
			WithAPIVersion(v1alpha1.GroupVersion.String()).
			WithKind(v1alpha1.BranchingReplicaKind).
			WithName(replica.Name).
			WithUID(replica.UID).
			WithBlockOwnerDeletion(true).
			WithController(true)).
		WithSpec(resources.ClusterSpec(replica.Name, cfg))

	return r.Apply(ctx, ac, client.FieldOwner(OperatorName), client.ForceOwnership)
}
