package reconciler

import (
	"context"
	"maps"
	"strconv"

	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	apiv1ac "github.com/xataio/xata-cnpg/pkg/client/applyconfiguration/api/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/reconciler/resources"
)

const (
	BranchAnnotation                = "xata.io/branch"
	ScaleToZeroEnabledAnnotation    = "xata.io/scale-to-zero-enabled"
	ScaleToZeroInactivityAnnotation = "xata.io/scale-to-zero-inactivity-minutes"
	HibernationAnnotation           = "cnpg.io/hibernation"
	PodPatchAnnotation              = "cnpg.io/podPatch"
	//nolint: gosec
	SkipWALArchivingAnnotation = "cnpg.io/skipWalArchiving"
)

// reconcileCluster creates or updates the CNPG Cluster for the given Branch
// using server-side apply.
func (r *BranchReconciler) reconcileCluster(
	ctx context.Context,
	branch *v1alpha1.Branch,
) error {
	if !branch.HasClusterName() {
		return nil
	}

	cfg := resources.ClusterConfig{
		ClusterSpec:       branch.Spec.ClusterSpec,
		BackupSpec:        branch.Spec.BackupSpec,
		InheritedMetadata: branch.Spec.InheritedMetadata,
		RestoreSpec:       branch.Spec.Restore,
		Tolerations:       r.Tolerations,
		EnforceZone:       r.EnforceZone,
		ImagePullSecrets:  r.ImagePullSecrets,
	}

	ac := apiv1ac.Cluster(branch.ClusterName(), r.ClustersNamespace).
		WithLabels(clusterLabels(branch.Spec.InheritedMetadata)).
		WithAnnotations(clusterAnnotations(branch)).
		WithOwnerReferences(metav1ac.OwnerReference().
			WithAPIVersion(v1alpha1.GroupVersion.String()).
			WithKind("Branch").
			WithName(branch.Name).
			WithUID(branch.UID).
			WithBlockOwnerDeletion(true).
			WithController(true)).
		WithSpec(resources.ClusterSpec(branch.Name, branch.ClusterName(), cfg))

	return r.Apply(ctx, ac, client.FieldOwner(OperatorName), client.ForceOwnership)
}

// reconcileOwnedClusters ensures that only the Cluster named in the Branch
// spec is owned by the Branch, deleting any other Clusters owned by the
// Branch.
func (r *BranchReconciler) reconcileOwnedClusters(
	ctx context.Context,
	branch *v1alpha1.Branch,
) (controllerutil.OperationResult, error) {
	var clusterList apiv1.ClusterList

	// List all Clusters owned by the Branch
	err := r.List(ctx, &clusterList,
		client.InNamespace(r.ClustersNamespace),
		client.MatchingFields{ClusterOwnerKey: branch.Name},
	)
	if err != nil {
		return "", err
	}

	// Delete any owned Clusters not named in the Branch spec
	result := controllerutil.OperationResultNone
	for _, cluster := range clusterList.Items {
		// Don't delete the Cluster we should keep
		if cluster.Name == branch.ClusterName() {
			continue
		}

		// Don't delete Clusters that are already being deleted
		if !cluster.DeletionTimestamp.IsZero() {
			continue
		}

		// Delete the Cluster
		if err := r.Delete(ctx, &cluster); err != nil {
			return "", err
		}
		result = controllerutil.OperationResultUpdated
	}

	return result, nil
}

// clusterAnnotations builds the annotation map for a CNPG Cluster resource
func clusterAnnotations(branch *v1alpha1.Branch) map[string]string {
	annotations := map[string]string{
		BranchAnnotation:   branch.Name,
		PodPatchAnnotation: `[{"op": "add", "path": "/spec/enableServiceLinks", "value": false}]`,
	}

	cSpec := branch.Spec.ClusterSpec
	if cSpec.ScaleToZero != nil && cSpec.ScaleToZero.Enabled {
		annotations[ScaleToZeroEnabledAnnotation] = kubeTrue
		annotations[ScaleToZeroInactivityAnnotation] = strconv.Itoa(int(cSpec.ScaleToZero.InactivityPeriodMinutes))
	} else {
		annotations[ScaleToZeroEnabledAnnotation] = kubeFalse
	}

	if cSpec.Hibernation != nil && *cSpec.Hibernation == v1alpha1.HibernationModeEnabled {
		annotations[HibernationAnnotation] = "on"
	} else {
		annotations[HibernationAnnotation] = "off"
	}

	bSpec := branch.Spec.BackupSpec
	if bSpec.IsWALArchivingDisabled() {
		annotations[SkipWALArchivingAnnotation] = "enabled"
	} else {
		annotations[SkipWALArchivingAnnotation] = "disabled"
	}

	return annotations
}

// clusterLabels builds the label map for a CNPG Cluster resource
func clusterLabels(inheritedMetadata *v1alpha1.InheritedMetadata) map[string]string {
	labels := map[string]string{
		"app.kubernetes.io/managed-by": OperatorName,
	}
	if inheritedMetadata != nil && inheritedMetadata.Labels != nil {
		maps.Copy(labels, inheritedMetadata.Labels)
	}
	return labels
}
