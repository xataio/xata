package reconciler

import (
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// GenerationOrAnnotationChanged returns a predicate that filters events
// where either the generation or the annotations have changed
func GenerationOrAnnotationChanged() predicate.Predicate {
	return predicate.Or(
		predicate.GenerationChangedPredicate{},
		predicate.AnnotationChangedPredicate{},
	)
}

// ClusterPhaseOrGenerationOrAnnotationChanged is a predicate for Cluster
// resources that filters events where the Cluster's phase, backup status
// fields, generation, or annotations have changed
var ClusterPhaseOrGenerationOrAnnotationChanged = predicate.Or(
	predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			//nolint:forcetypeassert
			oldCluster := e.ObjectOld.(*apiv1.Cluster)
			//nolint:forcetypeassert
			newCluster := e.ObjectNew.(*apiv1.Cluster)

			// React to status Phase transitions
			if oldCluster.Status.Phase != newCluster.Status.Phase {
				return true
			}

			// React to the first primary, which marks the end of a restore
			if oldCluster.Status.CurrentPrimary != newCluster.Status.CurrentPrimary {
				return true
			}

			// React to backup status changes, so updateBackupStatus copies
			// them to the Branch. A backup changes no phase, generation, or
			// annotation. The fields are marked deprecated upstream because
			// backup plugins do not set them; our in-core pgbackrest does.
			//nolint:staticcheck
			return oldCluster.Status.FirstRecoverabilityPoint != newCluster.Status.FirstRecoverabilityPoint ||
				oldCluster.Status.LastSuccessfulBackup != newCluster.Status.LastSuccessfulBackup ||
				oldCluster.Status.LastFailedBackup != newCluster.Status.LastFailedBackup ||
				oldCluster.Status.LastRecoverabilityPoint != newCluster.Status.LastRecoverabilityPoint
		},
	},
	predicate.GenerationChangedPredicate{},
	predicate.AnnotationChangedPredicate{},
)
