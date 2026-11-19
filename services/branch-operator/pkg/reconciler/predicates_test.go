package reconciler_test

import (
	"testing"

	"xata/services/branch-operator/pkg/reconciler"

	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func TestPredicates(t *testing.T) {
	t.Parallel()

	t.Run("ServiceAccount annotation change", func(t *testing.T) {
		t.Parallel()
		old := &corev1.ServiceAccount{}
		updated := old.DeepCopy()
		updated.Annotations = map[string]string{"xata.io/restore-gcs-managed-folder-observed": "gs://bucket/folder/"}

		require.True(t, reconciler.GenerationOrAnnotationChanged().Update(event.UpdateEvent{ObjectOld: old, ObjectNew: updated}))
		require.False(t, reconciler.GenerationOrAnnotationChanged().Update(event.UpdateEvent{ObjectOld: old, ObjectNew: old.DeepCopy()}))
	})

	t.Run("Cluster currentPrimary change", func(t *testing.T) {
		t.Parallel()
		old := &apiv1.Cluster{}
		updated := old.DeepCopy()
		updated.Status.CurrentPrimary = "cluster-1"

		p := reconciler.ClusterPhaseOrGenerationOrAnnotationChanged
		require.True(t, p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: updated}))
		require.False(t, p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: old.DeepCopy()}))
	})
}
