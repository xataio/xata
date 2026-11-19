package reconciler_test

import (
	"context"
	"testing"
	"time"

	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/reconciler"

	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestInheritedMetadataLabels(t *testing.T) {
	t.Parallel()

	t.Run("inherited labels are applied to owned resources", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		const projectIDLabelKey = "xata.io/projectID"
		const orgIDLabelKey = "xata.io/organizationID"

		branch := NewBranchBuilder().
			WithBackupRetention("7d").
			WithBackupSchedule("0 0 * * *").
			WithInheritedMetadata(&v1alpha1.InheritedMetadata{
				Labels: map[string]string{
					orgIDLabelKey:     "some-org-id",
					projectIDLabelKey: "some-project-id",
				},
			}).
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			// Expect the Cluster to be created
			cluster := apiv1.Cluster{}
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name, &cluster)
			})

			// Ensure the labels are present on the Cluster
			require.Equal(t, "some-org-id", cluster.Labels[orgIDLabelKey])
			require.Equal(t, "some-project-id", cluster.Labels[projectIDLabelKey])

			// Expect the NetworkPolicy to be created
			np := networkingv1.NetworkPolicy{}
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name, &np)
			})

			// Ensure the labels are present on the NetworkPolicy
			require.Equal(t, "some-org-id", np.Labels[orgIDLabelKey])
			require.Equal(t, "some-project-id", np.Labels[projectIDLabelKey])

			// Expect the ScheduledBackup to be created
			sb := apiv1.ScheduledBackup{}
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.Name, &sb)
			})

			// Ensure the labels are present on the ScheduledBackup
			require.Equal(t, "some-org-id", sb.Labels[orgIDLabelKey])
			require.Equal(t, "some-project-id", sb.Labels[projectIDLabelKey])
		})
	})
}

func TestPauseReconciliationAnnotation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("branch status reflects reconciliation pause annotation", func(t *testing.T) {
		t.Parallel()

		branch := NewBranchBuilder().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			// Add the pause reconciliation annotation
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				if b.Annotations == nil {
					b.Annotations = make(map[string]string)
				}
				b.Annotations[reconciler.ReconciliationPausedAnnotation] = "true"
			})
			require.NoError(t, err)

			// Expect the status of the Branch to be updated. The condition should
			// reflect the fact that reconciliation is paused.
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, br.Name, br)
				if err != nil {
					return false
				}
				c := meta.FindStatusCondition(br.Status.Conditions, v1alpha1.BranchReadyConditionType)
				if c == nil {
					return false
				}
				return c.Status == metav1.ConditionUnknown && c.Reason == v1alpha1.ReconciliationPausedReason
			})

			// Remove the pause reconciliation annotation
			err = retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				delete(b.Annotations, reconciler.ReconciliationPausedAnnotation)
			})
			require.NoError(t, err)

			// Expect the status of the Branch to be updated. Branch reconciliation
			// is no longer paused, so the status should eventually turn to
			// Ready=True.
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, br.Name, br)
				if err != nil {
					return false
				}
				c := meta.FindStatusCondition(br.Status.Conditions, v1alpha1.BranchReadyConditionType)
				if c == nil {
					return false
				}
				return meta.IsStatusConditionTrue(br.Status.Conditions, v1alpha1.BranchReadyConditionType)
			})
		})
	})
}

func TestRestoreImmutability(t *testing.T) {
	t.Parallel()

	t.Run("restore spec can not be added after branch creation", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			// Attempt to add a restore spec
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.Restore = &v1alpha1.RestoreSpec{
					Type: v1alpha1.RestoreTypeVolumeSnapshot,
					Name: "some-snapshot",
				}
			})

			// Expect the update to be rejected with a validation error
			require.Error(t, err)
			require.True(t, apierrors.IsInvalid(err))
		})
	})

	t.Run("restore spec can not be removed after branch creation", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithRestore(v1alpha1.RestoreTypeVolumeSnapshot, "some-snapshot").
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			// Attempt to remove the restore spec
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.Restore = nil
			})

			// Expect the update to be rejected with a validation error
			require.Error(t, err)
			require.True(t, apierrors.IsInvalid(err))
		})
	})

	t.Run("restore spec type can not be modified", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithRestore(v1alpha1.RestoreTypeVolumeSnapshot, "some-snapshot").
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			// Attempt to modify the restore type
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.Restore.Type = v1alpha1.RestoreTypeObjectStore
			})

			// Expect the update to be rejected with a validation error
			require.Error(t, err)
			require.True(t, apierrors.IsInvalid(err))
		})
	})

	t.Run("restore spec name can not be modified", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().
			WithRestore(v1alpha1.RestoreTypeVolumeSnapshot, "some-snapshot").
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			// Attempt to modify the restore name
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.Restore.Name = "different-snapshot"
			})

			// Expect the update to be rejected with a validation error
			require.Error(t, err)
			require.True(t, apierrors.IsInvalid(err))
		})
	})

	t.Run("restore spec timestamp can not be modified", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		timestamp := time.Now().Add(-1 * time.Hour)
		branch := NewBranchBuilder().
			WithRestoreTimestamp(v1alpha1.RestoreTypeObjectStore, "some-object-store", timestamp).
			Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			// Attempt to modify the restore timestamp
			newTimestamp := time.Now()
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.Restore.Timestamp = &metav1.Time{Time: newTimestamp}
			})

			// Expect the update to be rejected with a validation error
			require.Error(t, err)
			require.True(t, apierrors.IsInvalid(err))
		})
	})
}

func TestStorageValidation(t *testing.T) {
	t.Parallel()

	t.Run("storage size can not be decreased", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			// Increase the storage size to a valid value
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Storage.Size = "100Gi"
			})
			require.NoError(t, err)

			// Attempt to decrease the storage size
			err = retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Storage.Size = "10Gi"
			})

			// Expect the update to be rejected with a validation error
			require.Error(t, err)
			require.True(t, apierrors.IsInvalid(err))
		})
	})
}

func TestPgBackRestBackendValidation(t *testing.T) {
	t.Parallel()

	const serviceAccount = "backups@project.iam.gserviceaccount.com"

	t.Run("s3 sub-struct only is accepted", func(t *testing.T) {
		t.Parallel()
		requirePgBackRestValid(t, &v1alpha1.PgBackRestSpec{
			S3: &v1alpha1.PgBackRestS3Spec{Bucket: "test-bucket", Region: "us-east-1"},
		}, true)
	})

	t.Run("gcs sub-struct only is accepted", func(t *testing.T) {
		t.Parallel()
		requirePgBackRestValid(t, &v1alpha1.PgBackRestSpec{
			GCS: &v1alpha1.PgBackRestGCSSpec{Bucket: "test-bucket", ServiceAccountEmail: serviceAccount},
		}, true)
	})

	t.Run("azure sub-struct only is accepted", func(t *testing.T) {
		t.Parallel()
		requirePgBackRestValid(t, &v1alpha1.PgBackRestSpec{
			Azure: &v1alpha1.PgBackRestAzureSpec{Account: "testaccount", Container: "backups"},
		}, true)
	})

	t.Run("s3 and gcs together is rejected", func(t *testing.T) {
		t.Parallel()
		requirePgBackRestValid(t, &v1alpha1.PgBackRestSpec{
			S3:  &v1alpha1.PgBackRestS3Spec{Bucket: "test-bucket", Region: "us-east-1"},
			GCS: &v1alpha1.PgBackRestGCSSpec{Bucket: "test-bucket", ServiceAccountEmail: serviceAccount},
		}, false)
	})

	t.Run("s3 and azure together is rejected", func(t *testing.T) {
		t.Parallel()
		requirePgBackRestValid(t, &v1alpha1.PgBackRestSpec{
			S3:    &v1alpha1.PgBackRestS3Spec{Bucket: "test-bucket", Region: "us-east-1"},
			Azure: &v1alpha1.PgBackRestAzureSpec{Account: "testaccount", Container: "backups"},
		}, false)
	})

	t.Run("gcs and azure together is rejected", func(t *testing.T) {
		t.Parallel()
		requirePgBackRestValid(t, &v1alpha1.PgBackRestSpec{
			GCS:   &v1alpha1.PgBackRestGCSSpec{Bucket: "test-bucket", ServiceAccountEmail: serviceAccount},
			Azure: &v1alpha1.PgBackRestAzureSpec{Account: "testaccount", Container: "backups"},
		}, false)
	})

	t.Run("no backend is rejected", func(t *testing.T) {
		t.Parallel()
		requirePgBackRestValid(t, &v1alpha1.PgBackRestSpec{}, false)
	})
}

// requirePgBackRestValid creates a pgbackrest branch with the given spec and
// asserts whether the API server admits it (the CEL one-of validation).
func requirePgBackRestValid(t *testing.T, pgb *v1alpha1.PgBackRestSpec, wantValid bool) {
	t.Helper()
	ctx := context.Background()

	branch := NewBranchBuilder().Build()
	branch.Spec.BackupSpec = &v1alpha1.BackupSpec{
		Method:     v1alpha1.BackupMethodPgBackRest,
		PgBackRest: pgb,
	}

	err := k8sClient.Create(ctx, &branch)
	if wantValid {
		require.NoError(t, err)
		require.NoError(t, k8sClient.Delete(ctx, &branch))
		return
	}
	require.Error(t, err)
	require.True(t, apierrors.IsInvalid(err))
}
