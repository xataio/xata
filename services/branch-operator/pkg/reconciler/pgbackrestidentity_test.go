package reconciler_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"xata/internal/pgbackrest"
	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/reconciler"

	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestPgBackRestIdentityReconciliation(t *testing.T) {
	t.Parallel()

	t.Run("service account and token are created for web-id backups", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().WithPgBackRestWebIdentity("some-bucket", "us-east-1").Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			sa := corev1.ServiceAccount{}
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, pgbackrest.ServiceAccountName(br.Name), &sa)
			})
			require.Equal(t, br.Name+"-pgbackrest", sa.Name)
			require.Equal(t, br.ClusterName()+"-pgbackrest-web-identity", pgbackrest.WebIdentitySecretName(br.ClusterName()))
			require.Len(t, sa.GetOwnerReferences(), 1)
			require.Equal(t, br.Name, sa.GetOwnerReferences()[0].Name)

			secret := corev1.Secret{}
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, pgbackrest.WebIdentitySecretName(br.ClusterName()), &secret)
			})
			require.NotEmpty(t, secret.Data[pgbackrest.TokenKey])
			require.Contains(t, secret.Annotations[reconciler.PgBackRestTokenRequestAnnotation], "serviceAccount="+sa.Name+",")
			require.Contains(t, secret.Annotations[reconciler.PgBackRestTokenRequestAnnotation], "audiences=[sts.amazonaws.com]")

			expiresAt, err := time.Parse(time.RFC3339, secret.Annotations[reconciler.PgBackRestTokenExpirationAnnotation])
			require.NoError(t, err)
			refreshAt, err := time.Parse(time.RFC3339, secret.Annotations[reconciler.PgBackRestTokenRefreshAnnotation])
			require.NoError(t, err)
			require.True(t, refreshAt.Before(expiresAt))
		})
	})

	t.Run("node role Branches get and mount the identity but do not use it", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		// AWS IAM backups with the default keyType use the node role
		branch := NewBranchBuilder().WithPgBackRest("some-bucket", "us-east-1").Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, pgbackrest.WebIdentitySecretName(br.ClusterName()), &corev1.Secret{})
			})
			require.NoError(t, getK8SObject(ctx, pgbackrest.ServiceAccountName(br.Name), &corev1.ServiceAccount{}))

			cluster := apiv1.Cluster{}
			require.NoError(t, getK8SObject(ctx, br.ClusterName(), &cluster))
			wantVolume, wantEnv := pgbackrest.WebIdentity(br.ClusterName(), "arn:aws:iam::123456789012:role/test-cnpg-backups")
			require.Equal(t, &wantVolume, cluster.Spec.ProjectedVolumeTemplate)
			require.Equal(t, wantEnv, cluster.Spec.Env)
			require.Equal(t, "auto", cluster.Spec.Backup.PgBackRest.Repository.S3.KeyType)
		})
	})

	t.Run("no identity without pgbackrest backups, but the Cluster mounts it", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			cluster := apiv1.Cluster{}
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.ClusterName(), &cluster)
			})

			// The Cluster gets the web identity volume also without the
			// Secret.
			wantVolume, wantEnv := pgbackrest.WebIdentity(br.ClusterName(), "arn:aws:iam::123456789012:role/test-cnpg-backups")
			require.Equal(t, &wantVolume, cluster.Spec.ProjectedVolumeTemplate)
			require.Equal(t, wantEnv, cluster.Spec.Env)

			err := getK8SObject(ctx, pgbackrest.ServiceAccountName(br.Name), &corev1.ServiceAccount{})
			require.True(t, apierrors.IsNotFound(err))
			err = getK8SObject(ctx, pgbackrest.WebIdentitySecretName(br.ClusterName()), &corev1.Secret{})
			require.True(t, apierrors.IsNotFound(err))
		})
	})

	t.Run("Secret is owned by the Cluster and the ServiceAccount by the Branch", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().WithPgBackRestWebIdentity("some-bucket", "us-east-1").Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			secret := corev1.Secret{}
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, pgbackrest.WebIdentitySecretName(br.ClusterName()), &secret)
			})
			cluster := apiv1.Cluster{}
			require.NoError(t, getK8SObject(ctx, br.ClusterName(), &cluster))

			// envtest runs no garbage collector, so check the owner reference
			// that makes Kubernetes delete the Secret with the Cluster.
			require.Len(t, secret.GetOwnerReferences(), 1)
			owner := secret.GetOwnerReferences()[0]
			require.Equal(t, "Cluster", owner.Kind)
			require.Equal(t, cluster.UID, owner.UID)
			require.Nil(t, owner.Controller)

			// Remove the cluster from the Branch
			err := retryOnConflict(ctx, br, func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Name = nil
			})
			require.NoError(t, err)

			requireEventuallyTrue(t, func() bool {
				return apierrors.IsNotFound(getK8SObject(ctx, cluster.Name, &apiv1.Cluster{}))
			})
			require.NoError(t, getK8SObject(ctx, pgbackrest.ServiceAccountName(br.Name), &corev1.ServiceAccount{}))
		})
	})
	t.Run("restore from another Branch borrows its token until the first primary", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		source := NewBranchBuilder().Build().Name
		sourceSA := &corev1.ServiceAccount{Name: pgbackrest.ServiceAccountName(source), Namespace: XataClustersNamespace}
		require.NoError(t, k8sClient.Create(ctx, sourceSA))

		branch := NewBranchBuilder().WithPgBackRestWebIdentity("some-bucket", "us-east-1").Build()
		branch.Spec.Restore = &v1alpha1.RestoreSpec{
			Type: v1alpha1.RestoreTypeObjectStore,
			Name: source,
			PgBackRest: &v1alpha1.RestorePgBackRestSpec{
				RepoPath:   pgbackrest.RepoPath(XataClustersNamespace, source),
				StanzaName: source,
				S3: &v1alpha1.PgBackRestS3Spec{
					Bucket: "some-bucket", Region: "us-east-1", KeyType: pgbackrest.KeyTypeWebID,
				},
			},
		}

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			cluster := apiv1.Cluster{}
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.ClusterName(), &cluster)
			})

			// The API server accepts the role on the external cluster only
			repo := cluster.Spec.ExternalClusters[0].PgBackRest.Repository.S3
			require.Equal(t, testRestoreRoleARN, repo.RoleARN)
			require.Equal(t, pgbackrest.KeyTypeWebID, repo.KeyType)
			require.Empty(t, cluster.Spec.Backup.PgBackRest.Repository.S3.RoleARN)

			secret := corev1.Secret{}
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, pgbackrest.WebIdentitySecretName(br.ClusterName()), &secret)
				return err == nil && len(secret.OwnerReferences) == 1 && secret.OwnerReferences[0].Kind == "Cluster"
			})
			require.Equal(t, cluster.UID, secret.OwnerReferences[0].UID)
			require.Equal(t, "serviceAccount="+sourceSA.Name+",uid="+string(sourceSA.UID)+
				",audiences=[sts.amazonaws.com/restore],expirationSeconds=3600",
				secret.Annotations[reconciler.PgBackRestTokenRequestAnnotation])

			// The first instance pod marks itself as primary
			require.NoError(t, retryStatusOnConflict(ctx, &cluster, func(c *apiv1.Cluster) {
				c.Status.CurrentPrimary = c.Name + "-1"
			}))
			requireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, pgbackrest.WebIdentitySecretName(br.ClusterName()), &secret)
				return err == nil && strings.HasPrefix(secret.Annotations[reconciler.PgBackRestTokenRequestAnnotation],
					"serviceAccount="+pgbackrest.ServiceAccountName(br.Name)+",")
			})
			require.Contains(t, secret.Annotations[reconciler.PgBackRestTokenRequestAnnotation],
				",audiences=[sts.amazonaws.com],expirationSeconds=86400")

			b := v1alpha1.Branch{}
			require.NoError(t, k8sClient.Get(ctx, client.ObjectKey{Name: br.Name}, &b))
			cond := meta.FindStatusCondition(b.Status.Conditions, v1alpha1.RestoreSourceAccessConditionType)
			require.NotNil(t, cond)
			require.Equal(t, v1alpha1.RestoreSourceAccessReleasedReason, cond.Reason)
		})
	})

	t.Run("Cluster mounts the web identity Secret", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		branch := NewBranchBuilder().WithPgBackRestWebIdentity("some-bucket", "us-east-1").Build()

		withBranch(ctx, t, branch, func(t *testing.T, br *v1alpha1.Branch) {
			cluster := apiv1.Cluster{}
			requireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, br.ClusterName(), &cluster)
			})

			wantVolume, wantEnv := pgbackrest.WebIdentity(br.ClusterName(), "arn:aws:iam::123456789012:role/test-cnpg-backups")
			require.Equal(t, &wantVolume, cluster.Spec.ProjectedVolumeTemplate)
			require.Equal(t, wantEnv, cluster.Spec.Env)

			pgb := cluster.Spec.Backup.PgBackRest
			require.Equal(t, pgbackrest.KeyTypeWebID, pgb.Repository.S3.KeyType)
			require.Equal(t, "system:serviceaccount:"+XataClustersNamespace+":"+br.Name+"-pgbackrest", pgb.Options.RepoPath)
		})
	})
}
