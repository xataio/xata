package reconciler_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	walgv1beta1 "github.com/wal-g/cnpg-plugin-wal-g/api/v1beta1"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"

	"xata/internal/envtestutil"
	"xata/services/branching-replica-operator/api/v1alpha1"
	"xata/services/branching-replica-operator/pkg/reconciler/resources"
)

func TestResourceCreation(t *testing.T) {
	t.Parallel()

	t.Run("backup config and cluster are created for a branching replica", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		withBranchingReplica(ctx, t, baseReplicaSpec(), func(t *testing.T, replica *v1alpha1.BranchingReplica) {
			// Expect the BackupConfig to be created with the name of the
			// BranchingReplica
			var backupConfig walgv1beta1.BackupConfig
			envtestutil.RequireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, replica.Name, &backupConfig)
			})

			// Expect the BackupConfig to be owned by the BranchingReplica
			requireControlledBy(t, &backupConfig, replica)
			require.Equal(t, replica.Name, backupConfig.Labels["xata.io/branching-replica"])

			// Expect the BackupConfig to describe the source's archive
			require.Equal(t, resources.BackupConfigSpec(replica).Storage, backupConfig.Spec.Storage)
			require.True(t, backupConfig.Spec.Retention.IgnoreForBackupConfigDeletion)

			// Expect the Cluster to be created with the name of the
			// BranchingReplica
			var cluster apiv1.Cluster
			envtestutil.RequireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, replica.Name, &cluster)
			})

			// Expect the Cluster to be owned by the BranchingReplica
			requireControlledBy(t, &cluster, replica)
			require.Equal(t, replica.Name, cluster.Labels["xata.io/branching-replica"])
			require.Equal(t, "enabled", cluster.Annotations["cnpg.io/skipWalArchiving"])

			// Expect the Cluster to be a replica cluster that uses the
			// BackupConfig
			require.Equal(t, testImage, cluster.Spec.ImageName)
			require.NotNil(t, cluster.Spec.ReplicaCluster)
			require.NotNil(t, cluster.Spec.ReplicaCluster.Enabled)
			require.True(t, *cluster.Spec.ReplicaCluster.Enabled)
			require.Len(t, cluster.Spec.Plugins, 1)
			require.Equal(t, replica.Name, cluster.Spec.Plugins[0].Parameters["backupConfig"])

			// Expect the Cluster to carry the reconciler's tolerations and
			// image pull secrets
			require.Equal(t, testTolerations, cluster.Spec.Affinity.Tolerations)
			require.Equal(t,
				[]apiv1.LocalObjectReference{{Name: testImagePullSecret}},
				cluster.Spec.ImagePullSecrets)
		})
	})
}

func TestResourceUpdates(t *testing.T) {
	t.Parallel()

	t.Run("postgres parameters change is applied to the cluster", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		withBranchingReplica(ctx, t, baseReplicaSpec(), func(t *testing.T, replica *v1alpha1.BranchingReplica) {
			// Expect the Cluster to be created
			var cluster apiv1.Cluster
			envtestutil.RequireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, replica.Name, &cluster)
			})

			// Set a PostgreSQL parameter on the BranchingReplica
			err := envtestutil.RetryOnConflict(ctx, k8sClient, replica,
				func(r *v1alpha1.BranchingReplica) {
					r.Spec.Postgres.Parameters = map[string]string{
						"max_connections": "500",
					}
				})
			require.NoError(t, err)

			// Expect the parameter to be set on the Cluster
			envtestutil.RequireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, replica.Name, &cluster)
				return err == nil &&
					cluster.Spec.PostgresConfiguration.Parameters["max_connections"] == "500"
			})
		})
	})

	t.Run("sidecar download concurrency change is applied to the backup config", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		withBranchingReplica(ctx, t, baseReplicaSpec(), func(t *testing.T, replica *v1alpha1.BranchingReplica) {
			// Expect the BackupConfig to be created
			var backupConfig walgv1beta1.BackupConfig
			envtestutil.RequireEventuallyNoErr(t, func() error {
				return getK8SObject(ctx, replica.Name, &backupConfig)
			})

			// Set the download concurrency on the BranchingReplica
			err := envtestutil.RetryOnConflict(ctx, k8sClient, replica,
				func(r *v1alpha1.BranchingReplica) {
					r.Spec.WALG = &v1alpha1.WALGSpec{
						DownloadConcurrency: new(4),
					}
				})
			require.NoError(t, err)

			// Expect the download concurrency to be set on the BackupConfig
			envtestutil.RequireEventuallyTrue(t, func() bool {
				err := getK8SObject(ctx, replica.Name, &backupConfig)
				return err == nil &&
					backupConfig.Spec.DownloadConcurrency != nil &&
					*backupConfig.Spec.DownloadConcurrency == 4
			})
		})
	})
}
