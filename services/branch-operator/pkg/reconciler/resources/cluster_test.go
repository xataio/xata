package resources_test

import (
	"testing"
	"time"

	machineryapi "github.com/cloudnative-pg/machinery/pkg/api"
	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	apiv1ac "github.com/xataio/xata-cnpg/pkg/client/applyconfiguration/api/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/utils/ptr"

	"xata/internal/pgbackrest"
	"xata/internal/postgresversions"
	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/reconciler/resources"
	"xata/services/branch-operator/pkg/shared"
)

const (
	testBranchName = "test-branch"
	testImage      = "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.7"
)

func TestClusterSpec(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		cfgModifier func(*resources.ClusterConfig)
		expected    *apiv1ac.ClusterSpecApplyConfiguration
	}{
		{
			name:        "basic - minimal configuration",
			cfgModifier: nil,
			expected:    baseExpectedSpec().WithPlugins(scaleToZeroPlugin(), barmanPlugin(false, map[string]string{"barmanObjectName": testBranchName})),
		},
		{
			name: "instances - number of instances set",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Instances = 3
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin(), barmanPlugin(false, map[string]string{"barmanObjectName": testBranchName})).
				WithInstances(3).
				WithEnablePDB(true),
		},
		{
			name: "enforce zone - pod anti-affinity enabled",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.EnforceZone = true
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin(), barmanPlugin(false, map[string]string{"barmanObjectName": testBranchName})).
				WithAffinity(apiv1ac.AffinityConfiguration().
					WithTopologyKey("topology.kubernetes.io/zone").
					WithPodAntiAffinityType("preferred")),
		},
		{
			name: "storage size - storage size is set",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Storage.Size = "100Gi"
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin(), barmanPlugin(false, map[string]string{"barmanObjectName": testBranchName})).
				WithStorageConfiguration(apiv1ac.StorageConfiguration().
					WithSize("100Gi")),
		},
		{
			name: "storage class - storage class is set",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Storage.StorageClass = new("some-new-storage-class")
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin(), barmanPlugin(false, map[string]string{"barmanObjectName": testBranchName})).
				WithStorageConfiguration(apiv1ac.StorageConfiguration().
					WithSize("10Gi").
					WithStorageClass("some-new-storage-class")),
		},
		{
			name: "volume attributes class - volume attributes class is set",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Storage.VolumeAttributesClass = new("some-volume-attributes-class")
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin(), barmanPlugin(false, map[string]string{"barmanObjectName": testBranchName})).
				WithStorageConfiguration(apiv1ac.StorageConfiguration().
					WithSize("10Gi").
					WithPersistentVolumeClaimTemplate(corev1.PersistentVolumeClaimSpec{
						VolumeAttributesClassName: new("some-volume-attributes-class"),
					})),
		},
		{
			name: "volume snapshot class - volume snapshot class is set",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Storage.VolumeSnapshotClass = new("some-other-snapshot-class")
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin(), barmanPlugin(false, map[string]string{"barmanObjectName": testBranchName})).
				WithBackup(apiv1ac.BackupConfiguration().
					WithVolumeSnapshot(apiv1ac.VolumeSnapshotConfiguration().
						WithClassName("some-other-snapshot-class").
						WithOnline(true).
						WithOnlineConfiguration(apiv1ac.OnlineConfiguration().
							WithImmediateCheckpoint(true)))),
		},
		{
			name: "postgres image - postgres image is set",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Image = "ghcr.io/some/postgres:latest"
			},
			expected: func() *apiv1ac.ClusterSpecApplyConfiguration {
				majorVersion := postgresversions.ExtractMajorVersionFromImage("ghcr.io/some/postgres:latest")
				return baseExpectedSpec().
					WithPlugins(scaleToZeroPlugin(), barmanPlugin(false, map[string]string{"barmanObjectName": testBranchName})).
					WithImageName("ghcr.io/some/postgres:latest").
					WithBootstrap(apiv1ac.BootstrapConfiguration().
						WithInitDB(resources.BootstrapInitDB(testBranchName, majorVersion)))
			}(),
		},
		{
			name: "resources - cpu and memory specified",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Resources = corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("1Gi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("2000m"),
						corev1.ResourceMemory: resource.MustParse("4Gi"),
					},
				}
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin(), barmanPlugin(false, map[string]string{"barmanObjectName": testBranchName})).
				WithResources(corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("1Gi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("2000m"),
						corev1.ResourceMemory: resource.MustParse("4Gi"),
					},
				}),
		},
		{
			name: "labels - custom labels in inherited metadata",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.InheritedMetadata = &v1alpha1.InheritedMetadata{
					Labels: map[string]string{"app": "my-app"},
				}
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin(), barmanPlugin(false, map[string]string{"barmanObjectName": testBranchName})).
				WithInheritedMetadata(apiv1ac.EmbeddedObjectMetadata().
					WithAnnotations(resources.InheritedAnnotations).
					WithLabels(map[string]string{"app": "my-app"})),
		},
		{
			name: "backup configuration - barman plugin enabled when backup schedule is configured",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.BackupSpec = &v1alpha1.BackupSpec{
					Retention: "30d",
					ScheduledBackup: &v1alpha1.ScheduledBackupSpec{
						Schedule: "0 0 0 * * *",
					},
				}
			},
			expected: baseExpectedSpec().WithPlugins(
				scaleToZeroPlugin(),
				barmanPlugin(true, map[string]string{"barmanObjectName": testBranchName}),
			),
		},
		{
			name: "backup configuration - barman plugin enabled when WAL archiving is enabled",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.BackupSpec = &v1alpha1.BackupSpec{
					Retention:    "30d",
					WALArchiving: v1alpha1.WALArchivingModeEnabled,
				}
			},
			expected: baseExpectedSpec().WithPlugins(
				scaleToZeroPlugin(),
				barmanPlugin(true, map[string]string{"barmanObjectName": testBranchName}),
			),
		},
		{
			name: "backup configuration - barman plugin disabled when no WAL archiving or backup schedule configured",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.BackupSpec = &v1alpha1.BackupSpec{
					Retention: "30d",
				}
			},
			expected: baseExpectedSpec().WithPlugins(scaleToZeroPlugin(), barmanPlugin(false, map[string]string{"barmanObjectName": testBranchName})),
		},
		{
			name: "backup configuration - serverName set when different from branch name",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.BackupSpec = &v1alpha1.BackupSpec{
					Retention:    "30d",
					WALArchiving: v1alpha1.WALArchivingModeEnabled,
					ServerName:   "custom-server",
				}
			},
			expected: baseExpectedSpec().WithPlugins(
				scaleToZeroPlugin(),
				barmanPlugin(true, map[string]string{"barmanObjectName": testBranchName, "serverName": "custom-server"}),
			),
		},
		{
			name: "backup configuration - serverName omitted when matching branch name",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.BackupSpec = &v1alpha1.BackupSpec{
					Retention:    "30d",
					WALArchiving: v1alpha1.WALArchivingModeEnabled,
					ServerName:   testBranchName,
				}
			},
			expected: baseExpectedSpec().WithPlugins(
				scaleToZeroPlugin(),
				barmanPlugin(true, map[string]string{"barmanObjectName": testBranchName}),
			),
		},
		{
			name: "node selector - scheduling constraints applied",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Affinity = &v1alpha1.AffinitySpec{
					NodeSelector: map[string]string{
						"node.kubernetes.io/instance-type": "m5.2xlarge",
					},
				}
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin(), barmanPlugin(false, map[string]string{"barmanObjectName": testBranchName})).
				WithAffinity(apiv1ac.AffinityConfiguration().
					WithNodeSelector(map[string]string{
						"node.kubernetes.io/instance-type": "m5.2xlarge",
					})),
		},
		{
			name: "tolerations - pod tolerations for tainted nodes",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Tolerations = []corev1.Toleration{
					{
						Key:      "dedicated",
						Operator: corev1.TolerationOpEqual,
						Value:    "database",
						Effect:   corev1.TaintEffectNoSchedule,
					},
				}
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin(), barmanPlugin(false, map[string]string{"barmanObjectName": testBranchName})).
				WithAffinity(apiv1ac.AffinityConfiguration().
					WithTolerations(corev1.Toleration{
						Key:      "dedicated",
						Operator: corev1.TolerationOpEqual,
						Value:    "database",
						Effect:   corev1.TaintEffectNoSchedule,
					})),
		},
		{
			name: "image pull secrets - multiple secrets configured for private registries",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.ImagePullSecrets = []string{"ghcr-secret", "ecr-secret"}
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin(), barmanPlugin(false, map[string]string{"barmanObjectName": testBranchName})).
				WithImagePullSecrets(
					corev1ac.LocalObjectReference().WithName("ghcr-secret"),
					corev1ac.LocalObjectReference().WithName("ecr-secret"),
				),
		},
		{
			name: "postgres configuration - postgres parameters and shared libraries are set",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Postgres = &v1alpha1.PostgresConfiguration{
					Parameters: []v1alpha1.PostgresParameter{
						{Name: "max_connections", Value: "200"},
					},
					SharedPreloadLibraries: []string{"pg_stat_statements"},
				}
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin(), barmanPlugin(false, map[string]string{"barmanObjectName": testBranchName})).
				WithPostgresConfiguration(apiv1ac.PostgresConfiguration().
					WithParameters(map[string]string{"max_connections": "200"}).
					WithAdditionalLibraries("pg_stat_statements")),
		},
		{
			name: "recovery - recovery from volume snapshot",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.RestoreSpec = &v1alpha1.RestoreSpec{
					Type: v1alpha1.RestoreTypeVolumeSnapshot,
					Name: "some-parent-cluster",
				}
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin(), barmanPlugin(false, map[string]string{"barmanObjectName": testBranchName})).
				WithBootstrap(apiv1ac.BootstrapConfiguration().
					WithRecovery(resources.VolumeSnapshotBootstrapRecovery(testBranchName, "some-parent-cluster"))),
		},
		{
			name: "recovery - recovery from object store (PITR) without timestamp",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.RestoreSpec = &v1alpha1.RestoreSpec{
					Type: v1alpha1.RestoreTypeObjectStore,
					Name: "source-cluster",
				}
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin(), barmanPlugin(false, map[string]string{"barmanObjectName": testBranchName})).
				WithBootstrap(apiv1ac.BootstrapConfiguration().
					WithRecovery(resources.ObjectStoreBootstrapRecovery(testBranchName, &v1alpha1.RestoreSpec{
						Type: v1alpha1.RestoreTypeObjectStore,
						Name: "source-cluster",
					}))).
				WithExternalClusters(resources.ExternalClusters(resources.ClusterConfig{
					RestoreSpec: &v1alpha1.RestoreSpec{
						Type: v1alpha1.RestoreTypeObjectStore,
						Name: "source-cluster",
					},
				})...),
		},
		{
			name: "recovery - recovery from object store (PITR) with timestamp",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				timestamp := metav1.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
				cfg.RestoreSpec = &v1alpha1.RestoreSpec{
					Type:      v1alpha1.RestoreTypeObjectStore,
					Name:      "source-cluster",
					Timestamp: &timestamp,
				}
			},
			expected: func() *apiv1ac.ClusterSpecApplyConfiguration {
				timestamp := metav1.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
				restoreSpec := &v1alpha1.RestoreSpec{
					Type:      v1alpha1.RestoreTypeObjectStore,
					Name:      "source-cluster",
					Timestamp: &timestamp,
				}
				return baseExpectedSpec().
					WithPlugins(scaleToZeroPlugin(), barmanPlugin(false, map[string]string{"barmanObjectName": testBranchName})).
					WithBootstrap(apiv1ac.BootstrapConfiguration().
						WithRecovery(resources.ObjectStoreBootstrapRecovery(testBranchName, restoreSpec))).
					WithExternalClusters(resources.ExternalClusters(resources.ClusterConfig{
						RestoreSpec: restoreSpec,
					})...)
			}(),
		},
		{
			name: "recovery - object store with custom serverName",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.RestoreSpec = &v1alpha1.RestoreSpec{
					Type:       v1alpha1.RestoreTypeObjectStore,
					Name:       "source-cluster",
					ServerName: "some-other-servername",
				}
			},
			expected: func() *apiv1ac.ClusterSpecApplyConfiguration {
				restoreSpec := &v1alpha1.RestoreSpec{
					Type:       v1alpha1.RestoreTypeObjectStore,
					Name:       "source-cluster",
					ServerName: "some-other-servername",
				}
				return baseExpectedSpec().
					WithPlugins(scaleToZeroPlugin(), barmanPlugin(false, map[string]string{"barmanObjectName": testBranchName})).
					WithBootstrap(apiv1ac.BootstrapConfiguration().
						WithRecovery(resources.ObjectStoreBootstrapRecovery(testBranchName, restoreSpec))).
					WithExternalClusters(resources.ExternalClusters(resources.ClusterConfig{
						RestoreSpec: restoreSpec,
					})...)
			}(),
		},
		{
			name: "recovery - xvol clone sets NoOp bootstrap",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.RestoreSpec = &v1alpha1.RestoreSpec{
					Type: v1alpha1.RestoreTypeXVolClone,
					Name: "source-cluster",
				}
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin(), barmanPlugin(false, map[string]string{"barmanObjectName": testBranchName})).
				WithBootstrap(apiv1ac.BootstrapConfiguration().
					WithNoop(apiv1.BootstrapNoop{})),
		},
		{
			name: "pgbackrest backup configuration",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.BackupSpec = &v1alpha1.BackupSpec{
					Method: v1alpha1.BackupMethodPgBackRest,
					PgBackRest: &v1alpha1.PgBackRestSpec{
						S3: &v1alpha1.PgBackRestS3Spec{
							Bucket:  "test-bucket",
							Region:  "us-east-1",
							KeyType: "auto",
						},
						RetentionFullDays:   7,
						CompressType:        "lz4",
						ArchiveAsync:        true,
						ArchivePushQueueMax: "2GiB",
						ArchiveGetQueueMax:  "2GiB",
					},
				}
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin()).
				WithBackup(apiv1ac.BackupConfiguration().
					WithVolumeSnapshot(apiv1ac.VolumeSnapshotConfiguration().
						WithClassName("snapshot-class").
						WithOnline(true).
						WithOnlineConfiguration(apiv1ac.OnlineConfiguration().
							WithImmediateCheckpoint(true))).
					WithPgBackRest(apiv1ac.PgBackRestConfiguration().
						WithStanzaName(testBranchName).
						WithRepository(apiv1ac.PgBackRestRepository().
							WithS3(apiv1ac.PgBackRestS3().
								WithBucket("test-bucket").
								WithRegion("us-east-1").
								WithKeyType("auto"))).
						WithOptions(apiv1ac.PgBackRestOptions().
							WithCompressType("lz4").
							WithArchiveAsync(true).
							WithArchivePushQueueMax("2GiB").
							WithArchiveGetQueueMax("2GiB").
							WithBundle(true).
							WithBlockIncremental(true).
							WithStartFast(true).
							WithDelta(true).
							WithPriority(19).
							WithRetention(apiv1ac.PgBackRestRetention().
								WithFull(7).
								WithFullType("time")))).
					WithTarget(apiv1.BackupTargetStandby)),
		},
		{
			name: "pgbackrest stanza name override (same-name restore)",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.BackupSpec = &v1alpha1.BackupSpec{
					Method: v1alpha1.BackupMethodPgBackRest,
					PgBackRest: &v1alpha1.PgBackRestSpec{
						S3: &v1alpha1.PgBackRestS3Spec{
							Bucket:  "test-bucket",
							Region:  "us-east-1",
							KeyType: "auto",
						},
						RetentionFullDays:   7,
						CompressType:        "lz4",
						ArchiveAsync:        true,
						ArchivePushQueueMax: "2GiB",
						ArchiveGetQueueMax:  "2GiB",
						StanzaName:          "test-branch-restore-1",
					},
				}
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin()).
				WithBackup(apiv1ac.BackupConfiguration().
					WithVolumeSnapshot(apiv1ac.VolumeSnapshotConfiguration().
						WithClassName("snapshot-class").
						WithOnline(true).
						WithOnlineConfiguration(apiv1ac.OnlineConfiguration().
							WithImmediateCheckpoint(true))).
					WithPgBackRest(apiv1ac.PgBackRestConfiguration().
						WithStanzaName("test-branch-restore-1").
						WithRepository(apiv1ac.PgBackRestRepository().
							WithS3(apiv1ac.PgBackRestS3().
								WithBucket("test-bucket").
								WithRegion("us-east-1").
								WithKeyType("auto"))).
						WithOptions(apiv1ac.PgBackRestOptions().
							WithCompressType("lz4").
							WithArchiveAsync(true).
							WithArchivePushQueueMax("2GiB").
							WithArchiveGetQueueMax("2GiB").
							WithBundle(true).
							WithBlockIncremental(true).
							WithStartFast(true).
							WithDelta(true).
							WithPriority(19).
							WithRetention(apiv1ac.PgBackRestRetention().
								WithFull(7).
								WithFullType("time")))).
					WithTarget(apiv1.BackupTargetStandby)),
		},
		{
			name: "pgbackrest backup with rustfs endpoint",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.BackupSpec = &v1alpha1.BackupSpec{
					Method: v1alpha1.BackupMethodPgBackRest,
					PgBackRest: &v1alpha1.PgBackRestSpec{
						S3: &v1alpha1.PgBackRestS3Spec{
							Bucket:   "test-bucket",
							Region:   "us-east-1",
							Endpoint: "http://rustfs.local:9000",
						},
						RetentionFullDays:   7,
						CompressType:        "lz4",
						ArchiveAsync:        true,
						ArchivePushQueueMax: "2GiB",
						ArchiveGetQueueMax:  "2GiB",
					},
				}
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin()).
				WithBackup(apiv1ac.BackupConfiguration().
					WithVolumeSnapshot(apiv1ac.VolumeSnapshotConfiguration().
						WithClassName("snapshot-class").
						WithOnline(true).
						WithOnlineConfiguration(apiv1ac.OnlineConfiguration().
							WithImmediateCheckpoint(true))).
					WithPgBackRest(apiv1ac.PgBackRestConfiguration().
						WithStanzaName(testBranchName).
						WithRepository(apiv1ac.PgBackRestRepository().
							WithS3(apiv1ac.PgBackRestS3().
								WithBucket("test-bucket").
								WithRegion("us-east-1").
								WithEndpoint("http://rustfs.local:9000").
								WithAccessKeyID(machineryapi.SecretKeySelector{
									Name: "rustfs-eu",
									Key:  "accessKey",
								}).
								WithSecretAccessKey(machineryapi.SecretKeySelector{
									Name: "rustfs-eu",
									Key:  "secretKey",
								}))).
						WithOptions(apiv1ac.PgBackRestOptions().
							WithCompressType("lz4").
							WithArchiveAsync(true).
							WithArchivePushQueueMax("2GiB").
							WithArchiveGetQueueMax("2GiB").
							WithBundle(true).
							WithBlockIncremental(true).
							WithStartFast(true).
							WithDelta(true).
							WithPriority(19).
							WithRetention(apiv1ac.PgBackRestRetention().
								WithFull(7).
								WithFullType("time")))).
					WithTarget(apiv1.BackupTargetStandby)),
		},
		{
			name: "pgbackrest backup with S3-compatible endpoint and custom credentials",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.BackupSpec = &v1alpha1.BackupSpec{
					Method: v1alpha1.BackupMethodPgBackRest,
					PgBackRest: &v1alpha1.PgBackRestSpec{
						S3: &v1alpha1.PgBackRestS3Spec{
							Bucket:   "s3://example-backups",
							Region:   "auto",
							Endpoint: "https://s3.example.com",
						},
						RetentionFullDays:   7,
						CompressType:        "lz4",
						ArchiveAsync:        true,
						ArchivePushQueueMax: "2GiB",
						ArchiveGetQueueMax:  "2GiB",
					},
				}
				cfg.BackupCredentials = resources.BackupCredentials{
					SecretName:         "backup-s3-credentials",
					AccessKeyIDKey:     "ACCESS_KEY_ID",
					SecretAccessKeyKey: "SECRET_ACCESS_KEY",
				}
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin()).
				WithBackup(apiv1ac.BackupConfiguration().
					WithVolumeSnapshot(apiv1ac.VolumeSnapshotConfiguration().
						WithClassName("snapshot-class").
						WithOnline(true).
						WithOnlineConfiguration(apiv1ac.OnlineConfiguration().
							WithImmediateCheckpoint(true))).
					WithPgBackRest(apiv1ac.PgBackRestConfiguration().
						WithStanzaName(testBranchName).
						WithRepository(apiv1ac.PgBackRestRepository().
							WithS3(apiv1ac.PgBackRestS3().
								WithBucket("s3://example-backups").
								WithRegion("auto").
								WithEndpoint("https://s3.example.com").
								WithAccessKeyID(machineryapi.SecretKeySelector{
									Name: "backup-s3-credentials",
									Key:  "ACCESS_KEY_ID",
								}).
								WithSecretAccessKey(machineryapi.SecretKeySelector{
									Name: "backup-s3-credentials",
									Key:  "SECRET_ACCESS_KEY",
								}))).
						WithOptions(apiv1ac.PgBackRestOptions().
							WithCompressType("lz4").
							WithArchiveAsync(true).
							WithArchivePushQueueMax("2GiB").
							WithArchiveGetQueueMax("2GiB").
							WithBundle(true).
							WithBlockIncremental(true).
							WithStartFast(true).
							WithDelta(true).
							WithPriority(19).
							WithRetention(apiv1ac.PgBackRestRetention().
								WithFull(7).
								WithFullType("time")))).
					WithTarget(apiv1.BackupTargetStandby)),
		},
		{
			name: "pgbackrest S3 with branch-pinned credentials secret overriding the operator default",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.BackupSpec = &v1alpha1.BackupSpec{
					Method: v1alpha1.BackupMethodPgBackRest,
					PgBackRest: &v1alpha1.PgBackRestSpec{
						S3: &v1alpha1.PgBackRestS3Spec{
							Bucket:                "s3://example-backups",
							Region:                "auto",
							Endpoint:              "https://s3.example.com",
							CredentialsSecretName: "branch-pinned-credentials",
						},
						RetentionFullDays:   7,
						CompressType:        "lz4",
						ArchiveAsync:        true,
						ArchivePushQueueMax: "2GiB",
						ArchiveGetQueueMax:  "2GiB",
					},
				}
				cfg.BackupCredentials = resources.BackupCredentials{
					SecretName:         "backup-s3-credentials",
					AccessKeyIDKey:     "ACCESS_KEY_ID",
					SecretAccessKeyKey: "SECRET_ACCESS_KEY",
				}
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin()).
				WithBackup(apiv1ac.BackupConfiguration().
					WithVolumeSnapshot(apiv1ac.VolumeSnapshotConfiguration().
						WithClassName("snapshot-class").
						WithOnline(true).
						WithOnlineConfiguration(apiv1ac.OnlineConfiguration().
							WithImmediateCheckpoint(true))).
					WithPgBackRest(apiv1ac.PgBackRestConfiguration().
						WithStanzaName(testBranchName).
						WithRepository(apiv1ac.PgBackRestRepository().
							WithS3(apiv1ac.PgBackRestS3().
								WithBucket("s3://example-backups").
								WithRegion("auto").
								WithEndpoint("https://s3.example.com").
								WithAccessKeyID(machineryapi.SecretKeySelector{
									Name: "branch-pinned-credentials",
									Key:  "ACCESS_KEY_ID",
								}).
								WithSecretAccessKey(machineryapi.SecretKeySelector{
									Name: "branch-pinned-credentials",
									Key:  "SECRET_ACCESS_KEY",
								}))).
						WithOptions(apiv1ac.PgBackRestOptions().
							WithCompressType("lz4").
							WithArchiveAsync(true).
							WithArchivePushQueueMax("2GiB").
							WithArchiveGetQueueMax("2GiB").
							WithBundle(true).
							WithBlockIncremental(true).
							WithStartFast(true).
							WithDelta(true).
							WithPriority(19).
							WithRetention(apiv1ac.PgBackRestRetention().
								WithFull(7).
								WithFullType("time")))).
					WithTarget(apiv1.BackupTargetStandby)),
		},
		{
			name: "pgbackrest restore from object store",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.BackupSpec = &v1alpha1.BackupSpec{
					Method: v1alpha1.BackupMethodPgBackRest,
					PgBackRest: &v1alpha1.PgBackRestSpec{
						S3: &v1alpha1.PgBackRestS3Spec{
							Bucket:  "test-bucket",
							Region:  "us-east-1",
							KeyType: "auto",
						},
					},
				}
				cfg.RestoreSpec = &v1alpha1.RestoreSpec{
					Type: v1alpha1.RestoreTypeObjectStore,
					Name: "source-cluster",
				}
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin()).
				WithBackup(apiv1ac.BackupConfiguration().
					WithVolumeSnapshot(apiv1ac.VolumeSnapshotConfiguration().
						WithClassName("snapshot-class").
						WithOnline(true).
						WithOnlineConfiguration(apiv1ac.OnlineConfiguration().
							WithImmediateCheckpoint(true))).
					WithPgBackRest(apiv1ac.PgBackRestConfiguration().
						WithStanzaName(testBranchName).
						WithRepository(apiv1ac.PgBackRestRepository().
							WithS3(apiv1ac.PgBackRestS3().
								WithBucket("test-bucket").
								WithRegion("us-east-1").
								WithKeyType("auto"))).
						WithOptions(apiv1ac.PgBackRestOptions().
							WithCompressType("").
							WithArchiveAsync(false).
							WithArchivePushQueueMax("").
							WithArchiveGetQueueMax("").
							WithBundle(true).
							WithBlockIncremental(true).
							WithStartFast(true).
							WithDelta(true).
							WithPriority(19).
							WithRetention(apiv1ac.PgBackRestRetention().
								WithFull(0).
								WithFullType("time")))).
					WithTarget(apiv1.BackupTargetStandby)).
				WithBootstrap(apiv1ac.BootstrapConfiguration().
					WithRecovery(resources.ObjectStoreBootstrapRecovery(testBranchName, &v1alpha1.RestoreSpec{
						Type: v1alpha1.RestoreTypeObjectStore,
						Name: "source-cluster",
					}))).
				WithExternalClusters(
					apiv1ac.ExternalCluster().
						WithName("source-cluster").
						WithPgBackRest(apiv1ac.PgBackRestExternalCluster().
							WithRepository(apiv1ac.PgBackRestRepository().
								WithS3(apiv1ac.PgBackRestS3().
									WithBucket("test-bucket").
									WithRegion("us-east-1").
									WithKeyType("auto"))).
							WithOptions(apiv1ac.PgBackRestOptions().
								WithRepoPath("source-cluster"))),
				),
		},
		{
			name: "pgbackrest backup with s3 sub-struct",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.BackupSpec = &v1alpha1.BackupSpec{
					Method: v1alpha1.BackupMethodPgBackRest,
					PgBackRest: &v1alpha1.PgBackRestSpec{
						S3: &v1alpha1.PgBackRestS3Spec{
							Bucket:  "test-bucket",
							Region:  "us-east-1",
							KeyType: "auto",
						},
						RetentionFullDays:   7,
						CompressType:        "lz4",
						ArchiveAsync:        true,
						ArchivePushQueueMax: "2GiB",
						ArchiveGetQueueMax:  "2GiB",
					},
				}
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin()).
				WithBackup(apiv1ac.BackupConfiguration().
					WithVolumeSnapshot(apiv1ac.VolumeSnapshotConfiguration().
						WithClassName("snapshot-class").
						WithOnline(true).
						WithOnlineConfiguration(apiv1ac.OnlineConfiguration().
							WithImmediateCheckpoint(true))).
					WithPgBackRest(apiv1ac.PgBackRestConfiguration().
						WithStanzaName(testBranchName).
						WithRepository(apiv1ac.PgBackRestRepository().
							WithS3(apiv1ac.PgBackRestS3().
								WithBucket("test-bucket").
								WithRegion("us-east-1").
								WithKeyType("auto"))).
						WithOptions(apiv1ac.PgBackRestOptions().
							WithCompressType("lz4").
							WithArchiveAsync(true).
							WithArchivePushQueueMax("2GiB").
							WithArchiveGetQueueMax("2GiB").
							WithBundle(true).
							WithBlockIncremental(true).
							WithStartFast(true).
							WithDelta(true).
							WithPriority(19).
							WithRetention(apiv1ac.PgBackRestRetention().
								WithFull(7).
								WithFullType("time")))).
					WithTarget(apiv1.BackupTargetStandby)),
		},
		{
			name: "pgbackrest backup with gcs backend",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.BackupSpec = &v1alpha1.BackupSpec{
					Method: v1alpha1.BackupMethodPgBackRest,
					PgBackRest: &v1alpha1.PgBackRestSpec{
						GCS: &v1alpha1.PgBackRestGCSSpec{
							Bucket:              "test-bucket",
							ServiceAccountEmail: "backups@project.iam.gserviceaccount.com",
							KeyType:             "auto",
						},
						RetentionFullDays:   7,
						CompressType:        "lz4",
						ArchiveAsync:        true,
						ArchivePushQueueMax: "2GiB",
						ArchiveGetQueueMax:  "2GiB",
					},
				}
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin()).
				WithServiceAccountTemplate(apiv1ac.ServiceAccountTemplate().
					WithMetadata(apiv1ac.Metadata().
						WithAnnotations(map[string]string{
							"iam.gke.io/gcp-service-account": "backups@project.iam.gserviceaccount.com",
						}))).
				WithBackup(apiv1ac.BackupConfiguration().
					WithVolumeSnapshot(apiv1ac.VolumeSnapshotConfiguration().
						WithClassName("snapshot-class").
						WithOnline(true).
						WithOnlineConfiguration(apiv1ac.OnlineConfiguration().
							WithImmediateCheckpoint(true))).
					WithPgBackRest(apiv1ac.PgBackRestConfiguration().
						WithStanzaName(testBranchName).
						WithRepository(apiv1ac.PgBackRestRepository().
							WithGCS(apiv1ac.PgBackRestGCS().
								WithBucket("test-bucket").
								WithKeyType("auto"))).
						WithOptions(apiv1ac.PgBackRestOptions().
							WithCompressType("lz4").
							WithArchiveAsync(true).
							WithArchivePushQueueMax("2GiB").
							WithArchiveGetQueueMax("2GiB").
							WithBundle(true).
							WithBlockIncremental(true).
							WithStartFast(true).
							WithDelta(true).
							WithPriority(19).
							WithRetention(apiv1ac.PgBackRestRetention().
								WithFull(7).
								WithFullType("time")))).
					WithTarget(apiv1.BackupTargetStandby)),
		},
		{
			name: "pgbackrest restore from gcs backend",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.BackupSpec = &v1alpha1.BackupSpec{
					Method: v1alpha1.BackupMethodPgBackRest,
					PgBackRest: &v1alpha1.PgBackRestSpec{
						GCS: &v1alpha1.PgBackRestGCSSpec{
							Bucket:              "test-bucket",
							ServiceAccountEmail: "backups@project.iam.gserviceaccount.com",
							KeyType:             "auto",
						},
					},
				}
				cfg.RestoreSpec = &v1alpha1.RestoreSpec{
					Type: v1alpha1.RestoreTypeObjectStore,
					Name: "source-cluster",
				}
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin()).
				WithServiceAccountTemplate(apiv1ac.ServiceAccountTemplate().
					WithMetadata(apiv1ac.Metadata().
						WithAnnotations(map[string]string{
							"iam.gke.io/gcp-service-account": "backups@project.iam.gserviceaccount.com",
						}))).
				WithBackup(apiv1ac.BackupConfiguration().
					WithVolumeSnapshot(apiv1ac.VolumeSnapshotConfiguration().
						WithClassName("snapshot-class").
						WithOnline(true).
						WithOnlineConfiguration(apiv1ac.OnlineConfiguration().
							WithImmediateCheckpoint(true))).
					WithPgBackRest(apiv1ac.PgBackRestConfiguration().
						WithStanzaName(testBranchName).
						WithRepository(apiv1ac.PgBackRestRepository().
							WithGCS(apiv1ac.PgBackRestGCS().
								WithBucket("test-bucket").
								WithKeyType("auto"))).
						WithOptions(apiv1ac.PgBackRestOptions().
							WithCompressType("").
							WithArchiveAsync(false).
							WithArchivePushQueueMax("").
							WithArchiveGetQueueMax("").
							WithBundle(true).
							WithBlockIncremental(true).
							WithStartFast(true).
							WithDelta(true).
							WithPriority(19).
							WithRetention(apiv1ac.PgBackRestRetention().
								WithFull(0).
								WithFullType("time")))).
					WithTarget(apiv1.BackupTargetStandby)).
				WithBootstrap(apiv1ac.BootstrapConfiguration().
					WithRecovery(resources.ObjectStoreBootstrapRecovery(testBranchName, &v1alpha1.RestoreSpec{
						Type: v1alpha1.RestoreTypeObjectStore,
						Name: "source-cluster",
					}))).
				WithExternalClusters(
					apiv1ac.ExternalCluster().
						WithName("source-cluster").
						WithPgBackRest(apiv1ac.PgBackRestExternalCluster().
							WithRepository(apiv1ac.PgBackRestRepository().
								WithGCS(apiv1ac.PgBackRestGCS().
									WithBucket("test-bucket").
									WithKeyType("auto"))).
							WithOptions(apiv1ac.PgBackRestOptions().
								WithRepoPath("source-cluster"))),
				),
		},
		{
			name: "pgbackrest backup with azure backend",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.BackupSpec = &v1alpha1.BackupSpec{
					Method: v1alpha1.BackupMethodPgBackRest,
					PgBackRest: &v1alpha1.PgBackRestSpec{
						Azure: &v1alpha1.PgBackRestAzureSpec{
							Account:   "testaccount",
							Container: "backups",
						},
						RetentionFullDays:   7,
						CompressType:        "lz4",
						ArchiveAsync:        true,
						ArchivePushQueueMax: "2GiB",
						ArchiveGetQueueMax:  "2GiB",
					},
				}
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin()).
				WithBackup(apiv1ac.BackupConfiguration().
					WithVolumeSnapshot(apiv1ac.VolumeSnapshotConfiguration().
						WithClassName("snapshot-class").
						WithOnline(true).
						WithOnlineConfiguration(apiv1ac.OnlineConfiguration().
							WithImmediateCheckpoint(true))).
					WithPgBackRest(apiv1ac.PgBackRestConfiguration().
						WithStanzaName(testBranchName).
						WithRepository(apiv1ac.PgBackRestRepository().
							WithAzure(apiv1ac.PgBackRestAzure().
								WithAccount("testaccount").
								WithContainer("backups").
								WithKeyType("auto"))).
						WithOptions(apiv1ac.PgBackRestOptions().
							WithCompressType("lz4").
							WithArchiveAsync(true).
							WithArchivePushQueueMax("2GiB").
							WithArchiveGetQueueMax("2GiB").
							WithBundle(true).
							WithBlockIncremental(true).
							WithStartFast(true).
							WithDelta(true).
							WithPriority(19).
							WithRetention(apiv1ac.PgBackRestRetention().
								WithFull(7).
								WithFullType("time")))).
					WithTarget(apiv1.BackupTargetStandby)),
		},
		{
			name: "pgbackrest restore from azure backend",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.BackupSpec = &v1alpha1.BackupSpec{
					Method: v1alpha1.BackupMethodPgBackRest,
					PgBackRest: &v1alpha1.PgBackRestSpec{
						Azure: &v1alpha1.PgBackRestAzureSpec{
							Account:   "testaccount",
							Container: "backups",
						},
					},
				}
				cfg.RestoreSpec = &v1alpha1.RestoreSpec{
					Type: v1alpha1.RestoreTypeObjectStore,
					Name: "source-cluster",
				}
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin()).
				WithBackup(apiv1ac.BackupConfiguration().
					WithVolumeSnapshot(apiv1ac.VolumeSnapshotConfiguration().
						WithClassName("snapshot-class").
						WithOnline(true).
						WithOnlineConfiguration(apiv1ac.OnlineConfiguration().
							WithImmediateCheckpoint(true))).
					WithPgBackRest(apiv1ac.PgBackRestConfiguration().
						WithStanzaName(testBranchName).
						WithRepository(apiv1ac.PgBackRestRepository().
							WithAzure(apiv1ac.PgBackRestAzure().
								WithAccount("testaccount").
								WithContainer("backups").
								WithKeyType("auto"))).
						WithOptions(apiv1ac.PgBackRestOptions().
							WithCompressType("").
							WithArchiveAsync(false).
							WithArchivePushQueueMax("").
							WithArchiveGetQueueMax("").
							WithBundle(true).
							WithBlockIncremental(true).
							WithStartFast(true).
							WithDelta(true).
							WithPriority(19).
							WithRetention(apiv1ac.PgBackRestRetention().
								WithFull(0).
								WithFullType("time")))).
					WithTarget(apiv1.BackupTargetStandby)).
				WithBootstrap(apiv1ac.BootstrapConfiguration().
					WithRecovery(resources.ObjectStoreBootstrapRecovery(testBranchName, &v1alpha1.RestoreSpec{
						Type: v1alpha1.RestoreTypeObjectStore,
						Name: "source-cluster",
					}))).
				WithExternalClusters(
					apiv1ac.ExternalCluster().
						WithName("source-cluster").
						WithPgBackRest(apiv1ac.PgBackRestExternalCluster().
							WithRepository(apiv1ac.PgBackRestRepository().
								WithAzure(apiv1ac.PgBackRestAzure().
									WithAccount("testaccount").
									WithContainer("backups").
									WithKeyType("auto"))).
							WithOptions(apiv1ac.PgBackRestOptions().
								WithRepoPath("source-cluster"))),
				),
		},
		{
			name: "smart shutdown timeout - custom value set",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.SmartShutdownTimeout = ptr.To[int32](300)
			},
			expected: baseExpectedSpec().
				WithPlugins(scaleToZeroPlugin(), barmanPlugin(false, map[string]string{"barmanObjectName": testBranchName})).
				WithSmartShutdownTimeout(300),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := baseClusterConfig()
			if tc.cfgModifier != nil {
				tc.cfgModifier(&cfg)
			}

			spec := resources.ClusterSpec(testBranchName, testBranchName, cfg)

			require.Equal(t, tc.expected, spec)
		})
	}
}

// baseClusterConfig provides a base ClusterConfig for tests on which each
// testcase can apply modifications
func baseClusterConfig() resources.ClusterConfig {
	return resources.ClusterConfig{
		Instances: 1,
		Storage: v1alpha1.StorageSpec{
			Size:                "10Gi",
			VolumeSnapshotClass: new("snapshot-class"),
		},
		Image: testImage,
		BackupCredentials: resources.BackupCredentials{
			SecretName:         "rustfs-eu",
			AccessKeyIDKey:     "accessKey",
			SecretAccessKeyKey: "secretKey",
		},
	}
}

func TestClusterSpecPgBackRestCipherReferences(t *testing.T) {
	t.Parallel()

	cfg := baseClusterConfig()
	cfg.BackupSpec = &v1alpha1.BackupSpec{
		Method: v1alpha1.BackupMethodPgBackRest,
		PgBackRest: &v1alpha1.PgBackRestSpec{
			S3: &v1alpha1.PgBackRestS3Spec{
				Bucket: "backup-bucket",
				Region: "eu-west-1",
			},
			CipherPassphraseSecretRef: &corev1.SecretKeySelector{
				Name: "branch-pgbackrest",
				Key:  "cipher-passphrase",
			},
		},
	}
	cfg.RestoreSpec = &v1alpha1.RestoreSpec{
		Type: v1alpha1.RestoreTypeObjectStore,
		Name: "source-branch",
		PgBackRestCipherPassphraseSecretRef: &corev1.SecretKeySelector{
			Name: "branch-pgbackrest",
			Key:  "restore-cipher-passphrase",
		},
	}

	spec := resources.ClusterSpec(testBranchName, testBranchName, cfg)
	targetCipher := spec.Backup.PgBackRest.Repository.Cipher
	require.NotNil(t, targetCipher)
	require.Equal(t, new("aes-256-cbc"), targetCipher.Type)
	require.Equal(t, "branch-pgbackrest", targetCipher.Passphrase.Name)
	require.Equal(t, "cipher-passphrase", targetCipher.Passphrase.Key)

	require.Len(t, spec.ExternalClusters, 1)
	restoreCipher := spec.ExternalClusters[0].PgBackRest.Repository.Cipher
	require.NotNil(t, restoreCipher)
	require.Equal(t, new("aes-256-cbc"), restoreCipher.Type)
	require.Equal(t, "branch-pgbackrest", restoreCipher.Passphrase.Name)
	require.Equal(t, "restore-cipher-passphrase", restoreCipher.Passphrase.Key)
}

func TestClusterSpecAWSS3DoesNotAnnotateClusterServiceAccount(t *testing.T) {
	t.Parallel()

	cfg := baseClusterConfig()
	cfg.BackupSpec = &v1alpha1.BackupSpec{
		Method: v1alpha1.BackupMethodPgBackRest,
		PgBackRest: &v1alpha1.PgBackRestSpec{
			S3: &v1alpha1.PgBackRestS3Spec{
				Bucket:  "backup-bucket",
				Region:  "us-east-1",
				KeyType: "auto",
			},
		},
	}
	cfg.RestoreSpec = &v1alpha1.RestoreSpec{
		Type: v1alpha1.RestoreTypeObjectStore,
		Name: "source-branch",
	}

	spec := resources.ClusterSpec(testBranchName, testBranchName, cfg)
	require.Nil(t, spec.ServiceAccountTemplate)
	require.Equal(t, new("auto"), spec.Backup.PgBackRest.Repository.S3.KeyType)
	require.Equal(t, new("auto"), spec.ExternalClusters[0].PgBackRest.Repository.S3.KeyType)
}

func TestClusterSpecAzureDoesNotAnnotateClusterServiceAccount(t *testing.T) {
	t.Parallel()

	cfg := baseClusterConfig()
	cfg.BackupSpec = &v1alpha1.BackupSpec{
		Method: v1alpha1.BackupMethodPgBackRest,
		PgBackRest: &v1alpha1.PgBackRestSpec{
			Azure: &v1alpha1.PgBackRestAzureSpec{
				Account:   "testaccount",
				Container: "backups",
			},
		},
	}

	spec := resources.ClusterSpec(testBranchName, testBranchName, cfg)
	require.Nil(t, spec.ServiceAccountTemplate)
	require.Equal(t, new("auto"), spec.Backup.PgBackRest.Repository.Azure.KeyType)
}

func TestGeneratePostInitSQL(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name               string
		majorVersion       int
		expectedPrivileges []string
		shouldNotContain   []string
		pgMonitorAdmin     bool
		description        string
	}{
		{
			name:         "pg 14 - base privileges only",
			majorVersion: 14,
			expectedPrivileges: []string{
				"pg_read_all_data",
				"pg_write_all_data",
				"pg_monitor",
				"pg_stat_scan_tables",
				"pg_signal_backend",
				"pg_read_all_settings",
				"pg_read_all_stats",
			},
			shouldNotContain: []string{
				"pg_checkpoint",
				"pg_maintain",
				"pg_create_subscription",
				"pg_use_reserved_connections",
			},
			description: "PG 14 should only have base privileges",
		},
		{
			name:         "pg 15 - base + pg_checkpoint",
			majorVersion: 15,
			expectedPrivileges: []string{
				"pg_read_all_data",
				"pg_write_all_data",
				"pg_monitor",
				"pg_stat_scan_tables",
				"pg_signal_backend",
				"pg_read_all_settings",
				"pg_read_all_stats",
				"pg_checkpoint",
			},
			shouldNotContain: []string{
				"pg_maintain",
				"pg_create_subscription",
				"pg_use_reserved_connections",
			},
			description: "PG 15 should have base privileges plus pg_checkpoint",
		},
		{
			name:         "pg 16 - all privileges",
			majorVersion: 16,
			expectedPrivileges: []string{
				"pg_read_all_data",
				"pg_write_all_data",
				"pg_monitor",
				"pg_stat_scan_tables",
				"pg_signal_backend",
				"pg_read_all_settings",
				"pg_read_all_stats",
				"pg_checkpoint",
				"pg_create_subscription",
				"pg_use_reserved_connections",
			},
			shouldNotContain: []string{},
			pgMonitorAdmin:   true,
			description:      "PG 16 should have all privileges",
		},
		{
			name:         "pg 17 - all privileges",
			majorVersion: 17,
			expectedPrivileges: []string{
				"pg_read_all_data",
				"pg_write_all_data",
				"pg_monitor",
				"pg_stat_scan_tables",
				"pg_signal_backend",
				"pg_read_all_settings",
				"pg_read_all_stats",
				"pg_checkpoint",
				"pg_maintain",
				"pg_create_subscription",
				"pg_use_reserved_connections",
			},
			shouldNotContain: []string{},
			pgMonitorAdmin:   true,
			description:      "PG 17 should have all privileges",
		},
		{
			name:         "pg 18 - all privileges",
			majorVersion: 18,
			expectedPrivileges: []string{
				"pg_read_all_data",
				"pg_write_all_data",
				"pg_monitor",
				"pg_stat_scan_tables",
				"pg_signal_backend",
				"pg_read_all_settings",
				"pg_read_all_stats",
				"pg_checkpoint",
				"pg_maintain",
				"pg_create_subscription",
				"pg_use_reserved_connections",
			},
			shouldNotContain: []string{},
			pgMonitorAdmin:   true,
			description:      "PG 18 should have all privileges",
		},
		{
			name:         "unknown version - defaults to base only",
			majorVersion: 0,
			expectedPrivileges: []string{
				"pg_read_all_data",
				"pg_write_all_data",
				"pg_monitor",
				"pg_stat_scan_tables",
				"pg_signal_backend",
				"pg_read_all_settings",
				"pg_read_all_stats",
			},
			shouldNotContain: []string{
				"pg_checkpoint",
				"pg_maintain",
				"pg_create_subscription",
				"pg_use_reserved_connections",
			},
			description: "Unknown version should default to base privileges only",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sql := resources.GeneratePostInitSQL(tc.majorVersion)

			// Verify basic structure
			require.GreaterOrEqual(t, len(sql), 6, "Should have at least 6 SQL statements")
			require.Contains(t, sql[0], "CREATE ROLE xata_superuser")
			require.Contains(t, sql[1], "GRANT")
			require.Contains(t, sql[2], "GRANT xata_superuser TO xata")

			// Extract GRANT statement and verify privileges
			grantStatement := sql[1]
			for _, privilege := range tc.expectedPrivileges {
				require.Contains(t, grantStatement, privilege, "GRANT statement should contain %s for %s", privilege, tc.description)
			}

			for _, privilege := range tc.shouldNotContain {
				require.NotContains(t, grantStatement, privilege, "GRANT statement should not contain %s for %s", privilege, tc.description)
			}

			adminGrant := "GRANT pg_monitor TO xata_superuser WITH ADMIN OPTION;"
			if tc.pgMonitorAdmin {
				require.Contains(t, sql, adminGrant, tc.description)
			} else {
				require.NotContains(t, sql, adminGrant, tc.description)
			}
		})
	}
}

func scaleToZeroPlugin() *apiv1ac.PluginConfigurationApplyConfiguration {
	return apiv1ac.PluginConfiguration().WithName("cnpg-i-scale-to-zero.xata.io")
}

func barmanPlugin(enabled bool, params map[string]string) *apiv1ac.PluginConfigurationApplyConfiguration {
	return apiv1ac.PluginConfiguration().
		WithName("barman-cloud.cloudnative-pg.io").
		WithEnabled(enabled).
		WithIsWALArchiver(true).
		WithParameters(params)
}

// baseExpectedSpec returns the expected apply configuration for a default
// baseClusterConfig(). Test cases that modify the config should also modify
// the expected spec to match.
func baseExpectedSpec() *apiv1ac.ClusterSpecApplyConfiguration {
	majorVersion := postgresversions.ExtractMajorVersionFromImage(testImage)
	return apiv1ac.ClusterSpec().
		WithInstances(1).
		WithEnablePDB(false).
		WithStorageConfiguration(apiv1ac.StorageConfiguration().
			WithSize("10Gi")).
		WithImageName(testImage).
		WithEnableSuperuserAccess(true).
		WithSuperuserSecret(corev1ac.LocalObjectReference().
			WithName(testBranchName + "-superuser")).
		WithBootstrap(apiv1ac.BootstrapConfiguration().
			WithInitDB(resources.BootstrapInitDB(testBranchName, majorVersion))).
		WithPostgresConfiguration(apiv1ac.PostgresConfiguration()).
		WithResources(corev1.ResourceRequirements{}).
		WithBackup(apiv1ac.BackupConfiguration().
			WithVolumeSnapshot(apiv1ac.VolumeSnapshotConfiguration().
				WithClassName("snapshot-class").
				WithOnline(true).
				WithOnlineConfiguration(apiv1ac.OnlineConfiguration().
					WithImmediateCheckpoint(true)))).
		WithProbes(apiv1ac.ProbesConfiguration().
			WithStartup(apiv1ac.ProbeWithStrategy().
				WithTimeoutSeconds(5).
				WithPeriodSeconds(1).
				WithSuccessThreshold(1).
				WithFailureThreshold(3600))).
		WithManaged(apiv1ac.ManagedConfiguration().
			WithServices(apiv1ac.ManagedServices().
				WithDisabledDefaultServices(
					apiv1.ServiceSelectorTypeR,
					apiv1.ServiceSelectorTypeRO)).
			WithRoles(shared.XataRoleConfiguration(testBranchName))).
		WithMonitoring(apiv1ac.MonitoringConfiguration().
			WithTLSConfig(apiv1ac.ClusterMonitoringTLSConfiguration().
				WithEnabled(true)).
			WithCustomQueriesConfigMap(machineryapi.ConfigMapKeySelector{
				Key:  "metrics.yaml",
				Name: "cnpg-custom-metrics",
			})).
		WithAffinity(apiv1ac.AffinityConfiguration()).
		WithInheritedMetadata(apiv1ac.EmbeddedObjectMetadata().
			WithAnnotations(resources.InheritedAnnotations)).
		WithPrimaryUpdateStrategy(apiv1.PrimaryUpdateStrategy("unsupervised")).
		WithPrimaryUpdateMethod(apiv1.PrimaryUpdateMethod("switchover")).
		WithSmartShutdownTimeout(resources.DefaultSmartShutdownTimeout)
}

// TestExternalClustersPgBackRestRepoPath checks the repo path of the restore
// source. The path has no leading "/", because the instance manager adds one.
func TestExternalClustersPgBackRestRepoPath(t *testing.T) {
	testcases := map[string]struct {
		repoPath string
		want     string
	}{
		"no repo path": {
			want: "source-cluster",
		},
		"defined repo path": {
			repoPath: "system:serviceaccount:xata-clusters:source-cluster-pgbackrest",
			want:     "system:serviceaccount:xata-clusters:source-cluster-pgbackrest",
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			ext := resources.ExternalClusters(resources.ClusterConfig{
				RestoreSpec: &v1alpha1.RestoreSpec{
					Type: v1alpha1.RestoreTypeObjectStore,
					Name: "source-cluster",
				},
				BackupSpec: &v1alpha1.BackupSpec{
					Method: v1alpha1.BackupMethodPgBackRest,
					PgBackRest: &v1alpha1.PgBackRestSpec{
						RepoPath: tc.repoPath,
						S3: &v1alpha1.PgBackRestS3Spec{
							Bucket: "test-bucket",
							Region: "us-east-1",
						},
					},
				},
			})

			require.Len(t, ext, 1)
			require.NotNil(t, ext[0].PgBackRest)
			require.NotNil(t, ext[0].PgBackRest.Options)
			require.NotNil(t, ext[0].PgBackRest.Options.RepoPath)
			require.Equal(t, tc.want, *ext[0].PgBackRest.Options.RepoPath)
		})
	}
}

func TestUsesAWSIAM(t *testing.T) {
	t.Parallel()

	testcases := map[string]struct {
		spec *v1alpha1.BackupSpec
		want bool
	}{
		"no backup spec": {},
		"barman": {
			spec: &v1alpha1.BackupSpec{Method: v1alpha1.BackupMethodBarman},
		},
		"s3 with IAM": {
			spec: &v1alpha1.BackupSpec{
				Method:     v1alpha1.BackupMethodPgBackRest,
				PgBackRest: &v1alpha1.PgBackRestSpec{S3: &v1alpha1.PgBackRestS3Spec{Bucket: "b"}},
			},
			want: true,
		},
		"no storage block": {
			spec: &v1alpha1.BackupSpec{
				Method:     v1alpha1.BackupMethodPgBackRest,
				PgBackRest: &v1alpha1.PgBackRestSpec{},
			},
			want: false,
		},
		"s3-compatible endpoint": {
			spec: &v1alpha1.BackupSpec{
				Method: v1alpha1.BackupMethodPgBackRest,
				PgBackRest: &v1alpha1.PgBackRestSpec{S3: &v1alpha1.PgBackRestS3Spec{
					Bucket: "b", Endpoint: "http://rustfs:9000",
				}},
			},
		},
		"gcs": {
			spec: &v1alpha1.BackupSpec{
				Method:     v1alpha1.BackupMethodPgBackRest,
				PgBackRest: &v1alpha1.PgBackRestSpec{GCS: &v1alpha1.PgBackRestGCSSpec{Bucket: "b"}},
			},
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := resources.UsesAWSIAM(tc.spec)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestClusterSpecPgBackRestGCSWebIdentity(t *testing.T) {
	t.Parallel()

	cfg := baseClusterConfig()
	require.Nil(t, resources.ClusterSpec(testBranchName, "cluster-1", cfg).ProjectedVolumeTemplate)

	cfg.PgBackRestGCSWebIdentity = true
	got := resources.ClusterSpec(testBranchName, "cluster-1", cfg)

	wantVolume, wantEnv := pgbackrest.GCSWebIdentity("cluster-1")
	require.Equal(t, &wantVolume, got.ProjectedVolumeTemplate)
	require.Equal(t, wantEnv, got.Env)
	require.Equal(t, "cluster-1-pgbackrest-web-identity", got.ProjectedVolumeTemplate.Sources[0].Secret.Name)
	require.Equal(t, []corev1.KeyToPath{
		{Key: "token", Path: "pgbackrest/token"},
		{Key: "gcs-credentials.json", Path: "pgbackrest/gcs-credentials.json"},
	}, got.ProjectedVolumeTemplate.Sources[0].Secret.Items)
	require.Equal(t, []corev1.EnvVar{
		{Name: "PGBACKREST_REPO1_GCS_KEY", Value: "/projected/pgbackrest/gcs-credentials.json"},
	}, got.Env)
}

func TestClusterSpecPgBackRestWebIdentity(t *testing.T) {
	t.Parallel()

	const roleARN = "arn:aws:iam::123456789012:role/test-cnpg-backups"

	testcases := map[string]struct {
		roleARN     string
		keyType     string
		wantKeyType string
		wantMount   bool
	}{
		"auto keyType mounts the token but uses the node role": {
			roleARN:     roleARN,
			keyType:     "auto",
			wantKeyType: "auto",
			wantMount:   true,
		},
		"web-id mounts and uses the token of the Branch": {
			roleARN:     roleARN,
			keyType:     pgbackrest.KeyTypeWebID,
			wantKeyType: pgbackrest.KeyTypeWebID,
			wantMount:   true,
		},
		"no role mounts nothing": {
			keyType:     "auto",
			wantKeyType: "auto",
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			cfg := baseClusterConfig()
			cfg.PgBackRestWebIdentityRoleARN = tc.roleARN
			cfg.BackupSpec = &v1alpha1.BackupSpec{
				Method: v1alpha1.BackupMethodPgBackRest,
				PgBackRest: &v1alpha1.PgBackRestSpec{
					S3: &v1alpha1.PgBackRestS3Spec{
						Bucket:  "backup-bucket",
						Region:  "us-east-1",
						KeyType: tc.keyType,
					},
				},
			}

			got := resources.ClusterSpec(testBranchName, "cluster-1", cfg)
			require.Equal(t, new(tc.wantKeyType), got.Backup.PgBackRest.Repository.S3.KeyType)
			if !tc.wantMount {
				require.Nil(t, got.ProjectedVolumeTemplate)
				require.Empty(t, got.Env)
				return
			}

			wantVolume, wantEnv := pgbackrest.WebIdentity("cluster-1", roleARN)
			require.Equal(t, &wantVolume, got.ProjectedVolumeTemplate)
			require.Equal(t, wantEnv, got.Env)
			require.Equal(t, "cluster-1-pgbackrest-web-identity", got.ProjectedVolumeTemplate.Sources[0].Secret.Name)
			require.Equal(t, []corev1.EnvVar{
				{Name: "AWS_ROLE_ARN", Value: roleARN},
				{Name: "AWS_WEB_IDENTITY_TOKEN_FILE", Value: "/projected/pgbackrest/token"},
			}, got.Env)
		})
	}
}

func TestClusterSpecPgBackRestGCSKeyType(t *testing.T) {
	t.Parallel()

	testcases := map[string]struct {
		keyType string
		want    string
	}{
		"auto keyType":   {keyType: "auto", want: "auto"},
		"web-id keyType": {keyType: pgbackrest.KeyTypeWebID, want: pgbackrest.KeyTypeWebID},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			cfg := baseClusterConfig()
			cfg.BackupSpec = &v1alpha1.BackupSpec{
				Method: v1alpha1.BackupMethodPgBackRest,
				PgBackRest: &v1alpha1.PgBackRestSpec{
					GCS: &v1alpha1.PgBackRestGCSSpec{
						Bucket:              "backup-bucket",
						ServiceAccountEmail: "backups@project.iam.gserviceaccount.com",
						KeyType:             tc.keyType,
					},
				},
			}

			got := resources.ClusterSpec(testBranchName, "cluster-1", cfg)
			require.Equal(t, new(tc.want), got.Backup.PgBackRest.Repository.GCS.KeyType)
		})
	}
}

func TestClusterSpecPgBackRestGCSServiceAccountEmail(t *testing.T) {
	t.Parallel()

	cfg := baseClusterConfig()
	cfg.BackupSpec = &v1alpha1.BackupSpec{
		Method: v1alpha1.BackupMethodPgBackRest,
		PgBackRest: &v1alpha1.PgBackRestSpec{
			GCS: &v1alpha1.PgBackRestGCSSpec{Bucket: "backup-bucket"},
		},
	}
	require.Nil(t, resources.ClusterSpec(testBranchName, "cluster-1", cfg).ServiceAccountTemplate)

	cfg.PgBackRest.GCS.ServiceAccountEmail = "backups@project.iam.gserviceaccount.com"
	got := resources.ClusterSpec(testBranchName, "cluster-1", cfg)
	require.Equal(t, map[string]string{
		"iam.gke.io/gcp-service-account": "backups@project.iam.gserviceaccount.com",
	}, got.ServiceAccountTemplate.Metadata.Annotations)
}
