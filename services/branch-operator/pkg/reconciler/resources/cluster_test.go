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

	"xata/internal/postgresversions"
	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/reconciler/resources"
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
			expected:    baseExpectedSpec(),
		},
		{
			name: "instances - number of instances set",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Instances = 3
			},
			expected: baseExpectedSpec().
				WithInstances(3).
				WithEnablePDB(true),
		},
		{
			name: "enforce zone - pod anti-affinity enabled",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.EnforceZone = true
			},
			expected: baseExpectedSpec().
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
				WithStorageConfiguration(apiv1ac.StorageConfiguration().
					WithSize("100Gi")),
		},
		{
			name: "storage class - storage class is set",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Storage.StorageClass = new("some-new-storage-class")
			},
			expected: baseExpectedSpec().
				WithStorageConfiguration(apiv1ac.StorageConfiguration().
					WithSize("10Gi").
					WithStorageClass("some-new-storage-class")),
		},
		{
			name: "volume snapshot class - volume snapshot class is set",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Storage.VolumeSnapshotClass = new("some-other-snapshot-class")
			},
			expected: baseExpectedSpec().
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
			expected: baseExpectedSpecWithPlugins(
				apiv1ac.PluginConfiguration().
					WithName("cnpg-i-scale-to-zero.xata.io"),
				apiv1ac.PluginConfiguration().
					WithName("barman-cloud.cloudnative-pg.io").
					WithEnabled(true).
					WithIsWALArchiver(true).
					WithParameters(map[string]string{
						"barmanObjectName": testBranchName,
					}),
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
			expected: baseExpectedSpecWithPlugins(
				apiv1ac.PluginConfiguration().
					WithName("cnpg-i-scale-to-zero.xata.io"),
				apiv1ac.PluginConfiguration().
					WithName("barman-cloud.cloudnative-pg.io").
					WithEnabled(true).
					WithIsWALArchiver(true).
					WithParameters(map[string]string{
						"barmanObjectName": testBranchName,
					}),
			),
		},
		{
			name: "backup configuration - barman plugin disabled when no WAL archiving or backup schedule configured",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.BackupSpec = &v1alpha1.BackupSpec{
					Retention: "30d",
				}
			},
			expected: baseExpectedSpec(),
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
			expected: baseExpectedSpecWithPlugins(
				apiv1ac.PluginConfiguration().
					WithName("cnpg-i-scale-to-zero.xata.io"),
				apiv1ac.PluginConfiguration().
					WithName("barman-cloud.cloudnative-pg.io").
					WithEnabled(true).
					WithIsWALArchiver(true).
					WithParameters(map[string]string{
						"barmanObjectName": testBranchName,
						"serverName":       "custom-server",
					}),
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
			expected: baseExpectedSpecWithPlugins(
				apiv1ac.PluginConfiguration().
					WithName("cnpg-i-scale-to-zero.xata.io"),
				apiv1ac.PluginConfiguration().
					WithName("barman-cloud.cloudnative-pg.io").
					WithEnabled(true).
					WithIsWALArchiver(true).
					WithParameters(map[string]string{
						"barmanObjectName": testBranchName,
					}),
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
					WithBootstrap(apiv1ac.BootstrapConfiguration().
						WithRecovery(resources.ObjectStoreBootstrapRecovery(testBranchName, restoreSpec))).
					WithExternalClusters(resources.ExternalClusters(resources.ClusterConfig{
						RestoreSpec: restoreSpec,
					})...)
			}(),
		},
		{
			name: "smart shutdown timeout - custom value set",
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.SmartShutdownTimeout = ptr.To[int32](300)
			},
			expected: baseExpectedSpec().
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
		ClusterSpec: v1alpha1.ClusterSpec{
			Instances: 1,
			Storage: v1alpha1.StorageSpec{
				Size:                "10Gi",
				VolumeSnapshotClass: new("snapshot-class"),
			},
			Image: testImage,
		},
	}
}

func TestGeneratePostInitSQL(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name               string
		majorVersion       int
		expectedPrivileges []string
		shouldNotContain   []string
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
		})
	}
}

// baseExpectedSpecWithPlugins returns baseExpectedSpec with the plugins
// replaced by the given values.
func baseExpectedSpecWithPlugins(plugins ...*apiv1ac.PluginConfigurationApplyConfiguration) *apiv1ac.ClusterSpecApplyConfiguration {
	spec := baseExpectedSpec()
	spec.Plugins = nil
	return spec.WithPlugins(plugins...)
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
			WithName(testBranchName+"-superuser")).
		WithBootstrap(apiv1ac.BootstrapConfiguration().
			WithInitDB(resources.BootstrapInitDB(testBranchName, majorVersion))).
		WithPostgresConfiguration(apiv1ac.PostgresConfiguration()).
		WithPlugins(
			apiv1ac.PluginConfiguration().
				WithName("cnpg-i-scale-to-zero.xata.io"),
			apiv1ac.PluginConfiguration().
				WithName("barman-cloud.cloudnative-pg.io").
				WithEnabled(false).
				WithIsWALArchiver(true).
				WithParameters(map[string]string{
					"barmanObjectName": testBranchName,
				}),
		).
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
					apiv1.ServiceSelectorTypeRO))).
		WithMonitoring(apiv1ac.MonitoringConfiguration().
			WithTLSConfig(apiv1ac.ClusterMonitoringTLSConfiguration().
				WithEnabled(true)).
			WithCustomQueriesConfigMap(machineryapi.ConfigMapKeySelector{
				Key: "metrics.yaml",
				LocalObjectReference: machineryapi.LocalObjectReference{
					Name: "cnpg-custom-metrics",
				},
			})).
		WithAffinity(apiv1ac.AffinityConfiguration()).
		WithInheritedMetadata(apiv1ac.EmbeddedObjectMetadata().
			WithAnnotations(resources.InheritedAnnotations)).
		WithPrimaryUpdateStrategy(apiv1.PrimaryUpdateStrategy("unsupervised")).
		WithPrimaryUpdateMethod(apiv1.PrimaryUpdateMethod("switchover")).
		WithSmartShutdownTimeout(resources.DefaultSmartShutdownTimeout)
}
