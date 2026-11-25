package resources_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	apiv1ac "github.com/xataio/xata-cnpg/pkg/client/applyconfiguration/api/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"

	"xata/services/branching-replica-operator/api/v1alpha1"
	"xata/services/branching-replica-operator/pkg/reconciler/resources"
)

const (
	testReplicaName = "test-replica"
	testImage       = "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:18.7"
	testPrefix      = "s3://test-bucket/archive"
)

func TestClusterSpec(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		cfgModifier func(*resources.ClusterConfig)
		want        *apiv1ac.ClusterSpecApplyConfiguration
	}{
		"minimal configuration": {
			cfgModifier: nil,
			want:        baseClusterSpec(),
		},
		"image pull secrets": {
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.ImagePullSecrets = []string{"ghcr-secret", "other-secret"}
			},
			want: baseClusterSpec().
				WithImagePullSecrets(
					corev1ac.LocalObjectReference().WithName("ghcr-secret"),
					corev1ac.LocalObjectReference().WithName("other-secret"),
				),
		},
		"tolerations": {
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Tolerations = []corev1.Toleration{
					{
						Key:    "xata.io/workload",
						Value:  "dataplane",
						Effect: corev1.TaintEffectNoSchedule,
					},
				}
			},
			want: baseClusterSpec().
				WithAffinity(apiv1ac.AffinityConfiguration().
					WithTolerations(corev1.Toleration{
						Key:    "xata.io/workload",
						Value:  "dataplane",
						Effect: corev1.TaintEffectNoSchedule,
					})),
		},
		"storage class": {
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Storage.StorageClass = new("some-storage-class")
			},
			want: baseClusterSpec().
				WithStorageConfiguration(apiv1ac.StorageConfiguration().
					WithSize("10Gi").
					WithStorageClass("some-storage-class")),
		},
		"postgres resources": {
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Postgres.Resources = corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("250m"),
						corev1.ResourceMemory: resource.MustParse("512Mi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("1"),
						corev1.ResourceMemory: resource.MustParse("512Mi"),
					},
				}
			},
			want: baseClusterSpec().
				WithResources(corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("250m"),
						corev1.ResourceMemory: resource.MustParse("512Mi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("1"),
						corev1.ResourceMemory: resource.MustParse("512Mi"),
					},
				}),
		},
		"postgres parameters": {
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Postgres.Parameters = map[string]string{
					"max_connections":      "500",
					"max_worker_processes": "32",
				}
			},
			want: baseClusterSpec().
				WithPostgresConfiguration(apiv1ac.PostgresConfiguration().
					WithParameters(map[string]string{
						"max_connections":      "500",
						"max_worker_processes": "32",
					})),
		},
		"session token": {
			cfgModifier: func(cfg *resources.ClusterConfig) {
				cfg.Source.S3.SessionTokenRef = &corev1.SecretKeySelector{
					Name: "test-credentials",
					Key:  "AWS_SESSION_TOKEN",
				}
			},
			want: baseClusterSpec().
				WithEnv(corev1.EnvVar{
					Name: "AWS_SESSION_TOKEN",
					ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							Name: "test-credentials",
							Key:  "AWS_SESSION_TOKEN",
						},
					},
				}),
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			cfg := baseClusterConfig()
			if tt.cfgModifier != nil {
				tt.cfgModifier(&cfg)
			}

			got := resources.ClusterSpec(testReplicaName, cfg)

			require.Equal(t, tt.want, got)
		})
	}
}

func TestClusterLabels(t *testing.T) {
	t.Parallel()

	want := map[string]string{
		"xata.io/branching-replica": testReplicaName,
	}

	got := resources.ClusterLabels(testReplicaName)

	require.Equal(t, want, got)
}

func TestClusterAnnotations(t *testing.T) {
	t.Parallel()

	want := map[string]string{
		"cnpg.io/skipWalArchiving": "enabled",
	}

	got := resources.ClusterAnnotations()

	require.Equal(t, want, got)
}

// baseClusterConfig returns a ClusterConfig with only the required fields of
// a BranchingReplica spec set
func baseClusterConfig() resources.ClusterConfig {
	return resources.ClusterConfig{
		Source: v1alpha1.SourceSpec{
			S3: v1alpha1.S3Spec{
				Prefix: testPrefix,
				AccessKeyIDRef: corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "test-credentials"},
					Key:                  "AWS_ACCESS_KEY_ID",
				},
				AccessKeySecretRef: corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "test-credentials"},
					Key:                  "AWS_SECRET_ACCESS_KEY",
				},
			},
		},
		Postgres: v1alpha1.PostgresSpec{
			Image: testImage,
		},
		Storage: v1alpha1.StorageSpec{
			Size: "10Gi",
		},
	}
}

// baseClusterSpec returns the expected apply configuration for a default
// baseClusterConfig()
func baseClusterSpec() *apiv1ac.ClusterSpecApplyConfiguration {
	return apiv1ac.ClusterSpec().
		WithInstances(1).
		WithImageName(testImage).
		WithAffinity(apiv1ac.AffinityConfiguration()).
		WithStorageConfiguration(apiv1ac.StorageConfiguration().
			WithSize("10Gi")).
		WithResources(corev1.ResourceRequirements{}).
		WithPostgresConfiguration(apiv1ac.PostgresConfiguration()).
		WithBootstrap(apiv1ac.BootstrapConfiguration().
			WithRecovery(apiv1ac.BootstrapRecovery().
				WithSource("upstream"))).
		WithReplicaCluster(apiv1ac.ReplicaClusterConfiguration().
			WithEnabled(true).
			WithSource("upstream")).
		WithExternalClusters(apiv1ac.ExternalCluster().
			WithName("upstream").
			WithPluginConfiguration(apiv1ac.PluginConfiguration().
				WithName("cnpg-extensions.yandex.cloud").
				WithParameters(map[string]string{
					"backupConfig": testReplicaName,
				}))).
		WithPlugins(apiv1ac.PluginConfiguration().
			WithName("cnpg-extensions.yandex.cloud").
			WithEnabled(true).
			WithParameters(map[string]string{
				"backupConfig": testReplicaName,
			})).
		WithEnv(corev1.EnvVar{
			Name:  "WALG_S3_PREFIX",
			Value: testPrefix,
		}).
		WithManaged(apiv1ac.ManagedConfiguration().
			WithServices(apiv1ac.ManagedServices().
				WithDisabledDefaultServices(
					apiv1.ServiceSelectorTypeRO,
					apiv1.ServiceSelectorTypeR,
				)))
}
