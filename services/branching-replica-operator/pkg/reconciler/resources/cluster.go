package resources

import (
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	apiv1ac "github.com/xataio/xata-cnpg/pkg/client/applyconfiguration/api/v1"
	corev1 "k8s.io/api/core/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"

	"xata/services/branching-replica-operator/api/v1alpha1"
)

const (
	BranchingReplicaLabel      = "xata.io/branching-replica"
	WALGPluginName             = "cnpg-extensions.yandex.cloud"
	walgBackupConfigParameter  = "backupConfig"
	walgS3PrefixEnv            = "WALG_S3_PREFIX"
	externalClusterName        = "upstream"
	SkipWALArchivingAnnotation = "cnpg.io/skipWalArchiving" //nolint:gosec
	awsSessionTokenEnv         = "AWS_SESSION_TOKEN"        //nolint:gosec
)

// ClusterConfig defines all the configuration needed to build a CNPG Cluster
// resource. It combines the user-defined config from the BranchingReplica
// spec together with environmental config defined in the operator itself
type ClusterConfig struct {
	v1alpha1.BranchingReplicaSpec
	Tolerations      []corev1.Toleration
	ImagePullSecrets []string
}

// ClusterLabels returns the labels of the Cluster of the named
// BranchingReplica
func ClusterLabels(replicaName string) map[string]string {
	return map[string]string{
		BranchingReplicaLabel: replicaName,
	}
}

// ClusterAnnotations returns the annotations of the Cluster of a
// BranchingReplica
func ClusterAnnotations() map[string]string {
	return map[string]string{
		SkipWALArchivingAnnotation: "enabled",
	}
}

// ClusterSpec generates the CNPG Cluster spec apply configuration from the
// provided configuration. The result is a replica cluster with one instance
// that bootstraps from the latest base backup in the source's archive and
// then follows the archive. backupConfigName is the name of the BackupConfig
// that describes the archive.
func ClusterSpec(
	backupConfigName string,
	cfg ClusterConfig,
) *apiv1ac.ClusterSpecApplyConfiguration {
	var imagePullSecrets []*corev1ac.LocalObjectReferenceApplyConfiguration
	for _, secretName := range cfg.ImagePullSecrets {
		imagePullSecrets = append(imagePullSecrets,
			corev1ac.LocalObjectReference().WithName(secretName))
	}

	storage := apiv1ac.StorageConfiguration().
		WithSize(cfg.Storage.Size)
	if cfg.Storage.StorageClass != nil {
		storage = storage.WithStorageClass(*cfg.Storage.StorageClass)
	}

	return apiv1ac.ClusterSpec().
		WithInstances(1).
		WithImageName(cfg.Postgres.Image).
		WithImagePullSecrets(imagePullSecrets...).
		WithAffinity(apiv1ac.AffinityConfiguration().
			WithTolerations(cfg.Tolerations...)).
		WithStorageConfiguration(storage).
		WithResources(cfg.Postgres.Resources).
		WithPostgresConfiguration(apiv1ac.PostgresConfiguration().
			WithParameters(cfg.Postgres.Parameters)).
		WithBootstrap(apiv1ac.BootstrapConfiguration().
			WithRecovery(apiv1ac.BootstrapRecovery().
				WithSource(externalClusterName))).
		WithReplicaCluster(apiv1ac.ReplicaClusterConfiguration().
			WithEnabled(true).
			WithSource(externalClusterName)).
		WithExternalClusters(apiv1ac.ExternalCluster().
			WithName(externalClusterName).
			WithPluginConfiguration(walgPlugin(backupConfigName))).
		WithPlugins(walgPlugin(backupConfigName).
			WithEnabled(true)).
		WithEnv(sourceEnv(cfg.Source)...).
		WithManaged(apiv1ac.ManagedConfiguration().
			WithServices(apiv1ac.ManagedServices().
				WithDisabledDefaultServices(
					apiv1.ServiceSelectorTypeRO,
					apiv1.ServiceSelectorTypeR,
				)))
}

// walgPlugin returns the plugin configuration that points the WAL-G plugin
// at the named BackupConfig
func walgPlugin(backupConfigName string) *apiv1ac.PluginConfigurationApplyConfiguration {
	return apiv1ac.PluginConfiguration().
		WithName(WALGPluginName).
		WithParameters(map[string]string{
			walgBackupConfigParameter: backupConfigName,
		})
}

// sourceEnv returns the environment variables that WAL-G needs to read the
// source's archive, beyond what the plugin takes from the BackupConfig. CNPG
// sets them on the postgres container and the plugin copies that container's
// environment into its sidecar.
func sourceEnv(source v1alpha1.SourceSpec) []corev1.EnvVar {
	env := []corev1.EnvVar{{
		Name:  walgS3PrefixEnv,
		Value: source.S3.Prefix,
	}}

	if source.S3.SessionTokenRef != nil {
		env = append(env, corev1.EnvVar{
			Name: awsSessionTokenEnv,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: source.S3.SessionTokenRef,
			},
		})
	}

	return env
}
