package resources

import (
	"fmt"
	"strings"

	machineryapi "github.com/cloudnative-pg/machinery/pkg/api"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	apiv1ac "github.com/xataio/xata-cnpg/pkg/client/applyconfiguration/api/v1"
	corev1 "k8s.io/api/core/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"

	"xata/internal/postgresversions"
	"xata/services/branch-operator/api/v1alpha1"
)

// InheritedAnnotations are defined on the Cluster; CNPG will propagate them to
// all resources it creates
var InheritedAnnotations = map[string]string{
	// TLS is enabled on the metrics endpoint
	"prometheus.io/scheme": "https",

	// Mark cluster services as Cilium global services
	"service.cilium.io/global": "true",
}

// PostgreSQL version-specific privileges for xata_superuser role
var (
	// pg14Privileges contains privileges available in PostgreSQL 14 and earlier
	pg14Privileges = []string{
		"pg_read_all_data",
		"pg_write_all_data",
		"pg_monitor",
		"pg_stat_scan_tables",
		"pg_signal_backend",
		"pg_read_all_settings",
		"pg_read_all_stats",
	}

	// pg15Privileges contains privileges available in PostgreSQL 15 (adds pg_checkpoint)
	pg15Privileges = []string{
		"pg_read_all_data",
		"pg_write_all_data",
		"pg_monitor",
		"pg_stat_scan_tables",
		"pg_signal_backend",
		"pg_checkpoint",
		"pg_read_all_settings",
		"pg_read_all_stats",
	}

	// pg16Privileges contains privileges available in PostgreSQL 16 (adds pg_create_subscription, pg_use_reserved_connections)
	pg16Privileges = []string{
		"pg_read_all_data",
		"pg_write_all_data",
		"pg_monitor",
		"pg_stat_scan_tables",
		"pg_signal_backend",
		"pg_checkpoint",
		"pg_read_all_settings",
		"pg_read_all_stats",
		"pg_create_subscription",
		"pg_use_reserved_connections",
	}

	// pg17Privileges contains privileges available in PostgreSQL 17+ (adds pg_maintain)
	pg17Privileges = []string{
		"pg_read_all_data",
		"pg_write_all_data",
		"pg_maintain",
		"pg_monitor",
		"pg_stat_scan_tables",
		"pg_signal_backend",
		"pg_checkpoint",
		"pg_read_all_settings",
		"pg_read_all_stats",
		"pg_create_subscription",
		"pg_use_reserved_connections",
	}
)

// GeneratePostInitSQL generates PostInitSQL statements based on PostgreSQL major version
func GeneratePostInitSQL(majorVersion int) []string {
	// Select privileges based on version
	var privileges []string
	switch {
	case majorVersion >= 17:
		privileges = pg17Privileges
	case majorVersion >= 16:
		privileges = pg16Privileges
	case majorVersion >= 15:
		privileges = pg15Privileges
	default:
		// PG 14 or unknown version - use base privileges
		privileges = pg14Privileges
	}

	grantStatement := fmt.Sprintf("GRANT %s TO xata_superuser;", strings.Join(privileges, ", "))

	return []string{
		// These are executed as the superuser.
		// Create a pseudo-superuser role that we then assign to the `xata` user.
		"CREATE ROLE xata_superuser NOLOGIN;",
		grantStatement,
		// make `xata` the pseudo-superuser
		"GRANT xata_superuser TO xata;",
		// Allow the xata role to own schemas in the `postgres` database, but not the database itself, so it cannot drop it.
		"ALTER ROLE xata LOGIN INHERIT CREATEDB CREATEROLE BYPASSRLS REPLICATION;",
		"ALTER SCHEMA public OWNER TO xata;",
		"GRANT CREATE ON DATABASE postgres TO xata;",
	}
}

// ClusterConfig defines all the configuration needed to build a CNPG Cluster
// resource. It combines the user-defined config from the Branch spec together
// with environmental config defined in the operator
type ClusterConfig struct {
	v1alpha1.ClusterSpec
	*v1alpha1.BackupSpec
	InheritedMetadata *v1alpha1.InheritedMetadata
	RestoreSpec       *v1alpha1.RestoreSpec
	Tolerations       []corev1.Toleration
	EnforceZone       bool
	ImagePullSecrets  []string
}

// ClusterSpec generates the CNPG Cluster spec apply configuration from the
// provided configuration
func ClusterSpec(
	branchName string,
	clusterName string,
	cfg ClusterConfig,
) *apiv1ac.ClusterSpecApplyConfiguration {
	// Build image pull secrets
	var imagePullSecrets []*corev1ac.LocalObjectReferenceApplyConfiguration
	for _, secretName := range cfg.ImagePullSecrets {
		imagePullSecrets = append(imagePullSecrets,
			corev1ac.LocalObjectReference().WithName(secretName))
	}

	// Build Bootstrap configuration based on restore type
	var bootstrap *apiv1ac.BootstrapConfigurationApplyConfiguration
	var restoreType v1alpha1.RestoreType
	if cfg.RestoreSpec != nil {
		restoreType = cfg.RestoreSpec.Type
	}

	switch restoreType {
	case v1alpha1.RestoreTypeVolumeSnapshot:
		bootstrap = apiv1ac.BootstrapConfiguration().
			WithRecovery(VolumeSnapshotBootstrapRecovery(branchName, cfg.RestoreSpec.Name))
	case v1alpha1.RestoreTypeObjectStore:
		bootstrap = apiv1ac.BootstrapConfiguration().
			WithRecovery(ObjectStoreBootstrapRecovery(branchName, cfg.RestoreSpec))
	default:
		majorVersion := postgresversions.ExtractMajorVersionFromImage(cfg.Image)
		bootstrap = apiv1ac.BootstrapConfiguration().
			WithInitDB(BootstrapInitDB(branchName, majorVersion))
	}

	// Build external clusters for object store recovery
	externalClusters := ExternalClusters(cfg)

	// Build affinity configuration
	affinity := apiv1ac.AffinityConfiguration().
		WithNodeSelector(cfg.Affinity.GetNodeSelector()).
		WithTolerations(cfg.Tolerations...)
	if cfg.EnforceZone {
		affinity = affinity.
			WithPodAntiAffinityType("preferred").
			WithTopologyKey("topology.kubernetes.io/zone")
	}

	// Build storage configuration
	storage := apiv1ac.StorageConfiguration().
		WithSize(cfg.Storage.Size)
	if cfg.Storage.StorageClass != nil {
		storage = storage.WithStorageClass(*cfg.Storage.StorageClass)
	}
	if cfg.Storage.MountPropagation != nil {
		storage = storage.WithMountPropagation(corev1.MountPropagationMode(*cfg.Storage.MountPropagation))
	}

	instances := int(cfg.Instances)
	spec := apiv1ac.ClusterSpec().
		WithInstances(instances).
		WithEnablePDB(instances > 1).
		WithStorageConfiguration(storage).
		WithImageName(cfg.Image).
		WithImagePullSecrets(imagePullSecrets...).
		WithEnableSuperuserAccess(true).
		WithSuperuserSecret(corev1ac.LocalObjectReference().
			WithName(branchName+"-superuser")).
		WithBootstrap(bootstrap).
		WithPostgresConfiguration(apiv1ac.PostgresConfiguration().
			WithParameters(PostgresParametersToMap(cfg.Postgres)).
			WithAdditionalLibraries(cfg.Postgres.GetSharedPreloadLibraries()...)).
		WithPlugins(
			apiv1ac.PluginConfiguration().
				WithName("cnpg-i-scale-to-zero.xata.io"),
			apiv1ac.PluginConfiguration().
				WithName("barman-cloud.cloudnative-pg.io").
				WithEnabled(cfg.RequiresBarmanPlugin()).
				WithIsWALArchiver(true).
				WithParameters(BarmanPluginParameters(branchName, cfg.GetServerName())),
		).
		WithResources(cfg.Resources).
		WithBackup(apiv1ac.BackupConfiguration().
			WithVolumeSnapshot(apiv1ac.VolumeSnapshotConfiguration().
				WithClassName(cfg.Storage.GetVolumeSnapshotClass()).
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
		WithAffinity(affinity).
		WithInheritedMetadata(apiv1ac.EmbeddedObjectMetadata().
			WithAnnotations(InheritedAnnotations).
			WithLabels(LabelsFromInheritedMetadata(cfg.InheritedMetadata))).
		WithPrimaryUpdateStrategy(apiv1.PrimaryUpdateStrategy("unsupervised")).
		WithPrimaryUpdateMethod(apiv1.PrimaryUpdateMethod("switchover")).
		WithExternalClusters(externalClusters...).
		WithSmartShutdownTimeout(smartShutdownTimeout(cfg.SmartShutdownTimeout))

	return spec
}

// BarmanPluginParameters builds the parameter map for the barman-cloud plugin.
func BarmanPluginParameters(branchName, serverName string) map[string]string {
	params := map[string]string{
		"barmanObjectName": branchName,
	}
	if serverName != "" && serverName != branchName {
		params["serverName"] = serverName
	}
	return params
}

// VolumeSnapshotBootstrapRecovery creates a BootstrapRecovery apply configuration
// using the provided branch and target cluster names.
func VolumeSnapshotBootstrapRecovery(branchName, targetBranchName string) *apiv1ac.BootstrapRecoveryApplyConfiguration {
	return apiv1ac.BootstrapRecovery().
		WithOwner("xata").
		WithDatabase("xata").
		WithSecret(corev1ac.LocalObjectReference().
			WithName(branchName + "-app")).
		WithVolumeSnapshots(apiv1ac.DataSource().
			WithStorage(corev1ac.TypedLocalObjectReference().
				WithName(targetBranchName + "-" + branchName).
				WithKind("VolumeSnapshot").
				WithAPIGroup("snapshot.storage.k8s.io"))).
		WithRecoveryTarget(apiv1ac.RecoveryTarget().
			WithTargetImmediate(true))
}

// ObjectStoreBootstrapRecovery creates a BootstrapRecovery apply configuration
// for point-in-time recovery from object storage.
func ObjectStoreBootstrapRecovery(branchName string, restoreSpec *v1alpha1.RestoreSpec) *apiv1ac.BootstrapRecoveryApplyConfiguration {
	recovery := apiv1ac.BootstrapRecovery().
		WithOwner("xata").
		WithDatabase("xata").
		WithSecret(corev1ac.LocalObjectReference().
			WithName(branchName + "-app")).
		WithSource(restoreSpec.Name)

	if restoreSpec.Timestamp != nil {
		recovery = recovery.WithRecoveryTarget(apiv1ac.RecoveryTarget().
			WithTargetTime(restoreSpec.Timestamp.Format("2006-01-02 15:04:05.000000")))
	}

	return recovery
}

// ExternalClusters creates the ExternalClusters apply configuration for
// point-in-time recovery from object storage using barman-cloud plugin.
// Returns nil if the restore spec is not for ObjectStore type.
func ExternalClusters(cfg ClusterConfig) []*apiv1ac.ExternalClusterApplyConfiguration {
	if !cfg.RestoreSpec.IsObjectStoreType() {
		return nil
	}

	pluginParams := map[string]string{
		"barmanObjectName": cfg.RestoreSpec.Name,
		"serverName":       cfg.RestoreSpec.GetServerName(),
	}

	return []*apiv1ac.ExternalClusterApplyConfiguration{
		apiv1ac.ExternalCluster().
			WithName(cfg.RestoreSpec.Name).
			WithPluginConfiguration(apiv1ac.PluginConfiguration().
				WithName("barman-cloud.cloudnative-pg.io").
				WithParameters(pluginParams)),
	}
}

// BootstrapInitDB returns a BootstrapInitDB apply configuration with version-specific SQL
func BootstrapInitDB(branchName string, majorVersion int) *apiv1ac.BootstrapInitDBApplyConfiguration {
	return apiv1ac.BootstrapInitDB().
		WithDatabase("xata").
		WithEncoding("UTF8").
		WithLocaleCType("C").
		WithLocaleCollate("C").
		WithOwner("xata").
		WithSecret(corev1ac.LocalObjectReference().
			WithName(branchName + "-app")).
		WithPostInitSQL(GeneratePostInitSQL(majorVersion)...)
}

// PostgresParametersToMap converts a list of PostgresParameter to a map
func PostgresParametersToMap(cfg *v1alpha1.PostgresConfiguration) map[string]string {
	if cfg == nil {
		return nil
	}

	params := cfg.Parameters
	result := make(map[string]string, len(params))
	for _, param := range params {
		result[param.Name] = param.Value
	}
	return result
}

// DefaultSmartShutdownTimeout is set to 1 second, overriding the CNPG default of
// 180 seconds. The main reason is that pool-hibernation happens quickly.
const DefaultSmartShutdownTimeout int32 = 1

// smartShutdownTimeout returns the provided timeout or the default (1s)
func smartShutdownTimeout(t *int32) int32 {
	if t != nil {
		return *t
	}
	return DefaultSmartShutdownTimeout
}

// LabelsFromInheritedMetadata extracts labels from InheritedMetadata, handling nil
func LabelsFromInheritedMetadata(m *v1alpha1.InheritedMetadata) map[string]string {
	if m == nil {
		return nil
	}
	return m.Labels
}
