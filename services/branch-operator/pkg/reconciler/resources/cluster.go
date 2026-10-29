package resources

import (
	"cmp"
	"fmt"
	"strings"

	machineryapi "github.com/cloudnative-pg/machinery/pkg/api"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	apiv1ac "github.com/xataio/xata-cnpg/pkg/client/applyconfiguration/api/v1"
	corev1 "k8s.io/api/core/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"

	"xata/internal/pgbackrest"
	"xata/internal/postgresversions"
	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/shared"
)

const (
	// Fixed pgbackrest defaults not exposed to users
	pgbackrestPriority = 19

	// pgBackRestAzureKeyTypeAuto selects managed-identity auth for the Azure repo.
	pgBackRestAzureKeyTypeAuto = "auto"

	// gkeServiceAccountAnnotation maps the cluster's pod ServiceAccount to a GCP
	// service account via Workload Identity.
	gkeServiceAccountAnnotation = "iam.gke.io/gcp-service-account"
)

// InheritedAnnotations are defined on the Cluster; CNPG will propagate them to
// all resources it creates
var InheritedAnnotations = map[string]string{
	// TLS is enabled on the metrics endpoint
	"prometheus.io/scheme": "https",
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

	sql := []string{
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
	if majorVersion >= 16 {
		// Since PG 16, CREATEROLE alone can't grant pg_monitor; this lets xata grant it to a monitoring role.
		sql = append(sql, "GRANT pg_monitor TO xata_superuser WITH ADMIN OPTION;")
	}
	return sql
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
	// BackupCredentials references the Secret holding static S3 credentials,
	// used for pgbackrest when targeting a non-AWS S3-compatible endpoint
	// (Cloudflare R2, or RustFS for local dev). Mirrors the barman ObjectStore.
	BackupCredentials BackupCredentials
	// PgBackRestWebIdentityRoleARN is the ARN of the IAM role that pgBackRest uses for
	// Branches with s3.keyType=web-id. If it is set, each Cluster gets the web
	// identity token volume and the AWS_ROLE_ARN environment variable.
	PgBackRestWebIdentityRoleARN string
	// PgBackRestGCSWebIdentity gives each Cluster the web identity token
	// volume and the PGBACKREST_REPO1_GCS_KEY environment variable, which
	// pgBackRest uses for Branches with gcs.keyType=web-id.
	PgBackRestGCSWebIdentity bool
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
	case v1alpha1.RestoreTypeXVolClone:
		bootstrap = apiv1ac.BootstrapConfiguration().
			WithNoop(apiv1.BootstrapNoop{})
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
	if cfg.Storage.VolumeAttributesClass != nil {
		storage = storage.WithPersistentVolumeClaimTemplate(corev1.PersistentVolumeClaimSpec{
			VolumeAttributesClassName: cfg.Storage.VolumeAttributesClass,
		})
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
			WithName(branchName + "-superuser")).
		WithBootstrap(bootstrap).
		WithPostgresConfiguration(apiv1ac.PostgresConfiguration().
			WithParameters(PostgresParametersToMap(cfg.Postgres)).
			WithAdditionalLibraries(cfg.Postgres.GetSharedPreloadLibraries()...)).
		WithPlugins(clusterPlugins(branchName, cfg)...).
		WithResources(cfg.Resources).
		WithBackup(backupConfiguration(branchName, cfg)).
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
					apiv1.ServiceSelectorTypeRO,
				)).
			WithRoles(shared.XataRoleConfiguration(branchName))).
		WithMonitoring(apiv1ac.MonitoringConfiguration().
			WithTLSConfig(apiv1ac.ClusterMonitoringTLSConfiguration().
				WithEnabled(true)).
			WithCustomQueriesConfigMap(machineryapi.ConfigMapKeySelector{
				Key:  "metrics.yaml",
				Name: "cnpg-custom-metrics",
			})).
		WithAffinity(affinity).
		WithInheritedMetadata(apiv1ac.EmbeddedObjectMetadata().
			WithAnnotations(InheritedAnnotations).
			WithLabels(LabelsFromInheritedMetadata(cfg.InheritedMetadata))).
		WithPrimaryUpdateStrategy(apiv1.PrimaryUpdateStrategy("unsupervised")).
		WithPrimaryUpdateMethod(apiv1.PrimaryUpdateMethod("switchover")).
		WithExternalClusters(externalClusters...).
		WithSmartShutdownTimeout(smartShutdownTimeout(cfg.SmartShutdownTimeout)).
		WithServiceAccountTemplate(serviceAccountTemplate(cfg))

	switch {
	case cfg.PgBackRestWebIdentityRoleARN != "":
		volume, env := pgbackrest.WebIdentity(clusterName, cfg.PgBackRestWebIdentityRoleARN)
		spec = spec.
			WithProjectedVolumeTemplate(volume).
			WithEnv(env...)
	case cfg.PgBackRestGCSWebIdentity:
		volume, env := pgbackrest.GCSWebIdentity(clusterName)
		spec = spec.
			WithProjectedVolumeTemplate(volume).
			WithEnv(env...)
	}

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
// point-in-time recovery from object storage. For pgbackrest it points at the
// source branch's repository (S3, GCS, or Azure); for barman it uses the
// barman-cloud plugin. Returns nil if the restore spec is not for ObjectStore
// type.
func ExternalClusters(cfg ClusterConfig) []*apiv1ac.ExternalClusterApplyConfiguration {
	if !cfg.RestoreSpec.IsObjectStoreType() {
		return nil
	}

	// pgbackrest: external cluster points to the source branch's repo (S3,
	// GCS, or Azure)
	if cfg.IsPgBackRest() {
		pgb := cfg.PgBackRest

		// The source backups are at the defined repo path of the Branch. Without
		// one, they are at the stanza (= the restore source name).
		// RepoPath is the bare prefix; the instance manager prepends the leading
		// "/" when it writes repo1-path, so do NOT add one here (else repo1-path
		// becomes "//<name>", which pgbackrest rejects).
		options := apiv1ac.PgBackRestOptions().
			WithRepoPath(cmp.Or(pgb.RepoPath, cfg.RestoreSpec.Name))

		return []*apiv1ac.ExternalClusterApplyConfiguration{
			apiv1ac.ExternalCluster().
				WithName(cfg.RestoreSpec.Name).
				WithPgBackRest(apiv1ac.PgBackRestExternalCluster().
					WithRepository(pgbackrestRepository(
						pgb,
						cfg.BackupCredentials,
						cfg.RestoreSpec.PgBackRestCipherPassphraseSecretRef,
					)).
					WithOptions(options)),
		}
	}

	// barman: external cluster uses barman-cloud plugin
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

// backupConfiguration builds the CNPG BackupConfiguration for the cluster.
// clusterPlugins returns the plugin configurations for the CNPG Cluster.
// The scale-to-zero plugin is always included. The barman-cloud plugin is
// only included when barman is the backup method.
func clusterPlugins(branchName string, cfg ClusterConfig) []*apiv1ac.PluginConfigurationApplyConfiguration {
	plugins := []*apiv1ac.PluginConfigurationApplyConfiguration{
		apiv1ac.PluginConfiguration().
			WithName("cnpg-i-scale-to-zero.xata.io"),
	}

	if !cfg.IsPgBackRest() {
		plugins = append(plugins, apiv1ac.PluginConfiguration().
			WithName("barman-cloud.cloudnative-pg.io").
			WithEnabled(cfg.RequiresBarmanPlugin()).
			WithIsWALArchiver(true).
			WithParameters(BarmanPluginParameters(branchName, cfg.GetServerName())))
	}

	return plugins
}

// Volume snapshot config is always included regardless of backup method.
// For barman, backup storage is configured via the separate ObjectStore CR.
// For pgbackrest, backup storage is configured inline on the Cluster.
func backupConfiguration(branchName string, cfg ClusterConfig) *apiv1ac.BackupConfigurationApplyConfiguration {
	backup := apiv1ac.BackupConfiguration().
		WithVolumeSnapshot(apiv1ac.VolumeSnapshotConfiguration().
			WithClassName(cfg.Storage.GetVolumeSnapshotClass()).
			WithOnline(true).
			WithOnlineConfiguration(apiv1ac.OnlineConfiguration().
				WithImmediateCheckpoint(true)))

	// this is barman
	if !cfg.IsPgBackRest() {
		return backup
	}

	pgb := cfg.PgBackRest

	options := apiv1ac.PgBackRestOptions().
		WithCompressType(pgb.CompressType).
		WithArchiveAsync(pgb.ArchiveAsync).
		WithArchivePushQueueMax(pgb.ArchivePushQueueMax).
		WithArchiveGetQueueMax(pgb.ArchiveGetQueueMax).
		WithBundle(true).
		WithBlockIncremental(true).
		WithStartFast(true).
		WithDelta(true).
		WithPriority(pgbackrestPriority).
		WithRetention(apiv1ac.PgBackRestRetention().
			WithFull(pgb.RetentionFullDays).
			WithFullType("time"))

	if pgb.CompressLevel != nil {
		options = options.WithCompressLevel(*pgb.CompressLevel)
	}
	if pgb.RepoPath != "" {
		options = options.WithRepoPath(pgb.RepoPath)
	}

	// Pin the stanza to the branch name so backups stay continuous even when the
	// underlying cluster is recreated (e.g. warm-pool wakeups, where the cluster
	// name changes). repoPath defaults to the stanza. An explicit StanzaName
	// override is used when restoring a branch under the same name, so the
	// restored branch writes to a fresh stanza instead of the one it restored
	// from.
	stanza := pgb.GetStanzaName(branchName)

	return backup.
		WithPgBackRest(apiv1ac.PgBackRestConfiguration().
			WithStanzaName(stanza).
			WithRepository(pgbackrestRepository(
				pgb,
				cfg.BackupCredentials,
				pgb.CipherPassphraseSecretRef,
			)).
			WithOptions(options)).
		WithTarget(apiv1.BackupTargetStandby)
}

// pgbackrestRepository selects the pgbackrest storage backend. Precedence is
// azure > gcs > s3.
func pgbackrestRepository(
	pgb *v1alpha1.PgBackRestSpec,
	creds BackupCredentials,
	cipherPassphraseSecretRef *corev1.SecretKeySelector,
) *apiv1ac.PgBackRestRepositoryApplyConfiguration {
	repo := apiv1ac.PgBackRestRepository()
	switch {
	case pgb.Azure != nil:
		repo = repo.WithAzure(pgbackrestAzure(pgb.Azure))
	case pgb.GCS != nil:
		repo = repo.WithGCS(pgbackrestGCS(pgb.GCS))
	case pgb.S3 != nil:
		repo = repo.WithS3(pgbackrestS3(pgb.S3, creds))
	}

	if cipherPassphraseSecretRef != nil {
		repo = repo.WithCipher(apiv1ac.PgBackRestCipher().
			WithType("aes-256-cbc").
			WithPassphrase(machineryapi.SecretKeySelector{
				Name: cipherPassphraseSecretRef.Name,
				Key:  cipherPassphraseSecretRef.Key,
			}))
	}

	return repo
}

// pgbackrestS3 builds the S3 apply configuration.
func pgbackrestS3(
	s3 *v1alpha1.PgBackRestS3Spec,
	creds BackupCredentials,
) *apiv1ac.PgBackRestS3ApplyConfiguration {
	ac := apiv1ac.PgBackRestS3().
		WithBucket(s3.Bucket).
		WithRegion(s3.Region)
	if s3.Endpoint == "" {
		return ac.WithKeyType(s3.KeyType)
	}

	// The branch-stamped Secret wins over the operator-wide default, so
	// existing branches keep their store's credentials when the cell-wide
	// default changes.
	secretName := cmp.Or(s3.CredentialsSecretName, creds.SecretName)
	return ac.WithEndpoint(s3.Endpoint).
		WithAccessKeyID(machineryapi.SecretKeySelector{
			Name: secretName,
			Key:  creds.AccessKeyIDKey,
		}).
		WithSecretAccessKey(machineryapi.SecretKeySelector{
			Name: secretName,
			Key:  creds.SecretAccessKeyKey,
		})
}

// pgbackrestGCS builds the GCS apply configuration. With keyType=auto, the GCP
// service account is bound via the cluster's serviceAccountTemplate, not the
// repository.
func pgbackrestGCS(gcs *v1alpha1.PgBackRestGCSSpec) *apiv1ac.PgBackRestGCSApplyConfiguration {
	return apiv1ac.PgBackRestGCS().
		WithBucket(gcs.Bucket).
		WithKeyType(gcs.KeyType)
}

// pgbackrestAzure builds the Azure apply configuration. Auth is managed
// identity only (keyType=auto).
func pgbackrestAzure(azure *v1alpha1.PgBackRestAzureSpec) *apiv1ac.PgBackRestAzureApplyConfiguration {
	return apiv1ac.PgBackRestAzure().
		WithAccount(azure.Account).
		WithContainer(azure.Container).
		WithKeyType(pgBackRestAzureKeyTypeAuto)
}

// UsesAWSIAM reports whether pgBackRest stores the backups in AWS S3 with IAM
// credentials, and not with static keys for an S3-compatible endpoint.
func UsesAWSIAM(b *v1alpha1.BackupSpec) bool {
	if !b.IsPgBackRest() || b.PgBackRest.Azure != nil || b.PgBackRest.GCS != nil {
		return false
	}
	s3 := b.PgBackRest.S3
	return s3 != nil && s3.Endpoint == ""
}

// serviceAccountTemplate maps each cluster-specific Kubernetes ServiceAccount
// to the cell-wide GCP service account through Workload Identity. AWS and
// Azure do not use the cluster ServiceAccount: on AWS the Branch has its own
// ServiceAccount, and Azure managed identity comes from the node.
func serviceAccountTemplate(cfg ClusterConfig) *apiv1ac.ServiceAccountTemplateApplyConfiguration {
	if !cfg.IsPgBackRest() || cfg.PgBackRest.Azure != nil || cfg.PgBackRest.GCS == nil {
		return nil
	}
	if cfg.PgBackRest.GCS.ServiceAccountEmail == "" {
		return nil
	}

	return apiv1ac.ServiceAccountTemplate().
		WithMetadata(apiv1ac.Metadata().
			WithAnnotations(map[string]string{
				gkeServiceAccountAnnotation: cfg.PgBackRest.GCS.ServiceAccountEmail,
			}))
}

// LabelsFromInheritedMetadata extracts labels from InheritedMetadata, handling nil
func LabelsFromInheritedMetadata(m *v1alpha1.InheritedMetadata) map[string]string {
	if m == nil {
		return nil
	}
	return m.Labels
}
