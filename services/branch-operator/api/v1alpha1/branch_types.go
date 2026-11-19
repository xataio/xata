package v1alpha1

import (
	"cmp"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BranchSpec defines the desired state of a Branch
// +kubebuilder:validation:XValidation:rule="has(oldSelf.restore) == has(self.restore)",message="restore is immutable"
type BranchSpec struct {
	// Restore specifies how to restore the branch from a backup source.
	// +kubebuilder:validation:Optional
	Restore *RestoreSpec `json:"restore,omitempty"`

	// ClusterSpec defines the CNPG cluster configuration for the branch
	// +kubebuilder:validation:Required
	ClusterSpec ClusterSpec `json:"cluster"`

	// BackupSpec configures pgbackrest backups for the branch. When nil, the
	// branch has no WAL archiving or scheduled backups.
	// +optional
	BackupSpec *BackupSpec `json:"backup,omitempty"`

	// Pooler configures a PgBouncer connection pooler for the branch.
	// When nil, no pooler is created.
	// +optional
	Pooler *PoolerSpec `json:"pooler,omitempty"`

	// InheritedMetadata defines metadata to be inherited by all resources
	// created by the operator
	// +optional
	InheritedMetadata *InheritedMetadata `json:"inheritedMetadata,omitempty"`
}

// RestoreSpec defines how to restore the branch from a backup source
// +kubebuilder:validation:XValidation:rule="has(oldSelf.pgbackrest) == has(self.pgbackrest)",message="restore.pgbackrest is immutable"
type RestoreSpec struct {
	// Type specifies the type of restore source
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="restore.type is immutable"
	// +kubebuilder:validation:Enum=VolumeSnapshot;ObjectStore;BaseBackup;XVolClone
	Type RestoreType `json:"type"`

	// Name is the name of the restore source (for volume snapshot, object store, base backup or XVol clone)
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="restore.name is immutable"
	// +kubebuilder:validation:MaxLength=256
	Name string `json:"name"`

	// Timestamp specifies the point-in-time to restore to.
	// Only applicable when Type is ObjectStore.
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="restore.timestamp is immutable"
	Timestamp *metav1.Time `json:"timestamp,omitempty"`

	// PgBackRestCipherPassphraseSecretRef references the passphrase for the
	// pgbackrest repository used as the restore source.
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="restore.pgbackrestCipherPassphraseSecretRef is immutable"
	PgBackRestCipherPassphraseSecretRef *corev1.SecretKeySelector `json:"pgbackrestCipherPassphraseSecretRef,omitempty"`

	// PgBackRest is the pgbackrest repository of the restore source. Only
	// applicable when Type is ObjectStore. When nil, the restore reads the
	// repository of this branch at stanza Name.
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="restore.pgbackrest is immutable"
	PgBackRest *RestorePgBackRestSpec `json:"pgbackrest,omitempty"`
}

// RestorePgBackRestSpec locates the pgbackrest repository of a restore source.
// +kubebuilder:validation:XValidation:rule="(has(self.s3) ? 1 : 0) + (has(self.gcs) ? 1 : 0) + (has(self.azure) ? 1 : 0) == 1",message="exactly one of s3, gcs, or azure is required"
type RestorePgBackRestSpec struct {
	// RepoPath is the repository path of the source.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	RepoPath string `json:"repoPath"`

	// StanzaName is the stanza of the source.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9._-]+$`
	StanzaName string `json:"stanzaName"`

	// S3 is the S3 backend of the source.
	// +optional
	S3 *PgBackRestS3Spec `json:"s3,omitempty"`

	// GCS is the GCS backend of the source.
	// +optional
	GCS *PgBackRestGCSSpec `json:"gcs,omitempty"`

	// Azure is the Azure backend of the source.
	// +optional
	Azure *PgBackRestAzureSpec `json:"azure,omitempty"`
}

// RestoreType defines the type of restore source
type RestoreType string

const (
	// RestoreTypeVolumeSnapshot restores from a Kubernetes volume snapshot
	RestoreTypeVolumeSnapshot RestoreType = "VolumeSnapshot"
	// RestoreTypeXVolClone restores from a clone of an existing XVol
	RestoreTypeXVolClone RestoreType = "XVolClone"
	// RestoreTypeObjectStore restores from object storage (supports PITR with timestamp)
	RestoreTypeObjectStore RestoreType = "ObjectStore"
	// RestoreTypeBaseBackup restores from a base backup
	RestoreTypeBaseBackup RestoreType = "BaseBackup"
)

func (r *RestoreSpec) IsXVolCloneType() bool {
	return r != nil && r.Type == RestoreTypeXVolClone
}

func (r *RestoreSpec) IsVolumeSnapshotType() bool {
	return r != nil && r.Type == RestoreTypeVolumeSnapshot
}

func (r *RestoreSpec) IsObjectStoreType() bool {
	return r != nil && r.Type == RestoreTypeObjectStore
}

// XVolCloneName returns the name of the cloned XVol for the given parent and
// child branch names.
func XVolCloneName(parentBranchName, childBranchName string) string {
	return parentBranchName + "-" + childBranchName
}

// ClusterSpec defines the PostgreSQL cluster configuration
type ClusterSpec struct {
	// Name is the name of the CNPG Cluster resource.
	// If nil, the branch has no cluster associated with it.
	// +optional
	Name *string `json:"name,omitempty"`

	// Image is the PostgreSQL container image to use
	// +kubebuilder:validation:Required
	Image string `json:"image"`

	// Instances is the number of PostgreSQL instances in the cluster
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=5
	// +optional
	// +kubebuilder:default:=1
	Instances int32 `json:"instances,omitempty"`

	// Resources defines the compute resources for the PostgreSQL instances
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Storage defines the persistent volume configuration for each instance
	// +kubebuilder:default:={size:"1Gi"}
	// +optional
	Storage StorageSpec `json:"storage,omitempty"`

	// Postgres defines PostgreSQL-specific configuration
	// +kubebuilder:validation:Optional
	Postgres *PostgresConfiguration `json:"postgres,omitempty"`

	// ScaleToZero defines scale-to-zero configuration for the cluster
	// +optional
	ScaleToZero *ScaleToZeroConfiguration `json:"scaleToZero,omitempty"`

	// Hibernation specifies whether the cluster should be hibernated
	// +optional
	// +kubebuilder:validation:Enum=Enabled;Disabled
	Hibernation *HibernationMode `json:"hibernation,omitempty"`

	// Affinity defines scheduling constraints for the cluster pods
	// +optional
	Affinity *AffinitySpec `json:"affinity,omitempty"`

	// SmartShutdownTimeout is the time in seconds reserved for the smart
	// shutdown of Postgres to complete. If not set, the CNPG default (180s)
	// is used.
	// +optional
	// +kubebuilder:validation:Minimum=1
	SmartShutdownTimeout *int32 `json:"smartShutdownTimeout,omitempty"`
}

// ClusterName returns the CNPG Cluster name for this branch. If
// Spec.ClusterSpec.Name is nil, the branch has no associated cluster and an
// empty string is returned.
func (b *Branch) ClusterName() string {
	if b.Spec.ClusterSpec.Name == nil {
		return ""
	}
	return *b.Spec.ClusterSpec.Name
}

// HasClusterName returns true iff the branch has a CNPG Cluster name specified
func (b *Branch) HasClusterName() bool {
	return b.Spec.ClusterSpec.Name != nil
}

// HasBackupData returns true iff the branch has written a backup or archived
// WAL to its repository.
func (b *Branch) HasBackupData() bool {
	return b.Status.FirstRecoverabilityPoint != "" ||
		b.Status.LastSuccessfulBackup != "" ||
		b.Status.LastRecoverabilityPoint != ""
}

// AffinitySpec defines pod scheduling constraints
type AffinitySpec struct {
	// NodeSelector specifies node labels that pods must match to be scheduled
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
}

func (a *AffinitySpec) GetNodeSelector() map[string]string {
	if a == nil {
		return nil
	}
	return a.NodeSelector
}

// StorageSpec defines storage configuration for the cluster
type StorageSpec struct {
	// Size is the size of the persistent volume for each instance
	// +kubebuilder:validation:Pattern=`^\d+(\.\d+)?(Gi|Ti)$`
	// +kubebuilder:default:="1Gi"
	// +kubebuilder:validation:XValidation:rule="quantity(self).compareTo(quantity(oldSelf)) >= 0",message="storage size cannot be decreased"
	// +optional
	Size string `json:"size,omitempty"`

	// StorageClass is the Kubernetes storage class for the cluster PVCs
	// +optional
	StorageClass *string `json:"storageClass,omitempty"`

	// VolumeAttributesClass is the Kubernetes volume attributes class for the cluster PVCs
	// +optional
	VolumeAttributesClass *string `json:"volumeAttributesClass,omitempty"`

	// VolumeSnapshotClass is the Kubernetes volume snapshot class for snapshots
	// +optional
	VolumeSnapshotClass *string `json:"volumeSnapshotClass,omitempty"`

	// MountPropagation sets the mount propagation mode for the storage volume.
	// Valid values are "None", "HostToContainer", and "Bidirectional".
	// +optional
	// +kubebuilder:validation:Enum=None;HostToContainer;Bidirectional
	MountPropagation *string `json:"mountPropagation,omitempty"`
}

func (s *StorageSpec) GetVolumeSnapshotClass() string {
	if s.VolumeSnapshotClass == nil {
		return ""
	}
	return *s.VolumeSnapshotClass
}

// PostgresConfiguration defines PostgreSQL-specific settings
type PostgresConfiguration struct {
	// Parameters are PostgreSQL configuration parameters
	// +optional
	// +listType=map
	// +listMapKey=name
	Parameters []PostgresParameter `json:"parameters,omitempty"`

	// SharedPreloadLibraries are PostgreSQL shared preload libraries
	// +optional
	// +listType=set
	SharedPreloadLibraries []string `json:"sharedPreloadLibraries,omitempty"`
}

func (p *PostgresConfiguration) GetSharedPreloadLibraries() []string {
	if p == nil {
		return nil
	}
	return p.SharedPreloadLibraries
}

// PostgresParameter defines a PostgreSQL configuration parameter
type PostgresParameter struct {
	// Name is the parameter name
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Value is the parameter value
	// +kubebuilder:validation:Required
	Value string `json:"value"`
}

// PoolerSpec configures a PgBouncer connection pooler for the branch.
type PoolerSpec struct {
	// Instances is the number of PgBouncer instances
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=5
	// +optional
	// +kubebuilder:default:=1
	Instances int32 `json:"instances,omitempty"`

	// Mode is the PgBouncer pool mode
	// +kubebuilder:validation:Enum=session;transaction
	// +optional
	// +kubebuilder:default:="session"
	Mode PoolMode `json:"mode,omitempty"`

	// MaxClientConn is the maximum number of client connections to PgBouncer
	// +optional
	// +kubebuilder:default:="100"
	MaxClientConn string `json:"maxClientConn,omitempty"`

	// DefaultPoolSize overrides the PgBouncer default_pool_size parameter.
	// When empty, default_pool_size is derived as floor(0.9 * max_connections)
	// from the branch's Postgres configuration.
	// +optional
	// +kubebuilder:validation:Pattern="^[1-9][0-9]*$"
	DefaultPoolSize string `json:"defaultPoolSize,omitempty"`
}

// PoolMode is the PgBouncer pool mode
type PoolMode string

const (
	PoolModeSession     PoolMode = "session"
	PoolModeTransaction PoolMode = "transaction"
)

func (p *PoolerSpec) IsEnabled() bool {
	return p != nil
}

// InheritedMetadata defines metadata to be inherited by all resources created
// by the operator
type InheritedMetadata struct {
	// Labels are additional labels to apply to resources created by the operator
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
}

func (im *InheritedMetadata) GetLabels() map[string]string {
	if im == nil {
		return nil
	}
	return im.Labels
}

// ScaleToZeroConfiguration defines scale-to-zero settings
type ScaleToZeroConfiguration struct {
	// Enabled controls whether scale-to-zero is active.
	// When false the configuration (e.g. InactivityPeriodMinutes) is preserved
	// but scale-to-zero is disabled.
	Enabled bool `json:"enabled"`
	// InactivityPeriodMinutes is the period of inactivity before scaling to zero
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1440
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=30
	InactivityPeriodMinutes int32 `json:"inactivityPeriodMinutes,omitempty"`
}

// HibernationMode describes the hibernation state for a branch
type HibernationMode string

const (
	HibernationModeEnabled  HibernationMode = "Enabled"
	HibernationModeDisabled HibernationMode = "Disabled"

	WakeupPoolAnnotation     = "xata.io/wakeup-pool"
	AwokenByXVolAnnotation   = "xata.io/awoken-by-xvol"
	AwaitingWakeupAnnotation = "xata.io/awaiting-wakeup"
)

// IsEnabled returns true if hibernation mode is enabled
func (hm *HibernationMode) IsEnabled() bool {
	return hm != nil && *hm == HibernationModeEnabled
}

// HasWakeupPoolAnnotation returns true if the Branch has the wakeup pool
// annotation
func (b *Branch) HasWakeupPoolAnnotation() bool {
	_, ok := b.Annotations[WakeupPoolAnnotation]
	return ok
}

// WakeupPoolName returns the name of the wakeup pool from the Branch's
// annotations. If the annotation is not present, an empty string is returned.
func (b *Branch) WakeupPoolName() string {
	return b.Annotations[WakeupPoolAnnotation]
}

// IsAwaitingWakeup returns true if the Branch has the awaiting wakeup
// annotation set to true
func (b *Branch) IsAwaitingWakeup() bool {
	value, ok := b.Annotations[AwaitingWakeupAnnotation]
	if !ok {
		return false
	}

	// Check the value of the annotation
	awaiting, err := strconv.ParseBool(value)
	if err != nil {
		return false
	}

	return awaiting
}

type WALArchivingMode string

const (
	WALArchivingModeEnabled  WALArchivingMode = "Enabled"
	WALArchivingModeDisabled WALArchivingMode = "Disabled"
)

// BackupMethod defines the backup method to use
type BackupMethod string

const (
	// BackupMethodPgBackRest uses in-core pgbackrest for WAL archiving and backups
	BackupMethodPgBackRest BackupMethod = "pgbackrest"
)

// BackupSpec defines backup settings for a Branch
// +kubebuilder:validation:XValidation:rule="has(self.pgbackrest)",message="pgbackrest spec is required"
type BackupSpec struct {
	// Method specifies the backup method. Only "pgbackrest" (in-core
	// pgbackrest) is supported.
	// +kubebuilder:validation:Enum=pgbackrest
	// +kubebuilder:default:="pgbackrest"
	// +optional
	Method BackupMethod `json:"method,omitempty"`

	// Retention is the retention period as requested through the clusters API
	// and reported back by it. Examples: "60d", "4w", "2m". The effective
	// pgbackrest retention is PgBackRest.RetentionFullDays, derived from it.
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Pattern=`^\d+[dwm]$`
	// +kubebuilder:default:="2d"
	Retention string `json:"retention,omitempty"`

	// WALArchiving specifies whether WAL archiving is enabled
	// +kubebuilder:validation:Enum=Enabled;Disabled
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:="Enabled"
	WALArchiving WALArchivingMode `json:"walArchiving,omitempty"`

	// ScheduledBackup configures periodic base backups.
	// +kubebuilder:validation:Optional
	ScheduledBackup *ScheduledBackupSpec `json:"scheduledBackup,omitempty"`

	// PgBackRest configures pgbackrest storage and options.
	// Required when Method is "pgbackrest".
	// +optional
	PgBackRest *PgBackRestSpec `json:"pgbackrest,omitempty"`
}

// PgBackRestSpec defines pgbackrest-specific backup configuration.
//
// Exactly one storage backend must be specified via the s3, gcs, or azure
// sub-struct.
//
// The following pgbackrest options are set internally with fixed defaults
// and not exposed to users:
//   - retention fullType: always "time" (days)
//   - retention archive: follows full retention (pgbackrest default)
//   - bundle: always on (fewer S3 API calls)
//   - blockIncremental: always on (requires bundle; reduces storage for large DBs)
//   - startFast: always on (immediate checkpoint, no waiting)
//   - delta: always on (faster restores)
//   - backupStandby: controlled via backup.target on the Cluster
//   - priority: 19 (lowest CPU priority, avoids impacting postgres)
//   - processMax: computed from instance CPU resources
//
// +kubebuilder:validation:XValidation:rule="(has(self.s3) ? 1 : 0) + (has(self.gcs) ? 1 : 0) + (has(self.azure) ? 1 : 0) <= 1",message="s3, gcs, and azure are mutually exclusive"
// +kubebuilder:validation:XValidation:rule="has(self.s3) || has(self.gcs) || has(self.azure)",message="a pgbackrest backend is required: set s3, gcs, or azure"
type PgBackRestSpec struct {
	// S3 configures an S3-compatible storage backend.
	// +optional
	S3 *PgBackRestS3Spec `json:"s3,omitempty"`

	// GCS configures a Google Cloud Storage backend (Workload Identity auth).
	// +optional
	GCS *PgBackRestGCSSpec `json:"gcs,omitempty"`

	// Azure configures an Azure Blob Storage backend (managed identity auth).
	// +optional
	Azure *PgBackRestAzureSpec `json:"azure,omitempty"`

	// CipherPassphraseSecretRef references the passphrase for client-side
	// repository encryption. When unset, pgbackrest does not encrypt the
	// repository.
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="pgbackrest.cipherPassphraseSecretRef is immutable"
	CipherPassphraseSecretRef *corev1.SecretKeySelector `json:"cipherPassphraseSecretRef,omitempty"`

	// RetentionFullDays is the number of days to retain full backups.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default:=7
	RetentionFullDays int `json:"retentionFullDays,omitempty"`

	// CompressType sets the compression algorithm.
	// +optional
	// +kubebuilder:validation:Enum=none;gz;bz2;lz4;zst
	// +kubebuilder:default:="lz4"
	CompressType string `json:"compressType,omitempty"`

	// CompressLevel sets the compression level (0-9).
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=9
	CompressLevel *int `json:"compressLevel,omitempty"`

	// ArchiveAsync enables asynchronous WAL archiving.
	// +optional
	// +kubebuilder:default:=true
	ArchiveAsync bool `json:"archiveAsync,omitempty"`

	// ArchivePushQueueMax is the max WAL push queue size when async is enabled.
	// Uses pgbackrest size format (e.g. "2GiB", "256MiB").
	// +optional
	// +kubebuilder:default:="2GiB"
	ArchivePushQueueMax string `json:"archivePushQueueMax,omitempty"`

	// ArchiveGetQueueMax is the max WAL restore queue size when async is enabled.
	// Uses pgbackrest size format (e.g. "2GiB", "256MiB").
	// +optional
	// +kubebuilder:default:="2GiB"
	ArchiveGetQueueMax string `json:"archiveGetQueueMax,omitempty"`

	// RepoPath overrides the storage path prefix. Defaults to the stanza name.
	// +optional
	RepoPath string `json:"repoPath,omitempty"`

	// StanzaName overrides the pgbackrest stanza this branch archives to.
	// When empty it defaults to the branch name.
	// +optional
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9._-]+$`
	StanzaName string `json:"stanzaName,omitempty"`
}

// PgBackRestS3Spec configures an S3-compatible pgbackrest backend. Mirrors the
// cnpg PgBackRestS3 fields maki uses; credentials come from the pod IAM role or,
// when Endpoint is set, the operator-configured credentials Secret.
type PgBackRestS3Spec struct {
	// Bucket is the S3 bucket for backups and WAL archives.
	// +kubebuilder:validation:Required
	Bucket string `json:"bucket"`

	// Region is the S3 region.
	// +kubebuilder:validation:Required
	Region string `json:"region"`

	// Endpoint overrides S3 endpoint discovery. Required for non-AWS
	// S3-compatible storage (e.g. Cloudflare R2, or RustFS for local dev). When
	// set, static credentials from the operator-configured credentials Secret
	// are used instead of an IAM role.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// CredentialsSecretName is the Secret (in the clusters namespace) holding
	// the static credentials for this backend. When empty, the
	// operator-configured credentials Secret is used. Stamped at branch
	// creation so existing branches keep their store's credentials when the
	// cell-wide default changes.
	// +optional
	CredentialsSecretName string `json:"credentialsSecretName,omitempty"`

	// KeyType is the cnpg s3.keyType of the repository. The operator ignores
	// it when Endpoint is set.
	// +optional
	// +kubebuilder:default:="auto"
	// +kubebuilder:validation:Enum=auto;web-id
	KeyType string `json:"keyType,omitempty"`
}

// PgBackRestGCSSpec configures a Google Cloud Storage pgbackrest backend.
// Authentication is Workload Identity only.
type PgBackRestGCSSpec struct {
	// Bucket is the GCS bucket for backups and WAL archives.
	// +kubebuilder:validation:Required
	Bucket string `json:"bucket"`

	// ServiceAccountEmail is the GCP service account that the pods of the
	// cluster impersonate with keyType=auto. The operator sets it as the
	// iam.gke.io/gcp-service-account annotation on the ServiceAccount of the
	// cluster. If empty, the pods use the ServiceAccount of the cluster.
	// +optional
	ServiceAccountEmail string `json:"serviceAccountEmail,omitempty"`

	// KeyType is the cnpg gcs.keyType of the repository.
	// +optional
	// +kubebuilder:default:="auto"
	// +kubebuilder:validation:Enum=auto;web-id
	KeyType string `json:"keyType,omitempty"`
}

// PgBackRestAzureSpec configures an Azure Blob Storage pgbackrest backend.
// Authentication is managed identity only: the operator renders the cnpg
// Azure repository with keyType=auto, an IMDS token request that resolves to
// the node's kubelet identity on AKS.
type PgBackRestAzureSpec struct {
	// Account is the Azure storage account for backups and WAL archives.
	// +kubebuilder:validation:Required
	Account string `json:"account"`

	// Container is the Azure Blob Storage container within Account.
	// +kubebuilder:validation:Required
	Container string `json:"container"`
}

// ScheduledBackupSpec configures periodic base backups
type ScheduledBackupSpec struct {
	// Schedule in CNPG 6-field cron format.
	// The format is : "second minute hour day month dayofweek"
	// Examples: "0 0 0 * * *" (daily at midnight), "0 0 2 * * 0" (weekly on Sunday at 2am)
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:="0 0 0 * * 0"
	Schedule string `json:"schedule,omitempty"`
}

func (b *BackupSpec) IsWALArchivingDisabled() bool {
	return b != nil && b.WALArchiving == WALArchivingModeDisabled
}

func (b *BackupSpec) IsScheduledBackupEnabled() bool {
	return b != nil && b.ScheduledBackup != nil
}

func (b *BackupSpec) IsPgBackRest() bool {
	return b != nil && b.Method == BackupMethodPgBackRest && b.PgBackRest != nil
}

// BranchStatus defines the observed state of a Branch
type BranchStatus struct {
	// ObservedGeneration reflects the generation of the most recently observed Branch spec
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations of the Branch's state
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// LastError contains the last error message encountered during
	// reconciliation, if any.
	// +optional
	LastError string `json:"lastError,omitempty"`

	// PrimaryXVolName is the name of the XVol backing the primary instance's
	// PersistentVolume. Retained after cluster removal.
	// +optional
	PrimaryXVolName string `json:"primaryXVolName,omitempty"`

	// FirstRecoverabilityPoint is the oldest point in time the branch can be
	// restored to (RFC3339). Copied from the Cluster status and retained
	// across hibernation and cluster replacement.
	// +optional
	FirstRecoverabilityPoint string `json:"firstRecoverabilityPoint,omitempty"`

	// LastSuccessfulBackup is the time of the last successful backup
	// (RFC3339). Copied from the Cluster status and retained across
	// hibernation and cluster replacement.
	// +optional
	LastSuccessfulBackup string `json:"lastSuccessfulBackup,omitempty"`

	// LastFailedBackup is the time of the last failed backup (RFC3339).
	// Copied from the Cluster status and retained across hibernation and
	// cluster replacement.
	// +optional
	LastFailedBackup string `json:"lastFailedBackup,omitempty"`

	// LastRecoverabilityPoint is the newest point in time the branch can be
	// restored to (RFC3339), based on the last archived WAL. Copied from the
	// Cluster status and retained across hibernation and cluster replacement.
	// +optional
	LastRecoverabilityPoint string `json:"lastRecoverabilityPoint,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=br
// +kubebuilder:printcolumn:name="Cluster",type="string",JSONPath=".spec.cluster.name"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Message",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].message"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// Branch is the Schema for the branches API
type Branch struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BranchSpec   `json:"spec,omitempty"`
	Status BranchStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
// BranchList contains a list of Branch
type BranchList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Branch `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Branch{}, &BranchList{})
}

// GetStanzaName returns the configured stanza or the Branch name.
func (p *PgBackRestSpec) GetStanzaName(branchName string) string {
	return cmp.Or(p.StanzaName, branchName)
}

// GetRepoPath returns the configured repository path or its stanza name.
func (p *PgBackRestSpec) GetRepoPath(branchName string) string {
	return cmp.Or(p.RepoPath, p.GetStanzaName(branchName))
}
