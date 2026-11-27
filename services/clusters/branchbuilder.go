package clusters

import (
	"fmt"
	"maps"
	"slices"

	clustersv1 "xata/gen/proto/clusters/v1"
	"xata/internal/pgbackrest"
	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/shared"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

const (
	LabelOrgID                     = "xata.io/organizationID"
	LabelProjectID                 = "xata.io/projectID"
	LabelBranchID                  = "xata.io/branchID"
	LabelXatastorAntiAffinityGroup = "xatastor.xata.io/anti-affinity-group"
	AnnotationIdempotencyKey       = "xata.io/idempotency-key"

	DefaultPgBackRestRetentionDays = 7
	DefaultPgBackRestCompressType  = "lz4"
	DefaultPgBackRestArchiveAsync  = true
	DefaultPgBackRestPushQueueMax  = "2GiB"
	DefaultPgBackRestGetQueueMax   = "2GiB"
)

// BranchBuilder builds Branch resources.
type BranchBuilder struct {
	branch *v1alpha1.Branch
}

// NewBranchBuilder creates a new BranchBuilder
func NewBranchBuilder() *BranchBuilder {
	return &BranchBuilder{}
}

// resourceConfigurer defines an interface for types that provide resource
// configuration.
type resourceConfigurer interface {
	GetVcpuRequest() string
	GetVcpuLimit() string
	GetMemory() string
}

// FromCreateClusterRequest configures the BranchBuilder using the values from the
// CreatePostgresClusterRequest.
func (b *BranchBuilder) FromCreateClusterRequest(r *clustersv1.CreatePostgresClusterRequest) *BranchBuilder {
	b.branch = &v1alpha1.Branch{
		Name: r.GetId(),
		Labels: map[string]string{
			LabelOrgID:     r.GetOrganizationId(),
			LabelProjectID: r.GetProjectId(),
		},
		Spec: v1alpha1.BranchSpec{
			InheritedMetadata: &v1alpha1.InheritedMetadata{
				Labels: map[string]string{
					LabelOrgID:                     r.GetOrganizationId(),
					LabelProjectID:                 r.GetProjectId(),
					LabelBranchID:                  r.GetId(),
					LabelXatastorAntiAffinityGroup: r.GetId(),
				},
			},
			Restore: restoreSpec(r),
			ClusterSpec: v1alpha1.ClusterSpec{
				Name:      new(r.GetId()),
				Image:     r.GetConfiguration().GetImageName(),
				Instances: r.GetConfiguration().GetNumInstances(),
				Storage: v1alpha1.StorageSpec{
					Size:             storageSize(r.GetConfiguration().GetStorageSize()),
					MountPropagation: new(string(corev1.MountPropagationHostToContainer)),
				},
				Resources: resourceRequirements(r.GetConfiguration()),
				Postgres: &v1alpha1.PostgresConfiguration{
					Parameters:             postgresParametersFromMap(r.GetConfiguration().GetPostgresConfigurationParameters()),
					SharedPreloadLibraries: r.GetConfiguration().GetPreloadLibraries(),
				},
				ScaleToZero: scaleToZeroConfig(r.GetConfiguration().GetScaleToZero()),
			},
			BackupSpec: backupSpec(r.GetBackupConfiguration()),
		},
	}
	if r.GetIdempotencyKey() != "" {
		b.branch.Annotations = map[string]string{
			AnnotationIdempotencyKey: r.GetIdempotencyKey(),
		}
	}

	return b
}

// FromExistingBranch configures the BranchBuilder using an existing Branch.
func (b *BranchBuilder) FromExistingBranch(branch *v1alpha1.Branch) *BranchBuilder {
	b.branch = branch.DeepCopy()
	return b
}

// WithOverridesFromParent configures the BranchBuilder with overrides from the
// parent Branch.
func (b *BranchBuilder) WithOverridesFromParent(parent *v1alpha1.Branch) *BranchBuilder {
	if parent == nil {
		return b
	}

	clusterSpec := &b.branch.Spec.ClusterSpec

	// Override Branch spec fields with values from the parent Branch
	clusterSpec.Storage.Size = parent.Spec.ClusterSpec.Storage.Size
	clusterSpec.Storage.VolumeSnapshotClass = parent.Spec.ClusterSpec.Storage.VolumeSnapshotClass
	clusterSpec.Storage.VolumeAttributesClass = parent.Spec.ClusterSpec.Storage.VolumeAttributesClass
	clusterSpec.Storage.MountPropagation = parent.Spec.ClusterSpec.Storage.MountPropagation
	clusterSpec.Image = parent.Spec.ClusterSpec.Image
	clusterSpec.Resources = parent.Spec.ClusterSpec.Resources
	clusterSpec.Postgres = parent.Spec.ClusterSpec.Postgres
	clusterSpec.SmartShutdownTimeout = parent.Spec.ClusterSpec.SmartShutdownTimeout

	// By default the child inherits the parent's storage class.
	parentSC := parent.Spec.ClusterSpec.Storage.StorageClass
	clusterSpec.Storage.StorageClass = parentSC

	// Adopt the child into the parent's wakeup pool, but only when it is being
	// created from the parent's volume (the branching case, restore type
	// VolumeSnapshot):
	// * Annotate the child with the same wakeup pool annotation
	// * Annotate the child with an "awaiting wakeup" annotation
	// * Upgrade the restore type from VolumeSnapshot to XVolClone
	// * Clear the cluster name from the child Branch, so that the cluster comes
	//   from the pool when the WakeupRequest is served
	if parent.HasWakeupPoolAnnotation() && b.branch.Spec.Restore.IsVolumeSnapshotType() {
		if b.branch.Annotations == nil {
			b.branch.Annotations = make(map[string]string)
		}
		b.branch.Annotations[v1alpha1.WakeupPoolAnnotation] = parent.Annotations[v1alpha1.WakeupPoolAnnotation]
		b.branch.Annotations[v1alpha1.AwaitingWakeupAnnotation] = "true"
		b.branch.Spec.Restore.Type = v1alpha1.RestoreTypeXVolClone
		b.branch.Spec.ClusterSpec.Name = nil
	}

	// Child branches always start with one instance, regardless of the parent's
	// instance count
	clusterSpec.Instances = 1

	// Disable backups on the Branch if they are not enabled on the parent
	if parent.Spec.BackupSpec == nil {
		b.branch.Spec.BackupSpec = nil
	}

	return b
}

// WithSQLIdentity sets the SQL identity profile of a new Branch. A Branch made
// from the data of another Branch (a clone or a restore) takes the profile of
// that source, because the roles and the database already exist in its data.
// Otherwise the Branch takes the profile of the cell.
func (b *BranchBuilder) WithSQLIdentity(cellProfile v1alpha1.SQLIdentityProfile, source *v1alpha1.Branch) *BranchBuilder {
	if source != nil {
		b.branch.Spec.SQLIdentity = source.Spec.SQLIdentity
		return b
	}
	b.branch.Spec.SQLIdentity = cellProfile
	return b
}

// WithUpdatesFrom configures the BranchBuilder with updates from the
// UpdatePostgresClusterRequest.
func (b *BranchBuilder) WithUpdatesFrom(req *clustersv1.UpdatePostgresClusterRequest, useStorageQoSClasses bool) *BranchBuilder {
	update := req.GetUpdateConfiguration()
	branchSpec := &b.branch.Spec

	// Update Branch hibernation settings
	if update.Hibernate != nil {
		// If hibernating, modify the Branch spec appropriately based on whether it
		// uses a wakeup pool or not
		if update.GetHibernate() {
			if b.branch.HasWakeupPoolAnnotation() {
				b.branch.Spec.ClusterSpec.Hibernation = nil
				b.branch.Spec.ClusterSpec.Name = nil
				b.branch.Annotations[v1alpha1.AwaitingWakeupAnnotation] = "false"
			} else {
				b.branch.Spec.ClusterSpec.Hibernation = ptr.To(v1alpha1.HibernationModeEnabled)
			}
			// If waking up, modify the Branch spec appropriately based on whether it
			// uses a wakeup pool or not.
		} else {
			if b.branch.HasWakeupPoolAnnotation() {
				b.branch.Spec.ClusterSpec.Hibernation = nil
				b.branch.Annotations[v1alpha1.AwaitingWakeupAnnotation] = "true"
			} else {
				b.branch.Spec.ClusterSpec.Hibernation = ptr.To(v1alpha1.HibernationModeDisabled)
			}
		}
	}

	// Update the image name
	if update.ImageName != nil {
		branchSpec.ClusterSpec.Image = update.GetImageName()
	}

	// Update the number of instances
	if update.NumInstances != nil {
		branchSpec.ClusterSpec.Instances = update.GetNumInstances()
	}

	// Update storage size
	if update.StorageSize != nil {
		branchSpec.ClusterSpec.Storage.Size = storageSize(update.GetStorageSize())
	}

	// Update storage QoS class
	if update.StorageQosClass != nil {
		branchSpec.ClusterSpec.Storage = storageWithQoSClass(branchSpec.ClusterSpec.Storage, useStorageQoSClasses, update.GetStorageQosClass())
	}

	// Update resource requests and limits
	if update.VcpuRequest != nil && update.VcpuLimit != nil && update.Memory != nil {
		branchSpec.ClusterSpec.Resources = resourceRequirements(update)
	}

	// Update scale-to-zero configuration
	if update.ScaleToZero != nil {
		branchSpec.ClusterSpec.ScaleToZero = scaleToZeroConfig(update.GetScaleToZero())
	}

	// Update PostgreSQL configuration parameters (merge, don't replace)
	if len(update.GetPostgresConfigurationParameters()) > 0 {
		existingParams := make(map[string]string)
		for _, p := range branchSpec.ClusterSpec.Postgres.Parameters {
			existingParams[p.Name] = p.Value
		}
		maps.Copy(existingParams, update.GetPostgresConfigurationParameters())
		branchSpec.ClusterSpec.Postgres.Parameters = postgresParametersFromMap(existingParams)
	}

	// Update preload libraries (replace)
	if len(update.GetPreloadLibraries()) > 0 {
		branchSpec.ClusterSpec.Postgres.SharedPreloadLibraries = update.GetPreloadLibraries()
	}

	// Update backup configuration. When no existing BackupSpec, create one
	// from scratch (enables backups on a branch that didn't have them).
	// When an existing spec exists, merge schedule/retention to preserve
	// the backup method and pgbackrest settings.
	if update.BackupConfiguration != nil {
		if branchSpec.BackupSpec == nil {
			branchSpec.BackupSpec = backupSpec(update.GetBackupConfiguration())
		} else {
			bc := update.GetBackupConfiguration()
			if bc.GetBackupSchedule() != "" {
				if branchSpec.BackupSpec.ScheduledBackup == nil {
					branchSpec.BackupSpec.ScheduledBackup = &v1alpha1.ScheduledBackupSpec{}
				}
				branchSpec.BackupSpec.ScheduledBackup.Schedule = bc.GetBackupSchedule()
			}
			if bc.GetBackupRetention() != "" {
				branchSpec.BackupSpec.Retention = bc.GetBackupRetention()
				if branchSpec.BackupSpec.IsPgBackRest() {
					var parsed int
					if _, err := fmt.Sscanf(bc.GetBackupRetention(), "%dd", &parsed); err == nil && parsed > 0 {
						branchSpec.BackupSpec.PgBackRest.RetentionFullDays = parsed
					}
				}
			}
		}
	}

	return b
}

// WithClusterFromPool sets the cluster name to the pool cluster and annotates
// the branch with the wakeup pool name so that both the scale-to-zero sidecar
// and the manual hibernation path know to use pool hibernation.
func (b *BranchBuilder) WithClusterFromPool(clusterName, wakeupPool string) *BranchBuilder {
	if clusterName != "" {
		b.branch.Spec.ClusterSpec.Name = &clusterName
		if b.branch.Annotations == nil {
			b.branch.Annotations = make(map[string]string)
		}
		b.branch.Annotations[v1alpha1.WakeupPoolAnnotation] = wakeupPool
	}
	return b
}

// WithDefaultStorageSize sets a default storage size (in Gi) if not set
func (b *BranchBuilder) WithDefaultStorageSize(size int32) *BranchBuilder {
	if b.branch.Spec.ClusterSpec.Storage.Size == "" {
		b.branch.Spec.ClusterSpec.Storage.Size = fmt.Sprintf("%dGi", size)
	}
	return b
}

// WithDefaultStorageClass sets a default storage class if not set
func (b *BranchBuilder) WithDefaultStorageClass(sc string) *BranchBuilder {
	if b.branch.Spec.ClusterSpec.Storage.StorageClass == nil {
		b.branch.Spec.ClusterSpec.Storage.StorageClass = new(sc)
	}
	return b
}

// WithDefaultVolumeSnapshotClass sets a default volume snapshot class if not set
func (b *BranchBuilder) WithDefaultVolumeSnapshotClass(vsc string) *BranchBuilder {
	if b.branch.Spec.ClusterSpec.Storage.VolumeSnapshotClass == nil {
		b.branch.Spec.ClusterSpec.Storage.VolumeSnapshotClass = new(vsc)
	}
	return b
}

// WithStorageQoSClass sets the storage QoS class for the Branch
func (b *BranchBuilder) WithStorageQoSClass(useStorageQoSClasses bool, storageQoSClass string) *BranchBuilder {
	b.branch.Spec.ClusterSpec.Storage = storageWithQoSClass(b.branch.Spec.ClusterSpec.Storage, useStorageQoSClasses, storageQoSClass)
	return b
}

// WithPgBackRest sets the pgbackrest storage backend for the cell's cloud
// provider: gcs for gcp (bucket + optional GCP service account, Workload Identity
// auth), azure for azure (storage account + container, managed identity
// auth), s3 otherwise. Only applies when the backup method is pgbackrest. A
// non-empty s3 endpoint targets a non-AWS S3-compatible store (RustFS,
// Cloudflare R2), which makes the operator authenticate with static
// credentials instead of an IAM role. credentialsSecretName pins the Secret
// holding those credentials to the branch, so later cell-wide credential
// changes don't affect it; empty falls back to the operator-configured
// Secret. On aws without an endpoint, s3 uses the web identity token of the
// branch ServiceAccount in clustersNamespace.
func (b *BranchBuilder) WithPgBackRest(provider, bucket, region, endpoint, serviceAccount, credentialsSecretName, azureAccount, azureContainer, clustersNamespace string) *BranchBuilder {
	if b.branch.Spec.BackupSpec == nil || !b.branch.Spec.BackupSpec.IsPgBackRest() {
		return b
	}

	switch provider {
	case CloudProviderGCP:
		b.branch.Spec.BackupSpec.PgBackRest.GCS = &v1alpha1.PgBackRestGCSSpec{
			Bucket:              bucket,
			ServiceAccountEmail: serviceAccount,
			KeyType:             pgbackrest.KeyTypeWebID,
		}
		// With web identity the token has access only to the managed folder of
		// the branch, named after the branch. Pin the repo path to that folder
		// so the repository sits inside the grant. Without it the repo path
		// defaults to the stanza, which differs from the branch name once the
		// branch is restored (the stanza gets a -N suffix) and then falls
		// outside the granted folder. Set only on new branches, so existing
		// branches keep their repository.
		b.branch.Spec.BackupSpec.PgBackRest.RepoPath = b.branch.Name
	case CloudProviderAzure:
		b.branch.Spec.BackupSpec.PgBackRest.Azure = &v1alpha1.PgBackRestAzureSpec{
			Account:   azureAccount,
			Container: azureContainer,
		}
	case CloudProviderAWS:
		b.branch.Spec.BackupSpec.PgBackRest.S3 = pgBackRestS3Spec(bucket, region, endpoint, credentialsSecretName)
		// Without an endpoint, the branch operator keeps a ServiceAccount and a
		// web identity token for each branch. Use them with the repo path that
		// the IAM policy allows for the token. The values are set only on new
		// branches, so existing branches keep their repository.
		//
		// TODO(Martin): Select static keys with credentialsSecretName, not with
		// the endpoint. clararx has no IAM role on its bare-metal nodes and sets
		// the AWS S3 endpoint https://s3.us-west-1.amazonaws.com only to get
		// static keys. After the change, clararx can remove the endpoint, and
		// local dev must set credentialsSecret for RustFS.
		if endpoint == "" {
			b.branch.Spec.BackupSpec.PgBackRest.S3.KeyType = pgbackrest.KeyTypeWebID
			b.branch.Spec.BackupSpec.PgBackRest.RepoPath = pgbackrest.RepoPath(clustersNamespace, b.branch.Name)
		}
	default:
		b.branch.Spec.BackupSpec.PgBackRest.S3 = pgBackRestS3Spec(bucket, region, endpoint, credentialsSecretName)
	}

	return b
}

// WithPgBackRestCipher references the cipher Secret that the branch operator
// creates for the Branch. Only applies when the backup method is pgbackrest.
func (b *BranchBuilder) WithPgBackRestCipher(encryptionEnabled bool) *BranchBuilder {
	if !encryptionEnabled || !b.branch.Spec.BackupSpec.IsPgBackRest() {
		return b
	}

	b.branch.Spec.BackupSpec.PgBackRest.CipherPassphraseSecretRef = &corev1.SecretKeySelector{
		Name: pgbackrest.CipherSecretName(b.branch.Name),
		Key:  pgbackrest.CipherPassphraseKey,
	}
	return b
}

// WithPgBackRestRestoreCipher references the restore cipher when the Branch
// restores from the encrypted pgBackRest repository of source. Only applies to
// an object store restore when the backup method is pgbackrest and the source
// is encrypted.
func (b *BranchBuilder) WithPgBackRestRestoreCipher(source *v1alpha1.Branch) *BranchBuilder {
	if source == nil || !b.branch.Spec.Restore.IsObjectStoreType() || !b.branch.Spec.BackupSpec.IsPgBackRest() {
		return b
	}
	if !source.Spec.BackupSpec.IsPgBackRest() || source.Spec.BackupSpec.PgBackRest.CipherPassphraseSecretRef == nil {
		return b
	}

	b.branch.Spec.Restore.PgBackRestCipherPassphraseSecretRef = &corev1.SecretKeySelector{
		Name: pgbackrest.CipherSecretName(b.branch.Name),
		Key:  pgbackrest.RestoreCipherPassphraseKey,
	}
	return b
}

// WithPgBackRestRestoreSource points an object store restore at the pgBackRest
// repository of source. Only applies when the Branch and source both use
// pgbackrest.
func (b *BranchBuilder) WithPgBackRestRestoreSource(source *v1alpha1.Branch) *BranchBuilder {
	if source == nil || !b.branch.Spec.Restore.IsObjectStoreType() || !b.branch.Spec.BackupSpec.IsPgBackRest() || !source.Spec.BackupSpec.IsPgBackRest() {
		return b
	}

	pgb := source.Spec.BackupSpec.PgBackRest.DeepCopy()
	b.branch.Spec.Restore.PgBackRest = &v1alpha1.RestorePgBackRestSpec{
		RepoPath:   pgb.GetRepoPath(source.Name),
		StanzaName: pgb.GetStanzaName(source.Name),
		S3:         pgb.S3,
		GCS:        pgb.GCS,
		Azure:      pgb.Azure,
	}
	return b
}

// pgBackRestS3Spec returns the S3 backend of a new branch.
func pgBackRestS3Spec(bucket, region, endpoint, credentialsSecretName string) *v1alpha1.PgBackRestS3Spec {
	return &v1alpha1.PgBackRestS3Spec{
		Bucket:                bucket,
		Region:                region,
		Endpoint:              endpoint,
		CredentialsSecretName: credentialsSecretName,
	}
}

// WithUtilsPreloadLibrary makes the security library of the Branch's SQL
// identity the first entry in the SharedPreloadLibraries list. For a
// non-legacy identity it also removes the libraries of other profiles. Set
// the SQL identity first.
func (b *BranchBuilder) WithUtilsPreloadLibrary() *BranchBuilder {
	if b.branch.Spec.ClusterSpec.Postgres == nil {
		b.branch.Spec.ClusterSpec.Postgres = &v1alpha1.PostgresConfiguration{}
	}

	id := shared.BranchSQLIdentity(b.branch)
	lib := id.UtilsLibrary
	libs := slices.DeleteFunc(slices.Clone(b.branch.Spec.ClusterSpec.Postgres.SharedPreloadLibraries), func(s string) bool {
		return s == lib || (id != shared.LegacySQLIdentity() && shared.IsUtilsLibrary(s))
	})
	b.branch.Spec.ClusterSpec.Postgres.SharedPreloadLibraries = append([]string{lib}, libs...)

	return b
}

// WithMandatoryPostgresParameters applies mandatory PostgreSQL parameter
// overrides for the Branch's SQL identity to the Branch's Postgres
// configuration. Settings of any security library that the request set are
// removed. Set the SQL identity and the preload libraries first.
func (b *BranchBuilder) WithMandatoryPostgresParameters() *BranchBuilder {
	if b.branch.Spec.ClusterSpec.Postgres == nil {
		b.branch.Spec.ClusterSpec.Postgres = &v1alpha1.PostgresConfiguration{}
	}
	pg := b.branch.Spec.ClusterSpec.Postgres

	postgresParams := make(map[string]string)
	for _, p := range pg.Parameters {
		postgresParams[p.Name] = p.Value
	}

	applyPostgresParameterOverrides(postgresParams, b.branch.Spec.ClusterSpec.Image,
		pg.SharedPreloadLibraries, shared.BranchSQLIdentity(b.branch))

	pg.Parameters = postgresParametersFromMap(postgresParams)

	return b
}

// WithDefaultNodeSelector sets the default node selector for the cluster pods
func (b *BranchBuilder) WithDefaultNodeSelector(nodeSelector map[string]string) *BranchBuilder {
	if len(nodeSelector) == 0 {
		return b
	}
	b.branch.Spec.ClusterSpec.Affinity = &v1alpha1.AffinitySpec{
		NodeSelector: nodeSelector,
	}
	return b
}

// WithPooler enables a PgBouncer connection pooler for the branch.
func (b *BranchBuilder) WithPooler(enabled bool) *BranchBuilder {
	if enabled {
		b.branch.Spec.Pooler = &v1alpha1.PoolerSpec{
			Instances:     1,
			Mode:          v1alpha1.PoolModeTransaction,
			MaxClientConn: "10000",
		}
	}
	return b
}

// Build returns the built Branch.
func (b *BranchBuilder) Build() *v1alpha1.Branch {
	return b.branch
}

// storageWithQoSClass returns the StorageSpec with its VolumeAttributesClass
// set for the given storage QoS class
func storageWithQoSClass(storage v1alpha1.StorageSpec, useStorageQoSClasses bool, storageQoSClass string) v1alpha1.StorageSpec {
	if !useStorageQoSClasses || storage.StorageClass == nil {
		return storage
	}

	// Look up the VolumeAttributesClass for the branch's storage class; not
	// every storage class supports VolumeAttributesClasses, and unknown QoS
	// class names are ignored
	vac, ok := volumeAttributesClassNames[*storage.StorageClass][storageQoSClass]
	if !ok {
		return storage
	}

	storage.VolumeAttributesClass = new(vac)
	return storage
}

// scaleToZeroConfig converts a clustersv1.ScaleToZero to a
// v1alpha1.ScaleToZeroConfiguration
func scaleToZeroConfig(s *clustersv1.ScaleToZero) *v1alpha1.ScaleToZeroConfiguration {
	if s == nil {
		return nil
	}

	return &v1alpha1.ScaleToZeroConfiguration{
		Enabled:                 s.GetEnabled(),
		InactivityPeriodMinutes: int32(s.GetInactivityPeriodMinutes()),
	}
}

// backupSpec converts a clustersv1.BackupConfiguration to a
// v1alpha1.BackupSpec
func backupSpec(b *clustersv1.BackupConfiguration) *v1alpha1.BackupSpec {
	if b == nil || !b.GetBackupsEnabled() {
		return nil
	}

	// Convert the scheduled backup spec if a schedule is provided
	var sbSpec *v1alpha1.ScheduledBackupSpec
	if b.GetBackupSchedule() != "" {
		sbSpec = &v1alpha1.ScheduledBackupSpec{
			Schedule: b.GetBackupSchedule(),
		}
	}

	retentionDays := DefaultPgBackRestRetentionDays
	var parsed int
	if _, err := fmt.Sscanf(b.GetBackupRetention(), "%dd", &parsed); err == nil && parsed > 0 {
		retentionDays = parsed
	}

	spec := &v1alpha1.BackupSpec{
		Method:          v1alpha1.BackupMethodPgBackRest,
		ScheduledBackup: sbSpec,
		Retention:       b.GetBackupRetention(),
		PgBackRest: &v1alpha1.PgBackRestSpec{
			RetentionFullDays:   retentionDays,
			CompressType:        DefaultPgBackRestCompressType,
			ArchiveAsync:        DefaultPgBackRestArchiveAsync,
			ArchivePushQueueMax: DefaultPgBackRestPushQueueMax,
			ArchiveGetQueueMax:  DefaultPgBackRestGetQueueMax,
		},
	}

	return spec
}

// restoreSpec converts a CreatePostgresClusterRequest data source to a *v1alpha1.RestoreSpec
func restoreSpec(req *clustersv1.CreatePostgresClusterRequest) *v1alpha1.RestoreSpec {
	switch ds := req.GetDataSource().(type) {
	case *clustersv1.CreatePostgresClusterRequest_ClusterSnapshot:
		return &v1alpha1.RestoreSpec{
			Type: v1alpha1.RestoreTypeVolumeSnapshot,
			Name: ds.ClusterSnapshot.GetClusterId(),
		}
	case *clustersv1.CreatePostgresClusterRequest_ContinuousBackup:
		spec := &v1alpha1.RestoreSpec{
			Type: v1alpha1.RestoreTypeObjectStore,
			Name: ds.ContinuousBackup.GetClusterId(),
		}
		if ds.ContinuousBackup.GetTimestamp() != nil {
			spec.Timestamp = &v1.Time{Time: ds.ContinuousBackup.GetTimestamp().AsTime()}
		}
		return spec
	case *clustersv1.CreatePostgresClusterRequest_BaseBackup:
		return &v1alpha1.RestoreSpec{
			Type: v1alpha1.RestoreTypeBaseBackup,
			Name: ds.BaseBackup.GetBackupId(),
		}
	default:
		return nil
	}
}

// storageSize formats a storage size in Gi to a string
func storageSize(sizeGi int32) string {
	if sizeGi == 0 {
		return ""
	}
	return fmt.Sprintf("%dGi", sizeGi)
}

// postgresParametersFromMap converts a map of PostgreSQL parameters to a slice
// of v1alpha1.PostgresParameter
func postgresParametersFromMap(params map[string]string) []v1alpha1.PostgresParameter {
	// Extract and sort keys to ensure deterministic output
	keys := slices.Sorted(maps.Keys(params))

	// Build the slice in sorted order
	pgParams := make([]v1alpha1.PostgresParameter, 0, len(keys))
	for _, k := range keys {
		pgParams = append(pgParams, v1alpha1.PostgresParameter{
			Name:  k,
			Value: params[k],
		})
	}

	return pgParams
}

// poolerMemoryReservation is the amount of memory reserved on each node for
// the PgBouncer pooler pod. The cluster memory request and limit are reduced
// by this amount so both PostgreSQL and PgBouncer fit on the same node.
var poolerMemoryReservation = resource.MustParse("100Mi")

// resourceRequirements converts ClusterConfiguration resource fields to
// corev1.ResourceRequirements. The memory request and limit are reduced by
// poolerMemoryReservation to leave room for the PgBouncer pooler pod.
func resourceRequirements(cfg resourceConfigurer) corev1.ResourceRequirements {
	var reqs corev1.ResourceRequirements

	// Build Requests if values are provided
	reqs.Requests = corev1.ResourceList{}
	if cfg.GetVcpuRequest() != "" {
		reqs.Requests[corev1.ResourceCPU] = resource.MustParse(cfg.GetVcpuRequest())
	}
	if cfg.GetMemory() != "" {
		mem := resource.MustParse(cfg.GetMemory())
		mem.Sub(poolerMemoryReservation)
		reqs.Requests[corev1.ResourceMemory] = mem
	}

	// Build Limits if values are provided
	reqs.Limits = corev1.ResourceList{}
	if cfg.GetVcpuLimit() != "" {
		reqs.Limits[corev1.ResourceCPU] = resource.MustParse(cfg.GetVcpuLimit())
	}
	if cfg.GetMemory() != "" {
		mem := resource.MustParse(cfg.GetMemory())
		mem.Sub(poolerMemoryReservation)
		reqs.Limits[corev1.ResourceMemory] = mem
	}

	return reqs
}
