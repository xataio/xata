package clusters

import (
	"fmt"
	"maps"
	"slices"

	clustersv1 "xata/gen/proto/clusters/v1"
	"xata/services/branch-operator/api/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

const (
	LabelOrgID           = "xata.io/organizationID"
	LabelProjectID       = "xata.io/projectID"
	XataUtilsPreloadName = "xatautils"
)

// BranchBuilder builds Branch resources.
type BranchBuilder struct {
	branch *v1alpha1.Branch
}

// NewBranchBuilder creates a new BranchBuilder.
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
		ObjectMeta: v1.ObjectMeta{
			Name: r.GetId(),
			Labels: map[string]string{
				LabelOrgID:     r.GetOrganizationId(),
				LabelProjectID: r.GetProjectId(),
			},
		},
		Spec: v1alpha1.BranchSpec{
			InheritedMetadata: &v1alpha1.InheritedMetadata{
				Labels: map[string]string{
					LabelOrgID:     r.GetOrganizationId(),
					LabelProjectID: r.GetProjectId(),
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
	clusterSpec.Storage.StorageClass = parent.Spec.ClusterSpec.Storage.StorageClass
	clusterSpec.Storage.VolumeSnapshotClass = parent.Spec.ClusterSpec.Storage.VolumeSnapshotClass
	clusterSpec.Storage.MountPropagation = parent.Spec.ClusterSpec.Storage.MountPropagation
	clusterSpec.Image = parent.Spec.ClusterSpec.Image
	clusterSpec.Resources = parent.Spec.ClusterSpec.Resources
	clusterSpec.Postgres = parent.Spec.ClusterSpec.Postgres
	clusterSpec.SmartShutdownTimeout = parent.Spec.ClusterSpec.SmartShutdownTimeout

	// Child branches always start with one instance, regardless of the parent's
	// instance count
	clusterSpec.Instances = 1

	// Disable backups on the Branch if they are not enabled on the parent
	if parent.Spec.BackupSpec == nil {
		b.branch.Spec.BackupSpec = nil
	}

	return b
}

// WithUpdatesFrom configures the BranchBuilder with updates from the
// UpdatePostgresClusterRequest.
func (b *BranchBuilder) WithUpdatesFrom(req *clustersv1.UpdatePostgresClusterRequest) *BranchBuilder {
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
			} else {
				b.branch.Spec.ClusterSpec.Hibernation = ptr.To(v1alpha1.HibernationModeEnabled)
			}
			// If waking up, modify the Branch spec appropriately based on whether it
			// uses a wakeup pool or not.
		} else {
			if b.branch.HasWakeupPoolAnnotation() {
				b.branch.Spec.ClusterSpec.Hibernation = nil
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

	// Update backup configuration
	if update.BackupConfiguration != nil {
		branchSpec.BackupSpec = backupSpec(update.GetBackupConfiguration())
	}

	return b
}

// WithClusterFromPool sets the cluster name to the pool cluster, disables
// backups (pool clusters don't have the barman-cloud plugin), and annotates the
// branch with the wakeup pool name so that both the scale-to-zero sidecar and
// the manual hibernation path know to use pool hibernation.
func (b *BranchBuilder) WithClusterFromPool(clusterName, wakeupPool string) *BranchBuilder {
	if clusterName != "" {
		b.branch.Spec.ClusterSpec.Name = &clusterName
		b.branch.Spec.BackupSpec = nil
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

// WithXataUtilsPreloadLibrary ensures that the "xatautils" library is the
// first entry in the SharedPreloadLibraries list.
func (b *BranchBuilder) WithXataUtilsPreloadLibrary() *BranchBuilder {
	if b.branch.Spec.ClusterSpec.Postgres == nil {
		b.branch.Spec.ClusterSpec.Postgres = &v1alpha1.PostgresConfiguration{}
	}

	libs := b.branch.Spec.ClusterSpec.Postgres.SharedPreloadLibraries

	// Remove all existing entries
	libs = slices.DeleteFunc(libs, func(s string) bool {
		return s == XataUtilsPreloadName
	})

	// Add "xatautils" as the first entry
	libs = append([]string{XataUtilsPreloadName}, libs...)
	b.branch.Spec.ClusterSpec.Postgres.SharedPreloadLibraries = libs

	return b
}

// WithMandatoryPostgresParameters applies mandatory PostgreSQL parameter
// overrides to the Branch's Postgres configuration.
func (b *BranchBuilder) WithMandatoryPostgresParameters() *BranchBuilder {
	if b.branch.Spec.ClusterSpec.Postgres == nil {
		b.branch.Spec.ClusterSpec.Postgres = &v1alpha1.PostgresConfiguration{}
	}

	// Convert existing parameters to a map for easy merging
	postgresParams := make(map[string]string)
	for _, p := range b.branch.Spec.ClusterSpec.Postgres.Parameters {
		postgresParams[p.Name] = p.Value
	}

	// Apply mandatatory postgres parameters using the branch's image
	maps.Copy(postgresParams, MandatoryPostgresParameters(b.branch.Spec.ClusterSpec.Image))

	// Convert back to slice and set on Branch spec
	b.branch.Spec.ClusterSpec.Postgres.Parameters = postgresParametersFromMap(postgresParams)

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

	return &v1alpha1.BackupSpec{
		ScheduledBackup: sbSpec,
		Retention:       b.GetBackupRetention(),
	}
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
