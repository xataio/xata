package branch

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"xata/internal/flags"
	"xata/internal/postgresversions"
	"xata/services/projects/provisioner"
	"xata/services/projects/store"

	clustersv1 "xata/gen/proto/clusters/v1"
)

// BuildPayload assembles the provisioner payload for the requested creation
// mode, validating the configuration, image, region, instance type, and
// marketplace region. The returned payload carries the resolved Region before
// any provisioning happens, so a transport can set its region o11y attribute
// from it and keep that attribute on failed provisioning too.
//
// Branch creation is a two-step call — BuildPayload then Provision — precisely
// so the caller can act on the resolved region between the two. Each call
// returns a fresh payload; Provision mutates it, so build one payload per branch.
func (s *Service) BuildPayload(ctx context.Context, in CreateInput) (*provisioner.ClusterServicePayload, error) {
	// Validate request inputs here, not in the REST handler, so this is the single
	// validation gate for every caller — and it runs before the expensive lookups
	// in the mode switch below, preserving the handler's fast-fail behavior.
	if in.Name == "" {
		return nil, ErrInvalidParam{Param: "name", Message: "branch name is required"}
	}
	if err := ValidateBackup(in.Name, in.Backup); err != nil {
		return nil, err
	}

	switch mode := in.Mode.(type) {
	// mode: inherit - we create a child branch
	case FromParent:
		// keeping feature flag separate from other checks for visibility
		if s.feat.BoolValue(ctx, flags.ChildBranchCreationDisabled) {
			return nil, ErrChildBranchCreationDisabled{}
		}
		payload, err := s.PayloadFromParent(ctx, in.OrganizationID, in.ProjectID, in.Name, mode.ParentID)
		if err != nil {
			return nil, err
		}
		if err := s.ValidateMarketplaceRegion(ctx, MarketplaceRegionParams{
			OrganizationID: in.OrganizationID,
			Marketplace:    in.Marketplace,
			Region:         payload.Region,
		}); err != nil {
			return nil, ErrInvalidParam{BranchName: in.Name, Param: "region", Message: err.Error()}
		}
		return &payload, nil
	// mode custom - we create a main branch
	case FromConfiguration:
		payload, err := s.PayloadFromConfiguration(ctx, in.OrganizationID, in.ProjectID, in.Name, mode.Config, in.OrgLimits, in.Marketplace)
		if err != nil {
			return nil, err
		}
		return &payload, nil
	default:
		return nil, fmt.Errorf("unsupported branch creation mode: %T", in.Mode)
	}
}

// Provision finalizes the payload from BuildPayload, provisions the branch
// under the project lock, and returns it with its connection string. Callers
// resolve the input and shape the response/analytics around it.
//
// It is transport agnostic: it does not touch the HTTP request/response (no
// region o11y attribute, no Retry-After header). On lock contention it returns
// an error wrapping store.ErrProjectBusy so the caller can surface Retry-After,
// unless in.ProjectLockHeld says the caller already holds the lock, in which
// case Provision does not touch it.
//
// Provision mutates payload in place to finalize it, so it takes ownership of
// the value BuildPayload returned: pair each Provision with its own BuildPayload
// call (one per branch) and never reuse a payload across two Provision calls.
func (s *Service) Provision(ctx context.Context, in CreateInput, payload *provisioner.ClusterServicePayload) (*store.Branch, string, error) {
	// Input validation (name, backup format/range) happened in BuildPayload; this
	// backups-disabled check stays here because it needs the region's
	// BackupsEnabled from the built payload.
	if !payload.BackupsEnabled && in.Backup != nil {
		return nil, "", ErrInvalidParam{BranchName: in.Name, Param: "backupConfiguration", Message: "backup configuration cannot be specified when backups are disabled in the selected region"}
	}

	// Populate remaining payload fields for the provisioner
	payload.Description = in.Description
	payload.BackupRetentionPeriod = BackupRetentionDays(in.Backup)
	payload.BackupConfig = ClustersBackupConfig(in.Backup, payload.BackupsEnabled, in.UsePgBackRest)
	payload.Flags = in.Flags
	payload.UsageTier = in.UsageTier
	payload.Limits = in.OrgLimits.StoreLimits()

	if in.ScaleToZero != nil {
		payload.Configuration.ScaleToZero = &clustersv1.ScaleToZero{
			Enabled:                 in.ScaleToZero.Enabled,
			InactivityPeriodMinutes: int64(in.ScaleToZero.InactivityPeriodMinutes),
		}
	}

	// The store takes the project lock in shared mode inside the create
	// transaction, so it guards against races with IP filtering updates only
	// while the branch is created, and is released before the read-only
	// connection-string fetch below (a cell round-trip). When the caller already
	// holds the lock (in.ProjectLockHeld), the store must not take it again.
	payload.ProjectLockHeld = in.ProjectLockHeld
	created, err := s.provisioner.CreateBranch(ctx, in.ProjectID, in.OrganizationID, in.Name, payload)
	if err != nil {
		if notFound, ok := errors.AsType[provisioner.ErrBranchNotFound](err); ok {
			return nil, "", ErrBranchNotFound{BranchID: notFound.BranchID}
		}
		if invalidCfg, ok := errors.AsType[provisioner.ErrInvalidConfiguration](err); ok {
			return nil, "", ErrInvalidParam{BranchName: in.Name, Param: "configuration", Message: invalidCfg.Message}
		}
		if unhealthy, ok := errors.AsType[provisioner.ErrParentBranchUnhealthy](err); ok {
			return nil, "", ErrParentBranchUnhealthy{ParentID: unhealthy.ParentID}
		}
		return nil, "", err
	}

	// swallow the error, the resource got created and the connection string will be eventually available
	connString, _ := s.ConnectionString(ctx, in.OrganizationID, created)
	return created, connString, nil
}

// PayloadFromParent builds the provisioner payload for a child branch that
// inherits its parent's configuration and data.
func (s *Service) PayloadFromParent(ctx context.Context, organizationID, projectID, branchName, parentID string) (provisioner.ClusterServicePayload, error) {
	if parentID == "" {
		return provisioner.ClusterServicePayload{}, ErrInvalidParam{BranchName: branchName, Param: "parentID", Message: "parentId is required for 'inherit' mode"}
	}

	// get the cell ID from the parent branch
	parentBranch, err := s.store.DescribeBranch(ctx, organizationID, projectID, parentID)
	if err != nil {
		if _, ok := errors.AsType[store.ErrBranchNotFound](err); ok {
			return provisioner.ClusterServicePayload{}, ErrBranchNotFound{BranchID: parentID}
		}
		return provisioner.ClusterServicePayload{}, err
	}

	return provisioner.ClusterServicePayload{
		ParentID: &parentID,
		// If the parent branch ID is present, all settings (including vcpu, memory, and Postgres parameters) get copied from the
		// parent branch. This means we don't need to send them over, they just get copied locally in the cell.
		Configuration:  clustersv1.ClusterConfiguration{},
		CellID:         parentBranch.CellID,
		Region:         parentBranch.Region,
		BackupsEnabled: parentBranch.BackupsEnabled,
	}, nil
}

// PayloadFromConfiguration builds the provisioner payload for a root branch from
// an explicit configuration.
func (s *Service) PayloadFromConfiguration(ctx context.Context, organizationID, projectID, branchName string, cfg Configuration, orgLimits provisioner.OrgLimits, marketplace string) (provisioner.ClusterServicePayload, error) {
	if err := s.validateConfiguration(branchName, cfg, orgLimits); err != nil {
		return provisioner.ClusterServicePayload{}, err
	}

	// validate image - from this moment on, the image is in the correct format, no need for prefix, suffix, extra validation
	validImageFormat, err := s.ValidateImage(ctx, organizationID, cfg.Image)
	if err != nil {
		return provisioner.ClusterServicePayload{}, ErrInvalidParam{BranchName: branchName, Param: "configuration", Message: "invalid image: " + err.Error()}
	}

	region, err := s.store.GetRegion(ctx, organizationID, cfg.Region)
	if err != nil {
		return provisioner.ClusterServicePayload{}, ErrInvalidParam{BranchName: branchName, Param: "configuration", Message: "invalid region: " + err.Error()}
	}
	if err := validateRegionForMarketplace(marketplace, *region); err != nil {
		return provisioner.ClusterServicePayload{}, ErrInvalidParam{BranchName: branchName, Param: "region", Message: err.Error()}
	}

	// allocate to a cell in the region
	cellID, err := s.allocateCell(ctx, organizationID, branchName, cfg.Region)
	if err != nil {
		return provisioner.ClusterServicePayload{}, err
	}

	// look up the instance type, enforcing the org's compute limit
	it, err := s.InstanceTypeByName(ctx, organizationID, cfg.Region, cfg.InstanceType, orgLimits.MaxAllowedInstanceType)
	if err != nil {
		return provisioner.ClusterServicePayload{}, ErrInvalidParam{BranchName: branchName, Param: "instanceType", Message: err.Error()}
	}
	// Extract major version from image name
	majorVersion := postgresversions.ExtractMajorVersionFromImage(validImageFormat)

	// use configured preload libraries if provided, otherwise use defaults
	var preloadLibraries []string
	if cfg.PreloadLibraries != nil && len(*cfg.PreloadLibraries) > 0 {
		if err := s.postgresConfigProvider.ValidatePreloadLibraries(validImageFormat, *cfg.PreloadLibraries); err != nil {
			return provisioner.ClusterServicePayload{}, ErrInvalidParam{BranchName: branchName, Param: "preloadLibraries", Message: err.Error()}
		}
		preloadLibraries = *cfg.PreloadLibraries
	} else {
		preloadLibraries, err = s.postgresConfigProvider.GetDefaultPreloadLibraries(validImageFormat)
		if err != nil {
			return provisioner.ClusterServicePayload{}, ErrInvalidParam{BranchName: branchName, Param: "image", Message: fmt.Sprintf("failed to get default preload libraries: %v", err)}
		}
	}

	// validate configured postgres parameters if provided
	if cfg.PostgresConfigurationParameters != nil {
		errs, err := s.postgresConfigProvider.ValidateSettings(cfg.InstanceType, *cfg.PostgresConfigurationParameters, majorVersion, validImageFormat, preloadLibraries)
		if err != nil {
			return provisioner.ClusterServicePayload{}, ErrInvalidParam{BranchName: branchName, Param: "postgresConfigurationParameters", Message: fmt.Sprintf("validation failed: %v", err)}
		}
		if errs != nil {
			paramNames := slices.Sorted(maps.Keys(errs))
			var errorMessages []string
			for _, paramName := range paramNames {
				errorMessages = append(errorMessages, fmt.Sprintf("%s: %s", paramName, errs[paramName].Error()))
			}
			return provisioner.ClusterServicePayload{}, ErrInvalidParam{BranchName: branchName, Param: "postgresConfigurationParameters", Message: strings.Join(errorMessages, "; ")}
		}
	}

	// compute default Postgres parameters based on instance type, image, and preloaded extensions
	postgresParameters, err := s.postgresConfigProvider.GetDefaultPostgresParameters(cfg.InstanceType, majorVersion, validImageFormat, preloadLibraries)
	if err != nil {
		return provisioner.ClusterServicePayload{}, ErrInvalidParam{BranchName: branchName, Param: "instanceType", Message: fmt.Sprintf("failed to compute Postgres parameters: %v", err)}
	}

	// merge configured postgres parameters if provided (they override defaults)
	if cfg.PostgresConfigurationParameters != nil {
		maps.Copy(postgresParameters, *cfg.PostgresConfigurationParameters)
	}

	numInstances := cfg.Replicas + 1 // the primary is always created

	// storage QoS class is optional, so only set it if it's not empty
	var storageQoSClass *string
	if it.StorageQoSClass != "" {
		storageQoSClass = &it.StorageQoSClass
	}

	// Zero leaves the cluster service to apply its configured default.
	var storageSize int32
	if cfg.Storage != nil {
		storageSize = *cfg.Storage
	}

	return provisioner.ClusterServicePayload{
		ParentID: nil,
		Configuration: clustersv1.ClusterConfiguration{
			NumInstances:                    numInstances,
			ImageName:                       validImageFormat,
			VcpuRequest:                     it.CPURequest(),
			VcpuLimit:                       it.CPULimit(),
			Memory:                          it.Memory(),
			PostgresConfigurationParameters: postgresParameters,
			PreloadLibraries:                preloadLibraries,
			StorageQosClass:                 storageQoSClass,
			StorageSize:                     storageSize,
		},
		CellID:         cellID,
		Region:         cfg.Region,
		BackupsEnabled: region.BackupsEnabled,
	}, nil
}

func (s *Service) validateConfiguration(name string, cfg Configuration, orgLimits provisioner.OrgLimits) error {
	if cfg == (Configuration{}) {
		return ErrInvalidParam{BranchName: name, Param: "configuration", Message: "configuration is required for 'custom' mode"}
	}
	if cfg.Storage != nil {
		if err := ValidateStorage(name, *cfg.Storage, orgLimits.MaxStorageGBPerBranch); err != nil {
			return err
		}
	}
	return ValidateReplicaCount(name, cfg.Replicas, orgLimits.MinInstancesPerBranch, orgLimits.MaxInstancesPerBranch)
}

// ValidateStorage validates a requested storage size (in GB) against the org's
// limit. A max of 0 means "no limit". Exported so the api validator delegates
// here instead of keeping its own copy.
func ValidateStorage(branchName string, storageGB int32, max int) error {
	if storageGB < 1 {
		return ErrInvalidParam{BranchName: branchName, Param: "storage", Message: "storage size must be at least 1 GB"}
	}
	if max > 0 && storageGB > int32(max) {
		return ErrInvalidParam{BranchName: branchName, Param: "storage", Message: fmt.Sprintf("storage size exceeds the maximum of %d GB", max)}
	}
	return nil
}

// ValidateReplicaCount validates that the requested replica count is within the
// organization's instance limits.
func ValidateReplicaCount(branchName string, replicas int32, min, max int) error {
	numInstances := replicas + 1
	if numInstances < int32(min) {
		return ErrInvalidParam{BranchName: branchName, Param: "configuration", Message: fmt.Sprintf("number of replicas requires at least %d instance(s)", min)}
	}
	if numInstances > int32(max) {
		return ErrInvalidParam{BranchName: branchName, Param: "configuration", Message: fmt.Sprintf("number of replicas exceeds the maximum of %d instance(s)", max)}
	}
	return nil
}

// allocateCell allocates a cell in the region.
func (s *Service) allocateCell(ctx context.Context, organizationID, branchName, regionID string) (string, error) {
	cellList, err := s.store.ListCells(ctx, organizationID, regionID)
	if err != nil {
		return "", err
	}

	if len(cellList) == 0 {
		return "", ErrInvalidParam{BranchName: branchName, Param: "region", Message: "cannot allocate to given region"}
	}

	strategy := s.sched.StrategyForRegion(regionID)
	cell, err := strategy.Schedule(ctx, cellList)
	if err != nil {
		return "", fmt.Errorf("failed to schedule branch %q: %w", branchName, err)
	}

	return cell.ID, nil
}
