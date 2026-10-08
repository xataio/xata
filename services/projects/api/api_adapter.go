package api

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"xata/internal/postgresversions"

	clustersv1 "xata/gen/proto/clusters/v1"
	"xata/internal/postgrescfg"
	"xata/services/projects/api/spec"
	branchsvc "xata/services/projects/branch"
	"xata/services/projects/store"
)

const (
	defaultInactivityDuration = 30 * time.Minute
)

func storeToAPIProjectList(projects []store.Project) []spec.Project {
	apiProjects := make([]spec.Project, len(projects))
	for i, project := range projects {
		apiProjects[i] = *storeToAPIProject(&project)
	}
	return apiProjects
}

func storeToAPIProject(project *store.Project) *spec.Project {
	config := spec.ProjectConfiguration{
		ScaleToZero: spec.ProjectScaleToZeroConfiguration{
			BaseBranches:  storeToAPIScaleToZeroConfig(project.ScaleToZero.BaseBranches),
			ChildBranches: storeToAPIScaleToZeroConfig(project.ScaleToZero.ChildBranches),
		},
	}

	if len(project.IPFiltering.CIDRs) > 0 || project.IPFiltering.Enabled {
		ipFilteringConfig := spec.IPFilteringConfiguration{
			Enabled: project.IPFiltering.Enabled,
			Cidr:    storeToAPICIDREntries(project.IPFiltering.CIDRs),
		}
		config.IpFiltering = &ipFilteringConfig
	}

	return &spec.Project{
		Id:            project.ID,
		Name:          project.Name,
		CreatedAt:     project.CreatedAt,
		UpdatedAt:     project.UpdatedAt,
		Configuration: config,
	}
}

func storeToAPIBranchShortMetadata(branch *store.Branch, connString string) *spec.BranchShortMetadata {
	var connStringPtr *string
	if connString != "" {
		connStringPtr = &connString
	}
	return &spec.BranchShortMetadata{
		Id:               branch.ID,
		Name:             branch.Name,
		ParentID:         branch.ParentID,
		CreatedAt:        branch.CreatedAt,
		UpdatedAt:        branch.UpdatedAt,
		Description:      branch.Description,
		ConnectionString: connStringPtr,
		Region:           getRegion(branch.Region),
		PublicAccess:     branch.PublicAccess,
	}
}

func storeToAPIBranchListMetadata(branch *store.Branch) *spec.BranchListMetadata {
	return &spec.BranchListMetadata{
		Id:             branch.ID,
		Name:           branch.Name,
		ParentID:       branch.ParentID,
		CreatedAt:      branch.CreatedAt,
		UpdatedAt:      branch.UpdatedAt,
		Description:    branch.Description,
		Region:         getRegion(branch.Region),
		PublicAccess:   branch.PublicAccess,
		BackupsEnabled: branch.BackupsEnabled,
	}
}

func storeToAPIBranchMetadata(branch *store.Branch, connString, instanceType string, cluster *clustersv1.DescribePostgresClusterResponse) *spec.BranchMetadata {
	var connStringPtr *string
	if connString != "" {
		connStringPtr = &connString
	}

	return &spec.BranchMetadata{
		Id:               branch.ID,
		Name:             branch.Name,
		ParentID:         branch.ParentID,
		CreatedAt:        branch.CreatedAt,
		UpdatedAt:        branch.UpdatedAt,
		Description:      branch.Description,
		ConnectionString: connStringPtr,
		Configuration: spec.ClusterConfiguration{
			Storage:                         new(cluster.Configuration.StorageSize),
			Image:                           postgresversions.ShortImageName(cluster.Configuration.ImageName),
			InstanceType:                    instanceType,
			Replicas:                        cluster.Configuration.NumInstances - 1,
			PostgresConfigurationParameters: new(cluster.Configuration.PostgresConfigurationParameters),
			PreloadLibraries:                new(cluster.Configuration.PreloadLibraries),
		},
		Status:              mapStatus(cluster.Status),
		Region:              getRegion(branch.Region),
		PublicAccess:        branch.PublicAccess,
		BackupsEnabled:      branch.BackupsEnabled,
		ScaleToZero:         clustersToAPIScaleToZero(cluster.Configuration.ScaleToZero),
		BackupConfiguration: clustersToAPIBackupConfig(cluster.BackupConfiguration),
	}
}

func storeToAPIListBranchListMetadata(branches []store.Branch) []spec.BranchListMetadata {
	apiBranches := make([]spec.BranchListMetadata, len(branches))
	for i, branch := range branches {
		apiBranches[i] = *storeToAPIBranchListMetadata(&branch)
	}
	return apiBranches
}

func storeToAPIScaleToZeroConfig(scaleToZero store.ScaleToZero) spec.ScaleToZeroConfiguration {
	inactivityPeriod := defaultInactivityDuration
	if scaleToZero.InactivityPeriod > 0 {
		inactivityPeriod = scaleToZero.InactivityPeriod.Duration()
	}
	return spec.ScaleToZeroConfiguration{
		Enabled:                 scaleToZero.Enabled,
		InactivityPeriodMinutes: int(inactivityPeriod.Minutes()),
	}
}

func apiToStoreCreateProjectConfig(req spec.CreateProjectJSONBody, usageTier string) *store.CreateProjectConfiguration {
	projectScaleToZeroConfig := defaultProjectScaleToZeroConfig()
	if req.Configuration != nil {
		projectScaleToZeroConfig = store.ProjectScaleToZero{
			BaseBranches:  apiToStoreScaleToZeroConfig(req.Configuration.ScaleToZero.BaseBranches),
			ChildBranches: apiToStoreScaleToZeroConfig(req.Configuration.ScaleToZero.ChildBranches),
		}
	}

	ipFiltering := store.IPFiltering{
		Enabled: false,
		CIDRs:   []store.CIDREntry{},
	}
	if req.Configuration != nil && req.Configuration.IpFiltering != nil {
		ipFiltering = apiToStoreIPFiltering(*req.Configuration.IpFiltering)
	}

	return &store.CreateProjectConfiguration{
		Name:        req.Name,
		ScaleToZero: projectScaleToZeroConfig,
		IPFiltering: ipFiltering,
		UsageTier:   usageTier,
	}
}

func apiToStoreScaleToZeroConfig(req spec.ScaleToZeroConfiguration) store.ScaleToZero {
	inactivityPeriod := defaultInactivityDuration
	if req.InactivityPeriodMinutes > 0 {
		inactivityPeriod = time.Minute * time.Duration(req.InactivityPeriodMinutes)
	}
	return store.ScaleToZero{
		Enabled:          req.Enabled,
		InactivityPeriod: store.InactivityPeriod(inactivityPeriod),
	}
}

func apiToStoreUpdateProjectConfig(req spec.UpdateProjectJSONBody) *store.UpdateProjectConfiguration {
	cfg := &store.UpdateProjectConfiguration{
		Name: req.Name,
	}
	if req.Configuration == nil {
		return cfg
	}
	if req.Configuration.ScaleToZero != nil {
		cfg.ScaleToZero = &store.ProjectScaleToZero{
			BaseBranches:  apiToStoreScaleToZeroConfig(req.Configuration.ScaleToZero.BaseBranches),
			ChildBranches: apiToStoreScaleToZeroConfig(req.Configuration.ScaleToZero.ChildBranches),
		}
	}
	if req.Configuration.IpFiltering != nil {
		ipFilteringVal := apiToStoreIPFiltering(*req.Configuration.IpFiltering)
		cfg.IPFiltering = &ipFilteringVal
	}
	return cfg
}

func apiToStoreUpdateBranchConfig(req spec.UpdateBranchJSONRequestBody) *store.UpdateBranchConfiguration {
	return &store.UpdateBranchConfiguration{
		Name:        req.Name,
		Description: req.Description,
	}
}

func apiToClustersScaleToZero(scaleToZero *spec.ScaleToZeroConfiguration, parentID *string, project *store.Project) *clustersv1.ScaleToZero {
	if scaleToZero != nil {
		return &clustersv1.ScaleToZero{
			Enabled:                 scaleToZero.Enabled,
			InactivityPeriodMinutes: int64(scaleToZero.InactivityPeriodMinutes),
		}
	}

	if project == nil {
		return nil
	}

	// if no scale to zero setting is provided, we use the project's default
	if parentID != nil {
		return storeToClustersScaleToZero(project.ScaleToZero.ChildBranches)
	}
	return storeToClustersScaleToZero(project.ScaleToZero.BaseBranches)
}

func storeToClustersScaleToZero(scaleToZero store.ScaleToZero) *clustersv1.ScaleToZero {
	return &clustersv1.ScaleToZero{
		Enabled:                 scaleToZero.Enabled,
		InactivityPeriodMinutes: int64(scaleToZero.InactivityPeriod.Duration().Minutes()),
	}
}

func storeToAPICIDREntries(cidrs []store.CIDREntry) []spec.CidrEntry {
	result := make([]spec.CidrEntry, len(cidrs))
	for i, entry := range cidrs {
		result[i].Cidr = entry.CIDR
		if entry.Description != "" {
			result[i].Description = &entry.Description
		}
	}
	return result
}

func apiToStoreIPFiltering(apiIPFiltering spec.IPFilteringConfiguration) store.IPFiltering {
	if apiIPFiltering.Cidr == nil {
		return store.IPFiltering{
			Enabled: apiIPFiltering.Enabled,
		}
	}
	cidrs := make([]store.CIDREntry, len(apiIPFiltering.Cidr))
	for i, entry := range apiIPFiltering.Cidr {
		cidrs[i] = store.CIDREntry{
			CIDR: entry.Cidr,
		}
		if entry.Description != nil {
			cidrs[i].Description = *entry.Description
		}
	}
	return store.IPFiltering{
		Enabled: apiIPFiltering.Enabled,
		CIDRs:   cidrs,
	}
}

func clustersToAPIScaleToZero(scaleToZero *clustersv1.ScaleToZero) spec.ScaleToZeroConfiguration {
	if scaleToZero == nil {
		return spec.ScaleToZeroConfiguration{
			Enabled:                 false,
			InactivityPeriodMinutes: int(defaultInactivityDuration.Minutes()),
		}
	}
	return spec.ScaleToZeroConfiguration{
		Enabled:                 scaleToZero.Enabled,
		InactivityPeriodMinutes: int(scaleToZero.InactivityPeriodMinutes),
	}
}

// generateSchedule converts a robfig/cron format to schedule string
// Input: robfig/cron format "second minute hour day-of-month month day-of-week"
// Output format: "d:hh:mm" where:
//   - d: * for daily, 0-6 for days of week (0=Sunday, 1=Monday, ..., 6=Saturday)
//   - hh: hour (00-23)
//   - mm: minute (00-59)
//
// Examples:
//
//	"0 30 14 * * *" -> "*:14:30" (daily at 2:30 PM)
//	"0 45 23 * * 0" -> "0:23:45" (Sunday at 11:45 PM)
//	"0 15 6 * * 1"  -> "1:06:15" (Monday at 6:15 AM)
func generateSchedule(cron string) string {
	parts := strings.Fields(cron)
	if len(parts) != 6 {
		return "*:00:00"
	}

	minute := parts[1]
	hour := parts[2]
	dayOfWeek := parts[5]

	// Pad single-digit hours and minutes with leading zeros
	if len(hour) == 1 {
		hour = "0" + hour
	}
	if len(minute) == 1 {
		minute = "0" + minute
	}

	if parts[3] == "*" && parts[4] == "*" && dayOfWeek == "*" {
		return fmt.Sprintf("*:%s:%s", hour, minute)
	}

	return fmt.Sprintf("%s:%s:%s", dayOfWeek, hour, minute)
}

// apiToClustersBackupConfig maps the spec backup configuration to the clusters
// backup configuration by delegating to the branch service, so the conversion
// logic lives in one place.
func apiToClustersBackupConfig(backupConfig *spec.BackupConfiguration, backupsEnabled bool, usePgBackRest bool) *clustersv1.BackupConfiguration {
	return branchsvc.ClustersBackupConfig(toBranchBackup(backupConfig), backupsEnabled, usePgBackRest)
}

func clustersToAPIBackupConfig(backupConfig *clustersv1.BackupConfiguration) *spec.BackupConfiguration {
	if backupConfig == nil || backupConfig.BackupSchedule == "" && backupConfig.BackupRetention == "" {
		return nil
	}

	var retentionDays int32
	_, err := fmt.Sscanf(backupConfig.BackupRetention, "%dd", &retentionDays)
	if err != nil {
		retentionDays = branchsvc.DefaultBackupRetentionPeriod
	}

	schedule := generateSchedule(backupConfig.BackupSchedule)
	return &spec.BackupConfiguration{
		BackupTime:      &schedule,
		RetentionPeriod: new(retentionDays),
	}
}

// apiToStoreBackupConfig returns the retention period for the spec backup
// configuration by delegating to the branch service.
func apiToStoreBackupConfig(backupConfig *spec.BackupConfiguration) int {
	return branchsvc.BackupRetentionDays(toBranchBackup(backupConfig))
}

func mapStatus(status *clustersv1.ClusterStatus) spec.BranchStatus {
	var branchStatus spec.BranchStatus
	branchStatus.Status = status.Status
	branchStatus.StatusType = status.StatusType.String()

	branchStatus.InstanceCount = int(status.InstanceCount)
	branchStatus.InstanceReadyCount = int(status.InstanceReadyCount)
	branchStatus.Instances = mapInstances(status.Instances)

	return branchStatus
}

func mapInstances(instances map[string]*clustersv1.InstanceStatus) []spec.InstanceStatus {
	// We want a deterministic API result
	keys := make([]string, 0, len(instances))
	for k := range instances {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	newInstances := make([]spec.InstanceStatus, 0, len(instances))
	for _, instanceID := range keys {
		newInstances = append(newInstances, spec.InstanceStatus{
			Id:            instanceID,
			Status:        instances[instanceID].Status,
			Primary:       instances[instanceID].Primary,
			TargetPrimary: instances[instanceID].TargetPrimary,
		})
	}

	return newInstances
}

func getRegion(region string) string {
	if region == "" {
		return store.DefaultRegion
	}
	return region
}

func defaultProjectScaleToZeroConfig() store.ProjectScaleToZero {
	return store.ProjectScaleToZero{
		BaseBranches:  defaultScaleToZeroConfig(),
		ChildBranches: defaultScaleToZeroConfig(),
	}
}

func defaultScaleToZeroConfig() store.ScaleToZero {
	return store.ScaleToZero{
		Enabled:          false,
		InactivityPeriod: store.InactivityPeriod(defaultInactivityDuration),
	}
}

func postgresConfigToAPIParameters(configurableParams postgrescfg.ParametersMap, instanceType string, clusterParams map[string]string, image string, preloadLibraries []string) []spec.PostgresConfigParameter {
	parameters := make([]spec.PostgresConfigParameter, 0, len(configurableParams))

	for paramName, paramSpec := range configurableParams {
		// Get current value from cluster configuration
		currentValue := paramSpec.DefaultValue // Default to merged default
		if clusterValue, exists := clusterParams[paramName]; exists {
			currentValue = clusterValue
		}

		// Determine default value source using the helper function
		configValueType, err := postgrescfg.DetermineConfigValueType(instanceType, paramName, paramSpec.DefaultValue, 0, image, preloadLibraries)
		if err != nil {
			// If we can't determine the type, default to postgres
			configValueType = postgrescfg.ConfigValueDefault
		}

		// Convert ConfigValueType to our enum
		var defaultValueSourceEnum spec.PostgresConfigParameterDefaultValueSource
		switch configValueType {
		case postgrescfg.ConfigValueInstanceDefault:
			defaultValueSourceEnum = spec.InstanceType
		case postgrescfg.ConfigValueDefault:
			defaultValueSourceEnum = spec.Postgres
		default:
			defaultValueSourceEnum = spec.Postgres
		}

		// Build acceptable range
		acceptableRange := &struct {
			EnumValues *[]string `json:"enumValues,omitempty"`
			MaxValue   *string   `json:"maxValue,omitempty"`
			MinValue   *string   `json:"minValue,omitempty"`
		}{}
		if paramSpec.MinValue != "" {
			acceptableRange.MinValue = &paramSpec.MinValue
		}
		if paramSpec.MaxValue != "" {
			acceptableRange.MaxValue = &paramSpec.MaxValue
		}
		if len(paramSpec.Values) > 0 {
			acceptableRange.EnumValues = &paramSpec.Values
		}

		// Convert parameter type to the correct enum type
		var paramType spec.PostgresConfigParameterType
		switch paramSpec.ParameterType {
		case postgrescfg.ParamTypeInt:
			paramType = spec.Int
		case postgrescfg.ParamTypeFloat:
			paramType = spec.Float
		case postgrescfg.ParamTypeBytes:
			paramType = spec.Bytes
		case postgrescfg.ParamTypeEnum:
			paramType = spec.Enum
		case postgrescfg.ParamTypeDuration:
			paramType = spec.Duration
		case postgrescfg.ParamTypeBoolean:
			paramType = spec.Boolean
		default:
			paramType = spec.String
		}

		var restartRequired *bool
		if paramSpec.RestartRequired {
			restartRequired = &paramSpec.RestartRequired
		}

		parameter := spec.PostgresConfigParameter{
			Name:               paramName,
			Type:               paramType,
			Description:        paramSpec.Description,
			Section:            paramSpec.Section,
			AcceptableRange:    acceptableRange,
			DefaultValue:       paramSpec.DefaultValue,
			DefaultValueSource: defaultValueSourceEnum,
			CurrentValue:       currentValue,
			DocumentationLink:  paramSpec.DocsLink,
			Recommendation:     paramSpec.Recommendation,
			RestartRequired:    restartRequired,
		}

		parameters = append(parameters, parameter)
	}

	return parameters
}

func storeToAPIGithubInstallation(inst *store.GithubInstallation) *spec.GithubInstallation {
	return &spec.GithubInstallation{
		Id:             inst.ID,
		InstallationId: inst.InstallationID,
		Organization:   inst.Organization,
		CreatedAt:      inst.CreatedAt,
		UpdatedAt:      inst.UpdatedAt,
	}
}

func storeToAPIGithubInstallationList(installations []store.GithubInstallation) []spec.GithubInstallation {
	apiInstallations := make([]spec.GithubInstallation, len(installations))
	for i, inst := range installations {
		apiInstallations[i] = *storeToAPIGithubInstallation(&inst)
	}
	return apiInstallations
}

func storeToAPIGithubRepository(mapping *store.GithubRepoMapping) *spec.GithubRepository {
	if mapping == nil {
		return nil
	}
	return &spec.GithubRepository{
		Id:                 mapping.ID,
		GithubRepositoryID: mapping.GithubRepositoryID,
		Project:            mapping.Project,
		RootBranchId:       mapping.RootBranchID,
		CreatedAt:          mapping.CreatedAt,
		UpdatedAt:          mapping.UpdatedAt,
	}
}

// parseRecoveryWindow parses the earliest and latest restore times from the
// recovery window of a branch. The latest restore time is the last
// recoverability point (last archived WAL) when the backup method reports it,
// and the last successful backup time otherwise. For newly created branches
// the fields are empty until the first backup completes.
func parseRecoveryWindow(window *clustersv1.GetRecoveryWindowResponse) (earliestRestore, latestRestore *time.Time, err error) {
	if window == nil {
		return nil, nil, nil
	}

	if window.FirstRecoverabilityPoint != "" {
		earliestRestoreTime, err := time.Parse(time.RFC3339, window.FirstRecoverabilityPoint)
		if err != nil {
			return nil, nil, fmt.Errorf("unexpected timestamp for earliest restore time: %w", err)
		}
		earliestRestore = new(earliestRestoreTime)
	}

	latest := window.LastRecoverabilityPoint
	if latest == "" {
		latest = window.LastSuccessfulBackup
	}
	if latest != "" {
		latestRestoreTime, err := time.Parse(time.RFC3339, latest)
		if err != nil {
			return nil, nil, fmt.Errorf("unexpected timestamp for latest restore time: %w", err)
		}
		latestRestore = new(latestRestoreTime)
	}

	return earliestRestore, latestRestore, nil
}
