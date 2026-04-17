package api

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"xata/internal/analytics"
	"xata/internal/analytics/events"
	"xata/internal/api"
	"xata/internal/extensions"
	"xata/internal/flags"
	"xata/internal/o11y"
	"xata/internal/openfeature"
	"xata/internal/postgrescfg"
	"xata/internal/postgresversions"
	"xata/services/clusters"
	"xata/services/projects/api/spec"
	"xata/services/projects/cells"
	"xata/services/projects/metrics"
	"xata/services/projects/scheduler"
	"xata/services/projects/store"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/resource"

	clustersv1 "xata/gen/proto/clusters/v1"

	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

const (
	FallbackInstanceType         = "custom"
	DefaultBackupRetentionPeriod = 2 // days
	MinRetentionPeriod           = 2
	MaxRetentionPeriod           = 35
	DefaultBackupFrequency       = "weekly"

	backupTimestampLayout = "2006-01-02 15:04:05 -0700 MST"
)

type Permission int

const (
	All Permission = iota
	OnlyEnabled
)

var (
	DefaultMaxInstances = 5
	DefaultMinInstances = 1
	DefaultRegion       = []string{"us-east-1"}

	// maxDateRange is the maximum date range for metrics queries
	maxDateRange = 6 * 30 * 24 * time.Hour // 6 months
	// we are still receiving this from the FE when loading our custom image
	// TODO remove once UI no longer uses it
	validImage = "postgresql:17"

	// internalExtensions are extensions that should not be exposed in the API
	internalExtensions = []string{"xatautils"}
)

type handler struct {
	store     store.ProjectsStore
	cells     cells.Cells
	feat      openfeature.Client
	sched     *scheduler.Scheduler
	analytics analytics.Client

	// defaultGatewayHostPort is the host:port of the gateway service, used to build connection strings
	defaultGatewayHostPort string

	// metricsClient is the client for the metrics service
	metricsClient metrics.Client

	// postgresConfigProvider is the provider for PostgreSQL configuration operations
	postgresConfigProvider postgrescfg.PostgresConfigProvider

	// imageProvider is the provider for the PostgreSQL images
	imageProvider postgresversions.ImageProvider
}

func NewAPIHandler(feat openfeature.Client, store store.ProjectsStore, cells cells.Cells, gatewayHostPort string, metricsClient metrics.Client, scheduler *scheduler.Scheduler, analytics analytics.Client, postgresConfigProvider postgrescfg.PostgresConfigProvider, imageProvider postgresversions.ImageProvider) spec.ServerInterface {
	return &handler{
		feat:                   feat,
		store:                  store,
		cells:                  cells,
		defaultGatewayHostPort: gatewayHostPort,
		metricsClient:          metricsClient,
		sched:                  scheduler,
		analytics:              analytics,
		postgresConfigProvider: postgresConfigProvider,
		imageProvider:          imageProvider,
	}
}

// Get list of regions available for the organization
// (GET /organizations/{organizationID}/regions)
func (s *handler) ListRegions(c echo.Context, organizationID spec.OrganizationID) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		regions, err := s.store.ListRegions(c.Request().Context(), organizationID)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, struct {
			Regions []store.Region `json:"regions"`
		}{regions})
	})
}

// Get list of images available for the organization
// (GET /organizations/{organizationID}/images)
func (s *handler) ListImages(c echo.Context, organizationID spec.OrganizationID, params spec.ListImagesParams) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		if params.Region != nil {
			err := s.validateRegion(c.Request().Context(), organizationID, *params.Region)
			if err != nil {
				if strings.Contains(err.Error(), "not found") {
					return ErrorInvalidParam{Param: "region", Message: "invalid region: " + err.Error()}
				}
				return err
			}
		}

		images := s.imageProvider.GetAllImageNames()

		// Filter out experimental images if the feature flag is not enabled
		experimentalEnabled := s.feat.BoolValue(c.Request().Context(), flags.ExperimentalImages)
		if !experimentalEnabled {
			filtered := make([]string, 0, len(images))
			for _, img := range images {
				if !strings.HasPrefix(img, "experimental:") {
					filtered = append(filtered, img)
				}
			}
			images = filtered
		}

		// Filter out analytics images if the feature flag is not enabled
		analyticsEnabled := s.feat.BoolValue(c.Request().Context(), flags.AnalyticsImages)
		if !analyticsEnabled {
			filtered := make([]string, 0, len(images))
			for _, img := range images {
				if !strings.HasPrefix(img, "analytics:") {
					filtered = append(filtered, img)
				}
			}
			images = filtered
		}

		imagesResp := make([]spec.Image, len(images))
		for i, it := range images {
			version := s.imageProvider.ExtractVersionFromImageName(it)
			imagesResp[i] = spec.Image{
				MajorVersion: s.imageProvider.GetMajorForVersion(version),
				FullVersion:  version,
				Name:         it,
			}
		}
		return c.JSON(http.StatusOK, struct {
			Images []spec.Image `json:"images"`
		}{imagesResp})
	})
}

// Get list of extensions available for the image
// (GET /organizations/{organizationID}/extensions)
func (s *handler) ListExtensions(c echo.Context, organizationID spec.OrganizationID, params spec.ListExtensionsParams) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		if params.Region != nil {
			err := s.validateRegion(c.Request().Context(), organizationID, *params.Region)
			if err != nil {
				if strings.Contains(err.Error(), "not found") {
					return ErrorInvalidParam{Param: "region", Message: "invalid region: " + err.Error()}
				}
				return err
			}
		}

		err := s.imageProvider.ValidateImage(params.Image)
		if err != nil {
			return ErrorInvalidParam{Param: "image", Message: err.Error()}
		}

		exts := extensions.GetExtensions(params.Image)
		if exts == nil {
			return ErrorInvalidParam{Param: "image", Message: "no extensions found for image"}
		}

		extensionsResp := make([]spec.Extension, 0, len(exts))
		for _, ext := range exts {
			if slices.Contains(internalExtensions, ext.Name) {
				continue
			}
			extensionsResp = append(extensionsResp, spec.Extension{
				Name:            ext.Name,
				Version:         ext.Version,
				Description:     ext.Description,
				Docs:            ext.DocsURL,
				PreloadRequired: ext.PreloadRequired,
				Type:            spec.ExtensionType(ext.Type),
			})
		}

		return c.JSON(http.StatusOK, struct {
			Extensions []spec.Extension `json:"extensions"`
		}{extensionsResp})
	})
}

// Get list of instance types available for the organization
// (GET /organizations/{organizationID}/instanceTypes)
func (s *handler) ListInstanceTypes(c echo.Context, organizationID spec.OrganizationID, params spec.ListInstanceTypesParams) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		err := s.validateRegion(c.Request().Context(), organizationID, params.Region)
		if err != nil {
			return ErrorInvalidParam{Param: "region", Message: "invalid region: " + err.Error()}
		}

		instanceTypes, err := s.store.ListInstanceTypes(c.Request().Context(), organizationID, params.Region)
		if err != nil {
			return err
		}

		type instanceType struct {
			Name               string  `json:"name"`
			VCPUs              int     `json:"vcpus"` // requested. This is an integer in milli-CPUs / millicores. E.g "500" -> 0.5 vCPU
			RAM                int     `json:"ram"`   // in GB
			HourlyRate         float64 `json:"hourlyRate"`
			StorageMonthlyRate float64 `json:"storageMonthlyRate"`
			// for now we have the same instance types in all regions, but this may not always be the case
			Region string `json:"region"`
		}

		instanceTypesResp := make([]instanceType, len(instanceTypes))
		for i, it := range instanceTypes {
			instanceTypesResp[i] = instanceType{
				Name:               it.Name,
				VCPUs:              it.VCPUsRequest,
				RAM:                it.RAM,
				HourlyRate:         it.HourlyRate,
				StorageMonthlyRate: it.StorageMonthlyRate,
				Region:             it.Region,
			}
		}
		return c.JSON(http.StatusOK, struct {
			InstanceTypes []instanceType `json:"instanceTypes"`
		}{instanceTypesResp})
	})
}

// List all projects
// (GET /organizations/{organizationID}/projects)
func (s *handler) ListProjects(c echo.Context, organizationID spec.OrganizationID) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		projects, err := s.store.ListProjects(c.Request().Context(), organizationID)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, struct {
			Projects []spec.Project `json:"projects"`
		}{storeToAPIProjectList(projects)})
	})
}

// Create a new project
// (POST /organizations/{organizationID}/projects)
func (s *handler) CreateProject(c echo.Context, organizationID spec.OrganizationID) error {
	return s.withOrganizationAccess(c, organizationID, OnlyEnabled, func() error {
		var req spec.CreateProjectJSONBody
		err := c.Bind(&req)
		if err != nil {
			return err
		}

		createdProject, err := s.store.CreateProject(c.Request().Context(), organizationID, apiToStoreCreateProjectConfig(req))
		if err != nil {
			return err
		}

		s.analytics.Track(c.Request().Context(), events.NewProjectCreatedEvent(string(organizationID), createdProject.ID))

		return c.JSON(http.StatusCreated, storeToAPIProject(createdProject))
	})
}

// Delete a project by ID
// (DELETE /organizations/{organizationID}/projects/{projectID})
func (s *handler) DeleteProject(c echo.Context, organizationID spec.OrganizationID, projectID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		err := s.store.DeleteProject(c.Request().Context(), organizationID, projectID)
		if err != nil {
			return err
		}

		s.analytics.Track(c.Request().Context(), events.NewProjectDeletedEvent(string(organizationID), projectID))

		return c.NoContent(http.StatusNoContent)
	})
}

// Get a project by ID
// (GET /organizations/{organizationID}/projects/{projectID})
func (s *handler) GetProject(c echo.Context, organizationID spec.OrganizationID, projectID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		project, err := s.store.GetProject(c.Request().Context(), organizationID, projectID)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, storeToAPIProject(project))
	})
}

// List project backups
// (GET /organizations/{organizationID}/projects/{projectID}/backups)
func (s *handler) ListBackups(c echo.Context, organizationID spec.OrganizationID, projectID string) error {
	return echo.NewHTTPError(http.StatusNotImplemented, "listing backups is not implemented")
}

// Get a backup by ID
// (GET /organizations/{organizationID}/projects/{projectID}/backups/{backupID})
func (s *handler) GetBackup(c echo.Context, organizationID spec.OrganizationID, projectID, backupID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		// backupID is the same as branchID, so we fetch the branch to get cell info
		branch, err := s.store.DescribeBranch(c.Request().Context(), organizationID, projectID, backupID)
		if err != nil {
			if errors.As(err, &store.ErrBranchNotFound{}) {
				return ErrorBackupNotFound{ID: backupID}
			}
			return err
		}
		client, err := s.cells.GetCellConnection(c.Request().Context(), organizationID, branch.CellID)
		if err != nil {
			return err
		}
		defer client.Close()

		status, err := client.GetObjectStore(c.Request().Context(), &clustersv1.GetObjectStoreRequest{
			Id: branch.ID,
		})
		if err != nil {
			return err
		}

		earliestRestore, latestRestore, err := parseRestoreTimes(status, backupID)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, spec.BackupMetadata{
			Id:              backupID,
			BranchID:        branch.ID,
			EarliestRestore: earliestRestore,
			LatestRestore:   latestRestore,
			Description:     "Continuous backup for branch " + branch.ID,
		})
	})
}

// Update a project by ID
// (PATCH /organizations/{organizationID}/projects/{projectID})
func (s *handler) UpdateProject(c echo.Context, organizationID spec.OrganizationID, projectID string) error {
	return s.withOrganizationAccess(c, organizationID, OnlyEnabled, func() error {
		var body spec.UpdateProjectJSONBody
		if err := api.ReadBody(c, &body); err != nil {
			return err
		}

		if body.Name == nil && body.Configuration == nil {
			return ErrorInvalidParam{ProjectID: projectID, Param: "all", Message: "at least one of the request fields needs to be set"}
		}

		ctx := c.Request().Context()
		updateConfig := apiToStoreUpdateProjectConfig(body)

		// If IP filtering is being updated, acquire project lock to prevent race conditions
		// with branch creation, then apply changes to primary cells before saving to DB
		if updateConfig.IPFiltering != nil {
			releaseLock, err := s.store.AcquireProjectLock(ctx, projectID)
			if err != nil {
				return fmt.Errorf("failed to acquire project lock: %w", err)
			}
			defer releaseLock()

			if err := s.applyIPFilteringToPrimaryCells(ctx, organizationID, projectID, updateConfig.IPFiltering); err != nil {
				return err
			}
		}

		project, err := s.store.UpdateProject(ctx, organizationID, projectID, updateConfig)
		if err != nil {
			return err
		}

		var changedFields []string
		newValues := map[string]any{}
		if body.Name != nil {
			changedFields = append(changedFields, "name")
			newValues["name"] = *body.Name
		}
		if updateConfig.IPFiltering != nil {
			changedFields = append(changedFields, "ip_filtering")
			newValues["ip_filtering_enabled"] = updateConfig.IPFiltering.Enabled
		}
		s.analytics.Track(ctx, events.NewProjectUpdatedEvent(string(organizationID), projectID, changedFields, newValues))

		return c.JSON(http.StatusOK, storeToAPIProject(project))
	})
}

// applyIPFilteringToPrimaryCells applies IP filtering settings to the primary cell of each branch's region
// before saving to the DB. Returns an error if any call fails.
func (s *handler) applyIPFilteringToPrimaryCells(ctx context.Context, organizationID string, projectID string, ipFiltering *store.IPFiltering) error {
	// Get all branches for the project
	branches, err := s.store.ListBranches(ctx, organizationID, projectID)
	if err != nil {
		return fmt.Errorf("listing branches: %w", err)
	}

	if len(branches) == 0 {
		// No branches to update, nothing to do
		return nil
	}

	// Group branches by region
	regionToBranches := make(map[string][]string)
	for _, branch := range branches {
		regionID := branch.Region
		if regionID == "" {
			return fmt.Errorf("branch %s has no region", branch.ID)
		}
		regionToBranches[regionID] = append(regionToBranches[regionID], branch.ID)
	}

	if len(regionToBranches) == 0 {
		return fmt.Errorf("no valid regions found for branches")
	}

	regionToCellClient := make(map[string]cells.CellClient)

	// Clean up cell connections when done
	defer func() {
		for _, client := range regionToCellClient {
			if client != nil {
				client.Close()
			}
		}
	}()

	// Get primary cell and connection for each unique region
	for regionID := range regionToBranches {
		primaryCell, err := s.store.GetPrimaryCell(ctx, organizationID, regionID)
		if err != nil {
			return fmt.Errorf("getting primary cell for region %s: %w", regionID, err)
		}

		cellClient, err := s.cells.GetCellConnection(ctx, organizationID, primaryCell.ID)
		if err != nil {
			return fmt.Errorf("connecting to primary cell %s for region %s: %w", primaryCell.ID, regionID, err)
		}

		regionToCellClient[regionID] = cellClient
	}

	ipFilteringConfig := &clustersv1.IPFilteringConfig{
		Enabled: ipFiltering.Enabled,
		Allowed: ipFiltering.CIDRStrings(),
	}

	// Apply IP filtering to all branches in each region with a single call per region
	for regionID, branchIDs := range regionToBranches {
		cellClient, exists := regionToCellClient[regionID]
		if !exists {
			return fmt.Errorf("no cell client found for region %s", regionID)
		}

		_, err := cellClient.SetBranchesIPFiltering(ctx, &clustersv1.SetBranchesIPFilteringRequest{
			BranchIds:   branchIDs,
			IpFiltering: ipFilteringConfig,
		})
		if err != nil {
			return fmt.Errorf("setting IP filtering for branches in region %s: %w", regionID, err)
		}
	}

	return nil
}

// List all branches of a project
// (GET /organizations/{organizationID}/projects/{projectID}/branches)
func (s *handler) ListBranches(c echo.Context, organizationID spec.OrganizationID, projectID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		branches, err := s.store.ListBranches(c.Request().Context(), organizationID, projectID)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, struct {
			Branches []spec.BranchListMetadata `json:"branches"`
		}{storeToAPIListBranchListMetadata(branches)})
	})
}

// Validate backup time format
func isValidBackupTimeFormat(s string) bool {
	// Regex pattern: ^(\*|[0-6]):(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9])$
	// 0-6-days of the week, * = daily
	s = strings.TrimSpace(s)
	pattern := regexp.MustCompile(`^(\*|[0-6]):(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9])$`)
	return pattern.MatchString(s)
}

type ValidatableCreateRequest interface {
	spec.CreateBranchJSONRequestBody | spec.RestoreFromBackupJSONRequestBody
}

func validateBranchRequestCommons[T ValidatableCreateRequest](body T) error {
	v := any(body)
	var desc *string
	var name string
	var backupConfig *spec.BackupConfiguration

	switch req := v.(type) {
	case spec.CreateBranchJSONRequestBody:
		desc = req.Description
		name = req.Name
		backupConfig = req.BackupConfiguration
	case spec.RestoreFromBackupJSONRequestBody:
		desc = req.Description
		name = req.Name
		backupConfig = req.BackupConfiguration
	}
	if desc != nil {
		err := IsBranchDescriptionValid(*desc)
		if err != nil {
			return err
		}
	}

	if name == "" {
		return ErrorInvalidParam{BranchName: name, Param: "name", Message: "branch name is required"}
	}

	return validateBackupConfiguration(name, backupConfig)
}

func validateBackupConfiguration(branchName string, c *spec.BackupConfiguration) error {
	if c == nil {
		return nil
	}
	if c.RetentionPeriod != nil && (*c.RetentionPeriod < MinRetentionPeriod || *c.RetentionPeriod > MaxRetentionPeriod) {
		return ErrorInvalidParam{BranchName: branchName, Param: "backup retentionPeriod", Message: fmt.Sprintf("must be at least %d days and maximum %d days", MinRetentionPeriod, MaxRetentionPeriod)}
	}
	if c.BackupTime != nil && !isValidBackupTimeFormat(*c.BackupTime) {
		return ErrorInvalidParam{BranchName: branchName, Param: "backup time", Message: fmt.Sprintf("invalid backup time format '%s', must match format 'D:HH:MM' where D= * or 0-6, HH=00-23, MM=00-59", *c.BackupTime)}
	}
	return nil
}

type ClusterServicePayload struct {
	ParentID       *string
	Configuration  clustersv1.ClusterConfiguration
	CellID         string
	Region         string
	BackupsEnabled bool
}

// Create a new branch
// (POST /organizations/{organizationID}/projects/{projectID}/branches)
func (s *handler) CreateBranch(c echo.Context, organizationID spec.OrganizationID, projectID string) error {
	return s.withOrganizationAccess(c, organizationID, OnlyEnabled, func() error {
		// Check if branch creation is disabled (any type of branch)
		if s.feat.BoolValue(c.Request().Context(), flags.BranchCreationDisabled) {
			return ErrorBranchCreationDisabled{}
		}

		ctx := c.Request().Context()

		claims := api.GetUserClaims(c)
		if claims != nil {
			if org, ok := claims.Organizations[string(organizationID)]; ok && org.IsNewOrganization() && string(organizationID) != "hrq60r" {
				count, err := s.store.CountOrganizationBranches(ctx, string(organizationID))
				if err != nil {
					return fmt.Errorf("count organization branches: %w", err)
				}
				if count >= store.MaxNewOrgBranches {
					return ErrorNewOrgBranchLimitExceeded{OrganizationID: string(organizationID)}
				}
			}
		}

		var body spec.CreateBranchJSONRequestBody
		if err := api.ReadBody(c, &body); err != nil {
			return err
		}

		if err := validateBranchRequestCommons(body); err != nil {
			return err
		}

		value, err := body.ValueByDiscriminator()
		if err != nil {
			return ErrorInvalidParam{BranchName: body.Name, Param: "body", Message: fmt.Sprintf("failed to parse branch creation details - %s", err.Error())}
		}

		var createClusterPayload ClusterServicePayload

		switch payload := value.(type) {
		// mode: inherit - we create a child branch
		case spec.BranchFromParent:
			// keeping feature flag separate from other checks for visibility
			if s.feat.BoolValue(ctx, flags.ChildBranchCreationDisabled) {
				return ErrorChildBranchCreationDisabled{}
			}
			createClusterPayload, err = s.handleBranchFromParent(ctx, organizationID, projectID, body.Name, payload)
			if err != nil {
				return err
			}
		// mode custom - we create a main branch
		case spec.BranchFromConfiguration:
			createClusterPayload, err = s.handleBranchFromConfiguration(ctx, organizationID, projectID, body.Name, payload)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported branch creation mode: %T", payload)

		}

		if !createClusterPayload.BackupsEnabled && body.BackupConfiguration != nil {
			return ErrorInvalidParam{BranchName: body.Name, Param: "backupConfiguration", Message: "backup configuration cannot be specified when backups are disabled in the selected region"}
		}

		// we are now in possession of the following:
		// cellID
		// parentID (valid for child branch, nil for main branch
		// AND
		// (vcpu request and limit, memory, default postgres params, and image) for main branch
		// for child branch - we get them from the parent in the clusters service

		// Acquire project lock BEFORE reading project to prevent race conditions with IP filtering updates
		// This ensures we read the current IP filtering settings even if an update is in progress
		releaseLock, err := s.store.AcquireProjectLock(ctx, projectID)
		if err != nil {
			return fmt.Errorf("failed to acquire project lock: %w", err)
		}
		defer releaseLock()

		return s.withProject(c, organizationID, projectID, func(project *store.Project) error {
			branch, err := s.store.CreateBranch(ctx, organizationID, projectID, createClusterPayload.CellID, &store.CreateBranchConfiguration{
				Name:                  body.Name,
				ParentID:              createClusterPayload.ParentID,
				Description:           body.Description,
				BackupRetentionPeriod: apiToStoreBackupConfig(body.BackupConfiguration),
				BackupsEnabled:        createClusterPayload.BackupsEnabled,
			}, func(branch *store.Branch) error {
				scaleToZero := apiToClustersScaleToZero(body.ScaleToZero, createClusterPayload.ParentID, project)
				createClusterPayload.Configuration.ScaleToZero = scaleToZero
				request := clustersv1.CreatePostgresClusterRequest{
					Id:                  branch.ID,
					ParentId:            branch.ParentID,
					OrganizationId:      organizationID,
					ProjectId:           projectID,
					Configuration:       &createClusterPayload.Configuration,
					BackupConfiguration: apiToClustersBackupConfig(body.BackupConfiguration, createClusterPayload.BackupsEnabled),
				}
				if branch.ParentID != nil {
					request.DataSource = &clustersv1.CreatePostgresClusterRequest_ClusterSnapshot{
						ClusterSnapshot: &clustersv1.ClusterSnapshot{
							ClusterId: *branch.ParentID,
						},
					}
				}
				usePool := s.feat.BoolValue(ctx, flags.UseClusterPool)
				log.Ctx(ctx).Info().Bool("usePool", usePool).Msg("cluster pool feature flag")
				if usePool {
					request.UsePool = proto.Bool(true)
				}

				client, err := s.cells.GetCellConnection(ctx, organizationID, createClusterPayload.CellID)
				if err != nil {
					return err
				}
				defer client.Close()

				_, err = client.CreatePostgresCluster(ctx, &request)
				if err != nil {
					return err
				}

				return s.setupBranchOnPrimaryCell(ctx, organizationID, createClusterPayload.Region, createClusterPayload.CellID, branch.ID, project)
			})
			if err != nil {
				st, _ := status.FromError(err)
				if st.Code() == codes.NotFound && createClusterPayload.ParentID != nil {
					return ErrorBranchNotFound{BranchID: *createClusterPayload.ParentID}
				}
				if st.Code() == codes.InvalidArgument {
					return ErrorInvalidParam{BranchName: body.Name, Param: "configuration", Message: st.Message()}
				}
				if st.Code() == codes.FailedPrecondition && createClusterPayload.ParentID != nil {
					return ErrorParentBranchUnhealthy{ParentID: *createClusterPayload.ParentID}
				}
				return err
			}

			var analyticsEvent events.Event
			switch payload := value.(type) {
			case spec.BranchFromConfiguration:
				analyticsEvent = events.NewBranchFromConfigurationEvent(
					string(organizationID),
					projectID,
					branch.ID,
					branch.Region,
					string(payload.Configuration.Image),
					payload.Configuration.InstanceType,
					int(payload.Configuration.Replicas),
					payload.Configuration.Storage,
				)
			case spec.BranchFromParent:
				analyticsEvent = events.NewBranchFromParentEvent(string(organizationID), projectID, payload.ParentID, branch.ID, branch.Region)
			}
			s.analytics.Track(c.Request().Context(), analyticsEvent)

			// get the connection string
			// swallow the error, the resource got created and the connection string will be eventually available
			connString, _ := s.getConnectionString(c, organizationID, branch)
			return c.JSON(http.StatusCreated, storeToAPIBranchShortMetadata(branch, connString))
		})
	})
}

func (s *handler) handleBranchFromParent(c context.Context, organizationID spec.OrganizationID, projectID, branchName string, payload spec.BranchFromParent) (ClusterServicePayload, error) {
	if err := validateBranchFromParent(branchName, payload); err != nil {
		return ClusterServicePayload{}, err
	}
	return s.prepareCreateClusterFromParent(c, organizationID, projectID, payload)
}

func validateBranchFromParent(branchName string, payload spec.BranchFromParent) error {
	if payload.ParentID == "" {
		return ErrorInvalidParam{BranchName: branchName, Param: "parentID", Message: "parentId is required for 'inherit' mode"}
	}
	return nil
}

func (s *handler) prepareCreateClusterFromParent(ctx context.Context, organizationID spec.OrganizationID, projectID string, payload spec.BranchFromParent) (ClusterServicePayload, error) {
	// get the cell ID from the parent branch
	parentBranch, err := s.store.DescribeBranch(ctx, organizationID, projectID, payload.ParentID)
	if err != nil {
		if errors.As(err, &store.ErrBranchNotFound{}) {
			return ClusterServicePayload{}, ErrorBranchNotFound{BranchID: payload.ParentID}
		}
		return ClusterServicePayload{}, err
	}

	return ClusterServicePayload{
		ParentID: new(payload.ParentID),
		// If the parent branch ID is present, all settings (including vcpu, memory, and Postgres parameters) get copied from the
		// parent branch. This means we don't need to send them over, they just get copied locally in the cell.
		Configuration:  clustersv1.ClusterConfiguration{},
		CellID:         parentBranch.CellID,
		Region:         parentBranch.Region,
		BackupsEnabled: parentBranch.BackupsEnabled,
	}, nil
}

func (s *handler) handleBranchFromConfiguration(c context.Context, organizationID spec.OrganizationID, projectID, branchName string, payload spec.BranchFromConfiguration) (ClusterServicePayload, error) {
	if err := s.validateBranchFromConfiguration(c, organizationID, branchName, payload); err != nil {
		return ClusterServicePayload{}, err
	}
	return s.prepareCreateClusterFromConfiguration(c, organizationID, projectID, branchName, payload)
}

func (s *handler) validateBranchFromConfiguration(ctx context.Context, organizationID spec.OrganizationID, name string, payload spec.BranchFromConfiguration) error {
	// validate payload
	if payload.Configuration == (spec.ClusterConfiguration{}) {
		return ErrorInvalidParam{BranchName: name, Param: "configuration", Message: "configuration is required for 'custom' mode"}
	}

	// validate replicas
	if payload.Configuration.Replicas < 0 {
		return ErrorInvalidParam{BranchName: name, Param: "configuration", Message: "number of replicas must be at least zero"}
	}

	return nil
}

func (s *handler) prepareCreateClusterFromConfiguration(ctx context.Context, organizationID spec.OrganizationID, projectID, branchName string, payload spec.BranchFromConfiguration) (ClusterServicePayload, error) {
	// validate image - from this moment on, the image is in the correct format, no need for prefix, suffix, extra validation
	validImageFormat, err := s.validateImage(ctx, organizationID, payload.Configuration.Image)
	if err != nil {
		return ClusterServicePayload{}, ErrorInvalidParam{BranchName: branchName, Param: "configuration", Message: "invalid image: " + err.Error()}
	}

	region, err := s.store.GetRegion(ctx, organizationID, payload.Configuration.Region)
	if err != nil {
		return ClusterServicePayload{}, ErrorInvalidParam{BranchName: branchName, Param: "configuration", Message: "invalid region: " + err.Error()}
	}

	// allocate to a cell in the region
	cellID, err := s.allocateCell(ctx, organizationID, branchName, payload.Configuration.Region)
	if err != nil {
		return ClusterServicePayload{}, err
	}

	// extract the vcpu and memory from the instance type
	vcpuRequest, vcpuLimit, memory, err := s.getResourcesByInstanceType(ctx, organizationID, payload.Configuration.Region, payload.Configuration.InstanceType)
	if err != nil {
		return ClusterServicePayload{}, ErrorInvalidParam{BranchName: branchName, Param: "instanceType", Message: err.Error()}
	}
	// Extract major version from image name
	majorVersion := postgresversions.ExtractMajorVersionFromImage(validImageFormat)

	// use configured preload libraries if provided, otherwise use defaults
	var preloadLibraries []string
	if payload.Configuration.PreloadLibraries != nil && len(*payload.Configuration.PreloadLibraries) > 0 {
		if err := s.postgresConfigProvider.ValidatePreloadLibraries(validImageFormat, *payload.Configuration.PreloadLibraries); err != nil {
			return ClusterServicePayload{}, ErrorInvalidParam{BranchName: branchName, Param: "preloadLibraries", Message: err.Error()}
		}
		preloadLibraries = *payload.Configuration.PreloadLibraries
	} else {
		preloadLibraries, err = s.postgresConfigProvider.GetDefaultPreloadLibraries(validImageFormat)
		if err != nil {
			return ClusterServicePayload{}, ErrorInvalidParam{BranchName: branchName, Param: "image", Message: fmt.Sprintf("failed to get default preload libraries: %v", err)}
		}
	}

	// validate configured postgres parameters if provided
	if payload.Configuration.PostgresConfigurationParameters != nil {
		errs, err := s.postgresConfigProvider.ValidateSettings(payload.Configuration.InstanceType, *payload.Configuration.PostgresConfigurationParameters, majorVersion, validImageFormat, preloadLibraries)
		if err != nil {
			return ClusterServicePayload{}, ErrorInvalidParam{BranchName: branchName, Param: "postgresConfigurationParameters", Message: fmt.Sprintf("validation failed: %v", err)}
		}
		if errs != nil {
			paramNames := slices.Sorted(maps.Keys(errs))
			var errorMessages []string
			for _, paramName := range paramNames {
				errorMessages = append(errorMessages, fmt.Sprintf("%s: %s", paramName, errs[paramName].Error()))
			}
			return ClusterServicePayload{}, ErrorInvalidParam{BranchName: branchName, Param: "postgresConfigurationParameters", Message: strings.Join(errorMessages, "; ")}
		}
	}

	// compute default Postgres parameters based on instance type, image, and preloaded extensions
	postgresParameters, err := s.postgresConfigProvider.GetDefaultPostgresParameters(payload.Configuration.InstanceType, majorVersion, validImageFormat, preloadLibraries)
	if err != nil {
		return ClusterServicePayload{}, ErrorInvalidParam{BranchName: branchName, Param: "instanceType", Message: fmt.Sprintf("failed to compute Postgres parameters: %v", err)}
	}

	// merge configured postgres parameters if provided (they override defaults)
	if payload.Configuration.PostgresConfigurationParameters != nil {
		maps.Copy(postgresParameters, *payload.Configuration.PostgresConfigurationParameters)
	}

	numInstances := payload.Configuration.Replicas + 1 // the primary is always created

	// TODO storage size: we are currently not using the storage size sent from the API.
	// when/if we change that, we need to add it to this payload and to the created event below
	return ClusterServicePayload{
		ParentID: nil,
		Configuration: clustersv1.ClusterConfiguration{
			NumInstances:                    numInstances,
			ImageName:                       validImageFormat,
			VcpuRequest:                     vcpuRequest,
			VcpuLimit:                       vcpuLimit,
			Memory:                          memory,
			PostgresConfigurationParameters: postgresParameters,
			PreloadLibraries:                preloadLibraries,
		},
		CellID:         cellID,
		Region:         payload.Configuration.Region,
		BackupsEnabled: region.BackupsEnabled,
	}, nil
}

// formatCPUresource formats milliCPUs (millicores) into K8s resource spec
func formatCPUResource(milliCPUs int) string {
	if milliCPUs < 1000 {
		return fmt.Sprintf("%dm", milliCPUs)
	}
	return fmt.Sprintf("%d", milliCPUs/1000)
}

// parseCPUResource parses k8s cpu spec into milliCPUs
func parseCPUResource(cpuSpec string) (int, error) {
	quantity, err := resource.ParseQuantity(cpuSpec)
	if err != nil {
		return 0, fmt.Errorf("failed to parse cpu resource: %w", err)
	}
	return int(quantity.MilliValue()), nil
}

// returns the vcpu and memory for the given instance type
func (s *handler) getResourcesByInstanceType(ctx context.Context, organizationID spec.OrganizationID, region string, name string) (cpuRequest string, cpuLimit string, memory string, err error) {
	instanceTypes, err := s.store.ListInstanceTypes(ctx, organizationID, region)
	if err != nil {
		return "", "", "", err
	}
	for _, instance := range instanceTypes {
		if instance.Name == name {
			return formatCPUResource(instance.VCPUsRequest), formatCPUResource(instance.VCPUsLimit), fmt.Sprintf("%dGi", instance.RAM), nil
		}
	}
	return "", "", "", fmt.Errorf("instance type %s is not found", name)
}

// returns the instance type for the pair vcpu, memory
func (s *handler) getInstanceTypeByResources(ctx context.Context, organizationID spec.OrganizationID, region string, cpuRequest string, cpuLimit string, memory string) (name string, err error) {
	instanceTypes, err := s.store.ListInstanceTypes(ctx, organizationID, region)
	if err != nil {
		return "", err
	}

	vcpusRequest, err := parseCPUResource(cpuRequest)
	if err != nil {
		return "", err
	}

	vcpusLimit, err := parseCPUResource(cpuLimit)
	if err != nil {
		return "", err
	}

	ram, err := strconv.Atoi(memory)
	if err != nil {
		return "", err
	}

	for _, instance := range instanceTypes {
		if instance.VCPUsRequest == vcpusRequest && instance.VCPUsLimit == vcpusLimit && instance.RAM == ram {
			return instance.Name, nil
		}
	}

	// for invalid combinations we will return the FallbackInstanceType defined as "custom" string as not to break the UI in the case we needed to amend the configs manually
	return FallbackInstanceType, nil
}

func (s *handler) validateRegion(ctx context.Context, organizationID spec.OrganizationID, region string) error {
	regions, err := s.store.ListRegions(ctx, organizationID)
	if err != nil {
		return err
	}

	for _, r := range regions {
		if region == r.ID {
			return nil
		}
	}
	return fmt.Errorf("region %s is not found", region)
}

func (s *handler) validateImage(ctx context.Context, organizationID spec.OrganizationID, image string) (string, error) {
	// Reject experimental images if the feature flag is not enabled
	if strings.HasPrefix(image, "experimental:") && !s.feat.BoolValue(ctx, flags.ExperimentalImages) {
		return "", fmt.Errorf("image %s is not available", image)
	}

	// Reject analytics images if the feature flag is not enabled
	if strings.HasPrefix(image, "analytics:") && !s.feat.BoolValue(ctx, flags.AnalyticsImages) {
		return "", fmt.Errorf("image %s is not available", image)
	}

	allValidImages := s.imageProvider.GetAllImageNames()
	// TODO once the UI starts sending valid responses, remove the validImages var
	// this is only for backward compat
	if image == validImage {
		return "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5", nil
	}
	if slices.Contains(allValidImages, image) {
		imageURL := s.imageProvider.BuildImageURL(image)
		return imageURL, nil
	}
	return "", fmt.Errorf("image %s is not valid", image)
}

func (s *handler) validateImageUpgrade(ctx context.Context, organizationID spec.OrganizationID, newImage, currentImage string) (string, error) {
	// if the new image a valid one?
	newImageURL, err := s.validateImage(ctx, organizationID, newImage)
	if err != nil {
		return "", err
	}

	// make sure the offering is the same and that the minor is bigger than the current one
	newImageInfo, err := s.imageProvider.ParseImageVersion(newImageURL)
	if err != nil {
		return "", err
	}
	currentImageInfo, err := s.imageProvider.ParseImageVersion(currentImage)
	if err != nil {
		return "", err
	}

	if newImageInfo.Offering != currentImageInfo.Offering {
		return "", fmt.Errorf("incompatible offering: %s is not compatible with %s", newImageInfo.Offering, currentImageInfo.Offering)
	}
	if newImageInfo.Major != currentImageInfo.Major {
		return "", fmt.Errorf("no major version upgrades supported: %d is different than current %d", newImageInfo.Major, currentImageInfo.Major)
	}

	if newImageInfo.Minor < currentImageInfo.Minor {
		return "", fmt.Errorf("new minor: %d is older than current  %d", newImageInfo.Minor, currentImageInfo.Minor)
	}

	return newImageURL, nil
}

// allocateCell allocates a cell in the region
func (s *handler) allocateCell(ctx context.Context, organizationID spec.OrganizationID, branchName, regionID string) (string, error) {
	cells, err := s.store.ListCells(ctx, organizationID, regionID)
	if err != nil {
		return "", err
	}

	if len(cells) == 0 {
		return "", ErrorInvalidParam{BranchName: branchName, Param: "region", Message: "cannot allocate to given region"}
	}

	strategy := s.sched.StrategyForRegion(regionID)
	cell, err := strategy.Schedule(ctx, cells)
	if err != nil {
		return "", fmt.Errorf("failed to schedule branch %q: %w", branchName, err)
	}

	return cell.ID, nil
}

// Describe a new branch
// (GET /organizations/{organizationID}/projects/{projectID}/branches/{branchID})
func (s *handler) DescribeBranch(c echo.Context, organizationID spec.OrganizationID, projectID, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		branch, err := s.store.DescribeBranch(c.Request().Context(), organizationID, projectID, branchID)
		if err != nil {
			return err
		}

		client, err := s.cells.GetCellConnection(c.Request().Context(), organizationID, branch.CellID)
		if err != nil {
			return err
		}
		defer client.Close()

		cluster, err := client.DescribePostgresCluster(c.Request().Context(), &clustersv1.DescribePostgresClusterRequest{Id: branchID})
		if err != nil {
			st, _ := status.FromError(err)
			if st.Code() == codes.NotFound {
				return ErrorBranchNotFound{
					BranchID: branchID,
				}
			}
			return err
		}

		// Extract major version from image name
		majorVersion := postgresversions.ExtractMajorVersionFromImage(cluster.Configuration.ImageName)

		// filter out parameters that are not configurable, as they might contain internal information
		cluster.Configuration.PostgresConfigurationParameters = s.postgresConfigProvider.FilterConfigurableParameters(cluster.Configuration.PostgresConfigurationParameters, majorVersion, cluster.Configuration.ImageName, cluster.Configuration.PreloadLibraries)

		// filter out internal preload libraries
		cluster.Configuration.PreloadLibraries = postgrescfg.FilterOutInternalPreloadLibraries(cluster.Configuration.PreloadLibraries)

		// get the connection string, ignore errors as we may not have a connection string yet
		connString, _ := s.getConnectionString(c, organizationID, branch)

		// get instance type from resources
		instanceType, err := s.getInstanceTypeByResources(c.Request().Context(), organizationID, branch.Region, cluster.Configuration.VcpuRequest, cluster.Configuration.VcpuLimit, cluster.Configuration.Memory)
		if err != nil {
			return fmt.Errorf("converting resources to instance type: %w", err)
		}

		return c.JSON(http.StatusOK, storeToAPIBranchMetadata(branch, connString, instanceType, cluster))
	})
}

func (s *handler) getConnectionString(c echo.Context, organizationID string, branch *store.Branch) (string, error) {
	// TODO I believe eventually this must be its own API call (ie, we may support several managed users in the future)
	client, err := s.cells.GetCellConnection(c.Request().Context(), organizationID, branch.CellID)
	if err != nil {
		return "", err
	}
	defer client.Close()

	// get gateway host:port from the region
	region, err := s.store.GetRegion(c.Request().Context(), organizationID, branch.Region)
	if err != nil {
		return "", err
	}

	hostPort := s.defaultGatewayHostPort
	if region.GatewayHostPort != "" {
		hostPort = region.GatewayHostPort
	}

	username := "superuser"
	// Do not expose superuser, use app user for Xata User feature flag
	if s.feat.BoolValue(c.Request().Context(), flags.XataUser) {
		username = "app"
	}

	creds, err := client.GetPostgresClusterCredentials(c.Request().Context(), &clustersv1.GetPostgresClusterCredentialsRequest{
		Id:       branch.ID,
		Username: username,
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("postgresql://%s:%s@%s.%s/xata?sslmode=require",
		creds.GetUsername(),
		creds.GetPassword(),
		branch.ID,
		hostPort), nil
}

// Get branch credentials
// (GET /organizations/{organizationID}/projects/{projectID}/branches/{branchID}/credentials)
func (s *handler) GetBranchCredentials(c echo.Context, organizationID spec.OrganizationID, projectID, branchID string, params spec.GetBranchCredentialsParams) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		username := "superuser"
		if params.Username != nil && *params.Username != "" {
			// default to superuser if not specified
			// TODO: Consider removing the default behavior in the future, making it mandatory to specify the type
			username = string(*params.Username)
		}
		branch, err := s.store.DescribeBranch(c.Request().Context(), organizationID, projectID, branchID)
		if err != nil {
			if errors.As(err, &store.ErrBranchNotFound{}) {
				return ErrorBranchNotFound{BranchID: branchID}
			}
			return err
		}

		client, err := s.cells.GetCellConnection(c.Request().Context(), organizationID, branch.CellID)
		if err != nil {
			return err
		}
		defer client.Close()

		creds, err := client.GetPostgresClusterCredentials(c.Request().Context(), &clustersv1.GetPostgresClusterCredentialsRequest{Id: branch.ID, Username: username})
		if err != nil {
			if errors.Is(err, clusters.SecretNotFoundForIDError(branch.ID)) {
				return ErrorCredentialsForBranchNotFound{BranchID: branch.ID, Username: username}
			}
			return err
		}

		return c.JSON(http.StatusOK, spec.BranchCredentials{
			Username: creds.GetUsername(),
			Password: creds.GetPassword(),
		})
	})
}

// Update a branch
// (PATCH /organizations/{organizationID}/projects/{projectID}/branches/{branchID})
func (s *handler) UpdateBranch(c echo.Context, organizationID spec.OrganizationID, projectID, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, OnlyEnabled, func() error {
		var body spec.UpdateBranchJSONRequestBody
		if err := api.ReadBody(c, &body); err != nil {
			return err
		}

		// we need at least one parameter to perform the update
		if body.Description == nil && body.Name == nil && !hasClusterConfigChanged(&body) {
			return ErrorInvalidParam{BranchName: branchID, Param: "all", Message: fmt.Sprintf("branch [%s]: at least one of the request fields needs to be set", branchID)}
		}

		if body.Replicas != nil && *body.Replicas < 0 {
			return ErrorInvalidParam{BranchName: branchID, Param: "configuration", Message: fmt.Sprintf("branch [%s]: cannot set number of replicas to less than 0", branchID)}
		}

		if err := validateBackupConfiguration(branchID, body.BackupConfiguration); err != nil {
			return err
		}

		branch, err := s.store.UpdateBranch(c.Request().Context(), organizationID, projectID, branchID, apiToStoreUpdateBranchConfig(body), func(branch *store.Branch) error {
			if !hasClusterConfigChanged(&body) {
				return nil
			}
			var config clustersv1.UpdateClusterConfiguration

			if body.Replicas != nil {
				config.NumInstances = new(*body.Replicas + 1)
			}
			if body.Storage != nil {
				config.StorageSize = body.Storage
			}
			if body.Hibernate != nil {
				config.Hibernate = body.Hibernate
			}
			if body.ScaleToZero != nil {
				config.ScaleToZero = apiToClustersScaleToZero(body.ScaleToZero, nil, nil)
			}
			// if the UI sends custom back we don't try to decode vcpu and memory
			if body.InstanceType != nil && *body.InstanceType != FallbackInstanceType {
				vcpuRequest, vcpuLimit, memory, err := s.getResourcesByInstanceType(c.Request().Context(), organizationID, branch.Region, *body.InstanceType)
				if err != nil {
					return ErrorInvalidParam{BranchName: branchID, Param: "configuration", Message: fmt.Sprintf("branch [%s]: unknown instance type %s", branchID, *body.InstanceType)}
				}
				config.VcpuRequest = &vcpuRequest
				config.VcpuLimit = &vcpuLimit
				config.Memory = &memory
			}

			client, err := s.cells.GetCellConnection(c.Request().Context(), organizationID, branch.CellID)
			if err != nil {
				return err
			}
			defer client.Close()

			// Fetch cluster info once if needed for any of the operations
			var cluster *clustersv1.DescribePostgresClusterResponse
			needsClusterInfo := (body.InstanceType != nil && *body.InstanceType != FallbackInstanceType) ||
				body.PostgresConfigurationParameters != nil ||
				body.PreloadLibraries != nil ||
				body.Image != nil
			if needsClusterInfo {
				cluster, err = client.DescribePostgresCluster(c.Request().Context(), &clustersv1.DescribePostgresClusterRequest{Id: branchID})
				if err != nil {
					st, _ := status.FromError(err)
					if st.Code() == codes.NotFound {
						return ErrorBranchNotFound{BranchID: branchID}
					}
					return err
				}
			}

			// Validate preload libraries against extensions available for this image
			if body.PreloadLibraries != nil {
				if err := s.postgresConfigProvider.ValidatePreloadLibraries(cluster.Configuration.ImageName, *body.PreloadLibraries); err != nil {
					return ErrorInvalidParam{BranchName: branchID, Param: "preloadLibraries", Message: fmt.Sprintf("branch [%s]: %v", branchID, err)}
				}
			}

			// If instance type is changing, we need to update default settings
			if body.InstanceType != nil && *body.InstanceType != FallbackInstanceType {
				oldInstanceType, err := s.getInstanceTypeByResources(c.Request().Context(), organizationID, branch.Region, cluster.Configuration.VcpuRequest, cluster.Configuration.VcpuLimit, cluster.Configuration.Memory)
				if err != nil {
					return fmt.Errorf("converting current resources (%s, %s, %s) to instance type: %w", cluster.Configuration.VcpuRequest, cluster.Configuration.VcpuLimit, cluster.Configuration.Memory, err)
				}

				if oldInstanceType != *body.InstanceType {
					currentParams := cluster.Configuration.PostgresConfigurationParameters
					if currentParams == nil {
						currentParams = make(map[string]string)
					}

					// Extract major version from image name
					majorVersion := postgresversions.ExtractMajorVersionFromImage(cluster.Configuration.ImageName)

					// Get parameter specifications for both old and new instance types
					oldSpecs, err := s.postgresConfigProvider.GetParametersSpec(oldInstanceType, majorVersion, cluster.Configuration.ImageName, cluster.Configuration.PreloadLibraries)
					if err != nil {
						return fmt.Errorf("failed to get parameter specifications for old instance type %s: %w", oldInstanceType, err)
					}

					newSpecs, err := s.postgresConfigProvider.GetParametersSpec(*body.InstanceType, majorVersion, cluster.Configuration.ImageName, cluster.Configuration.PreloadLibraries)
					if err != nil {
						return fmt.Errorf("failed to get parameter specifications for new instance type %s: %w", *body.InstanceType, err)
					}

					// Identify settings that are currently at their default values for the old instance type
					// and update them to the new instance type's defaults
					// For custom values, check if they're within the new instance type's min/max bounds
					updatedParams := make(map[string]string)
					for paramName, currentValue := range currentParams {
						oldSpec, oldExists := oldSpecs[paramName]
						newSpec, newExists := newSpecs[paramName]

						if !oldExists || !newExists {
							// Parameter doesn't exist in one of the specs, keep current value
							updatedParams[paramName] = currentValue
							continue
						}

						// Check if this parameter is at its default value for the old instance type
						if currentValue == oldSpec.DefaultValue {
							// This parameter is at its default value, update it to the new instance type's default
							updatedParams[paramName] = newSpec.DefaultValue
						} else {
							// This is a custom value, check if it's within the new instance type's bounds
							adjustedValue := currentValue

							// For numeric values, check min/max bounds
							if newSpec.MinValue != "" || newSpec.MaxValue != "" {
								adjustedValue = postgrescfg.AdjustValueToBounds(currentValue, newSpec)
							}

							updatedParams[paramName] = adjustedValue
						}
					}

					// Add any new default parameters that weren't in the current configuration
					for paramName, newSpec := range newSpecs {
						if _, exists := currentParams[paramName]; !exists {
							updatedParams[paramName] = newSpec.DefaultValue
						}
					}

					// Update the configuration with the new parameters
					config.PostgresConfigurationParameters = updatedParams
				}
			}

			if body.PostgresConfigurationParameters != nil {
				// find the instance type, because valid configuration depends on that
				instanceType := FallbackInstanceType
				if body.InstanceType != nil {
					body.InstanceType = &instanceType
				} else {
					// otherwise, find the instance type from the cluster (already fetched above)
					var err error
					instanceType, err = s.getInstanceTypeByResources(c.Request().Context(), organizationID, branch.Region, cluster.Configuration.VcpuRequest, cluster.Configuration.VcpuLimit, cluster.Configuration.Memory)
					if err != nil {
						return fmt.Errorf("converting resources to instance type: %w", err)
					}
				}
				params := *body.PostgresConfigurationParameters

				// Extract major version from image name (cluster is already fetched above)
				majorVersion := postgresversions.ExtractMajorVersionFromImage(cluster.Configuration.ImageName)

				// Use the new preload libraries if provided, otherwise use current
				preloadLibraries := cluster.Configuration.PreloadLibraries
				if body.PreloadLibraries != nil {
					preloadLibraries = *body.PreloadLibraries
				}

				errs, err := s.postgresConfigProvider.ValidateSettings(instanceType, params, majorVersion, cluster.Configuration.ImageName, preloadLibraries)
				if err != nil {
					return fmt.Errorf("invalid instance type %s: %w", instanceType, err)
				}
				if errs != nil {
					// Format validation errors into a user-friendly message
					var errorMessages []string
					for paramName, paramErr := range errs {
						errorMessages = append(errorMessages, fmt.Sprintf("%s: %s", paramName, paramErr.Error()))
					}
					errorMessage := fmt.Sprintf("PostgreSQL configuration validation failed: %s", strings.Join(errorMessages, "; "))
					return ErrorInvalidParam{
						BranchName: branchID,
						Param:      "postgres_configuration_parameters",
						Message:    errorMessage,
					}
				}

				config.PostgresConfigurationParameters = params
			}

			// Handle preload libraries update
			if body.PreloadLibraries != nil {
				config.PreloadLibraries = append(postgrescfg.GetInternalPreloadLibraries(), *body.PreloadLibraries...) // add internal preload libraries to the list

				majorVersion := postgresversions.ExtractMajorVersionFromImage(cluster.Configuration.ImageName)

				// Get current parameters to work with
				currentParams := config.PostgresConfigurationParameters
				if currentParams == nil {
					currentParams = cluster.Configuration.PostgresConfigurationParameters
				}
				if currentParams == nil {
					currentParams = make(map[string]string)
				}

				// Filter out parameters for extensions being removed from preload
				filteredParams := s.postgresConfigProvider.FilterConfigurableParameters(
					currentParams, majorVersion, cluster.Configuration.ImageName, config.PreloadLibraries)

				// Add default parameters for newly added extensions
				configurableParams := s.postgresConfigProvider.GetConfigurableParameters(majorVersion, cluster.Configuration.ImageName, config.PreloadLibraries)
				for paramName, spec := range configurableParams {
					// Only add extension parameters with a default value that don't already exist
					if spec.Extension != "" && spec.DefaultValue != "" {
						if _, exists := filteredParams[paramName]; !exists {
							filteredParams[paramName] = spec.DefaultValue
						}
					}
				}

				config.PostgresConfigurationParameters = filteredParams
			}

			// Handle backup configuration update
			if body.BackupConfiguration != nil {
				if !branch.BackupsEnabled {
					return ErrorInvalidParam{BranchName: branch.ID, Param: "backupConfiguration", Message: "backup configuration cannot be specified when backups are disabled in the selected region"}
				}
				backupConfig := &clustersv1.BackupConfiguration{
					BackupsEnabled: branch.BackupsEnabled,
				}
				if body.BackupConfiguration.RetentionPeriod != nil && *body.BackupConfiguration.RetentionPeriod != 0 {
					backupConfig.BackupRetention = fmt.Sprintf("%dd", *body.BackupConfiguration.RetentionPeriod)
				}
				if body.BackupConfiguration.BackupTime != nil && *body.BackupConfiguration.BackupTime != "" {
					backupConfig.BackupSchedule = generateCron(*body.BackupConfiguration.BackupTime)
				}
				config.BackupConfiguration = backupConfig
			}

			// Handle image minor version upgrades
			if body.Image != nil {
				// It can be argued that this should be decided by the operator. However - more than what
				// the operator supports - there should be a way for us to allow/disallow certain
				// upgrades - there can be business reasons for this and so it needs to happen here.
				imageURL, err := s.validateImageUpgrade(c.Request().Context(), organizationID, *body.Image, cluster.Configuration.ImageName)
				if err != nil {
					return ErrorInvalidParam{BranchName: branch.ID, Param: "image", Message: err.Error()}
				}
				config.ImageName = &imageURL
			}

			_, err = client.UpdatePostgresCluster(c.Request().Context(), &clustersv1.UpdatePostgresClusterRequest{
				Id:                  branch.ID,
				UpdateConfiguration: &config,
			})
			return err
		})
		if err != nil {
			st, _ := status.FromError(err)
			if st.Code() == codes.NotFound {
				return ErrorBranchNotFound{BranchID: branchID}
			}
			if st.Code() == codes.InvalidArgument {
				return ErrorInvalidParam{BranchName: branchID, Param: "configuration", Message: st.Message()}
			}
			if st.Code() == codes.PermissionDenied {
				return ErrorBranchUpdateForbidden{BranchID: branchID}
			}
			return err
		}

		var changedFields []string
		newValues := map[string]any{}
		if body.Name != nil {
			changedFields = append(changedFields, "name")
			newValues["name"] = *body.Name
		}
		if body.Description != nil {
			changedFields = append(changedFields, "description")
			newValues["description"] = *body.Description
		}
		if body.InstanceType != nil {
			changedFields = append(changedFields, "instance_type")
			newValues["instance_type"] = *body.InstanceType
		}
		if body.Replicas != nil {
			changedFields = append(changedFields, "replicas")
			newValues["replicas"] = *body.Replicas
		}
		if body.Storage != nil {
			changedFields = append(changedFields, "storage")
			newValues["storage_gi"] = *body.Storage
		}
		if body.Hibernate != nil {
			changedFields = append(changedFields, "hibernate")
			newValues["hibernate"] = *body.Hibernate
		}
		if body.ScaleToZero != nil {
			changedFields = append(changedFields, "scale_to_zero")
			newValues["scale_to_zero"] = body.ScaleToZero
		}
		if body.PostgresConfigurationParameters != nil {
			changedFields = append(changedFields, "postgres_config")
			newValues["postgres_config"] = *body.PostgresConfigurationParameters
		}
		if body.PreloadLibraries != nil {
			changedFields = append(changedFields, "preload_libraries")
			newValues["preload_libraries"] = *body.PreloadLibraries
		}
		if body.BackupConfiguration != nil {
			changedFields = append(changedFields, "backup_config")
			newValues["backup_config"] = body.BackupConfiguration
		}
		s.analytics.Track(c.Request().Context(), events.NewBranchUpdatedEvent(string(organizationID), projectID, branchID, changedFields, newValues))

		// get the connection string
		// swallow the error, the resource got created and the connection string will be eventually available
		connString, _ := s.getConnectionString(c, organizationID, branch)
		return c.JSON(http.StatusOK, storeToAPIBranchShortMetadata(branch, connString))
	})
}

func hasClusterConfigChanged(body *spec.UpdateBranchJSONRequestBody) bool {
	return body.Storage != nil ||
		body.InstanceType != nil ||
		body.Replicas != nil ||
		body.Hibernate != nil ||
		body.ScaleToZero != nil ||
		body.PostgresConfigurationParameters != nil ||
		body.PreloadLibraries != nil ||
		body.BackupConfiguration != nil ||
		body.Image != nil
}

// Delete a branch
// (DELETE /organizations/{organizationID}/projects/{projectID}/branches/{branchID})
func (s *handler) DeleteBranch(c echo.Context, organizationID spec.OrganizationID, projectID, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		log.Ctx(c.Request().Context()).Log().Msgf("Deleting branch [%s]", branchID)
		err := s.store.DeleteBranch(c.Request().Context(), organizationID, projectID, branchID, func(branch *store.Branch) error {
			return cells.DeprovisionBranch(c.Request().Context(), string(organizationID), s.store, s.cells, branch)
		})
		if err != nil {
			st, _ := status.FromError(err)
			if st.Code() == codes.NotFound {
				return ErrorBranchNotFound{BranchID: branchID}
			}
			return err
		}

		s.analytics.Track(c.Request().Context(), events.NewBranchDeletedEvent(string(organizationID), projectID, branchID))

		return c.NoContent(http.StatusNoContent)
	})
}

// GetDefaultProjectLimits returns the default project limits in the organization
// (GET /organizations/{organizationID}/projects/limits)
func (s *handler) GetDefaultProjectLimits(c echo.Context, organizationID spec.OrganizationID) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		// TODO remove validImages var once the UI no longer uses it
		allValidImages := append(s.imageProvider.GetAllImageNames(), validImage)
		return c.JSON(http.StatusOK, spec.ProjectLimits{
			MaxDescriptionLength: MaxBranchDescriptionLength,
			MaxInstances:         DefaultMaxInstances,
			MinInstances:         DefaultMinInstances,
			Images:               allValidImages,
			Regions:              DefaultRegion,
			MaxBranches:          store.MaxBranchesPerProject,
		})
	})
}

// BranchMetrics retrieves the branch metrics
// (POST /organizations/{organizationID}/projects/{projectID}/branches/{branchID}/metrics)
func (s *handler) BranchMetrics(c echo.Context, organizationID spec.OrganizationID, projectID string, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		var req spec.BranchMetricsRequest
		err := c.Bind(&req)
		if err != nil {
			return err
		}

		err = validateMetricsRequest(branchID, req)
		if err != nil {
			return err
		}

		_, err = s.store.DescribeBranch(c.Request().Context(), organizationID, projectID, branchID)
		if err != nil {
			return err
		}

		branchMetrics, err := s.metricsClient.GetMetric(c.Request().Context(), req.Start, req.End, string(req.Metric), stringArrayValue(req.Instances), stringArrayValue(req.Aggregations))
		if err != nil {
			return fmt.Errorf("getting metrics for branch [%s]: %w", branchID, err)
		}
		return c.JSON(http.StatusOK, branchMetrics)
	})
}

// Restore from backup
// (POST /organizations/{organizationID}/projects/{projectID}/branches/{branchID}/restore)
func (s *handler) RestoreFromBackup(c echo.Context, organizationID spec.OrganizationID, projectID, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, OnlyEnabled, func() error {
		// Check if branch creation is disabled (any type of branch)
		if s.feat.BoolValue(c.Request().Context(), flags.BranchCreationDisabled) {
			return ErrorBranchCreationDisabled{}
		}

		var body spec.RestoreFromBackupJSONRequestBody
		if err := api.ReadBody(c, &body); err != nil {
			return err
		}

		if err := validateBranchRequestCommons(body); err != nil {
			return err
		}

		var createClusterPayload ClusterServicePayload
		var err error
		ctx := c.Request().Context()

		// restore must happen in the same cell where the backup exists
		// otherwise we don't have access to the source object store
		sourceBranch, err := s.store.DescribeBranch(ctx, organizationID, projectID, branchID)
		if err != nil {
			if errors.As(err, &store.ErrBranchNotFound{}) {
				return ErrorBranchNotFound{BranchID: branchID}
			}
			return err
		}

		if body.Configuration == nil {
			// Inherit configuration from source branch
			if s.feat.BoolValue(ctx, flags.ChildBranchCreationDisabled) {
				return ErrorChildBranchCreationDisabled{}
			}
			createClusterPayload, err = s.handleBranchFromParent(ctx, organizationID, projectID, body.Name, spec.BranchFromParent{
				Mode:     spec.Inherit,
				ParentID: branchID,
			})
			if err != nil {
				return err
			}
		} else {
			// Use source branch's region if not specified, otherwise validate it matches
			if body.Configuration.Region == "" {
				body.Configuration.Region = sourceBranch.Region
			} else if body.Configuration.Region != sourceBranch.Region {
				return ErrorInvalidParam{BranchName: body.Name, Param: "region", Message: "restore must be in the same region as the source branch"}
			}
			// Use provided configuration
			createClusterPayload, err = s.handleBranchFromConfiguration(ctx, organizationID, projectID, body.Name, spec.BranchFromConfiguration{
				Mode:          spec.BranchFromConfigurationModeCustom,
				Configuration: *body.Configuration,
			})
			if err != nil {
				return err
			}
			// Use source branch's cell - the backup only exists there
			createClusterPayload.CellID = sourceBranch.CellID
			// Mark source branch as parent - this will always be the case
			createClusterPayload.ParentID = &branchID
		}

		if !createClusterPayload.BackupsEnabled && body.BackupConfiguration != nil {
			return ErrorInvalidParam{BranchName: body.Name, Param: "backupConfiguration", Message: "backup configuration cannot be specified when backups are disabled in the selected region"}
		}

		return s.withProject(c, organizationID, projectID, func(project *store.Project) error {
			branch, err := s.store.CreateBranch(ctx, organizationID, projectID, createClusterPayload.CellID, &store.CreateBranchConfiguration{
				Name:                  body.Name,
				ParentID:              createClusterPayload.ParentID,
				Description:           body.Description,
				BackupRetentionPeriod: apiToStoreBackupConfig(body.BackupConfiguration),
				BackupsEnabled:        createClusterPayload.BackupsEnabled,
			}, func(branch *store.Branch) error {
				scaleToZero := apiToClustersScaleToZero(body.ScaleToZero, createClusterPayload.ParentID, project)
				createClusterPayload.Configuration.ScaleToZero = scaleToZero
				request := clustersv1.CreatePostgresClusterRequest{
					Id:             branch.ID,
					OrganizationId: organizationID,
					ProjectId:      projectID,
					ParentId:       branch.ParentID,
					Configuration:  &createClusterPayload.Configuration,
					DataSource: &clustersv1.CreatePostgresClusterRequest_ContinuousBackup{
						ContinuousBackup: &clustersv1.ContinuousBackup{
							ClusterId: branchID, // the source branch ID
						},
					},
					BackupConfiguration: apiToClustersBackupConfig(body.BackupConfiguration, createClusterPayload.BackupsEnabled),
				}

				client, err := s.cells.GetCellConnection(ctx, organizationID, createClusterPayload.CellID)
				if err != nil {
					return err
				}
				defer client.Close()

				_, err = client.CreatePostgresCluster(ctx, &request)
				if err != nil {
					return err
				}

				return s.setupBranchOnPrimaryCell(ctx, organizationID, createClusterPayload.Region, createClusterPayload.CellID, branch.ID, project)
			})
			if err != nil {
				st, _ := status.FromError(err)
				if st.Code() == codes.NotFound {
					return ErrorBranchNotFound{BranchID: branchID}
				}
				if st.Code() == codes.InvalidArgument {
					return ErrorInvalidParam{BranchName: body.Name, Param: "configuration", Message: st.Message()}
				}
				return err
			}

			s.analytics.Track(ctx, events.NewBranchRestoredFromBackupEvent(string(organizationID), projectID, branchID, branch.ID))

			// swallow the error, the resource got created and the connection string will be eventually available
			connString, _ := s.getConnectionString(c, organizationID, branch)
			return c.JSON(http.StatusCreated, storeToAPIBranchShortMetadata(branch, connString))
		})
	})
}

func stringArrayValue[T ~string](v []T) []string {
	if len(v) == 0 {
		return nil
	}
	strs := make([]string, len(v))
	for i, s := range v {
		strs[i] = string(s)
	}
	return strs
}

func validateMetricsRequest(branchID string, req spec.BranchMetricsRequest) error {
	if req.End.Before(req.Start) {
		return ErrorInvalidParam{BranchName: branchID, Param: "start", Message: "start time must come before end time"}
	}

	if req.End.Sub(req.Start) > maxDateRange {
		return ErrorInvalidParam{BranchName: branchID, Param: "end", Message: "maximum date range is " + maxDateRange.String()}
	}

	for _, instance := range req.Instances {
		if !strings.HasPrefix(instance, branchID) {
			return ErrorInvalidParam{BranchName: branchID, Param: "instances", Message: fmt.Sprintf("invalid instance [%s]", instance)}
		}
	}

	return nil
}

// setupBranchOnPrimaryCell registers a cluster with the primary cell if it was
// created on a secondary cell, and applies IP filtering settings from the project.
func (s *handler) setupBranchOnPrimaryCell(ctx context.Context, organizationID spec.OrganizationID, region, cellID, branchID string, project *store.Project) error {
	primaryCell, err := s.store.GetPrimaryCell(ctx, organizationID, region)
	if err != nil {
		return err
	}

	hasIPFiltering := project.IPFiltering.Enabled || len(project.IPFiltering.CIDRs) > 0
	needsRegistration := primaryCell.ID != cellID

	if !hasIPFiltering && !needsRegistration {
		return nil
	}

	client, err := s.cells.GetCellConnection(ctx, organizationID, primaryCell.ID)
	if err != nil {
		return err
	}
	defer client.Close()

	if hasIPFiltering {
		_, err = client.SetBranchIPFiltering(ctx, &clustersv1.SetBranchIPFilteringRequest{
			BranchId: branchID,
			IpFiltering: &clustersv1.IPFilteringConfig{
				Enabled: project.IPFiltering.Enabled,
				Allowed: project.IPFiltering.CIDRStrings(),
			},
		})
		if err != nil {
			return fmt.Errorf("setting IP filtering for branch: %w", err)
		}
	}

	if needsRegistration {
		_, err = client.RegisterPostgresCluster(ctx, &clustersv1.RegisterPostgresClusterRequest{Id: branchID})
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *handler) withOrganizationAccess(c echo.Context, organizationID spec.OrganizationID, p Permission, fn func() error) error {
	claims := api.GetUserClaims(c)
	if claims == nil {
		return echo.NewHTTPError(http.StatusUnauthorized)
	}

	if !claims.HasAccessToOrganization(organizationID) {
		return api.ErrorAuthorizationFailed{Reason: fmt.Sprintf("no access to organization [%s]", organizationID)}
	}
	if p == OnlyEnabled && !claims.IsEnabledOrganization(organizationID) {
		return ErrorOrganizationDisabled{organizationID}
	}

	o11y.SetReqAttribute(c, api.OrganizationO11yK, organizationID)
	o11y.SetReqAttribute(c, api.UserIDO11yK, claims.UserID())

	return fn()
}

func (s *handler) withProject(c echo.Context, organizationID spec.OrganizationID, projectID string, fn func(project *store.Project) error) error {
	project, err := s.store.GetProject(c.Request().Context(), organizationID, projectID)
	if err != nil {
		return err
	}

	return fn(project)
}

// GetBranchPostgresConfig retrieves detailed information about PostgreSQL configuration parameters for a branch
// (GET /organizations/{organizationID}/projects/{projectID}/branches/{branchID}/postgres-config)
func (s *handler) GetBranchPostgresConfig(c echo.Context, organizationID spec.OrganizationID, projectID string, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		branch, err := s.store.DescribeBranch(c.Request().Context(), organizationID, projectID, branchID)
		if err != nil {
			return err
		}

		client, err := s.cells.GetCellConnection(c.Request().Context(), organizationID, branch.CellID)
		if err != nil {
			return err
		}
		defer client.Close()

		cluster, err := client.DescribePostgresCluster(c.Request().Context(), &clustersv1.DescribePostgresClusterRequest{Id: branchID})
		if err != nil {
			st, _ := status.FromError(err)
			if st.Code() == codes.NotFound {
				return ErrorBranchNotFound{
					BranchID: branchID,
				}
			}
			return err
		}

		instanceType, err := s.getInstanceTypeByResources(c.Request().Context(), organizationID, branch.Region, cluster.Configuration.VcpuRequest, cluster.Configuration.VcpuLimit, cluster.Configuration.Memory)
		if err != nil {
			return fmt.Errorf("converting resources to instance type: %w", err)
		}

		majorVersion := postgresversions.ExtractMajorVersionFromImage(cluster.Configuration.ImageName)

		configurableParams := s.postgresConfigProvider.GetConfigurableParameters(majorVersion, cluster.Configuration.ImageName, cluster.Configuration.PreloadLibraries)

		instanceDefaults, err := s.postgresConfigProvider.GetParametersSpec(instanceType, majorVersion, cluster.Configuration.ImageName, cluster.Configuration.PreloadLibraries)
		if err != nil {
			return fmt.Errorf("getting instance defaults: %w", err)
		}

		// Merge configurable parameters with instance-specific defaults
		mergedParams := postgrescfg.MergeParametersMaps(configurableParams, instanceDefaults)

		// Convert to API format using helper function
		parameters := postgresConfigToAPIParameters(mergedParams, instanceType, cluster.Configuration.PostgresConfigurationParameters, cluster.Configuration.ImageName, cluster.Configuration.PreloadLibraries)

		return c.JSON(http.StatusOK, spec.PostgresConfigDetails{
			Parameters: parameters,
		})
	})
}

// (POST /organizations/{organizationID}/githubapp/installations)
func (s *handler) CreateGithubAppInstallation(c echo.Context, organizationID spec.OrganizationID) error {
	return s.withOrganizationAccess(c, organizationID, OnlyEnabled, func() error {
		var body spec.CreateGithubAppInstallationJSONRequestBody
		if err := api.ReadBody(c, &body); err != nil {
			return err
		}

		inst, err := s.store.CreateGithubInstallation(c.Request().Context(), organizationID, body.InstallationId)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusCreated, storeToAPIGithubInstallation(inst))
	})
}

// (GET /organizations/{organizationID}/githubapp/installations)
func (s *handler) ListGithubAppInstallations(c echo.Context, organizationID spec.OrganizationID) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		installations, err := s.store.ListGithubInstallations(c.Request().Context(), organizationID)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, struct {
			Installations []spec.GithubInstallation `json:"installations"`
		}{storeToAPIGithubInstallationList(installations)})
	})
}

// (PUT /organizations/{organizationID}/githubapp/installations/{githubInstallationID})
func (s *handler) UpdateGithubAppInstallation(c echo.Context, organizationID spec.OrganizationID, githubInstallationID string) error {
	return s.withOrganizationAccess(c, organizationID, OnlyEnabled, func() error {
		var body spec.UpdateGithubAppInstallationJSONRequestBody
		if err := api.ReadBody(c, &body); err != nil {
			return err
		}

		inst, err := s.store.UpdateGithubInstallation(c.Request().Context(), organizationID, githubInstallationID, body.InstallationId)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, storeToAPIGithubInstallation(inst))
	})
}

// (GET /organizations/{organizationID}/projects/{projectID}/branches/{branchID}/githubapp/repository)
func (s *handler) GetGithubRepository(c echo.Context, organizationID spec.OrganizationID, projectID string, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		mapping, err := s.store.GetGithubRepoMappingByProject(c.Request().Context(), organizationID, projectID)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, map[string]any{"mapping": storeToAPIGithubRepository(mapping)})
	})
}

// (POST /organizations/{organizationID}/projects/{projectID}/branches/{branchID}/githubapp/repository)
func (s *handler) CreateGithubRepository(c echo.Context, organizationID spec.OrganizationID, projectID string, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, OnlyEnabled, func() error {
		var body spec.CreateGithubRepositoryJSONRequestBody
		if err := api.ReadBody(c, &body); err != nil {
			return err
		}
		if body.GithubRepositoryID <= 0 {
			return ErrorInvalidParam{Param: "githubRepositoryID", Message: "must be greater than 0"}
		}
		if strings.TrimSpace(branchID) == "" {
			return ErrorInvalidParam{Param: "branchID", Message: "must not be empty"}
		}

		mapping, err := s.store.CreateGithubRepoMapping(c.Request().Context(), organizationID, projectID, body.GithubRepositoryID, branchID)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusCreated, storeToAPIGithubRepository(mapping))
	})
}

// (PUT /organizations/{organizationID}/projects/{projectID}/branches/{branchID}/githubapp/repository)
func (s *handler) UpdateGithubRepository(c echo.Context, organizationID spec.OrganizationID, projectID string, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, OnlyEnabled, func() error {
		var body spec.UpdateGithubRepositoryJSONRequestBody
		if err := api.ReadBody(c, &body); err != nil {
			return err
		}
		if body.GithubRepositoryID <= 0 {
			return ErrorInvalidParam{Param: "githubRepositoryID", Message: "must be greater than 0"}
		}
		if strings.TrimSpace(branchID) == "" {
			return ErrorInvalidParam{Param: "branchID", Message: "must not be empty"}
		}

		mapping, err := s.store.UpdateGithubRepoMapping(c.Request().Context(), organizationID, projectID, body.GithubRepositoryID, branchID)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, storeToAPIGithubRepository(mapping))
	})
}

// (DELETE /organizations/{organizationID}/projects/{projectID}/branches/{branchID}/githubapp/repository)
func (s *handler) DeleteGithubRepository(c echo.Context, organizationID spec.OrganizationID, projectID string, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		err := s.store.DeleteGithubRepoMapping(c.Request().Context(), organizationID, projectID)
		if err != nil {
			return err
		}

		return c.NoContent(http.StatusNoContent)
	})
}
