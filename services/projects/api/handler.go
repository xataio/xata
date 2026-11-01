package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"xata/internal/analytics"
	"xata/internal/analytics/events"
	"xata/internal/api"
	"xata/internal/api/clienthttpheaders"
	"xata/internal/extensions"
	"xata/internal/flags"
	"xata/internal/idgen"
	"xata/internal/o11y"
	"xata/internal/openfeature"
	"xata/internal/postgrescfg"
	"xata/internal/postgresversions"
	"xata/services/clusters"
	"xata/services/projects/api/spec"
	branchsvc "xata/services/projects/branch"
	"xata/services/projects/cells"
	"xata/services/projects/metrics"
	"xata/services/projects/provisioner"
	"xata/services/projects/scheduler"
	"xata/services/projects/store"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/utils/ptr"

	clustersv1 "xata/gen/proto/clusters/v1"

	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

const (
	DefaultBackupFrequency = "weekly"
	projectLockRetryAfter  = "1"
)

type Permission int

const (
	All Permission = iota
	OnlyEnabled
)

var (
	DefaultMaxInstances = 5
	DefaultMinInstances = 1

	// maxDateRange is the maximum date range for metrics queries
	maxDateRange = 6 * 30 * 24 * time.Hour // 6 months

	// internalExtensions are extensions that should not be exposed in the API
	internalExtensions = []string{"xatautils"}
)

type handler struct {
	store     store.ProjectsStore
	cells     cells.Cells
	feat      openfeature.Client
	analytics analytics.Client

	// metricsClient routes branch metric/log queries to the per-cell
	// VictoriaMetrics/VictoriaLogs backend via the clusters gRPC service.
	metricsClient metrics.Client

	// postgresConfigProvider is the provider for PostgreSQL configuration operations
	postgresConfigProvider postgrescfg.PostgresConfigProvider

	// imageProvider is the provider for the PostgreSQL images
	imageProvider postgresversions.ImageProvider

	// provisioner handles branch create/delete provisioning
	provisioner provisioner.Provisioner

	// branches holds the branch business logic shared with other callers
	// (e.g. Vercel resource provisioning).
	branches *branchsvc.Service

	githubInstallationValidator GithubInstallationValidator
}

type GithubInstallationValidator interface {
	// ValidateUserInstallationAccess checks that the GitHub app installation is
	// visible to the GitHub user connected to the Xata user identified by the
	// given session token.
	ValidateUserInstallationAccess(ctx context.Context, sessionToken string, installationID int64) error
	ValidateRepositoryAccess(ctx context.Context, repositoryID int64, installationIDs []int64) error
}

type HandlerOption func(*handler)

func WithGithubInstallationValidator(v GithubInstallationValidator) HandlerOption {
	return func(h *handler) {
		h.githubInstallationValidator = v
	}
}

// WithBranchService injects the branch business-logic service so the handler
// shares a single instance with other callers (e.g. the Branches() accessor the
// Vercel resource handler uses). Without it the handler builds its own.
func WithBranchService(branches *branchsvc.Service) HandlerOption {
	return func(h *handler) {
		h.branches = branches
	}
}

func NewAPIHandler(feat openfeature.Client, store store.ProjectsStore, cells cells.Cells, gatewayHostPort string, metricsClient metrics.Client, scheduler *scheduler.Scheduler, analytics analytics.Client, postgresConfigProvider postgrescfg.PostgresConfigProvider, imageProvider postgresversions.ImageProvider, orch provisioner.Provisioner, opts ...HandlerOption) spec.ServerInterface {
	h := &handler{
		feat:                   feat,
		store:                  store,
		cells:                  cells,
		metricsClient:          metricsClient,
		analytics:              analytics,
		postgresConfigProvider: postgresConfigProvider,
		imageProvider:          imageProvider,
		provisioner:            orch,
	}
	for _, opt := range opts {
		opt(h)
	}
	// Build a default branch service when the caller did not inject one. SaaS
	// wrappers pass WithBranchService so the handler and the Branches() accessor
	// share a single instance.
	if h.branches == nil {
		h.branches = branchsvc.New(store, cells, feat, scheduler, gatewayHostPort, postgresConfigProvider, imageProvider, orch)
	}
	return h
}

// tryAcquireProjectLock takes the project lock exclusively, for operations that
// must not overlap with any branch creation (IP filtering updates). Branch
// creation takes the lock in shared mode inside the store's create transaction.
func (s *handler) tryAcquireProjectLock(c echo.Context, projectID string) (func() error, error) {
	release, err := s.store.TryAcquireProjectLock(c.Request().Context(), projectID)
	if err == nil {
		return release, nil
	}

	if _, ok := errors.AsType[store.ErrProjectBusy](err); ok {
		c.Response().Header().Set("Retry-After", projectLockRetryAfter)
	}

	return nil, fmt.Errorf("acquire project lock: %w", err)
}

// maxMetricsPerRequest bounds the per-request fan-out to the backend (one
// HTTP/PromQL call per metric). It matches the `metrics` maxItems in the
// OpenAPI spec, but is enforced here because requests are not validated
// against the spec at runtime.
const maxMetricsPerRequest = 15

// Get list of regions available for the organization
// (GET /organizations/{organizationID}/regions)
func (s *handler) ListRegions(c echo.Context, organizationID spec.OrganizationID) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		marketplace := api.GetUserClaims(c).Organizations[organizationID].Marketplace
		regions, err := s.store.ListRegions(c.Request().Context(), organizationID)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, struct {
			Regions []store.Region `json:"regions"`
		}{filterRegionsForMarketplace(marketplace, regions)})
	})
}

// Get list of images available for the organization
// (GET /organizations/{organizationID}/images)
func (s *handler) ListImages(c echo.Context, organizationID spec.OrganizationID, params spec.ListImagesParams) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		marketplace := api.GetUserClaims(c).Organizations[organizationID].Marketplace
		if params.Region != nil {
			err := s.validateRegion(c.Request().Context(), validateRegionParams{
				organizationID: organizationID,
				marketplace:    marketplace,
				region:         *params.Region,
			})
			if err != nil {
				if isInvalidRegionError(err) {
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

		// Filter out older minors hidden by default if the feature flag is not enabled
		legacyEnabled := s.feat.BoolValue(c.Request().Context(), flags.LegacyPgVersions)
		if !legacyEnabled {
			hidden := postgresversions.HiddenImageNames()
			filtered := make([]string, 0, len(images))
			for _, img := range images {
				if !slices.Contains(hidden, img) {
					filtered = append(filtered, img)
				}
			}
			images = filtered
		}

		// Filter out major versions hidden by default, unless the organization
		// has the flag for that major enabled
		if hiddenMajors := postgresversions.HiddenMajorImages(); len(hiddenMajors) > 0 {
			enabled := make(map[string]bool)
			filtered := make([]string, 0, len(images))
			for _, img := range images {
				major, hidden := hiddenMajors[img]
				if !hidden {
					filtered = append(filtered, img)
					continue
				}
				if _, ok := enabled[major]; !ok {
					enabled[major] = s.branches.MajorVersionEnabled(c.Request().Context(), major)
				}
				if enabled[major] {
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
		marketplace := api.GetUserClaims(c).Organizations[organizationID].Marketplace
		if params.Region != nil {
			err := s.validateRegion(c.Request().Context(), validateRegionParams{
				organizationID: organizationID,
				marketplace:    marketplace,
				region:         *params.Region,
			})
			if err != nil {
				if isInvalidRegionError(err) {
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
		marketplace := api.GetUserClaims(c).Organizations[organizationID].Marketplace
		err := s.validateRegion(c.Request().Context(), validateRegionParams{
			organizationID: organizationID,
			marketplace:    marketplace,
			region:         params.Region,
		})
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

		claims := api.GetUserClaims(c)
		filtered := make([]store.Project, 0, len(projects))
		for _, p := range projects {
			if claims.HasAccessToProject(p.ID) {
				filtered = append(filtered, p)
			}
		}

		return c.JSON(http.StatusOK, struct {
			Projects []spec.Project `json:"projects"`
		}{storeToAPIProjectList(filtered)})
	})
}

// Create a new project
// (POST /organizations/{organizationID}/projects)
func (s *handler) CreateProject(c echo.Context, organizationID spec.OrganizationID) error {
	return s.withOrganizationAccess(c, organizationID, OnlyEnabled, func() error {
		ctx := c.Request().Context()
		claims := api.GetUserClaims(c)

		var req spec.CreateProjectJSONBody
		if err := c.Bind(&req); err != nil {
			return err
		}

		if req.Configuration != nil {
			if err := validateIPFiltering(req.Configuration.IpFiltering); err != nil {
				return err
			}
		}

		cfg := apiToStoreCreateProjectConfig(req, claims.Organizations[organizationID].UsageTier)
		createdProject, err := s.store.CreateProject(ctx, organizationID, cfg)
		if err != nil {
			return err
		}

		s.analytics.Track(ctx, events.NewProjectCreatedEvent(string(organizationID), createdProject.ID))

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

		// Authorize the key against the resolved branch, not just the
		// organization: backupID is used as the branch ID, so a branch-scoped
		// key must not read another branch's recovery-window metadata. Return
		// not-found rather than forbidden to avoid leaking branch existence.
		if !api.GetUserClaims(c).HasAccessToBranch(branch.ID) {
			return ErrorBackupNotFound{ID: backupID}
		}

		setRegionReqAttribute(c, branch.Region)
		client, err := s.cells.GetCellConnection(c.Request().Context(), organizationID, branch.CellID)
		if err != nil {
			return err
		}
		defer client.Close()

		window, err := client.GetRecoveryWindow(c.Request().Context(), &clustersv1.GetRecoveryWindowRequest{
			Id: branch.ID,
		})
		if err != nil {
			// The cell returns NotFound when the branch has no backups.
			if status.Code(err) == codes.NotFound {
				return ErrorBackupNotFound{ID: backupID}
			}
			return err
		}

		earliestRestore, latestRestore, err := parseRecoveryWindow(window)
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

		if body.Configuration != nil {
			if err := validateIPFiltering(body.Configuration.IpFiltering); err != nil {
				return err
			}
		}

		ctx := c.Request().Context()
		updateConfig := apiToStoreUpdateProjectConfig(body)

		// If IP filtering is being updated, acquire project lock to prevent race conditions
		// with branch creation.
		if updateConfig.IPFiltering != nil {
			releaseLock, err := s.tryAcquireProjectLock(c, projectID)
			if err != nil {
				return err
			}
			defer releaseLock()
		}

		project, err := s.store.UpdateProject(ctx, organizationID, projectID, updateConfig, func(_ *store.Project) error {
			if updateConfig.IPFiltering == nil {
				return nil
			}
			return s.applyIPFilteringToBranchCells(ctx, organizationID, projectID, updateConfig.IPFiltering)
		})
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

// applyIPFilteringToBranchCells applies IP filtering settings to each branch
// on the cell it runs in, before saving to the DB. Returns an error if any
// call fails.
func (s *handler) applyIPFilteringToBranchCells(ctx context.Context, organizationID string, projectID string, ipFiltering *store.IPFiltering) error {
	// Get all branches for the project
	branches, err := s.store.ListBranches(ctx, organizationID, projectID)
	if err != nil {
		return fmt.Errorf("listing branches: %w", err)
	}

	if len(branches) == 0 {
		// No branches to update, nothing to do
		return nil
	}

	// Group branches by cell
	cellToBranches := make(map[string][]string)
	for _, branch := range branches {
		cellID := branch.CellID
		if cellID == "" {
			return fmt.Errorf("branch %s has no cell", branch.ID)
		}
		cellToBranches[cellID] = append(cellToBranches[cellID], branch.ID)
	}

	cellToClient := make(map[string]cells.CellClient)

	// Clean up cell connections when done
	defer func() {
		for _, client := range cellToClient {
			if client != nil {
				client.Close()
			}
		}
	}()

	// Get a connection for each unique cell
	for cellID := range cellToBranches {
		cellClient, err := s.cells.GetCellConnection(ctx, organizationID, cellID)
		if err != nil {
			return fmt.Errorf("connecting to cell %s: %w", cellID, err)
		}

		cellToClient[cellID] = cellClient
	}

	ipFilteringConfig := &clustersv1.IPFilteringConfig{
		Enabled: ipFiltering.Enabled,
		Allowed: ipFiltering.CIDRStrings(),
	}

	// Apply IP filtering to all branches in each cell with a single call per cell
	for cellID, branchIDs := range cellToBranches {
		_, err := cellToClient[cellID].SetBranchesIPFiltering(ctx, &clustersv1.SetBranchesIPFilteringRequest{
			BranchIds:   branchIDs,
			IpFiltering: ipFilteringConfig,
		})
		if err != nil {
			return fmt.Errorf("setting IP filtering for branches in cell %s: %w", cellID, err)
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

		claims := api.GetUserClaims(c)
		filtered := make([]store.Branch, 0, len(branches))
		for _, b := range branches {
			if claims.HasAccessToBranch(b.ID) {
				filtered = append(filtered, b)
			}
		}

		return c.JSON(http.StatusOK, struct {
			Branches []spec.BranchListMetadata `json:"branches"`
		}{storeToAPIListBranchListMetadata(filtered)})
	})
}

// validateStorageSize delegates to the branch service's storage validator so the
// rule lives in one place, mapping the domain error back to the api error type.
func validateStorageSize(branchName string, storageGB int32, max int) error {
	return apiErrorFromBranch(branchsvc.ValidateStorage(branchName, storageGB, max))
}

type ValidatableCreateRequest interface {
	spec.CreateBranchJSONRequestBody | spec.RestoreFromBackupJSONRequestBody
}

func validateBranchRequestCommons[T ValidatableCreateRequest](body T) error {
	v := any(body)
	var name string
	var backupConfig *spec.BackupConfiguration

	switch req := v.(type) {
	case spec.CreateBranchJSONRequestBody:
		name = req.Name
		backupConfig = req.BackupConfiguration
	case spec.RestoreFromBackupJSONRequestBody:
		name = req.Name
		backupConfig = req.BackupConfiguration
	}
	if name == "" {
		return ErrorInvalidParam{BranchName: name, Param: "name", Message: "branch name is required"}
	}

	return validateBackupConfiguration(name, backupConfig)
}

// validateBackupConfiguration delegates to the branch service's validator (via
// toBranchBackup) so the retention/time rules live in one place, and maps the
// domain error back to the api error type.
func validateBackupConfiguration(branchName string, c *spec.BackupConfiguration) error {
	return apiErrorFromBranch(branchsvc.ValidateBackup(branchName, toBranchBackup(c)))
}

// ClusterServicePayload is an alias for the provisioner payload type, kept
// for use by other handler methods (e.g. RestoreFromBackup).
type ClusterServicePayload = provisioner.ClusterServicePayload

// Create a new branch
// (POST /organizations/{organizationID}/projects/{projectID}/branches)
func (s *handler) CreateBranch(c echo.Context, organizationID spec.OrganizationID, projectID string) error {
	// NOTE: Branch creation is also triggered by the GitHub App webhook (saas-services/projects/webhooks).
	// Common creation logic lives in the branch service (BuildPayload + Provision), which both
	// paths call — changes to how branches are created should go there, not here. This handler
	// should only deal with HTTP concerns: input parsing, auth, feature flags, and analytics.
	return s.withOrganizationAccess(c, organizationID, OnlyEnabled, func() error {
		// Check if branch creation is disabled (any type of branch)
		if s.feat.BoolValue(c.Request().Context(), flags.BranchCreationDisabled) {
			return ErrorBranchCreationDisabled{}
		}

		ctx := c.Request().Context()
		useXatastor := s.feat.BoolValue(ctx, flags.UseXatastor)
		usePgBackRest := s.feat.BoolValue(ctx, flags.UsePgBackRest)
		log.Ctx(ctx).Info().Bool("usePgBackRest", usePgBackRest).Msg("pgbackrest feature flag")

		claims := api.GetUserClaims(c)
		marketplace := claims.Organizations[organizationID].Marketplace
		featureFlags := provisioner.FlagsConfig{
			UsePool:     s.feat.BoolValue(ctx, flags.UseClusterPool),
			UseXatastor: useXatastor,
		}
		orgLimits, err := provisioner.ResolveOrgLimits(ctx, s.store, claims.Organizations[organizationID].UsageTier, organizationID, projectID, featureFlags)
		if err != nil {
			return err
		}

		if claims != nil && !useXatastor {
			if org, ok := claims.Organizations[string(organizationID)]; ok && org.IsNewOrganization() {
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

		if err := IsBranchDescriptionValid(body.Description, orgLimits.MaxDescriptionLength); err != nil {
			return err
		}

		// Name and backup validation live in the branch service (BuildPayload)
		// now — the single gate shared with the Vercel handler — so we do not
		// re-run validateBranchRequestCommons here. RestoreFromBackup, which does
		// not go through the service, still calls it.

		value, err := body.ValueByDiscriminator()
		if err != nil {
			return ErrorInvalidParam{BranchName: body.Name, Param: "body", Message: fmt.Sprintf("failed to parse branch creation details - %s", err.Error())}
		}

		branchMode, err := toBranchMode(value)
		if err != nil {
			return err
		}

		in := branchsvc.CreateInput{
			OrganizationID: organizationID,
			ProjectID:      projectID,
			Name:           body.Name,
			Mode:           branchMode,
			Marketplace:    marketplace,
			UsageTier:      claims.Organizations[organizationID].UsageTier,
			OrgLimits:      orgLimits,
			Flags:          featureFlags,
			UsePgBackRest:  usePgBackRest,
			Description:    body.Description,
			Backup:         toBranchBackup(body.BackupConfiguration),
			ScaleToZero:    toBranchScaleToZero(body.ScaleToZero),
		}

		payload, err := s.branches.BuildPayload(ctx, in)
		if err != nil {
			return apiErrorFromBranch(err)
		}
		// Set the region attribute BEFORE provisioning so a failed create (lock
		// contention, the backups-disabled 400, any provisioner error) still
		// carries the region in the request log and span — the failures we
		// debug by region. setRegionReqAttribute replaces the request context,
		// so re-read it before provisioning.
		setRegionReqAttribute(c, payload.Region)
		ctx = c.Request().Context()

		br, connString, err := s.branches.Provision(ctx, in, payload)
		if err != nil {
			// Surface Retry-After on lock contention, matching the previous
			// tryAcquireProjectLock behavior.
			if _, ok := errors.AsType[store.ErrProjectBusy](err); ok {
				c.Response().Header().Set("Retry-After", projectLockRetryAfter)
			}
			return apiErrorFromBranch(err)
		}

		var analyticsEvent events.Event
		switch mode := value.(type) {
		case spec.BranchFromConfiguration:
			analyticsEvent = events.NewBranchFromConfigurationEvent(
				string(organizationID),
				projectID,
				br.ID,
				br.Region,
				string(mode.Configuration.Image),
				mode.Configuration.InstanceType,
				int(mode.Configuration.Replicas),
				mode.Configuration.Storage,
			)
		case spec.BranchFromParent:
			analyticsEvent = events.NewBranchFromParentEvent(string(organizationID), projectID, mode.ParentID, br.ID, br.Region)
		}
		s.analytics.Track(c.Request().Context(), analyticsEvent)

		return c.JSON(http.StatusCreated, storeToAPIBranchShortMetadata(br, connString))
	})
}

type validateRegionParams struct {
	organizationID spec.OrganizationID
	marketplace    string
	region         string
}

func (s *handler) validateRegion(ctx context.Context, params validateRegionParams) error {
	regions, err := s.store.ListRegions(ctx, params.organizationID)
	if err != nil {
		return err
	}

	for _, region := range regions {
		if params.region == region.ID {
			if branchsvc.IsRegionAvailableForMarketplace(params.marketplace, region) {
				return nil
			}
			return ErrorInvalidRegion{Message: fmt.Sprintf("provider %s is not available for %s marketplace organizations", region.Provider, params.marketplace)}
		}
	}
	return ErrorInvalidRegion{Message: fmt.Sprintf("region %s is not found", params.region)}
}

func filterRegionsForMarketplace(marketplace string, regions []store.Region) []store.Region {
	if marketplace == "" {
		return regions
	}

	filtered := make([]store.Region, 0, len(regions))
	for _, region := range regions {
		if branchsvc.IsRegionAvailableForMarketplace(marketplace, region) {
			filtered = append(filtered, region)
		}
	}
	return filtered
}

type ErrorInvalidRegion struct {
	Message string
}

func (e ErrorInvalidRegion) Error() string {
	return e.Message
}

func isInvalidRegionError(err error) bool {
	var invalid ErrorInvalidRegion
	return errors.As(err, &invalid)
}

// Describe a new branch
// (GET /organizations/{organizationID}/projects/{projectID}/branches/{branchID})
func (s *handler) DescribeBranch(c echo.Context, organizationID spec.OrganizationID, projectID, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		branch, err := s.store.DescribeBranch(c.Request().Context(), organizationID, projectID, branchID)
		if err != nil {
			return err
		}
		setRegionReqAttribute(c, branch.Region)

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
		connString, err := s.branches.ConnectionString(c.Request().Context(), organizationID, branch)
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			log.Ctx(c.Request().Context()).
				Info().
				Str("branchID", branch.ID).
				Str("grpc.message", st.Message()).
				Msg("connection string not found")
		}

		// get instance type from resources
		instanceType, err := s.branches.InstanceTypeByResources(c.Request().Context(), organizationID, branch.Region, cluster.Configuration.VcpuRequest, cluster.Configuration.VcpuLimit, cluster.Configuration.Memory)
		if err != nil {
			return fmt.Errorf("converting resources to instance type: %w", err)
		}

		headers := clienthttpheaders.FromContext(c.Request().Context())
		if headers != nil && headers.XataAgent.Service == "cli" {
			s.analytics.Track(c.Request().Context(), events.NewBranchDescribedEvent(string(organizationID), projectID, branchID))
		}
		return c.JSON(http.StatusOK, storeToAPIBranchMetadata(branch, connString, instanceType, cluster))
	})
}

// branchDatabaseName is the database managed users connect to on every branch.
const branchDatabaseName = "xata"

// Get branch credentials
// (GET /organizations/{organizationID}/projects/{projectID}/branches/{branchID}/credentials)
func (s *handler) GetBranchCredentials(c echo.Context, organizationID spec.OrganizationID, projectID, branchID string, params spec.GetBranchCredentialsParams) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		if params.Username != nil && *params.Username != "xata" {
			return ErrorInvalidParam{BranchName: branchID, Param: "username", Message: "only the xata user credentials can be retrieved"}
		}

		branch, err := s.store.DescribeBranch(c.Request().Context(), organizationID, projectID, branchID)
		if err != nil {
			if errors.As(err, &store.ErrBranchNotFound{}) {
				return ErrorBranchNotFound{BranchID: branchID}
			}
			return err
		}
		setRegionReqAttribute(c, branch.Region)

		client, err := s.cells.GetCellConnection(c.Request().Context(), organizationID, branch.CellID)
		if err != nil {
			return err
		}
		defer client.Close()

		creds, err := client.GetPostgresClusterCredentials(c.Request().Context(), &clustersv1.GetPostgresClusterCredentialsRequest{Id: branch.ID, Username: "app"})
		if err != nil {
			if errors.Is(err, clusters.SecretNotFoundForIDError(branch.ID)) {
				return ErrorCredentialsForBranchNotFound{BranchID: branch.ID, Username: "xata"}
			}
			return err
		}

		region, err := s.store.GetRegion(c.Request().Context(), organizationID, branch.Region)
		if err != nil {
			return err
		}

		cell, err := s.store.GetCell(c.Request().Context(), organizationID, branch.CellID)
		if err != nil {
			return err
		}

		hostname, port, err := s.branches.BranchEndpoint(region, cell.Subdomain, branch.ID)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, spec.BranchCredentials{
			Username:         creds.GetUsername(),
			Password:         creds.GetPassword(),
			Hostname:         hostname,
			Port:             port,
			Dbname:           branchDatabaseName,
			ConnectionString: branchsvc.FormatConnectionString(creds.GetUsername(), creds.GetPassword(), hostname, port),
		})
	})
}

// Rotate branch credentials
// (POST /organizations/{organizationID}/projects/{projectID}/branches/{branchID}/credentials/rotate)
func (s *handler) RotateBranchCredentials(c echo.Context, organizationID spec.OrganizationID, projectID, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, OnlyEnabled, func() error {
		var body spec.RotateBranchCredentialsJSONRequestBody
		if err := api.ReadBody(c, &body); err != nil {
			return err
		}

		if body.Username != "xata" {
			return ErrorInvalidParam{BranchName: branchID, Param: "username", Message: "only the xata user credentials can be rotated"}
		}

		branch, err := s.store.DescribeBranch(c.Request().Context(), organizationID, projectID, branchID)
		if err != nil {
			if errors.As(err, &store.ErrBranchNotFound{}) {
				return ErrorBranchNotFound{BranchID: branchID}
			}
			return err
		}
		setRegionReqAttribute(c, branch.Region)

		client, err := s.cells.GetCellConnection(c.Request().Context(), organizationID, branch.CellID)
		if err != nil {
			return err
		}
		defer client.Close()

		_, err = client.RotatePostgresClusterCredentials(c.Request().Context(), &clustersv1.RotatePostgresClusterCredentialsRequest{
			Id:   branch.ID,
			User: body.Username,
		})
		if err != nil {
			return err
		}

		return c.NoContent(http.StatusNoContent)
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

		claims := api.GetUserClaims(c)
		orgLimits, err := provisioner.ResolveOrgLimits(c.Request().Context(), s.store, claims.Organizations[organizationID].UsageTier, organizationID, projectID, provisioner.FlagsConfig{
			UseXatastor: s.feat.BoolValue(c.Request().Context(), flags.UseXatastor),
		})
		if err != nil {
			return err
		}

		if err := IsBranchDescriptionValid(body.Description, orgLimits.MaxDescriptionLength); err != nil {
			return err
		}

		if body.Replicas != nil {
			if err := branchsvc.ValidateReplicaCount(branchID, *body.Replicas, orgLimits.MinInstancesPerBranch, orgLimits.MaxInstancesPerBranch); err != nil {
				return apiErrorFromBranch(err)
			}
		}

		if body.Storage != nil {
			if err := validateStorageSize(branchID, *body.Storage, orgLimits.MaxStorageGBPerBranch); err != nil {
				return err
			}
		}

		// CNPG rejects simultaneous image and configuration changes when
		// primaryUpdateMethod is set to "switchover". Instance type changes are
		// included because they auto-adjust postgres parameters. Preload libraries
		// are included because they are passed as postgres configuration parameters.
		if body.Image != nil && (body.PostgresConfigurationParameters != nil || body.InstanceType != nil || body.PreloadLibraries != nil) {
			return ErrorInvalidParam{BranchName: branchID, Param: "image", Message: "image cannot be updated together with postgres configuration parameters, instance type, or preload libraries"}
		}

		if err := validateBackupConfiguration(branchID, body.BackupConfiguration); err != nil {
			return err
		}

		branch, err := s.store.UpdateBranch(c.Request().Context(), organizationID, projectID, branchID, apiToStoreUpdateBranchConfig(body), func(branch *store.Branch) error {
			setRegionReqAttribute(c, branch.Region)
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
			if body.InstanceType != nil && *body.InstanceType != branchsvc.FallbackInstanceType {
				it, err := s.branches.InstanceTypeByName(c.Request().Context(), organizationID, branch.Region, *body.InstanceType, orgLimits.MaxAllowedInstanceType)
				if err != nil {
					return ErrorInvalidParam{BranchName: branchID, Param: "configuration", Message: fmt.Sprintf("branch [%s]: %s", branchID, err.Error())}
				}
				config.VcpuRequest = new(it.CPURequest())
				config.VcpuLimit = new(it.CPULimit())
				config.Memory = new(it.Memory())

				// storage QoS class is optional, so only set it if it's not empty
				if it.StorageQoSClass != "" {
					config.StorageQosClass = new(it.StorageQoSClass)
				}
			}

			client, err := s.cells.GetCellConnection(c.Request().Context(), organizationID, branch.CellID)
			if err != nil {
				return err
			}
			defer client.Close()

			// Fetch cluster info once if needed for any of the operations
			var cluster *clustersv1.DescribePostgresClusterResponse
			needsClusterInfo := (body.InstanceType != nil && *body.InstanceType != branchsvc.FallbackInstanceType) ||
				body.PostgresConfigurationParameters != nil ||
				body.PreloadLibraries != nil ||
				body.Image != nil ||
				body.Storage != nil
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

			// Volumes can only grow: the Branch CR rejects a decrease, so catch it
			// here to report a 400 instead of an opaque failure from the operator.
			if body.Storage != nil && *body.Storage < cluster.Configuration.StorageSize {
				return ErrorInvalidParam{BranchName: branchID, Param: "storage", Message: fmt.Sprintf("storage size cannot be decreased (current: %d GB)", cluster.Configuration.StorageSize)}
			}

			// Validate preload libraries against extensions available for this image
			if body.PreloadLibraries != nil {
				if err := s.postgresConfigProvider.ValidatePreloadLibraries(cluster.Configuration.ImageName, *body.PreloadLibraries); err != nil {
					return ErrorInvalidParam{BranchName: branchID, Param: "preloadLibraries", Message: fmt.Sprintf("branch [%s]: %v", branchID, err)}
				}
			}

			// If instance type is changing, we need to update default settings
			if body.InstanceType != nil && *body.InstanceType != branchsvc.FallbackInstanceType {
				oldInstanceType, err := s.branches.InstanceTypeByResources(c.Request().Context(), organizationID, branch.Region, cluster.Configuration.VcpuRequest, cluster.Configuration.VcpuLimit, cluster.Configuration.Memory)
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
				instanceType := branchsvc.FallbackInstanceType
				if body.InstanceType != nil {
					body.InstanceType = &instanceType
				} else {
					// otherwise, find the instance type from the cluster (already fetched above)
					var err error
					instanceType, err = s.branches.InstanceTypeByResources(c.Request().Context(), organizationID, branch.Region, cluster.Configuration.VcpuRequest, cluster.Configuration.VcpuLimit, cluster.Configuration.Memory)
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
					backupConfig.BackupSchedule = branchsvc.GenerateCron(*body.BackupConfiguration.BackupTime)
				}
				config.BackupConfiguration = backupConfig
			}

			// Handle image minor version upgrades
			if body.Image != nil {
				// It can be argued that this should be decided by the operator. However - more than what
				// the operator supports - there should be a way for us to allow/disallow certain
				// upgrades - there can be business reasons for this and so it needs to happen here.
				imageURL, err := s.branches.ValidateImageUpgrade(c.Request().Context(), organizationID, *body.Image, cluster.Configuration.ImageName)
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
			if st.Code() == codes.Aborted {
				return ErrorBranchConflict{BranchID: branchID}
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
		connString, err := s.branches.ConnectionString(c.Request().Context(), organizationID, branch)
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			log.Ctx(c.Request().Context()).
				Err(err).
				Str("branchID", branch.ID).
				Str("grpc.message", st.Message()).
				Msg("connection string not found")
		}
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
		err := s.provisioner.DeleteBranch(c.Request().Context(), organizationID, projectID, branchID)
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

// GetProjectLimits returns the effective resource limits for a project, applying
// tier defaults, per-organization overrides and per-project overrides, in
// increasing order of precedence.
// (GET /organizations/{organizationID}/projects/{projectID}/limits)
func (s *handler) GetProjectLimits(c echo.Context, organizationID spec.OrganizationID, projectID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		return s.withProject(c, organizationID, projectID, func(_ *store.Project) error {
			ctx := c.Request().Context()
			claims := api.GetUserClaims(c)
			limits, err := provisioner.ResolveOrgLimits(ctx, s.store, claims.Organizations[organizationID].UsageTier, organizationID, projectID, provisioner.FlagsConfig{
				UseXatastor: s.feat.BoolValue(ctx, flags.UseXatastor),
			})
			if err != nil {
				return err
			}
			return c.JSON(http.StatusOK, spec.EffectiveProjectLimits{
				MaxDescriptionLength:   limits.MaxDescriptionLength,
				MaxBranchesPerProject:  limits.MaxBranchesPerProject,
				MaxInstancesPerBranch:  limits.MaxInstancesPerBranch,
				MinInstancesPerBranch:  limits.MinInstancesPerBranch,
				MaxAllowedInstanceType: limits.MaxAllowedInstanceType,
				MaxBranchesPerHour:     limits.MaxBranchesPerHour,
				MaxStorageGBPerBranch:  limits.MaxStorageGBPerBranch,
			})
		})
	})
}

// GetOrganizationLimits returns the effective limits for an organization, applying
// tier defaults and any per-organization overrides stored in the DB.
// T1 organizations always receive tier defaults with no DB lookup.
// (GET /organizations/{organizationID}/limits)
func (s *handler) GetOrganizationLimits(c echo.Context, organizationID spec.OrganizationID) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		claims := api.GetUserClaims(c)
		limits, err := provisioner.ResolveOrgLimits(c.Request().Context(), s.store, claims.Organizations[organizationID].UsageTier, organizationID, "", provisioner.FlagsConfig{
			UseXatastor: s.feat.BoolValue(c.Request().Context(), flags.UseXatastor),
		})
		if err != nil {
			return err
		}
		return c.JSON(http.StatusOK, orgLimitsToSpec(limits))
	})
}

func orgLimitsToSpec(l provisioner.OrgLimits) spec.OrganizationLimits {
	return spec.OrganizationLimits{
		MaxProjects:            l.MaxProjects,
		MaxProjectsPerHour:     l.MaxProjectsPerHour,
		MaxBranchesPerProject:  l.MaxBranchesPerProject,
		MaxBranchesPerOrg:      l.MaxBranchesPerOrg,
		MaxBranchesPerHour:     l.MaxBranchesPerHour,
		MaxInstancesPerBranch:  l.MaxInstancesPerBranch,
		MinInstancesPerBranch:  l.MinInstancesPerBranch,
		MaxDescriptionLength:   l.MaxDescriptionLength,
		MaxAllowedInstanceType: l.MaxAllowedInstanceType,
		MaxStorageGBPerBranch:  l.MaxStorageGBPerBranch,
	}
}

// BranchMetrics retrieves the branch metrics
// (POST /organizations/{organizationID}/projects/{projectID}/branches/{branchID}/metrics)
func (s *handler) BranchMetrics(c echo.Context, organizationID spec.OrganizationID, projectID string, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		var req spec.BranchMetricsRequest
		if err := c.Bind(&req); err != nil {
			return err
		}

		if len(req.Metrics) == 0 {
			return ErrorInvalidParam{BranchName: branchID, Param: "metrics", Message: "`metrics` must not be empty"}
		}
		metricNames := stringArrayValue(req.Metrics)

		if len(metricNames) > maxMetricsPerRequest {
			return ErrorInvalidParam{BranchName: branchID, Param: "metrics", Message: fmt.Sprintf("at most %d metrics may be requested", maxMetricsPerRequest)}
		}

		if err := validateTimeRange(branchID, req.Start, req.End); err != nil {
			return err
		}

		branch, err := s.store.DescribeBranch(c.Request().Context(), organizationID, projectID, branchID)
		if err != nil {
			return err
		}
		setRegionReqAttribute(c, branch.Region)

		instances := ptr.Deref(req.Instances, nil)
		if err := s.validateBranchInstances(c.Request().Context(), organizationID, branch, instances); err != nil {
			return err
		}

		results, err := s.metricsClient.GetMetrics(c.Request().Context(), organizationID, branch.CellID, req.Start, req.End, branchID, metricNames, instances, stringArrayValue(req.Aggregations))
		if err != nil {
			return fmt.Errorf("get metrics for branch [%s]: %w", branchID, err)
		}
		if len(results) == 0 {
			return fmt.Errorf("get metrics for branch [%s]: backend returned no results", branchID)
		}

		specResults := make([]spec.BranchMetricResult, len(results))
		for i, r := range results {
			specResults[i] = spec.BranchMetricResult{
				Metric: r.Metric,
				Unit:   r.Unit,
				Series: toSpecSeries(r.Series),
			}
		}

		return c.JSON(http.StatusOK, spec.BranchMetrics{
			Start:   req.Start,
			End:     req.End,
			Results: specResults,
		})
	})
}

// BranchLogs retrieves the branch logs
// (POST /organizations/{organizationID}/projects/{projectID}/branches/{branchID}/logs)
func (s *handler) BranchLogs(c echo.Context, organizationID spec.OrganizationID, projectID string, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		var req spec.BranchLogsRequest
		if err := c.Bind(&req); err != nil {
			return err
		}

		if err := validateTimeRange(branchID, req.Start, req.End); err != nil {
			return err
		}
		if err := validateLogsLimit(branchID, req.Limit); err != nil {
			return err
		}

		userFilters, err := validateLogFilters(branchID, ptr.Deref(req.Filters, nil))
		if err != nil {
			return err
		}

		branch, err := s.store.DescribeBranch(c.Request().Context(), organizationID, projectID, branchID)
		if err != nil {
			return err
		}
		setRegionReqAttribute(c, branch.Region)

		logs, err := s.metricsClient.GetLogs(
			c.Request().Context(),
			organizationID,
			branch.CellID,
			req.Start,
			req.End,
			branchID,
			userFilters,
			ptr.Deref(req.Limit, DefaultLogLimit),
			ptr.Deref(req.Cursor, ""),
		)
		if err != nil {
			return fmt.Errorf("getting logs for branch [%s]: %w", branchID, err)
		}

		return c.JSON(http.StatusOK, logs)
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

		ctx := c.Request().Context()

		claims := api.GetUserClaims(c)
		marketplace := claims.Organizations[organizationID].Marketplace
		useXatastor := s.feat.BoolValue(ctx, flags.UseXatastor)
		orgLimits, err := provisioner.ResolveOrgLimits(ctx, s.store, claims.Organizations[organizationID].UsageTier, organizationID, projectID, provisioner.FlagsConfig{
			UseXatastor: useXatastor,
		})
		if err != nil {
			return err
		}

		var body spec.RestoreFromBackupJSONRequestBody
		if err := api.ReadBody(c, &body); err != nil {
			return err
		}

		if err := IsBranchDescriptionValid(body.Description, orgLimits.MaxDescriptionLength); err != nil {
			return err
		}

		if err := validateBranchRequestCommons(body); err != nil {
			return err
		}

		var createClusterPayload ClusterServicePayload
		usePgBackRest := s.feat.BoolValue(ctx, flags.UsePgBackRest)
		log.Ctx(ctx).Info().Bool("usePgBackRest", usePgBackRest).Msg("pgbackrest feature flag")

		// restore must happen in the same cell where the backup exists
		// otherwise we don't have access to the source object store
		sourceBranch, err := s.store.DescribeBranch(ctx, organizationID, projectID, branchID)
		if err != nil {
			if errors.As(err, &store.ErrBranchNotFound{}) {
				return ErrorBranchNotFound{BranchID: branchID}
			}
			return err
		}
		setRegionReqAttribute(c, sourceBranch.Region)
		ctx = c.Request().Context()

		if body.Configuration == nil {
			// Inherit configuration from source branch
			if s.feat.BoolValue(ctx, flags.ChildBranchCreationDisabled) {
				return ErrorChildBranchCreationDisabled{}
			}
			createClusterPayload, err = s.branches.PayloadFromParent(ctx, organizationID, projectID, body.Name, branchID)
			if err != nil {
				return apiErrorFromBranch(err)
			}
		} else {
			// Use source branch's region if not specified, otherwise validate it matches
			if body.Configuration.Region == "" {
				body.Configuration.Region = sourceBranch.Region
			} else if body.Configuration.Region != sourceBranch.Region {
				return ErrorInvalidParam{BranchName: body.Name, Param: "region", Message: "restore must be in the same region as the source branch"}
			}
			// Use provided configuration
			createClusterPayload, err = s.branches.PayloadFromConfiguration(ctx, organizationID, projectID, body.Name, toBranchConfiguration(*body.Configuration), orgLimits, marketplace)
			if err != nil {
				return apiErrorFromBranch(err)
			}
			// Use source branch's cell - the backup only exists there
			createClusterPayload.CellID = sourceBranch.CellID
			// Mark source branch as parent - this will always be the case
			createClusterPayload.ParentID = &branchID
		}

		if body.Configuration == nil {
			if err := s.branches.ValidateMarketplaceRegion(ctx, branchsvc.MarketplaceRegionParams{
				OrganizationID: organizationID,
				Marketplace:    marketplace,
				Region:         createClusterPayload.Region,
			}); err != nil {
				return ErrorInvalidParam{BranchName: body.Name, Param: "region", Message: err.Error()}
			}
		}

		if !createClusterPayload.BackupsEnabled && body.BackupConfiguration != nil {
			return ErrorInvalidParam{BranchName: body.Name, Param: "backupConfiguration", Message: "backup configuration cannot be specified when backups are disabled in the selected region"}
		}

		if err := s.checkRestoreTarget(ctx, organizationID, sourceBranch, body.Name, body.TargetTime); err != nil {
			return err
		}

		return s.withProject(c, organizationID, projectID, func(project *store.Project) error {
			// Connect to the cell before the create transaction: the lookup uses
			// the store, and provisionFn must not take a second pooled connection
			// while the transaction holds one.
			client, err := s.cells.GetCellConnection(ctx, organizationID, createClusterPayload.CellID)
			if err != nil {
				return err
			}
			defer client.Close()

			branch, err := s.store.CreateBranch(ctx, organizationID, projectID, createClusterPayload.CellID, &store.CreateBranchConfiguration{
				Name:                  body.Name,
				ParentID:              createClusterPayload.ParentID,
				Description:           body.Description,
				BackupRetentionPeriod: apiToStoreBackupConfig(body.BackupConfiguration),
				BackupsEnabled:        createClusterPayload.BackupsEnabled,
				UsageTier:             claims.Organizations[organizationID].UsageTier,
				Limits:                orgLimits.StoreLimits(),
			}, func(branch *store.Branch, lockedProject *store.Project) error {
				scaleToZero := apiToClustersScaleToZero(body.ScaleToZero, createClusterPayload.ParentID, project)
				createClusterPayload.Configuration.ScaleToZero = scaleToZero
				request := clustersv1.CreatePostgresClusterRequest{
					Id:             branch.ID,
					IdempotencyKey: idgen.Generate(),
					OrganizationId: organizationID,
					ProjectId:      projectID,
					ParentId:       branch.ParentID,
					Configuration:  &createClusterPayload.Configuration,
					DataSource: &clustersv1.CreatePostgresClusterRequest_ContinuousBackup{
						ContinuousBackup: continuousBackupSource(branchID, body.TargetTime),
					},
					BackupConfiguration: apiToClustersBackupConfig(body.BackupConfiguration, createClusterPayload.BackupsEnabled, usePgBackRest),
				}

				if _, err := client.CreatePostgresCluster(ctx, &request); err != nil {
					return err
				}

				// Use the project read under the lock: an IP filtering update may
				// have committed since withProject read it.
				return cells.ApplyProjectIPFiltering(ctx, client, branch.ID, lockedProject)
			})
			if err != nil {
				if _, ok := errors.AsType[store.ErrProjectBusy](err); ok {
					c.Response().Header().Set("Retry-After", projectLockRetryAfter)
					return err
				}
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
			connString, _ := s.branches.ConnectionString(c.Request().Context(), organizationID, branch)
			return c.JSON(http.StatusCreated, storeToAPIBranchShortMetadata(branch, connString))
		})
	})
}

// checkRestoreTarget checks that the source branch can be restored to target.
// It returns ErrorNoRecoveryPoint if the source branch has no recovery point,
// and ErrorInvalidParam if target is outside its recovery window. A nil target
// is the latest point.
func (s *handler) checkRestoreTarget(ctx context.Context, organizationID spec.OrganizationID, source *store.Branch, name string, target *time.Time) error {
	client, err := s.cells.GetCellConnection(ctx, organizationID, source.CellID)
	if err != nil {
		return err
	}
	defer client.Close()

	window, err := client.GetRecoveryWindow(ctx, &clustersv1.GetRecoveryWindowRequest{Id: source.ID})
	if err != nil {
		// NotFound means the cell has no recovery window for the branch.
		if status.Code(err) == codes.NotFound {
			return ErrorNoRecoveryPoint{BranchID: source.ID}
		}
		return err
	}
	earliest, latest, err := parseRecoveryWindow(window)
	if err != nil {
		return err
	}
	if earliest == nil {
		return ErrorNoRecoveryPoint{BranchID: source.ID}
	}
	if target == nil {
		return nil
	}

	// The window has no latest point if the cell reports neither a recovery
	// point nor a backup time. Accept any time up to now in that case.
	upper := time.Now()
	if latest != nil {
		upper = *latest
	}
	if target.Before(*earliest) || target.After(upper) {
		return ErrorInvalidParam{
			BranchName: name,
			Param:      "targetTime",
			Message: fmt.Sprintf("must be between %s and %s, the recovery window of branch %s",
				earliest.UTC().Format(time.RFC3339), upper.UTC().Format(time.RFC3339), source.ID),
		}
	}
	return nil
}

// continuousBackupSource returns the restore data source for the source
// branch. A nil target restores to the end of the archived WAL.
func continuousBackupSource(sourceBranchID string, target *time.Time) *clustersv1.ContinuousBackup {
	cb := &clustersv1.ContinuousBackup{ClusterId: sourceBranchID}
	if target != nil {
		cb.Timestamp = timestamppb.New(*target)
	}
	return cb
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

// toSpecSeries: spec.MetricSeries.Values is an inline anonymous struct, so this can't be a cast.
func toSpecSeries(in []metrics.MetricSeries) []spec.MetricSeries {
	out := make([]spec.MetricSeries, len(in))
	for i, s := range in {
		values := make([]struct {
			Timestamp time.Time `json:"timestamp"`
			Value     float32   `json:"value"`
		}, len(s.Values))
		for j, v := range s.Values {
			values[j].Timestamp = v.Timestamp
			values[j].Value = v.Value
		}
		out[i] = spec.MetricSeries{
			Aggregation: spec.MetricSeriesAggregation(s.Aggregation),
			InstanceID:  s.InstanceID,
			Values:      values,
		}
	}
	return out
}

func validateTimeRange(branchID string, start, end time.Time) error {
	if end.Before(start) {
		return ErrorInvalidParam{BranchName: branchID, Param: "start", Message: "start time must come before end time"}
	}

	if end.Sub(start) > maxDateRange {
		return ErrorInvalidParam{BranchName: branchID, Param: "end", Message: "maximum date range is " + maxDateRange.String()}
	}

	return nil
}

// validateBranchInstances rejects instance names that don't belong to the
// branch's cluster. Pool clusters carry pod names that don't share the
// branchID prefix, so the check is against the actual cluster status keys
// rather than a string prefix.
func (s *handler) validateBranchInstances(ctx context.Context, organizationID spec.OrganizationID, branch *store.Branch, instances []string) error {
	if len(instances) == 0 {
		return nil
	}

	client, err := s.cells.GetCellConnection(ctx, organizationID, branch.CellID)
	if err != nil {
		return err
	}
	defer client.Close()

	cluster, err := client.DescribePostgresCluster(ctx, &clustersv1.DescribePostgresClusterRequest{Id: branch.ID})
	if err != nil {
		st, _ := status.FromError(err)
		if st.Code() == codes.NotFound {
			return ErrorBranchNotFound{BranchID: branch.ID}
		}
		return err
	}

	valid := cluster.GetStatus().GetInstances()
	for _, inst := range instances {
		if _, ok := valid[inst]; !ok {
			return ErrorInvalidParam{BranchName: branch.ID, Param: "instances", Message: fmt.Sprintf("unknown instance [%s]", inst)}
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

func setRegionReqAttribute(c echo.Context, region string) {
	if region != "" {
		o11y.SetReqAttribute(c, "region", region)
	}
}

// GetBranchPostgresConfig retrieves detailed information about PostgreSQL configuration parameters for a branch
// (GET /organizations/{organizationID}/projects/{projectID}/branches/{branchID}/postgres-config)
func (s *handler) GetBranchPostgresConfig(c echo.Context, organizationID spec.OrganizationID, projectID string, branchID string) error {
	return s.withOrganizationAccess(c, organizationID, All, func() error {
		branch, err := s.store.DescribeBranch(c.Request().Context(), organizationID, projectID, branchID)
		if err != nil {
			return err
		}
		setRegionReqAttribute(c, branch.Region)

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

		instanceType, err := s.branches.InstanceTypeByResources(c.Request().Context(), organizationID, branch.Region, cluster.Configuration.VcpuRequest, cluster.Configuration.VcpuLimit, cluster.Configuration.Memory)
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
		if err := s.validateGithubInstallationAccess(c, body.InstallationId); err != nil {
			return err
		}

		inst, err := s.store.CreateGithubInstallation(c.Request().Context(), organizationID, body.InstallationId)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusCreated, storeToAPIGithubInstallation(inst))
	})
}

func (s *handler) validateGithubInstallationAccess(c echo.Context, installationID int64) error {
	if installationID <= 0 {
		return ErrorInvalidParam{Param: "installationId", Message: "must be greater than 0"}
	}
	if s.githubInstallationValidator == nil {
		return ErrorGithubInstallationValidationUnavailable{Err: errors.New("github installation validator is not configured")}
	}

	claims := api.GetUserClaims(c)
	if claims == nil {
		return echo.NewHTTPError(http.StatusUnauthorized)
	}
	// API keys have no Keycloak session and therefore no connected GitHub account.
	if claims.APIKeyID() != "" {
		return ErrorGithubUserSessionRequired{}
	}

	sessionToken, err := api.BearerTokenFromHeader(c)
	if err != nil {
		return echo.NewHTTPError(http.StatusUnauthorized)
	}

	return s.githubInstallationValidator.ValidateUserInstallationAccess(c.Request().Context(), sessionToken, installationID)
}

func (s *handler) validateGithubRepositoryAccess(c echo.Context, repositoryID int64) error {
	if s.githubInstallationValidator == nil {
		return ErrorGithubRepositoryValidationUnavailable{Err: errors.New("github repository validator is not configured")}
	}

	claims := api.GetUserClaims(c)
	if claims == nil {
		return echo.NewHTTPError(http.StatusUnauthorized)
	}

	organizationIDs := make([]string, 0, len(claims.Organizations))
	for organizationID := range claims.Organizations {
		organizationIDs = append(organizationIDs, organizationID)
	}
	slices.Sort(organizationIDs)

	installationIDSet := map[int64]struct{}{}
	for _, organizationID := range organizationIDs {
		installations, err := s.store.ListGithubInstallations(c.Request().Context(), organizationID)
		if err != nil {
			return err
		}
		for _, installation := range installations {
			installationIDSet[installation.InstallationID] = struct{}{}
		}
	}

	installationIDs := make([]int64, 0, len(installationIDSet))
	for installationID := range installationIDSet {
		installationIDs = append(installationIDs, installationID)
	}
	slices.Sort(installationIDs)
	if len(installationIDs) == 0 {
		return ErrorInvalidParam{Param: "githubRepositoryID", Message: "github repository is not accessible"}
	}

	return s.githubInstallationValidator.ValidateRepositoryAccess(c.Request().Context(), repositoryID, installationIDs)
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
		if err := s.validateGithubInstallationAccess(c, body.InstallationId); err != nil {
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

		// Authorize the key against the branch the mapping actually belongs to,
		// not just the organization or the branchID in the path: the mapping is
		// project-wide and tied to its root branch, so a branch-scoped key must
		// not read integration metadata for a branch it cannot access. Return
		// not-found rather than forbidden to avoid leaking the mapping's
		// existence.
		if !api.GetUserClaims(c).HasAccessToBranch(mapping.RootBranchID) {
			return store.ErrGithubRepoMappingNotFound{Organization: organizationID, Project: projectID}
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
		if err := s.validateGithubRepositoryAccess(c, body.GithubRepositoryID); err != nil {
			return err
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
		if err := s.validateGithubRepositoryAccess(c, body.GithubRepositoryID); err != nil {
			return err
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
		// Resolve the mapping first so we can authorize the key against the
		// branch it belongs to. Deleting the project-wide mapping is
		// disruptive, so a branch-scoped key must not delete a mapping for a
		// branch it cannot access. Return not-found rather than forbidden to
		// avoid leaking the mapping's existence.
		mapping, err := s.store.GetGithubRepoMappingByProject(c.Request().Context(), organizationID, projectID)
		if err != nil {
			return err
		}
		if !api.GetUserClaims(c).HasAccessToBranch(mapping.RootBranchID) {
			return store.ErrGithubRepoMappingNotFound{Organization: organizationID, Project: projectID}
		}

		if err := s.store.DeleteGithubRepoMapping(c.Request().Context(), organizationID, projectID); err != nil {
			return err
		}

		return c.NoContent(http.StatusNoContent)
	})
}
