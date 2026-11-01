// Package branch holds the branch business logic of the projects service:
// building provisioner payloads, resolving images/instance types/regions,
// provisioning branches, and computing connection strings. It is transport
// agnostic — it takes neutral domain inputs and returns neutral domain errors —
// so the REST handler and other callers (e.g. Vercel resource provisioning)
// share one implementation.
package branch

import (
	"net/http"

	"xata/internal/openfeature"
	"xata/internal/postgrescfg"
	"xata/internal/postgresversions"
	"xata/services/projects/cells"
	"xata/services/projects/provisioner"
	"xata/services/projects/scheduler"
	"xata/services/projects/store"
)

const (
	// FallbackInstanceType is returned by InstanceTypeByResources for vcpu/memory
	// combinations that do not match a named instance type. It is exported so the
	// api package shares this constant instead of keeping its own copy.
	FallbackInstanceType = "custom"

	// DefaultBackupRetentionPeriod, BackupMethodBarman, and BackupMethodPgBackRest
	// are exported so the api backup adapters delegate here instead of keeping
	// their own copies.
	DefaultBackupRetentionPeriod = 2 // days
	BackupMethodBarman           = "barman"
	BackupMethodPgBackRest       = "pgbackrest"

	// branchDatabaseName is the database managed users connect to on every branch.
	branchDatabaseName = "xata"
	// defaultPostgresPort is omitted from connection strings, clients assume it.
	defaultPostgresPort = 5432
	// deprecatedHostSuffix marks hostnames served in the deprecated branch
	// connectionString field. The gateway strips it and logs the connection,
	// so clients still using that field can be detected.
	deprecatedHostSuffix = "-deprecated"

	// validImage is a legacy image identifier the UI still sends; kept for
	// backward compatibility until the UI stops using it.
	validImage = "postgresql:17"
)

// Service carries the dependencies the branch business logic needs. It mirrors
// the subset of the API handler's dependencies used by branch operations.
type Service struct {
	store                  store.ProjectsStore
	cells                  cells.Cells
	feat                   openfeature.Client
	sched                  *scheduler.Scheduler
	defaultGatewayHostPort string
	postgresConfigProvider postgrescfg.PostgresConfigProvider
	imageProvider          postgresversions.ImageProvider
	provisioner            provisioner.Provisioner
}

// New builds a branch Service.
func New(
	store store.ProjectsStore,
	cells cells.Cells,
	feat openfeature.Client,
	sched *scheduler.Scheduler,
	defaultGatewayHostPort string,
	postgresConfigProvider postgrescfg.PostgresConfigProvider,
	imageProvider postgresversions.ImageProvider,
	prov provisioner.Provisioner,
) *Service {
	return &Service{
		store:                  store,
		cells:                  cells,
		feat:                   feat,
		sched:                  sched,
		defaultGatewayHostPort: defaultGatewayHostPort,
		postgresConfigProvider: postgresConfigProvider,
		imageProvider:          imageProvider,
		provisioner:            prov,
	}
}

// CreateInput is the neutral, transport-independent request for CreateBranch.
// Mode selects the creation mode (FromParent or FromConfiguration).
type CreateInput struct {
	OrganizationID string
	ProjectID      string
	Name           string
	Mode           Mode
	Marketplace    string
	UsageTier      string
	OrgLimits      provisioner.OrgLimits
	Flags          provisioner.FlagsConfig
	UsePgBackRest  bool
	Description    *string
	Backup         *BackupConfiguration
	ScaleToZero    *ScaleToZero
	// ProjectLockHeld tells Provision the caller already holds the project lock
	// (shared or exclusive), so it must not acquire it again: a second acquire on
	// another session can deadlock behind a waiting exclusive holder. The GitHub
	// webhook holds the shared lock across its whole PR-opened handler, so it
	// waits out an IP filtering update instead of failing on contention.
	ProjectLockHeld bool
}

// Mode is the sealed set of branch creation modes: FromParent or
// FromConfiguration.
type Mode interface {
	isMode()
}

// FromParent creates a child branch that inherits the parent's configuration
// and data.
type FromParent struct {
	ParentID string
}

func (FromParent) isMode() {}

// FromConfiguration creates a root branch from an explicit configuration.
type FromConfiguration struct {
	Config Configuration
}

func (FromConfiguration) isMode() {}

// Configuration mirrors the cluster configuration fields the branch logic needs.
type Configuration struct {
	Image                           string
	Region                          string
	InstanceType                    string
	Replicas                        int32
	PreloadLibraries                *[]string
	PostgresConfigurationParameters *map[string]string
	// Storage is the requested branch storage in GB; when set it is validated
	// against the org limit and applied to the cluster configuration.
	Storage *int32
}

// BackupConfiguration mirrors the backup fields the branch logic needs.
type BackupConfiguration struct {
	BackupTime      *string
	RetentionPeriod *int32
}

// ScaleToZero mirrors the scale-to-zero fields the branch logic needs.
type ScaleToZero struct {
	Enabled                 bool
	InactivityPeriodMinutes int
}

// ErrInvalidParam reports an invalid request parameter. Its Error() string
// preserves the "Project [...]: " / "Branch [...]: " prefixes the frontend
// parses (see cleanPostgresError in the webapp).
type ErrInvalidParam struct {
	ProjectID  string
	BranchName string
	Param      string
	Message    string
}

func (e ErrInvalidParam) Error() string {
	errMsg := ""
	if e.ProjectID != "" {
		errMsg += "Project [" + e.ProjectID + "]: "
	}
	if e.BranchName != "" {
		errMsg += "Branch [" + e.BranchName + "]: "
	}
	errMsg += "invalid parameter [" + e.Param + "]: " + e.Message
	return errMsg
}

func (e ErrInvalidParam) StatusCode() int { return http.StatusBadRequest }

// ErrBranchNotFound reports that a branch does not exist.
type ErrBranchNotFound struct {
	BranchID string
}

func (e ErrBranchNotFound) Error() string {
	return "Branch with ID [" + e.BranchID + "]: not found"
}

func (e ErrBranchNotFound) StatusCode() int { return http.StatusNotFound }

// ErrInvalidRegion reports an invalid or unavailable region.
type ErrInvalidRegion struct {
	Message string
}

func (e ErrInvalidRegion) Error() string { return e.Message }

func (e ErrInvalidRegion) StatusCode() int { return http.StatusBadRequest }

// ErrChildBranchCreationDisabled reports that child branch creation is disabled.
type ErrChildBranchCreationDisabled struct{}

func (e ErrChildBranchCreationDisabled) Error() string {
	return "Child branch creation is temporarily disabled"
}

func (e ErrChildBranchCreationDisabled) StatusCode() int { return http.StatusServiceUnavailable }

// ErrParentBranchUnhealthy reports that a child branch cannot be created because
// its parent is unhealthy.
type ErrParentBranchUnhealthy struct {
	ParentID string
}

func (e ErrParentBranchUnhealthy) Error() string {
	return "Cannot create child branch because parent branch with ID [" + e.ParentID + "] is not healthy"
}

func (e ErrParentBranchUnhealthy) StatusCode() int { return http.StatusPreconditionFailed }
