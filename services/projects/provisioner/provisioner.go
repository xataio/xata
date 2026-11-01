package provisioner

import (
	"context"
	"errors"
	"fmt"

	clustersv1 "xata/gen/proto/clusters/v1"
	"xata/internal/idgen"
	"xata/services/projects/cells"
	"xata/services/projects/store"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FlagsConfig struct {
	UsePool     bool
	UseXatastor bool
}

type OrgLimits struct {
	MaxProjects            int
	MaxProjectsPerHour     int
	MaxBranchesPerProject  int
	MaxBranchesPerOrg      int
	MaxBranchesPerHour     int
	MaxInstancesPerBranch  int
	MinInstancesPerBranch  int
	MaxDescriptionLength   int
	MaxAllowedInstanceType int
	MaxStorageGBPerBranch  int
}

// StoreLimits returns the subset of limits needed by store.CreateBranch.
func (l OrgLimits) StoreLimits() *store.OrgLimits {
	return &store.OrgLimits{
		MaxBranchesPerOrg:     l.MaxBranchesPerOrg,
		MaxBranchesPerProject: l.MaxBranchesPerProject,
		MaxBranchesPerHour:    l.MaxBranchesPerHour,
	}
}

// ResolveOrgLimits returns the effective limits for an organization by combining
// tier defaults, flag-based adjustments, and per-org DB overrides.
// T1 orgs skip the DB lookup.
func ResolveOrgLimits(ctx context.Context, s store.ProjectsStore, usageTier, organizationID, projectID string, flags FlagsConfig) (OrgLimits, error) {
	tier := parseTier(ctx, usageTier)
	def := func(key store.LimitKey) int { return store.TierDefaultInt(tier, key, 0) }

	var overrides map[store.LimitKey]any
	if tier != store.TierT1 {
		var err error
		overrides, err = s.GetOrgLimits(ctx, organizationID, projectID)
		if err != nil {
			return OrgLimits{}, fmt.Errorf("get org limits: %w", err)
		}
	}

	maxBranchesPerOrg := def(store.LimitMaxBranchesPerOrg)
	maxBranchesPerProject := def(store.LimitMaxBranchesPerProject)
	maxBranchesPerHour := def(store.LimitMaxBranchesPerHour)
	if flags.UseXatastor && tier != store.TierT1 {
		maxBranchesPerOrg = store.XatastorMaxBranchesPerOrg
		maxBranchesPerProject = store.XatastorMaxBranchesPerProject
		maxBranchesPerHour = store.XatastorMaxBranchesPerHour
	}

	resolve := func(key store.LimitKey, fallback int) int {
		return store.ResolveIntLimit(overrides, key, fallback)
	}

	return OrgLimits{
		MaxProjects:            resolve(store.LimitMaxProjects, def(store.LimitMaxProjects)),
		MaxProjectsPerHour:     resolve(store.LimitMaxProjectsPerHour, def(store.LimitMaxProjectsPerHour)),
		MaxBranchesPerProject:  resolve(store.LimitMaxBranchesPerProject, maxBranchesPerProject),
		MaxBranchesPerOrg:      resolve(store.LimitMaxBranchesPerOrg, maxBranchesPerOrg),
		MaxBranchesPerHour:     resolve(store.LimitMaxBranchesPerHour, maxBranchesPerHour),
		MaxInstancesPerBranch:  resolve(store.LimitMaxInstancesPerBranch, def(store.LimitMaxInstancesPerBranch)),
		MinInstancesPerBranch:  resolve(store.LimitMinInstancesPerBranch, def(store.LimitMinInstancesPerBranch)),
		MaxDescriptionLength:   resolve(store.LimitMaxDescriptionLength, def(store.LimitMaxDescriptionLength)),
		MaxAllowedInstanceType: resolve(store.LimitMaxAllowedInstanceType, def(store.LimitMaxAllowedInstanceType)),
		MaxStorageGBPerBranch:  resolve(store.LimitMaxStorageGBPerBranch, def(store.LimitMaxStorageGBPerBranch)),
	}, nil
}

func parseTier(ctx context.Context, usageTier string) store.UsageTier {
	tier, ok := store.ParseUsageTier(usageTier)
	if !ok {
		log.Ctx(ctx).Warn().Str("usage_tier", usageTier).Msg("unknown usage tier, defaulting to t1")
	}
	return tier
}

type ClusterServicePayload struct {
	ParentID              *string
	Configuration         clustersv1.ClusterConfiguration
	Description           *string
	CellID                string
	Region                string
	BackupsEnabled        bool
	BackupRetentionPeriod int
	BackupConfig          *clustersv1.BackupConfiguration
	Flags                 FlagsConfig
	UsageTier             string
	Limits                *store.OrgLimits
	// ProjectLockHeld tells the store the caller already holds the project lock,
	// so the create transaction must not take it again.
	ProjectLockHeld bool
}

//go:generate go run github.com/vektra/mockery/v2 --output mocks --outpkg mocks --with-expecter --name Provisioner
type Provisioner interface {
	CreateBranch(ctx context.Context, projectID, organizationID, name string, payload *ClusterServicePayload) (*store.Branch, error)
	DeleteBranch(ctx context.Context, organizationID, projectID, branchID string) error
	DeleteProject(ctx context.Context, organizationID, projectID string) (int, error)
}

type BranchProvisioner struct {
	store store.ProjectsStore
	cells cells.Cells
}

func NewBranchProvisioner(store store.ProjectsStore, cells cells.Cells) *BranchProvisioner {
	return &BranchProvisioner{
		store: store,
		cells: cells,
	}
}

func (p *BranchProvisioner) CreateBranch(ctx context.Context, projectID, organizationID, name string, payload *ClusterServicePayload) (*store.Branch, error) {
	project, err := p.store.GetProject(ctx, organizationID, projectID)
	if err != nil {
		return nil, err
	}

	// Default scale-to-zero from project settings when not explicitly provided
	if payload.Configuration.ScaleToZero == nil {
		s2z := project.ScaleToZero.BaseBranches
		if payload.ParentID != nil {
			s2z = project.ScaleToZero.ChildBranches
		}
		payload.Configuration.ScaleToZero = &clustersv1.ScaleToZero{
			Enabled:                 s2z.Enabled,
			InactivityPeriodMinutes: int64(s2z.InactivityPeriod.Duration().Minutes()),
		}
	}

	// Connect to the cell before the create transaction: the lookup uses the
	// store, and provisionFn must not take a second pooled connection while the
	// transaction holds one.
	client, err := p.cells.GetCellConnection(ctx, organizationID, payload.CellID)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	branch, err := p.store.CreateBranch(ctx, organizationID, projectID, payload.CellID, &store.CreateBranchConfiguration{
		Name:                  name,
		ParentID:              payload.ParentID,
		Description:           payload.Description,
		BackupRetentionPeriod: payload.BackupRetentionPeriod,
		BackupsEnabled:        payload.BackupsEnabled,
		UsageTier:             payload.UsageTier,
		Limits:                payload.Limits,
		ProjectLockHeld:       payload.ProjectLockHeld,
	}, func(branch *store.Branch, lockedProject *store.Project) error {
		request := clustersv1.CreatePostgresClusterRequest{
			Id:                  branch.ID,
			IdempotencyKey:      idgen.Generate(),
			ParentId:            branch.ParentID,
			OrganizationId:      organizationID,
			ProjectId:           projectID,
			Configuration:       &payload.Configuration,
			BackupConfiguration: payload.BackupConfig,
		}
		if branch.ParentID != nil {
			request.DataSource = &clustersv1.CreatePostgresClusterRequest_ClusterSnapshot{
				ClusterSnapshot: &clustersv1.ClusterSnapshot{
					ClusterId: *branch.ParentID,
				},
			}
		}

		if payload.Flags.UsePool {
			request.UsePool = new(true)
		}

		if branch.ParentID == nil {
			if payload.Flags.UseXatastor {
				request.UseXatastor = new(true)
			}
		}

		if _, err := client.CreatePostgresCluster(ctx, &request); err != nil {
			return err
		}

		// Use the project read under the lock, not the one read above: an IP
		// filtering update may have committed in between.
		return cells.ApplyProjectIPFiltering(ctx, client, branch.ID, lockedProject)
	})
	if err != nil {
		st, _ := status.FromError(err)
		if st.Code() == codes.NotFound && payload.ParentID != nil {
			return nil, ErrBranchNotFound{BranchID: *payload.ParentID}
		}
		if st.Code() == codes.InvalidArgument {
			return nil, ErrInvalidConfiguration{Name: name, Message: st.Message()}
		}
		if st.Code() == codes.FailedPrecondition && payload.ParentID != nil {
			return nil, ErrParentBranchUnhealthy{ParentID: *payload.ParentID}
		}
		return nil, err
	}
	return branch, nil
}

func (p *BranchProvisioner) DeleteBranch(ctx context.Context, organizationID, projectID, branchID string) error {
	return p.store.DeleteBranch(ctx, organizationID, projectID, branchID, func(branch *store.Branch) error {
		return cells.DeprovisionBranch(ctx, organizationID, p.cells, branch)
	})
}

// DeleteProject deprovisions every branch of a project and then the now-empty
// project. Best-effort and idempotent: a branch that fails to tear down does not
// stop the others, already-gone branches or project are tolerated, and the project
// is deleted only once all branches are gone (else it is left for a retry). It
// returns the number of branches deprovisioned and the joined branch/project
// errors (nil when the project is fully gone).
func (p *BranchProvisioner) DeleteProject(ctx context.Context, organizationID, projectID string) (int, error) {
	branches, err := p.store.ListBranches(ctx, organizationID, projectID)
	if err != nil {
		return 0, fmt.Errorf("list branches for project %s: %w", projectID, err)
	}
	deleted := 0
	var errs []error
	for _, b := range branches {
		if err := p.DeleteBranch(ctx, organizationID, projectID, b.ID); err != nil {
			if _, ok := errors.AsType[store.ErrBranchNotFound](err); ok {
				continue // raced with another delete; already gone
			}
			errs = append(errs, fmt.Errorf("delete branch %s: %w", b.ID, err))
			continue
		}
		deleted++
	}
	if len(errs) == 0 {
		if err := p.store.DeleteProject(ctx, organizationID, projectID); err != nil {
			if _, ok := errors.AsType[store.ErrProjectNotFound](err); !ok {
				errs = append(errs, fmt.Errorf("delete project %s: %w", projectID, err))
			}
		}
	}
	return deleted, errors.Join(errs...)
}
