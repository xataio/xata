package sqlstore

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"time"

	"xata/internal/idgen"
	"xata/internal/o11y"
	"xata/internal/pgroll"
	"xata/services/projects/store"

	"github.com/lib/pq"
)

//go:embed migrations/*.json
var migrationsFS embed.FS

const (
	UniqueConstraintProject                          = "unique_org_project_active"
	UniqueConstraintBranch                           = "unique_project_branch_name_active"
	UniqueRegionsConstraint                          = "regions_pkey"
	UniqueConstraintGithubInstallationOrg            = "github_installations_unique_org_installation"
	UniqueConstraintGithubInstallationInstallationID = "github_installations_unique_installation_id"
)

const (
	StatusActive     = "active"
	StatusTerminated = "terminated"
)

const (
	maxOpenConns    = 25
	maxIdleConns    = 10
	connMaxLifetime = 30 * time.Minute
	connMaxIdleTime = 5 * time.Minute
)

// check if sqlProjectStore implements the ProjectStore interface
var _ store.ProjectsStore = (*sqlProjectStore)(nil)

type sqlProjectStore struct {
	config Config
	sql    *sql.DB
	pgroll *pgroll.PGRoll
	// maxDepth is the maximum depth of the branch tree
	maxDepth int32
}

// NewSQLProjectStore creates a ProjectStore backend that uses SQL.
func NewSQLProjectStore(ctx context.Context, cfg Config, maxBranchTreeDepth int32) (*sqlProjectStore, error) {
	// set search path to the latest known version
	pgroll, err := pgroll.FromEmbeddedFS(&migrationsFS)
	if err != nil {
		return nil, fmt.Errorf("failed to create pgroll: %w", err)
	}

	// connect to the database (with the latest schema version)
	latest := pgroll.LatestVersionSchema(ctx)
	db, err := sql.Open("postgres", cfg.ConnectionString()+"&search_path="+latest)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)
	db.SetConnMaxLifetime(connMaxLifetime)
	db.SetConnMaxIdleTime(connMaxIdleTime)

	return &sqlProjectStore{
		sql:      db,
		config:   cfg,
		pgroll:   pgroll,
		maxDepth: maxBranchTreeDepth,
	}, nil
}

// Setup runs DB migrations for the store
func (s *sqlProjectStore) Setup(ctx context.Context) error {
	// TODO move this to its own package (+ CLI tool?)
	logger := o11y.Ctx(ctx).Logger()
	logger.Info().Msg("Running DB migrations")

	err := s.pgroll.ApplyMigrations(ctx, s.config.ConnectionString())
	if err != nil {
		return fmt.Errorf("failed to apply migrations: %w", err)
	}
	return nil
}

func (s *sqlProjectStore) Close(ctx context.Context) error {
	return s.sql.Close()
}

func (s *sqlProjectStore) ListRegions(ctx context.Context, organizationID string) ([]store.Region, error) {
	res, err := s.sql.QueryContext(ctx, "SELECT id, organization_id, public_access, backups_enabled, provider, hostport, created_at FROM regions WHERE organization_id = $1 OR organization_id IS NULL ORDER BY id", organizationID)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	regions := []store.Region{}
	for res.Next() {
		var region store.Region
		err := res.Scan(&region.ID, &region.OrganizationID, &region.PublicAccess, &region.BackupsEnabled, &region.Provider, &region.GatewayHostPort, &region.CreatedAt)
		if err != nil {
			return nil, err
		}

		regions = append(regions, region)
	}

	return regions, nil
}

// ListAllRegions returns a list of all existing regions
func (s *sqlProjectStore) ListAllRegions(ctx context.Context) ([]store.Region, error) {
	res, err := s.sql.QueryContext(ctx, "SELECT id, organization_id, public_access, backups_enabled, provider, hostport, created_at FROM regions ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer res.Close()
	regions := []store.Region{}
	for res.Next() {
		var region store.Region
		err := res.Scan(&region.ID, &region.OrganizationID, &region.PublicAccess, &region.BackupsEnabled, &region.Provider, &region.GatewayHostPort, &region.CreatedAt)
		if err != nil {
			return nil, err
		}
		regions = append(regions, region)
	}
	return regions, nil
}

func (s *sqlProjectStore) CreateRegion(ctx context.Context, regionID string, flags store.RegionFlags, hostport string) (*store.Region, error) {
	provider, err := store.ParseProvider(string(flags.Provider))
	if err != nil {
		return nil, err
	}
	res := s.sql.QueryRowContext(ctx,
		"INSERT INTO regions (id, public_access, backups_enabled, provider, hostport) VALUES ($1, $2, $3, $4, $5) RETURNING id, organization_id, public_access, backups_enabled, provider, hostport, created_at",
		regionID, flags.PublicAccess, flags.BackupsEnabled, provider, hostport)
	var region store.Region
	err = res.Scan(&region.ID, &region.OrganizationID, &region.PublicAccess, &region.BackupsEnabled, &region.Provider, &region.GatewayHostPort, &region.CreatedAt)
	if err != nil {
		// region already exists in this organization (regions_pkey constraint)
		if IsConstraintError(err, UniqueRegionsConstraint) {
			return nil, store.ErrRegionAlreadyExists{ID: regionID}
		}

		return nil, err
	}

	return &region, nil
}

func (s *sqlProjectStore) CreateOrganizationRegion(ctx context.Context, organizationID string, regionID string, flags store.RegionFlags, hostport string) (*store.Region, error) {
	provider, err := store.ParseProvider(string(flags.Provider))
	if err != nil {
		return nil, err
	}
	res := s.sql.QueryRowContext(ctx,
		"INSERT INTO regions (id, public_access, backups_enabled, provider, organization_id, hostport) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id, organization_id, public_access, backups_enabled, provider, hostport, created_at",
		regionID,
		flags.PublicAccess,
		flags.BackupsEnabled,
		provider,
		organizationID,
		hostport)
	var region store.Region
	err = res.Scan(&region.ID, &region.OrganizationID, &region.PublicAccess, &region.BackupsEnabled, &region.Provider, &region.GatewayHostPort, &region.CreatedAt)
	if err != nil {
		// region already exists in this organization (regions_pkey constraint)
		if IsConstraintError(err, UniqueRegionsConstraint) {
			return nil, store.ErrRegionAlreadyExists{ID: regionID}
		}
		return nil, err
	}

	return &region, nil
}

func (s *sqlProjectStore) DeleteRegion(ctx context.Context, regionID string) error {
	res, err := s.sql.ExecContext(ctx, "DELETE FROM regions WHERE id = $1", regionID)
	if err != nil {
		return fmt.Errorf("failed to delete region: %w", err)
	}

	if count, _ := res.RowsAffected(); count == 0 {
		return store.ErrRegionNotFound{ID: regionID}
	}

	return nil
}

func (s *sqlProjectStore) GetRegion(ctx context.Context, organizationID string, regionID string) (*store.Region, error) {
	res := s.sql.QueryRowContext(ctx, "SELECT id, organization_id, public_access, backups_enabled, provider, hostport, created_at FROM regions WHERE id = $1 AND (organization_id = $2 OR organization_id IS NULL)", regionID, organizationID)
	var region store.Region
	err := res.Scan(&region.ID, &region.OrganizationID, &region.PublicAccess, &region.BackupsEnabled, &region.Provider, &region.GatewayHostPort, &region.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrRegionNotFound{ID: regionID}
		}
		return nil, err
	}

	return &region, nil
}

func (s *sqlProjectStore) ListCells(ctx context.Context, organizationID string, regionID string) ([]store.Cell, error) {
	res, err := s.sql.QueryContext(ctx, "SELECT c.id, c.region_id, c.grpc_url, c.created_at, c.is_primary FROM cells c INNER JOIN regions ON c.region_id = regions.id WHERE c.region_id = $1 AND (regions.organization_id = $2 OR regions.organization_id IS NULL)",
		regionID, organizationID)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	cells := []store.Cell{}
	for res.Next() {
		var cell store.Cell
		err := res.Scan(&cell.ID, &cell.RegionID, &cell.ClustersGRPCURL, &cell.CreatedAt, &cell.Primary)
		if err != nil {
			return nil, err
		}

		cells = append(cells, cell)
	}

	return cells, nil
}

// ListAllCells returns a list of all existing cells (across all regions)
func (s *sqlProjectStore) ListAllCells(ctx context.Context) ([]store.Cell, error) {
	res, err := s.sql.QueryContext(ctx, "SELECT id, region_id, grpc_url, created_at, is_primary FROM cells")
	if err != nil {
		return nil, err
	}
	defer res.Close()

	cells := []store.Cell{}
	for res.Next() {
		var cell store.Cell
		err := res.Scan(&cell.ID, &cell.RegionID, &cell.ClustersGRPCURL, &cell.CreatedAt, &cell.Primary)
		if err != nil {
			return nil, err
		}

		cells = append(cells, cell)
	}

	return cells, nil
}

func (s *sqlProjectStore) CreateCell(ctx context.Context, regionID, cellID, grpcURL string, isPrimary bool) (*store.Cell, error) {
	rows := s.sql.QueryRowContext(ctx, "INSERT INTO cells (id, region_id, grpc_url, is_primary) VALUES ($1, $2, $3, $4) RETURNING id, region_id, grpc_url, created_at, is_primary",
		cellID, regionID, grpcURL, isPrimary)
	var cell store.Cell

	err := rows.Scan(&cell.ID, &cell.RegionID, &cell.ClustersGRPCURL, &cell.CreatedAt, &cell.Primary)
	if err != nil {
		// cell already exists in this region
		if IsConstraintError(err, "cells_pkey") {
			return nil, store.ErrCellAlreadyExists{ID: cellID}
		}
		return nil, err
	}

	return &cell, nil
}

func (s *sqlProjectStore) GetCell(ctx context.Context, organizationID string, cellID string) (*store.Cell, error) {
	tx, err := s.sql.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	cell, err := s.getCell(ctx, tx, organizationID, cellID)
	if err != nil {
		return nil, err
	}

	return cell, nil
}

func (s *sqlProjectStore) GetPrimaryCell(ctx context.Context, organizationID string, regionID string) (*store.Cell, error) {
	res := s.sql.QueryRowContext(ctx, "SELECT c.id, c.region_id, c.grpc_url, c.created_at, c.is_primary FROM cells c INNER JOIN regions ON c.region_id = regions.id WHERE c.region_id = $1 AND c.is_primary = true AND (regions.organization_id = $2 OR regions.organization_id IS NULL)", regionID, organizationID)

	var cell store.Cell
	err := res.Scan(&cell.ID, &cell.RegionID, &cell.ClustersGRPCURL, &cell.CreatedAt, &cell.Primary)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrCellNotFound{ID: regionID}
		}

		return nil, err
	}

	return &cell, nil
}

func (s *sqlProjectStore) DeleteCell(ctx context.Context, cellID string) error {
	res, err := s.sql.ExecContext(ctx, "DELETE FROM cells WHERE id = $1", cellID)
	if err != nil {
		return fmt.Errorf("failed to delete cell: %w", err)
	}

	if count, _ := res.RowsAffected(); count == 0 {
		return store.ErrCellNotFound{ID: cellID}
	}

	return nil
}

func (s *sqlProjectStore) CreateProject(ctx context.Context, organizationID string, config *store.CreateProjectConfiguration) (*store.Project, error) {
	if err := s.enforceProjectCreationLimits(ctx, organizationID, config.UsageTier); err != nil {
		return nil, err
	}

	if config.Name == "" {
		return nil, store.ErrInvalidProjectName{Name: config.Name}
	}

	cidrs := config.IPFiltering.CIDRs
	if cidrs == nil {
		cidrs = []store.CIDREntry{}
	}
	cidrsJSON, err := json.Marshal(cidrs)
	if err != nil {
		return nil, fmt.Errorf("marshal cidrs: %w", err)
	}

	projectID := idgen.GenerateWithPrefix("prj")
	res := s.sql.QueryRowContext(ctx,
		`INSERT INTO projects (
			id,
			name,
			organization_id,
			scale_to_zero_base_enabled,
			scale_to_zero_base_inactivity_minutes,
			scale_to_zero_child_enabled,
			scale_to_zero_child_inactivity_minutes,
			ip_filtering_enabled,
			ip_filtering_cidrs
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 RETURNING
			id,
			name,
			created_at,
			updated_at,
			scale_to_zero_base_enabled,
			scale_to_zero_base_inactivity_minutes,
			scale_to_zero_child_enabled,
			scale_to_zero_child_inactivity_minutes,
			ip_filtering_enabled,
			ip_filtering_cidrs`,
		projectID,
		config.Name,
		organizationID,
		config.ScaleToZero.BaseBranches.Enabled,
		config.ScaleToZero.BaseBranches.InactivityPeriod,
		config.ScaleToZero.ChildBranches.Enabled,
		config.ScaleToZero.ChildBranches.InactivityPeriod,
		config.IPFiltering.Enabled,
		cidrsJSON)

	var project store.Project
	var cidrsRaw []byte
	err = res.Scan(
		&project.ID,
		&project.Name,
		&project.CreatedAt,
		&project.UpdatedAt,
		&project.ScaleToZero.BaseBranches.Enabled,
		&project.ScaleToZero.BaseBranches.InactivityPeriod,
		&project.ScaleToZero.ChildBranches.Enabled,
		&project.ScaleToZero.ChildBranches.InactivityPeriod,
		&project.IPFiltering.Enabled,
		&cidrsRaw)
	if err != nil {
		// project already exists in this organization
		if IsConstraintError(err, UniqueConstraintProject) {
			return nil, store.ErrProjectAlreadyExists{Name: config.Name}
		}
		return nil, err
	}

	if err := json.Unmarshal(cidrsRaw, &project.IPFiltering.CIDRs); err != nil {
		return nil, fmt.Errorf("unmarshal cidrs: %w", err)
	}

	return &project, nil
}

func (s *sqlProjectStore) DeleteProject(ctx context.Context, organizationID string, projectID string) error {
	tx, err := s.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// check if there are active branches
	branches, err := tx.QueryContext(ctx, "SELECT id, parent_id, name, created_at, updated_at FROM branches WHERE project_id = $1 AND status = $2", projectID, StatusActive)
	if err != nil {
		return err
	}
	defer branches.Close()

	// there is at least one branch in the project
	if branches.Next() {
		return store.ErrProjectNotEmpty{ID: projectID}
	}
	if err := branches.Err(); err != nil {
		return err
	}

	// clean up any remaining backup entries for this project
	_, err = tx.ExecContext(ctx, "DELETE FROM backups WHERE project_id = $1", projectID)
	if err != nil {
		return err
	}

	res, err := tx.ExecContext(ctx,
		"UPDATE projects SET status = $1, status_changed_at = NOW() WHERE id = $2 AND organization_id = $3",
		StatusTerminated, projectID, organizationID)
	if err != nil {
		return err
	}

	if count, _ := res.RowsAffected(); count == 0 {
		return store.ErrProjectNotFound{ID: projectID}
	}

	// all good, commit
	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (s *sqlProjectStore) GetProject(ctx context.Context, organizationID string, projectID string) (*store.Project, error) {
	tx, err := s.sql.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	project, err := s.getProject(ctx, tx, organizationID, projectID)
	if err != nil {
		return nil, err
	}

	return project, nil
}

func (s *sqlProjectStore) getProject(ctx context.Context, tx *sql.Tx, organizationID string, projectID string) (*store.Project, error) {
	query := `SELECT
		id,
		name,
		created_at,
		updated_at,
		scale_to_zero_base_enabled,
		scale_to_zero_base_inactivity_minutes,
		scale_to_zero_child_enabled,
		scale_to_zero_child_inactivity_minutes,
		ip_filtering_enabled,
		ip_filtering_cidrs
	FROM projects WHERE id = $1 AND organization_id = $2 AND status = $3`
	var row *sql.Row
	if tx == nil {
		row = s.sql.QueryRow(query, projectID, organizationID, StatusActive)
	} else {
		row = tx.QueryRowContext(ctx, query, projectID, organizationID, StatusActive)
	}
	var project store.Project
	var cidrsRaw []byte
	err := row.Scan(
		&project.ID,
		&project.Name,
		&project.CreatedAt,
		&project.UpdatedAt,
		&project.ScaleToZero.BaseBranches.Enabled,
		&project.ScaleToZero.BaseBranches.InactivityPeriod,
		&project.ScaleToZero.ChildBranches.Enabled,
		&project.ScaleToZero.ChildBranches.InactivityPeriod,
		&project.IPFiltering.Enabled,
		&cidrsRaw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrProjectNotFound{ID: projectID}
		}

		return nil, err
	}

	if err := json.Unmarshal(cidrsRaw, &project.IPFiltering.CIDRs); err != nil {
		return nil, fmt.Errorf("unmarshal cidrs: %w", err)
	}

	return &project, nil
}

func (s *sqlProjectStore) getCell(ctx context.Context, tx *sql.Tx, organizationID, cellID string) (*store.Cell, error) {
	res := tx.QueryRowContext(ctx, "SELECT c.id, c.region_id, c.grpc_url, c.created_at, c.is_primary FROM cells c INNER JOIN regions ON c.region_id = regions.id WHERE c.id = $1 AND (regions.organization_id = $2 OR regions.organization_id IS NULL)", cellID, organizationID)

	var cell store.Cell
	err := res.Scan(&cell.ID, &cell.RegionID, &cell.ClustersGRPCURL, &cell.CreatedAt, &cell.Primary)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrCellNotFound{ID: cellID}
		}

		return nil, err
	}

	return &cell, nil
}

func (s *sqlProjectStore) ListProjects(ctx context.Context, organizationID string) ([]store.Project, error) {
	rows, err := s.sql.QueryContext(ctx, `SELECT
		id,
		name,
		created_at,
		updated_at,
		scale_to_zero_base_enabled,
		scale_to_zero_base_inactivity_minutes,
		scale_to_zero_child_enabled,
		scale_to_zero_child_inactivity_minutes,
		ip_filtering_enabled,
		ip_filtering_cidrs
	FROM projects WHERE organization_id = $1 AND status = $2`, organizationID, StatusActive)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	projects := []store.Project{}
	for rows.Next() {
		var project store.Project
		var cidrsRaw []byte
		err := rows.Scan(
			&project.ID,
			&project.Name,
			&project.CreatedAt,
			&project.UpdatedAt,
			&project.ScaleToZero.BaseBranches.Enabled,
			&project.ScaleToZero.BaseBranches.InactivityPeriod,
			&project.ScaleToZero.ChildBranches.Enabled,
			&project.ScaleToZero.ChildBranches.InactivityPeriod,
			&project.IPFiltering.Enabled,
			&cidrsRaw)
		if err != nil {
			return nil, err
		}

		if err := json.Unmarshal(cidrsRaw, &project.IPFiltering.CIDRs); err != nil {
			return nil, fmt.Errorf("unmarshal cidrs: %w", err)
		}

		projects = append(projects, project)
	}

	return projects, rows.Err()
}

func (s *sqlProjectStore) UpdateProject(ctx context.Context, organizationID, projectID string, config *store.UpdateProjectConfiguration, updateFn func(project *store.Project) error) (*store.Project, error) {
	tx, err := s.sql.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// check project exists in the organization used for authorization to block unauthorized access to branch
	if _, err := s.getProject(ctx, tx, organizationID, projectID); err != nil {
		return nil, err
	}

	query := ""
	var args []any
	switch {
	case config.Name != nil || config.ScaleToZero != nil || config.IPFiltering != nil:
		query = `UPDATE projects SET
			updated_at = NOW(),
			name=COALESCE($1, name),
			scale_to_zero_base_enabled=COALESCE($2, scale_to_zero_base_enabled),
			scale_to_zero_base_inactivity_minutes=COALESCE($3, scale_to_zero_base_inactivity_minutes),
			scale_to_zero_child_enabled=COALESCE($4, scale_to_zero_child_enabled),
			scale_to_zero_child_inactivity_minutes=COALESCE($5, scale_to_zero_child_inactivity_minutes),
			ip_filtering_enabled=COALESCE($6, ip_filtering_enabled),
			ip_filtering_cidrs=COALESCE($7, ip_filtering_cidrs)
			WHERE id=$8 RETURNING
			id,
			name,
			created_at,
			updated_at,
			scale_to_zero_base_enabled,
			scale_to_zero_base_inactivity_minutes,
			scale_to_zero_child_enabled,
			scale_to_zero_child_inactivity_minutes,
			ip_filtering_enabled,
			ip_filtering_cidrs`
		cidrs := config.IPFiltering.GetCIDRs()
		var cidrsJSON any
		if cidrs != nil {
			b, err := json.Marshal(*cidrs)
			if err != nil {
				return nil, fmt.Errorf("marshal cidrs: %w", err)
			}
			cidrsJSON = b
		}
		args = []any{
			config.Name,
			config.ScaleToZero.GetBaseEnabled(),
			config.ScaleToZero.GetBaseInactivityPeriod(),
			config.ScaleToZero.GetChildEnabled(),
			config.ScaleToZero.GetChildInactivityPeriod(),
			config.IPFiltering.GetEnabled(),
			cidrsJSON,
			projectID,
		}
	default:
		query = `SELECT
		id,
		name,
		created_at,
		updated_at,
		scale_to_zero_base_enabled,
		scale_to_zero_base_inactivity_minutes,
		scale_to_zero_child_enabled,
		scale_to_zero_child_inactivity_minutes,
		ip_filtering_enabled,
		ip_filtering_cidrs
		FROM projects WHERE id = $1`
		args = []any{projectID}
	}

	row := tx.QueryRowContext(ctx, query, args...)

	var project store.Project
	var cidrsRaw []byte
	err = row.Scan(
		&project.ID,
		&project.Name,
		&project.CreatedAt,
		&project.UpdatedAt,
		&project.ScaleToZero.BaseBranches.Enabled,
		&project.ScaleToZero.BaseBranches.InactivityPeriod,
		&project.ScaleToZero.ChildBranches.Enabled,
		&project.ScaleToZero.ChildBranches.InactivityPeriod,
		&project.IPFiltering.Enabled,
		&cidrsRaw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return &project, store.ErrProjectNotFound{ID: projectID}
		}
		if IsConstraintError(err, UniqueConstraintProject) && config.Name != nil {
			return &project, store.ErrProjectAlreadyExists{Name: *config.Name}
		}
		return &project, err
	}

	if err := json.Unmarshal(cidrsRaw, &project.IPFiltering.CIDRs); err != nil {
		return nil, fmt.Errorf("unmarshal cidrs: %w", err)
	}

	if updateFn != nil {
		if err := updateFn(&project); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &project, nil
}

func (s *sqlProjectStore) ListBranches(ctx context.Context, organizationID string, projectID string) ([]store.Branch, error) {
	tx, err := s.sql.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// check project exists in the organization used for authorization to block unauthorized access to branch
	if _, err := s.getProject(ctx, tx, organizationID, projectID); err != nil {
		return nil, err
	}

	res, err := tx.QueryContext(ctx,
		`SELECT
			b.id,
			b.parent_id,
			b.name,
			b.created_at,
			b.updated_at,
			b.cell_id,
			r.id,
			r.public_access
			FROM branches b
				INNER JOIN cells c ON b.cell_id = c.id
				INNER JOIN regions r ON c.region_id = r.id
			 WHERE b.project_id = $1 AND b.status = $2`, projectID, StatusActive)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	branches := []store.Branch{}
	for res.Next() {
		var branch store.Branch
		err := res.Scan(
			&branch.ID,
			&branch.ParentID,
			&branch.Name,
			&branch.CreatedAt,
			&branch.UpdatedAt,
			&branch.CellID,
			&branch.Region,
			&branch.PublicAccess)
		if err != nil {
			return nil, err
		}

		branches = append(branches, branch)
	}

	return branches, nil
}

func (s *sqlProjectStore) CreateBranch(ctx context.Context, organizationID, projectID, cellID string, config *store.CreateBranchConfiguration, provisionFn func(b *store.Branch) error) (*store.Branch, error) {
	if config.Limits == nil {
		return nil, fmt.Errorf("internal error: branch creation limits not resolved")
	}
	if err := s.enforceBranchCreationLimits(ctx, organizationID, projectID, *config.Limits); err != nil {
		return nil, err
	}

	tx, err := s.sql.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// check cellID exists in the organization
	_, err = s.getCell(ctx, tx, organizationID, cellID)
	if err != nil {
		return nil, err
	}

	// check project exists in the organization used for authorization to block unauthorized access to branch
	if _, err := s.getProject(ctx, tx, organizationID, projectID); err != nil {
		return nil, err
	}

	// check parent branch exists
	var parentBranch *store.Branch
	if config.ParentID != nil {
		parentBranch, err = s.describeBranch(ctx, tx, projectID, *config.ParentID)
		if err != nil {
			return nil, err
		}
	}

	branchDepth := int32(1)
	if config.ParentID != nil {
		// check whether a child branch can be added
		branchDepth, err = parentBranch.CanAddChild(s.maxDepth)
		if err != nil {
			return nil, err
		}
	}

	// insert branch
	branchID := idgen.GenerateClusterID()
	query := `INSERT INTO branches (
		id,
		name,
		project_id,
		parent_id,
		description,
		cell_id,
		depth
	) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING
		id,
		name,
		parent_id,
		description,
		cell_id,
		depth,
		created_at,
		updated_at`
	row := tx.QueryRowContext(ctx, query,
		branchID,
		config.Name,
		projectID,
		config.ParentID,
		config.Description,
		cellID,
		branchDepth)

	var branch store.Branch
	err = row.Scan(
		&branch.ID,
		&branch.Name,
		&branch.ParentID,
		&branch.Description,
		&branch.CellID,
		&branch.Depth,
		&branch.CreatedAt,
		&branch.UpdatedAt)
	if err != nil {
		// branch already exists in this project
		if IsConstraintError(err, UniqueConstraintBranch) {
			return nil, store.ErrBranchAlreadyExists{Name: config.Name}
		}
		return nil, err
	}

	// get region ID & sql access info
	row = tx.QueryRowContext(ctx, "SELECT c.region_id, r.public_access FROM cells c INNER JOIN regions r ON c.region_id = r.id WHERE c.id = $1", branch.CellID)
	err = row.Scan(&branch.Region, &branch.PublicAccess)
	if err != nil {
		return nil, err
	}

	// provision branch
	if err := provisionFn(&branch); err != nil {
		return nil, err
	}

	// all good, commit
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &branch, nil
}

func (s *sqlProjectStore) DescribeBranch(ctx context.Context, organizationID, projectID, branchID string) (*store.Branch, error) {
	tx, err := s.sql.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// check project exists in the organization used for authorization to block unauthorized access to branch
	if _, err := s.getProject(ctx, tx, organizationID, projectID); err != nil {
		return nil, err
	}

	branch, err := s.describeBranch(ctx, tx, projectID, branchID)
	if err != nil {
		return nil, err
	}

	return branch, nil
}

func (s *sqlProjectStore) describeBranch(ctx context.Context, tx *sql.Tx, projectID, branchID string) (*store.Branch, error) {
	row := tx.QueryRowContext(ctx,
		`SELECT
			b.id,
			b.name,
			b.created_at,
			b.updated_at,
			b.parent_id,
			b.description,
			b.cell_id,
			b.depth,
			r.id,
			r.public_access,
			r.backups_enabled
			FROM branches b
			INNER JOIN cells c ON b.cell_id = c.id
			INNER JOIN regions r ON c.region_id = r.id WHERE b.project_id = $1 AND b.id = $2 AND b.status = $3`, projectID, branchID, StatusActive)

	var branch store.Branch
	err := row.Scan(
		&branch.ID,
		&branch.Name,
		&branch.CreatedAt,
		&branch.UpdatedAt,
		&branch.ParentID,
		&branch.Description,
		&branch.CellID,
		&branch.Depth,
		&branch.Region,
		&branch.PublicAccess,
		&branch.BackupsEnabled,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrBranchNotFound{ID: branchID}
		}

		return nil, err
	}

	return &branch, nil
}

func (s *sqlProjectStore) GetBranchByName(ctx context.Context, organizationID, projectID, name string) (*store.Branch, error) {
	tx, err := s.sql.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if _, err := s.getProject(ctx, tx, organizationID, projectID); err != nil {
		return nil, err
	}

	row := tx.QueryRowContext(ctx,
		`SELECT
			b.id,
			b.name,
			b.created_at,
			b.updated_at,
			b.parent_id,
			b.description,
			b.cell_id,
			b.depth,
			r.id,
			r.public_access,
			r.backups_enabled
			FROM branches b
			INNER JOIN cells c ON b.cell_id = c.id
			INNER JOIN regions r ON c.region_id = r.id WHERE b.project_id = $1 AND b.name = $2 AND b.status = $3`, projectID, name, StatusActive)

	var branch store.Branch
	if err := row.Scan(
		&branch.ID,
		&branch.Name,
		&branch.CreatedAt,
		&branch.UpdatedAt,
		&branch.ParentID,
		&branch.Description,
		&branch.CellID,
		&branch.Depth,
		&branch.Region,
		&branch.PublicAccess,
		&branch.BackupsEnabled,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrBranchNotFound{ID: name}
		}
		return nil, err
	}

	return &branch, nil
}

func (s *sqlProjectStore) UpdateBranch(ctx context.Context, organizationID, projectID, branchID string, config *store.UpdateBranchConfiguration, updateFn func(b *store.Branch) error) (*store.Branch, error) {
	tx, err := s.sql.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// check project exists in the organization used for authorization to block unauthorized access to branch
	if _, err := s.getProject(ctx, tx, organizationID, projectID); err != nil {
		return nil, err
	}

	// ensure the branch belongs to this project (and is active) before mutating it
	if _, err := s.describeBranch(ctx, tx, projectID, branchID); err != nil {
		return nil, err
	}

	query := ""
	var args []any
	// update if any parameters need updating
	if config.Name != nil || config.Description != nil {
		query = `UPDATE branches SET
			updated_at = NOW(),
			name=COALESCE($1, name),
			description=COALESCE($2, description)
			WHERE id=$3 RETURNING
			id,
			name,
			parent_id,
			description,
			cell_id,
			depth,
			created_at,
			updated_at`
		args = []any{
			config.Name,
			config.Description,
			branchID,
		}
	} else {
		query = `SELECT
			id,
			name,
			parent_id,
			description,
			cell_id,
			depth,
			created_at,
			updated_at
		FROM branches WHERE id = $1`
		args = []any{branchID}
	}

	row := tx.QueryRowContext(ctx, query, args...)

	var branch store.Branch
	err = row.Scan(
		&branch.ID,
		&branch.Name,
		&branch.ParentID,
		&branch.Description,
		&branch.CellID,
		&branch.Depth,
		&branch.CreatedAt,
		&branch.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrBranchNotFound{ID: branchID}
		}
		// branch already exists in this project this will only happen if we want to
		// change the branch name since that is the only field with a constraint, but
		// adding a nil check to be on the safe side
		if IsConstraintError(err, UniqueConstraintBranch) && config.Name != nil {
			return nil, store.ErrBranchAlreadyExists{Name: *config.Name}
		}
		return nil, err
	}

	// get region info
	row = tx.QueryRowContext(ctx,
		`SELECT cells.region_id, regions.backups_enabled
		 FROM cells
		 JOIN regions ON cells.region_id = regions.id
		 WHERE cells.id = $1`, branch.CellID)
	err = row.Scan(&branch.Region, &branch.BackupsEnabled)
	if err != nil {
		return nil, err
	}

	// update branch config in k8s
	if err := updateFn(&branch); err != nil {
		return nil, err
	}

	// all good, commit
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &branch, nil
}

func (s *sqlProjectStore) DeleteBranch(ctx context.Context, organizationID, projectID, branchID string, deprovisionFn func(b *store.Branch) error) error {
	tx, err := s.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// check project exists in the organization used for authorization to block unauthorized access to branch
	if _, err := s.getProject(ctx, tx, organizationID, projectID); err != nil {
		return err
	}

	// get branch
	branch, err := s.describeBranch(ctx, tx, projectID, branchID)
	if err != nil {
		return err
	}

	// soft delete branch
	result, err := tx.ExecContext(ctx,
		"UPDATE branches SET status = $1, status_changed_at = NOW() WHERE id = $2",
		StatusTerminated, branchID)
	if err != nil {
		return err
	}
	if result == nil {
		return fmt.Errorf("could not delete branch %s", branchID)
	}

	numRows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if numRows == 0 {
		return store.ErrBranchNotFound{ID: branchID}
	}
	if numRows > 1 {
		return store.ErrTooManyRecords{NumRecords: numRows}
	}

	// delete continuous backup
	// TODO  when we implement backup deletes, this will only happen if the user chooses not to keep backups
	result, err = tx.ExecContext(ctx,
		"DELETE FROM backups WHERE id = $1", branchID)
	if err != nil {
		return err
	}
	if result == nil {
		return fmt.Errorf("could not delete backup %s", branchID)
	}

	numRows, err = result.RowsAffected()
	if err != nil {
		return err
	}
	//TODO add this check back in a safe way
	//if numRows == 0 {
	//	return store.ErrBranchNotFound{ID: branchID}
	//}
	if numRows > 1 {
		return store.ErrTooManyRecords{NumRecords: numRows}
	}

	// de-provision branch
	if err := deprovisionFn(branch); err != nil {
		return err
	}

	// all good, commit
	return tx.Commit()
}

// IsConstraintError checks if a given constraint was not met
func IsConstraintError(err error, constraint string) bool {
	if pqErr, ok := errors.AsType[*pq.Error](err); ok {
		return pqErr.Code == "23505" && pqErr.Constraint == constraint
	}
	return false
}

func (s *sqlProjectStore) ListInstanceTypes(ctx context.Context, organizationID string, region string) ([]store.InstanceType, error) {
	// Get the region multiplier, default to 1.0 if region not found
	multiplier := 1.0
	if regionMultiplier, exists := store.RegionMultiplier[region]; exists {
		multiplier = regionMultiplier
	}

	// Create a copy of instance types with adjusted hourly rates
	instanceTypes := make([]store.InstanceType, len(store.InstanceTypes))
	for i, instanceType := range store.InstanceTypes {
		instanceTypes[i] = instanceType
		instanceTypes[i].HourlyRate = math.Round(instanceType.HourlyRate*multiplier*1000) / 1000
		instanceTypes[i].StorageMonthlyRate = math.Round(instanceType.StorageMonthlyRate*multiplier*100) / 100
	}

	return instanceTypes, nil
}

func (s *sqlProjectStore) ValidateHierarchy(ctx context.Context, organizationIds []string, projectIds []string, branchIds []string) error {
	if len(organizationIds) == 0 || slices.Contains(organizationIds, "") {
		return store.ErrInvalidHierarchy{Type: "organization"}
	}

	if len(projectIds) > 0 || len(branchIds) > 0 {
		var itemType, itemID string
		err := s.sql.QueryRowContext(ctx, `
			SELECT 'project' AS type, p.id
			FROM unnest($1::text[]) AS p(id)
			WHERE NOT EXISTS (
				SELECT 1 FROM projects
				WHERE id = p.id AND organization_id = ANY($2)
			)
			UNION ALL
			SELECT 'branch' AS type, b.id
			FROM unnest($3::text[]) AS b(id)
			WHERE NOT EXISTS (
				SELECT 1 FROM branches
				WHERE id = b.id AND project_id = ANY($4)
			)
			LIMIT 1`,
			pq.Array(projectIds), pq.Array(organizationIds),
			pq.Array(branchIds), pq.Array(projectIds)).Scan(&itemType, &itemID)

		if err == nil {
			return store.ErrInvalidHierarchy{
				Type: itemType,
				ID:   itemID,
			}
		}
		// If no rows were found, it means all projects and branches are valid
		if err != sql.ErrNoRows {
			return store.ErrInvalidHierarchy{}
		}
	}

	return nil
}

func (s *sqlProjectStore) CleanupTerminatedBranches(ctx context.Context, cellID string, terminatedFor time.Duration) (int64, error) {
	result, err := s.sql.ExecContext(ctx,
		`DELETE FROM branches
		 WHERE cell_id = $1
		 AND status = $2
		 AND status_changed_at < NOW() - $3 * interval '1 second'`,
		cellID, StatusTerminated, terminatedFor.Seconds())
	if err != nil {
		return 0, fmt.Errorf("delete terminated branches: %w", err)
	}

	return result.RowsAffected()
}

func (s *sqlProjectStore) CleanupTerminatedProjects(ctx context.Context, terminatedFor time.Duration) (int64, error) {
	result, err := s.sql.ExecContext(ctx,
		`DELETE FROM projects
		 WHERE status = $1
		 AND status_changed_at < NOW() - $2 * interval '1 second'`,
		StatusTerminated, terminatedFor.Seconds())
	if err != nil {
		return 0, fmt.Errorf("delete terminated projects: %w", err)
	}

	return result.RowsAffected()
}

func (s *sqlProjectStore) CountActiveProjectBranches(ctx context.Context, projectID string) (int64, error) {
	var count int64
	err := s.sql.QueryRowContext(ctx,
		`SELECT COUNT(id) FROM branches WHERE project_id = $1 AND status = $2`,
		projectID, StatusActive).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count active project branches: %w", err)
	}
	return count, nil
}

func (s *sqlProjectStore) CountOrganizationBranches(ctx context.Context, organizationID string) (int64, error) {
	var count int64
	err := s.sql.QueryRowContext(ctx,
		`SELECT COUNT(b.id) FROM branches b
		 INNER JOIN projects p ON b.project_id = p.id
		 WHERE p.organization_id = $1`,
		organizationID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count organization branches: %w", err)
	}
	return count, nil
}

func (s *sqlProjectStore) CountActiveOrgBranches(ctx context.Context, organizationID string) (int64, error) {
	var count int64
	err := s.sql.QueryRowContext(ctx,
		`SELECT COUNT(b.id) FROM branches b
		 INNER JOIN projects p ON b.project_id = p.id
		 WHERE p.organization_id = $1 AND b.status = $2`,
		organizationID, StatusActive).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count active org branches: %w", err)
	}
	return count, nil
}

func (s *sqlProjectStore) CountActiveOrgProjects(ctx context.Context, organizationID string) (int64, error) {
	var count int64
	err := s.sql.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM projects WHERE organization_id = $1 AND status = $2`,
		organizationID, StatusActive).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count active org projects: %w", err)
	}
	return count, nil
}

func (s *sqlProjectStore) CountBranchesCreatedInLastHour(ctx context.Context, organizationID string) (int64, error) {
	var count int64
	err := s.sql.QueryRowContext(ctx,
		`SELECT COUNT(b.id) FROM branches b
		 INNER JOIN projects p ON b.project_id = p.id
		 WHERE p.organization_id = $1 AND b.created_at > NOW() - INTERVAL '1 hour'`,
		organizationID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count branches created in last hour: %w", err)
	}
	return count, nil
}

func (s *sqlProjectStore) CountProjectsCreatedInLastHour(ctx context.Context, organizationID string) (int64, error) {
	var count int64
	err := s.sql.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM projects WHERE organization_id = $1 AND created_at > NOW() - INTERVAL '1 hour'`,
		organizationID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count projects created in last hour: %w", err)
	}
	return count, nil
}

// AcquireProjectLock acquires a PostgreSQL advisory lock for the given projectID.
// Uses hashtextextended() to convert projectID string to a bigint for the advisory lock key,
// avoiding the collision risk of the 32-bit hashtext().
// The lock is released by explicitly calling pg_advisory_unlock before returning
// the connection to the pool. Using context.Background() for the unlock ensures
// the lock is released even if the request context has been cancelled.
func (s *sqlProjectStore) AcquireProjectLock(ctx context.Context, projectID string) (func() error, error) {
	conn, err := s.sql.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("get database connection: %w", err)
	}

	_, err = conn.ExecContext(ctx, `SELECT pg_advisory_lock(hashtextextended($1, 0))`, projectID)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("acquire project lock: %w", err)
	}

	release := func() error {
		_, _ = conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, projectID)
		return conn.Close()
	}

	return release, nil
}

func (s *sqlProjectStore) enforceProjectCreationLimits(ctx context.Context, organizationID, usageTier string) error {
	tier, _ := store.ParseUsageTier(usageTier)
	var overrides map[store.LimitKey]any
	// We don't allow overrides for T1 organizations
	if tier != store.TierT1 {
		var err error
		overrides, err = s.GetOrgLimits(ctx, organizationID, "")
		if err != nil {
			return fmt.Errorf("get org limits: %w", err)
		}
	}
	if maxProjects := store.ResolveIntLimit(overrides, store.LimitMaxProjects, store.TierDefaultInt(tier, store.LimitMaxProjects, 0)); maxProjects != 0 {
		count, err := s.CountActiveOrgProjects(ctx, organizationID)
		if err != nil {
			return fmt.Errorf("count active org projects: %w", err)
		}
		if count >= int64(maxProjects) {
			return store.ErrOrgProjectLimitExceeded{OrganizationID: organizationID, Limit: maxProjects}
		}
	}
	if maxPerHour := store.ResolveIntLimit(overrides, store.LimitMaxProjectsPerHour, store.TierDefaultInt(tier, store.LimitMaxProjectsPerHour, 0)); maxPerHour != 0 {
		count, err := s.CountProjectsCreatedInLastHour(ctx, organizationID)
		if err != nil {
			return fmt.Errorf("count projects created in last hour: %w", err)
		}
		if count >= int64(maxPerHour) {
			return store.ErrProjectRateLimitExceeded{OrganizationID: organizationID, Limit: maxPerHour}
		}
	}
	return nil
}

func (s *sqlProjectStore) enforceBranchCreationLimits(ctx context.Context, organizationID, projectID string, limits store.OrgLimits) error {
	count, err := s.CountActiveOrgBranches(ctx, organizationID)
	if err != nil {
		return fmt.Errorf("count active org branches: %w", err)
	}
	if count >= int64(limits.MaxBranchesPerOrg) {
		return store.ErrOrgBranchLimitExceeded{OrganizationID: organizationID, Limit: limits.MaxBranchesPerOrg}
	}
	count, err = s.CountBranchesCreatedInLastHour(ctx, organizationID)
	if err != nil {
		return fmt.Errorf("count branches created in last hour: %w", err)
	}
	if count >= int64(limits.MaxBranchesPerHour) {
		return store.ErrBranchRateLimitExceeded{OrganizationID: organizationID, Limit: limits.MaxBranchesPerHour}
	}
	count, err = s.CountActiveProjectBranches(ctx, projectID)
	if err != nil {
		return fmt.Errorf("count active project branches: %w", err)
	}
	if count >= int64(limits.MaxBranchesPerProject) {
		return store.ErrTooManyBranches{ID: projectID, Limit: limits.MaxBranchesPerProject}
	}
	return nil
}

func (s *sqlProjectStore) CreateGithubInstallation(ctx context.Context, organization string, installationID int64) (*store.GithubInstallation, error) {
	var inst store.GithubInstallation
	err := s.sql.QueryRowContext(ctx,
		`INSERT INTO github_installations (id, installation_id, organization, created_at, updated_at)
		 VALUES ($1, $2, $3, NOW(), NOW())
		 RETURNING id, installation_id, organization, created_at, updated_at`,
		idgen.Generate(), installationID, organization).Scan(
		&inst.ID, &inst.InstallationID, &inst.Organization, &inst.CreatedAt, &inst.UpdatedAt)
	if err != nil {
		if isGithubInstallationAlreadyExistsError(err) {
			return nil, store.ErrGithubInstallationAlreadyExists{Organization: organization, InstallationID: installationID}
		}
		return nil, err
	}
	return &inst, nil
}

func (s *sqlProjectStore) ListGithubInstallations(ctx context.Context, organization string) ([]store.GithubInstallation, error) {
	rows, err := s.sql.QueryContext(ctx,
		`SELECT id, installation_id, organization, created_at, updated_at
		 FROM github_installations
		 WHERE organization = $1
		 ORDER BY created_at`,
		organization)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	installations := []store.GithubInstallation{}
	for rows.Next() {
		var inst store.GithubInstallation
		if err := rows.Scan(&inst.ID, &inst.InstallationID, &inst.Organization, &inst.CreatedAt, &inst.UpdatedAt); err != nil {
			return nil, err
		}
		installations = append(installations, inst)
	}
	return installations, rows.Err()
}

func (s *sqlProjectStore) UpdateGithubInstallation(ctx context.Context, organization, id string, installationID int64) (*store.GithubInstallation, error) {
	var inst store.GithubInstallation
	err := s.sql.QueryRowContext(ctx,
		`UPDATE github_installations
		 SET installation_id = $1, updated_at = NOW()
		 WHERE id = $2 AND organization = $3
		 RETURNING id, installation_id, organization, created_at, updated_at`,
		installationID, id, organization).Scan(
		&inst.ID, &inst.InstallationID, &inst.Organization, &inst.CreatedAt, &inst.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrGithubInstallationNotFound{Organization: organization, ID: id}
		}
		if isGithubInstallationAlreadyExistsError(err) {
			return nil, store.ErrGithubInstallationAlreadyExists{Organization: organization, InstallationID: installationID}
		}
		return nil, err
	}
	return &inst, nil
}

func isGithubInstallationAlreadyExistsError(err error) bool {
	return IsConstraintError(err, UniqueConstraintGithubInstallationOrg) ||
		IsConstraintError(err, UniqueConstraintGithubInstallationInstallationID)
}

func (s *sqlProjectStore) CreateGithubRepoMapping(ctx context.Context, organization, project string, repoID int64, rootBranchID string) (*store.GithubRepoMapping, error) {
	var mapping store.GithubRepoMapping
	err := s.sql.QueryRowContext(ctx,
		`INSERT INTO github_repositories (id, github_repository_id, project_id, root_branch_id, created_at, updated_at)
		 SELECT $1, $2, $3, $4, NOW(), NOW()
		 WHERE EXISTS (SELECT 1 FROM projects WHERE id = $3 AND organization_id = $5 AND status = $6)
		   AND EXISTS (SELECT 1 FROM branches WHERE id = $4 AND project_id = $3 AND status = $7)
		 RETURNING id, github_repository_id, project_id, root_branch_id, created_at, updated_at`,
		idgen.Generate(), repoID, project, rootBranchID, organization, StatusActive, StatusActive).Scan(
		&mapping.ID, &mapping.GithubRepositoryID,
		&mapping.Project, &mapping.RootBranchID,
		&mapping.CreatedAt, &mapping.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			var exists bool
			if err := s.sql.QueryRowContext(ctx,
				`SELECT EXISTS (SELECT 1 FROM projects WHERE id = $1 AND organization_id = $2 AND status = $3)`,
				project, organization, StatusActive,
			).Scan(&exists); err != nil {
				return nil, err
			}
			if !exists {
				return nil, store.ErrProjectNotFound{ID: project}
			}
			if err := s.sql.QueryRowContext(ctx,
				`SELECT EXISTS (SELECT 1 FROM branches WHERE id = $1 AND project_id = $2 AND status = $3)`,
				rootBranchID, project, StatusActive,
			).Scan(&exists); err != nil {
				return nil, err
			}
			if !exists {
				return nil, store.ErrBranchNotFound{ID: rootBranchID}
			}
			return nil, fmt.Errorf("create github repo mapping: %w", err)
		}
		if IsConstraintError(err, "github_repositories_unique_project_id") {
			return nil, store.ErrGithubRepoMappingAlreadyExists{Organization: organization, Project: project}
		}
		if IsConstraintError(err, "github_repositories_unique_repo_id") {
			return nil, store.ErrGithubRepositoryAlreadyMapped{RepositoryID: repoID}
		}
		return nil, err
	}
	return &mapping, nil
}

func (s *sqlProjectStore) GetGithubRepoMappingByProject(ctx context.Context, organization, project string) (*store.GithubRepoMapping, error) {
	var mapping store.GithubRepoMapping
	err := s.sql.QueryRowContext(ctx,
		`SELECT m.id, m.github_repository_id, m.project_id, m.root_branch_id, m.created_at, m.updated_at
		 FROM github_repositories m
		 JOIN projects p ON p.id = m.project_id
		 WHERE m.project_id = $1 AND p.organization_id = $2 AND p.status = $3`,
		project, organization, StatusActive).Scan(
		&mapping.ID, &mapping.GithubRepositoryID,
		&mapping.Project, &mapping.RootBranchID,
		&mapping.CreatedAt, &mapping.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			var exists bool
			if err := s.sql.QueryRowContext(ctx,
				`SELECT EXISTS (SELECT 1 FROM projects WHERE id = $1 AND organization_id = $2 AND status = $3)`,
				project, organization, StatusActive,
			).Scan(&exists); err != nil {
				return nil, err
			}
			if !exists {
				return nil, store.ErrProjectNotFound{ID: project}
			}
			return nil, store.ErrGithubRepoMappingNotFound{Organization: organization, Project: project}
		}
		return nil, err
	}
	return &mapping, nil
}

func (s *sqlProjectStore) UpdateGithubRepoMapping(ctx context.Context, organization, project string, repoID int64, rootBranchID string) (*store.GithubRepoMapping, error) {
	var mapping store.GithubRepoMapping
	err := s.sql.QueryRowContext(ctx,
		`UPDATE github_repositories
		 SET github_repository_id = $1, root_branch_id = $2, updated_at = NOW()
		 WHERE project_id = $3
		   AND EXISTS (SELECT 1 FROM projects WHERE id = $3 AND organization_id = $4 AND status = $5)
		   AND EXISTS (SELECT 1 FROM branches WHERE id = $2 AND project_id = $3 AND status = $6)
		 RETURNING id, github_repository_id, project_id, root_branch_id, created_at, updated_at`,
		repoID, rootBranchID, project, organization, StatusActive, StatusActive).Scan(
		&mapping.ID, &mapping.GithubRepositoryID,
		&mapping.Project, &mapping.RootBranchID,
		&mapping.CreatedAt, &mapping.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			var exists bool
			if err := s.sql.QueryRowContext(ctx,
				`SELECT EXISTS (SELECT 1 FROM projects WHERE id = $1 AND organization_id = $2 AND status = $3)`,
				project, organization, StatusActive,
			).Scan(&exists); err != nil {
				return nil, err
			}
			if !exists {
				return nil, store.ErrProjectNotFound{ID: project}
			}
			if err := s.sql.QueryRowContext(ctx,
				`SELECT EXISTS (
					SELECT 1 FROM github_repositories m
					JOIN projects p ON p.id = m.project_id
					WHERE p.id = $1 AND p.organization_id = $2 AND p.status = $3
				)`,
				project, organization, StatusActive,
			).Scan(&exists); err != nil {
				return nil, err
			}
			if !exists {
				return nil, store.ErrGithubRepoMappingNotFound{Organization: organization, Project: project}
			}
			if err := s.sql.QueryRowContext(ctx,
				`SELECT EXISTS (SELECT 1 FROM branches WHERE id = $1 AND project_id = $2 AND status = $3)`,
				rootBranchID, project, StatusActive,
			).Scan(&exists); err != nil {
				return nil, err
			}
			if !exists {
				return nil, store.ErrBranchNotFound{ID: rootBranchID}
			}
			return nil, fmt.Errorf("update github repo mapping: %w", err)
		}
		if IsConstraintError(err, "github_repositories_unique_repo_id") {
			return nil, store.ErrGithubRepositoryAlreadyMapped{RepositoryID: repoID}
		}
		return nil, err
	}
	return &mapping, nil
}

func (s *sqlProjectStore) DeleteGithubRepoMapping(ctx context.Context, organization, project string) error {
	result, err := s.sql.ExecContext(ctx,
		`DELETE FROM github_repositories m
		 USING projects p
		 WHERE m.project_id = p.id AND p.id = $1 AND p.organization_id = $2`,
		project, organization)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		var exists bool
		if err := s.sql.QueryRowContext(ctx,
			`SELECT EXISTS (SELECT 1 FROM projects WHERE id = $1 AND organization_id = $2)`,
			project, organization,
		).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return store.ErrProjectNotFound{ID: project}
		}
		return store.ErrGithubRepoMappingNotFound{Organization: organization, Project: project}
	}
	return nil
}

func (s *sqlProjectStore) GetGithubRepoMappingByRepoID(ctx context.Context, repoID int64) (*store.GithubRepoMappingWithOrg, error) {
	var mapping store.GithubRepoMappingWithOrg
	err := s.sql.QueryRowContext(ctx,
		`SELECT m.id, m.github_repository_id, m.project_id, m.root_branch_id, m.created_at, m.updated_at, p.organization_id
		 FROM github_repositories m
		 JOIN projects p ON p.id = m.project_id
		 WHERE m.github_repository_id = $1 AND p.status = $2`,
		repoID, StatusActive).Scan(
		&mapping.ID, &mapping.GithubRepositoryID,
		&mapping.Project, &mapping.RootBranchID,
		&mapping.CreatedAt, &mapping.UpdatedAt,
		&mapping.OrganizationID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrGithubRepoMappingNotFound{RepoID: repoID}
		}
		return nil, err
	}
	return &mapping, nil
}

func (s *sqlProjectStore) DeleteGithubInstallation(ctx context.Context, installationID int64) error {
	_, err := s.sql.ExecContext(ctx,
		`DELETE FROM github_installations WHERE installation_id = $1`,
		installationID)
	if err != nil {
		return fmt.Errorf("delete installation %d: %w", installationID, err)
	}
	return nil
}

func decodeLimits[K ~string](raw []byte) (map[K]any, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var m map[K]any
	if err := d.Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

// GetOrgLimits returns stored limit overrides for the given org and project, with
// project-level overrides taking precedence over org-level overrides.
func (s *sqlProjectStore) GetOrgLimits(ctx context.Context, orgID, projectID string) (map[store.LimitKey]any, error) {
	rows, err := s.sql.QueryContext(ctx, `
		SELECT project_id, limits
		FROM organization_limits
		WHERE organization_id = $1 AND project_id IN ('', $2)
	`, orgID, projectID)
	if err != nil {
		return nil, fmt.Errorf("query org limits: %w", err)
	}
	defer rows.Close()

	orgLimits := make(map[store.LimitKey]any)
	projectLimits := make(map[store.LimitKey]any)
	for rows.Next() {
		var pid string
		var raw []byte
		if err := rows.Scan(&pid, &raw); err != nil {
			return nil, fmt.Errorf("scan org limit: %w", err)
		}
		m, err := decodeLimits[store.LimitKey](raw)
		if err != nil {
			return nil, fmt.Errorf("unmarshal org limits: %w", err)
		}
		if pid == "" {
			orgLimits = m
		} else {
			projectLimits = m
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate org limits: %w", err)
	}

	maps.Copy(orgLimits, projectLimits)
	return orgLimits, nil
}

// SetOrgLimit upserts an override for a single limit at the org or project level.
func (s *sqlProjectStore) SetOrgLimit(ctx context.Context, orgID, projectID string, key store.LimitKey, value any) error {
	if !key.IsValid() {
		return fmt.Errorf("unknown limit key %q", key)
	}
	valueJSON, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal limit value: %w", err)
	}
	_, err = s.sql.ExecContext(ctx, `
		INSERT INTO organization_limits (organization_id, project_id, limits)
		VALUES ($1, $2, jsonb_build_object($3::text, $4::jsonb))
		ON CONFLICT (organization_id, project_id) DO UPDATE
		SET limits = organization_limits.limits || jsonb_build_object($3::text, $4::jsonb)
	`, orgID, projectID, key, valueJSON)
	if err != nil {
		return fmt.Errorf("set org limit: %w", err)
	}
	return nil
}

// DeleteOrgLimit removes an override for a single limit at the org or project level.
func (s *sqlProjectStore) DeleteOrgLimit(ctx context.Context, orgID, projectID string, key store.LimitKey) error {
	_, err := s.sql.ExecContext(ctx, `
		UPDATE organization_limits
		SET limits = limits - $3::text
		WHERE organization_id = $1 AND project_id = $2
	`, orgID, projectID, key)
	if err != nil {
		return fmt.Errorf("delete org limit: %w", err)
	}
	return nil
}
