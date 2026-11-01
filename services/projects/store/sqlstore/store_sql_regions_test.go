package sqlstore

import (
	"context"
	"testing"
	"time"

	"xata/services/projects/store"

	"github.com/stretchr/testify/require"
)

func TestSQLStoreRegions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sqlStore := setupSQLStore(ctx, t, maxDepth)

	testRegionID := "test-region"
	testOrganizationRegionID := "test-organization-region"
	testCellID := "test-cell"
	testOrganizationCellID := "test-organization-cell"
	testOrganizationID := "test-organization"
	testGRPCURL := "grpc://localhost:50051"

	// no regions
	regions, err := sqlStore.ListRegions(ctx, testOrganizationID)
	require.NoError(t, err)
	require.Empty(t, regions)

	// region available to everyone
	region, err := sqlStore.CreateRegion(ctx, testRegionID, store.RegionFlags{PublicAccess: true, BackupsEnabled: true, Provider: store.ProviderAWS}, "custom.domain.tld:1234")
	require.NoError(t, err)
	require.Equal(t, testRegionID, region.ID)
	require.Equal(t, (*string)(nil), region.OrganizationID)
	require.Equal(t, true, region.PublicAccess)
	require.Equal(t, true, region.BackupsEnabled)
	require.Equal(t, store.ProviderAWS, region.Provider)
	require.Equal(t, "custom.domain.tld:1234", region.GatewayHostPort)

	gotRegion, err := sqlStore.GetRegion(ctx, testOrganizationID, testRegionID)
	require.NoError(t, err)
	require.Equal(t, region, gotRegion)

	regions, err = sqlStore.ListRegions(ctx, testOrganizationID)
	require.NoError(t, err)
	require.ElementsMatch(t, regions, []store.Region{*region})

	// region available to organization only
	orgRegion, err := sqlStore.CreateOrganizationRegion(ctx, testOrganizationID, testOrganizationRegionID, store.RegionFlags{PublicAccess: false, BackupsEnabled: true, Provider: store.ProviderCustom}, "")
	require.NoError(t, err)
	require.Equal(t, testOrganizationRegionID, orgRegion.ID)
	require.Equal(t, &testOrganizationID, orgRegion.OrganizationID)
	require.Equal(t, false, orgRegion.PublicAccess)
	require.Equal(t, true, orgRegion.BackupsEnabled)
	require.Equal(t, store.ProviderCustom, orgRegion.Provider)

	regions, err = sqlStore.ListRegions(ctx, testOrganizationID)
	require.NoError(t, err)
	require.ElementsMatch(t, regions, []store.Region{*region, *orgRegion})

	regions, err = sqlStore.ListRegions(ctx, "other")
	require.NoError(t, err)
	require.ElementsMatch(t, regions, []store.Region{*region})

	// Test creating region with backups disabled
	regionNoBackups, err := sqlStore.CreateRegion(ctx, "no-backups-region", store.RegionFlags{PublicAccess: false, BackupsEnabled: false, Provider: store.ProviderAWS}, "")
	require.NoError(t, err)
	require.Equal(t, "no-backups-region", regionNoBackups.ID)
	require.Equal(t, false, regionNoBackups.PublicAccess)
	require.Equal(t, false, regionNoBackups.BackupsEnabled)

	// Test creating region with a different provider
	gcpRegion, err := sqlStore.CreateRegion(ctx, "gcp-region", store.RegionFlags{PublicAccess: true, BackupsEnabled: true, Provider: store.ProviderGCP}, "")
	require.NoError(t, err)
	require.Equal(t, store.ProviderGCP, gcpRegion.Provider)

	// invalid provider is rejected
	_, err = sqlStore.CreateRegion(ctx, "invalid-provider-region", store.RegionFlags{Provider: "digitalocean"}, "")
	require.Error(t, err)
	require.ErrorContains(t, err, "unknown provider")

	// provider is required
	_, err = sqlStore.CreateRegion(ctx, "missing-provider-region", store.RegionFlags{}, "")
	require.Error(t, err)
	require.ErrorContains(t, err, "unknown provider")

	// duplicate region cannot be created
	_, err = sqlStore.CreateRegion(ctx, testRegionID, store.RegionFlags{PublicAccess: true, BackupsEnabled: true, Provider: store.ProviderAWS}, "")
	require.Error(t, err)
	require.ErrorAs(t, err, &store.ErrRegionAlreadyExists{})

	_, err = sqlStore.CreateOrganizationRegion(ctx, "other", testOrganizationRegionID, store.RegionFlags{PublicAccess: false, BackupsEnabled: true, Provider: store.ProviderAWS}, "")
	require.Error(t, err)
	require.ErrorAs(t, err, &store.ErrRegionAlreadyExists{})

	// list cells
	cells, err := sqlStore.ListCells(ctx, testOrganizationID, testRegionID)
	require.NoError(t, err)
	require.Empty(t, cells)

	// create cell in public region
	cell, err := sqlStore.CreateCell(ctx, testRegionID, testCellID, testGRPCURL, nil)
	require.NoError(t, err)
	require.Equal(t, testCellID, cell.ID)
	require.Equal(t, testRegionID, cell.RegionID)
	require.Equal(t, testGRPCURL, cell.ClustersGRPCURL)

	cells, err = sqlStore.ListCells(ctx, testOrganizationID, testRegionID)
	require.NoError(t, err)
	require.ElementsMatch(t, cells, []store.Cell{*cell})

	// cell names must be unique
	_, err = sqlStore.CreateCell(ctx, testRegionID, testCellID, testGRPCURL, nil)
	require.Error(t, err)
	require.ErrorAs(t, err, &store.ErrCellAlreadyExists{})

	// create cell in organization region
	orgCell, err := sqlStore.CreateCell(ctx, testOrganizationRegionID, testOrganizationCellID, testGRPCURL, nil)
	require.NoError(t, err)
	require.Equal(t, testOrganizationCellID, orgCell.ID)
	require.Equal(t, testOrganizationRegionID, orgCell.RegionID)
	require.Equal(t, testGRPCURL, orgCell.ClustersGRPCURL)

	cells, err = sqlStore.ListCells(ctx, testOrganizationID, testOrganizationRegionID)
	require.NoError(t, err)
	require.ElementsMatch(t, cells, []store.Cell{*orgCell})

	cells, err = sqlStore.ListCells(ctx, "other", testOrganizationRegionID)
	require.NoError(t, err)
	require.Empty(t, cells)

	// create a project & branches in both regions
	project, err := sqlStore.CreateProject(ctx, testOrganizationID, createProjectConfig("test", nil))
	require.NoError(t, err)

	// create a branch in the public region
	branch, err := sqlStore.CreateBranch(ctx, testOrganizationID, project.ID, testCellID, createBranchConfig("test-branch", nil, nil), func(b *store.Branch, _ *store.Project) error {
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, testCellID, branch.CellID)
	require.Equal(t, testRegionID, branch.Region)
	require.Equal(t, true, branch.PublicAccess)

	orgRegionBranch, err := sqlStore.CreateBranch(ctx, testOrganizationID, project.ID, testOrganizationCellID, createBranchConfig("test-branch-org", nil, nil), func(b *store.Branch, _ *store.Project) error {
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, testOrganizationCellID, orgRegionBranch.CellID)
	require.Equal(t, testOrganizationRegionID, orgRegionBranch.Region)
	require.Equal(t, false, orgRegionBranch.PublicAccess)

	// list branches
	branches, err := sqlStore.ListBranches(ctx, testOrganizationID, project.ID)
	require.NoError(t, err)
	require.Len(t, branches, 2)
	require.Equal(t, testRegionID, branches[0].Region)
	require.Equal(t, true, branches[0].PublicAccess)
	require.Equal(t, testOrganizationRegionID, branches[1].Region)
	require.Equal(t, false, branches[1].PublicAccess)

	// describe branches
	gotBranch, err := sqlStore.DescribeBranch(ctx, testOrganizationID, project.ID, branch.ID)
	require.NoError(t, err)
	require.Equal(t, branch.ID, gotBranch.ID)
	require.Equal(t, testRegionID, gotBranch.Region)
	require.Equal(t, true, gotBranch.PublicAccess)

	gotOrgRegionBranch, err := sqlStore.DescribeBranch(ctx, testOrganizationID, project.ID, orgRegionBranch.ID)
	require.NoError(t, err)
	require.Equal(t, orgRegionBranch.ID, gotOrgRegionBranch.ID)
	require.Equal(t, testOrganizationRegionID, gotOrgRegionBranch.Region)
	require.Equal(t, false, gotOrgRegionBranch.PublicAccess)

	// delete branches
	err = sqlStore.DeleteBranch(ctx, testOrganizationID, project.ID, branch.ID, func(*store.Branch) error { return nil })
	require.NoError(t, err)

	err = sqlStore.DeleteBranch(ctx, testOrganizationID, project.ID, orgRegionBranch.ID, func(*store.Branch) error { return nil })
	require.NoError(t, err)

	// get cell
	gotCell, err := sqlStore.GetCell(ctx, testOrganizationID, testCellID)
	require.NoError(t, err)
	require.Equal(t, cell, gotCell)

	gotOrgCell, err := sqlStore.GetCell(ctx, testOrganizationID, testOrganizationCellID)
	require.NoError(t, err)
	require.Equal(t, orgCell, gotOrgCell)

	// get cell not found
	gotCell, err = sqlStore.GetCell(ctx, testOrganizationID, "not-found")
	require.Error(t, err)
	require.Nil(t, gotCell)
	require.ErrorAs(t, err, &store.ErrCellNotFound{})

	// cannot delete regions with cells
	err = sqlStore.DeleteRegion(ctx, testRegionID)
	require.Error(t, err)

	err = sqlStore.DeleteRegion(ctx, testOrganizationRegionID)
	require.Error(t, err)

	err = sqlStore.DeleteRegion(ctx, "not-found")
	require.Error(t, err)

	// Cannot delete cell with soft-deleted (terminated) branches
	err = sqlStore.DeleteCell(ctx, testCellID)
	require.Error(t, err)

	// Clean up terminated branches
	_, err = sqlStore.CleanupTerminatedBranches(ctx, testCellID, time.Duration(0))
	require.NoError(t, err)
	_, err = sqlStore.CleanupTerminatedBranches(ctx, testOrganizationCellID, time.Duration(0))
	require.NoError(t, err)

	// delete cells
	err = sqlStore.DeleteCell(ctx, testCellID)
	require.NoError(t, err)
	err = sqlStore.DeleteCell(ctx, testOrganizationCellID)
	require.NoError(t, err)

	// delete cells not found
	err = sqlStore.DeleteCell(ctx, "not-found")
	require.Error(t, err)
	require.ErrorAs(t, err, &store.ErrCellNotFound{})

	// delete regions
	err = sqlStore.DeleteRegion(ctx, testRegionID)
	require.NoError(t, err)
	err = sqlStore.DeleteRegion(ctx, testOrganizationRegionID)
	require.NoError(t, err)

	// delete regions not found
	err = sqlStore.DeleteRegion(ctx, "not-found")
	require.Error(t, err)
	require.ErrorAs(t, err, &store.ErrRegionNotFound{})
}

func TestCellSubdomain(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sqlStore := setupSQLStore(ctx, t, maxDepth)

	testRegionID := "test-region"
	testOrganizationID := "test-organization"
	testGRPCURL := "grpc://localhost:50051"

	_, err := sqlStore.CreateRegion(ctx, testRegionID, store.RegionFlags{PublicAccess: true, BackupsEnabled: true, Provider: store.ProviderAWS}, "")
	require.NoError(t, err)

	// a cell created without a subdomain round-trips it as nil
	plainCell, err := sqlStore.CreateCell(ctx, testRegionID, "plain-cell", testGRPCURL, nil)
	require.NoError(t, err)
	require.Nil(t, plainCell.Subdomain)

	// a cell created with a subdomain round-trips it on every read path
	subdomain := "cell-2"
	cell, err := sqlStore.CreateCell(ctx, testRegionID, "subdomain-cell", testGRPCURL, &subdomain)
	require.NoError(t, err)
	require.NotNil(t, cell.Subdomain)
	require.Equal(t, subdomain, *cell.Subdomain)

	gotCell, err := sqlStore.GetCell(ctx, "", cell.ID)
	require.NoError(t, err)
	require.Equal(t, cell.Subdomain, gotCell.Subdomain)

	cells, err := sqlStore.ListCells(ctx, testOrganizationID, testRegionID)
	require.NoError(t, err)
	require.ElementsMatch(t, []store.Cell{*plainCell, *cell}, cells)

	allCells, err := sqlStore.ListAllCells(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []store.Cell{*plainCell, *cell}, allCells)

	// Ensure that only valid subdomain labels are accepted
	for _, invalid := range []string{"Not.A.Label", "UPPER", "-leading-hyphen", "trailing-hyphen-", ""} {
		_, err := sqlStore.CreateCell(ctx, testRegionID, "invalid-subdomain-cell", testGRPCURL, &invalid)
		require.Error(t, err, "subdomain %q should be rejected", invalid)
		require.ErrorContains(t, err, "cells_subdomain_label")
	}
}
