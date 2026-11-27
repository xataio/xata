package api

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clustersv1 "xata/gen/proto/clusters/v1"
	"xata/gen/protomocks"
	analyticsmocks "xata/internal/analytics/client/mocks"
	"xata/internal/api"
	"xata/internal/apitest"
	openfeaturetest "xata/internal/openfeature/client/mocks"
	postgrescfgmocks "xata/internal/postgrescfg/mocks"
	"xata/internal/postgresversions"
	postgresversionsmocks "xata/internal/postgresversions/mocks"
	"xata/services/clusters"
	"xata/services/projects/cells/cellsmock"
	"xata/services/projects/provisioner"
	"xata/services/projects/scheduler"
	"xata/services/projects/scheduler/strategy"
	"xata/services/projects/store"
	"xata/services/projects/store/mocks"
)

// imageUnavailableErr is the error a cell returns when it does not offer the
// requested image.
func imageUnavailableErr(t *testing.T, msg string) error {
	t.Helper()
	st, err := status.New(codes.FailedPrecondition, msg).
		WithDetails(&errdetails.ErrorInfo{Reason: clusters.ImageUnavailableReason, Domain: "xata"})
	require.NoError(t, err)
	return st.Err()
}

func TestBranchImageUnavailable(t *testing.T) {
	tests := map[string]struct {
		cellErr     error
		wantStatus  int
		wantMessage string // checked for 4xx only: 5xx messages are not shown
	}{
		"image refused by the cell is a client error": {
			cellErr:     imageUnavailableErr(t, "Postgres 17 is not available in this cell"),
			wantStatus:  http.StatusBadRequest,
			wantMessage: "invalid parameter [image]: Postgres 17 is not available in this cell",
		},
		"offering refused by the cell is a client error": {
			cellErr:     imageUnavailableErr(t, "the analytics offering is not available in this cell"),
			wantStatus:  http.StatusBadRequest,
			wantMessage: "invalid parameter [image]: the analytics offering is not available in this cell",
		},
		"other FailedPrecondition stays internal": {
			cellErr:    status.Error(codes.FailedPrecondition, "Postgres 17 is not available in this cell"),
			wantStatus: http.StatusInternalServerError,
		},
	}

	for name, tc := range tests {
		t.Run("create/"+name, func(t *testing.T) {
			mockStore := mocks.NewProjectsStore(t)
			mockStore.EXPECT().GetCell(mock.Anything, mock.Anything, mock.Anything).Return(&store.Cell{ID: "cell_id"}, nil).Maybe()
			mockClusters := protomocks.NewClustersServiceClient(t)
			mockCells := cellsmock.NewCellsMock(t, mockClusters)
			mockPostgresConfig := postgrescfgmocks.NewPostgresConfigProvider(t)
			mockImageProvider := postgresversionsmocks.NewImageProvider(t)
			sched := &scheduler.Scheduler{DefaultStrategy: &strategy.Random{}}
			prov := provisioner.NewBranchProvisioner(mockStore, mockCells)
			h := NewAPIHandler(openfeaturetest.NewClient(nil), mockStore, mockCells, "testdomain:5432", nil, sched, analyticsmocks.NewClient(t), mockPostgresConfig, mockImageProvider, prov)
			e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

			branch := store.Branch{ID: "123", Name: "main", CellID: "cell_id", Region: "region-id-1"}
			project := store.Project{ID: "project_id"}

			mockStore.EXPECT().GetOrgLimits(mock.Anything, apitest.TestOrganization, "project_id").Return(map[store.LimitKey]any{}, nil).Once()
			mockImageProvider.EXPECT().GetAllImageNames().Return([]string{testImage}).Once()
			mockImageProvider.EXPECT().BuildImageURL(testImage).Return(testImageURL).Once()
			mockStore.EXPECT().ListCells(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.Cell{{ID: "cell_id", RegionID: "region-id-1"}}, nil).Once()
			mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1}}, nil).Once()
			mockPostgresConfig.EXPECT().GetDefaultPostgresParameters("xata.micro", mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(map[string]string{}, nil).Once()
			mockPostgresConfig.EXPECT().GetDefaultPreloadLibraries(mock.AnythingOfType("string")).Return([]string{"pg_stat_statements"}, nil).Once()
			mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: "region-id-1", BackupsEnabled: true}, nil).Once()
			mockStore.EXPECT().GetProject(mock.Anything, apitest.TestOrganization, "project_id").Return(&project, nil).Once()
			// The store runs the cell call inside the create transaction and
			// rolls it back on error, so no branch row is left behind.
			mockStore.EXPECT().CreateBranch(mock.Anything, apitest.TestOrganization, "project_id", "cell_id", mock.Anything, mock.Anything).
				RunAndReturn(func(_ context.Context, _, _, _ string, _ *store.CreateBranchConfiguration, provisionFn func(*store.Branch, *store.Project) error) (*store.Branch, error) {
					return nil, provisionFn(&branch, &project)
				}).Once()
			mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, mock.Anything).Return(nil, tc.cellErr).Once()

			c, _ := e.POST("/organizations/" + apitest.TestOrganization + "/projects/project_id/branches").
				WithJSONBody(map[string]any{"name": branch.Name, "mode": "custom", "configuration": map[string]any{"image": testImage, "replicas": 0, "region": "region-id-1", "instanceType": "xata.micro"}}).Context()
			err := h.CreateBranch(c, apitest.TestOrganization, "project_id")
			require.Error(t, err)
			require.Equal(t, tc.wantStatus, api.GetErrorStatusCode(err), "error: %v", err)
			if tc.wantMessage != "" {
				require.Equal(t, "Branch [main]: "+tc.wantMessage, api.ErrorMessage(err))
			}
		})

		t.Run("update/"+name, func(t *testing.T) {
			mockStore := mocks.NewProjectsStore(t)
			mockClusters := protomocks.NewClustersServiceClient(t)
			mockImageProvider := postgresversionsmocks.NewImageProvider(t)
			sched := &scheduler.Scheduler{DefaultStrategy: &strategy.Random{}}
			h := NewAPIHandler(openfeaturetest.NewClient(nil), mockStore, cellsmock.NewCellsMock(t, mockClusters), "testdomain:5432", nil, sched, analyticsmocks.NewClient(t), postgrescfgmocks.NewPostgresConfigProvider(t), mockImageProvider, nil)
			e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

			branch := store.Branch{ID: "123", Name: "main", CellID: "cell_id", Region: "region-id-1"}
			const currentURL = "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5"

			mockStore.EXPECT().GetOrgLimits(mock.Anything, apitest.TestOrganization, "project_id").Return(map[store.LimitKey]any{}, nil).Once()
			mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", updateBranchConfig(nil, nil), mock.Anything).
				RunAndReturn(func(_ context.Context, _, _, _ string, _ *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) (*store.Branch, error) {
					return nil, provisionFn(&branch)
				}).Once()
			mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: "123"}).Return(&clustersv1.DescribePostgresClusterResponse{
				Id:            "123",
				Configuration: &clustersv1.ClusterConfiguration{ImageName: currentURL},
			}, nil).Once()
			mockImageProvider.EXPECT().GetAllImageNames().Return([]string{testImage}).Once()
			mockImageProvider.EXPECT().BuildImageURL(testImage).Return(testImageURL).Once()
			mockImageProvider.EXPECT().ParseImageVersion(testImageURL).Return(&postgresversions.ImageVersion{Offering: "postgres", Major: 17, Minor: 7}, nil).Once()
			mockImageProvider.EXPECT().ParseImageVersion(currentURL).Return(&postgresversions.ImageVersion{Offering: "postgres", Major: 17, Minor: 5}, nil).Once()
			mockClusters.EXPECT().UpdatePostgresCluster(mock.Anything, mock.Anything).Return(nil, tc.cellErr).Once()

			c, _ := e.PATCH("/organizations/" + apitest.TestOrganization + "/projects/project_id/branches/123").
				WithJSONBody(map[string]any{"image": testImage}).Context()
			err := h.UpdateBranch(c, apitest.TestOrganization, "project_id", "123")
			require.Error(t, err)
			require.Equal(t, tc.wantStatus, api.GetErrorStatusCode(err), "error: %v", err)
			if tc.wantMessage != "" {
				require.Equal(t, "Branch [123]: "+tc.wantMessage, api.ErrorMessage(err))
			}
		})
	}
}
