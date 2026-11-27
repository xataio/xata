package api

import (
	"errors"
	"net/http"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clustersv1 "xata/gen/proto/clusters/v1"
	"xata/gen/protomocks"
	analyticsmocks "xata/internal/analytics/client/mocks"
	"xata/internal/api"
	"xata/internal/apitest"
	openfeaturetest "xata/internal/openfeature/client/mocks"
	"xata/internal/postgrescfg"
	postgrescfgmocks "xata/internal/postgrescfg/mocks"
	"xata/services/projects/api/spec"
	"xata/services/projects/cells/cellsmock"
	"xata/services/projects/scheduler"
	"xata/services/projects/scheduler/strategy"
	"xata/services/projects/store"
	"xata/services/projects/store/mocks"
)

// Credentials as cells report them: an older cell reports no database, a
// newer cell reports the database and the login of the branch.
var (
	credsOlderCell = &clustersv1.GetPostgresClusterCredentialsResponse{Username: "xata", Password: "pass"}
	credsXata      = &clustersv1.GetPostgresClusterCredentialsResponse{Username: "xata", Password: "pass", Database: "xata"}
	credsOther     = &clustersv1.GetPostgresClusterCredentialsResponse{Username: "dbadmin", Password: "pass", Database: "defaultdb"}
)

var credsRequest = &clustersv1.GetPostgresClusterCredentialsRequest{Id: "branchID", Username: "app"}

func TestGetBranchCredentialsBranchLogin(t *testing.T) {
	tests := map[string]struct {
		creds    *clustersv1.GetPostgresClusterCredentialsResponse
		username *string
		// lookups is how many times the handler reads the branch and its
		// credentials.
		lookups     int
		wantErr     string
		wantLogin   string
		wantDB      string
		wantConnStr string
	}{
		"older cell, no username": {
			creds: credsOlderCell, lookups: 1,
			wantLogin: "xata", wantDB: "xata", wantConnStr: "postgresql://xata:pass@branchID.testdomain/xata",
		},
		"older cell, xata": {
			creds: credsOlderCell, username: new("xata"), lookups: 1,
			wantLogin: "xata", wantDB: "xata", wantConnStr: "postgresql://xata:pass@branchID.testdomain/xata",
		},
		"older cell, other username": {
			creds: credsOlderCell, username: new("dbadmin"), lookups: 1,
			wantErr: "Branch [branchID]: invalid parameter [username]: only the xata user credentials can be retrieved",
		},
		"older cell does not report another login": {
			creds:    &clustersv1.GetPostgresClusterCredentialsResponse{Username: "user", Password: "pass"},
			username: new("user"), lookups: 1,
			wantErr: "Branch [branchID]: invalid parameter [username]: only the xata user credentials can be retrieved",
		},
		"xata branch, xata": {
			creds: credsXata, username: new("xata"), lookups: 1,
			wantLogin: "xata", wantDB: "xata", wantConnStr: "postgresql://xata:pass@branchID.testdomain/xata",
		},
		"xata branch, other username": {
			creds: credsXata, username: new("dbadmin"), lookups: 1,
			wantErr: "Branch [branchID]: invalid parameter [username]: only the xata user credentials can be retrieved",
		},
		"other login, no username": {
			creds: credsOther, lookups: 1,
			wantLogin: "dbadmin", wantDB: "defaultdb", wantConnStr: "postgresql://dbadmin:pass@branchID.testdomain/defaultdb",
		},
		"other login, its username": {
			creds: credsOther, username: new("dbadmin"), lookups: 2,
			wantLogin: "dbadmin", wantDB: "defaultdb", wantConnStr: "postgresql://dbadmin:pass@branchID.testdomain/defaultdb",
		},
		"other login, wrong username": {
			creds: credsOther, username: new("postgres"), lookups: 1,
			wantErr: "Branch [branchID]: invalid parameter [username]: only the dbadmin user credentials can be retrieved",
		},
		// xata is accepted without a lookup, as before, and selects the login
		// of the branch.
		"other login, xata": {
			creds: credsOther, username: new("xata"), lookups: 1,
			wantLogin: "dbadmin", wantDB: "defaultdb", wantConnStr: "postgresql://dbadmin:pass@branchID.testdomain/defaultdb",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			mockStore := mocks.NewProjectsStore(t)
			mockClusters := protomocks.NewClustersServiceClient(t)
			sched := &scheduler.Scheduler{DefaultStrategy: &strategy.Random{}}
			h := NewAPIHandler(openfeaturetest.NewClient(nil), mockStore, cellsmock.NewCellsMock(t, mockClusters), "testdomain:5432", nil, sched, analyticsmocks.NewClient(t), nil, nil, nil)
			e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

			branch := store.Branch{ID: "branchID"}
			mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", "branchID").Return(&branch, nil).Times(tc.lookups)
			mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, credsRequest).Return(tc.creds, nil).Times(tc.lookups)
			if tc.wantErr == "" {
				mockStore.EXPECT().GetCell(mock.Anything, mock.Anything, mock.Anything).Return(&store.Cell{ID: "cell_id"}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, branch.Region).Return(&store.Region{}, nil).Once()
			}

			c, rec := e.GET("/organizations/" + apitest.TestOrganization + "/projects/project_id/branches/branchID/credentials").Context()
			err := h.GetBranchCredentials(c, apitest.TestOrganization, "project_id", "branchID", spec.GetBranchCredentialsParams{Username: tc.username})
			if tc.wantErr != "" {
				require.Error(t, err)
				require.Equal(t, http.StatusBadRequest, api.GetErrorStatusCode(err))
				require.Equal(t, tc.wantErr, err.Error())
				return
			}
			require.NoError(t, err)
			rec.MustCode(http.StatusOK)

			var resp spec.BranchCredentials
			rec.ReadBody(&resp)
			assert.Equal(t, spec.BranchCredentials{
				Username:         tc.wantLogin,
				Password:         "pass",
				Hostname:         "branchID.testdomain",
				Port:             5432,
				Dbname:           tc.wantDB,
				ConnectionString: tc.wantConnStr,
			}, resp)
		})
	}
}

func TestRotateBranchCredentialsBranchLogin(t *testing.T) {
	tests := map[string]struct {
		username string
		// creds is the answer of the cell to the login lookup, nil if the
		// handler must not ask.
		creds     *clustersv1.GetPostgresClusterCredentialsResponse
		rotateErr error
		wantErr   string
	}{
		"xata rotates without a lookup": {
			username: "xata",
		},
		"older cell, other username": {
			username: "dbadmin", creds: credsOlderCell,
			wantErr: "Branch [branchID]: invalid parameter [username]: only the xata user credentials can be rotated",
		},
		"xata branch, other username": {
			username: "postgres", creds: credsXata,
			wantErr: "Branch [branchID]: invalid parameter [username]: only the xata user credentials can be rotated",
		},
		"other login, its username": {
			username: "dbadmin", creds: credsOther,
		},
		"other login, wrong username": {
			username: "postgres", creds: credsOther,
			wantErr: "Branch [branchID]: invalid parameter [username]: only the dbadmin user credentials can be rotated",
		},
		"other login, xata is refused by the cell": {
			username:  "xata",
			rotateErr: status.Error(codes.InvalidArgument, `unknown user "xata"`),
			wantErr:   `Branch [branchID]: invalid parameter [username]: unknown user "xata"`,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			mockStore := mocks.NewProjectsStore(t)
			mockClusters := protomocks.NewClustersServiceClient(t)
			sched := &scheduler.Scheduler{DefaultStrategy: &strategy.Random{}}
			h := NewAPIHandler(openfeaturetest.NewClient(nil), mockStore, cellsmock.NewCellsMock(t, mockClusters), "testdomain:5432", nil, sched, analyticsmocks.NewClient(t), nil, nil, nil)
			e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

			rotates := tc.wantErr == "" || tc.rotateErr != nil
			lookups := 0
			if tc.creds != nil {
				lookups++
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, credsRequest).Return(tc.creds, nil).Once()
			}
			if rotates {
				lookups++
				mockClusters.EXPECT().
					RotatePostgresClusterCredentials(mock.Anything, &clustersv1.RotatePostgresClusterCredentialsRequest{Id: "branchID", User: tc.username}).
					Return(&clustersv1.RotatePostgresClusterCredentialsResponse{}, tc.rotateErr).Once()
			}
			mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", "branchID").Return(&store.Branch{ID: "branchID"}, nil).Times(lookups)

			c, rec := e.POST("/organizations/" + apitest.TestOrganization + "/projects/project_id/branches/branchID/credentials/rotate").
				WithJSONBody(map[string]string{"username": tc.username}).Context()
			err := h.RotateBranchCredentials(c, apitest.TestOrganization, "project_id", "branchID")
			if tc.wantErr != "" {
				require.Error(t, err)
				require.Equal(t, http.StatusBadRequest, api.GetErrorStatusCode(err))
				require.Equal(t, tc.wantErr, err.Error())
				return
			}
			require.NoError(t, err)
			rec.MustCode(http.StatusNoContent)
		})
	}
}

// A failed login lookup must not answer as if the login were xata.
func TestBranchLoginLookupFails(t *testing.T) {
	cellErr := status.Error(codes.Unavailable, "cell unavailable")
	username := "dbadmin"

	tests := map[string]func(h spec.ServerInterface, c echo.Context) error{
		"get credentials": func(h spec.ServerInterface, c echo.Context) error {
			return h.GetBranchCredentials(c, apitest.TestOrganization, "project_id", "branchID", spec.GetBranchCredentialsParams{Username: &username})
		},
		"rotate credentials": func(h spec.ServerInterface, c echo.Context) error {
			return h.RotateBranchCredentials(c, apitest.TestOrganization, "project_id", "branchID")
		},
	}

	for name, call := range tests {
		t.Run(name, func(t *testing.T) {
			mockStore := mocks.NewProjectsStore(t)
			mockClusters := protomocks.NewClustersServiceClient(t)
			sched := &scheduler.Scheduler{DefaultStrategy: &strategy.Random{}}
			h := NewAPIHandler(openfeaturetest.NewClient(nil), mockStore, cellsmock.NewCellsMock(t, mockClusters), "testdomain:5432", nil, sched, analyticsmocks.NewClient(t), nil, nil, nil)
			e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

			mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", "branchID").Return(&store.Branch{ID: "branchID"}, nil).Once()
			mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, credsRequest).Return(nil, cellErr).Once()

			c, _ := e.POST("/organizations/" + apitest.TestOrganization + "/projects/project_id/branches/branchID/credentials/rotate").
				WithJSONBody(map[string]string{"username": username}).Context()
			err := call(h, c)
			require.ErrorIs(t, err, cellErr)
		})
	}
}

func TestDescribeBranchOtherLogin(t *testing.T) {
	mockStore := mocks.NewProjectsStore(t)
	mockStore.EXPECT().GetCell(mock.Anything, mock.Anything, mock.Anything).Return(&store.Cell{ID: "cell_id"}, nil).Maybe()
	mockClusters := protomocks.NewClustersServiceClient(t)
	mockPostgresConfig := postgrescfgmocks.NewPostgresConfigProvider(t)
	sched := &scheduler.Scheduler{DefaultStrategy: &strategy.Random{}}
	h := NewAPIHandler(openfeaturetest.NewClient(nil), mockStore, cellsmock.NewCellsMock(t, mockClusters), "testdomain:5432", nil, sched, analyticsmocks.NewClient(t), mockPostgresConfig, nil, nil)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

	branch := store.Branch{ID: "branchID", Name: "test", Region: "region-id-1"}
	region := &store.Region{ID: "region-id-1", GatewayHostPort: "footest.tld:1234"}
	preload := []string{"pgprivs", "pg_stat_statements"}

	mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", branch.ID).Return(&branch, nil).Once()
	mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: branch.ID}).Return(&clustersv1.DescribePostgresClusterResponse{
		Id: branch.ID,
		Configuration: &clustersv1.ClusterConfiguration{
			NumInstances:     1,
			StorageSize:      defaultStorage,
			VcpuRequest:      "500m",
			VcpuLimit:        "2",
			Memory:           "2",
			ImageName:        testImageURL,
			PreloadLibraries: preload,
		},
		Status: &clustersv1.ClusterStatus{
			Status:             apiv1.PhaseHealthy,
			StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
			InstanceCount:      1,
			InstanceReadyCount: 1,
			Instances:          map[string]*clustersv1.InstanceStatus{"1": {Status: apiv1.PodHealthy, Primary: true}},
		},
	}, nil).Once()
	mockPostgresConfig.EXPECT().FilterConfigurableParameters(mock.Anything, mock.AnythingOfType("int"), testImageURL, preload).Return(map[string]string{}).Once()
	mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, credsRequest).Return(credsOther, nil).Once()
	mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, branch.Region).Return(region, nil).Once()
	mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, region.ID).Return([]store.InstanceType{
		{Name: "small", VCPUsRequest: 500, VCPUsLimit: 2000, RAM: 2},
	}, nil).Once()

	c, rec := e.GET("/organizations/" + apitest.TestOrganization + "/projects/project_id/branches/" + branch.ID).Context()
	require.NoError(t, h.DescribeBranch(c, apitest.TestOrganization, "project_id", branch.ID))
	rec.MustCode(http.StatusOK)

	var got spec.BranchMetadata
	rec.ReadBody(&got)
	assert.Equal(t, "postgresql://dbadmin:pass@branchID-deprecated.footest.tld:1234/defaultdb?sslmode=require", *got.ConnectionString)
	assert.Equal(t, []string{"pg_stat_statements"}, *got.Configuration.PreloadLibraries)
}

func TestGetBranchPostgresConfigBranchDefaults(t *testing.T) {
	tests := map[string]struct {
		creds    *clustersv1.GetPostgresClusterCredentialsResponse
		credsErr error
		wantDB   string
		wantRole string
	}{
		"older cell keeps the catalog defaults":  {creds: credsOlderCell, wantDB: "xata", wantRole: "xata"},
		"failed call keeps the catalog defaults": {credsErr: errors.New("cell unavailable"), wantDB: "xata", wantRole: "xata"},
		"xata branch":                            {creds: credsXata, wantDB: "xata", wantRole: "xata"},
		"other login and database":               {creds: credsOther, wantDB: "defaultdb", wantRole: "dbadmin"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			mockStore := mocks.NewProjectsStore(t)
			mockClusters := protomocks.NewClustersServiceClient(t)
			mockPostgresConfig := postgrescfgmocks.NewPostgresConfigProvider(t)
			sched := &scheduler.Scheduler{DefaultStrategy: &strategy.Random{}}
			h := NewAPIHandler(openfeaturetest.NewClient(nil), mockStore, cellsmock.NewCellsMock(t, mockClusters), "", nil, sched, analyticsmocks.NewClient(t), mockPostgresConfig, nil, nil)
			e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

			branch := &store.Branch{ID: "branchID", CellID: "test-cell", Region: "us-east-1"}
			mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", "branchID").Return(branch, nil).Once()
			mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: "branchID"}).Return(&clustersv1.DescribePostgresClusterResponse{
				Configuration: &clustersv1.ClusterConfiguration{VcpuRequest: "500m", VcpuLimit: "2000m", Memory: "2"},
			}, nil).Once()
			mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "us-east-1").Return([]store.InstanceType{
				{Name: "small", VCPUsRequest: 500, VCPUsLimit: 2000, RAM: 2},
			}, nil).Once()
			mockPostgresConfig.EXPECT().GetConfigurableParameters(mock.Anything, mock.Anything, mock.Anything).Return(postgrescfg.ParametersMap{
				"cron.database_name":   {DefaultValue: "xata", Extension: "pg_cron"},
				"duckdb.postgres_role": {DefaultValue: "xata", Extension: "pg_duckdb"},
			}).Once()
			mockPostgresConfig.EXPECT().GetParametersSpec("small", mock.Anything, mock.Anything, mock.Anything).Return(postgrescfg.ParametersMap{}, nil).Once()
			mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, credsRequest).Return(tc.creds, tc.credsErr).Once()

			c, rec := e.GET("/organizations/" + apitest.TestOrganization + "/projects/project_id/branches/branchID/postgres-config").Context()
			require.NoError(t, h.GetBranchPostgresConfig(c, apitest.TestOrganization, "project_id", "branchID"))
			rec.MustCode(http.StatusOK)

			var got spec.PostgresConfigDetails
			rec.ReadBody(&got)
			defaults := map[string]string{}
			for _, p := range got.Parameters {
				defaults[p.Name] = p.DefaultValue
			}
			assert.Equal(t, map[string]string{"cron.database_name": tc.wantDB, "duckdb.postgres_role": tc.wantRole}, defaults)
		})
	}
}
