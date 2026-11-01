package provisioner

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	clustersv1 "xata/gen/proto/clusters/v1"
	"xata/gen/protomocks"
	"xata/services/clusters"
	"xata/services/projects/cells/cellsmock"
	"xata/services/projects/store"
	storemocks "xata/services/projects/store/mocks"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	orgID     = "org-1"
	projectID = "project-1"
)

func TestCreateBranch(t *testing.T) {
	project := &store.Project{
		ID:   projectID,
		Name: "test",
		ScaleToZero: store.ProjectScaleToZero{
			BaseBranches:  store.ScaleToZero{Enabled: false, InactivityPeriod: store.InactivityPeriod(30 * time.Minute)},
			ChildBranches: store.ScaleToZero{Enabled: true, InactivityPeriod: store.InactivityPeriod(15 * time.Minute)},
		},
	}
	projectWithIPFiltering := &store.Project{
		ID:   projectID,
		Name: "test",
		IPFiltering: store.IPFiltering{
			Enabled: true,
			CIDRs:   []store.CIDREntry{{CIDR: "10.0.0.0/8"}},
		},
	}

	mainBranch := store.Branch{ID: "branch-1", CellID: "cell-1", Region: "us-east-1"}
	parentID := "parent-1"
	childBranch := store.Branch{ID: "branch-2", ParentID: &parentID, CellID: "cell-1", Region: "us-east-1"}

	basePayload := func() *ClusterServicePayload {
		return &ClusterServicePayload{
			Configuration: clustersv1.ClusterConfiguration{NumInstances: 1, ImageName: "postgres:17"},
			CellID:        "cell-1",
			Region:        "us-east-1",
			BackupConfig:  &clustersv1.BackupConfiguration{BackupsEnabled: true},
		}
	}

	// mockCreateBranch sets up store.CreateBranch to invoke the provision callback with
	// lockedProject (the project the store reads under the lock) and propagate its result.
	mockCreateBranch := func(mockStore *storemocks.ProjectsStore, branch store.Branch, lockedProject *store.Project) {
		b := branch
		var provisionErr error
		mockStore.On("CreateBranch", mock.Anything, orgID, projectID, mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				provisionFn, ok := args.Get(5).(func(*store.Branch, *store.Project) error)
				require.True(t, ok, "provisionFn should be func(*store.Branch, *store.Project) error")
				provisionErr = provisionFn(&b, lockedProject)
			}).
			Return(
				func(_ context.Context, _, _, _ string, _ *store.CreateBranchConfiguration, _ func(*store.Branch, *store.Project) error) *store.Branch {
					if provisionErr != nil {
						return nil
					}
					return &b
				},
				func(_ context.Context, _, _, _ string, _ *store.CreateBranchConfiguration, _ func(*store.Branch, *store.Project) error) error {
					return provisionErr
				},
			).Once()
	}

	tests := map[string]struct {
		payload    func() *ClusterServicePayload
		branch     store.Branch
		setupMocks func(mockStore *storemocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient)
		wantErr    error
	}{
		"main branch succeeds": {
			payload: basePayload,
			branch:  mainBranch,
			setupMocks: func(mockStore *storemocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient) {
				mockStore.EXPECT().GetProject(mock.Anything, orgID, projectID).Return(project, nil).Once()
				mockCreateBranch(mockStore, mainBranch, project)
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, mock.MatchedBy(func(req *clustersv1.CreatePostgresClusterRequest) bool {
					return req.GetIdempotencyKey() != ""
				})).
					Return(&clustersv1.CreatePostgresClusterResponse{}, nil).Once()
			},
		},
		"child branch sets ClusterSnapshot data source": {
			payload: func() *ClusterServicePayload {
				p := basePayload()
				p.ParentID = &parentID
				return p
			},
			branch: childBranch,
			setupMocks: func(mockStore *storemocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient) {
				mockStore.EXPECT().GetProject(mock.Anything, orgID, projectID).Return(project, nil).Once()
				mockCreateBranch(mockStore, childBranch, project)
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, mock.MatchedBy(func(req *clustersv1.CreatePostgresClusterRequest) bool {
					snap, ok := req.DataSource.(*clustersv1.CreatePostgresClusterRequest_ClusterSnapshot)
					return ok && snap.ClusterSnapshot.ClusterId == parentID
				})).Return(&clustersv1.CreatePostgresClusterResponse{}, nil).Once()
			},
		},
		"UsePool flag is forwarded": {
			payload: func() *ClusterServicePayload {
				p := basePayload()
				p.Flags.UsePool = true
				return p
			},
			branch: mainBranch,
			setupMocks: func(mockStore *storemocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient) {
				mockStore.EXPECT().GetProject(mock.Anything, orgID, projectID).Return(project, nil).Once()
				mockCreateBranch(mockStore, mainBranch, project)
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, mock.MatchedBy(func(req *clustersv1.CreatePostgresClusterRequest) bool {
					return req.UsePool != nil && *req.UsePool
				})).Return(&clustersv1.CreatePostgresClusterResponse{}, nil).Once()
			},
		},
		"UseXatastor flag is forwarded for main branch": {
			payload: func() *ClusterServicePayload {
				p := basePayload()
				p.Flags.UseXatastor = true
				return p
			},
			branch: mainBranch,
			setupMocks: func(mockStore *storemocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient) {
				mockStore.EXPECT().GetProject(mock.Anything, orgID, projectID).Return(project, nil).Once()
				mockCreateBranch(mockStore, mainBranch, project)
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, mock.MatchedBy(func(req *clustersv1.CreatePostgresClusterRequest) bool {
					return req.UseXatastor != nil && *req.UseXatastor
				})).Return(&clustersv1.CreatePostgresClusterResponse{}, nil).Once()
			},
		},
		"UseXatastor flag is not set for child branch": {
			payload: func() *ClusterServicePayload {
				p := basePayload()
				p.ParentID = &parentID
				p.Flags.UseXatastor = true
				return p
			},
			branch: childBranch,
			setupMocks: func(mockStore *storemocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient) {
				mockStore.EXPECT().GetProject(mock.Anything, orgID, projectID).Return(project, nil).Once()
				mockCreateBranch(mockStore, childBranch, project)
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, mock.MatchedBy(func(req *clustersv1.CreatePostgresClusterRequest) bool {
					return req.UseXatastor == nil
				})).Return(&clustersv1.CreatePostgresClusterResponse{}, nil).Once()
			},
		},
		"IP filtering is applied from the project read under the lock": {
			payload: basePayload,
			branch:  mainBranch,
			setupMocks: func(mockStore *storemocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient) {
				// IP filtering was enabled between the unlocked read and the locked one.
				mockStore.EXPECT().GetProject(mock.Anything, orgID, projectID).Return(project, nil).Once()
				mockCreateBranch(mockStore, mainBranch, projectWithIPFiltering)
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, mock.Anything).
					Return(&clustersv1.CreatePostgresClusterResponse{}, nil).Once()
				mockClusters.EXPECT().SetBranchIPFiltering(mock.Anything, &clustersv1.SetBranchIPFilteringRequest{
					BranchId: mainBranch.ID,
					IpFiltering: &clustersv1.IPFilteringConfig{
						Enabled: true,
						Allowed: []string{"10.0.0.0/8"},
					},
				}).Return(nil, nil).Once()
			},
		},
		"IP filtering from the unlocked read is not applied": {
			payload: basePayload,
			branch:  mainBranch,
			setupMocks: func(mockStore *storemocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient) {
				// IP filtering was disabled between the unlocked read and the locked
				// one, so SetBranchIPFiltering must not be called.
				mockStore.EXPECT().GetProject(mock.Anything, orgID, projectID).Return(projectWithIPFiltering, nil).Once()
				mockCreateBranch(mockStore, mainBranch, project)
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, mock.Anything).
					Return(&clustersv1.CreatePostgresClusterResponse{}, nil).Once()
			},
		},
		"ProjectLockHeld is forwarded to the store": {
			payload: func() *ClusterServicePayload {
				p := basePayload()
				p.ProjectLockHeld = true
				return p
			},
			setupMocks: func(mockStore *storemocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient) {
				mockStore.EXPECT().GetProject(mock.Anything, orgID, projectID).Return(project, nil).Once()
				mockStore.On("CreateBranch", mock.Anything, orgID, projectID, mock.Anything, mock.MatchedBy(func(cfg *store.CreateBranchConfiguration) bool {
					return cfg.ProjectLockHeld
				}), mock.Anything).Return(nil, fmt.Errorf("stop after the store call")).Once()
			},
			wantErr: fmt.Errorf("stop after the store call"),
		},
		"scale-to-zero defaults from project base branch settings": {
			payload: basePayload,
			branch:  mainBranch,
			setupMocks: func(mockStore *storemocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient) {
				mockStore.EXPECT().GetProject(mock.Anything, orgID, projectID).Return(project, nil).Once()
				mockCreateBranch(mockStore, mainBranch, project)
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, mock.MatchedBy(func(req *clustersv1.CreatePostgresClusterRequest) bool {
					return req.Configuration.ScaleToZero != nil &&
						req.Configuration.ScaleToZero.Enabled == false &&
						req.Configuration.ScaleToZero.InactivityPeriodMinutes == 30
				})).Return(&clustersv1.CreatePostgresClusterResponse{}, nil).Once()
			},
		},
		"scale-to-zero defaults from project child branch settings": {
			payload: func() *ClusterServicePayload {
				p := basePayload()
				p.ParentID = &parentID
				return p
			},
			branch: childBranch,
			setupMocks: func(mockStore *storemocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient) {
				mockStore.EXPECT().GetProject(mock.Anything, orgID, projectID).Return(project, nil).Once()
				mockCreateBranch(mockStore, childBranch, project)
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, mock.MatchedBy(func(req *clustersv1.CreatePostgresClusterRequest) bool {
					return req.Configuration.ScaleToZero != nil &&
						req.Configuration.ScaleToZero.Enabled == true &&
						req.Configuration.ScaleToZero.InactivityPeriodMinutes == 15
				})).Return(&clustersv1.CreatePostgresClusterResponse{}, nil).Once()
			},
		},
		"explicit scale-to-zero is not overridden by project defaults": {
			payload: func() *ClusterServicePayload {
				p := basePayload()
				p.Configuration.ScaleToZero = &clustersv1.ScaleToZero{
					Enabled:                 true,
					InactivityPeriodMinutes: 60,
				}
				return p
			},
			branch: mainBranch,
			setupMocks: func(mockStore *storemocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient) {
				mockStore.EXPECT().GetProject(mock.Anything, orgID, projectID).Return(project, nil).Once()
				mockCreateBranch(mockStore, mainBranch, project)
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, mock.MatchedBy(func(req *clustersv1.CreatePostgresClusterRequest) bool {
					return req.Configuration.ScaleToZero != nil &&
						req.Configuration.ScaleToZero.Enabled == true &&
						req.Configuration.ScaleToZero.InactivityPeriodMinutes == 60
				})).Return(&clustersv1.CreatePostgresClusterResponse{}, nil).Once()
			},
		},
		"GetProject error is returned": {
			payload: basePayload,
			setupMocks: func(mockStore *storemocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient) {
				mockStore.EXPECT().GetProject(mock.Anything, orgID, projectID).Return(nil, fmt.Errorf("project not found")).Once()
			},
			wantErr: fmt.Errorf("project not found"),
		},
		"store CreateBranch error is returned": {
			payload: basePayload,
			setupMocks: func(mockStore *storemocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient) {
				mockStore.EXPECT().GetProject(mock.Anything, orgID, projectID).Return(project, nil).Once()
				mockStore.On("CreateBranch", mock.Anything, orgID, projectID, mock.Anything, mock.Anything, mock.Anything).
					Return(nil, fmt.Errorf("branch already exists")).Once()
			},
			wantErr: fmt.Errorf("branch already exists"),
		},
		"NotFound with parent maps to ErrBranchNotFound": {
			payload: func() *ClusterServicePayload {
				p := basePayload()
				p.ParentID = &parentID
				return p
			},
			branch: childBranch,
			setupMocks: func(mockStore *storemocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient) {
				mockStore.EXPECT().GetProject(mock.Anything, orgID, projectID).Return(project, nil).Once()
				mockCreateBranch(mockStore, childBranch, project)
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, mock.Anything).
					Return(nil, status.Error(codes.NotFound, "not found")).Once()
			},
			wantErr: ErrBranchNotFound{BranchID: parentID},
		},
		"InvalidArgument maps to ErrInvalidConfiguration": {
			payload: basePayload,
			branch:  mainBranch,
			setupMocks: func(mockStore *storemocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient) {
				mockStore.EXPECT().GetProject(mock.Anything, orgID, projectID).Return(project, nil).Once()
				mockCreateBranch(mockStore, mainBranch, project)
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, mock.Anything).
					Return(nil, status.Error(codes.InvalidArgument, "bad config")).Once()
			},
			wantErr: ErrInvalidConfiguration{Name: "test-branch", Message: "bad config"},
		},
		"FailedPrecondition with parent maps to ErrParentBranchUnhealthy": {
			payload: func() *ClusterServicePayload {
				p := basePayload()
				p.ParentID = &parentID
				return p
			},
			branch: childBranch,
			setupMocks: func(mockStore *storemocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient) {
				mockStore.EXPECT().GetProject(mock.Anything, orgID, projectID).Return(project, nil).Once()
				mockCreateBranch(mockStore, childBranch, project)
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, mock.Anything).
					Return(nil, status.Error(codes.FailedPrecondition, "unhealthy")).Once()
			},
			wantErr: ErrParentBranchUnhealthy{ParentID: parentID},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			mockStore := storemocks.NewProjectsStore(t)
			mockClusters := protomocks.NewClustersServiceClient(t)
			mockCells := cellsmock.NewCellsMock(t, mockClusters)

			tt.setupMocks(mockStore, mockClusters)

			prov := NewBranchProvisioner(mockStore, mockCells)
			payload := tt.payload()
			got, err := prov.CreateBranch(context.Background(), projectID, orgID, "test-branch", payload)

			if tt.wantErr != nil {
				require.Error(t, err)
				require.Equal(t, tt.wantErr, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, got)
				require.Equal(t, tt.branch.ID, got.ID)
			}
		})
	}
}

func TestDeleteBranch(t *testing.T) {
	testBranch := store.Branch{
		ID:     "branch-1",
		CellID: "cell-1",
		Region: "us-east-1",
	}

	tests := map[string]struct {
		branch     store.Branch
		setupMocks func(mockStore *storemocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient)
		wantErr    string
	}{
		"delete succeeds": {
			branch: testBranch,
			setupMocks: func(mockStore *storemocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DeletePostgresCluster(mock.Anything, &clustersv1.DeletePostgresClusterRequest{Id: testBranch.ID}).
					Return(&clustersv1.DeletePostgresClusterResponse{}, nil).Once()
				mockClusters.EXPECT().DeleteBranchIPFiltering(mock.Anything, &clustersv1.DeleteBranchIPFilteringRequest{BranchId: testBranch.ID}).
					Return(nil, nil).Once()
			},
		},
		"cluster not found in kubernetes proceeds with deletion": {
			branch: testBranch,
			setupMocks: func(mockStore *storemocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DeletePostgresCluster(mock.Anything, &clustersv1.DeletePostgresClusterRequest{Id: testBranch.ID}).
					Return(nil, clusters.ClusterNotFoundError(testBranch.ID)).Once()
				mockClusters.EXPECT().DeleteBranchIPFiltering(mock.Anything, &clustersv1.DeleteBranchIPFilteringRequest{BranchId: testBranch.ID}).
					Return(nil, nil).Once()
			},
		},
		"cluster delete error is returned": {
			branch: testBranch,
			setupMocks: func(mockStore *storemocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DeletePostgresCluster(mock.Anything, &clustersv1.DeletePostgresClusterRequest{Id: testBranch.ID}).
					Return(nil, fmt.Errorf("infra error")).Once()
			},
			wantErr: "infra error",
		},
		"store delete error is returned": {
			branch:     testBranch,
			setupMocks: func(mockStore *storemocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient) {},
			wantErr:    "store error",
		},
		"IP filtering cleanup failure does not fail deletion": {
			branch: testBranch,
			setupMocks: func(mockStore *storemocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient) {
				mockClusters.EXPECT().DeletePostgresCluster(mock.Anything, &clustersv1.DeletePostgresClusterRequest{Id: testBranch.ID}).
					Return(&clustersv1.DeletePostgresClusterResponse{}, nil).Once()
				mockClusters.EXPECT().DeleteBranchIPFiltering(mock.Anything, &clustersv1.DeleteBranchIPFilteringRequest{BranchId: testBranch.ID}).
					Return(nil, fmt.Errorf("ip filtering error")).Once()
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			mockStore := storemocks.NewProjectsStore(t)
			mockClusters := protomocks.NewClustersServiceClient(t)
			mockCells := cellsmock.NewCellsMock(t, mockClusters)

			tt.setupMocks(mockStore, mockClusters)

			// Mock store.DeleteBranch to invoke the deprovision callback and propagate its error
			if tt.wantErr == "store error" {
				mockStore.EXPECT().DeleteBranch(mock.Anything, orgID, projectID, tt.branch.ID, mock.Anything).
					Return(fmt.Errorf("store error")).Once()
			} else {
				b := tt.branch
				mockStore.On("DeleteBranch", mock.Anything, orgID, projectID, tt.branch.ID, mock.Anything).
					Return(func(_ context.Context, _, _, _ string, deprovisionFn func(*store.Branch) error) error {
						return deprovisionFn(&b)
					}).Once()
			}

			prov := NewBranchProvisioner(mockStore, mockCells)
			err := prov.DeleteBranch(context.Background(), orgID, projectID, tt.branch.ID)

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDeleteProject(t *testing.T) {
	const org, proj = "org-1", "proj-1"
	branches := []store.Branch{{ID: "b1"}, {ID: "b2"}}

	t.Run("deprovisions all branches then the project", func(t *testing.T) {
		st := storemocks.NewProjectsStore(t)
		st.EXPECT().ListBranches(mock.Anything, org, proj).Return(branches, nil).Once()
		st.EXPECT().DeleteBranch(mock.Anything, org, proj, "b1", mock.Anything).Return(nil).Once()
		st.EXPECT().DeleteBranch(mock.Anything, org, proj, "b2", mock.Anything).Return(nil).Once()
		st.EXPECT().DeleteProject(mock.Anything, org, proj).Return(nil).Once()

		n, err := NewBranchProvisioner(st, cellsmock.NewCells(t)).DeleteProject(context.Background(), org, proj)
		require.NoError(t, err)
		require.Equal(t, 2, n)
	})

	t.Run("idempotent: already-gone branches and project are tolerated", func(t *testing.T) {
		st := storemocks.NewProjectsStore(t)
		st.EXPECT().ListBranches(mock.Anything, org, proj).Return([]store.Branch{{ID: "b1"}}, nil).Once()
		st.EXPECT().DeleteBranch(mock.Anything, org, proj, "b1", mock.Anything).Return(store.ErrBranchNotFound{ID: "b1"}).Once()
		st.EXPECT().DeleteProject(mock.Anything, org, proj).Return(store.ErrProjectNotFound{ID: proj}).Once()

		n, err := NewBranchProvisioner(st, cellsmock.NewCells(t)).DeleteProject(context.Background(), org, proj)
		require.NoError(t, err)
		require.Equal(t, 0, n)
	})

	t.Run("a failing branch does not stop the others, and the project is left", func(t *testing.T) {
		st := storemocks.NewProjectsStore(t)
		st.EXPECT().ListBranches(mock.Anything, org, proj).Return(branches, nil).Once()
		st.EXPECT().DeleteBranch(mock.Anything, org, proj, "b1", mock.Anything).Return(fmt.Errorf("cell down")).Once()
		st.EXPECT().DeleteBranch(mock.Anything, org, proj, "b2", mock.Anything).Return(nil).Once()
		// DeleteProject is not called: a branch failed, so the project is left for a retry.

		n, err := NewBranchProvisioner(st, cellsmock.NewCells(t)).DeleteProject(context.Background(), org, proj)
		require.ErrorContains(t, err, "delete branch b1")
		require.Equal(t, 1, n)
	})

	t.Run("listing branches fails: nothing is deleted", func(t *testing.T) {
		st := storemocks.NewProjectsStore(t)
		st.EXPECT().ListBranches(mock.Anything, org, proj).Return(nil, fmt.Errorf("db down")).Once()
		// No DeleteBranch/DeleteProject: without the branch list the cascade cannot proceed.

		n, err := NewBranchProvisioner(st, cellsmock.NewCells(t)).DeleteProject(context.Background(), org, proj)
		require.ErrorContains(t, err, "list branches for project proj-1")
		require.Equal(t, 0, n)
	})
}

func TestResolveOrgLimits(t *testing.T) {
	t1Defaults := OrgLimits{
		MaxProjects:            store.TierDefaultInt(store.TierT1, store.LimitMaxProjects, 0),
		MaxProjectsPerHour:     store.TierDefaultInt(store.TierT1, store.LimitMaxProjectsPerHour, 0),
		MaxBranchesPerProject:  store.TierDefaultInt(store.TierT1, store.LimitMaxBranchesPerProject, 0),
		MaxBranchesPerOrg:      store.TierDefaultInt(store.TierT1, store.LimitMaxBranchesPerOrg, 0),
		MaxBranchesPerHour:     store.TierDefaultInt(store.TierT1, store.LimitMaxBranchesPerHour, 0),
		MaxInstancesPerBranch:  store.TierDefaultInt(store.TierT1, store.LimitMaxInstancesPerBranch, 0),
		MinInstancesPerBranch:  store.TierDefaultInt(store.TierT1, store.LimitMinInstancesPerBranch, 0),
		MaxDescriptionLength:   store.TierDefaultInt(store.TierT1, store.LimitMaxDescriptionLength, 0),
		MaxAllowedInstanceType: store.TierDefaultInt(store.TierT1, store.LimitMaxAllowedInstanceType, 0),
		MaxStorageGBPerBranch:  store.TierDefaultInt(store.TierT1, store.LimitMaxStorageGBPerBranch, 0),
	}

	t2Defaults := OrgLimits{
		MaxProjects:            store.TierDefaultInt(store.TierT2, store.LimitMaxProjects, 0),
		MaxProjectsPerHour:     store.TierDefaultInt(store.TierT2, store.LimitMaxProjectsPerHour, 0),
		MaxBranchesPerProject:  store.TierDefaultInt(store.TierT2, store.LimitMaxBranchesPerProject, 0),
		MaxBranchesPerOrg:      store.TierDefaultInt(store.TierT2, store.LimitMaxBranchesPerOrg, 0),
		MaxBranchesPerHour:     store.TierDefaultInt(store.TierT2, store.LimitMaxBranchesPerHour, 0),
		MaxInstancesPerBranch:  store.TierDefaultInt(store.TierT2, store.LimitMaxInstancesPerBranch, 0),
		MinInstancesPerBranch:  store.TierDefaultInt(store.TierT2, store.LimitMinInstancesPerBranch, 0),
		MaxDescriptionLength:   store.TierDefaultInt(store.TierT2, store.LimitMaxDescriptionLength, 0),
		MaxAllowedInstanceType: store.TierDefaultInt(store.TierT2, store.LimitMaxAllowedInstanceType, 0),
		MaxStorageGBPerBranch:  store.TierDefaultInt(store.TierT2, store.LimitMaxStorageGBPerBranch, 0),
	}

	tests := map[string]struct {
		usageTier  string
		flags      FlagsConfig
		overrides  map[store.LimitKey]any
		setupMocks func(*storemocks.ProjectsStore)
		want       OrgLimits
		wantErr    string
	}{
		"t1 returns tier defaults without DB lookup": {
			usageTier: "t1",
			setupMocks: func(s *storemocks.ProjectsStore) {
				// No GetOrgLimits call expected for T1
			},
			want: t1Defaults,
		},
		"t2 returns tier defaults when no DB overrides": {
			usageTier: "t2",
			setupMocks: func(s *storemocks.ProjectsStore) {
				s.EXPECT().GetOrgLimits(mock.Anything, orgID, projectID).Return(map[store.LimitKey]any{}, nil).Once()
			},
			want: t2Defaults,
		},
		"t2 with xatastor overrides branch-count defaults": {
			usageTier: "t2",
			flags:     FlagsConfig{UseXatastor: true},
			setupMocks: func(s *storemocks.ProjectsStore) {
				s.EXPECT().GetOrgLimits(mock.Anything, orgID, projectID).Return(map[store.LimitKey]any{}, nil).Once()
			},
			want: func() OrgLimits {
				l := t2Defaults
				l.MaxBranchesPerOrg = store.XatastorMaxBranchesPerOrg
				l.MaxBranchesPerProject = store.XatastorMaxBranchesPerProject
				l.MaxBranchesPerHour = store.XatastorMaxBranchesPerHour
				return l
			}(),
		},
		"t1 with xatastor does not override branch-count defaults": {
			usageTier:  "t1",
			flags:      FlagsConfig{UseXatastor: true},
			setupMocks: func(s *storemocks.ProjectsStore) {},
			want:       t1Defaults,
		},
		"DB override wins over tier default": {
			usageTier: "t2",
			setupMocks: func(s *storemocks.ProjectsStore) {
				s.EXPECT().GetOrgLimits(mock.Anything, orgID, projectID).Return(map[store.LimitKey]any{
					store.LimitMaxBranchesPerProject: json.Number("999"),
					store.LimitMaxDescriptionLength:  json.Number("200"),
				}, nil).Once()
			},
			want: func() OrgLimits {
				l := t2Defaults
				l.MaxBranchesPerProject = 999
				l.MaxDescriptionLength = 200
				return l
			}(),
		},
		"DB override wins over xatastor default": {
			usageTier: "t2",
			flags:     FlagsConfig{UseXatastor: true},
			setupMocks: func(s *storemocks.ProjectsStore) {
				s.EXPECT().GetOrgLimits(mock.Anything, orgID, projectID).Return(map[store.LimitKey]any{
					store.LimitMaxBranchesPerOrg: json.Number("5000"),
				}, nil).Once()
			},
			want: func() OrgLimits {
				l := t2Defaults
				l.MaxBranchesPerOrg = 5000
				l.MaxBranchesPerProject = store.XatastorMaxBranchesPerProject
				l.MaxBranchesPerHour = store.XatastorMaxBranchesPerHour
				return l
			}(),
		},
		"unknown tier defaults to t1": {
			usageTier:  "unknown",
			setupMocks: func(s *storemocks.ProjectsStore) {},
			want:       t1Defaults,
		},
		"GetOrgLimits error is returned": {
			usageTier: "t2",
			setupMocks: func(s *storemocks.ProjectsStore) {
				s.EXPECT().GetOrgLimits(mock.Anything, orgID, projectID).Return(nil, fmt.Errorf("db error")).Once()
			},
			wantErr: "get org limits",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			mockStore := storemocks.NewProjectsStore(t)
			tt.setupMocks(mockStore)

			got, err := ResolveOrgLimits(context.Background(), mockStore, tt.usageTier, orgID, projectID, tt.flags)

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
