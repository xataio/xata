package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"xata/gen/protomocks"
	"xata/internal/analytics/events"
	"xata/internal/api"
	"xata/internal/apitest"
	"xata/internal/apitest/validation"
	"xata/internal/flags"
	"xata/internal/openfeature"
	"xata/internal/postgrescfg"
	"xata/internal/postgresversions"
	"xata/services/clusters"
	"xata/services/projects/api/spec"
	"xata/services/projects/cells"
	"xata/services/projects/cells/cellsmock"
	"xata/services/projects/metrics"
	"xata/services/projects/metrics/metricsmock"
	"xata/services/projects/scheduler"
	"xata/services/projects/scheduler/strategy"
	"xata/services/projects/store"
	"xata/services/projects/store/mocks"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/require"
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	analyticsmocks "xata/internal/analytics/client/mocks"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	clustersv1 "xata/gen/proto/clusters/v1"

	openfeaturetest "xata/internal/openfeature/client/mocks"

	postgrescfgmocks "xata/internal/postgrescfg/mocks"

	postgresversionsmocks "xata/internal/postgresversions/mocks"
)

var projectsSpec *openapi3.T

func TestMain(m *testing.M) {
	var err error
	projectsSpec, err = validation.LoadProjectsSpec()
	if err != nil {
		log.Fatalf("failed to load projects spec: %v", err)
	}
	os.Exit(m.Run())
}

const defaultStorage = int32(250)

// createNewSigNozClient creates a metrics client for testing, ignoring errors
func createNewSigNozClient(t *testing.T) metrics.Client {
	client, err := metrics.NewSigNozClient("", "", "")
	require.NoError(t, err)
	return client
}

func TestListRegions(t *testing.T) {
	mockStore := mocks.NewProjectsStore(t)
	feat := openfeaturetest.NewClient(nil)
	sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
	handler := NewAPIHandler(feat, mockStore, nil, "", createNewSigNozClient(t), sched, analyticsmocks.NewClient(t), nil, nil)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

	// Test list regions
	regions := []store.Region{
		{ID: "us-east-1", OrganizationID: new("org-id"), PublicAccess: false},
		{ID: "us-west-2", PublicAccess: true},
	}
	mockStore.EXPECT().ListRegions(mock.Anything, apitest.TestOrganization).Return(regions, nil)

	c, rec := e.GET("/organizations/" + apitest.TestOrganization + "/regions").Context()
	err := handler.ListRegions(c, apitest.TestOrganization)
	assert.Nil(t, err)

	var res struct {
		Regions []store.Region `json:"regions"`
	}
	rec.MustCode(http.StatusOK)
	rec.ReadBody(&res)

	assert.Len(t, res.Regions, 2)
	assert.Equal(t, "us-east-1", res.Regions[0].ID)
	assert.Equal(t, false, res.Regions[0].PublicAccess)
	assert.Equal(t, new("org-id"), res.Regions[0].OrganizationID) // org-id is set

	assert.Equal(t, "us-west-2", res.Regions[1].ID)
	assert.Equal(t, true, res.Regions[1].PublicAccess)
	assert.Nil(t, res.Regions[1].OrganizationID) // no org-id

	mockStore.AssertExpectations(t)
}

func TestCreateProject(t *testing.T) {
	now, _ := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")
	project := store.Project{
		ID:          "123",
		Name:        "test",
		CreatedAt:   now,
		ScaleToZero: defaultProjectScaleToZeroConfig(),
	}
	mockStore := mocks.NewProjectsStore(t)
	feat := openfeaturetest.NewClient(nil)
	sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
	mockAnalytics := analyticsmocks.NewClient(t)
	handler := NewAPIHandler(feat, mockStore, nil, "", createNewSigNozClient(t), sched, mockAnalytics, nil, nil)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

	// Test create a project
	mockStore.EXPECT().CreateProject(mock.Anything, apitest.TestOrganization, createProjectConfig("test", nil)).Return(&project, nil)
	mockAnalytics.EXPECT().Track(mock.Anything, events.NewProjectCreatedEvent(apitest.TestOrganization, project.ID)).Return()

	c, rec := e.POST("/organizations/" + apitest.TestOrganization + "/projects").WithJSONBody(map[string]string{"name": "test"}).Context()
	err := handler.CreateProject(c, apitest.TestOrganization)
	assert.Nil(t, err)

	rec.MustCode(http.StatusCreated)
	var gotProject spec.Project
	rec.ReadBody(&gotProject)
	assert.Equal(t, project.ID, gotProject.Id)
	assert.Equal(t, project.Name, gotProject.Name)
	assert.Equal(t, project.CreatedAt, gotProject.CreatedAt)
	assert.Equal(t, project.ScaleToZero.BaseBranches.Enabled, gotProject.Configuration.ScaleToZero.BaseBranches.Enabled)
	assert.Equal(t, int(project.ScaleToZero.BaseBranches.InactivityPeriod.Duration().Minutes()), gotProject.Configuration.ScaleToZero.BaseBranches.InactivityPeriodMinutes)
	assert.Equal(t, project.ScaleToZero.ChildBranches.Enabled, gotProject.Configuration.ScaleToZero.ChildBranches.Enabled)
	assert.Equal(t, int(project.ScaleToZero.ChildBranches.InactivityPeriod.Duration().Minutes()), gotProject.Configuration.ScaleToZero.ChildBranches.InactivityPeriodMinutes)

	mockStore.AssertExpectations(t)

	// Test create a project with an invalid name
	mockStore.EXPECT().CreateProject(mock.Anything, apitest.TestOrganization, createProjectConfig("", nil)).Return(nil, &store.ErrInvalidProjectName{Name: ""})

	c, _ = e.POST("/organizations/" + apitest.TestOrganization + "/projects").WithJSONBody(map[string]string{}).Context()
	err = handler.CreateProject(c, apitest.TestOrganization)
	assert.Equal(t, &store.ErrInvalidProjectName{Name: ""}, err)
	mockStore.AssertExpectations(t)

	// Test create a project with custom scale to zero config
	customProject := store.Project{
		ID:        "123",
		Name:      "customProject",
		CreatedAt: now,
		ScaleToZero: store.ProjectScaleToZero{
			BaseBranches: store.ScaleToZero{
				Enabled:          false,
				InactivityPeriod: store.InactivityPeriod(10 * time.Minute),
			},
			ChildBranches: store.ScaleToZero{
				Enabled:          true,
				InactivityPeriod: store.InactivityPeriod(25 * time.Minute),
			},
		},
	}

	mockStore.EXPECT().CreateProject(mock.Anything, apitest.TestOrganization, createProjectConfig("customProject", &store.ProjectScaleToZero{
		BaseBranches: store.ScaleToZero{
			Enabled:          false,
			InactivityPeriod: store.InactivityPeriod(10 * time.Minute),
		},
		ChildBranches: store.ScaleToZero{
			Enabled:          true,
			InactivityPeriod: store.InactivityPeriod(25 * time.Minute),
		},
	})).Return(&customProject, nil)

	c, rec = e.POST("/organizations/" + apitest.TestOrganization + "/projects").WithJSONBody(map[string]any{
		"name": "customProject",
		"configuration": map[string]any{
			"scaleToZero": map[string]any{
				"baseBranches": map[string]any{
					"enabled":                 false,
					"inactivityPeriodMinutes": 10,
				},
				"childBranches": map[string]any{
					"enabled":                 true,
					"inactivityPeriodMinutes": 25,
				},
			},
		},
	}).Context()
	err = handler.CreateProject(c, apitest.TestOrganization)
	assert.Nil(t, err)

	rec.MustCode(http.StatusCreated)
	gotProject = spec.Project{}
	rec.ReadBody(&gotProject)
	assert.Equal(t, customProject.ID, gotProject.Id)
	assert.Equal(t, customProject.Name, gotProject.Name)
	assert.Equal(t, customProject.CreatedAt, gotProject.CreatedAt)
	assert.Equal(t, customProject.ScaleToZero.BaseBranches.Enabled, gotProject.Configuration.ScaleToZero.BaseBranches.Enabled)
	assert.Equal(t, int(customProject.ScaleToZero.BaseBranches.InactivityPeriod.Duration().Minutes()), gotProject.Configuration.ScaleToZero.BaseBranches.InactivityPeriodMinutes)
	assert.Equal(t, customProject.ScaleToZero.ChildBranches.Enabled, gotProject.Configuration.ScaleToZero.ChildBranches.Enabled)
	assert.Equal(t, int(customProject.ScaleToZero.ChildBranches.InactivityPeriod.Duration().Minutes()), gotProject.Configuration.ScaleToZero.ChildBranches.InactivityPeriodMinutes)

	mockStore.AssertExpectations(t)
}

func TestListProjects(t *testing.T) {
	now, _ := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")
	projects := []store.Project{
		{
			ID:        "123",
			Name:      "test",
			CreatedAt: now,
			ScaleToZero: store.ProjectScaleToZero{
				BaseBranches: defaultScaleToZeroConfig(),
				ChildBranches: store.ScaleToZero{
					Enabled:          true,
					InactivityPeriod: store.InactivityPeriod(20 * time.Minute),
				},
			},
		},
		{
			ID:          "456",
			Name:        "other",
			CreatedAt:   now,
			ScaleToZero: defaultProjectScaleToZeroConfig(),
		},
	}
	mockStore := mocks.NewProjectsStore(t)
	feat := openfeaturetest.NewClient(nil)
	sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
	handler := NewAPIHandler(feat, mockStore, nil, "", createNewSigNozClient(t), sched, analyticsmocks.NewClient(t), nil, nil)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

	// Test create a project
	mockStore.EXPECT().ListProjects(mock.Anything, apitest.TestOrganization).Return(projects, nil)

	c, rec := e.GET("/organizations/" + apitest.TestOrganization + "/projects").Context()
	err := handler.ListProjects(c, apitest.TestOrganization)
	assert.Nil(t, err)

	var res struct {
		Projects []spec.Project `json:"projects"`
	}
	rec.MustCode(http.StatusOK)
	rec.ReadBody(&res)
	assert.Equal(t, 2, len(res.Projects))
	assert.Equal(t, projects[0].ID, res.Projects[0].Id)
	assert.Equal(t, projects[0].Name, res.Projects[0].Name)
	assert.Equal(t, projects[0].CreatedAt, res.Projects[0].CreatedAt)
	assert.Equal(t, projects[0].ScaleToZero.BaseBranches.Enabled, res.Projects[0].Configuration.ScaleToZero.BaseBranches.Enabled)
	assert.Equal(t, int(projects[0].ScaleToZero.BaseBranches.InactivityPeriod.Duration().Minutes()), res.Projects[0].Configuration.ScaleToZero.BaseBranches.InactivityPeriodMinutes)
	assert.Equal(t, projects[0].ScaleToZero.ChildBranches.Enabled, res.Projects[0].Configuration.ScaleToZero.ChildBranches.Enabled)
	assert.Equal(t, int(projects[0].ScaleToZero.ChildBranches.InactivityPeriod.Duration().Minutes()), res.Projects[0].Configuration.ScaleToZero.ChildBranches.InactivityPeriodMinutes)

	assert.Equal(t, projects[1].ID, res.Projects[1].Id)
	assert.Equal(t, projects[1].Name, res.Projects[1].Name)
	assert.Equal(t, projects[1].CreatedAt, res.Projects[1].CreatedAt)
	assert.Equal(t, projects[1].ScaleToZero.BaseBranches.Enabled, res.Projects[1].Configuration.ScaleToZero.BaseBranches.Enabled)
	assert.Equal(t, int(projects[1].ScaleToZero.BaseBranches.InactivityPeriod.Duration().Minutes()), res.Projects[1].Configuration.ScaleToZero.BaseBranches.InactivityPeriodMinutes)
	assert.Equal(t, projects[1].ScaleToZero.ChildBranches.Enabled, res.Projects[1].Configuration.ScaleToZero.ChildBranches.Enabled)
	assert.Equal(t, int(projects[1].ScaleToZero.ChildBranches.InactivityPeriod.Duration().Minutes()), res.Projects[1].Configuration.ScaleToZero.ChildBranches.InactivityPeriodMinutes)

	mockStore.AssertExpectations(t)
}

func TestDeleteProject(t *testing.T) {
	mockStore := mocks.NewProjectsStore(t)
	feat := openfeaturetest.NewClient(nil)
	sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
	mockAnalytics := analyticsmocks.NewClient(t)
	handler := NewAPIHandler(feat, mockStore, nil, "", createNewSigNozClient(t), sched, mockAnalytics, nil, nil)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

	// Test delete a project
	mockStore.EXPECT().DeleteProject(mock.Anything, apitest.TestOrganization, "test").Return(nil)
	mockAnalytics.EXPECT().Track(mock.Anything, events.NewProjectDeletedEvent(apitest.TestOrganization, "test")).Return()

	c, rec := e.DELETE("/organizations/" + apitest.TestOrganization + "/projects/test").Context()
	err := handler.DeleteProject(c, apitest.TestOrganization, "test")
	assert.Nil(t, err)

	rec.MustCode(http.StatusNoContent)

	// Test delete a project that does not exist
	mockStore.EXPECT().DeleteProject(mock.Anything, apitest.TestOrganization, "1234").Return(&store.ErrProjectNotFound{ID: "1234"})
	c, _ = e.DELETE("/organizations/" + apitest.TestOrganization + "/projects/" + "1234").Context()
	err = handler.DeleteProject(c, apitest.TestOrganization, "1234")
	assert.Equal(t, &store.ErrProjectNotFound{ID: "1234"}, err)

	// deleting a project fails if storage layer errors out
	mockStore.EXPECT().DeleteProject(mock.Anything, apitest.TestOrganization, "withBranches").Return(store.ErrProjectNotEmpty{ID: "withBranches"})
	c, _ = e.DELETE("/organizations/" + apitest.TestOrganization + "/projects/" + "withBranches").Context()
	err = handler.DeleteProject(c, apitest.TestOrganization, "withBranches")
	assert.Equal(t, store.ErrProjectNotEmpty{ID: "withBranches"}, err)
}

func TestGetProject(t *testing.T) {
	time, _ := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")
	project := store.Project{
		ID:          "123",
		Name:        "test",
		CreatedAt:   time,
		ScaleToZero: defaultProjectScaleToZeroConfig(),
	}
	mockStore := mocks.NewProjectsStore(t)
	feat := openfeaturetest.NewClient(nil)
	sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
	handler := NewAPIHandler(feat, mockStore, nil, "", createNewSigNozClient(t), sched, analyticsmocks.NewClient(t), nil, nil)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

	// Test get a project
	mockStore.EXPECT().GetProject(mock.Anything, apitest.TestOrganization, "test").Return(&project, nil)

	c, rec := e.GET("/organizations/" + apitest.TestOrganization + "/projects/test").Context()
	err := handler.GetProject(c, apitest.TestOrganization, "test")
	assert.Nil(t, err)

	var gotProject spec.Project
	rec.MustCode(http.StatusOK)
	rec.ReadBody(&gotProject)
	assert.Equal(t, project.ID, gotProject.Id)
	assert.Equal(t, project.Name, gotProject.Name)
	assert.Equal(t, project.CreatedAt, gotProject.CreatedAt)
	assert.Equal(t, project.ScaleToZero.BaseBranches.Enabled, gotProject.Configuration.ScaleToZero.BaseBranches.Enabled)
	assert.Equal(t, int(project.ScaleToZero.BaseBranches.InactivityPeriod.Duration().Minutes()), gotProject.Configuration.ScaleToZero.BaseBranches.InactivityPeriodMinutes)
	assert.Equal(t, project.ScaleToZero.ChildBranches.Enabled, gotProject.Configuration.ScaleToZero.ChildBranches.Enabled)
	assert.Equal(t, int(project.ScaleToZero.ChildBranches.InactivityPeriod.Duration().Minutes()), gotProject.Configuration.ScaleToZero.ChildBranches.InactivityPeriodMinutes)
	mockStore.AssertExpectations(t)

	// Test get a project that does not exist
	mockStore.EXPECT().GetProject(mock.Anything, apitest.TestOrganization, "1234").Return(nil, &store.ErrProjectNotFound{ID: "1234"})

	c, _ = e.GET("/organizations/" + apitest.TestOrganization + "/projects/" + "test/branches/main").Context()
	err = handler.GetProject(c, apitest.TestOrganization, "1234")
	assert.Equal(t, &store.ErrProjectNotFound{ID: "1234"}, err)
	mockStore.AssertExpectations(t)
}

func TestUpdateProject(t *testing.T) {
	t.Parallel()

	now, _ := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")
	project := store.Project{
		ID:        "123",
		Name:      "test",
		CreatedAt: now,
		UpdatedAt: now,
		ScaleToZero: store.ProjectScaleToZero{
			BaseBranches: defaultScaleToZeroConfig(),
			ChildBranches: store.ScaleToZero{
				Enabled:          true,
				InactivityPeriod: store.InactivityPeriod(15 * time.Minute),
			},
		},
	}

	scaleToZeroJSON := map[string]any{
		"baseBranches": map[string]any{
			"enabled":                 false,
			"inactivityPeriodMinutes": int(30),
		},
		"childBranches": map[string]any{
			"enabled":                 true,
			"inactivityPeriodMinutes": int(15),
		},
	}
	scaleToZeroStore := &store.ProjectScaleToZero{
		BaseBranches: defaultScaleToZeroConfig(),
		ChildBranches: store.ScaleToZero{
			Enabled:          true,
			InactivityPeriod: store.InactivityPeriod(15 * time.Minute),
		},
	}
	mockIPFilteringLock := func(mockStore *mocks.ProjectsStore) {
		mockStore.EXPECT().AcquireProjectLock(mock.Anything, "123").Return(func() error { return nil }, nil)
		mockStore.EXPECT().ListBranches(mock.Anything, apitest.TestOrganization, "123").Return(nil, nil)
	}

	feat := openfeaturetest.NewClient(nil)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

	tests := []struct {
		name          string
		jsonBody      any
		setupMocks    func(*mocks.ProjectsStore)
		wantError     bool
		expectedError error
	}{
		{
			name:     "update a project name succeeds",
			jsonBody: map[string]string{"name": "test"},
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().UpdateProject(mock.Anything, apitest.TestOrganization, "123", updateProjectConfig(new("test"), nil, nil)).Return(&project, nil)
			},
		},
		{
			name: "update a project configuration succeeds",
			jsonBody: map[string]any{
				"configuration": map[string]any{"scaleToZero": scaleToZeroJSON},
			},
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().UpdateProject(mock.Anything, apitest.TestOrganization, "123", updateProjectConfig(nil, scaleToZeroStore, nil)).Return(&project, nil)
			},
		},
		{
			name: "update only ipFiltering succeeds",
			jsonBody: map[string]any{
				"configuration": map[string]any{
					"ipFiltering": map[string]any{"enabled": true, "cidr": []map[string]any{{"cidr": "10.0.0.0/8"}}},
				},
			},
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockIPFilteringLock(mockStore)
				mockStore.EXPECT().UpdateProject(mock.Anything, apitest.TestOrganization, "123", updateProjectConfig(nil, nil, &store.IPFiltering{
					Enabled: true,
					CIDRs:   []store.CIDREntry{{CIDR: "10.0.0.0/8"}},
				})).Return(&project, nil)
			},
		},
		{
			name: "update both scaleToZero and ipFiltering succeeds",
			jsonBody: map[string]any{
				"configuration": map[string]any{
					"scaleToZero": scaleToZeroJSON,
					"ipFiltering": map[string]any{"enabled": true, "cidr": []map[string]any{{"cidr": "192.168.1.0/24"}}},
				},
			},
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockIPFilteringLock(mockStore)
				mockStore.EXPECT().UpdateProject(mock.Anything, apitest.TestOrganization, "123", updateProjectConfig(nil, scaleToZeroStore, &store.IPFiltering{
					Enabled: true,
					CIDRs:   []store.CIDREntry{{CIDR: "192.168.1.0/24"}},
				})).Return(&project, nil)
			},
		},
		{
			name: "update both name and configuration succeeds",
			jsonBody: map[string]any{
				"name":          "test",
				"configuration": map[string]any{"scaleToZero": scaleToZeroJSON},
			},
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().UpdateProject(mock.Anything, apitest.TestOrganization, "123", updateProjectConfig(new("test"), scaleToZeroStore, nil)).Return(&project, nil)
			},
		},
		{
			name:       "update a project with no changes fails",
			jsonBody:   map[string]string{},
			setupMocks: func(mockStore *mocks.ProjectsStore) {},
			wantError:  true,
			expectedError: ErrorInvalidParam{
				ProjectID: "123",
				Param:     "all",
				Message:   "at least one of the request fields needs to be set",
			},
		},
		{
			name:     "update a project with invalid name fails",
			jsonBody: map[string]string{"name": ""},
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().UpdateProject(mock.Anything, apitest.TestOrganization, "123", updateProjectConfig(new(""), nil, nil)).Return(nil, &store.ErrInvalidProjectName{})
			},
			wantError:     true,
			expectedError: &store.ErrInvalidProjectName{Name: ""},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockStore := mocks.NewProjectsStore(t)
			tt.setupMocks(mockStore)
			mockAnalytics := analyticsmocks.NewClient(t)
			if !tt.wantError {
				mockAnalytics.EXPECT().Track(mock.Anything, mock.Anything).Return()
			}
			sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
			handler := NewAPIHandler(feat, mockStore, nil, "", createNewSigNozClient(t), sched, mockAnalytics, nil, nil)

			c, rec := e.PATCH("/organizations/" + apitest.TestOrganization + "/projects/123").WithJSONBody(tt.jsonBody).Context()
			err := handler.UpdateProject(c, apitest.TestOrganization, "123")
			if tt.wantError {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				assert.NoError(t, err)
				var gotProject spec.Project
				rec.ReadBody(&gotProject)
				rec.MustCode(http.StatusOK)
				assert.Equal(t, project.ID, gotProject.Id)
				assert.Equal(t, project.Name, gotProject.Name)
				assert.Equal(t, project.CreatedAt, gotProject.CreatedAt)
				assert.Equal(t, project.UpdatedAt, gotProject.UpdatedAt)
				assert.Equal(t, project.ScaleToZero.BaseBranches.Enabled, gotProject.Configuration.ScaleToZero.BaseBranches.Enabled)
				assert.Equal(t, int(project.ScaleToZero.BaseBranches.InactivityPeriod.Duration().Minutes()), gotProject.Configuration.ScaleToZero.BaseBranches.InactivityPeriodMinutes)
				assert.Equal(t, project.ScaleToZero.ChildBranches.Enabled, gotProject.Configuration.ScaleToZero.ChildBranches.Enabled)
				assert.Equal(t, int(project.ScaleToZero.ChildBranches.InactivityPeriod.Duration().Minutes()), gotProject.Configuration.ScaleToZero.ChildBranches.InactivityPeriodMinutes)
			}

			mockStore.AssertExpectations(t)
		})
	}
}

func TestAuth(t *testing.T) {
	// check wrong claims forbid access to all endpoints
	noAccessOrgID := "123456"
	mockStore := mocks.NewProjectsStore(t)
	feat := openfeaturetest.NewClient(nil)
	sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
	handler := NewAPIHandler(feat, mockStore, nil, "", createNewSigNozClient(t), sched, analyticsmocks.NewClient(t), nil, nil)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

	wantErr := api.ErrorAuthorizationFailed{Reason: fmt.Sprintf("no access to organization [%s]", noAccessOrgID)}

	c, _ := e.POST("/organizations/" + apitest.TestOrganization + "/projects").Context()

	assert.ErrorIs(t, handler.CreateProject(c, noAccessOrgID), wantErr)
	assert.ErrorIs(t, handler.DeleteProject(c, noAccessOrgID, "test"), wantErr)
	assert.ErrorIs(t, handler.GetProject(c, noAccessOrgID, "test"), wantErr)
	assert.ErrorIs(t, handler.ListProjects(c, noAccessOrgID), wantErr)
	assert.ErrorIs(t, handler.UpdateProject(c, noAccessOrgID, "test"), wantErr)
	assert.ErrorIs(t, handler.CreateBranch(c, noAccessOrgID, "test"), wantErr)
	assert.ErrorIs(t, handler.ListBranches(c, noAccessOrgID, "test"), wantErr)
	assert.ErrorIs(t, handler.ListRegions(c, noAccessOrgID), wantErr)
	assert.ErrorIs(t, handler.CreateGithubAppInstallation(c, noAccessOrgID), wantErr)
	assert.ErrorIs(t, handler.ListGithubAppInstallations(c, noAccessOrgID), wantErr)
	assert.ErrorIs(t, handler.UpdateGithubAppInstallation(c, noAccessOrgID, "inst"), wantErr)
	assert.ErrorIs(t, handler.GetGithubRepository(c, noAccessOrgID, "test", "branch"), wantErr)
	assert.ErrorIs(t, handler.CreateGithubRepository(c, noAccessOrgID, "test", "branch"), wantErr)
	assert.ErrorIs(t, handler.UpdateGithubRepository(c, noAccessOrgID, "test", "branch"), wantErr)
	assert.ErrorIs(t, handler.DeleteGithubRepository(c, noAccessOrgID, "test", "branch"), wantErr)
}

func TestAuthDisabledOrg(t *testing.T) {
	// check disabled organization prevents write access to all write endpoints
	noWriteAccessOrgID := apitest.TestOrganizationDisabled
	mockStore := mocks.NewProjectsStore(t)
	mockAnalytics := analyticsmocks.NewClient(t)
	feat := openfeaturetest.NewClient(nil)
	sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
	handler := NewAPIHandler(feat, mockStore, nil, "", createNewSigNozClient(t), sched, mockAnalytics, nil, nil)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaimsDisabled)

	wantErr := ErrorOrganizationDisabled{OrganizationID: noWriteAccessOrgID}

	c, _ := e.POST("/organizations/" + apitest.TestOrganization + "/projects").Context()

	// READs should still work
	mockStore.EXPECT().GetProject(c.Request().Context(), noWriteAccessOrgID, "test").Return(&store.Project{}, nil)
	mockStore.EXPECT().ListGithubInstallations(c.Request().Context(), noWriteAccessOrgID).Return([]store.GithubInstallation{}, nil)
	mockStore.EXPECT().GetGithubRepoMappingByProject(c.Request().Context(), noWriteAccessOrgID, "test").Return(&store.GithubRepoMapping{}, nil)
	assert.NoError(t, handler.GetProject(c, noWriteAccessOrgID, "test"))
	assert.NoError(t, handler.ListGithubAppInstallations(c, noWriteAccessOrgID))
	assert.NoError(t, handler.GetGithubRepository(c, noWriteAccessOrgID, "test", "branch"))

	// WRITEs should be blocked
	assert.ErrorIs(t, handler.CreateProject(c, noWriteAccessOrgID), wantErr)
	assert.ErrorIs(t, handler.UpdateProject(c, noWriteAccessOrgID, "test"), wantErr)
	assert.ErrorIs(t, handler.CreateBranch(c, noWriteAccessOrgID, "test"), wantErr)
	assert.ErrorIs(t, handler.UpdateBranch(c, noWriteAccessOrgID, "test", "branch"), wantErr)
	assert.ErrorIs(t, handler.CreateGithubAppInstallation(c, noWriteAccessOrgID), wantErr)
	assert.ErrorIs(t, handler.UpdateGithubAppInstallation(c, noWriteAccessOrgID, "inst"), wantErr)
	assert.ErrorIs(t, handler.CreateGithubRepository(c, noWriteAccessOrgID, "test", "branch"), wantErr)
	assert.ErrorIs(t, handler.UpdateGithubRepository(c, noWriteAccessOrgID, "test", "branch"), wantErr)

	// DELETEs should still work
	mockStore.EXPECT().DeleteProject(c.Request().Context(), noWriteAccessOrgID, "test").Return(nil)
	mockStore.EXPECT().DeleteBranch(c.Request().Context(), noWriteAccessOrgID, "test", "branch", mock.Anything).Return(nil)
	mockStore.EXPECT().DeleteGithubRepoMapping(c.Request().Context(), noWriteAccessOrgID, "test").Return(nil)
	mockAnalytics.EXPECT().Track(mock.Anything, mock.Anything)
	assert.NoError(t, handler.DeleteProject(c, noWriteAccessOrgID, "test"))
	assert.NoError(t, handler.DeleteBranch(c, noWriteAccessOrgID, "test", "branch"))
	assert.NoError(t, handler.DeleteGithubRepository(c, noWriteAccessOrgID, "test", "branch"))
}

func TestCreateBranch(t *testing.T) {
	mockStore := mocks.NewProjectsStore(t)
	mockClusters := protomocks.NewClustersServiceClient(t)
	mockCells := cellsmock.NewCellsMock(t, mockClusters)
	mockPostgresConfig := postgrescfgmocks.NewPostgresConfigProvider(t)
	mockImageProvider := postgresversionsmocks.NewImageProvider(t)

	feat := openfeaturetest.NewClient(nil)
	sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
	mockAnalytics := analyticsmocks.NewClient(t)
	handler := NewAPIHandler(feat, mockStore, mockCells, "", createNewSigNozClient(t), sched, mockAnalytics, mockPostgresConfig, mockImageProvider)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

	now := time.Now()
	childBranchesScaleToZero := store.ScaleToZero{
		Enabled:          true,
		InactivityPeriod: store.InactivityPeriod(15 * time.Minute),
	}
	project := store.Project{
		ID:        "project_id",
		Name:      "test",
		CreatedAt: now,
		ScaleToZero: store.ProjectScaleToZero{
			BaseBranches:  defaultScaleToZeroConfig(),
			ChildBranches: childBranchesScaleToZero,
		},
	}

	branch := store.Branch{
		ID:             "123",
		Name:           "test",
		ParentID:       nil,
		CellID:         "cell_id",
		Region:         "region-id-1",
		BackupsEnabled: true,
	}

	childBranch := store.Branch{
		ID:       "1234",
		Name:     "childBranch",
		ParentID: &branch.ID,
		CellID:   "cell_id",
		Region:   "region-id-1",
	}

	configuration := spec.ClusterConfiguration{
		Replicas:     int32(0),
		Image:        "postgres:17.5",
		Storage:      new(defaultStorage),
		InstanceType: "xata.micro",
		Region:       "region-id-1",
	}

	correctDescription := "description"
	invalidDescription := "-description"

	createBranchTests := []struct {
		name             string
		projectID        string
		branch           store.Branch
		connectionString *string
		configuration    spec.ClusterConfiguration
		jsonBody         any
		setupMocks       func(capturedRequest **clustersv1.CreatePostgresClusterRequest)
		validateCaptured func(t *testing.T, req *clustersv1.CreatePostgresClusterRequest)
		wantError        bool
		expectedError    error
	}{
		{
			name:             "create a branch succeeds",
			projectID:        "project_id",
			branch:           branch,
			connectionString: new("postgresql://user:pass@123./xata?sslmode=require"),
			jsonBody:         map[string]any{"name": branch.Name, "mode": "custom", "configuration": map[string]any{"image": configuration.Image, "replicas": configuration.Replicas, "region": configuration.Region, "instanceType": configuration.InstanceType}, "backupConfiguration": map[string]any{"retentionPeriod": 2, "backupTime": "2:23:23"}},
			configuration:    configuration,
			setupMocks: func(capturedRequest **clustersv1.CreatePostgresClusterRequest) {
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.5"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.5").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Once()
				mockStore.EXPECT().CreateBranch(mock.Anything, apitest.TestOrganization, "project_id", "cell_id", createBranchConfig(branch.Name, nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID, projectID, cellID string, cfg *store.CreateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Nil(t, err)
				}).Return(&branch, nil).Once()
				mockStore.EXPECT().ListCells(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.Cell{{ID: "cell_id", RegionID: "region-id-1", Primary: true}}, nil).Once()
				mockStore.EXPECT().GetPrimaryCell(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Cell{ID: "cell_id", RegionID: "region-id-1", Primary: true}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1}}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPostgresParameters("xata.micro", mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(map[string]string{
					"max_connections": "50",
					"shared_buffers":  "256MB",
					"work_mem":        "2259kB",
				}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPreloadLibraries(mock.AnythingOfType("string")).Return([]string{"pg_stat_statements", "auto_explain"}, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: branch.ID, Username: "superuser"}).
					Return(&clustersv1.GetPostgresClusterCredentialsResponse{Username: "user", Password: "pass"}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: configuration.Region, GatewayHostPort: "", BackupsEnabled: true}, nil).Twice()
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, &clustersv1.CreatePostgresClusterRequest{Id: branch.ID, Configuration: &clustersv1.ClusterConfiguration{
					NumInstances: configuration.Replicas + 1,
					ImageName:    "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
					VcpuRequest:  "250m",
					VcpuLimit:    "2",
					Memory:       "1Gi",
					ScaleToZero:  defaultClustersScaleToZero(),
					PostgresConfigurationParameters: map[string]string{
						"max_connections": "50",
						"shared_buffers":  "256MB",
						"work_mem":        "2259kB",
					},
					PreloadLibraries: []string{"pg_stat_statements", "auto_explain"},
				}, OrganizationId: apitest.TestOrganization, ProjectId: "project_id", BackupConfiguration: &clustersv1.BackupConfiguration{BackupSchedule: "0 23 23 * * 2", BackupRetention: "2d", BackupsEnabled: true}}).
							Return(&clustersv1.CreatePostgresClusterResponse{}, nil).Once()
				mockStore.EXPECT().GetProject(mock.Anything, apitest.TestOrganization, "project_id").Return(&project, nil).Once()
				mockAnalytics.EXPECT().Track(mock.Anything, events.NewBranchFromConfigurationEvent(
					apitest.TestOrganization, "project_id", branch.ID, configuration.Region,
					"postgres:17.5", configuration.InstanceType, int(configuration.Replicas), nil)).Return().Once()
			},
			wantError: false,
		},
		{
			name:             "create a branch succeeds with missing backup settings",
			projectID:        "project_id",
			branch:           branch,
			connectionString: new("postgresql://user:pass@123./xata?sslmode=require"),
			jsonBody:         map[string]any{"name": branch.Name, "mode": "custom", "configuration": map[string]any{"image": configuration.Image, "replicas": configuration.Replicas, "region": configuration.Region, "instanceType": configuration.InstanceType}},
			configuration:    configuration,
			setupMocks: func(capturedRequest **clustersv1.CreatePostgresClusterRequest) {
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.5"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.5").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Once()
				mockStore.EXPECT().CreateBranch(mock.Anything, apitest.TestOrganization, "project_id", "cell_id", createBranchConfig(branch.Name, nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID, projectID, cellID string, cfg *store.CreateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Nil(t, err)
				}).Return(&branch, nil).Once()
				mockStore.EXPECT().ListCells(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.Cell{{ID: "cell_id", RegionID: "region-id-1", Primary: true}}, nil).Once()
				mockStore.EXPECT().GetPrimaryCell(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Cell{ID: "cell_id", RegionID: "region-id-1", Primary: true}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1}}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPostgresParameters("xata.micro", mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(map[string]string{
					"max_connections": "50",
					"shared_buffers":  "256MB",
					"work_mem":        "2259kB",
				}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPreloadLibraries(mock.AnythingOfType("string")).Return([]string{"pg_stat_statements", "auto_explain"}, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: branch.ID, Username: "superuser"}).
					Return(&clustersv1.GetPostgresClusterCredentialsResponse{Username: "user", Password: "pass"}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: configuration.Region, GatewayHostPort: "", BackupsEnabled: true}, nil).Twice()
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, mock.Anything).Run(func(ctx context.Context, req *clustersv1.CreatePostgresClusterRequest, opts ...grpc.CallOption) {
					*capturedRequest = req // Store the captured request
				}).Return(&clustersv1.CreatePostgresClusterResponse{}, nil).Once()

				mockStore.EXPECT().GetProject(mock.Anything, apitest.TestOrganization, "project_id").Return(&project, nil).Once()
				mockAnalytics.EXPECT().Track(mock.Anything, events.NewBranchFromConfigurationEvent(
					apitest.TestOrganization, "project_id", branch.ID, configuration.Region,
					"postgres:17.5", configuration.InstanceType, int(configuration.Replicas), nil)).Return().Once()
			},
			validateCaptured: func(t *testing.T, req *clustersv1.CreatePostgresClusterRequest) {
				defaultScaleToZero := defaultClustersScaleToZero()
				assert.NotNil(t, req, "CreatePostgresCluster should have been called")
				assert.Equal(t, branch.ID, req.Id)
				assert.Equal(t, configuration.Replicas+1, req.Configuration.NumInstances)
				assert.Equal(t, "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5", req.Configuration.ImageName)
				assert.Equal(t, "250m", req.Configuration.VcpuRequest)
				assert.Equal(t, "2", req.Configuration.VcpuLimit)
				assert.Equal(t, "1Gi", req.Configuration.Memory)
				assert.Equal(t, defaultScaleToZero.Enabled, req.Configuration.ScaleToZero.Enabled)
				assert.Equal(t, defaultScaleToZero.InactivityPeriodMinutes, req.Configuration.ScaleToZero.InactivityPeriodMinutes)
				assert.Equal(t, 3, len(req.Configuration.PostgresConfigurationParameters))
				assert.Equal(t, "50", req.Configuration.PostgresConfigurationParameters["max_connections"])
				assert.Equal(t, "256MB", req.Configuration.PostgresConfigurationParameters["shared_buffers"])
				assert.Equal(t, "2259kB", req.Configuration.PostgresConfigurationParameters["work_mem"])
				assert.Equal(t, apitest.TestOrganization, req.OrganizationId)
				assert.Equal(t, "project_id", req.ProjectId)
				assert.Nil(t, req.DataSource)
				assert.True(t, req.BackupConfiguration.BackupsEnabled, "BackupsEnabled should be true")
				assert.NotNil(t, req.BackupConfiguration, "BackupConfiguration should be present")
				assert.NotEmpty(t, req.BackupConfiguration.BackupSchedule, "BackupSchedule should not be empty")

				// Validate cron format
				parts := strings.Split(req.BackupConfiguration.BackupSchedule, " ")
				assert.Equal(t, 6, len(parts), "Backup schedule should be a valid cron expression with 6 parts")
			},
			wantError: false,
		},
		{
			name:             "create a branch succeeds with incomplete backup settings",
			projectID:        "project_id",
			branch:           branch,
			connectionString: new("postgresql://user:pass@123./xata?sslmode=require"),
			jsonBody:         map[string]any{"name": branch.Name, "mode": "custom", "configuration": map[string]any{"image": configuration.Image, "replicas": configuration.Replicas, "region": configuration.Region, "instanceType": configuration.InstanceType}, "backupConfiguration": map[string]any{"backupTime": "*:23:23"}},
			configuration:    configuration,
			setupMocks: func(capturedRequest **clustersv1.CreatePostgresClusterRequest) {
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.5"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.5").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Once()
				mockStore.EXPECT().CreateBranch(mock.Anything, apitest.TestOrganization, "project_id", "cell_id", createBranchConfig(branch.Name, nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID, projectID, cellID string, cfg *store.CreateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Nil(t, err)
				}).Return(&branch, nil).Once()
				mockStore.EXPECT().ListCells(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.Cell{{ID: "cell_id", RegionID: "region-id-1", Primary: true}}, nil).Once()
				mockStore.EXPECT().GetPrimaryCell(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Cell{ID: "cell_id", RegionID: "region-id-1", Primary: true}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1}}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPostgresParameters("xata.micro", mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(map[string]string{
					"max_connections": "50",
					"shared_buffers":  "256MB",
					"work_mem":        "2259kB",
				}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPreloadLibraries(mock.AnythingOfType("string")).Return([]string{"pg_stat_statements", "auto_explain"}, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: branch.ID, Username: "superuser"}).
					Return(&clustersv1.GetPostgresClusterCredentialsResponse{Username: "user", Password: "pass"}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: configuration.Region, GatewayHostPort: "", BackupsEnabled: true}, nil).Twice()
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, mock.Anything).Run(func(ctx context.Context, req *clustersv1.CreatePostgresClusterRequest, opts ...grpc.CallOption) {
					*capturedRequest = req // Store the captured request
				}).Return(&clustersv1.CreatePostgresClusterResponse{}, nil).Once()
				mockStore.EXPECT().GetProject(mock.Anything, apitest.TestOrganization, "project_id").Return(&project, nil).Once()
				mockAnalytics.EXPECT().Track(mock.Anything, events.NewBranchFromConfigurationEvent(
					apitest.TestOrganization, "project_id", branch.ID, configuration.Region,
					"postgres:17.5", configuration.InstanceType, int(configuration.Replicas), nil)).Return().Once()
			},
			validateCaptured: func(t *testing.T, req *clustersv1.CreatePostgresClusterRequest) {
				defaultScaleToZero := defaultClustersScaleToZero()
				assert.NotNil(t, req, "CreatePostgresCluster should have been called")
				assert.Equal(t, branch.ID, req.Id)
				assert.Equal(t, configuration.Replicas+1, req.Configuration.NumInstances)
				assert.Equal(t, "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5", req.Configuration.ImageName)
				assert.Equal(t, "250m", req.Configuration.VcpuRequest)
				assert.Equal(t, "2", req.Configuration.VcpuLimit)
				assert.Equal(t, "1Gi", req.Configuration.Memory)
				assert.Equal(t, defaultScaleToZero.Enabled, req.Configuration.ScaleToZero.Enabled)
				assert.Equal(t, defaultScaleToZero.InactivityPeriodMinutes, req.Configuration.ScaleToZero.InactivityPeriodMinutes)
				assert.Equal(t, 3, len(req.Configuration.PostgresConfigurationParameters))
				assert.Equal(t, "50", req.Configuration.PostgresConfigurationParameters["max_connections"])
				assert.Equal(t, "256MB", req.Configuration.PostgresConfigurationParameters["shared_buffers"])
				assert.Equal(t, "2259kB", req.Configuration.PostgresConfigurationParameters["work_mem"])
				assert.Equal(t, apitest.TestOrganization, req.OrganizationId)
				assert.Equal(t, "project_id", req.ProjectId)
				assert.Nil(t, req.DataSource)
				assert.True(t, req.BackupConfiguration.BackupsEnabled, "BackupsEnabled should be true")
				assert.NotNil(t, req.BackupConfiguration, "BackupConfiguration should be present")
				assert.NotEmpty(t, req.BackupConfiguration.BackupSchedule, "BackupSchedule should not be empty")

				// Validate cron format
				parts := strings.Split(req.BackupConfiguration.BackupSchedule, " ")
				assert.Equal(t, 6, len(parts), "Backup schedule should be a valid cron expression with 6 parts")
			},
			wantError: false,
		},
		{
			name:             "create a branch succeeds with daily backup settings",
			projectID:        "project_id",
			branch:           branch,
			connectionString: new("postgresql://user:pass@123./xata?sslmode=require"),
			jsonBody:         map[string]any{"name": branch.Name, "mode": "custom", "configuration": map[string]any{"image": configuration.Image, "replicas": configuration.Replicas, "region": configuration.Region, "instanceType": configuration.InstanceType}, "backupConfiguration": map[string]any{"retentionPeriod": 2, "backupTime": "*:23:23"}},
			configuration:    configuration,
			setupMocks: func(capturedRequest **clustersv1.CreatePostgresClusterRequest) {
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.5"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.5").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Once()
				mockStore.EXPECT().CreateBranch(mock.Anything, apitest.TestOrganization, "project_id", "cell_id", createBranchConfig(branch.Name, nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID, projectID, cellID string, cfg *store.CreateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Nil(t, err)
				}).Return(&branch, nil).Once()
				mockStore.EXPECT().ListCells(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.Cell{{ID: "cell_id", RegionID: "region-id-1", Primary: true}}, nil).Once()
				mockStore.EXPECT().GetPrimaryCell(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Cell{ID: "cell_id", RegionID: "region-id-1", Primary: true}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1}}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPostgresParameters("xata.micro", mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(map[string]string{
					"max_connections": "50",
					"shared_buffers":  "256MB",
					"work_mem":        "2259kB",
				}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPreloadLibraries(mock.AnythingOfType("string")).Return([]string{"pg_stat_statements", "auto_explain"}, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: branch.ID, Username: "superuser"}).
					Return(&clustersv1.GetPostgresClusterCredentialsResponse{Username: "user", Password: "pass"}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: configuration.Region, GatewayHostPort: "", BackupsEnabled: true}, nil).Twice()
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, mock.Anything).Run(func(ctx context.Context, req *clustersv1.CreatePostgresClusterRequest, opts ...grpc.CallOption) {
					*capturedRequest = req // Store the captured request
				}).Return(&clustersv1.CreatePostgresClusterResponse{}, nil).Once()
				mockStore.EXPECT().GetProject(mock.Anything, apitest.TestOrganization, "project_id").Return(&project, nil).Once()
				mockAnalytics.EXPECT().Track(mock.Anything, events.NewBranchFromConfigurationEvent(
					apitest.TestOrganization, "project_id", branch.ID, configuration.Region,
					"postgres:17.5", configuration.InstanceType, int(configuration.Replicas), nil)).Return().Once()
			},
			validateCaptured: func(t *testing.T, req *clustersv1.CreatePostgresClusterRequest) {
				defaultScaleToZero := defaultClustersScaleToZero()
				assert.NotNil(t, req, "CreatePostgresCluster should have been called")
				assert.Equal(t, branch.ID, req.Id)
				assert.Equal(t, configuration.Replicas+1, req.Configuration.NumInstances)
				assert.Equal(t, "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5", req.Configuration.ImageName)
				assert.Equal(t, "250m", req.Configuration.VcpuRequest)
				assert.Equal(t, "2", req.Configuration.VcpuLimit)
				assert.Equal(t, "1Gi", req.Configuration.Memory)
				assert.Equal(t, defaultScaleToZero.Enabled, req.Configuration.ScaleToZero.Enabled)
				assert.Equal(t, defaultScaleToZero.InactivityPeriodMinutes, req.Configuration.ScaleToZero.InactivityPeriodMinutes)
				assert.Equal(t, 3, len(req.Configuration.PostgresConfigurationParameters))
				assert.Equal(t, "50", req.Configuration.PostgresConfigurationParameters["max_connections"])
				assert.Equal(t, "256MB", req.Configuration.PostgresConfigurationParameters["shared_buffers"])
				assert.Equal(t, "2259kB", req.Configuration.PostgresConfigurationParameters["work_mem"])
				assert.Equal(t, apitest.TestOrganization, req.OrganizationId)
				assert.Equal(t, "project_id", req.ProjectId)
				assert.True(t, req.BackupConfiguration.BackupsEnabled, "BackupsEnabled should be true")
				assert.Nil(t, req.DataSource)
				assert.NotNil(t, req.BackupConfiguration, "BackupConfiguration should be present")
				assert.NotEmpty(t, req.BackupConfiguration.BackupSchedule, "BackupSchedule should not be empty")

				// Validate cron format
				parts := strings.Split(req.BackupConfiguration.BackupSchedule, " ")
				assert.Equal(t, 6, len(parts), "Backup schedule should be a valid cron expression with 6 parts")
			},
			wantError: false,
		},
		{
			name:             "create a child branch succeeds",
			projectID:        "project_id",
			branch:           childBranch,
			connectionString: new("postgresql://user:pass@1234./xata?sslmode=require"),
			jsonBody:         map[string]any{"name": childBranch.Name, "mode": "inherit", "parentID": *childBranch.ParentID, "backupConfiguration": map[string]any{"retentionPeriod": 2, "backupTime": "2:23:23"}},
			setupMocks: func(capturedRequest **clustersv1.CreatePostgresClusterRequest) {
				mockStore.EXPECT().CreateBranch(mock.Anything, apitest.TestOrganization, "project_id", "cell_id", createBranchConfig(childBranch.Name, childBranch.ParentID, nil), mock.Anything).Run(func(ctx context.Context, organizationID, projectID, cellID string, cfg *store.CreateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&childBranch)
					assert.Nil(t, err)
				}).Return(&childBranch, nil).Once()
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.5"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.5").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Once()
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", *childBranch.ParentID).Return(&branch, nil).Once()
				mockStore.EXPECT().GetPrimaryCell(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Cell{ID: "cell_id", RegionID: "region-id-1", Primary: true}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: configuration.Region, GatewayHostPort: "", BackupsEnabled: true}, nil).Twice()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: childBranch.ID, Username: "superuser"}).
					Return(&clustersv1.GetPostgresClusterCredentialsResponse{Username: "user", Password: "pass"}, nil).Once()
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything,
					&clustersv1.CreatePostgresClusterRequest{
						Id:             childBranch.ID,
						ParentId:       childBranch.ParentID,
						OrganizationId: apitest.TestOrganization,
						ProjectId:      "project_id",
						Configuration: &clustersv1.ClusterConfiguration{
							ScaleToZero: &clustersv1.ScaleToZero{Enabled: true, InactivityPeriodMinutes: 15},
						},
						BackupConfiguration: &clustersv1.BackupConfiguration{
							BackupSchedule:  "0 23 23 * * 2",
							BackupRetention: "2d",
							BackupsEnabled:  true,
						},
						DataSource: &clustersv1.CreatePostgresClusterRequest_ClusterSnapshot{
							ClusterSnapshot: &clustersv1.ClusterSnapshot{
								ClusterId: *childBranch.ParentID,
							},
						},
					}).Return(&clustersv1.CreatePostgresClusterResponse{}, nil).Once()
				mockStore.EXPECT().GetProject(mock.Anything, apitest.TestOrganization, "project_id").Return(&project, nil).Once()
				mockAnalytics.EXPECT().Track(mock.Anything, events.NewBranchFromParentEvent(
					apitest.TestOrganization, "project_id", *childBranch.ParentID, childBranch.ID, childBranch.Region)).Return().Once()
			},
			wantError: false,
		},
		{
			name:             "create a child branch fails for unhealthy parent",
			projectID:        "project_id",
			branch:           childBranch,
			connectionString: new("postgresql://user:pass@1234./xata?sslmode=require"),
			jsonBody:         map[string]any{"name": childBranch.Name, "mode": "inherit", "parentID": *childBranch.ParentID, "backupConfiguration": map[string]any{"retentionPeriod": 2, "backupTime": "2:23:23"}},
			setupMocks: func(capturedRequest **clustersv1.CreatePostgresClusterRequest) {
				mockStore.EXPECT().CreateBranch(mock.Anything, apitest.TestOrganization, "project_id", "cell_id", createBranchConfig(childBranch.Name, childBranch.ParentID, nil), mock.Anything).Run(func(ctx context.Context, organizationID, projectID, cellID string, cfg *store.CreateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&childBranch)
					assert.Error(t, err)
				}).Return(nil, clusters.ClusterNotHealthyError(*childBranch.ParentID)).Once()
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", *childBranch.ParentID).Return(&branch, nil).Once()
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything,
					&clustersv1.CreatePostgresClusterRequest{
						Id:             childBranch.ID,
						ParentId:       childBranch.ParentID,
						OrganizationId: apitest.TestOrganization,
						ProjectId:      "project_id",
						Configuration: &clustersv1.ClusterConfiguration{
							ScaleToZero: &clustersv1.ScaleToZero{Enabled: true, InactivityPeriodMinutes: 15},
						},
						BackupConfiguration: &clustersv1.BackupConfiguration{
							BackupSchedule:  "0 23 23 * * 2",
							BackupRetention: "2d",
							BackupsEnabled:  true,
						},
						DataSource: &clustersv1.CreatePostgresClusterRequest_ClusterSnapshot{
							ClusterSnapshot: &clustersv1.ClusterSnapshot{
								ClusterId: *childBranch.ParentID,
							},
						},
					}).Return(nil, clusters.ClusterNotHealthyError(*childBranch.ParentID)).Once()
				mockStore.EXPECT().GetProject(mock.Anything, apitest.TestOrganization, "project_id").Return(&project, nil).Once()
			},
			wantError:     true,
			expectedError: ErrorParentBranchUnhealthy{ParentID: *childBranch.ParentID},
		},
		{
			name:      "create a child branch fails for unknown parent",
			projectID: "project_id",
			jsonBody:  map[string]string{"name": childBranch.Name, "mode": "inherit", "parentID": *childBranch.ParentID},
			setupMocks: func(capturedRequest **clustersv1.CreatePostgresClusterRequest) {
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", *childBranch.ParentID).Return(nil, store.ErrBranchNotFound{ID: *childBranch.ParentID}).Once()
			},
			wantError:     true,
			expectedError: ErrorBranchNotFound{BranchID: *childBranch.ParentID},
		},
		{
			name:      "create a branch fails with error storage",
			projectID: "project_id",
			jsonBody:  map[string]any{"name": branch.Name, "mode": "custom", "configuration": map[string]any{"image": configuration.Image, "replicas": configuration.Replicas, "storage": configuration.Storage, "region": configuration.Region, "instanceType": configuration.InstanceType}, "backupConfiguration": map[string]any{"retentionPeriod": 2, "backupTime": "2:23:23"}},
			setupMocks: func(capturedRequest **clustersv1.CreatePostgresClusterRequest) {
				mockStore.EXPECT().CreateBranch(mock.Anything, apitest.TestOrganization, "project_id", "cell_id", createBranchConfig(branch.Name, nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID, projectID, cellID string, cfg *store.CreateBranchConfiguration, provisionFn func(*store.Branch) error) {
					// The provision function should succeed (CreatePostgresCluster succeeds)
					err := provisionFn(&branch)
					assert.Nil(t, err)
				}).Return(nil, fmt.Errorf("some storage error")).Once()
				mockStore.EXPECT().ListCells(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.Cell{{ID: "cell_id", RegionID: "region-id-1", Primary: true}}, nil).Once()
				mockStore.EXPECT().GetPrimaryCell(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Cell{ID: "cell_id", RegionID: "region-id-1", Primary: true}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1}}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPostgresParameters("xata.micro", mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(map[string]string{
					"max_connections": "50",
					"shared_buffers":  "256MB",
					"work_mem":        "2259kB",
				}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPreloadLibraries(mock.AnythingOfType("string")).Return([]string{"pg_stat_statements", "auto_explain"}, nil).Once()
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, &clustersv1.CreatePostgresClusterRequest{
					Id: branch.ID,
					Configuration: &clustersv1.ClusterConfiguration{
						NumInstances: configuration.Replicas + 1,
						ImageName:    "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
						VcpuRequest:  "250m",
						VcpuLimit:    "2",
						Memory:       "1Gi",
						ScaleToZero:  defaultClustersScaleToZero(),
						PostgresConfigurationParameters: map[string]string{
							"max_connections": "50",
							"shared_buffers":  "256MB",
							"work_mem":        "2259kB",
						},
						PreloadLibraries: []string{"pg_stat_statements", "auto_explain"},
					},
					BackupConfiguration: &clustersv1.BackupConfiguration{
						BackupSchedule:  "0 23 23 * * 2",
						BackupRetention: "2d",
						BackupsEnabled:  true,
					},
					OrganizationId: apitest.TestOrganization,
					ProjectId:      "project_id",
				}).Return(&clustersv1.CreatePostgresClusterResponse{}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: configuration.Region, GatewayHostPort: "", BackupsEnabled: true}, nil).Once()
				mockStore.EXPECT().GetProject(mock.Anything, apitest.TestOrganization, "project_id").Return(&project, nil).Once()
			},
			wantError:     true,
			expectedError: fmt.Errorf("some storage error"),
		},
		{
			name:      "create a branch fails with error infra",
			projectID: "project_id",
			jsonBody:  map[string]any{"name": branch.Name, "mode": "custom", "configuration": map[string]any{"image": configuration.Image, "replicas": configuration.Replicas, "storage": configuration.Storage, "region": configuration.Region, "instanceType": configuration.InstanceType}, "backupConfiguration": map[string]any{"retentionPeriod": 2, "backupTime": "2:23:23"}},
			setupMocks: func(capturedRequest **clustersv1.CreatePostgresClusterRequest) {
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.5"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.5").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Once()
				mockStore.EXPECT().CreateBranch(mock.Anything, apitest.TestOrganization, "project_id", "cell_id", createBranchConfig(branch.Name, nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID, projectID, cellID string, cfg *store.CreateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Error(t, err)
				}).Return(nil, fmt.Errorf("some cluster error")).Once()
				mockStore.EXPECT().ListCells(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.Cell{{ID: "cell_id", RegionID: "region-id-1", Primary: true}}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: configuration.Region, GatewayHostPort: "", BackupsEnabled: true}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1}}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPostgresParameters("xata.micro", mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(map[string]string{
					"max_connections": "50",
					"shared_buffers":  "256MB",
					"work_mem":        "2259kB",
				}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPreloadLibraries(mock.AnythingOfType("string")).Return([]string{"pg_stat_statements", "auto_explain"}, nil).Once()
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything,
					&clustersv1.CreatePostgresClusterRequest{
						Id: branch.ID,
						Configuration: &clustersv1.ClusterConfiguration{
							NumInstances: configuration.Replicas + 1,
							ImageName:    "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
							VcpuRequest:  "250m",
							VcpuLimit:    "2",
							Memory:       "1Gi",
							ScaleToZero:  defaultClustersScaleToZero(),
							PostgresConfigurationParameters: map[string]string{
								"max_connections": "50",
								"shared_buffers":  "256MB",
								"work_mem":        "2259kB",
							},
							PreloadLibraries: []string{"pg_stat_statements", "auto_explain"},
						},
						BackupConfiguration: &clustersv1.BackupConfiguration{
							BackupSchedule:  "0 23 23 * * 2",
							BackupRetention: "2d",
							BackupsEnabled:  true,
						},
						OrganizationId: apitest.TestOrganization,
						ProjectId:      "project_id",
					}).Return(nil, fmt.Errorf("some cluster error")).Once()
				mockStore.EXPECT().GetProject(mock.Anything, apitest.TestOrganization, "project_id").Return(&project, nil).Once()
			},
			wantError:     true,
			expectedError: fmt.Errorf("some cluster error"),
		},
		{
			name:      "create a branch fails with error invalid config replicas",
			projectID: "project_id",
			jsonBody:  map[string]any{"name": branch.Name, "mode": "custom", "configuration": map[string]any{"image": configuration.Image, "replicas": configuration.Replicas + 7, "storage": configuration.Storage, "region": configuration.Region, "instanceType": configuration.InstanceType}, "backupConfiguration": map[string]any{"retentionPeriod": 2, "backupTime": "2:23:23"}},
			setupMocks: func(capturedRequest **clustersv1.CreatePostgresClusterRequest) {
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.5"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.5").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Once()
				mockStore.EXPECT().CreateBranch(mock.Anything, apitest.TestOrganization, "project_id", "cell_id", createBranchConfig(branch.Name, nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID, projectID, cellID string, cfg *store.CreateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Error(t, err)
				}).Return(nil, clusters.ClusterInvalidParameter(branch.ID, "Number of instances", strconv.FormatInt(int64(configuration.Replicas+1+7), 10), fmt.Sprintf("Min: %d and Max: %d", DefaultMinInstances, DefaultMaxInstances))).Once()
				mockStore.EXPECT().ListCells(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.Cell{{ID: "cell_id", RegionID: "region-id-1", Primary: true}}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: configuration.Region, GatewayHostPort: "", BackupsEnabled: true}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1}}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPostgresParameters("xata.micro", mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(map[string]string{
					"max_connections": "50",
					"shared_buffers":  "256MB",
					"work_mem":        "2259kB",
				}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPreloadLibraries(mock.AnythingOfType("string")).Return([]string{"pg_stat_statements", "auto_explain"}, nil).Once()
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything,
					&clustersv1.CreatePostgresClusterRequest{
						Id:       branch.ID,
						ParentId: nil,
						Configuration: &clustersv1.ClusterConfiguration{
							NumInstances: configuration.Replicas + 1 + 7,
							ImageName:    "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
							VcpuRequest:  "250m",
							VcpuLimit:    "2",
							Memory:       "1Gi",
							ScaleToZero:  defaultClustersScaleToZero(),
							PostgresConfigurationParameters: map[string]string{
								"max_connections": "50",
								"shared_buffers":  "256MB",
								"work_mem":        "2259kB",
							},
							PreloadLibraries: []string{"pg_stat_statements", "auto_explain"},
						},
						BackupConfiguration: &clustersv1.BackupConfiguration{
							BackupSchedule:  "0 23 23 * * 2",
							BackupRetention: "2d",
							BackupsEnabled:  true,
						},
						OrganizationId: apitest.TestOrganization,
						ProjectId:      "project_id",
					}).Return(nil, clusters.ClusterInvalidParameter(branch.ID, "Number of instances", fmt.Sprintf("%dGi", configuration.Storage), fmt.Sprintf("Max: %d", DefaultMaxInstances))).Once()
				mockStore.EXPECT().GetProject(mock.Anything, apitest.TestOrganization, "project_id").Return(&project, nil).Once()
			},
			wantError:     true,
			expectedError: ErrorInvalidParam{BranchName: branch.Name, Param: "configuration", Message: fmt.Sprintf("cluster [%s] invalid parameter: %s has value %s and error is %s", branch.ID, "Number of instances", strconv.FormatInt(int64(configuration.Replicas+1+7), 10), fmt.Sprintf("Min: %d and Max: %d", DefaultMinInstances, DefaultMaxInstances))},
		},
		{
			name:      "create a branch with no mode param fails",
			jsonBody:  map[string]any{"name": branch.Name},
			wantError: true,
			expectedError: ErrorInvalidParam{
				BranchName: branch.Name,
				Param:      "body",
				Message:    "failed to parse branch creation details - unknown discriminator value: ",
			},
		},
		{
			name:      "create a branch with wrong mode custom fails",
			jsonBody:  map[string]any{"name": branch.Name, "mode": "custom", "parentID": "some-id"},
			wantError: true,
			expectedError: ErrorInvalidParam{
				BranchName: branch.Name,
				Param:      "configuration",
				Message:    "configuration is required for 'custom' mode",
			},
		},
		{
			name:      "create a branch with wrong mode inherit fails",
			jsonBody:  map[string]any{"name": branch.Name, "mode": "inherit", "configuration": map[string]any{}},
			wantError: true,
			expectedError: ErrorInvalidParam{
				BranchName: branch.Name,
				Param:      "parentID",
				Message:    "parentId is required for 'inherit' mode",
			},
		},
		{
			name:      "create a branch with wrong mode random fails",
			jsonBody:  map[string]any{"name": branch.Name, "mode": "random", "configuration": map[string]any{}},
			wantError: true,
			expectedError: ErrorInvalidParam{
				BranchName: branch.Name,
				Param:      "body",
				Message:    "failed to parse branch creation details - unknown discriminator value: random",
			},
		},
		{
			name:      "create a branch with config replicas less than 0 fails",
			jsonBody:  map[string]any{"name": "test", "mode": "custom", "configuration": map[string]any{"replicas": -1}},
			wantError: true,
			expectedError: ErrorInvalidParam{
				BranchName: branch.Name,
				Param:      "configuration",
				Message:    "number of replicas must be at least zero",
			},
		},
		{
			name:             "create a branch with unavailable connection string does not fail",
			branch:           branch,
			connectionString: nil,
			jsonBody:         map[string]any{"name": branch.Name, "mode": "custom", "configuration": map[string]any{"image": configuration.Image, "replicas": configuration.Replicas, "storage": configuration.Storage, "region": configuration.Region, "instanceType": configuration.InstanceType}, "backupConfiguration": map[string]any{"retentionPeriod": 2, "backupTime": "2:23:23"}},
			setupMocks: func(capturedRequest **clustersv1.CreatePostgresClusterRequest) {
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.5"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.5").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Once()
				mockStore.EXPECT().CreateBranch(mock.Anything, apitest.TestOrganization, "project_id", "cell_id", createBranchConfig(branch.Name, nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID, projectID, cellID string, cfg *store.CreateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Nil(t, err)
				}).Return(&branch, nil).Once()
				mockStore.EXPECT().ListCells(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.Cell{{ID: "cell_id", RegionID: "region-id-1", Primary: true}}, nil).Once()
				mockStore.EXPECT().GetPrimaryCell(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Cell{ID: "cell_id", RegionID: "region-id-1", Primary: true}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1}}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPostgresParameters("xata.micro", mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(map[string]string{
					"max_connections": "50",
					"shared_buffers":  "256MB",
					"work_mem":        "2259kB",
				}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPreloadLibraries(mock.AnythingOfType("string")).Return([]string{"pg_stat_statements", "auto_explain"}, nil).Once()
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything,
					&clustersv1.CreatePostgresClusterRequest{
						Id: branch.ID,
						Configuration: &clustersv1.ClusterConfiguration{
							NumInstances: configuration.Replicas + 1,
							ImageName:    "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
							VcpuRequest:  "250m",
							VcpuLimit:    "2",
							Memory:       "1Gi",
							ScaleToZero:  defaultClustersScaleToZero(),
							PostgresConfigurationParameters: map[string]string{
								"max_connections": "50",
								"shared_buffers":  "256MB",
								"work_mem":        "2259kB",
							},
							PreloadLibraries: []string{"pg_stat_statements", "auto_explain"},
						},
						BackupConfiguration: &clustersv1.BackupConfiguration{
							BackupSchedule:  "0 23 23 * * 2",
							BackupRetention: "2d",
							BackupsEnabled:  true,
						},
						OrganizationId: apitest.TestOrganization,
						ProjectId:      "project_id",
					}).Return(&clustersv1.CreatePostgresClusterResponse{}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: configuration.Region, GatewayHostPort: "", BackupsEnabled: true}, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{
					Id:       branch.ID,
					Username: "superuser",
				}).Return(nil, fmt.Errorf("some credentials error")).Once()
				mockStore.EXPECT().GetProject(mock.Anything, apitest.TestOrganization, "project_id").Return(&project, nil).Once()
				storageSize := int32(250)
				mockAnalytics.EXPECT().Track(mock.Anything, events.NewBranchFromConfigurationEvent(
					apitest.TestOrganization, "project_id", branch.ID, configuration.Region,
					"postgres:17.5", configuration.InstanceType, int(configuration.Replicas), &storageSize)).Return().Once()
			},
			wantError: false,
		},
		{
			name:             "create a branch with correct description succeeds",
			branch:           branch,
			connectionString: new("postgresql://user:pass@123./xata?sslmode=require"),
			jsonBody:         map[string]any{"name": branch.Name, "description": correctDescription, "mode": "custom", "configuration": map[string]any{"image": configuration.Image, "replicas": configuration.Replicas, "storage": configuration.Storage, "region": configuration.Region, "instanceType": configuration.InstanceType}, "backupConfiguration": map[string]any{"retentionPeriod": 2, "backupTime": "2:23:23"}},
			setupMocks: func(capturedRequest **clustersv1.CreatePostgresClusterRequest) {
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.5"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.5").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Once()
				mockStore.EXPECT().CreateBranch(mock.Anything, apitest.TestOrganization, "project_id", "cell_id", createBranchConfig(branch.Name, nil, &correctDescription), mock.Anything).Run(func(ctx context.Context, organizationID, projectID, cellID string, cfg *store.CreateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Nil(t, err)
				}).Return(&branch, nil).Once()
				mockStore.EXPECT().ListCells(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.Cell{{ID: "cell_id", RegionID: "region-id-1", Primary: true}}, nil).Once()
				mockStore.EXPECT().GetPrimaryCell(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Cell{ID: "cell_id", RegionID: "region-id-1", Primary: true}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1}}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPostgresParameters("xata.micro", mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(map[string]string{
					"max_connections": "50",
					"shared_buffers":  "256MB",
					"work_mem":        "2259kB",
				}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPreloadLibraries(mock.AnythingOfType("string")).Return([]string{"pg_stat_statements", "auto_explain"}, nil).Once()
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything,
					&clustersv1.CreatePostgresClusterRequest{
						Id: branch.ID,
						Configuration: &clustersv1.ClusterConfiguration{
							NumInstances: configuration.Replicas + 1,
							ImageName:    "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
							VcpuRequest:  "250m",
							VcpuLimit:    "2",
							Memory:       "1Gi",
							ScaleToZero:  defaultClustersScaleToZero(),
							PostgresConfigurationParameters: map[string]string{
								"max_connections": "50",
								"shared_buffers":  "256MB",
								"work_mem":        "2259kB",
							},
							PreloadLibraries: []string{"pg_stat_statements", "auto_explain"},
						},
						BackupConfiguration: &clustersv1.BackupConfiguration{
							BackupSchedule:  "0 23 23 * * 2",
							BackupRetention: "2d",
							BackupsEnabled:  true,
						},
						OrganizationId: apitest.TestOrganization,
						ProjectId:      "project_id",
					}).Return(&clustersv1.CreatePostgresClusterResponse{}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: configuration.Region, GatewayHostPort: "", BackupsEnabled: true}, nil).Twice()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{
					Id:       "123",
					Username: "superuser",
				}).Return(&clustersv1.GetPostgresClusterCredentialsResponse{
					Username: "user",
					Password: "pass",
				}, nil).Once()
				mockStore.EXPECT().GetProject(mock.Anything, apitest.TestOrganization, "project_id").Return(&project, nil).Once()
				storageSize := int32(250)
				mockAnalytics.EXPECT().Track(mock.Anything, events.NewBranchFromConfigurationEvent(
					apitest.TestOrganization, "project_id", branch.ID, configuration.Region,
					"postgres:17.5", configuration.InstanceType, int(configuration.Replicas), &storageSize)).Return().Once()
			},
			wantError: false,
		},
		{
			name:      "create a branch with invalid description fails",
			jsonBody:  map[string]any{"name": branch.Name, "description": invalidDescription, "configuration": map[string]any{"image": configuration.Image, "replicas": configuration.Replicas, "storage": configuration.Storage, "instanceType": configuration.InstanceType, "region": configuration.Region}},
			wantError: true,
			expectedError: ErrorInvalidDescription{
				Message:     fmt.Sprintf("invalid branch description %s", invalidDescription),
				Description: invalidDescription,
			},
		},
		{
			name:     "create a branch from non-existing parent fails",
			jsonBody: map[string]string{"name": childBranch.Name, "mode": "inherit", "parentID": *childBranch.ParentID},
			setupMocks: func(capturedRequest **clustersv1.CreatePostgresClusterRequest) {
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", *childBranch.ParentID).Return(nil, store.ErrBranchNotFound{ID: *childBranch.ParentID}).Once()
			},
			wantError: true,
			expectedError: ErrorBranchNotFound{
				BranchID: *childBranch.ParentID,
			},
		},
		{
			name:     "create a branch with already existing name fails",
			jsonBody: map[string]any{"name": branch.Name, "mode": "custom", "configuration": map[string]any{"image": configuration.Image, "replicas": configuration.Replicas, "storage": configuration.Storage, "region": configuration.Region, "instanceType": configuration.InstanceType}},
			setupMocks: func(capturedRequest **clustersv1.CreatePostgresClusterRequest) {
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.5"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.5").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: configuration.Region, GatewayHostPort: "", BackupsEnabled: true}, nil).Once()
				mockStore.EXPECT().ListCells(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.Cell{{ID: "cell_id", RegionID: "region-id-1", Primary: true}}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1}}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPostgresParameters("xata.micro", mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(map[string]string{
					"max_connections": "50",
					"shared_buffers":  "256MB",
					"work_mem":        "2259kB",
				}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPreloadLibraries(mock.AnythingOfType("string")).Return([]string{"pg_stat_statements", "auto_explain"}, nil).Once()
				mockStore.EXPECT().GetProject(mock.Anything, apitest.TestOrganization, "project_id").Return(&project, nil).Once()
				mockStore.EXPECT().CreateBranch(mock.Anything, apitest.TestOrganization, "project_id", "cell_id", createBranchConfig(branch.Name, nil, nil), mock.Anything).Return(nil, store.ErrBranchAlreadyExists{Name: branch.Name}).Once()
			},
			wantError: true,
			expectedError: store.ErrBranchAlreadyExists{
				Name: branch.Name,
			},
		},
		{
			name:      "create a branch fails with invalid instanceType",
			projectID: "project_id",
			jsonBody: map[string]any{
				"name": "invalid-instance-branch",
				"mode": "custom",
				"configuration": map[string]any{
					"image":        configuration.Image,
					"replicas":     configuration.Replicas,
					"region":       configuration.Region,
					"instanceType": "xata.invalid", // invalid type
				},
			},
			setupMocks: func(capturedRequest **clustersv1.CreatePostgresClusterRequest) {
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.5"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.5").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: configuration.Region, GatewayHostPort: "", BackupsEnabled: true}, nil).Once()
				mockStore.EXPECT().ListCells(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.Cell{{ID: "cell_id", RegionID: "region-id-1", Primary: true}}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1}}, nil).Once()
			},
			wantError: true,
			expectedError: ErrorInvalidParam{
				BranchName: "invalid-instance-branch",
				Param:      "instanceType",
				Message:    `instance type xata.invalid is not found`,
			},
		},
		{
			name:      "create a branch fails with invalid region",
			projectID: "project_id",
			jsonBody: map[string]any{
				"name": "invalid-region-branch",
				"mode": "custom",
				"configuration": map[string]any{
					"image":        configuration.Image,
					"replicas":     configuration.Replicas,
					"storage":      configuration.Storage,
					"region":       "invalid-region",
					"instanceType": configuration.InstanceType,
				},
			},
			setupMocks: func(capturedRequest **clustersv1.CreatePostgresClusterRequest) {
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.5"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.5").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "invalid-region").Return(nil, store.ErrRegionNotFound{ID: "invalid-region"}).Once()
			},
			wantError: true,
			expectedError: ErrorInvalidParam{
				BranchName: "invalid-region-branch",
				Param:      "configuration",
				Message:    `invalid region: region [invalid-region] not found`,
			},
		},
		{
			name:      "create a branch fails with invalid backup schedule",
			projectID: "project_id",
			jsonBody: map[string]any{
				"name": "invalid-schedule",
				"mode": "custom",
				"configuration": map[string]any{
					"image":        configuration.Image,
					"replicas":     configuration.Replicas,
					"storage":      configuration.Storage,
					"region":       configuration.Region,
					"instanceType": configuration.InstanceType,
				},
				"backupConfiguration": map[string]any{"retentionPeriod": 2, "backupTime": "2:83:23"},
			},
			wantError: true,
			expectedError: ErrorInvalidParam{
				BranchName: "invalid-schedule",
				Param:      "backup time",
				Message:    "invalid backup time format '2:83:23', must match format 'D:HH:MM' where D= * or 0-6, HH=00-23, MM=00-59",
			},
		},
		{
			name:      "create a branch fails with invalid backup retention time",
			projectID: "project_id",
			jsonBody: map[string]any{
				"name": "invalid-schedule",
				"mode": "custom",
				"configuration": map[string]any{
					"image":        configuration.Image,
					"replicas":     configuration.Replicas,
					"storage":      configuration.Storage,
					"region":       configuration.Region,
					"instanceType": configuration.InstanceType,
				},
				"backupConfiguration": map[string]any{"retentionPeriod": 1, "backupTime": "2:23:23"},
			},
			wantError: true,
			expectedError: ErrorInvalidParam{
				BranchName: "invalid-schedule",
				Param:      "backup retentionPeriod",
				Message:    "must be at least 2 days and maximum 35 days",
			},
		},
		{
			name:             "create a branch with custom preload libraries succeeds",
			projectID:        "project_id",
			branch:           branch,
			connectionString: new("postgresql://user:pass@123./xata?sslmode=require"),
			jsonBody: map[string]any{
				"name": branch.Name,
				"mode": "custom",
				"configuration": map[string]any{
					"image":            configuration.Image,
					"replicas":         configuration.Replicas,
					"region":           configuration.Region,
					"instanceType":     configuration.InstanceType,
					"preloadLibraries": []string{"pg_stat_statements", "pgaudit", "pg_cron"},
				},
				"backupConfiguration": map[string]any{"retentionPeriod": 2, "backupTime": "2:23:23"},
			},
			configuration: configuration,
			setupMocks: func(capturedRequest **clustersv1.CreatePostgresClusterRequest) {
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.5"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.5").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Once()
				mockStore.EXPECT().CreateBranch(mock.Anything, apitest.TestOrganization, "project_id", "cell_id", createBranchConfig(branch.Name, nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID, projectID, cellID string, cfg *store.CreateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Nil(t, err)
				}).Return(&branch, nil).Once()
				mockStore.EXPECT().ListCells(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.Cell{{ID: "cell_id", RegionID: "region-id-1", Primary: true}}, nil).Once()
				mockStore.EXPECT().GetPrimaryCell(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Cell{ID: "cell_id", RegionID: "region-id-1", Primary: true}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1}}, nil).Once()
				// Note: GetDefaultPreloadLibraries should NOT be called when custom libraries are provided
				// ValidatePreloadLibraries validates the custom libraries against available extensions for the image
				mockPostgresConfig.EXPECT().ValidatePreloadLibraries(mock.AnythingOfType("string"), []string{"pg_stat_statements", "pgaudit", "pg_cron"}).Return(nil).Once()
				// GetDefaultPostgresParameters returns parameters including extension-specific ones like pg_cron settings
				mockPostgresConfig.EXPECT().GetDefaultPostgresParameters("xata.micro", mock.AnythingOfType("int"), mock.AnythingOfType("string"), []string{"pg_stat_statements", "pgaudit", "pg_cron"}).Return(map[string]string{
					"max_connections":             "50",
					"shared_buffers":              "256MB",
					"work_mem":                    "2259kB",
					"cron.database_name":          "xata",
					"cron.use_background_workers": "on",
				}, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: branch.ID, Username: "superuser"}).
					Return(&clustersv1.GetPostgresClusterCredentialsResponse{Username: "user", Password: "pass"}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: configuration.Region, GatewayHostPort: "", BackupsEnabled: true}, nil).Twice()
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, mock.Anything).Run(func(ctx context.Context, req *clustersv1.CreatePostgresClusterRequest, opts ...grpc.CallOption) {
					*capturedRequest = req
				}).Return(&clustersv1.CreatePostgresClusterResponse{}, nil).Once()
				mockStore.EXPECT().GetProject(mock.Anything, apitest.TestOrganization, "project_id").Return(&project, nil).Once()
				mockAnalytics.EXPECT().Track(mock.Anything, events.NewBranchFromConfigurationEvent(
					apitest.TestOrganization, "project_id", branch.ID, configuration.Region,
					"postgres:17.5", configuration.InstanceType, int(configuration.Replicas), nil)).Return().Once()
			},
			validateCaptured: func(t *testing.T, req *clustersv1.CreatePostgresClusterRequest) {
				assert.NotNil(t, req, "CreatePostgresCluster should have been called")
				assert.Equal(t, []string{"pg_stat_statements", "pgaudit", "pg_cron"}, req.Configuration.PreloadLibraries)
				// Verify pg_cron parameters are included
				assert.Equal(t, "xata", req.Configuration.PostgresConfigurationParameters["cron.database_name"])
				assert.Equal(t, "on", req.Configuration.PostgresConfigurationParameters["cron.use_background_workers"])
			},
			wantError: false,
		},
		{
			name:             "create a branch with custom postgres configuration parameters succeeds",
			projectID:        "project_id",
			branch:           branch,
			connectionString: new("postgresql://user:pass@123./xata?sslmode=require"),
			jsonBody: map[string]any{
				"name": branch.Name,
				"mode": "custom",
				"configuration": map[string]any{
					"image":        configuration.Image,
					"replicas":     configuration.Replicas,
					"region":       configuration.Region,
					"instanceType": configuration.InstanceType,
					"postgresConfigurationParameters": map[string]string{
						"max_connections": "100",
						"work_mem":        "4MB",
						"custom_param":    "custom_value",
					},
				},
				"backupConfiguration": map[string]any{"retentionPeriod": 2, "backupTime": "2:23:23"},
			},
			configuration: configuration,
			setupMocks: func(capturedRequest **clustersv1.CreatePostgresClusterRequest) {
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.5"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.5").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Once()
				mockStore.EXPECT().CreateBranch(mock.Anything, apitest.TestOrganization, "project_id", "cell_id", createBranchConfig(branch.Name, nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID, projectID, cellID string, cfg *store.CreateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Nil(t, err)
				}).Return(&branch, nil).Once()
				mockStore.EXPECT().ListCells(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.Cell{{ID: "cell_id", RegionID: "region-id-1", Primary: true}}, nil).Once()
				mockStore.EXPECT().GetPrimaryCell(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Cell{ID: "cell_id", RegionID: "region-id-1", Primary: true}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1}}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPreloadLibraries(mock.AnythingOfType("string")).Return([]string{"pg_stat_statements", "auto_explain"}, nil).Once()
				mockPostgresConfig.EXPECT().ValidateSettings("xata.micro", map[string]string{
					"max_connections": "100",
					"work_mem":        "4MB",
					"custom_param":    "custom_value",
				}, mock.AnythingOfType("int"), mock.AnythingOfType("string"), []string{"pg_stat_statements", "auto_explain"}).Return(nil, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPostgresParameters("xata.micro", mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(map[string]string{
					"max_connections": "50",
					"shared_buffers":  "256MB",
					"work_mem":        "2259kB",
				}, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: branch.ID, Username: "superuser"}).
					Return(&clustersv1.GetPostgresClusterCredentialsResponse{Username: "user", Password: "pass"}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: configuration.Region, GatewayHostPort: "", BackupsEnabled: true}, nil).Twice()
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, mock.Anything).Run(func(ctx context.Context, req *clustersv1.CreatePostgresClusterRequest, opts ...grpc.CallOption) {
					*capturedRequest = req
				}).Return(&clustersv1.CreatePostgresClusterResponse{}, nil).Once()
				mockStore.EXPECT().GetProject(mock.Anything, apitest.TestOrganization, "project_id").Return(&project, nil).Once()
				mockAnalytics.EXPECT().Track(mock.Anything, events.NewBranchFromConfigurationEvent(
					apitest.TestOrganization, "project_id", branch.ID, configuration.Region,
					"postgres:17.5", configuration.InstanceType, int(configuration.Replicas), nil)).Return().Once()
			},
			validateCaptured: func(t *testing.T, req *clustersv1.CreatePostgresClusterRequest) {
				assert.NotNil(t, req, "CreatePostgresCluster should have been called")
				// Custom parameters should override defaults
				assert.Equal(t, "100", req.Configuration.PostgresConfigurationParameters["max_connections"])
				assert.Equal(t, "4MB", req.Configuration.PostgresConfigurationParameters["work_mem"])
				assert.Equal(t, "custom_value", req.Configuration.PostgresConfigurationParameters["custom_param"])
				// Default parameter should be preserved
				assert.Equal(t, "256MB", req.Configuration.PostgresConfigurationParameters["shared_buffers"])
			},
			wantError: false,
		},
		{
			name:             "create a branch with both custom preload libraries and postgres parameters succeeds",
			projectID:        "project_id",
			branch:           branch,
			connectionString: new("postgresql://user:pass@123./xata?sslmode=require"),
			jsonBody: map[string]any{
				"name": branch.Name,
				"mode": "custom",
				"configuration": map[string]any{
					"image":            configuration.Image,
					"replicas":         configuration.Replicas,
					"region":           configuration.Region,
					"instanceType":     configuration.InstanceType,
					"preloadLibraries": []string{"pg_stat_statements", "pgaudit"},
					"postgresConfigurationParameters": map[string]string{
						"max_connections": "200",
					},
				},
				"backupConfiguration": map[string]any{"retentionPeriod": 2, "backupTime": "2:23:23"},
			},
			configuration: configuration,
			setupMocks: func(capturedRequest **clustersv1.CreatePostgresClusterRequest) {
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.5"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.5").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Once()
				mockStore.EXPECT().CreateBranch(mock.Anything, apitest.TestOrganization, "project_id", "cell_id", createBranchConfig(branch.Name, nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID, projectID, cellID string, cfg *store.CreateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Nil(t, err)
				}).Return(&branch, nil).Once()
				mockStore.EXPECT().ListCells(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.Cell{{ID: "cell_id", RegionID: "region-id-1", Primary: true}}, nil).Once()
				mockStore.EXPECT().GetPrimaryCell(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Cell{ID: "cell_id", RegionID: "region-id-1", Primary: true}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1}}, nil).Once()
				// Note: GetDefaultPreloadLibraries should NOT be called when custom libraries are provided
				mockPostgresConfig.EXPECT().ValidatePreloadLibraries(mock.AnythingOfType("string"), []string{"pg_stat_statements", "pgaudit"}).Return(nil).Once()
				mockPostgresConfig.EXPECT().ValidateSettings("xata.micro", map[string]string{
					"max_connections": "200",
				}, mock.AnythingOfType("int"), mock.AnythingOfType("string"), []string{"pg_stat_statements", "pgaudit"}).Return(nil, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPostgresParameters("xata.micro", mock.AnythingOfType("int"), mock.AnythingOfType("string"), []string{"pg_stat_statements", "pgaudit"}).Return(map[string]string{
					"max_connections": "50",
					"shared_buffers":  "256MB",
					"work_mem":        "2259kB",
				}, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: branch.ID, Username: "superuser"}).
					Return(&clustersv1.GetPostgresClusterCredentialsResponse{Username: "user", Password: "pass"}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: configuration.Region, GatewayHostPort: "", BackupsEnabled: true}, nil).Twice()
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, mock.Anything).Run(func(ctx context.Context, req *clustersv1.CreatePostgresClusterRequest, opts ...grpc.CallOption) {
					*capturedRequest = req
				}).Return(&clustersv1.CreatePostgresClusterResponse{}, nil).Once()
				mockStore.EXPECT().GetProject(mock.Anything, apitest.TestOrganization, "project_id").Return(&project, nil).Once()
				mockAnalytics.EXPECT().Track(mock.Anything, events.NewBranchFromConfigurationEvent(
					apitest.TestOrganization, "project_id", branch.ID, configuration.Region,
					"postgres:17.5", configuration.InstanceType, int(configuration.Replicas), nil)).Return().Once()
			},
			validateCaptured: func(t *testing.T, req *clustersv1.CreatePostgresClusterRequest) {
				assert.NotNil(t, req, "CreatePostgresCluster should have been called")
				// Custom preload libraries
				assert.Equal(t, []string{"pg_stat_statements", "pgaudit"}, req.Configuration.PreloadLibraries)
				// Custom parameter should override default
				assert.Equal(t, "200", req.Configuration.PostgresConfigurationParameters["max_connections"])
				// Default parameters should be preserved
				assert.Equal(t, "256MB", req.Configuration.PostgresConfigurationParameters["shared_buffers"])
				assert.Equal(t, "2259kB", req.Configuration.PostgresConfigurationParameters["work_mem"])
			},
			wantError: false,
		},
		{
			name:      "create a branch with invalid preload libraries fails",
			projectID: "project_id",
			branch:    branch,
			jsonBody: map[string]any{
				"name": branch.Name,
				"mode": "custom",
				"configuration": map[string]any{
					"image":            configuration.Image,
					"replicas":         configuration.Replicas,
					"region":           configuration.Region,
					"instanceType":     configuration.InstanceType,
					"preloadLibraries": []string{"invalid_library", "another_invalid"},
				},
				"backupConfiguration": map[string]any{"retentionPeriod": 2, "backupTime": "2:23:23"},
			},
			configuration: configuration,
			setupMocks: func(capturedRequest **clustersv1.CreatePostgresClusterRequest) {
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.5"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.5").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: configuration.Region, GatewayHostPort: "", BackupsEnabled: true}, nil).Once()
				mockStore.EXPECT().ListCells(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.Cell{{ID: "cell_id", RegionID: "region-id-1", Primary: true}}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1}}, nil).Once()
				mockPostgresConfig.EXPECT().ValidatePreloadLibraries(mock.AnythingOfType("string"), []string{"invalid_library", "another_invalid"}).Return(errors.New("invalid preload library: invalid_library")).Once()
			},
			wantError:     true,
			expectedError: ErrorInvalidParam{BranchName: branch.Name, Param: "preloadLibraries", Message: "invalid preload library: invalid_library"},
		},
		{
			name:      "create a branch with invalid postgres configuration parameters fails",
			projectID: "project_id",
			branch:    branch,
			jsonBody: map[string]any{
				"name": branch.Name,
				"mode": "custom",
				"configuration": map[string]any{
					"image":        configuration.Image,
					"replicas":     configuration.Replicas,
					"region":       configuration.Region,
					"instanceType": configuration.InstanceType,
					"postgresConfigurationParameters": map[string]string{
						"invalid_param":   "invalid_value",
						"max_connections": "invalid",
					},
				},
				"backupConfiguration": map[string]any{"retentionPeriod": 2, "backupTime": "2:23:23"},
			},
			configuration: configuration,
			setupMocks: func(capturedRequest **clustersv1.CreatePostgresClusterRequest) {
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.5"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.5").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: configuration.Region, GatewayHostPort: "", BackupsEnabled: true}, nil).Once()
				mockStore.EXPECT().ListCells(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.Cell{{ID: "cell_id", RegionID: "region-id-1", Primary: true}}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1}}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPreloadLibraries(mock.AnythingOfType("string")).Return([]string{"pg_stat_statements", "auto_explain"}, nil).Once()
				mockPostgresConfig.EXPECT().ValidateSettings("xata.micro", map[string]string{
					"invalid_param":   "invalid_value",
					"max_connections": "invalid",
				}, mock.AnythingOfType("int"), mock.AnythingOfType("string"), []string{"pg_stat_statements", "auto_explain"}).Return(map[string]error{
					"invalid_param":   errors.New("unknown parameter"),
					"max_connections": errors.New("must be a valid integer"),
				}, nil).Once()
			},
			wantError:     true,
			expectedError: ErrorInvalidParam{BranchName: branch.Name, Param: "postgresConfigurationParameters", Message: "invalid_param: unknown parameter; max_connections: must be a valid integer"},
		},
		{
			name:      "rejects experimental image when feature flag is disabled",
			projectID: "project_id",
			branch:    branch,
			jsonBody: map[string]any{
				"name": "experimental-branch",
				"mode": "custom",
				"configuration": map[string]any{
					"image":        "experimental:17.7",
					"replicas":     0,
					"region":       configuration.Region,
					"instanceType": configuration.InstanceType,
				},
			},
			setupMocks: func(capturedRequest **clustersv1.CreatePostgresClusterRequest) {
				// No mocks needed - validateImage fails before any store calls
			},
			wantError: true,
			expectedError: ErrorInvalidParam{
				BranchName: "experimental-branch",
				Param:      "configuration",
				Message:    "invalid image: image experimental:17.7 is not available",
			},
		},
		{
			name:      "rejects analytics image when feature flag is disabled",
			projectID: "project_id",
			branch:    branch,
			jsonBody: map[string]any{
				"name": "analytics-branch",
				"mode": "custom",
				"configuration": map[string]any{
					"image":        "analytics:17.7",
					"replicas":     0,
					"region":       configuration.Region,
					"instanceType": configuration.InstanceType,
				},
			},
			setupMocks: func(capturedRequest **clustersv1.CreatePostgresClusterRequest) {
				// No mocks needed - validateImage fails before any store calls
			},
			wantError: true,
			expectedError: ErrorInvalidParam{
				BranchName: "analytics-branch",
				Param:      "configuration",
				Message:    "invalid image: image analytics:17.7 is not available",
			},
		},
	}
	for _, tt := range createBranchTests {
		t.Run(tt.name, func(t *testing.T) {
			var capturedRequest *clustersv1.CreatePostgresClusterRequest
			if tt.setupMocks != nil {
				tt.setupMocks(&capturedRequest)
			}
			mockStore.EXPECT().AcquireProjectLock(mock.Anything, "project_id").Return(func() error { return nil }, nil).Maybe()

			c, rec := e.POST("/organizations/" + apitest.TestOrganization + "/projects/project_id/branches").WithJSONBody(tt.jsonBody).Context()
			err := handler.CreateBranch(c, apitest.TestOrganization, "project_id")
			if tt.wantError {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				assert.NoError(t, err)
				var gotBranch spec.BranchShortMetadata
				rec.MustCode(http.StatusCreated)
				rec.ReadBody(&gotBranch)
				assert.Equal(t, tt.branch.ID, gotBranch.Id)
				assert.Equal(t, tt.branch.Name, gotBranch.Name)
				assert.Equal(t, tt.branch.CreatedAt, gotBranch.CreatedAt)
				assert.Equal(t, tt.branch.UpdatedAt, gotBranch.UpdatedAt)
				assert.Equal(t, tt.branch.ParentID, gotBranch.ParentID)
				assert.Equal(t, tt.branch.Description, gotBranch.Description)
				assert.Equal(t, tt.branch.Region, gotBranch.Region)
				assert.Equal(t, tt.branch.PublicAccess, gotBranch.PublicAccess)
				assert.Equal(t, tt.connectionString, gotBranch.ConnectionString)

				if tt.validateCaptured != nil {
					tt.validateCaptured(t, capturedRequest)
				}
			}
		})
	}

	mockStore.AssertExpectations(t)
	mockClusters.AssertExpectations(t)
}

func TestCreateBranchDisabled(t *testing.T) {
	mockStore := mocks.NewProjectsStore(t)
	mockClusters := protomocks.NewClustersServiceClient(t)
	mockCells := cellsmock.NewCellsMock(t, mockClusters)

	feat := openfeaturetest.NewClient(map[openfeature.FeatureFlag]bool{flags.BranchCreationDisabled: true})
	sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
	handler := NewAPIHandler(feat, mockStore, mockCells, "", createNewSigNozClient(t), sched, analyticsmocks.NewClient(t), nil, nil)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

	c, _ := e.POST("/organizations/" + apitest.TestOrganization + "/projects").Context()
	err := handler.CreateBranch(c, apitest.TestOrganization, "project_id")
	assert.Error(t, err)
	assert.Equal(t, ErrorBranchCreationDisabled{}, err)
}

func TestRestoreFromBackup(t *testing.T) {
	sourceBranchID := "source_branch_123"
	restoredBranch := store.Branch{
		ID:             "restored_branch_id",
		Name:           "restored",
		ParentID:       nil,
		CellID:         "cell_id",
		Region:         "region-id-1",
		BackupsEnabled: true,
	}
	sourceBranch := store.Branch{
		ID:             sourceBranchID,
		Name:           "source",
		ParentID:       nil,
		CellID:         "cell_id",
		Region:         "region-id-1",
		BackupsEnabled: true,
	}
	project := store.Project{
		ID:        "project_id",
		Name:      "test",
		CreatedAt: time.Now(),
		ScaleToZero: store.ProjectScaleToZero{
			BaseBranches:  defaultScaleToZeroConfig(),
			ChildBranches: store.ScaleToZero{Enabled: true, InactivityPeriod: store.InactivityPeriod(15 * time.Minute)},
		},
	}
	configuration := spec.ClusterConfiguration{
		Replicas:     int32(0),
		Image:        "postgres:17.5",
		Storage:      new(defaultStorage),
		InstanceType: "xata.micro",
		Region:       "region-id-1",
	}

	tests := []struct {
		name                  string
		mode                  string
		body                  map[string]any // optional custom body, if nil uses default based on mode
		setupMocks            func(*mocks.ProjectsStore, *protomocks.ClustersServiceClient, *postgrescfgmocks.PostgresConfigProvider, *postgresversionsmocks.ImageProvider)
		isCellConnectionError bool
		clusterError          error
		expectedError         any
		expectSuccess         bool
	}{
		{
			name: "restore from backup with custom mode succeeds",
			mode: "custom",
			setupMocks: func(mockStore *mocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient, mockPostgresConfig *postgrescfgmocks.PostgresConfigProvider, mockImageProvider *postgresversionsmocks.ImageProvider) {
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", sourceBranchID).Return(&sourceBranch, nil).Once()
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.5"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.5").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Once()
				mockStore.EXPECT().ListCells(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.Cell{{ID: "cell_id", RegionID: "region-id-1", Primary: true}}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1}}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPostgresParameters("xata.micro", mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(map[string]string{
					"max_connections": "50",
					"shared_buffers":  "256MB",
					"work_mem":        "2259kB",
				}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPreloadLibraries(mock.AnythingOfType("string")).Return([]string{"pg_stat_statements", "auto_explain"}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: configuration.Region, GatewayHostPort: "", BackupsEnabled: true}, nil).Twice()
				mockStore.EXPECT().CreateBranch(mock.Anything, apitest.TestOrganization, "project_id", "cell_id", createBranchConfig(restoredBranch.Name, &sourceBranchID, nil), mock.Anything).Run(func(ctx context.Context, organizationID, projectID, cellID string, cfg *store.CreateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&restoredBranch)
					assert.Nil(t, err)
				}).Return(&restoredBranch, nil).Once()
				mockStore.EXPECT().GetPrimaryCell(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Cell{ID: "cell_id", RegionID: "region-id-1", Primary: true}, nil).Once()
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, mock.MatchedBy(func(req *clustersv1.CreatePostgresClusterRequest) bool {
					if cb, ok := req.DataSource.(*clustersv1.CreatePostgresClusterRequest_ContinuousBackup); ok {
						return cb.ContinuousBackup.ClusterId == sourceBranchID
					}
					return false
				})).Return(&clustersv1.CreatePostgresClusterResponse{}, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: restoredBranch.ID, Username: "superuser"}).
					Return(&clustersv1.GetPostgresClusterCredentialsResponse{Username: "user", Password: "pass"}, nil).Once()
				mockStore.EXPECT().GetProject(mock.Anything, apitest.TestOrganization, "project_id").Return(&project, nil).Once()
			},
			expectSuccess: true,
		},
		{
			name: "restore from backup with clone mode succeeds",
			mode: "clone",
			setupMocks: func(mockStore *mocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient, mockPostgresConfig *postgrescfgmocks.PostgresConfigProvider, mockImageProvider *postgresversionsmocks.ImageProvider) {
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", sourceBranch.ID).Return(&sourceBranch, nil).Twice()
				mockStore.EXPECT().GetPrimaryCell(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Cell{ID: "cell_id", RegionID: "region-id-1", Primary: true}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: "region-id-1", GatewayHostPort: "", BackupsEnabled: true}, nil).Once()
				mockStore.EXPECT().CreateBranch(mock.Anything, apitest.TestOrganization, "project_id", "cell_id", createBranchConfig(restoredBranch.Name, &sourceBranchID, nil), mock.Anything).Run(func(ctx context.Context, organizationID, projectID, cellID string, cfg *store.CreateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&restoredBranch)
					assert.Nil(t, err)
				}).Return(&restoredBranch, nil).Once()
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, mock.MatchedBy(func(req *clustersv1.CreatePostgresClusterRequest) bool {
					if cb, ok := req.DataSource.(*clustersv1.CreatePostgresClusterRequest_ContinuousBackup); ok {
						return cb.ContinuousBackup.ClusterId == sourceBranchID
					}
					return false
				})).Return(&clustersv1.CreatePostgresClusterResponse{}, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: restoredBranch.ID, Username: "superuser"}).
					Return(&clustersv1.GetPostgresClusterCredentialsResponse{Username: "user", Password: "pass"}, nil).Once()
				mockStore.EXPECT().GetProject(mock.Anything, apitest.TestOrganization, "project_id").Return(&project, nil).Once()
			},
			expectSuccess: true,
		},
		{
			name:                  "GetCellConnection fails",
			mode:                  "custom",
			isCellConnectionError: true,
			clusterError:          errors.New("connection failed"),
			expectedError:         errors.New("connection failed"),
			setupMocks: func(mockStore *mocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient, mockPostgresConfig *postgrescfgmocks.PostgresConfigProvider, mockImageProvider *postgresversionsmocks.ImageProvider) {
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", sourceBranchID).Return(&sourceBranch, nil).Once()
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.5"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.5").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Once()
				mockStore.EXPECT().ListCells(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.Cell{{ID: "cell_id", RegionID: "region-id-1", Primary: true}}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1}}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPostgresParameters("xata.micro", mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(map[string]string{
					"max_connections": "50",
					"shared_buffers":  "256MB",
					"work_mem":        "2259kB",
				}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPreloadLibraries(mock.AnythingOfType("string")).Return([]string{"pg_stat_statements", "auto_explain"}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: configuration.Region, GatewayHostPort: "", BackupsEnabled: true}, nil).Once()
				mockStore.EXPECT().GetProject(mock.Anything, apitest.TestOrganization, "project_id").Return(&project, nil).Once()
				mockStore.EXPECT().CreateBranch(mock.Anything, apitest.TestOrganization, "project_id", "cell_id", createBranchConfig(restoredBranch.Name, &sourceBranchID, nil), mock.Anything).Run(func(ctx context.Context, organizationID, projectID, cellID string, cfg *store.CreateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&restoredBranch)
					assert.Error(t, err)
				}).Return(nil, errors.New("connection failed")).Once()
			},
		},
		{
			name:          "CreatePostgresCluster returns NotFound",
			mode:          "custom",
			clusterError:  status.Error(codes.NotFound, "cluster not found"),
			expectedError: ErrorBranchNotFound{BranchID: sourceBranchID},
			setupMocks: func(mockStore *mocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient, mockPostgresConfig *postgrescfgmocks.PostgresConfigProvider, mockImageProvider *postgresversionsmocks.ImageProvider) {
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", sourceBranchID).Return(&sourceBranch, nil).Once()
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.5"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.5").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Once()
				mockStore.EXPECT().ListCells(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.Cell{{ID: "cell_id", RegionID: "region-id-1", Primary: true}}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1}}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPostgresParameters("xata.micro", mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(map[string]string{
					"max_connections": "50",
					"shared_buffers":  "256MB",
					"work_mem":        "2259kB",
				}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPreloadLibraries(mock.AnythingOfType("string")).Return([]string{"pg_stat_statements", "auto_explain"}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: configuration.Region, GatewayHostPort: "", BackupsEnabled: true}, nil).Once()
				mockStore.EXPECT().GetProject(mock.Anything, apitest.TestOrganization, "project_id").Return(&project, nil).Once()
				notFoundErr := status.Error(codes.NotFound, "cluster not found")
				mockStore.EXPECT().CreateBranch(mock.Anything, apitest.TestOrganization, "project_id", "cell_id", createBranchConfig(restoredBranch.Name, &sourceBranchID, nil), mock.Anything).Run(func(ctx context.Context, organizationID, projectID, cellID string, cfg *store.CreateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&restoredBranch)
					assert.Error(t, err)
				}).Return(nil, notFoundErr).Once()
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, mock.Anything).Return(nil, notFoundErr).Once()
			},
		},
		{
			name:          "CreatePostgresCluster returns InvalidArgument",
			mode:          "custom",
			clusterError:  status.Error(codes.InvalidArgument, "invalid configuration parameter"),
			expectedError: ErrorInvalidParam{BranchName: restoredBranch.Name, Param: "configuration", Message: "invalid configuration parameter"},
			setupMocks: func(mockStore *mocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient, mockPostgresConfig *postgrescfgmocks.PostgresConfigProvider, mockImageProvider *postgresversionsmocks.ImageProvider) {
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", sourceBranchID).Return(&sourceBranch, nil).Once()
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.5"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.5").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Once()
				mockStore.EXPECT().ListCells(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.Cell{{ID: "cell_id", RegionID: "region-id-1", Primary: true}}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1}}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPostgresParameters("xata.micro", mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(map[string]string{
					"max_connections": "50",
					"shared_buffers":  "256MB",
					"work_mem":        "2259kB",
				}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPreloadLibraries(mock.AnythingOfType("string")).Return([]string{"pg_stat_statements", "auto_explain"}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: configuration.Region, GatewayHostPort: "", BackupsEnabled: true}, nil).Once()
				mockStore.EXPECT().GetProject(mock.Anything, apitest.TestOrganization, "project_id").Return(&project, nil).Once()
				invalidArgErr := status.Error(codes.InvalidArgument, "invalid configuration parameter")
				mockStore.EXPECT().CreateBranch(mock.Anything, apitest.TestOrganization, "project_id", "cell_id", createBranchConfig(restoredBranch.Name, &sourceBranchID, nil), mock.Anything).Run(func(ctx context.Context, organizationID, projectID, cellID string, cfg *store.CreateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&restoredBranch)
					assert.Error(t, err)
				}).Return(nil, invalidArgErr).Once()
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, mock.Anything).Return(nil, invalidArgErr).Once()
			},
		},
		{
			name:          "CreatePostgresCluster returns Internal error",
			mode:          "custom",
			clusterError:  status.Error(codes.Internal, "internal server error"),
			expectedError: status.Error(codes.Internal, "internal server error"),
			setupMocks: func(mockStore *mocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient, mockPostgresConfig *postgrescfgmocks.PostgresConfigProvider, mockImageProvider *postgresversionsmocks.ImageProvider) {
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", sourceBranchID).Return(&sourceBranch, nil).Once()
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.5"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.5").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Once()
				mockStore.EXPECT().ListCells(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.Cell{{ID: "cell_id", RegionID: "region-id-1", Primary: true}}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1}}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPostgresParameters("xata.micro", mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(map[string]string{
					"max_connections": "50",
					"shared_buffers":  "256MB",
					"work_mem":        "2259kB",
				}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPreloadLibraries(mock.AnythingOfType("string")).Return([]string{"pg_stat_statements", "auto_explain"}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: configuration.Region, GatewayHostPort: "", BackupsEnabled: true}, nil).Once()
				mockStore.EXPECT().GetProject(mock.Anything, apitest.TestOrganization, "project_id").Return(&project, nil).Once()
				internalErr := status.Error(codes.Internal, "internal server error")
				mockStore.EXPECT().CreateBranch(mock.Anything, apitest.TestOrganization, "project_id", "cell_id", createBranchConfig(restoredBranch.Name, &sourceBranchID, nil), mock.Anything).Run(func(ctx context.Context, organizationID, projectID, cellID string, cfg *store.CreateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&restoredBranch)
					assert.Error(t, err)
				}).Return(nil, internalErr).Once()
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, mock.Anything).Return(nil, internalErr).Once()
			},
		},
		{
			name: "backup config with backups disabled returns error",
			mode: "custom",
			body: map[string]any{
				"name":                "restored",
				"configuration":       map[string]any{"image": "postgres:17.5", "replicas": int32(0), "region": "region-id-1", "instanceType": "xata.micro"},
				"backupConfiguration": map[string]any{"retentionPeriod": 7},
			},
			expectedError: ErrorInvalidParam{BranchName: "restored", Param: "backupConfiguration", Message: "backup configuration cannot be specified when backups are disabled in the selected region"},
			setupMocks: func(mockStore *mocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient, mockPostgresConfig *postgrescfgmocks.PostgresConfigProvider, mockImageProvider *postgresversionsmocks.ImageProvider) {
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", sourceBranchID).Return(&sourceBranch, nil).Once()
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.5"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.5").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Once()
				mockStore.EXPECT().ListCells(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.Cell{{ID: "cell_id", RegionID: "region-id-1", Primary: true}}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1}}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPostgresParameters("xata.micro", mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(map[string]string{
					"max_connections": "50",
					"shared_buffers":  "256MB",
					"work_mem":        "2259kB",
				}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPreloadLibraries(mock.AnythingOfType("string")).Return([]string{"pg_stat_statements", "auto_explain"}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: "region-id-1", GatewayHostPort: "", BackupsEnabled: false}, nil).Once()
			},
		},
		{
			name: "restore with mismatched region returns error",
			mode: "custom",
			body: map[string]any{
				"name":          "restored",
				"configuration": map[string]any{"image": "postgres:17.5", "replicas": int32(0), "region": "different-region", "instanceType": "xata.micro"},
			},
			expectedError: ErrorInvalidParam{BranchName: "restored", Param: "region", Message: "restore must be in the same region as the source branch"},
			setupMocks: func(mockStore *mocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient, mockPostgresConfig *postgrescfgmocks.PostgresConfigProvider, mockImageProvider *postgresversionsmocks.ImageProvider) {
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", sourceBranchID).Return(&sourceBranch, nil).Once()
			},
		},
		{
			name:          "source branch not found returns error",
			mode:          "custom",
			expectedError: ErrorBranchNotFound{BranchID: sourceBranchID},
			setupMocks: func(mockStore *mocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient, mockPostgresConfig *postgrescfgmocks.PostgresConfigProvider, mockImageProvider *postgresversionsmocks.ImageProvider) {
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", sourceBranchID).Return(nil, store.ErrBranchNotFound{ID: sourceBranchID}).Once()
			},
		},
		{
			name:          "DescribeBranch fails returns error",
			mode:          "custom",
			expectedError: errors.New("database error"),
			setupMocks: func(mockStore *mocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient, mockPostgresConfig *postgrescfgmocks.PostgresConfigProvider, mockImageProvider *postgresversionsmocks.ImageProvider) {
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", sourceBranchID).Return(nil, errors.New("database error")).Once()
			},
		},
		{
			name: "restore with empty region defaults to source branch region",
			mode: "custom",
			body: map[string]any{
				"name":          "restored",
				"configuration": map[string]any{"image": "postgres:17.5", "replicas": int32(0), "region": "", "instanceType": "xata.micro"},
			},
			expectSuccess: true,
			setupMocks: func(mockStore *mocks.ProjectsStore, mockClusters *protomocks.ClustersServiceClient, mockPostgresConfig *postgrescfgmocks.PostgresConfigProvider, mockImageProvider *postgresversionsmocks.ImageProvider) {
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", sourceBranchID).Return(&sourceBranch, nil).Once()
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.5"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.5").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Once()
				mockStore.EXPECT().ListCells(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.Cell{{ID: "cell_id", RegionID: "region-id-1", Primary: true}}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1}}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPostgresParameters("xata.micro", mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(map[string]string{
					"max_connections": "50",
					"shared_buffers":  "256MB",
					"work_mem":        "2259kB",
				}, nil).Once()
				mockPostgresConfig.EXPECT().GetDefaultPreloadLibraries(mock.AnythingOfType("string")).Return([]string{"pg_stat_statements", "auto_explain"}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: "region-id-1", GatewayHostPort: "", BackupsEnabled: true}, nil).Twice()
				mockStore.EXPECT().CreateBranch(mock.Anything, apitest.TestOrganization, "project_id", "cell_id", createBranchConfig(restoredBranch.Name, &sourceBranchID, nil), mock.Anything).Run(func(ctx context.Context, organizationID, projectID, cellID string, cfg *store.CreateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&restoredBranch)
					assert.Nil(t, err)
				}).Return(&restoredBranch, nil).Once()
				mockStore.EXPECT().GetPrimaryCell(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Cell{ID: "cell_id", RegionID: "region-id-1", Primary: true}, nil).Once()
				mockClusters.EXPECT().CreatePostgresCluster(mock.Anything, mock.MatchedBy(func(req *clustersv1.CreatePostgresClusterRequest) bool {
					if cb, ok := req.DataSource.(*clustersv1.CreatePostgresClusterRequest_ContinuousBackup); ok {
						return cb.ContinuousBackup.ClusterId == sourceBranchID
					}
					return false
				})).Return(&clustersv1.CreatePostgresClusterResponse{}, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: restoredBranch.ID, Username: "superuser"}).
					Return(&clustersv1.GetPostgresClusterCredentialsResponse{Username: "user", Password: "pass"}, nil).Once()
				mockStore.EXPECT().GetProject(mock.Anything, apitest.TestOrganization, "project_id").Return(&project, nil).Once()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStore := mocks.NewProjectsStore(t)
			mockClusters := protomocks.NewClustersServiceClient(t)
			mockPostgresConfig := postgrescfgmocks.NewPostgresConfigProvider(t)
			mockImageProvider := postgresversionsmocks.NewImageProvider(t)

			var mockCells cells.Cells
			if tt.isCellConnectionError {
				mockCells = cellsmock.NewCells(t)
				if cellsMock, ok := mockCells.(*cellsmock.Cells); ok {
					cellsMock.EXPECT().GetCellConnection(mock.Anything, mock.Anything, mock.Anything).Return(nil, tt.clusterError).Once()
				}
			} else {
				mockCells = cellsmock.NewCellsMock(t, mockClusters)
			}

			feat := openfeaturetest.NewClient(nil)
			sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
			mockAnalytics := analyticsmocks.NewClient(t)
			if tt.expectSuccess {
				mockAnalytics.EXPECT().Track(mock.Anything, mock.Anything).Return()
			}
			handler := NewAPIHandler(feat, mockStore, mockCells, "", createNewSigNozClient(t), sched, mockAnalytics, mockPostgresConfig, mockImageProvider)
			e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

			tt.setupMocks(mockStore, mockClusters, mockPostgresConfig, mockImageProvider)

			var body map[string]any
			if tt.body != nil {
				body = tt.body
			} else if tt.mode == "custom" {
				body = map[string]any{"name": restoredBranch.Name, "configuration": map[string]any{"image": configuration.Image, "replicas": configuration.Replicas, "region": configuration.Region, "instanceType": configuration.InstanceType}}
			} else {
				// No configuration means inherit from source branch
				body = map[string]any{"name": restoredBranch.Name}
			}

			c, rec := e.POST("/organizations/" + apitest.TestOrganization + "/projects/project_id/backups/" + sourceBranchID + "/restore").
				WithJSONBody(body).
				Context()

			err := handler.RestoreFromBackup(c, apitest.TestOrganization, "project_id", sourceBranchID)

			if tt.expectSuccess {
				assert.NoError(t, err)
				assert.Equal(t, http.StatusCreated, rec.Code)
			} else {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			}
		})
	}
}

func TestListBranches(t *testing.T) {
	time, _ := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")

	mockStore := mocks.NewProjectsStore(t)
	mockClusters := protomocks.NewClustersServiceClient(t)
	mockCells := cellsmock.NewCellsMock(t, mockClusters)

	feat := openfeaturetest.NewClient(nil)
	sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
	handler := NewAPIHandler(feat, mockStore, mockCells, "", createNewSigNozClient(t), sched, analyticsmocks.NewClient(t), nil, nil)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

	branches := []store.Branch{
		{
			ID:        "1",
			Name:      "test1",
			CreatedAt: time,
			UpdatedAt: time,
			Region:    "us-east-1",
		}, {
			ID:        "2",
			Name:      "test2",
			CreatedAt: time,
			UpdatedAt: time,
			Region:    "us-east-1",
		},
	}

	// test project without branches
	mockStore.EXPECT().ListBranches(mock.Anything, apitest.TestOrganization, "project_id").Return(nil, nil).Once()

	c, rec := e.GET("/organizations/" + apitest.TestOrganization + "/projects/project_id/branches").Context()
	err := handler.ListBranches(c, apitest.TestOrganization, "project_id")
	assert.Nil(t, err)

	var resp struct {
		Branches []spec.BranchShortMetadata `json:"branches"`
	}
	rec.MustCode(http.StatusOK)
	rec.ReadBody(&resp)
	assert.Equal(t, 0, len(resp.Branches))
	assert.NotNil(t, resp.Branches)

	mockStore.EXPECT().ListBranches(mock.Anything, apitest.TestOrganization, "project_id").Return(branches, nil).Once()

	c, rec = e.GET("/organizations/" + apitest.TestOrganization + "/projects/project_id/branches").Context()
	err = handler.ListBranches(c, apitest.TestOrganization, "project_id")
	assert.Nil(t, err)

	rec.MustCode(http.StatusOK)
	rec.ReadBody(&resp)
	assert.Equal(t, len(branches), len(resp.Branches))
	for i, branch := range branches {
		response := resp.Branches[i]
		assert.Equal(t, branch.ID, response.Id)
		assert.Equal(t, branch.Name, response.Name)
		assert.Equal(t, branch.CreatedAt, response.CreatedAt)
		assert.Equal(t, branch.UpdatedAt, response.UpdatedAt)
		assert.Equal(t, branch.Region, response.Region)
	}
	mockStore.AssertExpectations(t)
}

func TestGetBackup(t *testing.T) {
	oneDay, _ := time.ParseDuration("24h")
	yesterday := time.Now().Round(time.Minute).UTC().Add(-oneDay)
	latestRestore := time.Now().Round(time.Minute).UTC().Add(-5 * time.Minute)
	// backupID == branchID since backups are tied to branches
	backupID := "branch-test-123"
	projectID := "project_id"
	cellID := "cell-1"
	description := "Continuous backup for branch " + backupID

	tests := map[string]struct {
		backupID       string
		projectID      string // Add projectID to override the default
		setupMocks     func() (*mocks.ProjectsStore, *protomocks.ClustersServiceClient)
		expectedStatus int
		expectedBackup *spec.BackupMetadata
		expectError    bool
	}{
		"get existing backup successfully": {
			backupID: backupID,
			setupMocks: func() (*mocks.ProjectsStore, *protomocks.ClustersServiceClient) {
				mockStore := mocks.NewProjectsStore(t)
				mockClusters := protomocks.NewClustersServiceClient(t)

				branch := &store.Branch{
					ID:        backupID,
					Name:      "test-branch",
					CreatedAt: yesterday,
					UpdatedAt: yesterday,
					CellID:    cellID,
				}

				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, projectID, backupID).Return(branch, nil).Once()

				// Mock GetObjectStore call
				mockClusters.EXPECT().GetObjectStore(mock.Anything, &clustersv1.GetObjectStoreRequest{
					Id: backupID,
				}).Return(&clustersv1.GetObjectStoreResponse{
					Status: &clustersv1.ObjectStoreStatus{
						ServerRecoveryWindow: map[string]*clustersv1.RecoveryWindow{
							backupID: {
								FirstRecoverabilityPoint: yesterday.Add(-oneDay * 5).Format(backupTimestampLayout),
								LastSuccessfulBackupTime: latestRestore.Format(backupTimestampLayout),
								LastFailedBackupTime:     "",
							},
						},
					},
				}, nil).Once()

				return mockStore, mockClusters
			},
			expectedStatus: http.StatusOK,
			expectedBackup: &spec.BackupMetadata{
				Id:              backupID,
				BranchID:        backupID,
				LatestRestore:   &latestRestore,
				EarliestRestore: new(yesterday.Add(-oneDay * 5)),
				Description:     description,
			},
			expectError: false,
		},
		"backup not found when branch not found": {
			backupID: "non-existent-backup",
			setupMocks: func() (*mocks.ProjectsStore, *protomocks.ClustersServiceClient) {
				mockStore := mocks.NewProjectsStore(t)
				mockClusters := protomocks.NewClustersServiceClient(t)

				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, projectID, "non-existent-backup").Return(nil, store.ErrBranchNotFound{ID: "non-existent-backup"}).Once()

				return mockStore, mockClusters
			},
			expectedStatus: http.StatusNotFound,
			expectError:    true,
		},
		"backup not found with empty ID": {
			backupID: "",
			setupMocks: func() (*mocks.ProjectsStore, *protomocks.ClustersServiceClient) {
				mockStore := mocks.NewProjectsStore(t)
				mockClusters := protomocks.NewClustersServiceClient(t)

				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, projectID, "").Return(nil, store.ErrBranchNotFound{ID: ""}).Once()

				return mockStore, mockClusters
			},
			expectedStatus: http.StatusNotFound,
			expectError:    true,
		},
		"unauthorized project access": {
			backupID:  backupID,
			projectID: "wrong-project",
			setupMocks: func() (*mocks.ProjectsStore, *protomocks.ClustersServiceClient) {
				mockStore := mocks.NewProjectsStore(t)
				mockClusters := protomocks.NewClustersServiceClient(t)

				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "wrong-project", backupID).Return(nil, store.ErrProjectNotFound{ID: "wrong-project"}).Once()

				return mockStore, mockClusters
			},
			expectedStatus: http.StatusNotFound,
			expectError:    true,
		},
		"object store service error": {
			backupID: backupID,
			setupMocks: func() (*mocks.ProjectsStore, *protomocks.ClustersServiceClient) {
				mockStore := mocks.NewProjectsStore(t)
				mockClusters := protomocks.NewClustersServiceClient(t)

				branch := &store.Branch{
					ID:        backupID,
					Name:      "test-branch",
					CreatedAt: yesterday,
					UpdatedAt: yesterday,
					CellID:    cellID,
				}

				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, projectID, backupID).Return(branch, nil).Once()

				// Mock GetObjectStore call failure
				mockClusters.EXPECT().GetObjectStore(mock.Anything, &clustersv1.GetObjectStoreRequest{
					Id: backupID,
				}).Return(nil, errors.New("object store service unavailable")).Once()

				return mockStore, mockClusters
			},
			expectedStatus: http.StatusInternalServerError,
			expectError:    true,
		},
		"invalid recovery window data": {
			backupID: backupID,
			setupMocks: func() (*mocks.ProjectsStore, *protomocks.ClustersServiceClient) {
				mockStore := mocks.NewProjectsStore(t)
				mockClusters := protomocks.NewClustersServiceClient(t)

				branch := &store.Branch{
					ID:        backupID,
					Name:      "test-branch",
					CreatedAt: yesterday,
					UpdatedAt: yesterday,
					CellID:    cellID,
				}

				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, projectID, backupID).Return(branch, nil).Once()

				// Mock GetObjectStore call with invalid timestamp format
				mockClusters.EXPECT().GetObjectStore(mock.Anything, &clustersv1.GetObjectStoreRequest{
					Id: backupID,
				}).Return(&clustersv1.GetObjectStoreResponse{
					Status: &clustersv1.ObjectStoreStatus{
						ServerRecoveryWindow: map[string]*clustersv1.RecoveryWindow{
							backupID: {
								FirstRecoverabilityPoint: "invalid-timestamp",
								LastSuccessfulBackupTime: "invalid-timestamp",
								LastFailedBackupTime:     "",
							},
						},
					},
				}, nil).Once()

				return mockStore, mockClusters
			},
			expectedStatus: http.StatusInternalServerError,
			expectError:    true,
		},
	}

	for name, testCase := range tests {
		t.Run(name, func(t *testing.T) {
			// Setup mocks for this test case
			mockStore, mockClusters := testCase.setupMocks()
			mockCells := cellsmock.NewCellsMock(t, mockClusters)

			feat := openfeaturetest.NewClient(nil)
			sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
			handler := NewAPIHandler(feat, mockStore, mockCells, "", createNewSigNozClient(t), sched, analyticsmocks.NewClient(t), nil, nil)
			e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

			// Make the request
			testProjectID := projectID
			if testCase.projectID != "" {
				testProjectID = testCase.projectID
			}
			c, rec := e.GET("/organizations/" + apitest.TestOrganization + "/projects/projectID/backups/" + testCase.backupID).Context()
			err := handler.GetBackup(c, apitest.TestOrganization, testProjectID, testCase.backupID)

			if testCase.expectError {
				assert.NotNil(t, err)
				// The error handling is done by echo middleware, so we don't check exact status codes here
			} else {
				assert.Nil(t, err)
				rec.MustCode(testCase.expectedStatus)

				if testCase.expectedBackup != nil {
					var resp spec.BackupMetadata
					rec.ReadBody(&resp)
					assert.Equal(t, *testCase.expectedBackup, resp)
				}
			}

			mockStore.AssertExpectations(t)
			mockClusters.AssertExpectations(t)
		})
	}
}

func TestGetBackupErrorTypes(t *testing.T) {
	t.Run("returns correct ErrorBackupNotFound type", func(t *testing.T) {
		mockStore := mocks.NewProjectsStore(t)
		mockClusters := protomocks.NewClustersServiceClient(t)
		mockCells := cellsmock.NewCellsMock(t, mockClusters)

		feat := openfeaturetest.NewClient(nil)
		sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
		handler := NewAPIHandler(feat, mockStore, mockCells, "", createNewSigNozClient(t), sched, analyticsmocks.NewClient(t), nil, nil)
		e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

		backupID := "missing-backup-123"
		projectID := "test-project"

		mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, projectID, backupID).Return(nil, store.ErrBranchNotFound{ID: backupID}).Once()

		c, _ := e.GET("/organizations/" + apitest.TestOrganization + "/projects/projectID/branches").Context()
		err := handler.GetBackup(c, apitest.TestOrganization, projectID, backupID)
		require.NotNil(t, err)
		var apiErr ErrorBackupNotFound
		require.True(t, errors.As(err, &apiErr), "Expected ErrorBackupNotFound type")
		assert.Equal(t, backupID, apiErr.ID)
		assert.Equal(t, http.StatusNotFound, apiErr.StatusCode())

		mockStore.AssertExpectations(t)
	})
}

func TestDescribeBranch(t *testing.T) {
	mockStore := mocks.NewProjectsStore(t)
	mockClusters := protomocks.NewClustersServiceClient(t)
	mockCells := cellsmock.NewCellsMock(t, mockClusters)
	mockPostgresConfig := postgrescfgmocks.NewPostgresConfigProvider(t)

	feat := openfeaturetest.NewClient(nil)
	sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
	apiHandler := NewAPIHandler(feat, mockStore, mockCells, "testdomain:5432", createNewSigNozClient(t), sched, analyticsmocks.NewClient(t), mockPostgresConfig, nil)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

	branch := store.Branch{
		ID:        "branchID",
		Name:      "test",
		CreatedAt: mustParseTime("2024-01-10T00:00:00Z"),
		UpdatedAt: mustParseTime("2024-01-10T00:00:00Z"),
		Region:    "region-id-1",
	}
	region := &store.Region{
		ID:              "region-id-1",
		GatewayHostPort: "footest.tld:1234",
	}
	credentials := &clustersv1.GetPostgresClusterCredentialsResponse{
		Username: "user",
		Password: "pass",
	}
	instanceTypes := []store.InstanceType{
		{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1},
		{Name: "xata.small", VCPUsRequest: 500, VCPUsLimit: 2000, RAM: 2},
	}

	scaleToZero := clustersv1.ScaleToZero{
		Enabled:                 false,
		InactivityPeriodMinutes: int64(defaultInactivityDuration.Minutes()),
	}

	wrongVcpu := "99"
	wrongMemory := "999"

	describeBranchTests := []struct {
		name           string
		setupMocks     func()
		expectedError  error
		expectedStatus int
		assertResponse func(t *testing.T, got spec.BranchMetadata)
	}{
		{
			name: "healthy branch",
			setupMocks: func() {
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", branch.ID).Return(&branch, nil).Once()
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: branch.ID}).Return(&clustersv1.DescribePostgresClusterResponse{
					Id: branch.ID,
					Configuration: &clustersv1.ClusterConfiguration{
						NumInstances:     1,
						StorageSize:      defaultStorage,
						VcpuRequest:      "500m",
						VcpuLimit:        "2",
						Memory:           "2",
						ImageName:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
						PreloadLibraries: []string{"pg_stat_statements", "auto_explain", "pg_cron"},
					},
					Status: &clustersv1.ClusterStatus{
						Status:             apiv1.PhaseHealthy,
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
						InstanceCount:      1,
						InstanceReadyCount: 1,
						Instances: map[string]*clustersv1.InstanceStatus{
							"1": {Status: apiv1.PodHealthy, Primary: true},
						},
					},
					BackupConfiguration: &clustersv1.BackupConfiguration{
						BackupSchedule:  "0 30 14 * * 0",
						BackupRetention: "7d",
					},
				}, nil).Once()
				mockPostgresConfig.EXPECT().FilterConfigurableParameters(mock.Anything, mock.AnythingOfType("int"), "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5", []string{"pg_stat_statements", "auto_explain", "pg_cron"}).Return(map[string]string{}).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: branch.ID, Username: "superuser"}).Return(credentials, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, branch.Region).Return(region, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, region.ID).Return(instanceTypes, nil).Once()
			},
			expectedStatus: http.StatusOK,
			assertResponse: func(t *testing.T, got spec.BranchMetadata) {
				assert.Equal(t, branch.ID, got.Id)
				assert.Equal(t, branch.Name, got.Name)
				assert.Equal(t, branch.CreatedAt, got.CreatedAt)
				assert.Equal(t, branch.UpdatedAt, got.UpdatedAt)
				assert.Equal(t, branch.ParentID, got.ParentID)
				assert.Equal(t, branch.Description, got.Description)
				assert.Equal(t, branch.PublicAccess, got.PublicAccess)
				assert.Equal(t, new("postgresql://user:pass@branchID.footest.tld:1234/xata?sslmode=require"), got.ConnectionString)
				assert.Equal(t, int32(0), got.Configuration.Replicas)
				assert.Equal(t, "xata.small", got.Configuration.InstanceType) // assuming VcpuLimit: 0.5, VcpuRequest: 2 / Ram 2 → xata.small
				assert.Equal(t, "postgres:17.5", got.Configuration.Image)
				assert.Equal(t, apiv1.PhaseHealthy, got.Status.Status)
				assert.Equal(t, 1, got.Status.InstanceCount)
				assert.Equal(t, 1, got.Status.InstanceReadyCount)
				require.Len(t, got.Status.Instances, 1)
				assert.True(t, got.Status.Instances[0].Primary)
				assert.Equal(t, branch.Region, got.Region)
				assert.Equal(t, scaleToZero.Enabled, got.ScaleToZero.Enabled)
				assert.Equal(t, int(scaleToZero.InactivityPeriodMinutes), got.ScaleToZero.InactivityPeriodMinutes)
				assert.NotNil(t, got.BackupConfiguration)
				assert.NotNil(t, got.BackupConfiguration.BackupTime)
				assert.Equal(t, "0:14:30", *got.BackupConfiguration.BackupTime)
				assert.NotNil(t, got.BackupConfiguration.RetentionPeriod)
				assert.Equal(t, int32(7), *got.BackupConfiguration.RetentionPeriod)
			},
		},
		{
			name: "hibernated branch",
			setupMocks: func() {
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", branch.ID).Return(&branch, nil).Once()
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: branch.ID}).Return(&clustersv1.DescribePostgresClusterResponse{
					Id: branch.ID,
					Configuration: &clustersv1.ClusterConfiguration{
						NumInstances:     1,
						StorageSize:      defaultStorage,
						VcpuRequest:      "500m",
						VcpuLimit:        "2",
						Memory:           "2",
						ImageName:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
						PreloadLibraries: []string{"pg_stat_statements", "auto_explain", "pg_cron"},
					},
					Status: &clustersv1.ClusterStatus{
						Status:             apiv1.PhaseHealthy,
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED,
						InstanceCount:      1,
						InstanceReadyCount: 0,
						Instances: map[string]*clustersv1.InstanceStatus{
							"1": {Status: clusters.InstanceStatusUnknown, Primary: true},
						},
					},
					BackupConfiguration: &clustersv1.BackupConfiguration{
						BackupSchedule:  "0 30 14 * * 0",
						BackupRetention: "7d",
					},
				}, nil).Once()
				mockPostgresConfig.EXPECT().FilterConfigurableParameters(mock.Anything, mock.AnythingOfType("int"), "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5", []string{"pg_stat_statements", "auto_explain", "pg_cron"}).Return(map[string]string{}).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: branch.ID, Username: "superuser"}).Return(credentials, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, branch.Region).Return(region, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, region.ID).Return(instanceTypes, nil).Once()
			},
			expectedStatus: http.StatusOK,
			assertResponse: func(t *testing.T, got spec.BranchMetadata) {
				assert.Equal(t, branch.ID, got.Id)
				assert.Equal(t, branch.Name, got.Name)
				assert.Equal(t, branch.CreatedAt, got.CreatedAt)
				assert.Equal(t, branch.UpdatedAt, got.UpdatedAt)
				assert.Equal(t, branch.ParentID, got.ParentID)
				assert.Equal(t, branch.Description, got.Description)
				assert.Equal(t, branch.PublicAccess, got.PublicAccess)
				assert.Equal(t, new("postgresql://user:pass@branchID.footest.tld:1234/xata?sslmode=require"), got.ConnectionString)
				assert.Equal(t, int32(0), got.Configuration.Replicas)
				assert.Equal(t, "xata.small", got.Configuration.InstanceType) // assuming Vcpu 2 / Ram 2 → xata.small
				assert.Equal(t, "postgres:17.5", got.Configuration.Image)
				assert.Equal(t, apiv1.PhaseHealthy, got.Status.Status)
				assert.Equal(t, clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED.String(), got.Status.StatusType)
				assert.Equal(t, 1, got.Status.InstanceCount)
				assert.Equal(t, 0, got.Status.InstanceReadyCount)
				require.Len(t, got.Status.Instances, 1)
				assert.True(t, got.Status.Instances[0].Primary)
				assert.Equal(t, clusters.InstanceStatusUnknown, got.Status.Instances[0].Status)
				assert.Equal(t, branch.Region, got.Region)
				assert.Equal(t, scaleToZero.Enabled, got.ScaleToZero.Enabled)
				assert.Equal(t, int(scaleToZero.InactivityPeriodMinutes), got.ScaleToZero.InactivityPeriodMinutes)
				assert.NotNil(t, got.BackupConfiguration)
				assert.NotNil(t, got.BackupConfiguration.BackupTime)
				assert.Equal(t, "0:14:30", *got.BackupConfiguration.BackupTime)
				assert.NotNil(t, got.BackupConfiguration.RetentionPeriod)
				assert.Equal(t, int32(7), *got.BackupConfiguration.RetentionPeriod)
			},
		},
		{
			name: "non-healthy branch",
			setupMocks: func() {
				mockStore.EXPECT().
					DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", branch.ID).
					Return(&branch, nil).Once()

				mockClusters.EXPECT().
					DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: branch.ID}).
					Return(&clustersv1.DescribePostgresClusterResponse{
						Id: branch.ID,
						Configuration: &clustersv1.ClusterConfiguration{
							NumInstances:     1,
							StorageSize:      1,
							VcpuRequest:      "500m",
							VcpuLimit:        "2",
							Memory:           "2",
							ImageName:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
							PreloadLibraries: []string{"pg_stat_statements", "auto_explain", "pg_cron"},
						},
						Status: &clustersv1.ClusterStatus{
							Status:             apiv1.PhaseImageCatalogError,
							StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_FAULT,
							InstanceCount:      2,
							InstanceReadyCount: 1,
							Instances: map[string]*clustersv1.InstanceStatus{
								"1": {Status: apiv1.PodHealthy, Primary: true},
								"2": {Status: apiv1.PodFailed, Primary: false},
							},
						},
						BackupConfiguration: &clustersv1.BackupConfiguration{
							BackupSchedule:  "0 30 14 * * 0",
							BackupRetention: "7d",
						},
					}, nil).Once()

				mockPostgresConfig.EXPECT().FilterConfigurableParameters(mock.Anything, mock.AnythingOfType("int"), "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5", []string{"pg_stat_statements", "auto_explain", "pg_cron"}).Return(map[string]string{}).Once()

				mockClusters.EXPECT().
					GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: branch.ID, Username: "superuser"}).
					Return(credentials, nil).Once()

				mockStore.EXPECT().
					GetRegion(mock.Anything, apitest.TestOrganization, branch.Region).
					Return(region, nil).Once()

				mockStore.EXPECT().
					ListInstanceTypes(mock.Anything, apitest.TestOrganization, region.ID).
					Return(instanceTypes, nil).Once()
			},
			expectedStatus: http.StatusOK,
			assertResponse: func(t *testing.T, got spec.BranchMetadata) {
				assert.Equal(t, branch.ID, got.Id)
				assert.Equal(t, branch.CreatedAt, got.CreatedAt)
				assert.Equal(t, branch.Name, got.Name)
				assert.Equal(t, new("postgresql://user:pass@branchID.footest.tld:1234/xata?sslmode=require"), got.ConnectionString)
				assert.Equal(t, int32(0), got.Configuration.Replicas)
				assert.Equal(t, int32(1), *got.Configuration.Storage)
				assert.Equal(t, apiv1.PhaseImageCatalogError, got.Status.Status)
				assert.Equal(t, 1, got.Status.InstanceReadyCount)
				assert.Equal(t, 2, got.Status.InstanceCount)
				require.Len(t, got.Status.Instances, 2)
				assert.True(t, got.Status.Instances[0].Primary)
				assert.False(t, got.Status.Instances[1].Primary)
				assert.Equal(t, "postgres:17.5", got.Configuration.Image)
				assert.Equal(t, scaleToZero.Enabled, got.ScaleToZero.Enabled)
				assert.Equal(t, int(scaleToZero.InactivityPeriodMinutes), got.ScaleToZero.InactivityPeriodMinutes)
				assert.NotNil(t, got.BackupConfiguration)
				assert.NotNil(t, got.BackupConfiguration.BackupTime)
				assert.Equal(t, "0:14:30", *got.BackupConfiguration.BackupTime)
				assert.NotNil(t, got.BackupConfiguration.RetentionPeriod)
				assert.Equal(t, int32(7), *got.BackupConfiguration.RetentionPeriod)
			},
		},
		{
			name: "instance type not found",
			setupMocks: func() {
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", branch.ID).Return(&branch, nil).Once()
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: branch.ID}).Return(&clustersv1.DescribePostgresClusterResponse{
					Id: branch.ID,
					Configuration: &clustersv1.ClusterConfiguration{
						NumInstances:     1,
						StorageSize:      defaultStorage,
						VcpuRequest:      wrongVcpu,
						VcpuLimit:        wrongVcpu,
						Memory:           wrongMemory,
						ImageName:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
						PreloadLibraries: []string{"pg_stat_statements", "auto_explain", "pg_cron"},
					},
					Status: &clustersv1.ClusterStatus{
						Status:             apiv1.PhaseHealthy,
						InstanceCount:      1,
						InstanceReadyCount: 1,
						Instances: map[string]*clustersv1.InstanceStatus{
							"1": {Status: apiv1.PodHealthy, Primary: true},
						},
					},
					BackupConfiguration: &clustersv1.BackupConfiguration{
						BackupSchedule:  "0 30 14 * * 0",
						BackupRetention: "7d",
					},
				}, nil).Once()
				mockPostgresConfig.EXPECT().FilterConfigurableParameters(mock.Anything, mock.AnythingOfType("int"), "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5", []string{"pg_stat_statements", "auto_explain", "pg_cron"}).Return(map[string]string{}).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: branch.ID, Username: "superuser"}).Return(credentials, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, branch.Region).Return(region, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, region.ID).Return(instanceTypes, nil).Once()
			},
			expectedStatus: http.StatusOK,
			assertResponse: func(t *testing.T, got spec.BranchMetadata) {
				assert.Equal(t, branch.ID, got.Id)
				assert.Equal(t, branch.Name, got.Name)
				assert.Equal(t, branch.CreatedAt, got.CreatedAt)
				assert.Equal(t, branch.UpdatedAt, got.UpdatedAt)
				assert.Equal(t, branch.ParentID, got.ParentID)
				assert.Equal(t, branch.Description, got.Description)
				assert.Equal(t, branch.PublicAccess, got.PublicAccess)
				assert.Equal(t, new("postgresql://user:pass@branchID.footest.tld:1234/xata?sslmode=require"), got.ConnectionString)
				assert.Equal(t, int32(0), got.Configuration.Replicas)
				assert.Equal(t, defaultStorage, *got.Configuration.Storage)
				assert.Equal(t, FallbackInstanceType, got.Configuration.InstanceType)
				assert.Equal(t, "postgres:17.5", got.Configuration.Image)
				assert.Equal(t, apiv1.PhaseHealthy, got.Status.Status)
				assert.Equal(t, 1, got.Status.InstanceCount)
				assert.Equal(t, 1, got.Status.InstanceReadyCount)
				require.Len(t, got.Status.Instances, 1)
				assert.True(t, got.Status.Instances[0].Primary)
				assert.Equal(t, branch.Region, got.Region)
				assert.Equal(t, scaleToZero.Enabled, got.ScaleToZero.Enabled)
				assert.Equal(t, int(scaleToZero.InactivityPeriodMinutes), got.ScaleToZero.InactivityPeriodMinutes)
				assert.NotNil(t, got.BackupConfiguration)
				assert.NotNil(t, got.BackupConfiguration.BackupTime)
				assert.Equal(t, "0:14:30", *got.BackupConfiguration.BackupTime)
				assert.NotNil(t, got.BackupConfiguration.RetentionPeriod)
				assert.Equal(t, int32(7), *got.BackupConfiguration.RetentionPeriod)
			},
		},
		{
			name: "no credentials",
			setupMocks: func() {
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", branch.ID).Return(&branch, nil).Once()
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: branch.ID}).Return(&clustersv1.DescribePostgresClusterResponse{
					Id: branch.ID,
					Configuration: &clustersv1.ClusterConfiguration{
						NumInstances:     1,
						StorageSize:      1,
						VcpuRequest:      "500m",
						VcpuLimit:        "2",
						Memory:           "2",
						ImageName:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
						PreloadLibraries: []string{"pg_stat_statements", "auto_explain", "pg_cron"},
					},
					Status: &clustersv1.ClusterStatus{Status: "healthy", InstanceCount: 2, InstanceReadyCount: 1, Instances: map[string]*clustersv1.InstanceStatus{"1": {Status: "healthy", Primary: true}, "2": {Status: "non-healthy", Primary: false}}},
				}, nil).Once()
				mockPostgresConfig.EXPECT().FilterConfigurableParameters(mock.Anything, mock.AnythingOfType("int"), "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5", []string{"pg_stat_statements", "auto_explain", "pg_cron"}).Return(map[string]string{}).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: branch.ID, Username: "superuser"}).Return(nil, errors.New("failed to get credentials")).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, branch.Region).Return(region, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, region.ID).Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1}, {Name: "xata.small", VCPUsRequest: 500, VCPUsLimit: 2000, RAM: 2}}, nil).Once()
			},
			expectedStatus: http.StatusOK,
			assertResponse: func(t *testing.T, got spec.BranchMetadata) {
				assert.Equal(t, (*string)(nil), got.ConnectionString)
			},
		},
		{
			name: "branch not found",
			setupMocks: func() {
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", branch.ID).Return(nil, &store.ErrBranchNotFound{ID: branch.ID}).Once()
			},
			expectedError: &store.ErrBranchNotFound{ID: branch.ID},
		},
		{
			name: "cluster not found",
			setupMocks: func() {
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", branch.ID).Return(&branch, nil).Once()
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: branch.ID}).Return(nil, clusters.ClusterNotFoundError(branch.ID)).Once()
			},
			expectedError: ErrorBranchNotFound{BranchID: branch.ID},
		},
		{
			name: "branch with preload libraries",
			setupMocks: func() {
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", branch.ID).Return(&branch, nil).Once()
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: branch.ID}).Return(&clustersv1.DescribePostgresClusterResponse{
					Id: branch.ID,
					Configuration: &clustersv1.ClusterConfiguration{
						NumInstances:     1,
						StorageSize:      defaultStorage,
						VcpuRequest:      "500m",
						VcpuLimit:        "2",
						Memory:           "2",
						ImageName:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
						PreloadLibraries: []string{"pg_stat_statements", "auto_explain", "pg_cron"},
					},
					Status: &clustersv1.ClusterStatus{
						Status:             apiv1.PhaseHealthy,
						StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
						InstanceCount:      1,
						InstanceReadyCount: 1,
						Instances: map[string]*clustersv1.InstanceStatus{
							"1": {Status: apiv1.PodHealthy, Primary: true},
						},
					},
				}, nil).Once()
				mockPostgresConfig.EXPECT().FilterConfigurableParameters(mock.Anything, mock.AnythingOfType("int"), "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5", []string{"pg_stat_statements", "auto_explain", "pg_cron"}).Return(map[string]string{}).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: branch.ID, Username: "superuser"}).Return(credentials, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, branch.Region).Return(region, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, region.ID).Return(instanceTypes, nil).Once()
			},
			expectedStatus: http.StatusOK,
			assertResponse: func(t *testing.T, got spec.BranchMetadata) {
				assert.Equal(t, branch.ID, got.Id)
				assert.Equal(t, branch.Name, got.Name)
				expectedPreloadLibraries := []string{"pg_stat_statements", "auto_explain", "pg_cron"}
				assert.Equal(t, expectedPreloadLibraries, *got.Configuration.PreloadLibraries)
				assert.Equal(t, new("postgresql://user:pass@branchID.footest.tld:1234/xata?sslmode=require"), got.ConnectionString)
				assert.Equal(t, int32(0), got.Configuration.Replicas)
				assert.Equal(t, "xata.small", got.Configuration.InstanceType)
				assert.Equal(t, "postgres:17.5", got.Configuration.Image)
				assert.Equal(t, apiv1.PhaseHealthy, got.Status.Status)
			},
		},
		{
			name: "infra error from cluster",
			setupMocks: func() {
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", branch.ID).Return(&branch, nil).Once()
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: branch.ID}).Return(nil, errors.New("some infra error")).Once()
			},
			expectedError: errors.New("some infra error"),
		},
	}

	for _, tc := range describeBranchTests {
		t.Run(tc.name, func(t *testing.T) {
			tc.setupMocks()

			c, rec := e.GET("/organizations/" + apitest.TestOrganization + "/projects/projectID/branches/" + branch.ID).Context()
			err := apiHandler.DescribeBranch(c, apitest.TestOrganization, "project_id", branch.ID)

			if tc.expectedError != nil {
				require.Error(t, err)
				assert.Equal(t, tc.expectedError.Error(), err.Error())
			} else {
				require.NoError(t, err)
				rec.MustCode(tc.expectedStatus)

				if tc.assertResponse != nil {
					var got spec.BranchMetadata
					rec.ReadBody(&got)
					tc.assertResponse(t, got)
				}
			}
		})
	}
}

func TestDescribeBranchXataUser(t *testing.T) {
	mockStore := mocks.NewProjectsStore(t)
	mockClusters := protomocks.NewClustersServiceClient(t)
	mockCells := cellsmock.NewCellsMock(t, mockClusters)
	mockPostgresConfig := postgrescfgmocks.NewPostgresConfigProvider(t)

	feat := openfeaturetest.NewClient(map[openfeature.FeatureFlag]bool{flags.XataUser: true})
	sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
	apiHandler := NewAPIHandler(feat, mockStore, mockCells, "testdomain:5432", createNewSigNozClient(t), sched, analyticsmocks.NewClient(t), mockPostgresConfig, nil)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

	branch := store.Branch{
		ID:        "branchID",
		Name:      "test",
		CreatedAt: mustParseTime("2024-01-10T00:00:00Z"),
		UpdatedAt: mustParseTime("2024-01-10T00:00:00Z"),
		Region:    "region-id-1",
	}
	region := &store.Region{
		ID:              "region-id-1",
		GatewayHostPort: "footest.tld:1234",
	}
	credentials := &clustersv1.GetPostgresClusterCredentialsResponse{
		Username: "xata",
		Password: "password",
	}
	instanceTypes := []store.InstanceType{
		{Name: "xata.micro", VCPUsRequest: 250, VCPUsLimit: 2000, RAM: 1},
		{Name: "xata.small", VCPUsRequest: 500, VCPUsLimit: 2000, RAM: 2},
	}

	mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", branch.ID).Return(&branch, nil).Once()
	mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: branch.ID}).Return(&clustersv1.DescribePostgresClusterResponse{
		Id: branch.ID,
		Configuration: &clustersv1.ClusterConfiguration{
			NumInstances:     1,
			StorageSize:      defaultStorage,
			VcpuRequest:      "500m",
			VcpuLimit:        "2",
			Memory:           "2",
			ImageName:        "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
			PreloadLibraries: []string{"pg_stat_statements", "auto_explain", "pg_cron"},
		},
		Status: &clustersv1.ClusterStatus{
			Status:             apiv1.PhaseHealthy,
			StatusType:         clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY,
			InstanceCount:      1,
			InstanceReadyCount: 1,
			Instances: map[string]*clustersv1.InstanceStatus{
				"1": {Status: apiv1.PodHealthy, Primary: true},
			},
		},
	}, nil).Once()
	mockPostgresConfig.EXPECT().FilterConfigurableParameters(mock.Anything, mock.AnythingOfType("int"), "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5", []string{"pg_stat_statements", "auto_explain", "pg_cron"}).Return(map[string]string{}).Once()
	mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: branch.ID, Username: "app"}).Return(credentials, nil).Once()
	mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, branch.Region).Return(region, nil).Once()
	mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, region.ID).Return(instanceTypes, nil).Once()

	c, rec := e.GET("/organizations/" + apitest.TestOrganization + "/projects/projectID/branches/" + branch.ID).Context()
	err := apiHandler.DescribeBranch(c, apitest.TestOrganization, "project_id", branch.ID)
	require.NoError(t, err)
	rec.MustCode(http.StatusOK)
	var got spec.BranchMetadata
	rec.ReadBody(&got)
	assert.Equal(t, "postgresql://xata:password@branchID.footest.tld:1234/xata?sslmode=require", *got.ConnectionString)
}

func TestBranchMetrics(t *testing.T) {
	mockStore := mocks.NewProjectsStore(t)
	mockClusters := protomocks.NewClustersServiceClient(t)
	mockCells := cellsmock.NewCellsMock(t, mockClusters)
	mockMetrics := metricsmock.NewClient(t)
	feat := openfeaturetest.NewClient(nil)
	sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}

	apiHandler := NewAPIHandler(feat, mockStore, mockCells, "testdomain:5432", mockMetrics, sched, analyticsmocks.NewClient(t), nil, nil)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

	convertAggs := func(aggs []spec.BranchMetricsRequestAggregations) []string {
		s := make([]string, len(aggs))
		for i := range aggs {
			s[i] = string(aggs[i])
		}
		return s
	}

	branchID := "branchID"
	unknownBranch := "branchUnknown"
	dataPoints := []metrics.Values{
		{
			Timestamp: time.Date(2025, 5, 20, 0, 0, 0, 0, time.UTC),
			Value:     0.5,
		},
		{
			Timestamp: time.Date(2025, 5, 20, 0, 1, 0, 0, time.UTC),
			Value:     0.5,
		},
		{
			Timestamp: time.Date(2025, 5, 20, 0, 2, 0, 0, time.UTC),
			Value:     0.5,
		},
	}

	cpuMetricRequest := spec.BranchMetricsRequest{
		Start:        time.Date(2025, 5, 20, 0, 0, 0, 0, time.UTC),
		End:          time.Date(2025, 5, 20, 1, 0, 0, 0, time.UTC),
		Metric:       "cpu",
		Instances:    []string{branchID + "-1"},
		Aggregations: []spec.BranchMetricsRequestAggregations{"avg"},
	}

	unknownBranchRequest := spec.BranchMetricsRequest{
		Start:        time.Date(2025, 5, 20, 0, 0, 0, 0, time.UTC),
		End:          time.Date(2025, 5, 20, 1, 0, 0, 0, time.UTC),
		Metric:       "cpu",
		Instances:    []string{unknownBranch + "-1"},
		Aggregations: []spec.BranchMetricsRequestAggregations{"avg"},
	}

	wrongDatesRequest := spec.BranchMetricsRequest{
		Start:        time.Date(2025, 5, 20, 1, 0, 0, 0, time.UTC),
		End:          time.Date(2025, 5, 20, 0, 0, 0, 0, time.UTC),
		Metric:       "cpu",
		Instances:    []string{branchID + "-1"},
		Aggregations: []spec.BranchMetricsRequestAggregations{"avg"},
	}

	dateRangeTooBigRequest := spec.BranchMetricsRequest{
		Start:        time.Date(2025, 5, 20, 0, 0, 0, 0, time.UTC),
		End:          time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC),
		Metric:       "cpu",
		Instances:    []string{branchID + "-1"},
		Aggregations: []spec.BranchMetricsRequestAggregations{"avg"},
	}

	wrongInstanceRequest := spec.BranchMetricsRequest{
		Start:        time.Date(2025, 5, 20, 0, 0, 0, 0, time.UTC),
		End:          time.Date(2025, 5, 20, 1, 0, 0, 0, time.UTC),
		Metric:       "cpu",
		Instances:    []string{"unknown-1"},
		Aggregations: []spec.BranchMetricsRequestAggregations{"avg"},
	}

	metricRequest2i2a := spec.BranchMetricsRequest{
		Start:        time.Date(2025, 5, 20, 0, 0, 0, 0, time.UTC),
		End:          time.Date(2025, 5, 20, 1, 0, 0, 0, time.UTC),
		Metric:       "cpu",
		Instances:    []string{branchID + "-1", branchID + "-2"},
		Aggregations: []spec.BranchMetricsRequestAggregations{"min", "max"},
	}

	branchMetricsTests := []struct {
		name           string
		branchID       string
		req            spec.BranchMetricsRequest
		setupMocks     func()
		expectedError  error
		expectedStatus int
		assertResponse func(t *testing.T, got spec.BranchMetrics)
	}{
		{
			name:     "request 1 instance 1 aggregate succeeds",
			branchID: branchID,
			req:      cpuMetricRequest,
			setupMocks: func() {
				mockMetrics.EXPECT().GetMetric(mock.Anything, cpuMetricRequest.Start, cpuMetricRequest.End, string(cpuMetricRequest.Metric), cpuMetricRequest.Instances, convertAggs(cpuMetricRequest.Aggregations)).Return(&metrics.BranchMetrics{
					Start:  cpuMetricRequest.Start,
					End:    cpuMetricRequest.End,
					Metric: string(cpuMetricRequest.Metric),
					Unit:   "percentage",
					Series: []metrics.MetricSeries{
						{
							InstanceID:  cpuMetricRequest.Instances[0],
							Aggregation: "avg",
							Values:      dataPoints,
						},
					},
				}, nil).Once()
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", branchID).Return(&store.Branch{ID: cpuMetricRequest.Instances[0]}, nil).Once()
			},
			assertResponse: func(t *testing.T, got spec.BranchMetrics) {
				assert.Equal(t, cpuMetricRequest.Start, got.Start)
				assert.Equal(t, cpuMetricRequest.End, got.End)
				assert.Equal(t, string(cpuMetricRequest.Metric), got.Metric)
				assert.Equal(t, "percentage", got.Unit)
				assert.Len(t, got.Series, 1)

				series := got.Series[0]
				assert.Equal(t, cpuMetricRequest.Instances[0], series.InstanceID)
				assert.Equal(t, "avg", string(series.Aggregation))
				assert.Len(t, series.Values, len(dataPoints))

				for i := range dataPoints {
					assert.Equal(t, dataPoints[i].Timestamp.Unix(), series.Values[i].Timestamp.Unix())
					assert.Equal(t, dataPoints[i].Value, series.Values[i].Value)
				}
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:     "request 2 instances 2 aggregates succeeds",
			branchID: branchID,
			req:      metricRequest2i2a,
			setupMocks: func() {
				mockMetrics.EXPECT().GetMetric(mock.Anything, metricRequest2i2a.Start, metricRequest2i2a.End, string(metricRequest2i2a.Metric), metricRequest2i2a.Instances, convertAggs(metricRequest2i2a.Aggregations)).Return(&metrics.BranchMetrics{
					Start:  metricRequest2i2a.Start,
					End:    metricRequest2i2a.End,
					Metric: string(metricRequest2i2a.Metric),
					Unit:   "percentage",
					Series: []metrics.MetricSeries{
						{
							InstanceID:  metricRequest2i2a.Instances[0],
							Aggregation: "min",
							Values:      dataPoints,
						},
						{
							InstanceID:  metricRequest2i2a.Instances[1],
							Aggregation: "min",
							Values:      dataPoints,
						},
						{
							InstanceID:  metricRequest2i2a.Instances[0],
							Aggregation: "max",
							Values:      dataPoints,
						},
						{
							InstanceID:  metricRequest2i2a.Instances[1],
							Aggregation: "max",
							Values:      dataPoints,
						},
					},
				}, nil).Once()
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", branchID).Return(&store.Branch{ID: cpuMetricRequest.Instances[0]}, nil).Once()
			},
			assertResponse: func(t *testing.T, got spec.BranchMetrics) {
				assert.Equal(t, metricRequest2i2a.Start, got.Start)
				assert.Equal(t, metricRequest2i2a.End, got.End)
				assert.Equal(t, string(metricRequest2i2a.Metric), got.Metric)
				assert.Equal(t, "percentage", got.Unit)
				assert.Len(t, got.Series, 4)

				for _, series := range got.Series {
					assert.Contains(t, metricRequest2i2a.Instances, series.InstanceID)
					assert.Contains(t, convertAggs(metricRequest2i2a.Aggregations), string(series.Aggregation))
					assert.Len(t, series.Values, len(dataPoints))

					for i := range dataPoints {
						assert.Equal(t, dataPoints[i].Timestamp.Unix(), series.Values[i].Timestamp.Unix())
						assert.Equal(t, dataPoints[i].Value, series.Values[i].Value)
					}
				}
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:     "branch not found",
			branchID: unknownBranch,
			req:      unknownBranchRequest,
			setupMocks: func() {
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "project_id", unknownBranch).Return(nil, &store.ErrBranchNotFound{ID: unknownBranch}).Once()
			},
			expectedError:  store.ErrBranchNotFound{ID: unknownBranch},
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "wrong dates request fails",
			branchID:       branchID,
			req:            wrongDatesRequest,
			setupMocks:     func() {},
			expectedError:  ErrorInvalidParam{BranchName: branchID, Param: "start", Message: "start time must come before end time"},
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "time range too large fails",
			branchID:       branchID,
			req:            dateRangeTooBigRequest,
			setupMocks:     func() {},
			expectedError:  ErrorInvalidParam{BranchName: branchID, Param: "end", Message: "maximum date range is 4320h0m0s"},
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "wrong instance request fails",
			branchID:       branchID,
			req:            wrongInstanceRequest,
			setupMocks:     func() {},
			expectedError:  ErrorInvalidParam{BranchName: branchID, Param: "instances", Message: "invalid instance [unknown-1]"},
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range branchMetricsTests {
		t.Run(tc.name, func(t *testing.T) {
			tc.setupMocks()

			c, rec := e.POST("/organizations/" + apitest.TestOrganization + "/projects/project_id/branches/" + tc.branchID + "/metrics").WithJSONBody(tc.req).Context()
			err := apiHandler.BranchMetrics(c, apitest.TestOrganization, "project_id", tc.branchID)

			if tc.expectedError != nil {
				require.Error(t, err)
				assert.Equal(t, tc.expectedError.Error(), err.Error())
			} else {
				require.NoError(t, err)
				rec.MustCode(tc.expectedStatus)

				if tc.assertResponse != nil {
					var got spec.BranchMetrics
					rec.ReadBody(&got)
					tc.assertResponse(t, got)
				}
			}
		})
	}
}

func TestGetBranchCredentials(t *testing.T) {
	mockStore := mocks.NewProjectsStore(t)
	mockClusters := protomocks.NewClustersServiceClient(t)
	mockCells := cellsmock.NewCellsMock(t, mockClusters)

	feat := openfeaturetest.NewClient(nil)
	sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
	handler := NewAPIHandler(feat, mockStore, mockCells, "testdomain:5432", createNewSigNozClient(t), sched, analyticsmocks.NewClient(t), nil, nil)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

	projectID := "project_id"
	branchID := "branchID"
	credentials := &clustersv1.GetPostgresClusterCredentialsResponse{
		Username: "user",
		Password: "pass",
	}

	getBranchCredentialsTests := []struct {
		name          string
		projectID     string
		branchID      string
		reqUsername   string
		setupMocks    func()
		wantError     bool
		expectedError error
	}{
		{
			name:      "get credentials - no error",
			projectID: projectID,
			branchID:  branchID,
			setupMocks: func() {
				branch := store.Branch{ID: branchID}
				mockStore.EXPECT().
					DescribeBranch(mock.Anything, apitest.TestOrganization, projectID, branchID).
					Return(&branch, nil).Once()
				mockClusters.EXPECT().
					GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: branch.ID, Username: "superuser"}).
					Return(credentials, nil).Once()
			},
			wantError: false,
		},
		{
			name:      "get credentials for non-existing branch fails",
			projectID: projectID,
			branchID:  branchID,
			setupMocks: func() {
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, projectID, branchID).Return(nil, store.ErrBranchNotFound{ID: branchID}).Once()
			},
			wantError:     true,
			expectedError: ErrorBranchNotFound{BranchID: branchID},
		},
		{
			name:        "get credentials with non-existing username",
			projectID:   projectID,
			branchID:    branchID,
			reqUsername: "nonexisting",
			setupMocks: func() {
				branch := store.Branch{ID: branchID}
				mockStore.EXPECT().
					DescribeBranch(mock.Anything, apitest.TestOrganization, projectID, branchID).
					Return(&branch, nil).Once()
				mockClusters.EXPECT().
					GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: branch.ID, Username: "nonexisting"}).
					Return(nil, clusters.SecretNotFoundForIDError(branchID)).Once()
			},
			wantError:     true,
			expectedError: ErrorCredentialsForBranchNotFound{BranchID: branchID, Username: "nonexisting"},
		},
	}

	for _, tc := range getBranchCredentialsTests {
		t.Run(tc.name, func(t *testing.T) {
			tc.setupMocks()

			c, rec := e.GET("/organizations/" + apitest.TestOrganization + "/projects/" + tc.projectID + "/branches/" + tc.branchID + "/credentials").Context()
			err := handler.GetBranchCredentials(c, apitest.TestOrganization, tc.projectID, tc.branchID, spec.GetBranchCredentialsParams{Username: &tc.reqUsername})
			if tc.wantError {
				assert.Error(t, err)
				assert.Equal(t, tc.expectedError.Error(), err.Error())
			} else {
				assert.NoError(t, err)
				rec.MustCode(http.StatusOK)

				var resp clustersv1.GetPostgresClusterCredentialsResponse
				rec.ReadBody(&resp)
				assert.Equal(t, credentials.Username, resp.Username)
				assert.Equal(t, credentials.Password, resp.Password)
			}
		})
	}
}

func TestUpdateBranch(t *testing.T) {
	mockStore := mocks.NewProjectsStore(t)
	mockClusters := protomocks.NewClustersServiceClient(t)
	mockCells := cellsmock.NewCellsMock(t, mockClusters)
	mockPostgresConfig := postgrescfgmocks.NewPostgresConfigProvider(t)
	mockImageProvider := postgresversionsmocks.NewImageProvider(t)

	feat := openfeaturetest.NewClient(nil)
	sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
	mockAnalytics := analyticsmocks.NewClient(t)
	mockAnalytics.EXPECT().Track(mock.Anything, mock.Anything).Return().Maybe()
	handler := NewAPIHandler(feat, mockStore, mockCells, "testdomain:5432", createNewSigNozClient(t), sched, mockAnalytics, mockPostgresConfig, mockImageProvider)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

	updateBranchTests := []struct {
		name          string
		projectID     string
		branchID      string
		jsonBody      any
		setupMocks    func()
		wantError     bool
		expectedError error
	}{
		{
			name:      "update non-existing branch fails",
			projectID: "project_id",
			branchID:  "branchID",
			jsonBody:  map[string]string{"name": "newTest", "description": "newDesc"},
			setupMocks: func() {
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "branchID", updateBranchConfig(new("newTest"), new("newDesc")), mock.Anything).Return(nil, store.ErrBranchNotFound{ID: "branchID"}).Once()
			},
			wantError:     true,
			expectedError: store.ErrBranchNotFound{ID: "branchID"},
		},
		{
			name:          "update without any parameters fails",
			projectID:     "project_id",
			branchID:      "branchID",
			jsonBody:      map[string]string{},
			setupMocks:    func() {},
			wantError:     true,
			expectedError: ErrorInvalidParam{BranchName: "branchID", Param: "all", Message: "branch [branchID]: at least one of the request fields needs to be set"},
		},
		{
			name:      "update branch name and/or description works",
			projectID: "project_id",
			branchID:  "123",
			jsonBody:  map[string]string{"name": "newTest", "description": "newDesc"},
			setupMocks: func() {
				branch := store.Branch{
					ID:          "123",
					Name:        "newTest",
					Description: new("newDesc"),
					Region:      "region-id-1",
				}
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", updateBranchConfig(new("newTest"), new("newDesc")), mock.Anything).Return(&branch, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: "region-id-1", GatewayHostPort: ""}, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: "123", Username: "superuser"}).Return(&clustersv1.GetPostgresClusterCredentialsResponse{
					Username: "user",
					Password: "pass",
				}, nil).Once()
			},
			wantError: false,
		},
		{
			name:      "update branch scale to zero works",
			projectID: "project_id",
			branchID:  "123",
			jsonBody:  map[string]any{"scaleToZero": map[string]any{"enabled": true, "inactivityPeriodMinutes": 60}},
			setupMocks: func() {
				branch := store.Branch{
					ID:          "123",
					Name:        "newTest",
					Description: new("newDesc"),
					Region:      "region-id-1",
				}
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", updateBranchConfig(nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchID string, cfg *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Nil(t, err)
				}).Return(&branch, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: "region-id-1", GatewayHostPort: ""}, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: "123", Username: "superuser"}).Return(&clustersv1.GetPostgresClusterCredentialsResponse{
					Username: "user",
					Password: "pass",
				}, nil).Once()
				mockClusters.EXPECT().UpdatePostgresCluster(mock.Anything, &clustersv1.UpdatePostgresClusterRequest{
					Id: "123",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						ScaleToZero: &clustersv1.ScaleToZero{
							Enabled:                 true,
							InactivityPeriodMinutes: 60,
						},
					},
				}).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()
			},
			wantError: false,
		},
		{
			name:      "update branch configuration replicas works",
			projectID: "project_id",
			branchID:  "123",
			jsonBody:  map[string]int32{"replicas": 3},
			setupMocks: func() {
				branch := store.Branch{
					ID:          "123",
					Name:        "newTest",
					Description: new("newDesc"),
					Region:      "region-id-1",
				}
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", updateBranchConfig(nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchID string, cfg *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Nil(t, err)
				}).Return(&branch, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: "123", Username: "superuser"}).Return(&clustersv1.GetPostgresClusterCredentialsResponse{
					Username: "user",
					Password: "pass",
				}, nil).Once()

				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: "region-id-1", GatewayHostPort: ""}, nil).Once()
				mockClusters.EXPECT().UpdatePostgresCluster(mock.Anything, &clustersv1.UpdatePostgresClusterRequest{
					Id: "123",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						NumInstances: new(int32(4)),
					},
				}).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()
			},
			wantError: false,
		},
		{
			name:      "update branch configuration instance type works",
			projectID: "project_id",
			branchID:  "123",
			jsonBody:  map[string]string{"instanceType": "xata.medium"},
			setupMocks: func() {
				branch := store.Branch{
					ID:          "123",
					Name:        "newTest",
					Description: new("newDesc"),
					Region:      "region-id-1",
				}
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", updateBranchConfig(nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchID string, cfg *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Nil(t, err)
				}).Return(&branch, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: "123", Username: "superuser"}).Return(&clustersv1.GetPostgresClusterCredentialsResponse{
					Username: "user",
					Password: "pass",
				}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: "region-id-1", GatewayHostPort: ""}, nil).Once()
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: "123"}).Return(&clustersv1.DescribePostgresClusterResponse{
					Id: "123",
					Configuration: &clustersv1.ClusterConfiguration{
						VcpuRequest: "500m",
						VcpuLimit:   "1000m",
						Memory:      "2",
					},
				}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{
					{Name: "xata.small", VCPUsRequest: 500, VCPUsLimit: 1000, RAM: 2},
					{Name: "xata.medium", VCPUsRequest: 1000, VCPUsLimit: 2000, RAM: 4},
				}, nil).Twice()
				mockPostgresConfig.EXPECT().GetParametersSpec("xata.small", mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(postgrescfg.ParametersMap{
					"shared_buffers": postgrescfg.PostgresParameterSpec{
						ParameterType: postgrescfg.ParamTypeBytes,
						DefaultValue:  "128MB",
						MinValue:      "16MB",
						MaxValue:      "256MB",
					},
				}, nil).Once()
				mockPostgresConfig.EXPECT().GetParametersSpec("xata.medium", mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(postgrescfg.ParametersMap{
					"shared_buffers": postgrescfg.PostgresParameterSpec{
						ParameterType: postgrescfg.ParamTypeBytes,
						DefaultValue:  "256MB",
						MinValue:      "32MB",
						MaxValue:      "512MB",
					},
				}, nil).Once()
				mockClusters.EXPECT().UpdatePostgresCluster(mock.Anything, &clustersv1.UpdatePostgresClusterRequest{
					Id: "123",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						VcpuRequest: new("1"),
						VcpuLimit:   new("2"),
						Memory:      new("4Gi"),
						PostgresConfigurationParameters: map[string]string{
							"shared_buffers": "256MB",
						},
					},
				}).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()
			},
			wantError: false,
		},
		{
			name:      "update branch hibernate works",
			projectID: "project_id",
			branchID:  "123",
			jsonBody:  map[string]any{"hibernate": true},
			setupMocks: func() {
				branch := store.Branch{
					ID:          "123",
					Name:        "newTest",
					Description: new("newDesc"),
					Region:      "region-id-1",
				}
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", updateBranchConfig(nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchID string, cfg *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Nil(t, err)
				}).Return(&branch, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: "123", Username: "superuser"}).Return(&clustersv1.GetPostgresClusterCredentialsResponse{
					Username: "user",
					Password: "pass",
				}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: "region-id-1", GatewayHostPort: ""}, nil).Once()
				mockClusters.EXPECT().UpdatePostgresCluster(mock.Anything, &clustersv1.UpdatePostgresClusterRequest{
					Id: "123",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						Hibernate: new(true),
					},
				}).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()
			},
			wantError: false,
		},
		{
			name:      "update branch configuration instance type 'custom' does not fail but ignores the instance type",
			projectID: "project_id",
			branchID:  "123",
			jsonBody:  map[string]string{"instanceType": FallbackInstanceType},
			setupMocks: func() {
				branch := store.Branch{
					ID:          "123",
					Name:        "newTest",
					Description: new("newDesc"),
					Region:      "region-id-1",
				}
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", updateBranchConfig(nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchID string, cfg *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Nil(t, err)
				}).Return(&branch, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: "123", Username: "superuser"}).Return(&clustersv1.GetPostgresClusterCredentialsResponse{
					Username: "user",
					Password: "pass",
				}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: "region-id-1", GatewayHostPort: ""}, nil).Once()
				mockClusters.EXPECT().UpdatePostgresCluster(mock.Anything, &clustersv1.UpdatePostgresClusterRequest{
					Id: "123", UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{},
				}).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()
			},
			wantError: false,
		},
		{
			name:      "update branch configuration with unknown instance type fails",
			projectID: "project_id",
			branchID:  "123",
			jsonBody:  map[string]string{"instanceType": "unknown"},
			setupMocks: func() {
				branch := store.Branch{
					ID:          "123",
					Name:        "newTest",
					Description: new("newDesc"),
					Region:      "region-id-1",
				}
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", updateBranchConfig(nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchID string, cfg *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Error(t, err)
					assert.Equal(t, err, ErrorInvalidParam{BranchName: "123", Param: "configuration", Message: fmt.Sprintf("branch [%s]: unknown instance type %s", "123", "unknown")})
				}).Return(nil, ErrorInvalidParam{BranchName: "123", Param: "configuration", Message: fmt.Sprintf("branch [%s]: unknown instance type %s", "123", "unknown")}).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.medium", VCPUsRequest: 1000, VCPUsLimit: 2000, RAM: 4}}, nil).Once()
			},
			wantError:     true,
			expectedError: ErrorInvalidParam{BranchName: "123", Param: "configuration", Message: fmt.Sprintf("branch [%s]: unknown instance type %s", "123", "unknown")},
		},
		{
			name:          "update branch with replicas less than 0 fails",
			projectID:     "project_id",
			branchID:      "123",
			jsonBody:      map[string]int32{"replicas": -1},
			setupMocks:    func() {},
			wantError:     true,
			expectedError: ErrorInvalidParam{BranchName: "123", Param: "configuration", Message: fmt.Sprintf("branch [%s]: cannot set number of replicas to less than 0", "123")},
		},
		{
			name:      "update branch configuration storage works",
			projectID: "project_id",
			branchID:  "123",
			jsonBody:  map[string]int32{"storage": 25},
			setupMocks: func() {
				branch := store.Branch{
					ID:          "123",
					Name:        "newTest",
					Description: new("newDesc"),
					Region:      "region-id-1",
				}
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", updateBranchConfig(nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchID string, cfg *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Nil(t, err)
				}).Return(&branch, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: "123", Username: "superuser"}).Return(&clustersv1.GetPostgresClusterCredentialsResponse{
					Username: "user",
					Password: "pass",
				}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: "region-id-1", GatewayHostPort: ""}, nil).Once()
				mockClusters.EXPECT().UpdatePostgresCluster(mock.Anything, &clustersv1.UpdatePostgresClusterRequest{
					Id: "123",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						StorageSize: new(int32(25)),
					},
				}).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()
			},
			wantError: false,
		},
		{
			name:      "update branch with valid PostgreSQL configuration parameters works",
			projectID: "project_id",
			branchID:  "123",
			jsonBody: map[string]any{
				"postgresConfigurationParameters": map[string]string{
					"max_connections": "25",
					"shared_buffers":  "128mb",
					"work_mem":        "1mb",
				},
			},
			setupMocks: func() {
				branch := store.Branch{
					ID:          "123",
					Name:        "newTest",
					Description: new("newDesc"),
					Region:      "region-id-1",
				}
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", &store.UpdateBranchConfiguration{}, mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchID string, cfg *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Nil(t, err)
				}).Return(&branch, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: "123", Username: "superuser"}).Return(&clustersv1.GetPostgresClusterCredentialsResponse{
					Username: "user",
					Password: "pass",
				}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: "region-id-1", GatewayHostPort: ""}, nil).Once()
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: "123"}).Return(&clustersv1.DescribePostgresClusterResponse{
					Configuration: &clustersv1.ClusterConfiguration{
						VcpuRequest: "500m",
						VcpuLimit:   "1000m",
						Memory:      "2",
					},
				}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 500, VCPUsLimit: 1000, RAM: 2}}, nil).Once()
				mockPostgresConfig.EXPECT().ValidateSettings("xata.micro", map[string]string{
					"max_connections": "25",
					"shared_buffers":  "128mb",
					"work_mem":        "1mb",
				}, mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(nil, nil).Once()
				mockClusters.EXPECT().UpdatePostgresCluster(mock.Anything, &clustersv1.UpdatePostgresClusterRequest{
					Id: "123",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						PostgresConfigurationParameters: map[string]string{
							"max_connections": "25",
							"shared_buffers":  "128mb",
							"work_mem":        "1mb",
						},
					},
				}).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()
			},
			wantError: false,
		},
		{
			name:      "update branch with invalid PostgreSQL configuration parameters fails",
			projectID: "project_id",
			branchID:  "123",
			jsonBody: map[string]any{
				"postgresConfigurationParameters": map[string]string{
					"max_connections": "100", // xata.micro has max of 50
					"work_mem":        "invalid_format",
					"unknown_param":   "value",
				},
			},
			setupMocks: func() {
				branch := store.Branch{
					ID:          "123",
					Name:        "newTest",
					Description: new("newDesc"),
					Region:      "region-id-1",
				}
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", &store.UpdateBranchConfiguration{}, mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchID string, cfg *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Error(t, err)
					var invalidParam ErrorInvalidParam
					assert.ErrorAs(t, err, &invalidParam)
					assert.Equal(t, "123", invalidParam.BranchName)
					assert.Equal(t, "postgres_configuration_parameters", invalidParam.Param)
					assert.Contains(t, invalidParam.Message, "PostgreSQL configuration validation failed:")
					assert.Contains(t, invalidParam.Message, "max_connections: value 100 is above maximum 50")
					assert.Contains(t, invalidParam.Message, "work_mem: invalid bytes value: invalid_format")
					assert.Contains(t, invalidParam.Message, "unknown_param: unknown parameter: unknown_param")
				}).Return(nil, ErrorInvalidParam{
					BranchName: "123",
					Param:      "postgres_configuration_parameters",
					Message:    "PostgreSQL configuration validation failed: max_connections: value 100 is above maximum 50; unknown_param: unknown parameter: unknown_param; work_mem: invalid bytes value: invalid_format",
				}).Once()
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: "123"}).Return(&clustersv1.DescribePostgresClusterResponse{
					Configuration: &clustersv1.ClusterConfiguration{
						VcpuRequest: "500m",
						VcpuLimit:   "1000m",
						Memory:      "2",
					},
				}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 500, VCPUsLimit: 1000, RAM: 2}}, nil).Once()
				mockPostgresConfig.EXPECT().ValidateSettings("xata.micro", map[string]string{
					"max_connections": "100",
					"unknown_param":   "value",
					"work_mem":        "invalid_format",
				}, mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(map[string]error{
					"max_connections": fmt.Errorf("value 100 is above maximum 50"),
					"work_mem":        fmt.Errorf("invalid bytes value: invalid_format"),
					"unknown_param":   fmt.Errorf("unknown parameter: unknown_param"),
				}, nil).Once()
			},
			wantError: true,
			expectedError: ErrorInvalidParam{
				BranchName: "123",
				Param:      "postgres_configuration_parameters",
				Message:    "PostgreSQL configuration validation failed: max_connections: value 100 is above maximum 50; unknown_param: unknown parameter: unknown_param; work_mem: invalid bytes value: invalid_format",
			},
		},
		{
			name:      "update branch with single PostgreSQL configuration validation error fails",
			projectID: "project_id",
			branchID:  "123",
			jsonBody: map[string]any{
				"postgresConfigurationParameters": map[string]string{
					"max_connections": "100", // xata.micro has max of 50
				},
			},
			setupMocks: func() {
				branch := store.Branch{
					ID:          "123",
					Name:        "newTest",
					Description: new("newDesc"),
					Region:      "region-id-1",
				}
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", &store.UpdateBranchConfiguration{}, mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchID string, cfg *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Error(t, err)
					assert.Equal(t, ErrorInvalidParam{
						BranchName: "123",
						Param:      "postgres_configuration_parameters",
						Message:    "PostgreSQL configuration validation failed: max_connections: value 100 is above maximum 50",
					}, err)
				}).Return(nil, ErrorInvalidParam{
					BranchName: "123",
					Param:      "postgres_configuration_parameters",
					Message:    "PostgreSQL configuration validation failed: max_connections: value 100 is above maximum 50",
				}).Once()
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: "123"}).Return(&clustersv1.DescribePostgresClusterResponse{
					Configuration: &clustersv1.ClusterConfiguration{
						VcpuRequest: "500m",
						VcpuLimit:   "1000m",
						Memory:      "2",
					},
				}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 500, VCPUsLimit: 1000, RAM: 2}}, nil).Once()
				mockPostgresConfig.EXPECT().ValidateSettings("xata.micro", map[string]string{
					"max_connections": "100",
				}, mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(map[string]error{
					"max_connections": fmt.Errorf("value 100 is above maximum 50"),
				}, nil).Once()
			},
			wantError: true,
			expectedError: ErrorInvalidParam{
				BranchName: "123",
				Param:      "postgres_configuration_parameters",
				Message:    "PostgreSQL configuration validation failed: max_connections: value 100 is above maximum 50",
			},
		},
		{
			name:      "update branch with invalid instance type for PostgreSQL configuration fails",
			projectID: "project_id",
			branchID:  "123",
			jsonBody: map[string]any{
				"postgresConfigurationParameters": map[string]string{
					"max_connections": "25",
				},
			},
			setupMocks: func() {
				branch := store.Branch{
					ID:          "123",
					Name:        "newTest",
					Description: new("newDesc"),
					Region:      "region-id-1",
				}
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", &store.UpdateBranchConfiguration{}, mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchID string, cfg *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Error(t, err)
					assert.Contains(t, err.Error(), "invalid instance type custom")
				}).Return(nil, fmt.Errorf("invalid instance type custom: unknown instance size custom")).Once()
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: "123"}).Return(&clustersv1.DescribePostgresClusterResponse{
					Configuration: &clustersv1.ClusterConfiguration{
						VcpuRequest: "999m", // Invalid combination that will result in "custom" instance type
						VcpuLimit:   "999m",
						Memory:      "999",
					},
				}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 500, VCPUsLimit: 1000, RAM: 2}}, nil).Once()
				mockPostgresConfig.EXPECT().ValidateSettings("custom", map[string]string{
					"max_connections": "25",
				}, mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(nil, fmt.Errorf("invalid instance type custom: unknown instance size custom")).Once()
			},
			wantError:     true,
			expectedError: fmt.Errorf("invalid instance type custom: unknown instance size custom"),
		},
		{
			name:      "update branch with not found cluster fails",
			projectID: "project_id",
			branchID:  "123",
			jsonBody:  map[string]int32{"replicas": 4},
			setupMocks: func() {
				branch := store.Branch{
					ID:          "123",
					Name:        "newTest",
					Description: new("newDesc"),
					Region:      "region-id-1",
				}
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", updateBranchConfig(nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchID string, cfg *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Error(t, err)
					assert.Equal(t, clusters.ClusterNotFoundError("123").Error(), err.Error())
				}).Return(nil, ErrorBranchNotFound{
					BranchID: "123",
				}).Once()

				mockClusters.EXPECT().UpdatePostgresCluster(mock.Anything, &clustersv1.UpdatePostgresClusterRequest{
					Id: "123",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						NumInstances: new(int32(5)),
					},
				}).Return(nil, clusters.ClusterNotFoundError("123")).Once()
			},
			wantError:     true,
			expectedError: ErrorBranchNotFound{BranchID: "123"},
		},
		{
			name:      "update branch with replicas >5 fails",
			projectID: "project_id",
			branchID:  "123",
			jsonBody:  map[string]int32{"replicas": 7},
			setupMocks: func() {
				branch := store.Branch{
					ID:          "123",
					Name:        "newTest",
					Description: new("newDesc"),
					Region:      "region-id-1",
				}
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", updateBranchConfig(nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchID string, cfg *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Error(t, err)
				}).Return(nil, ErrorInvalidParam{
					BranchName: "123",
					Param:      "configuration",
					Message:    "cluster [123] invalid parameter: Number of instances has value 7 and error is Min: 1 and Max: 5",
				}).Once()
				mockClusters.EXPECT().UpdatePostgresCluster(mock.Anything, &clustersv1.UpdatePostgresClusterRequest{
					Id: "123",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						NumInstances: new(int32(8)),
					},
				}).Return(nil, clusters.ClusterInvalidParameter("123", "Number of instances", "7", "Min:1 and Max: 5")).Once()
			},
			wantError:     true,
			expectedError: ErrorInvalidParam{BranchName: "123", Param: "configuration", Message: "cluster [123] invalid parameter: Number of instances has value 7 and error is Min: 1 and Max: 5"},
		},
		{
			name:      "update branch instance type with custom PostgreSQL parameters adjusts values correctly",
			projectID: "project_id",
			branchID:  "123",
			jsonBody:  map[string]string{"instanceType": "xata.medium"},
			setupMocks: func() {
				branch := store.Branch{
					ID:          "123",
					Name:        "newTest",
					Description: new("newDesc"),
					Region:      "region-id-1",
				}
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", updateBranchConfig(nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchID string, cfg *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Nil(t, err)
				}).Return(&branch, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: "123", Username: "superuser"}).Return(&clustersv1.GetPostgresClusterCredentialsResponse{
					Username: "user",
					Password: "pass",
				}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: "region-id-1", GatewayHostPort: ""}, nil).Once()
				// Current cluster has custom PostgreSQL parameters
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: "123"}).Return(&clustersv1.DescribePostgresClusterResponse{
					Id: "123",
					Configuration: &clustersv1.ClusterConfiguration{
						VcpuRequest: "500m",
						VcpuLimit:   "1000m",
						Memory:      "2",
						PostgresConfigurationParameters: map[string]string{
							"shared_buffers":  "200MB", // Custom value (not default)
							"work_mem":        "4MB",   // Custom value (not default)
							"max_connections": "25",    // Default value for xata.small
						},
					},
				}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{
					{Name: "xata.small", VCPUsRequest: 500, VCPUsLimit: 1000, RAM: 2},
					{Name: "xata.medium", VCPUsRequest: 1000, VCPUsLimit: 2000, RAM: 4},
				}, nil).Twice()
				// Old instance type (xata.small) specs
				mockPostgresConfig.EXPECT().GetParametersSpec("xata.small", mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(postgrescfg.ParametersMap{
					"shared_buffers": postgrescfg.PostgresParameterSpec{
						ParameterType: postgrescfg.ParamTypeBytes,
						DefaultValue:  "128MB",
						MinValue:      "16MB",
						MaxValue:      "256MB",
					},
					"work_mem": postgrescfg.PostgresParameterSpec{
						ParameterType: postgrescfg.ParamTypeBytes,
						DefaultValue:  "2MB",
						MinValue:      "1MB",
						MaxValue:      "8MB",
					},
					"max_connections": postgrescfg.PostgresParameterSpec{
						ParameterType: postgrescfg.ParamTypeInt,
						DefaultValue:  "25",
						MinValue:      "10",
						MaxValue:      "50",
					},
				}, nil).Once()
				// New instance type (xata.medium) specs
				mockPostgresConfig.EXPECT().GetParametersSpec("xata.medium", mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(postgrescfg.ParametersMap{
					"shared_buffers": postgrescfg.PostgresParameterSpec{
						ParameterType: postgrescfg.ParamTypeBytes,
						DefaultValue:  "256MB",
						MinValue:      "32MB",
						MaxValue:      "512MB",
					},
					"work_mem": postgrescfg.PostgresParameterSpec{
						ParameterType: postgrescfg.ParamTypeBytes,
						DefaultValue:  "4MB",
						MinValue:      "2MB",
						MaxValue:      "16MB",
					},
					"max_connections": postgrescfg.PostgresParameterSpec{
						ParameterType: postgrescfg.ParamTypeInt,
						DefaultValue:  "50",
						MinValue:      "20",
						MaxValue:      "100",
					},
				}, nil).Once()
				// Expected update with adjusted parameters:
				// - shared_buffers: 200MB (custom value, within new bounds, kept as-is)
				// - work_mem: 4MB (custom value, within new bounds, kept as-is)
				// - max_connections: 50 (was default for old instance, updated to new default)
				mockClusters.EXPECT().UpdatePostgresCluster(mock.Anything, &clustersv1.UpdatePostgresClusterRequest{
					Id: "123",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						VcpuRequest: new("1"),
						VcpuLimit:   new("2"),
						Memory:      new("4Gi"),
						PostgresConfigurationParameters: map[string]string{
							"shared_buffers":  "200MB", // Custom value preserved
							"work_mem":        "4MB",   // Custom value preserved
							"max_connections": "50",    // Updated to new default
						},
					},
				}).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()
			},
			wantError: false,
		},
		{
			name:      "update branch instance type with out-of-bounds PostgreSQL parameters adjusts to bounds",
			projectID: "project_id",
			branchID:  "123",
			jsonBody:  map[string]string{"instanceType": "xata.medium"},
			setupMocks: func() {
				branch := store.Branch{
					ID:          "123",
					Name:        "newTest",
					Description: new("newDesc"),
					Region:      "region-id-1",
				}
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", updateBranchConfig(nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchID string, cfg *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Nil(t, err)
				}).Return(&branch, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: "123", Username: "superuser"}).Return(&clustersv1.GetPostgresClusterCredentialsResponse{
					Username: "user",
					Password: "pass",
				}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: "region-id-1", GatewayHostPort: ""}, nil).Once()
				// Current cluster has out-of-bounds PostgreSQL parameters
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: "123"}).Return(&clustersv1.DescribePostgresClusterResponse{
					Id: "123",
					Configuration: &clustersv1.ClusterConfiguration{
						VcpuRequest: "500m",
						VcpuLimit:   "1000m",
						Memory:      "2",
						PostgresConfigurationParameters: map[string]string{
							"shared_buffers":  "600MB", // Above new instance type's max (512MB)
							"work_mem":        "1MB",   // Below new instance type's min (2MB)
							"max_connections": "15",    // Below new instance type's min (20)
						},
					},
				}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{
					{Name: "xata.small", VCPUsRequest: 500, VCPUsLimit: 1000, RAM: 2},
					{Name: "xata.medium", VCPUsRequest: 1000, VCPUsLimit: 2000, RAM: 4},
				}, nil).Twice()
				// Old instance type (xata.small) specs
				mockPostgresConfig.EXPECT().GetParametersSpec("xata.small", mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(postgrescfg.ParametersMap{
					"shared_buffers": postgrescfg.PostgresParameterSpec{
						ParameterType: postgrescfg.ParamTypeBytes,
						DefaultValue:  "128MB",
						MinValue:      "16MB",
						MaxValue:      "256MB",
					},
					"work_mem": postgrescfg.PostgresParameterSpec{
						ParameterType: postgrescfg.ParamTypeBytes,
						DefaultValue:  "2MB",
						MinValue:      "1MB",
						MaxValue:      "8MB",
					},
					"max_connections": postgrescfg.PostgresParameterSpec{
						ParameterType: postgrescfg.ParamTypeInt,
						DefaultValue:  "25",
						MinValue:      "10",
						MaxValue:      "50",
					},
				}, nil).Once()
				// New instance type (xata.medium) specs
				mockPostgresConfig.EXPECT().GetParametersSpec("xata.medium", mock.AnythingOfType("int"), mock.AnythingOfType("string"), mock.Anything).Return(postgrescfg.ParametersMap{
					"shared_buffers": postgrescfg.PostgresParameterSpec{
						ParameterType: postgrescfg.ParamTypeBytes,
						DefaultValue:  "256MB",
						MinValue:      "32MB",
						MaxValue:      "512MB",
					},
					"work_mem": postgrescfg.PostgresParameterSpec{
						ParameterType: postgrescfg.ParamTypeBytes,
						DefaultValue:  "4MB",
						MinValue:      "2MB",
						MaxValue:      "16MB",
					},
					"max_connections": postgrescfg.PostgresParameterSpec{
						ParameterType: postgrescfg.ParamTypeInt,
						DefaultValue:  "50",
						MinValue:      "20",
						MaxValue:      "100",
					},
				}, nil).Once()
				// Expected update with adjusted parameters:
				// - shared_buffers: 512MB (adjusted down from 600MB to max)
				// - work_mem: 2MB (adjusted up from 1MB to min)
				// - max_connections: 20 (adjusted up from 15 to min)
				mockClusters.EXPECT().UpdatePostgresCluster(mock.Anything, &clustersv1.UpdatePostgresClusterRequest{
					Id: "123",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						VcpuRequest: new("1"),
						VcpuLimit:   new("2"),
						Memory:      new("4Gi"),
						PostgresConfigurationParameters: map[string]string{
							"shared_buffers":  "512MB", // Adjusted to max
							"work_mem":        "2MB",   // Adjusted to min
							"max_connections": "20",    // Adjusted to min
						},
					},
				}).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()
			},
			wantError: false,
		},
		{
			name:      "update branch preload libraries works",
			projectID: "project_id",
			branchID:  "123",
			jsonBody:  map[string]any{"preloadLibraries": []string{"pg_stat_statements", "auto_explain", "pg_cron"}},
			setupMocks: func() {
				branch := store.Branch{
					ID:          "123",
					Name:        "test",
					Description: new("desc"),
					Region:      "region-id-1",
				}
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: "123"}).Return(&clustersv1.DescribePostgresClusterResponse{
					Configuration: &clustersv1.ClusterConfiguration{
						ImageName:   "analytics:17",
						VcpuRequest: "500m",
						VcpuLimit:   "1000m",
						Memory:      "2",
					},
				}, nil).Once()
				mockPostgresConfig.EXPECT().ValidatePreloadLibraries("analytics:17", []string{"pg_stat_statements", "auto_explain", "pg_cron"}).Return(nil).Once()
				mockPostgresConfig.EXPECT().FilterConfigurableParameters(map[string]string{}, mock.AnythingOfType("int"), "analytics:17", []string{"xatautils", "pg_stat_statements", "auto_explain", "pg_cron"}).Return(map[string]string{}).Once()
				mockPostgresConfig.EXPECT().GetConfigurableParameters(mock.AnythingOfType("int"), "analytics:17", []string{"xatautils", "pg_stat_statements", "auto_explain", "pg_cron"}).Return(map[string]postgrescfg.PostgresParameterSpec{
					"auto_explain.log_min_duration": {DefaultValue: "10s", Extension: "auto_explain"},
					"cron.database_name":            {DefaultValue: "xata", Extension: "pg_cron"},
					"cron.use_background_workers":   {DefaultValue: "on", Extension: "pg_cron"},
					"pg_stat_statements.max":        {DefaultValue: "5000", Extension: "pg_stat_statements"},
					"pg_stat_statements.track":      {DefaultValue: "top", Extension: "pg_stat_statements"},
				}).Once()
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", updateBranchConfig(nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchID string, cfg *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Nil(t, err)
				}).Return(&branch, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: "region-id-1", GatewayHostPort: ""}, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: "123", Username: "superuser"}).Return(&clustersv1.GetPostgresClusterCredentialsResponse{
					Username: "user",
					Password: "pass",
				}, nil).Once()
				// Default extension parameters are added for newly preloaded extensions
				mockClusters.EXPECT().UpdatePostgresCluster(mock.Anything, &clustersv1.UpdatePostgresClusterRequest{
					Id: "123",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						PreloadLibraries: []string{"xatautils", "pg_stat_statements", "auto_explain", "pg_cron"},
						PostgresConfigurationParameters: map[string]string{
							"auto_explain.log_min_duration": "10s",
							"cron.database_name":            "xata",
							"cron.use_background_workers":   "on",
							"pg_stat_statements.max":        "5000",
							"pg_stat_statements.track":      "top",
						},
					},
				}).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()
			},
			wantError: false,
		},
		{
			name:      "update branch with invalid preload libraries fails",
			projectID: "project_id",
			branchID:  "123",
			jsonBody:  map[string]any{"preloadLibraries": []string{"invalid_library", "another_invalid"}},
			setupMocks: func() {
				branch := store.Branch{
					ID:          "123",
					Name:        "test",
					Description: new("desc"),
					Region:      "region-id-1",
				}
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: "123"}).Return(&clustersv1.DescribePostgresClusterResponse{
					Configuration: &clustersv1.ClusterConfiguration{ImageName: "analytics:17"},
				}, nil).Once()
				mockPostgresConfig.EXPECT().ValidatePreloadLibraries("analytics:17", []string{"invalid_library", "another_invalid"}).Return(errors.New("invalid preload library: invalid_library")).Once()
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", updateBranchConfig(nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchID string, cfg *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) {
					_ = provisionFn(&branch)
				}).Return(nil, ErrorInvalidParam{BranchName: "123", Param: "preloadLibraries", Message: "branch [123]: invalid preload library: invalid_library"}).Once()
			},
			wantError:     true,
			expectedError: ErrorInvalidParam{BranchName: "123", Param: "preloadLibraries", Message: "branch [123]: invalid preload library: invalid_library"},
		},
		{
			name:      "update branch preload libraries combined with other fields works",
			projectID: "project_id",
			branchID:  "123",
			jsonBody:  map[string]any{"name": "new-name", "preloadLibraries": []string{"pg_stat_statements", "pgaudit"}},
			setupMocks: func() {
				branch := store.Branch{
					ID:          "123",
					Name:        "new-name",
					Description: new("desc"),
					Region:      "region-id-1",
				}
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: "123"}).Return(&clustersv1.DescribePostgresClusterResponse{
					Configuration: &clustersv1.ClusterConfiguration{
						ImageName:   "analytics:17",
						VcpuRequest: "500m",
						VcpuLimit:   "1000m",
						Memory:      "2",
					},
				}, nil).Once()
				mockPostgresConfig.EXPECT().ValidatePreloadLibraries("analytics:17", []string{"pg_stat_statements", "pgaudit"}).Return(nil).Once()
				mockPostgresConfig.EXPECT().FilterConfigurableParameters(map[string]string{}, mock.AnythingOfType("int"), "analytics:17", []string{"xatautils", "pg_stat_statements", "pgaudit"}).Return(map[string]string{}).Once()
				mockPostgresConfig.EXPECT().GetConfigurableParameters(mock.AnythingOfType("int"), "analytics:17", []string{"xatautils", "pg_stat_statements", "pgaudit"}).Return(map[string]postgrescfg.PostgresParameterSpec{
					"pg_stat_statements.max":   {DefaultValue: "5000", Extension: "pg_stat_statements"},
					"pg_stat_statements.track": {DefaultValue: "top", Extension: "pg_stat_statements"},
				}).Once()
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", updateBranchConfig(new("new-name"), nil), mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchID string, cfg *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Nil(t, err)
				}).Return(&branch, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: "region-id-1", GatewayHostPort: ""}, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: "123", Username: "superuser"}).Return(&clustersv1.GetPostgresClusterCredentialsResponse{
					Username: "user",
					Password: "pass",
				}, nil).Once()
				// Default extension parameters are added for newly preloaded extensions
				mockClusters.EXPECT().UpdatePostgresCluster(mock.Anything, &clustersv1.UpdatePostgresClusterRequest{
					Id: "123",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						PreloadLibraries: []string{"xatautils", "pg_stat_statements", "pgaudit"},
						PostgresConfigurationParameters: map[string]string{
							"pg_stat_statements.max":   "5000",
							"pg_stat_statements.track": "top",
						},
					},
				}).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()
			},
			wantError: false,
		},
		{
			name:      "update branch removing extension from preload cleans up its parameters",
			projectID: "project_id",
			branchID:  "123",
			jsonBody:  map[string]any{"preloadLibraries": []string{"auto_explain"}}, // removing pg_stat_statements
			setupMocks: func() {
				branch := store.Branch{
					ID:          "123",
					Name:        "test",
					Description: new("desc"),
					Region:      "region-id-1",
				}
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: "123"}).Return(&clustersv1.DescribePostgresClusterResponse{
					Configuration: &clustersv1.ClusterConfiguration{
						ImageName:        "postgres:17",
						VcpuRequest:      "500m",
						VcpuLimit:        "1000m",
						Memory:           "2",
						PreloadLibraries: []string{"pg_stat_statements", "auto_explain"},
						PostgresConfigurationParameters: map[string]string{
							"pg_stat_statements.max":   "10000",
							"pg_stat_statements.track": "all",
							"work_mem":                 "4MB",
						},
					},
				}, nil).Once()
				mockPostgresConfig.EXPECT().ValidatePreloadLibraries("postgres:17", []string{"auto_explain"}).Return(nil).Once()
				// FilterConfigurableParameters removes pg_stat_statements params since it's not preloaded anymore
				mockPostgresConfig.EXPECT().FilterConfigurableParameters(
					map[string]string{
						"pg_stat_statements.max":   "10000",
						"pg_stat_statements.track": "all",
						"work_mem":                 "4MB",
					},
					mock.AnythingOfType("int"),
					"postgres:17",
					[]string{"xatautils", "auto_explain"},
				).Return(map[string]string{
					"work_mem": "4MB",
				}).Once()
				mockPostgresConfig.EXPECT().GetConfigurableParameters(mock.AnythingOfType("int"), "postgres:17", []string{"xatautils", "auto_explain"}).Return(map[string]postgrescfg.PostgresParameterSpec{
					"auto_explain.log_min_duration": {DefaultValue: "10s", Extension: "auto_explain"},
				}).Once()
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", updateBranchConfig(nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchID string, cfg *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Nil(t, err)
				}).Return(&branch, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: "region-id-1", GatewayHostPort: ""}, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: "123", Username: "superuser"}).Return(&clustersv1.GetPostgresClusterCredentialsResponse{
					Username: "user",
					Password: "pass",
				}, nil).Once()
				mockClusters.EXPECT().UpdatePostgresCluster(mock.Anything, mock.MatchedBy(func(req *clustersv1.UpdatePostgresClusterRequest) bool {
					if req.Id != "123" || req.UpdateConfiguration == nil {
						return false
					}
					// Verify preload libraries are updated (with internal xatautils prepended)
					expectedPreload := []string{"xatautils", "auto_explain"}
					if !slices.Equal(req.UpdateConfiguration.PreloadLibraries, expectedPreload) {
						return false
					}
					// Verify pg_stat_statements parameters are cleaned up, but work_mem remains
					// Also verify auto_explain default params are added
					params := req.UpdateConfiguration.PostgresConfigurationParameters
					if params == nil {
						return false
					}
					_, hasMax := params["pg_stat_statements.max"]
					_, hasTrack := params["pg_stat_statements.track"]
					workMem, hasWorkMem := params["work_mem"]
					autoExplainDuration, hasAutoExplain := params["auto_explain.log_min_duration"]
					return !hasMax && !hasTrack && hasWorkMem && workMem == "4MB" && hasAutoExplain && autoExplainDuration == "10s"
				})).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()
			},
			wantError: false,
		},
		{
			name:      "update branch params validated against new preload libraries when both are updated",
			projectID: "project_id",
			branchID:  "123",
			jsonBody: map[string]any{
				"preloadLibraries":                []string{"pg_stat_statements", "auto_explain"},
				"postgresConfigurationParameters": map[string]string{"pg_stat_statements.max": "10000"},
			},
			setupMocks: func() {
				branch := store.Branch{
					ID:          "123",
					Name:        "test",
					Description: new("desc"),
					Region:      "region-id-1",
				}
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: "123"}).Return(&clustersv1.DescribePostgresClusterResponse{
					Configuration: &clustersv1.ClusterConfiguration{
						ImageName:        "postgres:17",
						VcpuRequest:      "500m",
						VcpuLimit:        "1000m",
						Memory:           "2",
						PreloadLibraries: []string{}, // pg_stat_statements NOT currently preloaded
					},
				}, nil).Once()
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "region-id-1").Return([]store.InstanceType{{Name: "xata.micro", VCPUsRequest: 500, VCPUsLimit: 1000, RAM: 2}}, nil).Once()
				// ValidateSettings is called with the NEW preload list (pg_stat_statements included)
				mockPostgresConfig.EXPECT().ValidateSettings("xata.micro", map[string]string{"pg_stat_statements.max": "10000"}, mock.AnythingOfType("int"), "postgres:17", []string{"pg_stat_statements", "auto_explain"}).Return(nil, nil).Once()
				mockPostgresConfig.EXPECT().ValidatePreloadLibraries("postgres:17", []string{"pg_stat_statements", "auto_explain"}).Return(nil).Once()
				mockPostgresConfig.EXPECT().FilterConfigurableParameters(
					map[string]string{"pg_stat_statements.max": "10000"},
					mock.AnythingOfType("int"),
					"postgres:17",
					[]string{"xatautils", "pg_stat_statements", "auto_explain"},
				).Return(map[string]string{"pg_stat_statements.max": "10000"}).Once()
				mockPostgresConfig.EXPECT().GetConfigurableParameters(mock.AnythingOfType("int"), "postgres:17", []string{"xatautils", "pg_stat_statements", "auto_explain"}).Return(map[string]postgrescfg.PostgresParameterSpec{
					"auto_explain.log_min_duration": {DefaultValue: "10s", Extension: "auto_explain"},
					"pg_stat_statements.max":        {DefaultValue: "5000", Extension: "pg_stat_statements"},
					"pg_stat_statements.track":      {DefaultValue: "top", Extension: "pg_stat_statements"},
				}).Once()
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", &store.UpdateBranchConfiguration{}, mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchID string, cfg *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Nil(t, err)
				}).Return(&branch, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: "region-id-1", GatewayHostPort: ""}, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: "123", Username: "superuser"}).Return(&clustersv1.GetPostgresClusterCredentialsResponse{
					Username: "user",
					Password: "pass",
				}, nil).Once()
				mockClusters.EXPECT().UpdatePostgresCluster(mock.Anything, mock.MatchedBy(func(req *clustersv1.UpdatePostgresClusterRequest) bool {
					return req.Id == "123" &&
						req.UpdateConfiguration != nil &&
						req.UpdateConfiguration.PostgresConfigurationParameters != nil &&
						req.UpdateConfiguration.PostgresConfigurationParameters["pg_stat_statements.max"] == "10000" &&
						slices.Equal(req.UpdateConfiguration.PreloadLibraries, []string{"xatautils", "pg_stat_statements", "auto_explain"})
				})).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()
			},
			wantError: false,
		},
		{
			name:      "update branch backup configuration when backups are enabled works",
			projectID: "project_id",
			branchID:  "123",
			jsonBody:  map[string]any{"backupConfiguration": map[string]any{"backupTime": "0:05:00", "retentionPeriod": 14}},
			setupMocks: func() {
				branch := store.Branch{
					ID:             "123",
					Name:           "test",
					Description:    new("desc"),
					Region:         "region-id-1",
					BackupsEnabled: true,
				}
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", updateBranchConfig(nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchID string, cfg *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Nil(t, err)
				}).Return(&branch, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: "region-id-1", GatewayHostPort: ""}, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: "123", Username: "superuser"}).Return(&clustersv1.GetPostgresClusterCredentialsResponse{
					Username: "user",
					Password: "pass",
				}, nil).Once()
				mockClusters.EXPECT().UpdatePostgresCluster(mock.Anything, mock.MatchedBy(func(req *clustersv1.UpdatePostgresClusterRequest) bool {
					return req.Id == "123" &&
						req.UpdateConfiguration != nil &&
						req.UpdateConfiguration.BackupConfiguration != nil &&
						req.UpdateConfiguration.BackupConfiguration.BackupSchedule != "" &&
						req.UpdateConfiguration.BackupConfiguration.BackupRetention == "14d" &&
						req.UpdateConfiguration.BackupConfiguration.BackupsEnabled == true
				})).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()
			},
			wantError: false,
		},
		{
			name:      "update branch backup configuration when backups are disabled fails",
			projectID: "project_id",
			branchID:  "123",
			jsonBody:  map[string]any{"backupConfiguration": map[string]any{"backupTime": "0:05:00", "retentionPeriod": 14}},
			setupMocks: func() {
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", updateBranchConfig(nil, nil), mock.Anything).Return(nil, ErrorInvalidParam{BranchName: "123", Param: "backupConfiguration", Message: "backup configuration cannot be specified when backups are disabled in the selected region"}).Once()
			},
			wantError:     true,
			expectedError: ErrorInvalidParam{BranchName: "123", Param: "backupConfiguration", Message: "backup configuration cannot be specified when backups are disabled in the selected region"},
		},
		{
			name:      "update branch image minor upgrade works",
			projectID: "project_id",
			branchID:  "123",
			jsonBody:  map[string]string{"image": "postgres:17.7"},
			setupMocks: func() {
				branch := store.Branch{
					ID:          "123",
					Name:        "newTest",
					Description: new("newDesc"),
					Region:      "region-id-1",
				}
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", updateBranchConfig(nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchID string, cfg *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Nil(t, err)
				}).Return(&branch, nil).Once()
				mockClusters.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: "123", Username: "superuser"}).Return(&clustersv1.GetPostgresClusterCredentialsResponse{
					Username: "user",
					Password: "pass",
				}, nil).Once()
				mockStore.EXPECT().GetRegion(mock.Anything, apitest.TestOrganization, "region-id-1").Return(&store.Region{ID: "region-id-1", GatewayHostPort: ""}, nil).Once()
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: "123"}).Return(&clustersv1.DescribePostgresClusterResponse{
					Id: "123",
					Configuration: &clustersv1.ClusterConfiguration{
						ImageName: "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
					},
				}, nil).Once()
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.7"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.7").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.7").Once()
				mockImageProvider.EXPECT().ParseImageVersion("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.7").Return(&postgresversions.ImageVersion{Offering: "postgres", Major: 17, Minor: 7}, nil).Once()
				mockImageProvider.EXPECT().ParseImageVersion("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Return(&postgresversions.ImageVersion{Offering: "postgres", Major: 17, Minor: 5}, nil).Once()
				mockClusters.EXPECT().UpdatePostgresCluster(mock.Anything, &clustersv1.UpdatePostgresClusterRequest{
					Id: "123",
					UpdateConfiguration: &clustersv1.UpdateClusterConfiguration{
						ImageName: new("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.7"),
					},
				}).Return(&clustersv1.UpdatePostgresClusterResponse{}, nil).Once()
			},
			wantError: false,
		},
		{
			name:      "update branch image with different offering fails",
			projectID: "project_id",
			branchID:  "123",
			jsonBody:  map[string]string{"image": "postgres:17.7"},
			setupMocks: func() {
				branch := store.Branch{
					ID:          "123",
					Name:        "newTest",
					Description: new("newDesc"),
					Region:      "region-id-1",
				}
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", updateBranchConfig(nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchID string, cfg *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Error(t, err)
				}).Return(nil, ErrorInvalidParam{BranchName: "123", Param: "image", Message: "incompatible offering: postgres is not compatible with analytics"}).Once()
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: "123"}).Return(&clustersv1.DescribePostgresClusterResponse{
					Id: "123",
					Configuration: &clustersv1.ClusterConfiguration{
						ImageName: "ghcr.io/xataio/postgres-images/xata-analytics:17.5",
					},
				}, nil).Once()
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.7"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.7").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.7").Once()
				mockImageProvider.EXPECT().ParseImageVersion("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.7").Return(&postgresversions.ImageVersion{Offering: "postgres", Major: 17, Minor: 7}, nil).Once()
				mockImageProvider.EXPECT().ParseImageVersion("ghcr.io/xataio/postgres-images/xata-analytics:17.5").Return(&postgresversions.ImageVersion{Offering: "analytics", Major: 17, Minor: 5}, nil).Once()
			},
			wantError:     true,
			expectedError: ErrorInvalidParam{BranchName: "123", Param: "image", Message: "incompatible offering: postgres is not compatible with analytics"},
		},
		{
			name:      "update branch image with major version change fails",
			projectID: "project_id",
			branchID:  "123",
			jsonBody:  map[string]string{"image": "postgres:18.0"},
			setupMocks: func() {
				branch := store.Branch{
					ID:          "123",
					Name:        "newTest",
					Description: new("newDesc"),
					Region:      "region-id-1",
				}
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", updateBranchConfig(nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchID string, cfg *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Error(t, err)
				}).Return(nil, ErrorInvalidParam{BranchName: "123", Param: "image", Message: "no major version upgrades supported: 18 is different than current 17"}).Once()
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: "123"}).Return(&clustersv1.DescribePostgresClusterResponse{
					Id: "123",
					Configuration: &clustersv1.ClusterConfiguration{
						ImageName: "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
					},
				}, nil).Once()
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:18.0"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:18.0").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:18.0").Once()
				mockImageProvider.EXPECT().ParseImageVersion("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:18.0").Return(&postgresversions.ImageVersion{Offering: "postgres", Major: 18, Minor: 0}, nil).Once()
				mockImageProvider.EXPECT().ParseImageVersion("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Return(&postgresversions.ImageVersion{Offering: "postgres", Major: 17, Minor: 5}, nil).Once()
			},
			wantError:     true,
			expectedError: ErrorInvalidParam{BranchName: "123", Param: "image", Message: "no major version upgrades supported: 18 is different than current 17"},
		},
		{
			name:      "update branch image with minor downgrade fails",
			projectID: "project_id",
			branchID:  "123",
			jsonBody:  map[string]string{"image": "postgres:17.3"},
			setupMocks: func() {
				branch := store.Branch{
					ID:          "123",
					Name:        "newTest",
					Description: new("newDesc"),
					Region:      "region-id-1",
				}
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", updateBranchConfig(nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchID string, cfg *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Error(t, err)
				}).Return(nil, ErrorInvalidParam{BranchName: "123", Param: "image", Message: "new minor: 3 is older than current  5"}).Once()
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: "123"}).Return(&clustersv1.DescribePostgresClusterResponse{
					Id: "123",
					Configuration: &clustersv1.ClusterConfiguration{
						ImageName: "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
					},
				}, nil).Once()
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.3"}).Once()
				mockImageProvider.EXPECT().BuildImageURL("postgres:17.3").Return("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.3").Once()
				mockImageProvider.EXPECT().ParseImageVersion("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.3").Return(&postgresversions.ImageVersion{Offering: "postgres", Major: 17, Minor: 3}, nil).Once()
				mockImageProvider.EXPECT().ParseImageVersion("ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5").Return(&postgresversions.ImageVersion{Offering: "postgres", Major: 17, Minor: 5}, nil).Once()
			},
			wantError:     true,
			expectedError: ErrorInvalidParam{BranchName: "123", Param: "image", Message: "new minor: 3 is older than current  5"},
		},
		{
			name:      "update branch image with invalid image fails",
			projectID: "project_id",
			branchID:  "123",
			jsonBody:  map[string]string{"image": "postgres:99.99"},
			setupMocks: func() {
				branch := store.Branch{
					ID:          "123",
					Name:        "newTest",
					Description: new("newDesc"),
					Region:      "region-id-1",
				}
				mockStore.EXPECT().UpdateBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", updateBranchConfig(nil, nil), mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchID string, cfg *store.UpdateBranchConfiguration, provisionFn func(*store.Branch) error) {
					err := provisionFn(&branch)
					assert.Error(t, err)
				}).Return(nil, ErrorInvalidParam{BranchName: "123", Param: "image", Message: "image postgres:99.99 is not valid"}).Once()
				mockClusters.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: "123"}).Return(&clustersv1.DescribePostgresClusterResponse{
					Id: "123",
					Configuration: &clustersv1.ClusterConfiguration{
						ImageName: "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
					},
				}, nil).Once()
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.5"}).Once()
			},
			wantError:     true,
			expectedError: ErrorInvalidParam{BranchName: "123", Param: "image", Message: "image postgres:99.99 is not valid"},
		},
	}
	for _, tt := range updateBranchTests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupMocks()
			c, rec := e.PATCH("/organizations/" + apitest.TestOrganization + "/projects/" + tt.projectID + "/branches/" + tt.branchID).WithJSONBody(tt.jsonBody).Context()
			err := handler.UpdateBranch(c, apitest.TestOrganization, tt.projectID, tt.branchID)
			if !tt.wantError {
				require.Nil(t, err, "expected no error, got %v", err)
				var gotBranch spec.BranchMetadata
				rec.MustCode(http.StatusOK)
				rec.ReadBody(&gotBranch)
				assert.Equal(t, tt.branchID, gotBranch.Id)
				assert.Equal(t, new("postgresql://user:pass@123.testdomain:5432/xata?sslmode=require"), gotBranch.ConnectionString)
			} else {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			}

			mockStore.AssertExpectations(t)
			mockClusters.AssertExpectations(t)
		})
	}
}

func TestDeleteBranch(t *testing.T) {
	mockStore := mocks.NewProjectsStore(t)
	mockClusters := protomocks.NewClustersServiceClient(t)
	mockCells := cellsmock.NewCellsMock(t, mockClusters)

	feat := openfeaturetest.NewClient(nil)
	sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
	mockAnalytics := analyticsmocks.NewClient(t)
	apiHandler := NewAPIHandler(feat, mockStore, mockCells, "", createNewSigNozClient(t), sched, mockAnalytics, nil, nil)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

	branch := store.Branch{
		ID:     "123",
		Name:   "test",
		CellID: "primary_cell",
		Region: "test-region",
	}

	secondaryBranch := store.Branch{
		ID:     "456",
		Name:   "test-secondary",
		CellID: "secondary_cell",
		Region: "test-region",
	}

	deleteBranchTests := []struct {
		name                          string
		projectID                     string
		branchID                      string
		wantError                     bool
		expectedError                 string
		deleteBranchCall              *mocks.ProjectsStore_DeleteBranch_Call
		deletePostgresClusterCall     *protomocks.ClustersServiceClient_DeletePostgresCluster_Call
		getPrimaryCellCall            *mocks.ProjectsStore_GetPrimaryCell_Call
		deregisterPostgresClusterCall *protomocks.ClustersServiceClient_DeregisterPostgresCluster_Call
	}{
		{
			name:      "delete branch works",
			projectID: "project_id",
			branchID:  branch.ID,
			wantError: false,
			deleteBranchCall: mockStore.EXPECT().DeleteBranch(mock.Anything, apitest.TestOrganization, "project_id", branch.ID, mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchName string, deprovisionFn func(*store.Branch) error) {
				err := deprovisionFn(&branch)
				assert.Nil(t, err)
			}).Return(nil),
			deletePostgresClusterCall: mockClusters.EXPECT().DeletePostgresCluster(mock.Anything, &clustersv1.DeletePostgresClusterRequest{Id: branch.ID}).Return(&clustersv1.DeletePostgresClusterResponse{}, nil),
			getPrimaryCellCall:        mockStore.EXPECT().GetPrimaryCell(mock.Anything, apitest.TestOrganization, "test-region").Return(&store.Cell{ID: "primary_cell", RegionID: "test-region", Primary: true}, nil),
		},
		{
			name:          "delete branch fails for storage error",
			projectID:     "project_id",
			branchID:      "123",
			wantError:     true,
			expectedError: fmt.Errorf("some error").Error(),
			deleteBranchCall: mockStore.EXPECT().DeleteBranch(mock.Anything, apitest.TestOrganization, "project_id", branch.ID, mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchName string, deprovisionFn func(*store.Branch) error) {
				err := deprovisionFn(&branch)
				assert.Nil(t, err)
			}).Return(fmt.Errorf("some error")),
			deletePostgresClusterCall: mockClusters.EXPECT().DeletePostgresCluster(mock.Anything, &clustersv1.DeletePostgresClusterRequest{Id: branch.ID}).Return(&clustersv1.DeletePostgresClusterResponse{}, nil),
			getPrimaryCellCall:        mockStore.EXPECT().GetPrimaryCell(mock.Anything, apitest.TestOrganization, "test-region").Return(&store.Cell{ID: "primary_cell", RegionID: "test-region", Primary: true}, nil),
		},
		{
			name:          "delete branch fails for infra error",
			projectID:     "project_id",
			branchID:      "123",
			wantError:     true,
			expectedError: fmt.Errorf("some cluster error").Error(),
			deleteBranchCall: mockStore.EXPECT().DeleteBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchName string, deprovisionFn func(*store.Branch) error) {
				err := deprovisionFn(&branch)
				assert.Error(t, err)
			}).Return(fmt.Errorf("some cluster error")),
			deletePostgresClusterCall: mockClusters.EXPECT().DeletePostgresCluster(mock.Anything, &clustersv1.DeletePostgresClusterRequest{Id: "123"}).Return(nil, fmt.Errorf("some cluster error")),
		},
		{
			name:          "delete branch fails for not found cluster error",
			projectID:     "project_id",
			branchID:      "123",
			wantError:     true,
			expectedError: ErrorBranchNotFound{BranchID: "123"}.Error(),
			deleteBranchCall: mockStore.EXPECT().DeleteBranch(mock.Anything, apitest.TestOrganization, "project_id", "123", mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchName string, deprovisionFn func(*store.Branch) error) {
				err := deprovisionFn(&branch)
				assert.Error(t, err)
			}).Return(clusters.ClusterNotFoundError("123")),
			deletePostgresClusterCall: mockClusters.EXPECT().DeletePostgresCluster(mock.Anything, &clustersv1.DeletePostgresClusterRequest{Id: "123"}).Return(nil, clusters.ClusterNotFoundError("123")),
		},
		{
			name:      "delete branch from secondary cell works",
			projectID: "project_id",
			branchID:  secondaryBranch.ID,
			wantError: false,
			deleteBranchCall: mockStore.EXPECT().DeleteBranch(mock.Anything, apitest.TestOrganization, "project_id", secondaryBranch.ID, mock.Anything).Run(func(ctx context.Context, organizationID string, projectID string, branchName string, deprovisionFn func(*store.Branch) error) {
				err := deprovisionFn(&secondaryBranch)
				assert.Nil(t, err)
			}).Return(nil),
			deletePostgresClusterCall:     mockClusters.EXPECT().DeletePostgresCluster(mock.Anything, &clustersv1.DeletePostgresClusterRequest{Id: secondaryBranch.ID}).Return(&clustersv1.DeletePostgresClusterResponse{}, nil),
			getPrimaryCellCall:            mockStore.EXPECT().GetPrimaryCell(mock.Anything, apitest.TestOrganization, "test-region").Return(&store.Cell{ID: "primary_cell", RegionID: "test-region", Primary: true}, nil),
			deregisterPostgresClusterCall: mockClusters.EXPECT().DeregisterPostgresCluster(mock.Anything, &clustersv1.DeregisterPostgresClusterRequest{Id: secondaryBranch.ID}).Return(&clustersv1.DeregisterPostgresClusterResponse{}, nil),
		},
	}
	for _, tt := range deleteBranchTests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.deleteBranchCall != nil {
				tt.deleteBranchCall.Once()
			}
			if tt.deletePostgresClusterCall != nil {
				tt.deletePostgresClusterCall.Once()
			}
			if tt.getPrimaryCellCall != nil {
				tt.getPrimaryCellCall.Once()
			}
			if tt.deregisterPostgresClusterCall != nil {
				tt.deregisterPostgresClusterCall.Once()
			}
			mockClusters.EXPECT().DeleteBranchIPFiltering(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
			if !tt.wantError {
				mockAnalytics.EXPECT().Track(mock.Anything, events.NewBranchDeletedEvent(apitest.TestOrganization, tt.projectID, tt.branchID)).Return().Once()
			}
			c, rec := e.DELETE("/organizations/" + apitest.TestOrganization + "/projects/" + tt.projectID + "/branches/" + tt.branchID).Context()
			err := apiHandler.DeleteBranch(c, apitest.TestOrganization, tt.projectID, tt.branchID)
			if tt.wantError {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err.Error())
			} else {
				assert.Nil(t, err)
				rec.MustCode(http.StatusNoContent)
			}
		})
	}
}

func TestHandler_GetDefaultProjectLimits(t *testing.T) {
	mockStore := mocks.NewProjectsStore(t)
	mockImageProvider := postgresversionsmocks.NewImageProvider(t)
	feat := openfeaturetest.NewClient(nil)
	sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
	handler := NewAPIHandler(feat, mockStore, nil, "", createNewSigNozClient(t), sched, analyticsmocks.NewClient(t), nil, mockImageProvider)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

	validVersions := []string{"cnpg-postgres-plus:17.5", "cnpg-postgres-plus:17.6", "cnpg-postgres-plus:16.3", "cnpg-postgres-plus:16.4"}

	limits := spec.ProjectLimits{
		MaxDescriptionLength: MaxBranchDescriptionLength,
		MaxInstances:         DefaultMaxInstances,
		MinInstances:         DefaultMinInstances,
		Images:               append(validVersions, "postgresql:17"),
		Regions:              DefaultRegion,
		MaxBranches:          store.MaxBranchesPerProject,
	}

	mockImageProvider.EXPECT().GetAllImageNames().Return(validVersions).Once()

	c, rec := e.GET("/organizations/" + apitest.TestOrganization + "/projects/limits").Context()
	err := handler.GetDefaultProjectLimits(c, apitest.TestOrganization)
	assert.Nil(t, err)

	var res spec.ProjectLimits
	rec.MustCode(http.StatusOK)
	rec.ReadBody(&res)
	assert.Equal(t, limits, res)
}

func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestFormatCPUResource(t *testing.T) {
	tests := []struct {
		name      string
		milliCPUs int
		expected  string
	}{
		{
			name:      "250 millicores",
			milliCPUs: 250,
			expected:  "250m",
		},
		{
			name:      "500 millicores",
			milliCPUs: 500,
			expected:  "500m",
		},
		{
			name:      "999 millicores",
			milliCPUs: 999,
			expected:  "999m",
		},
		{
			name:      "1000 millicores (1 core)",
			milliCPUs: 1000,
			expected:  "1",
		},
		{
			name:      "2000 millicores (2 cores)",
			milliCPUs: 2000,
			expected:  "2",
		},
		{
			name:      "3500 millicores (3.5 cores)",
			milliCPUs: 3500,
			expected:  "3",
		},
		{
			name:      "zero millicores",
			milliCPUs: 0,
			expected:  "0m",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatCPUResource(tt.milliCPUs)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseCPUResource(t *testing.T) {
	tests := []struct {
		name     string
		cpuSpec  string
		expected int
	}{
		{
			name:     "250 millicores",
			cpuSpec:  "250m",
			expected: 250,
		},
		{
			name:     "500 millicores",
			cpuSpec:  "500m",
			expected: 500,
		},
		{
			name:     "1000 millicores",
			cpuSpec:  "1000m",
			expected: 1000,
		},
		{
			name:     "1 core",
			cpuSpec:  "1",
			expected: 1000,
		},
		{
			name:     "2 cores",
			cpuSpec:  "2",
			expected: 2000,
		},
		{
			name:     "2.5 cores",
			cpuSpec:  "2.5",
			expected: 2500,
		},
		{
			name:     "0.5 cores",
			cpuSpec:  "0.5",
			expected: 500,
		},
		{
			name:     "zero cores",
			cpuSpec:  "0",
			expected: 0,
		},
		{
			name:     "zero millicores",
			cpuSpec:  "0m",
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseCPUResource(tt.cpuSpec)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test round-trip conversion: milliCPUs -> format -> parse -> milliCPUs
func TestCPUResourceRoundTrip(t *testing.T) {
	testCases := []int{250, 500, 999, 1000, 2000, 3500}

	for _, milliCPUs := range testCases {
		t.Run(fmt.Sprintf("%d_millicores", milliCPUs), func(t *testing.T) {
			formatted := formatCPUResource(milliCPUs)
			parsed, err := parseCPUResource(formatted)
			assert.NoError(t, err)

			// For values >= 1000, formatting truncates fractional cores
			if milliCPUs >= 1000 {
				expectedParsed := (milliCPUs / 1000) * 1000
				assert.Equal(t, expectedParsed, parsed)
			} else {
				assert.Equal(t, milliCPUs, parsed)
			}
		})
	}
}

func TestGetBranchPostgresConfig(t *testing.T) {
	tests := []struct {
		name                string
		organizationID      spec.OrganizationID
		projectID           string
		branchID            string
		setupMocks          func(*mocks.ProjectsStore, *protomocks.ClustersServiceClient)
		setupPostgresConfig func(*postgrescfgmocks.PostgresConfigProvider)
		expectedError       any
	}{
		{
			name:           "successful postgres config retrieval",
			organizationID: apitest.TestOrganization,
			projectID:      "test-project",
			branchID:       "test-branch",
			setupMocks: func(mockStore *mocks.ProjectsStore, mockClient *protomocks.ClustersServiceClient) {
				// Mock branch retrieval
				branch := &store.Branch{
					ID:     "test-branch",
					CellID: "test-cell",
					Region: "us-east-1",
				}
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "test-project", "test-branch").Return(branch, nil).Once()

				// Mock postgres cluster description
				cluster := &clustersv1.DescribePostgresClusterResponse{
					Configuration: &clustersv1.ClusterConfiguration{
						VcpuRequest: "500m",
						VcpuLimit:   "2000m",
						Memory:      "2",
						PostgresConfigurationParameters: map[string]string{
							"max_connections": "100",
						},
					},
				}
				mockClient.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: "test-branch"}).Return(cluster, nil).Once()

				// Mock instance types - use a valid instance type that matches the resources
				instanceTypes := []store.InstanceType{
					{Name: "xata.small", VCPUsRequest: 500, VCPUsLimit: 2000, RAM: 2},
				}
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "us-east-1").Return(instanceTypes, nil).Once()
			},
			setupPostgresConfig: func(mockPostgresConfig *postgrescfgmocks.PostgresConfigProvider) {
				configurableParams := map[string]postgrescfg.PostgresParameterSpec{
					"max_connections": {DefaultValue: "100"},
				}
				mockPostgresConfig.EXPECT().GetConfigurableParameters(mock.Anything, mock.Anything, mock.Anything).Return(configurableParams).Once()
				mockPostgresConfig.EXPECT().GetParametersSpec("xata.small", mock.Anything, mock.Anything, mock.Anything).Return(postgrescfg.ParametersMap{}, nil).Once()
			},
			expectedError: nil,
		},
		{
			name:           "branch not found",
			organizationID: apitest.TestOrganization,
			projectID:      "test-project",
			branchID:       "nonexistent-branch",
			setupMocks: func(mockStore *mocks.ProjectsStore, mockClient *protomocks.ClustersServiceClient) {
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "test-project", "nonexistent-branch").Return(nil, &store.ErrBranchNotFound{ID: "nonexistent-branch"}).Once()
			},
			setupPostgresConfig: nil,
			expectedError:       &store.ErrBranchNotFound{ID: "nonexistent-branch"},
		},
		{
			name:           "postgres cluster not found",
			organizationID: apitest.TestOrganization,
			projectID:      "test-project",
			branchID:       "test-branch",
			setupMocks: func(mockStore *mocks.ProjectsStore, mockClient *protomocks.ClustersServiceClient) {
				branch := &store.Branch{
					ID:     "test-branch",
					CellID: "test-cell",
					Region: "us-east-1",
				}
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "test-project", "test-branch").Return(branch, nil).Once()

				// Return NotFound error
				notFoundErr := status.Error(codes.NotFound, "cluster not found")
				mockClient.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: "test-branch"}).Return(nil, notFoundErr).Once()
			},
			setupPostgresConfig: nil,
			expectedError:       ErrorBranchNotFound{BranchID: "test-branch"},
		},
		{
			name:           "fallback to custom instance type",
			organizationID: apitest.TestOrganization,
			projectID:      "test-project",
			branchID:       "test-branch",
			setupMocks: func(mockStore *mocks.ProjectsStore, mockClient *protomocks.ClustersServiceClient) {
				branch := &store.Branch{
					ID:     "test-branch",
					CellID: "test-cell",
					Region: "us-east-1",
				}
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "test-project", "test-branch").Return(branch, nil).Once()

				cluster := &clustersv1.DescribePostgresClusterResponse{
					Configuration: &clustersv1.ClusterConfiguration{
						VcpuRequest:                     "999", // Invalid combination
						VcpuLimit:                       "999",
						Memory:                          "999999",
						PostgresConfigurationParameters: map[string]string{},
					},
				}
				mockClient.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: "test-branch"}).Return(cluster, nil).Once()

				// Mock instance types that don't match
				instanceTypes := []store.InstanceType{
					{Name: "xata.small", VCPUsRequest: 1, VCPUsLimit: 2, RAM: 2048},
				}
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "us-east-1").Return(instanceTypes, nil).Once()
			},
			setupPostgresConfig: func(mockPostgresConfig *postgrescfgmocks.PostgresConfigProvider) {
				configurableParams := map[string]postgrescfg.PostgresParameterSpec{}
				mockPostgresConfig.EXPECT().GetConfigurableParameters(mock.Anything, mock.Anything, mock.Anything).Return(configurableParams).Once()
				mockPostgresConfig.EXPECT().GetParametersSpec("custom", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("unknown instance size custom")).Once()
			},
			expectedError: errors.New("getting instance defaults: unknown instance size custom"),
		},
		{
			name:           "cell connection error",
			organizationID: apitest.TestOrganization,
			projectID:      "test-project",
			branchID:       "test-branch",
			setupMocks: func(mockStore *mocks.ProjectsStore, mockClient *protomocks.ClustersServiceClient) {
				branch := &store.Branch{
					ID:     "test-branch",
					CellID: "test-cell",
					Region: "us-east-1",
				}
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "test-project", "test-branch").Return(branch, nil).Once()
			},
			setupPostgresConfig: nil,
			expectedError:       errors.New("connection failed"),
		},
		{
			name:           "instance types list error",
			organizationID: apitest.TestOrganization,
			projectID:      "test-project",
			branchID:       "test-branch",
			setupMocks: func(mockStore *mocks.ProjectsStore, mockClient *protomocks.ClustersServiceClient) {
				branch := &store.Branch{
					ID:     "test-branch",
					CellID: "test-cell",
					Region: "us-east-1",
				}
				mockStore.EXPECT().DescribeBranch(mock.Anything, apitest.TestOrganization, "test-project", "test-branch").Return(branch, nil).Once()

				cluster := &clustersv1.DescribePostgresClusterResponse{
					Configuration: &clustersv1.ClusterConfiguration{
						VcpuRequest:                     "500m",
						VcpuLimit:                       "2000m",
						Memory:                          "2",
						PostgresConfigurationParameters: map[string]string{},
					},
				}
				mockClient.EXPECT().DescribePostgresCluster(mock.Anything, &clustersv1.DescribePostgresClusterRequest{Id: "test-branch"}).Return(cluster, nil).Once()

				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "us-east-1").Return(nil, errors.New("failed to list instance types")).Once()
			},
			setupPostgresConfig: nil,
			expectedError:       errors.New("converting resources to instance type: failed to list instance types"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStore := mocks.NewProjectsStore(t)
			mockClient := protomocks.NewClustersServiceClient(t)
			mockPostgresConfig := postgrescfgmocks.NewPostgresConfigProvider(t)

			// Use regular mock for cell connection error test, otherwise use helper
			var mockCells cells.Cells
			if tt.name == "cell connection error" {
				mockCells = cellsmock.NewCells(t)
				if cellsMock, ok := mockCells.(*cellsmock.Cells); ok {
					cellsMock.EXPECT().GetCellConnection(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("connection failed")).Once()
				}
			} else {
				mockCells = cellsmock.NewCellsMock(t, mockClient)
			}

			feat := openfeaturetest.NewClient(nil)
			sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
			handler := NewAPIHandler(feat, mockStore, mockCells, "", createNewSigNozClient(t), sched, analyticsmocks.NewClient(t), mockPostgresConfig, nil)
			e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

			tt.setupMocks(mockStore, mockClient)
			if tt.setupPostgresConfig != nil {
				tt.setupPostgresConfig(mockPostgresConfig)
			}

			c, rec := e.GET("/organizations/" + apitest.TestOrganization + "/projects/" + tt.projectID + "/branches/" + tt.branchID + "/postgres-config").Context()
			err := handler.GetBranchPostgresConfig(c, tt.organizationID, tt.projectID, tt.branchID)

			if tt.expectedError != nil {
				assert.Error(t, err)
				if expectedErr, ok := tt.expectedError.(error); ok {
					assert.Equal(t, expectedErr.Error(), err.Error())
				}
			} else {
				assert.NoError(t, err)
				rec.MustCode(http.StatusOK)
			}

			mockStore.AssertExpectations(t)
			mockClient.AssertExpectations(t)
		})
	}
}

func TestListInstanceTypes(t *testing.T) {
	t.Parallel()

	feat := openfeaturetest.NewClient(nil)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

	instanceTypes := []store.InstanceType{
		{
			Name:               "xata.micro",
			VCPUsRequest:       250,
			VCPUsLimit:         2000,
			RAM:                1,
			HourlyRate:         0.01,
			StorageMonthlyRate: 0.05,
			Region:             "us-east-1",
		},
		{
			Name:               "xata.small",
			VCPUsRequest:       500,
			VCPUsLimit:         2000,
			RAM:                2,
			HourlyRate:         0.02,
			StorageMonthlyRate: 0.10,
			Region:             "us-east-1",
		},
	}

	tests := []struct {
		name           string
		region         string
		setupMocks     func(*mocks.ProjectsStore)
		wantError      bool
		expectedError  error
		assertResponse func(t *testing.T, rec *apitest.ResponseRecorder)
	}{
		{
			name:   "list instance types succeeds",
			region: "us-east-1",
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().ListRegions(mock.Anything, apitest.TestOrganization).Return([]store.Region{{ID: "us-east-1"}}, nil)
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "us-east-1").Return(instanceTypes, nil)
			},
			wantError: false,
			assertResponse: func(t *testing.T, rec *apitest.ResponseRecorder) {
				var res struct {
					InstanceTypes []struct {
						Name               string  `json:"name"`
						VCPUs              int     `json:"vcpus"`
						RAM                int     `json:"ram"`
						HourlyRate         float64 `json:"hourlyRate"`
						StorageMonthlyRate float64 `json:"storageMonthlyRate"`
						Region             string  `json:"region"`
					} `json:"instanceTypes"`
				}
				rec.MustCode(http.StatusOK)
				rec.ReadBody(&res)
				assert.Equal(t, 2, len(res.InstanceTypes))
				assert.Equal(t, instanceTypes[0].Name, res.InstanceTypes[0].Name)
				assert.Equal(t, instanceTypes[0].VCPUsRequest, res.InstanceTypes[0].VCPUs)
				assert.Equal(t, instanceTypes[0].RAM, res.InstanceTypes[0].RAM)
				assert.Equal(t, instanceTypes[0].HourlyRate, res.InstanceTypes[0].HourlyRate)
				assert.Equal(t, instanceTypes[0].StorageMonthlyRate, res.InstanceTypes[0].StorageMonthlyRate)
				assert.Equal(t, instanceTypes[0].Region, res.InstanceTypes[0].Region)
				assert.Equal(t, instanceTypes[1].Name, res.InstanceTypes[1].Name)
				assert.Equal(t, instanceTypes[1].VCPUsRequest, res.InstanceTypes[1].VCPUs)
				assert.Equal(t, instanceTypes[1].RAM, res.InstanceTypes[1].RAM)
				assert.Equal(t, instanceTypes[1].HourlyRate, res.InstanceTypes[1].HourlyRate)
				assert.Equal(t, instanceTypes[1].StorageMonthlyRate, res.InstanceTypes[1].StorageMonthlyRate)
				assert.Equal(t, instanceTypes[1].Region, res.InstanceTypes[1].Region)
			},
		},
		{
			name:   "list instance types with invalid region fails",
			region: "invalid-region",
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().ListRegions(mock.Anything, apitest.TestOrganization).Return([]store.Region{{ID: "us-west-2"}}, nil)
			},
			wantError:     true,
			expectedError: ErrorInvalidParam{Param: "region", Message: "invalid region: region invalid-region is not found"},
		},
		{
			name:   "list instance types with store error fails",
			region: "us-east-1",
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().ListRegions(mock.Anything, apitest.TestOrganization).Return([]store.Region{{ID: "us-east-1"}}, nil)
				mockStore.EXPECT().ListInstanceTypes(mock.Anything, apitest.TestOrganization, "us-east-1").Return(nil, fmt.Errorf("store error"))
			},
			wantError:     true,
			expectedError: fmt.Errorf("store error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockStore := mocks.NewProjectsStore(t)
			tt.setupMocks(mockStore)
			sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
			handler := NewAPIHandler(feat, mockStore, nil, "", createNewSigNozClient(t), sched, analyticsmocks.NewClient(t), nil, nil)

			c, rec := e.GET("/organizations/" + apitest.TestOrganization + "/instanceTypes?region=" + tt.region).Context()
			err := handler.ListInstanceTypes(c, apitest.TestOrganization, spec.ListInstanceTypesParams{Region: tt.region})

			if tt.wantError {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				assert.NoError(t, err)
				if tt.assertResponse != nil {
					tt.assertResponse(t, rec)
				}
			}

			mockStore.AssertExpectations(t)
		})
	}
}

func TestListImages(t *testing.T) {
	listImagesTests := []struct {
		name           string
		organizationID string
		params         spec.ListImagesParams
		featureFlags   map[openfeature.FeatureFlag]bool
		setupMocks     func(mockStore *mocks.ProjectsStore, mockImageProvider *postgresversionsmocks.ImageProvider)
		wantError      bool
		expectedError  error
		expectedImages []spec.Image
	}{
		{
			name:           "list images successfully",
			organizationID: apitest.TestOrganization,
			params: spec.ListImagesParams{
				Region: new("us-east-1"),
			},
			setupMocks: func(mockStore *mocks.ProjectsStore, mockImageProvider *postgresversionsmocks.ImageProvider) {
				regions := []store.Region{
					{ID: "us-east-1", OrganizationID: new("org-id"), PublicAccess: false},
					{ID: "us-west-2", PublicAccess: true},
				}
				mockStore.EXPECT().ListRegions(mock.Anything, apitest.TestOrganization).Return(regions, nil).Once()
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.5", "postgres:17.6", "postgres:16.3", "postgres:16.4"}).Once()
				mockImageProvider.EXPECT().ExtractVersionFromImageName("postgres:17.5").Return("17.5").Once()
				mockImageProvider.EXPECT().GetMajorForVersion("17.5").Return("17").Once()
				mockImageProvider.EXPECT().ExtractVersionFromImageName("postgres:17.6").Return("17.6").Once()
				mockImageProvider.EXPECT().GetMajorForVersion("17.6").Return("17").Once()
				mockImageProvider.EXPECT().ExtractVersionFromImageName("postgres:16.3").Return("16.3").Once()
				mockImageProvider.EXPECT().GetMajorForVersion("16.3").Return("16").Once()
				mockImageProvider.EXPECT().ExtractVersionFromImageName("postgres:16.4").Return("16.4").Once()
				mockImageProvider.EXPECT().GetMajorForVersion("16.4").Return("16").Once()
			},
			wantError: false,
			expectedImages: []spec.Image{
				{
					MajorVersion: "17",
					FullVersion:  "17.5",
					Name:         "postgres:17.5",
				},
				{
					MajorVersion: "17",
					FullVersion:  "17.6",
					Name:         "postgres:17.6",
				},
				{
					MajorVersion: "16",
					FullVersion:  "16.3",
					Name:         "postgres:16.3",
				},
				{
					MajorVersion: "16",
					FullVersion:  "16.4",
					Name:         "postgres:16.4",
				},
			},
		},
		{
			name:           "list images with invalid region fails",
			organizationID: apitest.TestOrganization,
			params: spec.ListImagesParams{
				Region: new("invalid-region"),
			},
			setupMocks: func(mockStore *mocks.ProjectsStore, mockImageProvider *postgresversionsmocks.ImageProvider) {
				regions := []store.Region{
					{ID: "us-east-1", OrganizationID: new("org-id"), PublicAccess: false},
					{ID: "us-west-2", PublicAccess: true},
				}
				mockStore.EXPECT().ListRegions(mock.Anything, apitest.TestOrganization).Return(regions, nil).Once()
			},
			wantError:     true,
			expectedError: ErrorInvalidParam{Param: "region", Message: "invalid region: region invalid-region is not found"},
		},
		{
			name:           "list images without region parameter works",
			organizationID: apitest.TestOrganization,
			params:         spec.ListImagesParams{},
			setupMocks: func(mockStore *mocks.ProjectsStore, mockImageProvider *postgresversionsmocks.ImageProvider) {
				// No region validation when region is nil
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.6"}).Once()
				mockImageProvider.EXPECT().ExtractVersionFromImageName("postgres:17.6").Return("17.6").Once()
				mockImageProvider.EXPECT().GetMajorForVersion("17.6").Return("17").Once()
			},
			wantError: false,
			expectedImages: []spec.Image{
				{
					MajorVersion: "17",
					Name:         "postgres:17.6",
					FullVersion:  "17.6",
				},
			},
		},
		{
			name:           "list images with empty versions list returns empty response",
			organizationID: apitest.TestOrganization,
			params: spec.ListImagesParams{
				Region: new("us-east-1"),
			},
			setupMocks: func(mockStore *mocks.ProjectsStore, mockImageProvider *postgresversionsmocks.ImageProvider) {
				regions := []store.Region{
					{ID: "us-east-1", OrganizationID: new("org-id"), PublicAccess: false},
					{ID: "us-west-2", PublicAccess: true},
				}
				mockStore.EXPECT().ListRegions(mock.Anything, apitest.TestOrganization).Return(regions, nil).Once()
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{}).Once()
			},
			wantError:      false,
			expectedImages: []spec.Image{},
		},
		{
			name:           "list images with single version works",
			organizationID: apitest.TestOrganization,
			params: spec.ListImagesParams{
				Region: new("us-west-2"),
			},
			setupMocks: func(mockStore *mocks.ProjectsStore, mockImageProvider *postgresversionsmocks.ImageProvider) {
				regions := []store.Region{
					{ID: "us-east-1", OrganizationID: new("org-id"), PublicAccess: false},
					{ID: "us-west-2", PublicAccess: true},
				}
				mockStore.EXPECT().ListRegions(mock.Anything, apitest.TestOrganization).Return(regions, nil).Once()
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{"postgres:17.6"}).Once()
				mockImageProvider.EXPECT().ExtractVersionFromImageName("postgres:17.6").Return("17.6").Once()
				mockImageProvider.EXPECT().GetMajorForVersion("17.6").Return("17").Once()
			},
			wantError: false,
			expectedImages: []spec.Image{
				{
					MajorVersion: "17",
					Name:         "postgres:17.6",
					FullVersion:  "17.6",
				},
			},
		},
		{
			name:           "list images with region store error fails with direct error",
			organizationID: apitest.TestOrganization,
			params: spec.ListImagesParams{
				Region: new("us-east-1"),
			},
			setupMocks: func(mockStore *mocks.ProjectsStore, mockImageProvider *postgresversionsmocks.ImageProvider) {
				mockStore.EXPECT().ListRegions(mock.Anything, apitest.TestOrganization).Return(nil, fmt.Errorf("database connection error")).Once()
				// No image provider expectations since function returns early on validation error
			},
			wantError: true,
		},
		{
			name:           "filters out experimental images when flag is disabled",
			organizationID: apitest.TestOrganization,
			params:         spec.ListImagesParams{},
			featureFlags:   nil, // ExperimentalImages flag defaults to false
			setupMocks: func(mockStore *mocks.ProjectsStore, mockImageProvider *postgresversionsmocks.ImageProvider) {
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{
					"postgres:17.7",
					"experimental:17.7",
					"xata-analytics:17.7",
				}).Once()
				// Only non-experimental images should be processed
				mockImageProvider.EXPECT().ExtractVersionFromImageName("postgres:17.7").Return("17.7").Once()
				mockImageProvider.EXPECT().GetMajorForVersion("17.7").Return("17").Once()
				mockImageProvider.EXPECT().ExtractVersionFromImageName("xata-analytics:17.7").Return("17.7").Once()
				mockImageProvider.EXPECT().GetMajorForVersion("17.7").Return("17").Once()
			},
			wantError: false,
			expectedImages: []spec.Image{
				{MajorVersion: "17", FullVersion: "17.7", Name: "postgres:17.7"},
				{MajorVersion: "17", FullVersion: "17.7", Name: "xata-analytics:17.7"},
			},
		},
		{
			name:           "includes experimental images when flag is enabled",
			organizationID: apitest.TestOrganization,
			params:         spec.ListImagesParams{},
			featureFlags: map[openfeature.FeatureFlag]bool{
				flags.ExperimentalImages: true,
			},
			setupMocks: func(mockStore *mocks.ProjectsStore, mockImageProvider *postgresversionsmocks.ImageProvider) {
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{
					"postgres:17.7",
					"experimental:17.7",
					"xata-analytics:17.7",
				}).Once()
				// All images should be processed
				mockImageProvider.EXPECT().ExtractVersionFromImageName("postgres:17.7").Return("17.7").Once()
				mockImageProvider.EXPECT().GetMajorForVersion("17.7").Return("17").Once()
				mockImageProvider.EXPECT().ExtractVersionFromImageName("experimental:17.7").Return("17.7").Once()
				mockImageProvider.EXPECT().GetMajorForVersion("17.7").Return("17").Once()
				mockImageProvider.EXPECT().ExtractVersionFromImageName("xata-analytics:17.7").Return("17.7").Once()
				mockImageProvider.EXPECT().GetMajorForVersion("17.7").Return("17").Once()
			},
			wantError: false,
			expectedImages: []spec.Image{
				{MajorVersion: "17", FullVersion: "17.7", Name: "postgres:17.7"},
				{MajorVersion: "17", FullVersion: "17.7", Name: "experimental:17.7"},
				{MajorVersion: "17", FullVersion: "17.7", Name: "xata-analytics:17.7"},
			},
		},
		{
			name:           "filters out analytics images when flag is disabled",
			organizationID: apitest.TestOrganization,
			params:         spec.ListImagesParams{},
			featureFlags:   nil, // AnalyticsImages flag defaults to false
			setupMocks: func(mockStore *mocks.ProjectsStore, mockImageProvider *postgresversionsmocks.ImageProvider) {
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{
					"analytics:17.7",
					"postgres:17.7",
				}).Once()
				// Only non-analytics images should be processed
				mockImageProvider.EXPECT().ExtractVersionFromImageName("postgres:17.7").Return("17.7").Once()
				mockImageProvider.EXPECT().GetMajorForVersion("17.7").Return("17").Once()
			},
			wantError: false,
			expectedImages: []spec.Image{
				{MajorVersion: "17", FullVersion: "17.7", Name: "postgres:17.7"},
			},
		},
		{
			name:           "includes analytics images when flag is enabled",
			organizationID: apitest.TestOrganization,
			params:         spec.ListImagesParams{},
			featureFlags: map[openfeature.FeatureFlag]bool{
				flags.AnalyticsImages: true,
			},
			setupMocks: func(mockStore *mocks.ProjectsStore, mockImageProvider *postgresversionsmocks.ImageProvider) {
				mockImageProvider.EXPECT().GetAllImageNames().Return([]string{
					"analytics:17.7",
					"postgres:17.7",
				}).Once()
				// All images should be processed
				mockImageProvider.EXPECT().ExtractVersionFromImageName("analytics:17.7").Return("17.7").Once()
				mockImageProvider.EXPECT().GetMajorForVersion("17.7").Return("17").Once()
				mockImageProvider.EXPECT().ExtractVersionFromImageName("postgres:17.7").Return("17.7").Once()
				mockImageProvider.EXPECT().GetMajorForVersion("17.7").Return("17").Once()
			},
			wantError: false,
			expectedImages: []spec.Image{
				{MajorVersion: "17", FullVersion: "17.7", Name: "analytics:17.7"},
				{MajorVersion: "17", FullVersion: "17.7", Name: "postgres:17.7"},
			},
		},
	}

	for _, tt := range listImagesTests {
		t.Run(tt.name, func(t *testing.T) {
			// Create fresh mocks for each test case
			mockStore := mocks.NewProjectsStore(t)
			mockClusters := protomocks.NewClustersServiceClient(t)
			mockCells := cellsmock.NewCellsMock(t, mockClusters)
			mockPostgresConfig := postgrescfgmocks.NewPostgresConfigProvider(t)
			mockImageProvider := postgresversionsmocks.NewImageProvider(t)

			feat := openfeaturetest.NewClient(tt.featureFlags)
			sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
			handler := NewAPIHandler(feat, mockStore, mockCells, "testdomain:5432", createNewSigNozClient(t), sched, analyticsmocks.NewClient(t), mockPostgresConfig, mockImageProvider)
			e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

			// Pass mocks to setupMocks function
			tt.setupMocks(mockStore, mockImageProvider)

			c, rec := e.GET("/organizations/" + apitest.TestOrganization + "/images").Context()
			err := handler.ListImages(c, tt.organizationID, tt.params)

			if !tt.wantError {
				require.Nil(t, err, "expected no error, got %v", err)
				rec.MustCode(http.StatusOK)

				var response struct {
					Images []spec.Image `json:"images"`
				}
				rec.ReadBody(&response)

				assert.Equal(t, tt.expectedImages, response.Images)
			} else {
				assert.Error(t, err)
				if tt.expectedError != nil {
					assert.Equal(t, tt.expectedError, err)
					// For database connection error case, check error message content
				} else if strings.Contains(tt.name, "database connection") {
					assert.Contains(t, err.Error(), "database connection error")
				}

			}

			mockStore.AssertExpectations(t)
			mockImageProvider.AssertExpectations(t)
		})
	}
}

func TestListExtensions(t *testing.T) {
	listExtensionsTests := []struct {
		name               string
		organizationID     string
		params             spec.ListExtensionsParams
		setupMocks         func(mockStore *mocks.ProjectsStore, mockImageProvider *postgresversionsmocks.ImageProvider)
		wantError          bool
		expectedError      error
		validateExtensions func(t *testing.T, extensions []spec.Extension)
	}{
		{
			name:           "list extensions successfully for analytics:17",
			organizationID: apitest.TestOrganization,
			params: spec.ListExtensionsParams{
				Image:  "analytics:17.7",
				Region: new("us-east-1"),
			},
			setupMocks: func(mockStore *mocks.ProjectsStore, mockImageProvider *postgresversionsmocks.ImageProvider) {
				regions := []store.Region{
					{ID: "us-east-1", OrganizationID: new("org-id"), PublicAccess: false},
					{ID: "us-west-2", PublicAccess: true},
				}
				mockStore.EXPECT().ListRegions(mock.Anything, apitest.TestOrganization).Return(regions, nil).Once()
				mockImageProvider.EXPECT().ValidateImage("analytics:17.7").Return(nil).Once()
			},
			wantError: false,
			validateExtensions: func(t *testing.T, extensions []spec.Extension) {
				assert.NotEmpty(t, extensions)
				// Check that pg_duckdb is in the list (unique to analytics)
				var foundDuckDB bool
				for _, ext := range extensions {
					if ext.Name == "pg_duckdb" {
						foundDuckDB = true
						assert.True(t, ext.PreloadRequired)
						break
					}
				}
				assert.True(t, foundDuckDB, "pg_duckdb should be in analytics extensions")
			},
		},
		{
			name:           "list extensions without region parameter works",
			organizationID: apitest.TestOrganization,
			params: spec.ListExtensionsParams{
				Image: "analytics:17.5",
			},
			setupMocks: func(mockStore *mocks.ProjectsStore, mockImageProvider *postgresversionsmocks.ImageProvider) {
				mockImageProvider.EXPECT().ValidateImage("analytics:17.5").Return(nil).Once()
			},
			wantError: false,
			validateExtensions: func(t *testing.T, extensions []spec.Extension) {
				assert.NotEmpty(t, extensions)
			},
		},
		{
			name:           "list extensions with invalid region fails",
			organizationID: apitest.TestOrganization,
			params: spec.ListExtensionsParams{
				Image:  "analytics:17.7",
				Region: new("invalid-region"),
			},
			setupMocks: func(mockStore *mocks.ProjectsStore, mockImageProvider *postgresversionsmocks.ImageProvider) {
				regions := []store.Region{
					{ID: "us-east-1", OrganizationID: new("org-id"), PublicAccess: false},
					{ID: "us-west-2", PublicAccess: true},
				}
				mockStore.EXPECT().ListRegions(mock.Anything, apitest.TestOrganization).Return(regions, nil).Once()
			},
			wantError:     true,
			expectedError: ErrorInvalidParam{Param: "region", Message: "invalid region: region invalid-region is not found"},
		},
		{
			name:           "list extensions with invalid image format fails",
			organizationID: apitest.TestOrganization,
			params: spec.ListExtensionsParams{
				Image: "invalid-image-format",
			},
			setupMocks: func(mockStore *mocks.ProjectsStore, mockImageProvider *postgresversionsmocks.ImageProvider) {
				mockImageProvider.EXPECT().ValidateImage("invalid-image-format").Return(fmt.Errorf("invalid image format, expected 'offering:version'")).Once()
			},
			wantError:     true,
			expectedError: ErrorInvalidParam{Param: "image", Message: "invalid image format, expected 'offering:version'"},
		},
		{
			name:           "list extensions for non-existent offering fails",
			organizationID: apitest.TestOrganization,
			params: spec.ListExtensionsParams{
				Image: "nonexistent:17.7",
			},
			setupMocks: func(mockStore *mocks.ProjectsStore, mockImageProvider *postgresversionsmocks.ImageProvider) {
				mockImageProvider.EXPECT().ValidateImage("nonexistent:17.7").Return(nil).Once()
			},
			wantError:     true,
			expectedError: ErrorInvalidParam{Param: "image", Message: "no extensions found for image"},
		},
		{
			name:           "internal extensions are filtered from response",
			organizationID: apitest.TestOrganization,
			params: spec.ListExtensionsParams{
				Image: "analytics:17.7",
			},
			setupMocks: func(mockStore *mocks.ProjectsStore, mockImageProvider *postgresversionsmocks.ImageProvider) {
				mockImageProvider.EXPECT().ValidateImage("analytics:17.7").Return(nil).Once()
			},
			wantError: false,
			validateExtensions: func(t *testing.T, extensions []spec.Extension) {
				assert.NotEmpty(t, extensions)
				for _, ext := range extensions {
					for _, internal := range internalExtensions {
						assert.NotEqual(t, internal, ext.Name, "%s should not be exposed in the API", internal)
					}
				}
			},
		},
	}

	for _, tt := range listExtensionsTests {
		t.Run(tt.name, func(t *testing.T) {
			mockStore := mocks.NewProjectsStore(t)
			mockClusters := protomocks.NewClustersServiceClient(t)
			mockCells := cellsmock.NewCellsMock(t, mockClusters)
			mockPostgresConfig := postgrescfgmocks.NewPostgresConfigProvider(t)
			mockImageProvider := postgresversionsmocks.NewImageProvider(t)

			feat := openfeaturetest.NewClient(nil)
			sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
			handler := NewAPIHandler(feat, mockStore, mockCells, "testdomain:5432", createNewSigNozClient(t), sched, analyticsmocks.NewClient(t), mockPostgresConfig, mockImageProvider)
			e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

			tt.setupMocks(mockStore, mockImageProvider)

			url := "/organizations/" + apitest.TestOrganization + "/extensions?image=" + tt.params.Image
			if tt.params.Region != nil {
				url += "&region=" + *tt.params.Region
			}
			c, rec := e.GET(url).Context()
			err := handler.ListExtensions(c, tt.organizationID, tt.params)

			if !tt.wantError {
				require.Nil(t, err, "expected no error, got %v", err)
				rec.MustCode(http.StatusOK)

				var response struct {
					Extensions []spec.Extension `json:"extensions"`
				}
				rec.ReadBody(&response)

				if tt.validateExtensions != nil {
					tt.validateExtensions(t, response.Extensions)
				}
			} else {
				assert.Error(t, err)
				if tt.expectedError != nil {
					assert.Equal(t, tt.expectedError, err)
				}
			}

			mockStore.AssertExpectations(t)
			mockImageProvider.AssertExpectations(t)
		})
	}
}

func TestListRegionsWithBackupsEnabled(t *testing.T) {
	mockStore := mocks.NewProjectsStore(t)
	feat := openfeaturetest.NewClient(nil)
	sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
	handler := NewAPIHandler(feat, mockStore, nil, "", createNewSigNozClient(t), sched, analyticsmocks.NewClient(t), nil, nil)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

	// Test list regions with backupsEnabled field
	regions := []store.Region{
		{ID: "us-east-1", OrganizationID: new("org-id"), PublicAccess: false, BackupsEnabled: true},
		{ID: "us-west-2", PublicAccess: true, BackupsEnabled: false},
		{ID: "eu-central-1", PublicAccess: true, BackupsEnabled: true},
	}
	mockStore.EXPECT().ListRegions(mock.Anything, apitest.TestOrganization).Return(regions, nil)

	c, rec := e.GET("/organizations/" + apitest.TestOrganization + "/regions").Context()
	err := handler.ListRegions(c, apitest.TestOrganization)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)

	var response struct {
		Regions []struct {
			ID             string `json:"id"`
			PublicAccess   bool   `json:"publicAccess"`
			BackupsEnabled bool   `json:"backupsEnabled"`
		} `json:"regions"`
	}

	err = json.Unmarshal(rec.Body.Bytes(), &response)
	assert.Nil(t, err)
	assert.Len(t, response.Regions, 3)

	// Verify each region has the backupsEnabled field
	for i, region := range response.Regions {
		assert.Equal(t, regions[i].ID, region.ID)
		assert.Equal(t, regions[i].PublicAccess, region.PublicAccess)
		assert.Equal(t, regions[i].BackupsEnabled, region.BackupsEnabled)
	}
}

func TestExtractMajorVersionFromImage(t *testing.T) {
	tests := []struct {
		name        string
		imageName   string
		expected    int
		description string
	}{
		// Standard format from versions.yaml
		{
			name:        "standard_postgres_17_5",
			imageName:   "cnpg-postgres-plus:17.5",
			expected:    17,
			description: "Standard PostgreSQL 17.5 image format",
		},
		{
			name:        "standard_postgres_17_6",
			imageName:   "cnpg-postgres-plus:17.6",
			expected:    17,
			description: "Standard PostgreSQL 17.6 image format",
		},
		{
			name:        "standard_postgres_18rc1",
			imageName:   "cnpg-postgres-plus:18rc1",
			expected:    18,
			description: "Standard PostgreSQL 18rc1 image format",
		},

		// Full registry path format
		{
			name:        "full_registry_path_17_5",
			imageName:   "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
			expected:    17,
			description: "Full registry path with PostgreSQL 17.5",
		},
		{
			name:        "full_registry_path_17_6",
			imageName:   "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.6",
			expected:    17,
			description: "Full registry path with PostgreSQL 17.6",
		},
		{
			name:        "full_registry_path_18rc1",
			imageName:   "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:18rc1",
			expected:    18,
			description: "Full registry path with PostgreSQL 18rc1",
		},

		// With date suffix (build timestamp)
		{
			name:        "with_date_suffix_17_5",
			imageName:   "cnpg-postgres-plus:17.5-08092025",
			expected:    17,
			description: "PostgreSQL 17.5 with date suffix",
		},
		{
			name:        "with_date_suffix_17_6",
			imageName:   "cnpg-postgres-plus:17.6-08092025",
			expected:    17,
			description: "PostgreSQL 17.6 with date suffix",
		},
		{
			name:        "with_date_suffix_18rc1",
			imageName:   "cnpg-postgres-plus:18rc1-08092025",
			expected:    18,
			description: "PostgreSQL 18rc1 with date suffix",
		},

		// Full registry path with date suffix
		{
			name:        "full_registry_with_date_17_5",
			imageName:   "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5-08092025",
			expected:    17,
			description: "Full registry path with PostgreSQL 17.5 and date suffix",
		},
		{
			name:        "full_registry_with_date_17_6",
			imageName:   "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.6-08092025",
			expected:    17,
			description: "Full registry path with PostgreSQL 17.6 and date suffix",
		},
		{
			name:        "full_registry_with_date_18rc1",
			imageName:   "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:18rc1-08092025",
			expected:    18,
			description: "Full registry path with PostgreSQL 18rc1 and date suffix",
		},

		// Edge cases
		{
			name:        "no_colon",
			imageName:   "cnpg-postgres-plus",
			expected:    0,
			description: "Image name without colon should return 0",
		},
		{
			name:        "empty_string",
			imageName:   "",
			expected:    0,
			description: "Empty string should return 0",
		},
		{
			name:        "invalid_version_format",
			imageName:   "cnpg-postgres-plus:invalid",
			expected:    0,
			description: "Invalid version format should return 0",
		},
		{
			name:        "non_numeric_major_version",
			imageName:   "cnpg-postgres-plus:abc.5",
			expected:    0,
			description: "Non-numeric major version should return 0",
		},
		{
			name:        "only_major_version",
			imageName:   "cnpg-postgres-plus:17",
			expected:    17,
			description: "Only major version should work",
		},
		{
			name:        "major_version_with_dash",
			imageName:   "cnpg-postgres-plus:17-something",
			expected:    17,
			description: "Major version with dash should work",
		},

		// Multiple dashes (edge case)
		{
			name:        "multiple_dashes",
			imageName:   "cnpg-postgres-plus:17.5-08092025-extra",
			expected:    17,
			description: "Multiple dashes should only consider the first one",
		},

		// Different registry formats
		{
			name:        "docker_hub_format",
			imageName:   "postgres:17.5",
			expected:    17,
			description: "Docker Hub format should work",
		},
		{
			name:        "quay_io_format",
			imageName:   "quay.io/postgres/postgres:17.6",
			expected:    17,
			description: "Quay.io format should work",
		},

		// RC versions
		{
			name:        "rc_version_18",
			imageName:   "cnpg-postgres-plus:18rc1",
			expected:    18,
			description: "RC version should extract major version correctly",
		},
		{
			name:        "rc_version_with_date",
			imageName:   "cnpg-postgres-plus:18rc1-08092025",
			expected:    18,
			description: "RC version with date should extract major version correctly",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := postgresversions.ExtractMajorVersionFromImage(tt.imageName)
			if result != tt.expected {
				t.Errorf("ExtractMajorVersionFromImage(%q) = %d, want %d. %s",
					tt.imageName, result, tt.expected, tt.description)
			}
		})
	}
}

func TestEditingDisabledOrgFails(t *testing.T) {
	feat := openfeaturetest.NewClient(nil)
	sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
	mockAnalytics := analyticsmocks.NewClient(t)
	handler := NewAPIHandler(feat, nil, nil, "", createNewSigNozClient(t), sched, mockAnalytics, nil, nil)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaimsDisabled)

	createBranchTests := []struct {
		name          string
		jsonBody      any
		expectedError error
	}{
		{
			name:          "create a branch on a disabled organization fails",
			jsonBody:      map[string]any{"name": "branch", "mode": "custom", "configuration": map[string]any{"image": "cnpg-postgres-plus:17.5", "replicas": 0, "region": "region-id-1", "instanceType": "xata.micro"}},
			expectedError: ErrorOrganizationDisabled{OrganizationID: apitest.TestOrganizationDisabled},
		},
	}
	for _, tt := range createBranchTests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := e.POST("/organizations/" + apitest.TestOrganization + "/projects/project_id/branches").WithJSONBody(tt.jsonBody).Context()
			err := handler.CreateBranch(c, apitest.TestOrganizationDisabled, "project_id")
			assert.Error(t, err)
			assert.Equal(t, tt.expectedError, err)
		})
	}
}

func TestCreateGithubAppInstallation(t *testing.T) {
	now, _ := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")
	installation := store.GithubInstallation{
		ID:             "inst-1",
		InstallationID: 123,
		Organization:   apitest.TestOrganization,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	storeErr := errors.New("store error")

	feat := openfeaturetest.NewClient(nil)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

	tests := map[string]struct {
		jsonBody             any
		setupMocks           func(*mocks.ProjectsStore)
		wantError            bool
		expectedError        error
		expectedInstallation spec.GithubInstallation
	}{
		"create succeeds": {
			jsonBody: map[string]any{"installationId": 123},
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().CreateGithubInstallation(mock.Anything, apitest.TestOrganization, int64(123)).Return(&installation, nil)
			},
			expectedInstallation: spec.GithubInstallation{
				Id:             installation.ID,
				InstallationId: installation.InstallationID,
				Organization:   installation.Organization,
				CreatedAt:      installation.CreatedAt,
				UpdatedAt:      installation.UpdatedAt,
			},
		},
		"duplicate returns error": {
			jsonBody: map[string]any{"installationId": 456},
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().CreateGithubInstallation(mock.Anything, apitest.TestOrganization, int64(456)).Return(nil, store.ErrGithubInstallationAlreadyExists{Organization: apitest.TestOrganization, InstallationID: 456})
			},
			wantError:     true,
			expectedError: store.ErrGithubInstallationAlreadyExists{Organization: apitest.TestOrganization, InstallationID: 456},
		},
		"store returns generic error": {
			jsonBody: map[string]any{"installationId": 789},
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().CreateGithubInstallation(mock.Anything, apitest.TestOrganization, int64(789)).Return(nil, storeErr)
			},
			wantError:     true,
			expectedError: storeErr,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			mockStore := mocks.NewProjectsStore(t)
			tt.setupMocks(mockStore)
			sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
			handler := NewAPIHandler(feat, mockStore, nil, "", createNewSigNozClient(t), sched, analyticsmocks.NewClient(t), nil, nil)

			c, rec := e.POST("/organizations/" + apitest.TestOrganization + "/githubapp/installations").WithJSONBody(tt.jsonBody).Context()
			err := handler.CreateGithubAppInstallation(c, apitest.TestOrganization)
			if tt.wantError {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				assert.NoError(t, err)
				rec.MustCode(http.StatusCreated)
				var got spec.GithubInstallation
				rec.ReadBody(&got)
				assert.Equal(t, tt.expectedInstallation, got)
			}

			mockStore.AssertExpectations(t)
		})
	}
}

func TestListGithubAppInstallations(t *testing.T) {
	now, _ := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")
	installation := store.GithubInstallation{
		ID:             "inst-1",
		InstallationID: 123,
		Organization:   apitest.TestOrganization,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	storeErr := errors.New("store error")

	feat := openfeaturetest.NewClient(nil)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

	tests := map[string]struct {
		setupMocks            func(*mocks.ProjectsStore)
		wantError             bool
		expectedError         error
		expectedInstallations []spec.GithubInstallation
	}{
		"returns installations": {
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().ListGithubInstallations(mock.Anything, apitest.TestOrganization).Return([]store.GithubInstallation{installation}, nil)
			},
			expectedInstallations: []spec.GithubInstallation{
				{
					Id:             installation.ID,
					InstallationId: installation.InstallationID,
					Organization:   installation.Organization,
					CreatedAt:      installation.CreatedAt,
					UpdatedAt:      installation.UpdatedAt,
				},
			},
		},
		"empty slice returns empty slice": {
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().ListGithubInstallations(mock.Anything, apitest.TestOrganization).Return([]store.GithubInstallation{}, nil)
			},
			expectedInstallations: []spec.GithubInstallation{},
		},
		"store returns generic error": {
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().ListGithubInstallations(mock.Anything, apitest.TestOrganization).Return(nil, storeErr)
			},
			wantError:     true,
			expectedError: storeErr,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			mockStore := mocks.NewProjectsStore(t)
			tt.setupMocks(mockStore)
			sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
			handler := NewAPIHandler(feat, mockStore, nil, "", createNewSigNozClient(t), sched, analyticsmocks.NewClient(t), nil, nil)

			c, rec := e.GET("/organizations/" + apitest.TestOrganization + "/githubapp/installations").Context()
			err := handler.ListGithubAppInstallations(c, apitest.TestOrganization)
			if tt.wantError {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				assert.NoError(t, err)
				rec.MustCode(http.StatusOK)
				var res struct {
					Installations []spec.GithubInstallation `json:"installations"`
				}
				rec.ReadBody(&res)
				assert.Equal(t, tt.expectedInstallations, res.Installations)
			}

			mockStore.AssertExpectations(t)
		})
	}
}

func TestUpdateGithubAppInstallation(t *testing.T) {
	now, _ := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")
	updatedInstallation := store.GithubInstallation{
		ID:             "inst-1",
		InstallationID: 456,
		Organization:   apitest.TestOrganization,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	storeErr := errors.New("store error")

	feat := openfeaturetest.NewClient(nil)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

	tests := map[string]struct {
		githubInstallationID string
		jsonBody             any
		setupMocks           func(*mocks.ProjectsStore)
		wantError            bool
		expectedError        error
		expectedInstallation spec.GithubInstallation
	}{
		"update succeeds": {
			githubInstallationID: "inst-1",
			jsonBody:             map[string]any{"installationId": 456},
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().UpdateGithubInstallation(mock.Anything, apitest.TestOrganization, "inst-1", int64(456)).Return(&updatedInstallation, nil)
			},
			expectedInstallation: spec.GithubInstallation{
				Id:             updatedInstallation.ID,
				InstallationId: updatedInstallation.InstallationID,
				Organization:   updatedInstallation.Organization,
				CreatedAt:      updatedInstallation.CreatedAt,
				UpdatedAt:      updatedInstallation.UpdatedAt,
			},
		},
		"not found returns error": {
			githubInstallationID: "unknown-id",
			jsonBody:             map[string]any{"installationId": 456},
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().UpdateGithubInstallation(mock.Anything, apitest.TestOrganization, "unknown-id", int64(456)).Return(nil, store.ErrGithubInstallationNotFound{Organization: apitest.TestOrganization, ID: "unknown-id"})
			},
			wantError:     true,
			expectedError: store.ErrGithubInstallationNotFound{Organization: apitest.TestOrganization, ID: "unknown-id"},
		},
		"duplicate installation id returns error": {
			githubInstallationID: "inst-1",
			jsonBody:             map[string]any{"installationId": 789},
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().UpdateGithubInstallation(mock.Anything, apitest.TestOrganization, "inst-1", int64(789)).Return(nil, store.ErrGithubInstallationAlreadyExists{Organization: apitest.TestOrganization, InstallationID: 789})
			},
			wantError:     true,
			expectedError: store.ErrGithubInstallationAlreadyExists{Organization: apitest.TestOrganization, InstallationID: 789},
		},
		"store returns generic error": {
			githubInstallationID: "inst-1",
			jsonBody:             map[string]any{"installationId": 999},
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().UpdateGithubInstallation(mock.Anything, apitest.TestOrganization, "inst-1", int64(999)).Return(nil, storeErr)
			},
			wantError:     true,
			expectedError: storeErr,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			mockStore := mocks.NewProjectsStore(t)
			tt.setupMocks(mockStore)
			sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
			handler := NewAPIHandler(feat, mockStore, nil, "", createNewSigNozClient(t), sched, analyticsmocks.NewClient(t), nil, nil)

			c, rec := e.PUT("/organizations/" + apitest.TestOrganization + "/githubapp/installations/" + tt.githubInstallationID).WithJSONBody(tt.jsonBody).Context()
			err := handler.UpdateGithubAppInstallation(c, apitest.TestOrganization, tt.githubInstallationID)
			if tt.wantError {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				assert.NoError(t, err)
				rec.MustCode(http.StatusOK)
				var got spec.GithubInstallation
				rec.ReadBody(&got)
				assert.Equal(t, tt.expectedInstallation, got)
			}

			mockStore.AssertExpectations(t)
		})
	}
}

func TestGetGithubRepository(t *testing.T) {
	now, _ := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")
	mapping := store.GithubRepoMapping{
		ID:                 "map-1",
		GithubRepositoryID: 789,
		Project:            "proj-1",
		RootBranchID:       "branch-1",
		CreatedAt:          now,
		UpdatedAt:          now,
	}

	feat := openfeaturetest.NewClient(nil)

	tests := map[string]struct {
		projectID       string
		setupMocks      func(*mocks.ProjectsStore)
		wantError       bool
		expectedError   error
		expectedMapping spec.GithubRepository
	}{
		"returns mapping": {
			projectID: "proj-1",
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().GetGithubRepoMappingByProject(mock.Anything, apitest.TestOrganization, "proj-1").Return(&mapping, nil)
			},
			expectedMapping: spec.GithubRepository{
				Id:                 mapping.ID,
				GithubRepositoryID: mapping.GithubRepositoryID,
				Project:            mapping.Project,
				RootBranchId:       mapping.RootBranchID,
				CreatedAt:          mapping.CreatedAt,
				UpdatedAt:          mapping.UpdatedAt,
			},
		},
		"mapping not found returns error": {
			projectID: "proj-2",
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().GetGithubRepoMappingByProject(mock.Anything, apitest.TestOrganization, "proj-2").Return(nil, store.ErrGithubRepoMappingNotFound{Organization: apitest.TestOrganization, Project: "proj-2"})
			},
			wantError:     true,
			expectedError: store.ErrGithubRepoMappingNotFound{Organization: apitest.TestOrganization, Project: "proj-2"},
		},
		"project not found returns error": {
			projectID: "proj-unknown",
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().GetGithubRepoMappingByProject(mock.Anything, apitest.TestOrganization, "proj-unknown").Return(nil, store.ErrProjectNotFound{ID: "proj-unknown"})
			},
			wantError:     true,
			expectedError: store.ErrProjectNotFound{ID: "proj-unknown"},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			mockStore := mocks.NewProjectsStore(t)
			tt.setupMocks(mockStore)
			sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
			handler := NewAPIHandler(feat, mockStore, nil, "", createNewSigNozClient(t), sched, analyticsmocks.NewClient(t), nil, nil)

			e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

			c, rec := e.GET("/organizations/" + apitest.TestOrganization + "/projects/" + tt.projectID + "/branches/branch-1/githubapp/repository").Context()
			err := handler.GetGithubRepository(c, apitest.TestOrganization, tt.projectID, "branch-1")
			if tt.wantError {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				assert.NoError(t, err)
				rec.MustCode(http.StatusOK)
				var got struct {
					Mapping spec.GithubRepository `json:"mapping"`
				}
				rec.ReadBody(&got)
				assert.Equal(t, tt.expectedMapping, got.Mapping)
			}

			mockStore.AssertExpectations(t)
		})
	}
}

func TestCreateGithubRepository(t *testing.T) {
	now, _ := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")
	mapping := store.GithubRepoMapping{
		ID:                 "map-1",
		GithubRepositoryID: 789,
		Project:            "proj-1",
		RootBranchID:       "branch-1",
		CreatedAt:          now,
		UpdatedAt:          now,
	}

	feat := openfeaturetest.NewClient(nil)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

	tests := map[string]struct {
		projectID       string
		branchID        string
		jsonBody        any
		setupMocks      func(*mocks.ProjectsStore)
		wantError       bool
		expectedError   error
		expectedMapping spec.GithubRepository
	}{
		"create succeeds": {
			projectID: "proj-1",
			branchID:  "branch-1",
			jsonBody:  map[string]any{"githubRepositoryID": 789},
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().CreateGithubRepoMapping(mock.Anything, apitest.TestOrganization, "proj-1", int64(789), "branch-1").Return(&mapping, nil)
			},
			expectedMapping: spec.GithubRepository{
				Id:                 mapping.ID,
				GithubRepositoryID: mapping.GithubRepositoryID,
				Project:            mapping.Project,
				RootBranchId:       mapping.RootBranchID,
				CreatedAt:          mapping.CreatedAt,
				UpdatedAt:          mapping.UpdatedAt,
			},
		},
		"duplicate returns error": {
			projectID: "proj-dup",
			branchID:  "branch-1",
			jsonBody:  map[string]any{"githubRepositoryID": 789},
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().CreateGithubRepoMapping(mock.Anything, apitest.TestOrganization, "proj-dup", int64(789), "branch-1").Return(nil, store.ErrGithubRepoMappingAlreadyExists{Organization: apitest.TestOrganization, Project: "proj-dup"})
			},
			wantError:     true,
			expectedError: store.ErrGithubRepoMappingAlreadyExists{Organization: apitest.TestOrganization, Project: "proj-dup"},
		},
		"project not found returns error": {
			projectID: "proj-unknown",
			branchID:  "branch-1",
			jsonBody:  map[string]any{"githubRepositoryID": 789},
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().CreateGithubRepoMapping(mock.Anything, apitest.TestOrganization, "proj-unknown", int64(789), "branch-1").Return(nil, &store.ErrProjectNotFound{ID: "proj-unknown"})
			},
			wantError:     true,
			expectedError: &store.ErrProjectNotFound{ID: "proj-unknown"},
		},
		"branch not found returns error": {
			projectID: "proj-1",
			branchID:  "bad-branch",
			jsonBody:  map[string]any{"githubRepositoryID": 789},
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().CreateGithubRepoMapping(mock.Anything, apitest.TestOrganization, "proj-1", int64(789), "bad-branch").Return(nil, store.ErrBranchNotFound{ID: "bad-branch"})
			},
			wantError:     true,
			expectedError: store.ErrBranchNotFound{ID: "bad-branch"},
		},
		"store returns generic error": {
			projectID: "proj-1",
			branchID:  "branch-1",
			jsonBody:  map[string]any{"githubRepositoryID": 789},
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().CreateGithubRepoMapping(mock.Anything, apitest.TestOrganization, "proj-1", int64(789), "branch-1").Return(nil, errors.New("unexpected"))
			},
			wantError:     true,
			expectedError: errors.New("unexpected"),
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			mockStore := mocks.NewProjectsStore(t)
			tt.setupMocks(mockStore)
			sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
			handler := NewAPIHandler(feat, mockStore, nil, "", createNewSigNozClient(t), sched, analyticsmocks.NewClient(t), nil, nil)

			c, rec := e.POST("/organizations/" + apitest.TestOrganization + "/projects/" + tt.projectID + "/branches/" + tt.branchID + "/githubapp/repository").WithJSONBody(tt.jsonBody).Context()
			err := handler.CreateGithubRepository(c, apitest.TestOrganization, tt.projectID, tt.branchID)
			if tt.wantError {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				assert.NoError(t, err)
				rec.MustCode(http.StatusCreated)
				var got spec.GithubRepository
				rec.ReadBody(&got)
				assert.Equal(t, tt.expectedMapping, got)
			}

			mockStore.AssertExpectations(t)
		})
	}
}

func TestUpdateGithubRepository(t *testing.T) {
	now, _ := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")
	updated := store.GithubRepoMapping{
		ID:                 "map-1",
		GithubRepositoryID: 999,
		Project:            "proj-1",
		RootBranchID:       "branch-2",
		CreatedAt:          now,
		UpdatedAt:          now,
	}

	feat := openfeaturetest.NewClient(nil)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

	tests := map[string]struct {
		projectID       string
		branchID        string
		jsonBody        any
		setupMocks      func(*mocks.ProjectsStore)
		wantError       bool
		expectedError   error
		expectedMapping spec.GithubRepository
	}{
		"update succeeds": {
			projectID: "proj-1",
			branchID:  "branch-2",
			jsonBody:  map[string]any{"githubRepositoryID": 999},
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().UpdateGithubRepoMapping(mock.Anything, apitest.TestOrganization, "proj-1", int64(999), "branch-2").Return(&updated, nil)
			},
			expectedMapping: spec.GithubRepository{
				Id:                 updated.ID,
				GithubRepositoryID: updated.GithubRepositoryID,
				Project:            updated.Project,
				RootBranchId:       updated.RootBranchID,
				CreatedAt:          updated.CreatedAt,
				UpdatedAt:          updated.UpdatedAt,
			},
		},
		"mapping not found returns error": {
			projectID: "proj-missing",
			branchID:  "branch-2",
			jsonBody:  map[string]any{"githubRepositoryID": 999},
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().UpdateGithubRepoMapping(mock.Anything, apitest.TestOrganization, "proj-missing", int64(999), "branch-2").Return(nil, store.ErrGithubRepoMappingNotFound{Organization: apitest.TestOrganization, Project: "proj-missing"})
			},
			wantError:     true,
			expectedError: store.ErrGithubRepoMappingNotFound{Organization: apitest.TestOrganization, Project: "proj-missing"},
		},
		"branch not found returns error": {
			projectID: "proj-1",
			branchID:  "bad-branch",
			jsonBody:  map[string]any{"githubRepositoryID": 999},
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().UpdateGithubRepoMapping(mock.Anything, apitest.TestOrganization, "proj-1", int64(999), "bad-branch").Return(nil, store.ErrBranchNotFound{ID: "bad-branch"})
			},
			wantError:     true,
			expectedError: store.ErrBranchNotFound{ID: "bad-branch"},
		},
		"store returns generic error": {
			projectID: "proj-1",
			branchID:  "branch-2",
			jsonBody:  map[string]any{"githubRepositoryID": 999},
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().UpdateGithubRepoMapping(mock.Anything, apitest.TestOrganization, "proj-1", int64(999), "branch-2").Return(nil, errors.New("unexpected"))
			},
			wantError:     true,
			expectedError: errors.New("unexpected"),
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			mockStore := mocks.NewProjectsStore(t)
			tt.setupMocks(mockStore)
			sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
			handler := NewAPIHandler(feat, mockStore, nil, "", createNewSigNozClient(t), sched, analyticsmocks.NewClient(t), nil, nil)

			c, rec := e.PUT("/organizations/" + apitest.TestOrganization + "/projects/" + tt.projectID + "/branches/" + tt.branchID + "/githubapp/repository").WithJSONBody(tt.jsonBody).Context()
			err := handler.UpdateGithubRepository(c, apitest.TestOrganization, tt.projectID, tt.branchID)
			if tt.wantError {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				assert.NoError(t, err)
				rec.MustCode(http.StatusOK)
				var got spec.GithubRepository
				rec.ReadBody(&got)
				assert.Equal(t, tt.expectedMapping, got)
			}

			mockStore.AssertExpectations(t)
		})
	}
}

func TestDeleteGithubRepository(t *testing.T) {
	feat := openfeaturetest.NewClient(nil)
	e := apitest.New(t).WithOpenAPISpec(projectsSpec).WithClaims(apitest.TestClaims)

	tests := map[string]struct {
		projectID     string
		branchID      string
		setupMocks    func(*mocks.ProjectsStore)
		wantError     bool
		expectedError error
	}{
		"delete succeeds": {
			projectID: "proj-1",
			branchID:  "branch-1",
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().DeleteGithubRepoMapping(mock.Anything, apitest.TestOrganization, "proj-1").Return(nil)
			},
		},
		"mapping not found returns error": {
			projectID: "proj-missing",
			branchID:  "branch-1",
			setupMocks: func(mockStore *mocks.ProjectsStore) {
				mockStore.EXPECT().DeleteGithubRepoMapping(mock.Anything, apitest.TestOrganization, "proj-missing").Return(store.ErrGithubRepoMappingNotFound{Organization: apitest.TestOrganization, Project: "proj-missing"})
			},
			wantError:     true,
			expectedError: store.ErrGithubRepoMappingNotFound{Organization: apitest.TestOrganization, Project: "proj-missing"},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			mockStore := mocks.NewProjectsStore(t)
			tt.setupMocks(mockStore)
			sched := &scheduler.Scheduler{DefaultStrategy: &strategy.AlwaysPrimary{}}
			handler := NewAPIHandler(feat, mockStore, nil, "", createNewSigNozClient(t), sched, analyticsmocks.NewClient(t), nil, nil)

			c, rec := e.DELETE("/organizations/" + apitest.TestOrganization + "/projects/" + tt.projectID + "/branches/" + tt.branchID + "/githubapp/repository").Context()
			err := handler.DeleteGithubRepository(c, apitest.TestOrganization, tt.projectID, tt.branchID)
			if tt.wantError {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				assert.NoError(t, err)
				rec.MustCode(http.StatusNoContent)
			}

			mockStore.AssertExpectations(t)
		})
	}
}
