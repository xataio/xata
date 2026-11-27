package rpc

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"testing"

	projectsv1 "xata/gen/proto/projects/v1"
	"xata/services/projects/branch"
	"xata/services/projects/store"
	"xata/services/projects/store/mocks"

	ofsdk "github.com/open-feature/go-sdk/openfeature"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWithFlagContext(t *testing.T) {
	// Guards the fix for flags being evaluated with an empty distinct id (which the
	// PostHog provider rejects, making every flag fall back to its default): the org
	// is set as both the targeting key and the organization group.
	ec := ofsdk.TransactionContext(withFlagContext(context.Background(), "org-1"))
	require.Equal(t, "org-1", ec.TargetingKey())
	require.Equal(t, map[string]string{"organization": "org-1"}, ec.Attributes()["groups"])
}

func TestBranchModeFromProto(t *testing.T) {
	t.Run("from parent", func(t *testing.T) {
		mode, err := branchModeFromProto(&projectsv1.CreateBranchRequest{
			Mode: &projectsv1.CreateBranchRequest_FromParentBranchId{FromParentBranchId: "br_parent"},
		})
		require.NoError(t, err)
		fp, ok := mode.(branch.FromParent)
		require.True(t, ok)
		require.Equal(t, "br_parent", fp.ParentID)
	})

	t.Run("from configuration", func(t *testing.T) {
		mode, err := branchModeFromProto(&projectsv1.CreateBranchRequest{
			Mode: &projectsv1.CreateBranchRequest_FromConfiguration{FromConfiguration: &projectsv1.BranchConfiguration{
				Image:        "postgres:17.11",
				Region:       "us-east-1",
				InstanceType: "xata.micro",
			}},
		})
		require.NoError(t, err)
		fc, ok := mode.(branch.FromConfiguration)
		require.True(t, ok)
		require.Equal(t, "postgres:17.11", fc.Config.Image)
		require.Equal(t, "us-east-1", fc.Config.Region)
		require.Equal(t, "xata.micro", fc.Config.InstanceType)
	})

	t.Run("missing mode is invalid argument", func(t *testing.T) {
		_, err := branchModeFromProto(&projectsv1.CreateBranchRequest{})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})
}

func TestScaleToZeroFromProto(t *testing.T) {
	require.Nil(t, scaleToZeroFromProto(nil))

	got := scaleToZeroFromProto(&projectsv1.BranchScaleToZero{Enabled: true, InactivityPeriodMinutes: 5})
	require.Equal(t, &branch.ScaleToZero{Enabled: true, InactivityPeriodMinutes: 5}, got)
}

func TestCreateProject(t *testing.T) {
	mockStore := mocks.NewProjectsStore(t)
	mockStore.EXPECT().CreateProject(mock.Anything, "org-1", mock.MatchedBy(func(c *store.CreateProjectConfiguration) bool {
		return c.Name == "my-project" && c.UsageTier == "t2"
	})).Return(&store.Project{ID: "prj_1"}, nil).Once()

	svc := NewProjectsService(mockStore, nil)
	resp, err := svc.CreateProject(context.Background(), &projectsv1.CreateProjectRequest{
		OrganizationId: "org-1",
		Name:           "my-project",
		UsageTier:      "t2",
	})
	require.NoError(t, err)
	require.Equal(t, "prj_1", resp.GetProjectId())
}

func TestDeleteProject(t *testing.T) {
	t.Run("without branches is unimplemented", func(t *testing.T) {
		svc := NewProjectsService(mocks.NewProjectsStore(t), nil)
		_, err := svc.DeleteProject(context.Background(), &projectsv1.DeleteProjectRequest{ProjectId: "prj_1"})
		require.Equal(t, codes.Unimplemented, status.Code(err))
	})

	t.Run("acquires the project lock before cascading; a failed acquire aborts", func(t *testing.T) {
		// Guards that teardown holds the per-project lock (so it can't miss an
		// in-flight branch create): the lock is acquired first, and if it can't be
		// taken the cascade never runs. branch.Service is never reached here.
		mockStore := mocks.NewProjectsStore(t)
		mockStore.EXPECT().AcquireProjectLock(mock.Anything, "prj_1").
			Return(nil, errors.New("lock unavailable")).Once()
		svc := NewProjectsService(mockStore, nil)
		svc.SetBranches(&branch.Service{})
		_, err := svc.DeleteProject(context.Background(), &projectsv1.DeleteProjectRequest{
			OrganizationId: "org-1",
			ProjectId:      "prj_1",
		})
		require.Error(t, err)
	})
}

func httpStatusDetail(t *testing.T, st *status.Status) (int, bool) {
	t.Helper()
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			n, err := strconv.Atoi(info.GetMetadata()["httpStatus"])
			require.NoError(t, err)
			return n, true
		}
	}
	return 0, false
}

func TestProvisioningError(t *testing.T) {
	t.Run("invalid param carries its 400 status and the offending field", func(t *testing.T) {
		err := provisioningError("create branch", branch.ErrInvalidParam{Param: "region", Message: `unknown region "xx"`})

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())

		httpStatus, ok := httpStatusDetail(t, st)
		require.True(t, ok)
		require.Equal(t, http.StatusBadRequest, httpStatus)

		var br *errdetails.BadRequest
		for _, d := range st.Details() {
			if b, ok := d.(*errdetails.BadRequest); ok {
				br = b
			}
		}
		require.NotNil(t, br, "expected a BadRequest detail")
		require.Len(t, br.GetFieldViolations(), 1)
		require.Equal(t, "region", br.GetFieldViolations()[0].GetField())
		require.Equal(t, `unknown region "xx"`, br.GetFieldViolations()[0].GetDescription())
	})

	t.Run("an image the cell does not offer is a 400 on the image field", func(t *testing.T) {
		err := provisioningError("provision branch", branch.ErrInvalidParam{BranchName: "main", Param: "image", Message: "Postgres 17 is not available in this cell"})

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())

		httpStatus, ok := httpStatusDetail(t, st)
		require.True(t, ok)
		require.Equal(t, http.StatusBadRequest, httpStatus)

		var br *errdetails.BadRequest
		for _, d := range st.Details() {
			if b, ok := d.(*errdetails.BadRequest); ok {
				br = b
			}
		}
		require.NotNil(t, br, "expected a BadRequest detail")
		require.Equal(t, "image", br.GetFieldViolations()[0].GetField())
		require.Equal(t, "Postgres 17 is not available in this cell", br.GetFieldViolations()[0].GetDescription())
	})

	t.Run("a coded error carries its status without a field violation", func(t *testing.T) {
		err := provisioningError("create branch", store.ErrProjectBusy{ProjectID: "prj_1"})

		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.ResourceExhausted, st.Code())

		httpStatus, ok := httpStatusDetail(t, st)
		require.True(t, ok)
		require.Equal(t, http.StatusTooManyRequests, httpStatus)

		for _, d := range st.Details() {
			_, isBadRequest := d.(*errdetails.BadRequest)
			require.False(t, isBadRequest, "non-param errors carry no field violation")
		}
	})

	t.Run("an error with no HTTP status is wrapped, not a clean status", func(t *testing.T) {
		err := provisioningError("create branch", errors.New("boom"))
		st, _ := status.FromError(err)
		require.Equal(t, codes.Unknown, st.Code())
		_, ok := httpStatusDetail(t, st)
		require.False(t, ok)
	})
}
