package branch

import (
	"context"
	"errors"
	"testing"

	"xata/internal/flags"
	"xata/internal/openfeature"
	openfeaturetest "xata/internal/openfeature/client/mocks"
	"xata/services/projects/cells/cellsmock"
	"xata/services/projects/provisioner"
	provisionermocks "xata/services/projects/provisioner/mocks"
	"xata/services/projects/store"
	storemocks "xata/services/projects/store/mocks"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	testOrg     = "org-1"
	testProject = "project-1"
	testBranch  = "branch-1"
	testParent  = "parent-1"
)

// TestBuildPayloadRequiresName covers the basic invariant enforced for every
// caller of the shared entry point, independent of mode.
func TestBuildPayloadRequiresName(t *testing.T) {
	s := New(storemocks.NewProjectsStore(t), nil, openfeaturetest.NewClient(nil), nil, "", nil, nil, nil)
	_, err := s.BuildPayload(context.Background(), CreateInput{
		OrganizationID: testOrg,
		ProjectID:      testProject,
		Name:           "",
		Mode:           FromParent{ParentID: testParent},
	})
	require.Equal(t, ErrInvalidParam{Param: "name", Message: "branch name is required"}, err)
}

// TestGenerateCronMalformed guards against the panic the missing validation used
// to allow: GenerateCron splits on ":" and indexes three parts, so a value with
// fewer segments must not panic (it returns the default).
func TestGenerateCronMalformed(t *testing.T) {
	for _, in := range []string{"", "nope", "1:30", "a:b:c:d"} {
		require.NotPanics(t, func() { _ = GenerateCron(in) })
	}
	require.Equal(t, defaultBackupSchedule, GenerateCron("no-colons"))
}

// unknownMode is a Mode the switch does not know about, to exercise the default
// arm of BuildPayload.
type unknownMode struct{}

func (unknownMode) isMode() {}

func TestBuildPayload(t *testing.T) {
	tests := map[string]struct {
		mode    Mode
		backup  *BackupConfiguration
		flags   map[openfeature.FeatureFlag]bool
		setup   func(st *storemocks.ProjectsStore)
		wantErr error  // exact error to match, when set
		wantMsg string // substring to match, when wantErr is nil
		check   func(t *testing.T, p *provisioner.ClusterServicePayload)
	}{
		"child branch creation disabled": {
			mode:    FromParent{ParentID: testParent},
			flags:   map[openfeature.FeatureFlag]bool{flags.ChildBranchCreationDisabled: true},
			wantErr: ErrChildBranchCreationDisabled{},
		},
		"parent id is required": {
			mode:    FromParent{ParentID: ""},
			wantErr: ErrInvalidParam{BranchName: testBranch, Param: "parentID", Message: "parentId is required for 'inherit' mode"},
		},
		"parent branch not found": {
			mode: FromParent{ParentID: testParent},
			setup: func(st *storemocks.ProjectsStore) {
				st.EXPECT().DescribeBranch(mock.Anything, testOrg, testProject, testParent).
					Return(nil, store.ErrBranchNotFound{ID: testParent}).Once()
			},
			wantErr: ErrBranchNotFound{BranchID: testParent},
		},
		"parent branch inherits cell, region and backups": {
			mode: FromParent{ParentID: testParent},
			setup: func(st *storemocks.ProjectsStore) {
				st.EXPECT().DescribeBranch(mock.Anything, testOrg, testProject, testParent).
					Return(&store.Branch{ID: testParent, CellID: "cell-1", Region: "region-1", BackupsEnabled: true}, nil).Once()
			},
			check: func(t *testing.T, p *provisioner.ClusterServicePayload) {
				require.Equal(t, "cell-1", p.CellID)
				require.Equal(t, "region-1", p.Region)
				require.True(t, p.BackupsEnabled)
				require.NotNil(t, p.ParentID)
				require.Equal(t, testParent, *p.ParentID)
			},
		},
		"custom mode requires a configuration": {
			mode:    FromConfiguration{Config: Configuration{}},
			wantErr: ErrInvalidParam{BranchName: testBranch, Param: "configuration", Message: "configuration is required for 'custom' mode"},
		},
		"malformed backup time is rejected before any lookup": {
			mode:    FromParent{ParentID: testParent},
			backup:  &BackupConfiguration{BackupTime: new("not-a-time")},
			wantErr: ErrInvalidParam{BranchName: testBranch, Param: "backup time", Message: "invalid backup time format 'not-a-time', must match format 'D:HH:MM' where D= * or 0-6, HH=00-23, MM=00-59"},
		},
		"out-of-range retention is rejected before any lookup": {
			mode:    FromParent{ParentID: testParent},
			backup:  &BackupConfiguration{RetentionPeriod: new(int32(100))},
			wantErr: ErrInvalidParam{BranchName: testBranch, Param: "backup retentionPeriod", Message: "must be at least 2 days and maximum 35 days"},
		},
		"unknown mode is rejected": {
			mode:    unknownMode{},
			wantMsg: "unsupported branch creation mode",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			st := storemocks.NewProjectsStore(t)
			if tc.setup != nil {
				tc.setup(st)
			}
			s := New(st, nil, openfeaturetest.NewClient(tc.flags), nil, "", nil, nil, nil)

			payload, err := s.BuildPayload(context.Background(), CreateInput{
				OrganizationID: testOrg,
				ProjectID:      testProject,
				Name:           testBranch,
				Mode:           tc.mode,
				Backup:         tc.backup,
			})

			switch {
			case tc.wantErr != nil:
				require.Equal(t, tc.wantErr, err)
			case tc.wantMsg != "":
				require.ErrorContains(t, err, tc.wantMsg)
			default:
				require.NoError(t, err)
				require.NotNil(t, payload)
				tc.check(t, payload)
			}
		})
	}
}

func TestProvision(t *testing.T) {
	tests := map[string]struct {
		payload  *provisioner.ClusterServicePayload
		in       CreateInput
		setup    func(st *storemocks.ProjectsStore, prov *provisionermocks.Provisioner, cl *cellsmock.Cells)
		wantErr  error // exact domain error to match, when set
		wantBusy bool  // expect an error wrapping store.ErrProjectBusy
		wantOK   bool  // expect success (branch returned)
	}{
		"backup config rejected when backups are disabled": {
			payload: &provisioner.ClusterServicePayload{BackupsEnabled: false},
			in:      CreateInput{Name: testBranch, Backup: &BackupConfiguration{}},
			wantErr: ErrInvalidParam{BranchName: testBranch, Param: "backupConfiguration", Message: "backup configuration cannot be specified when backups are disabled in the selected region"},
		},
		"lock contention wraps ErrProjectBusy": {
			payload: &provisioner.ClusterServicePayload{BackupsEnabled: true},
			in:      CreateInput{OrganizationID: testOrg, ProjectID: testProject, Name: testBranch},
			setup: func(_ *storemocks.ProjectsStore, prov *provisionermocks.Provisioner, _ *cellsmock.Cells) {
				prov.EXPECT().CreateBranch(mock.Anything, testProject, testOrg, testBranch, mock.MatchedBy(func(p *provisioner.ClusterServicePayload) bool {
					return !p.ProjectLockHeld
				})).Return(nil, store.ErrProjectBusy{ProjectID: testProject}).Once()
			},
			wantBusy: true,
		},
		"provisioner branch-not-found maps to domain error": {
			payload: &provisioner.ClusterServicePayload{BackupsEnabled: true},
			in:      CreateInput{OrganizationID: testOrg, ProjectID: testProject, Name: testBranch},
			setup: func(st *storemocks.ProjectsStore, prov *provisionermocks.Provisioner, _ *cellsmock.Cells) {
				prov.EXPECT().CreateBranch(mock.Anything, testProject, testOrg, testBranch, mock.Anything).
					Return(nil, provisioner.ErrBranchNotFound{BranchID: testParent}).Once()
			},
			wantErr: ErrBranchNotFound{BranchID: testParent},
		},
		"provisioner invalid-configuration maps to invalid param": {
			payload: &provisioner.ClusterServicePayload{BackupsEnabled: true},
			in:      CreateInput{OrganizationID: testOrg, ProjectID: testProject, Name: testBranch},
			setup: func(st *storemocks.ProjectsStore, prov *provisionermocks.Provisioner, _ *cellsmock.Cells) {
				prov.EXPECT().CreateBranch(mock.Anything, testProject, testOrg, testBranch, mock.Anything).
					Return(nil, provisioner.ErrInvalidConfiguration{Name: testBranch, Message: "bad config"}).Once()
			},
			wantErr: ErrInvalidParam{BranchName: testBranch, Param: "configuration", Message: "bad config"},
		},
		"provisioner parent-unhealthy maps to domain error": {
			payload: &provisioner.ClusterServicePayload{BackupsEnabled: true},
			in:      CreateInput{OrganizationID: testOrg, ProjectID: testProject, Name: testBranch},
			setup: func(st *storemocks.ProjectsStore, prov *provisionermocks.Provisioner, _ *cellsmock.Cells) {
				prov.EXPECT().CreateBranch(mock.Anything, testProject, testOrg, testBranch, mock.Anything).
					Return(nil, provisioner.ErrParentBranchUnhealthy{ParentID: testParent}).Once()
			},
			wantErr: ErrParentBranchUnhealthy{ParentID: testParent},
		},
		"success returns the branch; connection string is best effort": {
			payload: &provisioner.ClusterServicePayload{BackupsEnabled: true},
			in:      CreateInput{OrganizationID: testOrg, ProjectID: testProject, Name: testBranch},
			setup: func(st *storemocks.ProjectsStore, prov *provisionermocks.Provisioner, cl *cellsmock.Cells) {
				prov.EXPECT().CreateBranch(mock.Anything, testProject, testOrg, testBranch, mock.Anything).
					Return(&store.Branch{ID: testBranch, CellID: "cell-1"}, nil).Once()
				// The connection string is fetched best-effort; a failure here is
				// swallowed and the branch is still returned.
				cl.EXPECT().GetCellConnection(mock.Anything, testOrg, "cell-1").
					Return(nil, errors.New("no connection")).Once()
			},
			wantOK: true,
		},
		"ProjectLockHeld is passed to the provisioner": {
			payload: &provisioner.ClusterServicePayload{BackupsEnabled: true},
			in:      CreateInput{OrganizationID: testOrg, ProjectID: testProject, Name: testBranch, ProjectLockHeld: true},
			setup: func(st *storemocks.ProjectsStore, prov *provisionermocks.Provisioner, cl *cellsmock.Cells) {
				// The caller holds the lock, so the store must be told not to take it again.
				prov.EXPECT().CreateBranch(mock.Anything, testProject, testOrg, testBranch, mock.MatchedBy(func(p *provisioner.ClusterServicePayload) bool {
					return p.ProjectLockHeld
				})).Return(&store.Branch{ID: testBranch, CellID: "cell-1"}, nil).Once()
				cl.EXPECT().GetCellConnection(mock.Anything, testOrg, "cell-1").
					Return(nil, errors.New("no connection")).Once()
			},
			wantOK: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			st := storemocks.NewProjectsStore(t)
			prov := provisionermocks.NewProvisioner(t)
			cl := cellsmock.NewCells(t)
			if tc.setup != nil {
				tc.setup(st, prov, cl)
			}
			s := New(st, cl, openfeaturetest.NewClient(nil), nil, "", nil, nil, prov)

			branch, connString, err := s.Provision(context.Background(), tc.in, tc.payload)

			switch {
			case tc.wantBusy:
				require.Error(t, err)
				_, ok := errors.AsType[store.ErrProjectBusy](err)
				require.True(t, ok, "expected error wrapping store.ErrProjectBusy, got %v", err)
			case tc.wantOK:
				require.NoError(t, err)
				require.NotNil(t, branch)
				require.Equal(t, testBranch, branch.ID)
				require.Empty(t, connString)
				// Every caller (API, Vercel, GitHub webhook) relies on Provision
				// populating the backup defaults when the request has none.
				require.Equal(t, DefaultBackupRetentionPeriod, tc.payload.BackupRetentionPeriod)
				require.NotNil(t, tc.payload.BackupConfig)
				require.True(t, tc.payload.BackupConfig.BackupsEnabled)
				require.Equal(t, "2d", tc.payload.BackupConfig.BackupRetention)
				require.Equal(t, BackupMethodBarman, tc.payload.BackupConfig.BackupMethod)
			default:
				require.Equal(t, tc.wantErr, err)
			}
		})
	}
}
