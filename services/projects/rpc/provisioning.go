package rpc

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	projectsv1 "xata/gen/proto/projects/v1"
	"xata/internal/flags"
	"xata/services/projects/branch"
	"xata/services/projects/provisioner"
	"xata/services/projects/store"

	ofsdk "github.com/open-feature/go-sdk/openfeature"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CreateProject creates a project for an organization.
func (p *ProjectsService) CreateProject(ctx context.Context, req *projectsv1.CreateProjectRequest) (*projectsv1.CreateProjectResponse, error) {
	project, err := p.store.CreateProject(ctx, req.GetOrganizationId(), &store.CreateProjectConfiguration{
		Name:      req.GetName(),
		UsageTier: req.GetUsageTier(),
	})
	if err != nil {
		return nil, provisioningError("create project", err)
	}
	return &projectsv1.CreateProjectResponse{ProjectId: project.ID}, nil
}

// CreateBranch provisions a branch and returns its id and connection string. The
// provisioning feature flags, org limits and the branch-creation kill switch are
// resolved here so callers only supply the branch spec.
func (p *ProjectsService) CreateBranch(ctx context.Context, req *projectsv1.CreateBranchRequest) (*projectsv1.CreateBranchResponse, error) {
	if p.branches == nil {
		return nil, status.Error(codes.Unimplemented, "branch provisioning is not configured")
	}
	// Flags are evaluated below and inside the branch service. The REST path gets
	// its evaluation context from request middleware; there is none here, so set it
	// from the request before reading any flag (see withFlagContext).
	ctx = withFlagContext(ctx, req.GetOrganizationId())

	// Global kill switch, like the REST handler: retryable when disabled fleet-wide.
	if p.feat != nil && p.feat.BoolValue(ctx, flags.BranchCreationDisabled) {
		return nil, status.Error(codes.FailedPrecondition, "branch creation is disabled")
	}

	mode, err := branchModeFromProto(req)
	if err != nil {
		return nil, err
	}

	branchFlags := provisioner.FlagsConfig{}
	if p.feat != nil {
		branchFlags.UseXatastor = p.feat.BoolValue(ctx, flags.UseXatastor)
	}

	limits, err := provisioner.ResolveOrgLimits(ctx, p.store, req.GetUsageTier(), req.GetOrganizationId(), req.GetProjectId(), branchFlags)
	if err != nil {
		return nil, provisioningError("resolve org limits", err)
	}

	in := branch.CreateInput{
		OrganizationID: req.GetOrganizationId(),
		ProjectID:      req.GetProjectId(),
		Name:           req.GetName(),
		Mode:           mode,
		Marketplace:    req.GetMarketplace(),
		UsageTier:      req.GetUsageTier(),
		OrgLimits:      limits,
		Flags:          branchFlags,
		ScaleToZero:    scaleToZeroFromProto(req.GetScaleToZero()),
	}
	payload, err := p.branches.BuildPayload(ctx, in)
	if err != nil {
		return nil, provisioningError("build branch payload", err)
	}
	br, conn, err := p.branches.Provision(ctx, in, payload)
	if err != nil {
		return nil, provisioningError("provision branch", err)
	}
	return &projectsv1.CreateBranchResponse{BranchId: br.ID, ConnectionString: conn}, nil
}

// DeleteProject tears down a project and all its branches (best-effort, idempotent).
// It holds the per-project lock across the cascade — the same lock branch creation
// takes — so an in-flight branch create cannot commit after the cascade has run
// and leave live compute behind an acknowledged deletion. The acquire is blocking
// (not try): the caller's teardown must run to completion, not back off, so we wait
// out an in-flight create rather than returning a busy error it would swallow.
func (p *ProjectsService) DeleteProject(ctx context.Context, req *projectsv1.DeleteProjectRequest) (*projectsv1.DeleteProjectResponse, error) {
	if p.branches == nil {
		return nil, status.Error(codes.Unimplemented, "branch provisioning is not configured")
	}
	release, err := p.store.AcquireProjectLock(ctx, req.GetProjectId())
	if err != nil {
		return nil, provisioningError("acquire project lock", err)
	}
	defer func() { _ = release() }()

	if err := p.branches.DeleteProject(ctx, req.GetOrganizationId(), req.GetProjectId()); err != nil {
		return nil, provisioningError("delete project", err)
	}
	return &projectsv1.DeleteProjectResponse{}, nil
}

// GetBranchConnectionString returns the connection string for a branch.
func (p *ProjectsService) GetBranchConnectionString(ctx context.Context, req *projectsv1.GetBranchConnectionStringRequest) (*projectsv1.GetBranchConnectionStringResponse, error) {
	if p.branches == nil {
		return nil, status.Error(codes.Unimplemented, "branch provisioning is not configured")
	}
	br, err := p.store.DescribeBranch(ctx, req.GetOrganizationId(), req.GetProjectId(), req.GetBranchId())
	if err != nil {
		return nil, provisioningError("describe branch", err)
	}
	conn, err := p.branches.ConnectionString(ctx, req.GetOrganizationId(), br)
	if err != nil {
		return nil, provisioningError("connection string", err)
	}
	return &projectsv1.GetBranchConnectionStringResponse{ConnectionString: conn}, nil
}

// withFlagContext attaches the OpenFeature evaluation context the branch flags
// are resolved against. The REST path gets this from request middleware, but the
// gRPC server has none, so it is built from the request. The org is used as both
// the distinct id (the PostHog provider rejects an empty one, which would make
// every flag error out and fall back to its default) and the targeting group, so
// org-targeted flags — xatastor, the kill switch — evaluate as
// they do on the REST path.
func withFlagContext(ctx context.Context, organizationID string) context.Context {
	return ofsdk.WithTransactionContext(ctx, ofsdk.NewEvaluationContext(
		organizationID,
		map[string]any{"groups": map[string]string{"organization": organizationID}},
	))
}

// branchModeFromProto maps the request's mode oneof to a branch.Mode.
func branchModeFromProto(req *projectsv1.CreateBranchRequest) (branch.Mode, error) {
	switch m := req.GetMode().(type) {
	case *projectsv1.CreateBranchRequest_FromParentBranchId:
		return branch.FromParent{ParentID: m.FromParentBranchId}, nil
	case *projectsv1.CreateBranchRequest_FromConfiguration:
		cfg := m.FromConfiguration
		c := branch.Configuration{
			Image:        cfg.GetImage(),
			Region:       cfg.GetRegion(),
			InstanceType: cfg.GetInstanceType(),
			Replicas:     cfg.GetReplicas(),
		}
		if pl := cfg.GetPreloadLibraries(); len(pl) > 0 {
			c.PreloadLibraries = &pl
		}
		if pp := cfg.GetPostgresConfigurationParameters(); len(pp) > 0 {
			c.PostgresConfigurationParameters = &pp
		}
		if cfg.StorageGb != nil {
			storage := cfg.GetStorageGb()
			c.Storage = &storage
		}
		return branch.FromConfiguration{Config: c}, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "mode is required (from_configuration or from_parent_branch_id)")
	}
}

func scaleToZeroFromProto(s *projectsv1.BranchScaleToZero) *branch.ScaleToZero {
	if s == nil {
		return nil
	}
	return &branch.ScaleToZero{Enabled: s.GetEnabled(), InactivityPeriodMinutes: int(s.GetInactivityPeriodMinutes())}
}

// provisioningError maps a store/branch error to a gRPC status. Any error that
// carries an HTTP status (ErrInvalidParam, limits, rate limits, duplicate
// project, unhealthy parent, …) keeps that status: it is attached as a
// google.rpc.ErrorInfo detail so the caller can reproduce the exact failure
// instead of collapsing everything to a retryable internal error. Invalid
// parameters additionally carry the offending field as a BadRequest detail.
// Errors with no HTTP status are wrapped (Unknown) and logged by the interceptor.
func provisioningError(op string, err error) error {
	coded, ok := errors.AsType[interface {
		error
		StatusCode() int
	}](err)
	if !ok {
		return fmt.Errorf("%s: %w", op, err)
	}
	httpStatus := coded.StatusCode()
	st := status.New(codeForHTTPStatus(httpStatus), err.Error())

	info := &errdetails.ErrorInfo{
		Reason:   "PROVISIONING_FAILED",
		Domain:   "projects",
		Metadata: map[string]string{"httpStatus": strconv.Itoa(httpStatus)},
	}
	var (
		withDetails *status.Status
		derr        error
	)
	if invalid, ok := errors.AsType[branch.ErrInvalidParam](err); ok && invalid.Param != "" {
		withDetails, derr = st.WithDetails(info, &errdetails.BadRequest{
			FieldViolations: []*errdetails.BadRequest_FieldViolation{
				{Field: invalid.Param, Description: invalid.Message},
			},
		})
	} else {
		withDetails, derr = st.WithDetails(info)
	}
	if derr == nil {
		st = withDetails
	}
	return st.Err()
}

// codeForHTTPStatus maps the HTTP status a store/branch error carries to a gRPC
// code, for callers and logs that read the code rather than the ErrorInfo detail.
func codeForHTTPStatus(httpStatus int) codes.Code {
	switch httpStatus {
	case http.StatusBadRequest:
		return codes.InvalidArgument
	case http.StatusForbidden:
		return codes.PermissionDenied
	case http.StatusNotFound:
		return codes.NotFound
	case http.StatusConflict:
		return codes.Aborted
	case http.StatusPreconditionFailed:
		return codes.FailedPrecondition
	case http.StatusTooManyRequests:
		return codes.ResourceExhausted
	case http.StatusServiceUnavailable:
		return codes.Unavailable
	default:
		return codes.Unknown
	}
}
