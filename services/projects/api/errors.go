package api

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"xata/internal/xvalidator"
)

// The empty string clears a description. Otherwise the separators - _ . / : are allowed
// after the first character so identifiers and label paths fit, e.g. "company/infra/managed-by-x".
var validBranchDescriptionRegex = regexp.MustCompile(`^([a-zA-Z0-9][a-zA-Z0-9\-_./: ]*)?$`)

type ErrorInvalidDescription struct {
	Message     string
	Description string
}

func (e ErrorInvalidDescription) Error() string {
	return fmt.Sprintf("description %s invalid: %s", e.Description, e.Message)
}

func (e ErrorInvalidDescription) StatusCode() int {
	return http.StatusBadRequest
}

func IsBranchDescriptionValid(e *string, maxLen int) error {
	if e == nil {
		return nil
	}
	if len(*e) > maxLen {
		return xvalidator.ErrorMaxLength{
			Limit: maxLen,
		}
	}
	if !validBranchDescriptionRegex.MatchString(*e) {
		return ErrorInvalidDescription{
			Description: *e,
			Message:     fmt.Sprintf("invalid branch description %s", *e),
		}
	}
	return nil
}

type ErrorInvalidParam struct {
	ProjectID  string
	BranchName string
	Param      string
	Message    string
}

func (e ErrorInvalidParam) Error() string {
	// Do not change the "Project [...]: " / "Branch [...]: " prefixes without
	// coordinating with the frontend: the webapp parses this exact format to
	// strip the prefix from postgres configuration errors (see
	// cleanPostgresError in apps/webapp/src/components/settings/postgres-configuration.tsx).
	errMsg := strings.Builder{}
	if e.ProjectID != "" {
		fmt.Fprintf(&errMsg, "Project [%s]: ", e.ProjectID)
	}
	if e.BranchName != "" {
		fmt.Fprintf(&errMsg, "Branch [%s]: ", e.BranchName)
	}

	fmt.Fprintf(&errMsg, "invalid parameter [%s]: %s", e.Param, e.Message)
	return errMsg.String()
}

func (e ErrorInvalidParam) StatusCode() int {
	return http.StatusBadRequest
}

type ErrorGithubInstallationValidationUnavailable struct {
	Err error
}

func (e ErrorGithubInstallationValidationUnavailable) Error() string {
	if e.Err == nil {
		return "cannot validate installation ID"
	}
	return fmt.Sprintf("cannot validate installation ID: %v", e.Err)
}

func (e ErrorGithubInstallationValidationUnavailable) Unwrap() error {
	return e.Err
}

func (e ErrorGithubInstallationValidationUnavailable) StatusCode() int {
	return http.StatusServiceUnavailable
}

// ErrorGithubUserSessionRequired is returned when github app installations are
// managed with an API key instead of a user session.
type ErrorGithubUserSessionRequired struct{}

func (e ErrorGithubUserSessionRequired) Error() string {
	return "github app installations can only be managed with a user session"
}

func (e ErrorGithubUserSessionRequired) StatusCode() int {
	return http.StatusForbidden
}

// ErrorGithubAccountNotConnected is returned when the user has no GitHub account
// connected to their Xata account.
type ErrorGithubAccountNotConnected struct{}

func (e ErrorGithubAccountNotConnected) Error() string {
	return "github account is not connected"
}

func (e ErrorGithubAccountNotConnected) StatusCode() int {
	return http.StatusBadRequest
}

// ErrorGithubAccountReconnectRequired is returned when the stored GitHub token is
// expired or revoked and the user needs to reconnect their GitHub account.
type ErrorGithubAccountReconnectRequired struct{}

func (e ErrorGithubAccountReconnectRequired) Error() string {
	return "github connection has expired, reconnect your github account"
}

func (e ErrorGithubAccountReconnectRequired) StatusCode() int {
	return http.StatusBadRequest
}

// ErrorGithubInstallationNotAccessible is returned when the github app installation
// is not visible to the user's GitHub account. It intentionally does not distinguish
// between installations that do not exist and installations of other users.
type ErrorGithubInstallationNotAccessible struct {
	InstallationID int64
}

func (e ErrorGithubInstallationNotAccessible) Error() string {
	return fmt.Sprintf("github app installation [%d] is not accessible", e.InstallationID)
}

func (e ErrorGithubInstallationNotAccessible) StatusCode() int {
	return http.StatusForbidden
}

type ErrorGithubRepositoryValidationUnavailable struct {
	Err error
}

func (e ErrorGithubRepositoryValidationUnavailable) Error() string {
	if e.Err == nil {
		return "cannot validate github repository ID"
	}
	return fmt.Sprintf("cannot validate github repository ID: %v", e.Err)
}

func (e ErrorGithubRepositoryValidationUnavailable) Unwrap() error {
	return e.Err
}

func (e ErrorGithubRepositoryValidationUnavailable) StatusCode() int {
	return http.StatusServiceUnavailable
}

type ErrorBranchNotFound struct {
	BranchID string
}

func (e ErrorBranchNotFound) Error() string {
	return fmt.Sprintf("Branch with ID [%s]: not found", e.BranchID)
}

func (e ErrorBranchNotFound) StatusCode() int {
	return http.StatusNotFound
}

type ErrorCredentialsForBranchNotFound struct {
	BranchID string
	Username string
}

func (e ErrorCredentialsForBranchNotFound) Error() string {
	if e.Username == "" {
		return fmt.Sprintf("Credentials on branch with ID [%s]: not found", e.BranchID)
	}
	return fmt.Sprintf("Credentials for username [%s] on branch with ID [%s]: not found", e.Username, e.BranchID)
}

func (e ErrorCredentialsForBranchNotFound) StatusCode() int {
	return http.StatusNotFound
}

type ErrorBranchCreationDisabled struct{}

func (e ErrorBranchCreationDisabled) Error() string {
	return "Branch creation is temporarily disabled"
}

func (e ErrorBranchCreationDisabled) StatusCode() int {
	return http.StatusServiceUnavailable
}

type ErrorOrganizationDisabled struct {
	OrganizationID string
}

func (e ErrorOrganizationDisabled) Error() string {
	return fmt.Sprintf("Organization with ID [%s] is disabled, please check your billing settings or contact support", e.OrganizationID)
}

func (e ErrorOrganizationDisabled) StatusCode() int {
	return http.StatusForbidden
}

type ErrorChildBranchCreationDisabled struct{}

func (e ErrorChildBranchCreationDisabled) Error() string {
	return "Child branch creation is temporarily disabled"
}

func (e ErrorChildBranchCreationDisabled) StatusCode() int {
	return http.StatusServiceUnavailable
}

type ErrorParentBranchUnhealthy struct {
	ParentID string
}

func (e ErrorParentBranchUnhealthy) Error() string {
	return fmt.Sprintf("Cannot create child branch because parent branch with ID [%s] is not healthy", e.ParentID)
}

func (e ErrorParentBranchUnhealthy) StatusCode() int {
	return http.StatusPreconditionFailed
}

type ErrorBranchUpdateForbidden struct {
	BranchID string
}

func (e ErrorBranchUpdateForbidden) Error() string {
	// Assume that forbidden branch updates are temporary for now, they should
	// only originate from Kubernetes admission policies used to temporarily
	// block updates.
	return fmt.Sprintf("Branch with ID [%s] update is temporarily unavailable", e.BranchID)
}

func (e ErrorBranchUpdateForbidden) StatusCode() int {
	return http.StatusForbidden
}

type ErrorBackupNotFound struct {
	ID string
}

func (e ErrorBackupNotFound) Error() string {
	return fmt.Sprintf("Backup with ID [%s]: not found", e.ID)
}

func (e ErrorBackupNotFound) StatusCode() int {
	return http.StatusNotFound
}

// ErrorNoRecoveryPoint is returned when a restore is requested from a branch
// that has no completed backup, so no point in time can be restored.
type ErrorNoRecoveryPoint struct {
	BranchID string
}

func (e ErrorNoRecoveryPoint) Error() string {
	return fmt.Sprintf("Branch with ID [%s] has no completed backup to restore from", e.BranchID)
}

func (e ErrorNoRecoveryPoint) StatusCode() int {
	return http.StatusPreconditionFailed
}

// ErrorRestoreSourceUnsupported is returned when the cell refuses to restore
// from the backups of the source branch.
type ErrorRestoreSourceUnsupported struct {
	BranchID string
}

func (e ErrorRestoreSourceUnsupported) Error() string {
	return fmt.Sprintf("The backups of branch with ID [%s] cannot be restored to a new branch", e.BranchID)
}

func (e ErrorRestoreSourceUnsupported) StatusCode() int {
	return http.StatusPreconditionFailed
}

type ErrorBranchConflict struct {
	BranchID string
}

func (e ErrorBranchConflict) Error() string {
	return fmt.Sprintf("branch with ID [%s] was modified concurrently, please retry", e.BranchID)
}

func (e ErrorBranchConflict) StatusCode() int {
	return http.StatusConflict
}

type ErrorNewOrgBranchLimitExceeded struct {
	OrganizationID string
}

func (e ErrorNewOrgBranchLimitExceeded) Error() string {
	return fmt.Sprintf("Organization with ID [%s] has reached the branch limit for new organizations", e.OrganizationID)
}

func (e ErrorNewOrgBranchLimitExceeded) StatusCode() int {
	return http.StatusForbidden
}
