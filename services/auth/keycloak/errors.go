package keycloak

import (
	"fmt"
	"net/http"
)

type ErrOrganizationNotFound struct {
	ID string
}

func (e ErrOrganizationNotFound) Error() string {
	return fmt.Sprintf("organization %s not found", e.ID)
}

func (e ErrOrganizationNotFound) StatusCode() int {
	return http.StatusNotFound
}

type ErrOrganizationAlreadyExists struct {
	ID string
}

func (e ErrOrganizationAlreadyExists) Error() string {
	return fmt.Sprintf("organization %s already exists", e.ID)
}

func (e ErrOrganizationAlreadyExists) StatusCode() int {
	return http.StatusConflict
}

type ErrUnableToAddMember struct {
	ID             string
	OrganizationID string
}

func (e ErrUnableToAddMember) Error() string {
	return fmt.Sprintf("unable to add user %s to organization %s", e.ID, e.OrganizationID)
}

func (e ErrUnableToAddMember) StatusCode() int {
	return http.StatusConflict
}

// ErrOrganizationMemberLimitReached is returned when an organization has reached the maximum number of members
type ErrOrganizationMemberLimitReached struct {
	OrganizationID string
}

func (e ErrOrganizationMemberLimitReached) Error() string {
	return fmt.Sprintf("organization %s has reached the maximum number of members", e.OrganizationID)
}

func (e ErrOrganizationMemberLimitReached) StatusCode() int {
	return http.StatusBadRequest
}

type ErrUserNotFound struct {
	ID string
}

func (e ErrUserNotFound) Error() string {
	return fmt.Sprintf("user %s not found", e.ID)
}

func (e ErrUserNotFound) StatusCode() int {
	return http.StatusNotFound
}

type ErrInvitationConflict struct {
	Email string
}

func (e ErrInvitationConflict) Error() string {
	return fmt.Sprintf("user %s already invited or member of the organization", e.Email)
}

func (e ErrInvitationConflict) StatusCode() int {
	return http.StatusConflict
}

type ErrInvitationFailed struct {
	Email string
}

func (e ErrInvitationFailed) Error() string {
	return fmt.Sprintf("failed to send invitation to %s", e.Email)
}

func (e ErrInvitationFailed) StatusCode() int {
	return http.StatusUnprocessableEntity
}
