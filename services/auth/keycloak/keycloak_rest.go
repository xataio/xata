package keycloak

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"xata/internal/idgen"
	"xata/services/auth/api/spec"
	"xata/services/auth/config"

	"github.com/Nerzal/gocloak/v13"
	"github.com/go-resty/resty/v2"
	openapi_types "github.com/oapi-codegen/runtime/types"
)

const (
	OrganizationDisabledByAdminKey           = "disabledByAdmin"
	OrganizationBillingStatusKey             = "billingStatus"
	OrganizationAdminReasonKey               = "adminReason"
	OrganizationBillingReasonKey             = "billingReason"
	OrganizationLastUpdatedKey               = "lastUpdated"
	OrganizationCreatedAtKey                 = "createdAt"
	OrganizationResourcesCleanedAtKey        = "resourcesCleanedAt"
	OrganizationBillingStatusNoPaymentMethod = "no_payment_method"

	OrganizationUsageTierKey = "usageTier"

	OrganizationMarketplaceKey   = "marketplace"
	OrganizationAWSCustomerIDKey = "awsCustomerId"
	OrganizationAWSProductIDKey  = "awsProductId"
	OrganizationAWSAccountIDKey  = "awsAccountId"
)

// Implements kc.go
type restKC struct {
	client     *gocloak.GoCloak
	authConfig config.AuthConfig
}

func NewRestKC(client *gocloak.GoCloak, authConfig config.AuthConfig) KeyCloak {
	return &restKC{
		client:     client,
		authConfig: authConfig,
	}
}

func (r *restKC) CreateOrganization(ctx context.Context, realm string, params OrganizationCreate) (spec.Organization, error) {
	if params.Marketplace != nil {
		if err := params.Marketplace.Validate(); err != nil {
			return spec.Organization{}, fmt.Errorf("validate marketplace: %w", err)
		}
	}

	orgsURL, err := r.buildRealmURL(realm, "organizations")
	if err != nil {
		return spec.Organization{}, err
	}

	id := idgen.GenerateOrganizationID()
	createOrg := r.buildCreateOrganizationPayload(id, params)

	resp, err := r.makeAuthenticatedRequest(ctx, "POST", orgsURL, nil, createOrg)
	if err != nil {
		return spec.Organization{}, fmt.Errorf("failed to create organization: %w", err)
	}

	if !r.isSuccessStatus(resp.StatusCode(), http.StatusCreated, http.StatusOK) {
		return spec.Organization{}, fmt.Errorf("failed to create organization: %s, status %d", params.Name, resp.StatusCode())
	}

	org := spec.Organization{
		Id:   id,
		Name: params.Name,
	}
	if params.Marketplace != nil {
		attrs := params.Marketplace.BuildKeycloakAttributes()
		if v, ok := attrs[OrganizationMarketplaceKey]; ok && len(v) > 0 {
			org.Marketplace = &v[0]
		}
	}
	return org, nil
}

func (r *restKC) buildCreateOrganizationPayload(id string, params OrganizationCreate) KeycloakOrganization {
	defaultBillingStatus := OrganizationBillingStatusNoPaymentMethod
	defaultBillingReason := "Organization created, no payment method set"
	if !r.authConfig.BillingRequired {
		defaultBillingStatus = string(spec.Ok)
		defaultBillingReason = "Organization enabled by default since billing is not required"
	}

	now := time.Now().UTC().Format(time.RFC3339)
	attrs := map[string][]string{
		"displayName":                  {params.Name},
		OrganizationDisabledByAdminKey: {"false"},
		OrganizationBillingStatusKey:   {defaultBillingStatus},
		OrganizationBillingReasonKey:   {defaultBillingReason},
		OrganizationUsageTierKey:       {string(params.usageTierOrDefault())},
		OrganizationLastUpdatedKey:     {now},
		OrganizationCreatedAtKey:       {now},
	}
	if params.Marketplace != nil {
		maps.Copy(attrs, params.Marketplace.BuildKeycloakAttributes())
	}
	return KeycloakOrganization{
		Name:        id,
		Alias:       id,
		Attributes:  attrs,
		RedirectURL: fmt.Sprintf(r.authConfig.FrontendURL+"/organizations/%s", id),
	}
}

func (r *restKC) GetOrganization(ctx context.Context, realm, alias string) (spec.Organization, error) {
	organization, err := r.searchOrganization(ctx, realm, alias)
	if err != nil {
		return spec.Organization{}, fmt.Errorf("failed to get organization: %w", err)
	}

	return r.convertToSpecOrganization(organization), nil
}

func (r *restKC) ListOrganizations(ctx context.Context, realm, userID string) ([]spec.Organization, error) {
	if userID == "" {
		return nil, fmt.Errorf("userID is empty")
	}

	orgsURL, err := r.buildRealmURL(realm, "organizations/members", userID, "organizations")
	if err != nil {
		return nil, err
	}

	queryParams := map[string]string{
		"briefRepresentation": "false",
	}

	resp, err := r.makeAuthenticatedRequest(ctx, "GET", orgsURL, queryParams, nil)
	if err != nil {
		return nil, fmt.Errorf("keycloak request: %w", err)
	}

	if !r.isSuccessStatus(resp.StatusCode(), http.StatusOK, http.StatusConflict) {
		return nil, fmt.Errorf("unexpected status code: %s, status code: %d", resp.String(), resp.StatusCode())
	}

	var keycloakOrganizations []KeycloakOrganization
	err = json.Unmarshal(resp.Body(), &keycloakOrganizations)
	if err != nil {
		return nil, fmt.Errorf("unmarshal organization list: %w", err)
	}

	orgs := make([]spec.Organization, len(keycloakOrganizations))
	for i, org := range keycloakOrganizations {
		orgs[i] = r.convertToSpecOrganization(org)
	}

	return orgs, nil
}

func (r *restKC) AddMember(ctx context.Context, realm string, organizationID string, userID string) error {
	organization, err := r.searchOrganization(ctx, realm, organizationID)
	if err != nil {
		return fmt.Errorf("failed to get organization: %w", err)
	}

	members, err := r.ListMembers(ctx, realm, organizationID)
	if err != nil {
		return fmt.Errorf("failed to list members: %w", err)
	}
	if len(members) >= MaxOrganizationMembers {
		return ErrOrganizationMemberLimitReached{OrganizationID: organizationID}
	}

	addMemberURL, err := r.buildRealmURL(realm, "organizations", organization.ID, "members")
	if err != nil {
		return fmt.Errorf("failed to join URL: %w", err)
	}

	resp, err := r.makeAuthenticatedRequest(ctx, "POST", addMemberURL, nil, userID)
	if err != nil {
		return ErrUnableToAddMember{userID, organizationID}
	}

	if !r.isSuccessStatus(resp.StatusCode(), http.StatusOK, http.StatusCreated) {
		return ErrUnableToAddMember{userID, organizationID}
	}
	return nil
}

func (r *restKC) RemoveMember(ctx context.Context, realm string, organizationID string, userID string) error {
	organization, err := r.searchOrganization(ctx, realm, organizationID)
	if err != nil {
		return fmt.Errorf("failed to get organization: %w", err)
	}

	delURL, err := r.buildRealmURL(realm, "organizations", organization.ID, "members", userID)
	if err != nil {
		return fmt.Errorf("failed to join URL: %w", err)
	}

	resp, err := r.makeAuthenticatedRequest(ctx, "DELETE", delURL, nil, nil)
	if err != nil {
		return err
	}
	if !r.isSuccessStatus(resp.StatusCode(), http.StatusOK, http.StatusNoContent) {
		return fmt.Errorf("failed to remove member: status code: %d", resp.StatusCode())
	}
	return nil
}

func (r *restKC) ListMembers(ctx context.Context, realm string, organizationID string) ([]spec.UserWithID, error) {
	organization, err := r.searchOrganization(ctx, realm, organizationID)
	if err != nil {
		return nil, fmt.Errorf("failed to get organization: %w", err)
	}

	listURL, err := r.buildRealmURL(realm, "organizations", organization.ID, "members")
	if err != nil {
		return nil, fmt.Errorf("failed to join URL: %w", err)
	}

	queryParams := map[string]string{
		"max": fmt.Sprintf("%d", MaxOrganizationMembers),
	}

	resp, err := r.makeAuthenticatedRequest(ctx, "GET", listURL, queryParams, nil)
	if err != nil {
		return nil, err
	}
	if !r.isSuccessStatus(resp.StatusCode(), http.StatusOK) {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode())
	}

	var users []User
	if err := json.Unmarshal(resp.Body(), &users); err != nil {
		return nil, fmt.Errorf("failed to unmarshal members: %w", err)
	}

	res := make([]spec.UserWithID, len(users))
	for i, u := range users {
		res[i] = spec.UserWithID{
			Email: openapi_types.Email(u.Email),
			Name:  fmt.Sprintf("%s %s", u.FirstName, u.LastName),
			Id:    u.ID,
		}
	}
	return res, nil
}

func (r *restKC) CreateInvitation(ctx context.Context, realm string, organizationID string, email string) error {
	organization, err := r.searchOrganization(ctx, realm, organizationID)
	if err != nil {
		return fmt.Errorf("failed to get organization: %w", err)
	}

	invURL, err := r.buildRealmURL(realm, "organizations", organization.ID, "members", "invite-user")
	if err != nil {
		return fmt.Errorf("failed to join URL: %w", err)
	}

	formData := url.Values{}
	formData.Set("email", email)

	resp, err := r.makeAuthenticatedRequest(ctx, "POST", invURL, nil, formData)
	if err != nil {
		return err
	}

	if resp.StatusCode() == http.StatusConflict {
		return ErrInvitationConflict{Email: email}
	}

	if !r.isSuccessStatus(resp.StatusCode(), http.StatusCreated, http.StatusNoContent, http.StatusOK) {
		return ErrInvitationFailed{Email: email}
	}

	return nil
}

func (r *restKC) ListInvitations(ctx context.Context, realm string, organizationID string, params ListInvitationsParams) ([]OrganizationInvitation, error) {
	organization, err := r.searchOrganization(ctx, realm, organizationID)
	if err != nil {
		return nil, fmt.Errorf("failed to get organization: %w", err)
	}

	listURL, err := r.buildRealmURL(realm, "organizations", organization.ID, "invitations")
	if err != nil {
		return nil, fmt.Errorf("failed to join URL: %w", err)
	}

	queryParams := make(map[string]string)
	if params.Status != nil {
		queryParams["status"] = *params.Status
	}
	if params.Email != nil {
		queryParams["email"] = *params.Email
	}
	if params.FirstName != nil {
		queryParams["firstName"] = *params.FirstName
	}
	if params.LastName != nil {
		queryParams["lastName"] = *params.LastName
	}
	if params.Search != nil {
		queryParams["search"] = *params.Search
	}
	if params.First != nil {
		queryParams["first"] = fmt.Sprintf("%d", *params.First)
	}
	if params.Max != nil {
		queryParams["max"] = fmt.Sprintf("%d", *params.Max)
	}

	resp, err := r.makeAuthenticatedRequest(ctx, "GET", listURL, queryParams, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to list invitations: %w", err)
	}

	if !r.isSuccessStatus(resp.StatusCode(), http.StatusOK) {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode())
	}

	var invitations []OrganizationInvitation
	if err := json.Unmarshal(resp.Body(), &invitations); err != nil {
		return nil, fmt.Errorf("failed to unmarshal invitations: %w", err)
	}

	return invitations, nil
}

func (r *restKC) GetInvitation(ctx context.Context, realm string, organizationID string, invitationID string) (OrganizationInvitation, error) {
	organization, err := r.searchOrganization(ctx, realm, organizationID)
	if err != nil {
		return OrganizationInvitation{}, fmt.Errorf("failed to get organization: %w", err)
	}

	getURL, err := r.buildRealmURL(realm, "organizations", organization.ID, "invitations", invitationID)
	if err != nil {
		return OrganizationInvitation{}, fmt.Errorf("failed to join URL: %w", err)
	}

	resp, err := r.makeAuthenticatedRequest(ctx, "GET", getURL, nil, nil)
	if err != nil {
		return OrganizationInvitation{}, fmt.Errorf("failed to get invitation: %w", err)
	}

	if !r.isSuccessStatus(resp.StatusCode(), http.StatusOK) {
		return OrganizationInvitation{}, fmt.Errorf("unexpected status code: %d", resp.StatusCode())
	}

	var invitation OrganizationInvitation
	if err := json.Unmarshal(resp.Body(), &invitation); err != nil {
		return OrganizationInvitation{}, fmt.Errorf("failed to unmarshal invitation: %w", err)
	}

	return invitation, nil
}

func (r *restKC) ResendInvitation(ctx context.Context, realm string, organizationID string, invitationID string) error {
	organization, err := r.searchOrganization(ctx, realm, organizationID)
	if err != nil {
		return fmt.Errorf("failed to get organization: %w", err)
	}

	resendURL, err := r.buildRealmURL(realm, "organizations", organization.ID, "invitations", invitationID, "resend")
	if err != nil {
		return fmt.Errorf("failed to join URL: %w", err)
	}

	resp, err := r.makeAuthenticatedRequest(ctx, "POST", resendURL, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to resend invitation: %w", err)
	}

	if !r.isSuccessStatus(resp.StatusCode(), http.StatusOK, http.StatusNoContent) {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode())
	}

	return nil
}

func (r *restKC) DeleteInvitation(ctx context.Context, realm string, organizationID string, invitationID string) error {
	organization, err := r.searchOrganization(ctx, realm, organizationID)
	if err != nil {
		return fmt.Errorf("failed to get organization: %w", err)
	}

	deleteURL, err := r.buildRealmURL(realm, "organizations", organization.ID, "invitations", invitationID)
	if err != nil {
		return fmt.Errorf("failed to join URL: %w", err)
	}

	resp, err := r.makeAuthenticatedRequest(ctx, "DELETE", deleteURL, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to delete invitation: %w", err)
	}

	if !r.isSuccessStatus(resp.StatusCode(), http.StatusOK, http.StatusNoContent) {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode())
	}

	return nil
}

func (r *restKC) UpdateOrganization(
	ctx context.Context,
	realm, organizationID string,
	update OrganizationUpdate,
) (spec.Organization, error) {
	organization, err := r.searchOrganization(ctx, realm, organizationID)
	if err != nil {
		return spec.Organization{}, fmt.Errorf("failed to get organization: %w", err)
	}

	// Collect all attribute updates first
	updates := make(map[string][]string)

	if update.Name != nil {
		updates["displayName"] = []string{*update.Name}
	}
	if update.DisabledByAdmin != nil {
		updates[OrganizationDisabledByAdminKey] = []string{fmt.Sprintf("%t", *update.DisabledByAdmin)}
	}
	if update.BillingStatus != nil {
		updates[OrganizationBillingStatusKey] = []string{*update.BillingStatus}
	}
	if update.AdminReason != nil {
		updates[OrganizationAdminReasonKey] = []string{*update.AdminReason}
	}
	if update.BillingReason != nil {
		updates[OrganizationBillingReasonKey] = []string{*update.BillingReason}
	}
	if update.UsageTier != nil {
		updates[OrganizationUsageTierKey] = []string{*update.UsageTier}
	}
	var deleteResourcesCleanedAt bool
	if update.ResourcesCleanedAt != nil {
		if *update.ResourcesCleanedAt == "" {
			deleteResourcesCleanedAt = true
		} else {
			updates[OrganizationResourcesCleanedAtKey] = []string{*update.ResourcesCleanedAt}
		}
	}

	if len(updates) == 0 && !deleteResourcesCleanedAt {
		return r.GetOrganization(ctx, realm, organizationID)
	}

	// Apply updates
	if organization.Attributes == nil {
		organization.Attributes = make(map[string][]string)
	}

	maps.Copy(organization.Attributes, updates)

	if deleteResourcesCleanedAt {
		delete(organization.Attributes, OrganizationResourcesCleanedAtKey)
	}
	organization.Attributes[OrganizationLastUpdatedKey] = []string{
		time.Now().UTC().Format(time.RFC3339),
	}

	updateOrgURL, err := r.buildRealmURL(realm, "organizations", organization.ID)
	if err != nil {
		return spec.Organization{}, fmt.Errorf("failed to join URL: %w", err)
	}

	resp, err := r.makeAuthenticatedRequest(ctx, http.MethodPut, updateOrgURL, nil, organization)
	if err != nil {
		return spec.Organization{}, fmt.Errorf("failed to update organization: %w", err)
	}

	if !r.isSuccessStatus(resp.StatusCode(), http.StatusOK, http.StatusNoContent) {
		return spec.Organization{}, fmt.Errorf(
			"failed to update organization: %s, status code: %d, error: %s",
			organizationID, resp.StatusCode(), resp.String(),
		)
	}

	return r.GetOrganization(ctx, realm, organizationID)
}

func (r *restKC) ListDisabledOrganizations(ctx context.Context, realm string, returnCleanedUpOrgs bool) ([]spec.Organization, error) {
	orgsURL, err := r.buildRealmURL(realm, "organizations")
	if err != nil {
		return nil, err
	}

	queries := []string{
		"disabledByAdmin:true",
		"billingStatus:no_payment_method",
		"billingStatus:invoice_overdue",
		"billingStatus:unknown",
	}

	seen := make(map[string]struct{})
	var result []spec.Organization

	const pageSize = 200

	for _, q := range queries {
		fetched := pageSize
		for first := 0; fetched >= pageSize; first += pageSize {
			queryParams := map[string]string{
				"q":                   q,
				"briefRepresentation": "false",
				"first":               fmt.Sprintf("%d", first),
				"max":                 fmt.Sprintf("%d", pageSize),
			}

			resp, err := r.makeAuthenticatedRequest(ctx, "GET", orgsURL, queryParams, nil)
			if err != nil {
				return nil, fmt.Errorf("keycloak request for %s: %w", q, err)
			}

			if !r.isSuccessStatus(resp.StatusCode(), http.StatusOK) {
				return nil, fmt.Errorf("unexpected status code for %s: %d", q, resp.StatusCode())
			}

			var keycloakOrganizations []KeycloakOrganization
			if err := json.Unmarshal(resp.Body(), &keycloakOrganizations); err != nil {
				return nil, fmt.Errorf("unmarshal organization list for %s: %w", q, err)
			}

			for _, org := range keycloakOrganizations {
				if _, exists := seen[org.Alias]; exists {
					continue
				}
				seen[org.Alias] = struct{}{}
				if !returnCleanedUpOrgs {
					if _, ok := firstAttr(org.Attributes, OrganizationResourcesCleanedAtKey); ok {
						continue
					}
				}
				result = append(result, r.convertToSpecOrganization(org))
			}

			fetched = len(keycloakOrganizations)
		}
	}

	return result, nil
}

func (r *restKC) GetUserRepresentation(ctx context.Context, realm string, userID string) (User, error) {
	userURL, err := r.buildRealmURL(realm, "users", userID)
	if err != nil {
		return User{}, fmt.Errorf("failed to join URL: %w", err)
	}

	resp, err := r.makeAuthenticatedRequest(ctx, "GET", userURL, nil, nil)
	if err != nil {
		return User{}, fmt.Errorf("failed to get user representation: %w", err)
	}

	if !r.isSuccessStatus(resp.StatusCode(), http.StatusOK) {
		return User{}, fmt.Errorf("unexpected status code: %d", resp.StatusCode())
	}

	var user User
	err = json.Unmarshal(resp.Body(), &user)
	if err != nil {
		return User{}, fmt.Errorf("failed to unmarshal user representation: %w", err)
	}

	parseUserAttributes(&user)
	return user, nil
}

func parseUserAttributes(user *User) {
	if v, ok := firstAttr(user.Attributes, "marketplace"); ok {
		user.Marketplace = v
	}
	if v, ok := firstAttr(user.Attributes, "awsCustomerId"); ok {
		user.AWSCustomerID = v
	}
	if v, ok := firstAttr(user.Attributes, "awsProductId"); ok {
		user.AWSProductID = v
	}
	if v, ok := firstAttr(user.Attributes, "awsAccountId"); ok {
		user.AWSAccountID = v
	}
}

// UpdateUserAttributes uses a GET-merge-PUT pattern because the Keycloak Admin REST API
// user endpoint is PUT (full replacement), not PATCH. We must GET the full user representation,
// merge in the new attributes, and PUT the entire object back to avoid clobbering existing fields.
// See: https://www.keycloak.org/docs-api/latest/rest-api/index.html#_users
func (r *restKC) UpdateUserAttributes(ctx context.Context, realm, userID string, update UserAttributesUpdate) error {
	updates := make(map[string][]string)
	if update.Marketplace != nil {
		updates["marketplace"] = []string{*update.Marketplace}
	}
	if update.MarketplaceRegisteredAt != nil {
		updates["marketplaceRegisteredAt"] = []string{*update.MarketplaceRegisteredAt}
	}
	if update.AWSAccountID != nil {
		updates["awsAccountId"] = []string{*update.AWSAccountID}
	}
	if update.AWSCustomerID != nil {
		updates["awsCustomerId"] = []string{*update.AWSCustomerID}
	}
	if update.AWSProductID != nil {
		updates["awsProductId"] = []string{*update.AWSProductID}
	}

	if len(updates) == 0 {
		return nil
	}

	userURL, err := r.buildRealmURL(realm, "users", userID)
	if err != nil {
		return fmt.Errorf("build user URL: %w", err)
	}

	resp, err := r.makeAuthenticatedRequest(ctx, "GET", userURL, nil, nil)
	if err != nil {
		return fmt.Errorf("get user: %w", err)
	}
	if !r.isSuccessStatus(resp.StatusCode(), http.StatusOK) {
		return fmt.Errorf("get user: status %d", resp.StatusCode())
	}

	var raw map[string]any
	if err := json.Unmarshal(resp.Body(), &raw); err != nil {
		return fmt.Errorf("unmarshal user: %w", err)
	}

	existing, _ := raw["attributes"].(map[string]any)
	if existing == nil {
		existing = make(map[string]any)
	}
	for k, v := range updates {
		existing[k] = v
	}
	raw["attributes"] = existing

	resp, err = r.makeAuthenticatedRequest(ctx, "PUT", userURL, nil, raw)
	if err != nil {
		return fmt.Errorf("update user: %w", err)
	}
	if !r.isSuccessStatus(resp.StatusCode(), http.StatusOK, http.StatusNoContent) {
		return fmt.Errorf("update user: status %d", resp.StatusCode())
	}

	return nil
}

func (r *restKC) getToken(ctx context.Context) (*gocloak.JWT, error) {
	jwt, err := r.client.LoginAdmin(ctx, "temp-admin", r.authConfig.KeycloakAdminPassword, "master")
	if err != nil {
		return nil, fmt.Errorf("failed to login as admin: %w", err)
	}
	return jwt, err
}

func (r *restKC) searchOrganization(ctx context.Context, realm, alias string) (KeycloakOrganization, error) {
	searchURL, err := r.buildRealmURL(realm, "organizations")
	if err != nil {
		return KeycloakOrganization{}, fmt.Errorf("failed to join URL: %w", err)
	}

	queryParams := map[string]string{
		"q":                   fmt.Sprintf("alias:%s", alias),
		"briefRepresentation": "false",
	}

	resp, err := r.makeAuthenticatedRequest(ctx, "GET", searchURL, queryParams, nil)
	if err != nil {
		return KeycloakOrganization{}, fmt.Errorf("failed to get organization: %w", err)
	}

	if !r.isSuccessStatus(resp.StatusCode(), http.StatusOK, http.StatusConflict) {
		return KeycloakOrganization{}, ErrOrganizationNotFound{ID: alias}
	}

	var orgs []KeycloakOrganization
	err = json.Unmarshal(resp.Body(), &orgs)
	if err != nil {
		return KeycloakOrganization{}, fmt.Errorf("failed to unmarshal organization: %w", err)
	}

	if len(orgs) == 0 {
		return KeycloakOrganization{}, ErrOrganizationNotFound{ID: alias}
	}

	return orgs[0], nil
}

func (r *restKC) buildRealmURL(realm string, pathSegments ...string) (string, error) {
	segments := append([]string{"admin/realms", realm}, pathSegments...)
	return url.JoinPath(r.authConfig.KeycloakURL, segments...)
}

func (r *restKC) makeAuthenticatedRequest(ctx context.Context, method, urlStr string, queryParams map[string]string, data any) (*resty.Response, error) {
	jwt, err := r.getToken(ctx)
	if err != nil {
		return nil, err
	}

	req := r.client.GetRequestWithBearerAuth(ctx, jwt.AccessToken)

	if data != nil {
		switch v := data.(type) {
		case url.Values:
			req = req.SetHeader("Content-Type", "application/x-www-form-urlencoded")
			req = req.SetBody(v.Encode())
			// Handle JSON body for any other type
		default:
			req = req.SetBody(data)
		}
	}

	for key, value := range queryParams {
		req = req.SetQueryParam(key, value)
	}

	switch method {
	case "GET":
		return req.Get(urlStr)
	case "POST":
		return req.Post(urlStr)
	case "PUT":
		return req.Put(urlStr)
	case "DELETE":
		return req.Delete(urlStr)
	default:
		return nil, fmt.Errorf("unsupported HTTP method: %s", method)
	}
}

func (r *restKC) isSuccessStatus(actual int, expected ...int) bool {
	return slices.Contains(expected, actual)
}

func (r *restKC) extractDisplayName(org KeycloakOrganization) string {
	if org.Attributes != nil {
		if displayName, ok := org.Attributes["displayName"]; ok && len(displayName) > 0 {
			return displayName[0]
		}
	}
	return org.Name
}

func (r *restKC) convertToSpecOrganization(org KeycloakOrganization) spec.Organization {
	result := spec.Organization{
		Id:     org.Alias,
		Name:   r.extractDisplayName(org),
		Status: r.extractStatus(org.Attributes),
	}
	if v, ok := firstAttr(org.Attributes, OrganizationMarketplaceKey); ok && v != "" {
		result.Marketplace = &v
	}
	return result
}

func (r *restKC) extractStatus(attributes map[string][]string) spec.OrganizationStatus {
	// This default apply for organizations with no status in Keycloak (created before org status was introduced)
	status := spec.OrganizationStatus{
		DisabledByAdmin: false,
		BillingStatus:   spec.Ok,
	}

	if v, ok := firstAttr(attributes, OrganizationDisabledByAdminKey); ok {
		switch strings.ToLower(v) {
		case "true":
			status.DisabledByAdmin = true
		case "false":
			status.DisabledByAdmin = false
		}
	}

	// Billing status default is `ok` for organizations created before billing integration
	status.BillingStatus = spec.Ok
	switch v, ok := firstAttr(attributes, OrganizationBillingStatusKey); {
	case ok && v == string(spec.Ok):
		status.BillingStatus = spec.Ok
	case ok && v == string(spec.InvoiceOverdue):
		status.BillingStatus = spec.InvoiceOverdue
	case ok && v == string(spec.NoPaymentMethod):
		status.BillingStatus = spec.NoPaymentMethod
	case ok:
		// If the billing status is unrecognized, set it to unknown
		status.BillingStatus = spec.Unknown
	}

	if v, ok := firstAttr(attributes, OrganizationAdminReasonKey); ok {
		status.AdminReason = &v
	}
	if v, ok := firstAttr(attributes, OrganizationBillingReasonKey); ok {
		status.BillingReason = &v
	}

	if v, ok := firstAttr(attributes, OrganizationLastUpdatedKey); ok {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			status.LastUpdated = t.UTC()
		}
	}
	if status.LastUpdated.IsZero() {
		status.LastUpdated = time.Unix(0, 0).UTC()
	}

	if v, ok := firstAttr(attributes, OrganizationCreatedAtKey); ok {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			createdAt := t.UTC()
			status.CreatedAt = &createdAt
		}
	}

	switch v, ok := firstAttr(attributes, OrganizationUsageTierKey); {
	case ok && v == string(spec.T2):
		status.UsageTier = spec.T2
	default:
		status.UsageTier = spec.T1
	}

	if !status.DisabledByAdmin && status.BillingStatus == spec.Ok {
		status.Status = spec.Enabled
	} else {
		status.Status = spec.Disabled
	}

	return status
}

func firstAttr(attrs map[string][]string, key string) (string, bool) {
	if attrs == nil {
		return "", false
	}
	v, ok := attrs[key]
	if !ok || len(v) == 0 {
		return "", false
	}
	s := strings.TrimSpace(v[0])
	return s, s != ""
}
