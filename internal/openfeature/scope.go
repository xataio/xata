package openfeature

import (
	"context"
	"maps"

	"github.com/open-feature/go-sdk/openfeature"
)

// WithBranchScope returns a context whose OpenFeature transaction context
// carries organizationId and projectId attributes on top of the existing one,
// so flags can target a project or an organization in a single flag. The
// PostHog provider sends these attributes as person properties, which keeps
// the flag locally evaluable.
//
// The existing targeting key and attributes are kept. When there is no
// targeting key (no request middleware ran), the organization is used, as the
// PostHog provider rejects an empty distinct id. The organization group is
// added when missing so org-targeted flags still resolve.
func WithBranchScope(ctx context.Context, organizationID, projectID string) context.Context {
	current := openfeature.TransactionContext(ctx)

	targetingKey := current.TargetingKey()
	if targetingKey == "" {
		targetingKey = organizationID
	}

	attrs := maps.Clone(current.Attributes())
	if attrs == nil {
		attrs = map[string]any{}
	}
	if groups, _ := attrs["groups"].(map[string]string); groups["organization"] == "" {
		groups = maps.Clone(groups)
		if groups == nil {
			groups = map[string]string{}
		}
		groups["organization"] = organizationID
		attrs["groups"] = groups
	}
	attrs["organizationId"] = organizationID
	attrs["projectId"] = projectID

	return openfeature.WithTransactionContext(ctx, openfeature.NewEvaluationContext(targetingKey, attrs))
}
