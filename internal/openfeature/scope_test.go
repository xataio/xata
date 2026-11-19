package openfeature

import (
	"context"
	"testing"

	"github.com/open-feature/go-sdk/openfeature"
	"github.com/stretchr/testify/require"
)

func TestWithBranchScope(t *testing.T) {
	tests := map[string]struct {
		parent    openfeature.EvaluationContext
		wantKey   string
		wantAttrs map[string]any
	}{
		"keeps the existing context": {
			parent: openfeature.NewEvaluationContext("user1", map[string]any{
				"email":  "user@example.com",
				"groups": map[string]string{"organization": "org1"},
			}),
			wantKey: "user1",
			wantAttrs: map[string]any{
				"email":          "user@example.com",
				"groups":         map[string]string{"organization": "org1"},
				"organizationId": "org1",
				"projectId":      "proj1",
			},
		},
		"falls back to the organization without a context": {
			parent:  openfeature.EvaluationContext{},
			wantKey: "org1",
			wantAttrs: map[string]any{
				"groups":         map[string]string{"organization": "org1"},
				"organizationId": "org1",
				"projectId":      "proj1",
			},
		},
		"adds the organization group when the context has none": {
			parent:  openfeature.NewEvaluationContext("apikey1", map[string]any{"email": ""}),
			wantKey: "apikey1",
			wantAttrs: map[string]any{
				"email":          "",
				"groups":         map[string]string{"organization": "org1"},
				"organizationId": "org1",
				"projectId":      "proj1",
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := openfeature.WithTransactionContext(context.Background(), tc.parent)

			got := openfeature.TransactionContext(WithBranchScope(ctx, "org1", "proj1"))

			require.Equal(t, tc.wantKey, got.TargetingKey())
			require.Equal(t, tc.wantAttrs, got.Attributes())
			// The parent context must not be mutated.
			require.NotContains(t, openfeature.TransactionContext(ctx).Attributes(), "projectId")
		})
	}
}
