package client

import (
	"context"
	"testing"

	"github.com/open-feature/go-sdk/openfeature"
	"github.com/stretchr/testify/require"
)

func TestBoolValueOverrides(t *testing.T) {
	tests := map[string]struct {
		env  string
		flag FeatureFlag
		want bool
	}{
		"enabled by override": {
			env:  "useXatastor:true",
			flag: FeatureFlag{Name: "useXatastor", DefaultEnabled: false},
			want: true,
		},
		"disabled by override": {
			env:  "useXatastor:true,organizationCreation:false",
			flag: FeatureFlag{Name: "organizationCreation", DefaultEnabled: true},
			want: false,
		},
		"not overridden": {
			env:  "useXatastor:true",
			flag: FeatureFlag{Name: "useClusterPool", DefaultEnabled: false},
			want: false,
		},
		"unset": {
			flag: FeatureFlag{Name: "organizationCreation", DefaultEnabled: true},
			want: true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Setenv("XATA_FEATURE_FLAGS", tt.env)

			c, err := NewClient("test", openfeature.NoopProvider{})
			require.NoError(t, err)

			got := c.BoolValue(context.Background(), tt.flag)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestNewClientInvalidOverrides(t *testing.T) {
	t.Setenv("XATA_FEATURE_FLAGS", "useXatastor:yes")

	_, err := NewClient("test", openfeature.NoopProvider{})
	require.ErrorContains(t, err, "read feature flags config")
}
