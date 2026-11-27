package clusters

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	tests := map[string]struct {
		cfg     Config
		wantErr string
	}{
		"valid config": {
			cfg: Config{
				ClustersStorageClass:        "xatastor",
				ClustersVolumeSnapshotClass: "xatastor",
				CloudProvider:               CloudProviderAWS,
			},
		},
		"valid gcp config": {
			cfg: Config{
				ClustersStorageClass:        "xatastor",
				ClustersVolumeSnapshotClass: "xatastor",
				CloudProvider:               CloudProviderGCP,
			},
		},
		"valid azure config": {
			cfg: Config{
				ClustersStorageClass:        "xatastor",
				ClustersVolumeSnapshotClass: "xatastor",
				CloudProvider:               CloudProviderAzure,
			},
		},
		"valid white label sql identity profile": {
			cfg: Config{
				ClustersStorageClass:        "xatastor",
				ClustersVolumeSnapshotClass: "xatastor",
				CloudProvider:               CloudProviderAWS,
				SQLIdentityProfile:          "WhiteLabel",
				SQLIdentityImages:           map[string]string{"18": "registry.example.com/cnpg-postgres-plus:18.7-wl1"},
			},
		},
		"valid white label sql identity profile with postgres images": {
			cfg: Config{
				ClustersStorageClass:        "xatastor",
				ClustersVolumeSnapshotClass: "xatastor",
				CloudProvider:               CloudProviderAWS,
				SQLIdentityProfile:          "WhiteLabel",
				SQLIdentityImages: map[string]string{
					"17": "registry.example.com/postgres:17.7@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
					"18": "registry.example.com/postgres:18.7@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
				},
			},
		},
		"invalid sql identity profile": {
			cfg: Config{
				ClustersStorageClass:        "xatastor",
				ClustersVolumeSnapshotClass: "xatastor",
				CloudProvider:               CloudProviderAWS,
				SQLIdentityProfile:          "whitelabel",
			},
			wantErr: "sql identity profile must be empty or",
		},
		"invalid cloud provider": {
			cfg: Config{
				ClustersStorageClass:        "xatastor",
				ClustersVolumeSnapshotClass: "xatastor",
				CloudProvider:               "digitalocean",
			},
			wantErr: "cloud provider must be",
		},
		"missing storage class": {
			cfg: Config{
				ClustersVolumeSnapshotClass: "xatastor",
			},
			wantErr: "storage class is required",
		},
		"missing volume snapshot class": {
			cfg: Config{
				ClustersStorageClass: "xatastor",
			},
			wantErr: "volume snapshot class is required",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}
