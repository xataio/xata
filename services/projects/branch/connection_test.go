package branch

import (
	"context"
	"testing"

	clustersv1 "xata/gen/proto/clusters/v1"
	"xata/gen/protomocks"
	"xata/services/projects/cells/cellsmock"
	"xata/services/projects/store"
	storemocks "xata/services/projects/store/mocks"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestBranchEndpoint(t *testing.T) {
	tests := map[string]struct {
		regionHostPort  string
		defaultHostPort string
		subdomain       string // "" means no cell subdomain (region-only hostname)
		wantHostname    string
		wantPort        int
		wantError       bool
	}{
		"region host:port": {
			regionHostPort: "eu-central-1.xata.tech:7654",
			wantHostname:   "br-1.eu-central-1.xata.tech",
			wantPort:       7654,
		},
		"host-only region uses the default postgres port": {
			regionHostPort: "us-east-1.xata.tech",
			wantHostname:   "br-1.us-east-1.xata.tech",
			wantPort:       5432,
		},
		"empty region falls back to the handler default": {
			defaultHostPort: "testdomain:5432",
			wantHostname:    "br-1.testdomain",
			wantPort:        5432,
		},
		"subdomain qualifies a host-only region": {
			regionHostPort: "us-east-1.xata.tech",
			subdomain:      "cell-2",
			wantHostname:   "br-1.cell-2.us-east-1.xata.tech",
			wantPort:       5432,
		},
		"subdomain qualifies a host:port region": {
			regionHostPort: "eu-central-1.xata.tech:7654",
			subdomain:      "cell-2",
			wantHostname:   "br-1.cell-2.eu-central-1.xata.tech",
			wantPort:       7654,
		},
		"subdomain qualifies the handler default": {
			defaultHostPort: "testdomain:5432",
			subdomain:       "cell-2",
			wantHostname:    "br-1.cell-2.testdomain",
			wantPort:        5432,
		},
		"no gateway configured at all fails": {
			wantError: true,
		},
		"no gateway configured fails even with a subdomain": {
			subdomain: "cell-2",
			wantError: true,
		},
		"non-numeric port fails": {
			regionHostPort: "us-east-1.xata.tech:sql",
			wantError:      true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var subdomain *string
			if tt.subdomain != "" {
				subdomain = &tt.subdomain
			}
			bs := New(nil, nil, nil, nil, tt.defaultHostPort, nil, nil, nil)
			gotHostname, gotPort, err := bs.BranchEndpoint(&store.Region{GatewayHostPort: tt.regionHostPort}, subdomain, "br-1")
			if tt.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantHostname, gotHostname)
			require.Equal(t, tt.wantPort, gotPort)
		})
	}
}

func TestConnectionStringDatabase(t *testing.T) {
	tests := map[string]struct {
		creds   *clustersv1.GetPostgresClusterCredentialsResponse
		wantDSN string
	}{
		"older cell reports no database": {
			creds:   &clustersv1.GetPostgresClusterCredentialsResponse{Username: "xata", Password: "pw"},
			wantDSN: "postgresql://xata:pw@br-1-deprecated.us-east-1.example.com/xata?sslmode=require",
		},
		"xata database": {
			creds:   &clustersv1.GetPostgresClusterCredentialsResponse{Username: "xata", Password: "pw", Database: "xata"},
			wantDSN: "postgresql://xata:pw@br-1-deprecated.us-east-1.example.com/xata?sslmode=require",
		},
		"other login and database": {
			creds:   &clustersv1.GetPostgresClusterCredentialsResponse{Username: "dbadmin", Password: "pw", Database: "defaultdb"},
			wantDSN: "postgresql://dbadmin:pw@br-1-deprecated.us-east-1.example.com/defaultdb?sslmode=require",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			st := storemocks.NewProjectsStore(t)
			st.EXPECT().GetRegion(mock.Anything, "org-1", "us-east-1").
				Return(&store.Region{GatewayHostPort: "us-east-1.example.com"}, nil).Once()
			st.EXPECT().GetCell(mock.Anything, "org-1", "cell-1").
				Return(&store.Cell{ID: "cell-1"}, nil).Once()
			cl := protomocks.NewClustersServiceClient(t)
			cl.EXPECT().GetPostgresClusterCredentials(mock.Anything, &clustersv1.GetPostgresClusterCredentialsRequest{Id: "br-1", Username: "app"}).
				Return(tc.creds, nil).Once()

			s := New(st, cellsmock.NewCellsMock(t, cl), nil, nil, "", nil, nil, nil)
			dsn, err := s.ConnectionString(context.Background(), "org-1", &store.Branch{ID: "br-1", Region: "us-east-1", CellID: "cell-1"})
			require.NoError(t, err)
			require.Equal(t, tc.wantDSN, dsn)
		})
	}
}
