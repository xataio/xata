package clusters

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clustersv1 "xata/gen/proto/clusters/v1"
	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/clusters/internal/connectors/cnpg"
	cnpgmocks "xata/services/clusters/internal/connectors/cnpg/mocks"
)

func TestGetPostgresClusterCredentialsDatabase(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		branch       *v1alpha1.Branch
		secret       string
		login        string
		wantDatabase string
		wantCode     codes.Code
	}{
		// A legacy login returns what the secret holds: no database and no
		// read of the Branch.
		"legacy branch": {
			branch: &v1alpha1.Branch{Name: "cluster-1"},
			secret: "app",
			login:  "xata",
		},
		"legacy login without a branch": {
			secret: "app",
			login:  "xata",
		},
		"superuser secret without a branch": {
			secret: "superuser",
			login:  "postgres",
		},
		"white label branch": {
			branch: &v1alpha1.Branch{
				Name: "cluster-1",
				Spec: v1alpha1.BranchSpec{SQLIdentity: v1alpha1.SQLIdentityWhiteLabel},
			},
			secret:       "app",
			login:        "dbadmin",
			wantDatabase: "defaultdb",
		},
		"white label login without a branch": {
			secret:   "app",
			login:    "dbadmin",
			wantCode: codes.NotFound,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			cnpgMock := cnpgmocks.NewConnector(t)
			cnpgMock.EXPECT().GetClusterCredentials(mock.Anything, "cluster-1", "xata-clusters", tt.secret).
				Return(&cnpg.Credentials{Username: tt.login, Password: "secret"}, nil)

			var objs []client.Object
			if tt.branch != nil {
				objs = append(objs, tt.branch)
			}
			svc, _ := setupTestClustersService(t, withCNPGConnector(cnpgMock), withExistingObjects(objs...))

			resp, err := svc.GetPostgresClusterCredentials(context.Background(),
				&clustersv1.GetPostgresClusterCredentialsRequest{Id: "cluster-1", Username: tt.secret})
			if tt.wantCode != codes.OK {
				require.Equal(t, tt.wantCode, status.Code(err))
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.login, resp.GetUsername())
			require.Equal(t, tt.wantDatabase, resp.GetDatabase())
		})
	}
}
