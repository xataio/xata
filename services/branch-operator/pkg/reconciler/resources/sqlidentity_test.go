package resources_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/reconciler/resources"
	"xata/services/branch-operator/pkg/shared"
)

func TestGeneratePostInitSQLWhiteLabel(t *testing.T) {
	// Renaming the legacy statements gives the white label statements, so the
	// grants and role attributes are the same.
	rename := strings.NewReplacer(
		"xata_superuser", "db_superuser",
		" xata;", " dbadmin;",
		" xata ", " dbadmin ",
	)

	for _, v := range []int{14, 15, 16, 17, 18} {
		legacy := resources.GeneratePostInitSQL(v, shared.LegacySQLIdentity())
		got := resources.GeneratePostInitSQL(v, shared.WhiteLabelSQLIdentity())

		want := make([]string, len(legacy))
		for i, s := range legacy {
			want[i] = rename.Replace(s)
		}
		require.Equal(t, want, got, "PG%d", v)

		for _, s := range got {
			require.NotContains(t, s, "xata", "PG%d", v)
			require.NotContains(t, strings.ToUpper(s), " SUPERUSER", "PG%d", v)
		}
		require.Contains(t, got, "CREATE ROLE db_superuser NOLOGIN;")
		require.Contains(t, got, "ALTER ROLE dbadmin LOGIN INHERIT CREATEDB CREATEROLE BYPASSRLS REPLICATION;")
	}
}

func TestSQLIdentityFor(t *testing.T) {
	require.Equal(t, shared.LegacySQLIdentity(), shared.SQLIdentityFor(v1alpha1.SQLIdentityLegacy))
	require.Equal(t, shared.WhiteLabelSQLIdentity(), shared.SQLIdentityFor(v1alpha1.SQLIdentityWhiteLabel))
	require.Equal(t, shared.SQLIdentity{Login: "xata", Database: "xata", PrivilegeGroup: "xata_superuser", UtilsLibrary: "xatautils"}, shared.LegacySQLIdentity())
	require.Equal(t, shared.SQLIdentity{Login: "dbadmin", Database: "defaultdb", PrivilegeGroup: "db_superuser", UtilsLibrary: "pgprivs"}, shared.WhiteLabelSQLIdentity())
}

func TestClusterSpecSQLIdentity(t *testing.T) {
	tests := map[string]struct {
		profile v1alpha1.SQLIdentityProfile
		want    shared.SQLIdentity
	}{
		"no profile is legacy": {profile: v1alpha1.SQLIdentityLegacy, want: shared.LegacySQLIdentity()},
		"white label":          {profile: v1alpha1.SQLIdentityWhiteLabel, want: shared.WhiteLabelSQLIdentity()},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			base := v1alpha1.ClusterSpec{
				Image:   "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:18.1",
				Storage: v1alpha1.StorageSpec{Size: "1Gi"},
			}

			root := resources.ClusterSpec("b1", "b1", resources.ClusterConfig{
				ClusterSpec: base,
				SQLIdentity: tc.profile,
			})
			require.Equal(t, tc.want.Database, *root.Bootstrap.InitDB.Database)
			require.Equal(t, tc.want.Login, *root.Bootstrap.InitDB.Owner)
			require.Equal(t, resources.GeneratePostInitSQL(18, tc.want), root.Bootstrap.InitDB.PostInitSQL)
			require.Equal(t, tc.want.Login, *root.Managed.Roles[0].Name)
			require.Equal(t, []string{tc.want.PrivilegeGroup}, root.Managed.Roles[0].InRoles)

			child := resources.ClusterSpec("b2", "b2", resources.ClusterConfig{
				ClusterSpec: base,
				RestoreSpec: &v1alpha1.RestoreSpec{Type: v1alpha1.RestoreTypeVolumeSnapshot, Name: "b1"},
				SQLIdentity: tc.profile,
			})
			require.Nil(t, child.Bootstrap.InitDB)
			require.Equal(t, tc.want.Database, *child.Bootstrap.Recovery.Database)
			require.Equal(t, tc.want.Login, *child.Bootstrap.Recovery.Owner)
			require.Equal(t, "b2-app", *child.Bootstrap.Recovery.Secret.Name)
			require.Equal(t, tc.want.Login, *child.Managed.Roles[0].Name)
		})
	}
}

// The post-init SQL depends on the major version that the image tag names.
func TestClusterSpecPostInitSQLImageReference(t *testing.T) {
	const digest = "@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	tests := map[string]struct {
		image string
		major int
	}{
		"tag":                      {image: "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:18.1", major: 18},
		"tag suffix":               {image: "registry.example.com/pg/cnpg-postgres-plus:18.7-wl1", major: 18},
		"tag suffix and digest":    {image: "registry.example.com/pg/cnpg-postgres-plus:18.7-wl1" + digest, major: 18},
		"registry port and digest": {image: "localhost:5000/cnpg-postgres-plus:17.7-wl1" + digest, major: 17},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			base := v1alpha1.ClusterSpec{
				Image:   tc.image,
				Storage: v1alpha1.StorageSpec{Size: "1Gi"},
			}
			spec := resources.ClusterSpec("b1", "b1", resources.ClusterConfig{
				ClusterSpec: base,
				SQLIdentity: v1alpha1.SQLIdentityWhiteLabel,
			})
			require.Equal(t, tc.image, *spec.ImageName)
			require.Equal(t, resources.GeneratePostInitSQL(tc.major, shared.WhiteLabelSQLIdentity()), spec.Bootstrap.InitDB.PostInitSQL)
			require.NotEqual(t, resources.GeneratePostInitSQL(14, shared.WhiteLabelSQLIdentity()), spec.Bootstrap.InitDB.PostInitSQL)
		})
	}
}
