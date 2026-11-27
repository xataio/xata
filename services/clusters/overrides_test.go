package clusters

import (
	"context"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clustersv1 "xata/gen/proto/clusters/v1"
	"xata/internal/extensions"
	"xata/internal/postgresversions"
	"xata/services/branch-operator/api/v1alpha1"
	"xata/services/branch-operator/pkg/shared"
)

const testImagePG18 = "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:18.1"

const allowedConfigs = "auth_delay.*, auto_explain.*, log_lock_waits, log_min_duration_statement, log_min_messages, log_replication_commands, log_statement, log_temp_files, pg_net.batch_size, pg_net.ttl, pg_stat_statements.*, pgaudit.log, pgaudit.log_catalog, pgaudit.log_client, pgaudit.log_level, pgaudit.log_relation, pgaudit.log_rows, pgaudit.log_statement, pgaudit.log_statement_once, pgaudit.role, pgrst.*, plan_filter.*, safeupdate.enabled, session_replication_role, track_io_timing, wal_compression"

// allExtensionNames is the privileged extension list as computed before SQL
// identity profiles existed.
func allExtensionNames(image string) string {
	var names []string
	for _, ext := range extensions.GetExtensions(postgresversions.ShortImageName(image)) {
		names = append(names, ext.Name)
	}
	slices.Sort(names)
	return strings.Join(names, ", ")
}

func legacyMandatoryParameters(image string) map[string]string {
	return map[string]string{
		"lc_messages":                               "C.utf8",
		"lc_monetary":                               "C.utf8",
		"lc_numeric":                                "C.utf8",
		"lc_time":                                   "C.utf8",
		"pg_stat_statements.track_utility":          "off",
		"recovery_init_sync_method":                 "syncfs",
		"xatautils.privileged_role":                 "xata",
		"xatautils.privileged_extensions":           allExtensionNames(image),
		"xatautils.extension_custom_scripts_path":   "/etc/xatautils/extensions",
		"xatautils.reserved_memberships":            "xata_superuser, pg_read_server_files, pg_write_server_files, pg_execute_server_program",
		"xatautils.reserved_roles":                  "cnpg_pooler_pgbouncer, pgbouncer, postgres, streaming_replica, xata_superuser",
		"xatautils.privileged_role_allowed_configs": allowedConfigs,
	}
}

func requireNoLegacyNames(t *testing.T, params []v1alpha1.PostgresParameter, libs []string) {
	t.Helper()
	for _, p := range params {
		require.NotContains(t, strings.ToLower(p.Name), "xata", "parameter name %q", p.Name)
		require.NotContains(t, strings.ToLower(p.Value), "xata", "parameter %q value %q", p.Name, p.Value)
	}
	for _, l := range libs {
		require.NotContains(t, strings.ToLower(l), "xata", "preload library %q", l)
	}
}

func paramsMap(params []v1alpha1.PostgresParameter) map[string]string {
	m := make(map[string]string, len(params))
	for _, p := range params {
		m[p.Name] = p.Value
	}
	return m
}

func TestMandatoryPostgresParameters(t *testing.T) {
	t.Parallel()

	for _, image := range []string{testImage, testImagePG18} {
		t.Run(image, func(t *testing.T) {
			t.Parallel()

			legacy := MandatoryPostgresParameters(image, shared.LegacySQLIdentity())
			require.Equal(t, legacyMandatoryParameters(image), legacy)
			require.Contains(t, legacy["xatautils.privileged_extensions"], "xatautils")

			// The white label list is the legacy list without xatautils
			ext := strings.Join(slices.DeleteFunc(
				strings.Split(legacy["xatautils.privileged_extensions"], ", "),
				func(name string) bool { return name == "xatautils" }), ", ")
			require.NotEmpty(t, ext)

			wl := MandatoryPostgresParameters(image, shared.WhiteLabelSQLIdentity())
			require.Equal(t, map[string]string{
				"lc_messages":                             "C.utf8",
				"lc_monetary":                             "C.utf8",
				"lc_numeric":                              "C.utf8",
				"lc_time":                                 "C.utf8",
				"pg_stat_statements.track_utility":        "off",
				"recovery_init_sync_method":               "syncfs",
				"pgprivs.privileged_role":                 "dbadmin",
				"pgprivs.privileged_extensions":           ext,
				"pgprivs.extension_custom_scripts_path":   "/etc/pgprivs/extensions",
				"pgprivs.reserved_memberships":            "db_superuser, pg_read_server_files, pg_write_server_files, pg_execute_server_program",
				"pgprivs.reserved_roles":                  "cnpg_pooler_pgbouncer, pgbouncer, postgres, streaming_replica, db_superuser",
				"pgprivs.privileged_role_allowed_configs": allowedConfigs,
			}, wl)
			requireNoLegacyNames(t, postgresParametersFromMap(wl), nil)
		})
	}
}

func TestCreatePostgresClusterUtilsLibrary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	userParams := map[string]string{
		"max_connections":             "200",
		"cron.database_name":          "xata",
		"duckdb.postgres_role":        "xata",
		"xatautils.privileged_role":   "attacker",
		"xatautils.reserved_roles":    "",
		"XataUtils.reserved_roles":    "",
		"pgprivs.privileged_role":     "attacker",
		"PGPRIVS.reserved_roles":      "",
		"pgprivs.unknown_placeholder": "x",
	}
	userLibs := []string{"pg_stat_statements", "pgprivs", "xatautils", "pg_cron", "pg_duckdb"}

	tests := map[string]struct {
		cellProfile v1alpha1.SQLIdentityProfile
		image       string
		wantLibs    []string
		wantParams  map[string]string
	}{
		// A legacy Branch keeps what it got before SQL identity profiles
		// existed: only xatautils moves, and no setting is removed.
		"legacy": {
			image:    testImage,
			wantLibs: []string{"xatautils", "pg_stat_statements", "pgprivs", "pg_cron", "pg_duckdb"},
			wantParams: merge(legacyMandatoryParameters(testImage), map[string]string{
				"max_connections":             "200",
				"cron.database_name":          "xata",
				"duckdb.postgres_role":        "xata",
				"XataUtils.reserved_roles":    "",
				"pgprivs.privileged_role":     "attacker",
				"PGPRIVS.reserved_roles":      "",
				"pgprivs.unknown_placeholder": "x",
			}),
		},
		"white label": {
			cellProfile: v1alpha1.SQLIdentityWhiteLabel,
			image:       testImagePG18,
			wantLibs:    []string{"pgprivs", "pg_stat_statements", "pg_cron", "pg_duckdb"},
			wantParams: merge(MandatoryPostgresParameters(testImagePG18, shared.WhiteLabelSQLIdentity()), map[string]string{
				"max_connections":      "200",
				"cron.database_name":   "defaultdb",
				"duckdb.postgres_role": "dbadmin",
			}),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			svc, k8sClient := setupTestClustersService(t, withSQLIdentityProfile(tc.cellProfile))
			req, _, _, _, _ := exampleRequestsAndBranches()
			req.Configuration.ImageName = tc.image
			req.Configuration.PostgresConfigurationParameters = userParams
			req.Configuration.PreloadLibraries = userLibs

			_, err := svc.CreatePostgresCluster(ctx, req)
			require.NoError(t, err)

			branch, err := getBranchFromK8s(ctx, k8sClient, req.GetId())
			require.NoError(t, err)
			pg := branch.Spec.ClusterSpec.Postgres
			require.Equal(t, tc.wantLibs, pg.SharedPreloadLibraries)
			require.Equal(t, tc.wantParams, paramsMap(pg.Parameters))
			if tc.cellProfile == v1alpha1.SQLIdentityWhiteLabel {
				requireNoLegacyNames(t, pg.Parameters, pg.SharedPreloadLibraries)
			}
		})
	}
}

func TestCreatePostgresClusterWhiteLabelDropsUnloadedProfileDefaults(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	svc, k8sClient := setupTestClustersService(t, withSQLIdentityProfile(v1alpha1.SQLIdentityWhiteLabel))
	req, _, _, _, _ := exampleRequestsAndBranches()
	req.Configuration.PostgresConfigurationParameters = map[string]string{
		"cron.database_name":   "xata",
		"duckdb.postgres_role": "xata",
	}

	_, err := svc.CreatePostgresCluster(ctx, req)
	require.NoError(t, err)

	branch, err := getBranchFromK8s(ctx, k8sClient, req.GetId())
	require.NoError(t, err)
	params := paramsMap(branch.Spec.ClusterSpec.Postgres.Parameters)
	require.NotContains(t, params, "cron.database_name")
	require.NotContains(t, params, "duckdb.postgres_role")
	requireNoLegacyNames(t, branch.Spec.ClusterSpec.Postgres.Parameters, branch.Spec.ClusterSpec.Postgres.SharedPreloadLibraries)
}

func TestUpdatePostgresClusterUtilsLibrary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// What Projects sends: the internal preload libraries first, and the
	// extension defaults.
	fullUpdate := &clustersv1.UpdateClusterConfiguration{
		PreloadLibraries: []string{"xatautils", "pg_stat_statements", "pg_cron"},
		PostgresConfigurationParameters: map[string]string{
			"max_connections":           "300",
			"cron.database_name":        "xata",
			"xatautils.privileged_role": "attacker",
			"pgprivs.privileged_role":   "attacker",
		},
	}

	tests := map[string]struct {
		stored      v1alpha1.SQLIdentityProfile
		cellProfile v1alpha1.SQLIdentityProfile
		update      *clustersv1.UpdateClusterConfiguration
		wantLibs    []string
		wantParams  map[string]string
	}{
		"legacy": {
			cellProfile: v1alpha1.SQLIdentityWhiteLabel,
			update:      fullUpdate,
			wantLibs:    []string{"xatautils", "pg_stat_statements", "pg_cron"},
			wantParams: merge(legacyMandatoryParameters(testImage), map[string]string{
				"max_connections":         "300",
				"shared_buffers":          "128MB",
				"cron.database_name":      "xata",
				"pgprivs.privileged_role": "attacker",
			}),
		},
		"white label in a cell without a profile": {
			stored:   v1alpha1.SQLIdentityWhiteLabel,
			update:   fullUpdate,
			wantLibs: []string{"pgprivs", "pg_stat_statements", "pg_cron"},
			wantParams: merge(MandatoryPostgresParameters(testImage, shared.WhiteLabelSQLIdentity()), map[string]string{
				"max_connections":    "300",
				"shared_buffers":     "128MB",
				"cron.database_name": "defaultdb",
			}),
		},
		"white label hibernate only": {
			stored:   v1alpha1.SQLIdentityWhiteLabel,
			update:   &clustersv1.UpdateClusterConfiguration{Hibernate: new(true)},
			wantLibs: []string{"pgprivs", "pg_stat_statements"},
			wantParams: merge(MandatoryPostgresParameters(testImage, shared.WhiteLabelSQLIdentity()), map[string]string{
				"max_connections": "100",
				"shared_buffers":  "128MB",
			}),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// The stored spec carries the settings of the other library, as if
			// it had been written before the profile was applied.
			existing := parentBranch(withParentSQLIdentity(tc.stored), func(b *v1alpha1.Branch) {
				b.Spec.ClusterSpec.Image = testImage
				b.Spec.ClusterSpec.Postgres.SharedPreloadLibraries = []string{"xatautils", "pgprivs", "pg_stat_statements"}
				b.Spec.ClusterSpec.Postgres.Parameters = append(b.Spec.ClusterSpec.Postgres.Parameters,
					v1alpha1.PostgresParameter{Name: "xatautils.privileged_role", Value: "xata"},
					v1alpha1.PostgresParameter{Name: "pgprivs.privileged_role", Value: "dbadmin"},
				)
			})
			svc, k8sClient := setupTestClustersService(t,
				withSQLIdentityProfile(tc.cellProfile),
				withExistingObjects(existing))

			_, err := svc.UpdatePostgresCluster(ctx, &clustersv1.UpdatePostgresClusterRequest{
				Id:                  existing.Name,
				UpdateConfiguration: tc.update,
			})
			require.NoError(t, err)

			branch, err := getBranchFromK8s(ctx, k8sClient, existing.Name)
			require.NoError(t, err)
			pg := branch.Spec.ClusterSpec.Postgres
			require.Equal(t, tc.wantLibs, pg.SharedPreloadLibraries)
			require.Equal(t, tc.wantParams, paramsMap(pg.Parameters))
			if tc.stored == v1alpha1.SQLIdentityWhiteLabel {
				requireNoLegacyNames(t, pg.Parameters, pg.SharedPreloadLibraries)
			}
		})
	}
}

func TestRotatePostgresClusterCredentialsSQLIdentity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := map[string]struct {
		profile  v1alpha1.SQLIdentityProfile
		user     string
		wantCode codes.Code
		// rotated is the suffix of the secret that gets a new password
		rotated string
	}{
		"legacy login":           {user: "xata", rotated: "-app"},
		"legacy rejects dbadmin": {user: "dbadmin", wantCode: codes.InvalidArgument},
		"white label login":      {profile: v1alpha1.SQLIdentityWhiteLabel, user: "dbadmin", rotated: "-app"},
		"white label rejects xata": {
			profile: v1alpha1.SQLIdentityWhiteLabel, user: "xata", wantCode: codes.InvalidArgument,
		},
		"white label superuser": {profile: v1alpha1.SQLIdentityWhiteLabel, user: "postgres", rotated: "-superuser"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			branch := parentBranch(withParentSQLIdentity(tc.profile))
			var secrets []client.Object
			for _, suffix := range []string{"-app", "-superuser"} {
				secrets = append(secrets, &corev1.Secret{
					Name:      branch.Name + suffix,
					Namespace: "xata-clusters",
					Type:      corev1.SecretTypeBasicAuth,
					Data:      map[string][]byte{corev1.BasicAuthPasswordKey: []byte("pass")},
				})
			}
			svc, k8sClient := setupTestClustersService(t, withExistingObjects(append(secrets, branch)...))

			_, err := svc.RotatePostgresClusterCredentials(ctx, &clustersv1.RotatePostgresClusterCredentialsRequest{
				Id:   branch.Name,
				User: tc.user,
			})
			require.Equal(t, tc.wantCode, status.Code(err), "%v", err)

			for _, suffix := range []string{"-app", "-superuser"} {
				secret := &corev1.Secret{}
				require.NoError(t, k8sClient.Get(ctx, client.ObjectKey{Namespace: "xata-clusters", Name: branch.Name + suffix}, secret))
				password := string(secret.Data[corev1.BasicAuthPasswordKey])
				if suffix == tc.rotated {
					require.NotEqual(t, "pass", password, "secret %s", suffix)
				} else {
					require.Equal(t, "pass", password, "secret %s", suffix)
				}
			}
		})
	}
}

func merge(a, b map[string]string) map[string]string {
	out := make(map[string]string, len(a)+len(b))
	maps.Copy(out, a)
	maps.Copy(out, b)
	return out
}

func TestApplyProfileDefaults(t *testing.T) {
	t.Parallel()

	whiteLabel := shared.SQLIdentityFor(v1alpha1.SQLIdentityWhiteLabel)
	pgCron := []string{"pg_cron"}

	tests := map[string]struct {
		id      shared.SQLIdentity
		preload []string
		params  map[string]string
		want    map[string]string
	}{
		"no value takes the database of the profile": {
			id: whiteLabel, preload: pgCron,
			params: map[string]string{},
			want:   map[string]string{"cron.database_name": "defaultdb"},
		},
		"the legacy default takes the database of the profile": {
			id: whiteLabel, preload: pgCron,
			params: map[string]string{"cron.database_name": "xata"},
			want:   map[string]string{"cron.database_name": "defaultdb"},
		},
		"another value stays": {
			id: whiteLabel, preload: pgCron,
			params: map[string]string{"cron.database_name": "jobs"},
			want:   map[string]string{"cron.database_name": "jobs"},
		},
		"a value without its library is removed": {
			id:     whiteLabel,
			params: map[string]string{"cron.database_name": "jobs"},
			want:   map[string]string{},
		},
		"legacy does not change": {
			id:     shared.LegacySQLIdentity(),
			params: map[string]string{"cron.database_name": "jobs"},
			want:   map[string]string{"cron.database_name": "jobs"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			applyProfileDefaults(tc.params, tc.preload, tc.id)
			require.Equal(t, tc.want, tc.params)
		})
	}
}
