package clusters

import (
	"maps"
	"slices"
	"strings"

	"xata/internal/extensions"
	"xata/internal/postgresversions"
	"xata/services/branch-operator/pkg/shared"
)

// MandatoryPostgresParameters are the Postgres parameters that must always be
// set on a Cluster - the user is not permitted to omit or override them. The
// security library settings use the prefix and names of the SQL identity.
func MandatoryPostgresParameters(image string, id shared.SQLIdentity) map[string]string {
	p := id.UtilsSettingPrefix()
	return map[string]string{
		"lc_messages": "C.utf8",
		"lc_monetary": "C.utf8",
		"lc_numeric":  "C.utf8",
		"lc_time":     "C.utf8",

		"pg_stat_statements.track_utility": "off",

		"recovery_init_sync_method": "syncfs",

		p + "privileged_role": id.Login,

		p + "privileged_extensions": privilegedExtensions(image, id),

		p + "extension_custom_scripts_path": id.UtilsScriptsPath(),

		p + "reserved_memberships": id.PrivilegeGroup + ", pg_read_server_files, pg_write_server_files, pg_execute_server_program",

		p + "reserved_roles": "cnpg_pooler_pgbouncer, pgbouncer, postgres, streaming_replica, " + id.PrivilegeGroup,

		p + "privileged_role_allowed_configs": "auth_delay.*, auto_explain.*, log_lock_waits, log_min_duration_statement, log_min_messages, log_replication_commands, log_statement, log_temp_files, pg_net.batch_size, pg_net.ttl, pg_stat_statements.*, pgaudit.log, pgaudit.log_catalog, pgaudit.log_client, pgaudit.log_level, pgaudit.log_relation, pgaudit.log_rows, pgaudit.log_statement, pgaudit.log_statement_once, pgaudit.role, pgrst.*, plan_filter.*, safeupdate.enabled, session_replication_role, track_io_timing, wal_compression",
	}
}

// applyPostgresParameterOverrides sets the mandatory parameters and the
// profile defaults of id. For a non-legacy id it first removes the settings
// of every security library from params.
func applyPostgresParameterOverrides(params map[string]string, image string, preloadLibraries []string, id shared.SQLIdentity) {
	if id != shared.LegacySQLIdentity {
		maps.DeleteFunc(params, func(k, _ string) bool { return isUtilsSetting(k) })
	}
	maps.Copy(params, MandatoryPostgresParameters(image, id))
	applyProfileDefaults(params, preloadLibraries, id)
}

// isUtilsSetting reports whether name is a setting of a security library. GUC
// names are case-insensitive.
func isUtilsSetting(name string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	for _, lib := range shared.UtilsLibraries {
		if strings.HasPrefix(name, lib+".") {
			return true
		}
	}
	return false
}

// profileDefaults are extension settings whose default value is a legacy SQL
// name.
var profileDefaults = []struct {
	library string
	setting string
	value   func(shared.SQLIdentity) string
}{
	{"pg_cron", "cron.database_name", func(id shared.SQLIdentity) string { return id.Database }},
	{"pg_duckdb", "duckdb.postgres_role", func(id shared.SQLIdentity) string { return id.Login }},
}

// applyProfileDefaults sets profileDefaults to the names of a non-legacy id
// when they have no value, or removes them when their library is not
// preloaded. Legacy Branches keep their values.
//
// Projects fills in the default from its parameter catalog, which is the
// legacy name, when a library is added to the preload list. That value is
// thus taken as no value, and a caller cannot ask for the legacy name on
// purpose. Remove this special case when Projects sends the default of the
// Branch.
func applyProfileDefaults(params map[string]string, preloadLibraries []string, id shared.SQLIdentity) {
	if id == shared.LegacySQLIdentity {
		return
	}
	for _, d := range profileDefaults {
		if !slices.Contains(preloadLibraries, d.library) {
			delete(params, d.setting)
			continue
		}
		if v, ok := params[d.setting]; !ok || v == d.value(shared.LegacySQLIdentity) {
			params[d.setting] = d.value(id)
		}
	}
}

// privilegedExtensions returns a comma-separated, alphabetically sorted list of
// extension names for the given image, without the security libraries of
// other profiles.
func privilegedExtensions(image string, id shared.SQLIdentity) string {
	shortImage := postgresversions.ShortImageName(image)
	exts := extensions.GetExtensions(shortImage)
	extNames := make([]string, 0, len(exts))
	for _, ext := range exts {
		if ext.Name != id.UtilsLibrary && slices.Contains(shared.UtilsLibraries, ext.Name) {
			continue
		}
		extNames = append(extNames, ext.Name)
	}
	slices.Sort(extNames)
	return strings.Join(extNames, ", ")
}
