package shared

import (
	apiv1 "github.com/xataio/xata-cnpg/api/v1"
	apiv1ac "github.com/xataio/xata-cnpg/pkg/client/applyconfiguration/api/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"

	"xata/services/branch-operator/api/v1alpha1"
)

// SQLIdentity holds the SQL names that a Branch exposes to the tenant.
type SQLIdentity struct {
	// Login is the application role. It owns Database.
	Login string
	// Database is the application database.
	Database string
	// PrivilegeGroup is a NOLOGIN, NOSUPERUSER role granted to Login.
	PrivilegeGroup string
	// UtilsLibrary is the security library in shared_preload_libraries. It is
	// also the prefix of its settings.
	UtilsLibrary string
}

// UtilsSettingPrefix returns the prefix of the security library settings.
func (id SQLIdentity) UtilsSettingPrefix() string {
	return id.UtilsLibrary + "."
}

// UtilsScriptsPath returns the directory of the extension custom scripts.
func (id SQLIdentity) UtilsScriptsPath() string {
	return "/etc/" + id.UtilsLibrary + "/extensions"
}

// LegacySQLIdentity returns the SQL names of a Branch without a SQL identity
// profile.
func LegacySQLIdentity() SQLIdentity {
	return SQLIdentity{
		Login:          "xata",
		Database:       "xata",
		PrivilegeGroup: "xata_superuser",
		UtilsLibrary:   "xatautils",
	}
}

// WhiteLabelSQLIdentity returns the SQL names of a Branch with the WhiteLabel
// profile.
func WhiteLabelSQLIdentity() SQLIdentity {
	return SQLIdentity{
		Login:          "dbadmin",
		Database:       "defaultdb",
		PrivilegeGroup: "db_superuser",
		UtilsLibrary:   "pgprivs",
	}
}

// IsUtilsLibrary reports whether name is the security library of a profile.
func IsUtilsLibrary(name string) bool {
	return name == LegacySQLIdentity().UtilsLibrary || name == WhiteLabelSQLIdentity().UtilsLibrary
}

// SQLIdentityFor returns the SQL names for the given profile. The Branch CRD
// accepts only known profiles.
func SQLIdentityFor(p v1alpha1.SQLIdentityProfile) SQLIdentity {
	if p == v1alpha1.SQLIdentityWhiteLabel {
		return WhiteLabelSQLIdentity()
	}
	return LegacySQLIdentity()
}

// BranchSQLIdentity returns the SQL names stored on the Branch.
func BranchSQLIdentity(b *v1alpha1.Branch) SQLIdentity {
	return SQLIdentityFor(b.Spec.SQLIdentity)
}

// AppRoleConfiguration returns the managed role configuration for the
// application role. CNPG reconciles the role's password from the referenced
// `{branchName}-app` secret, which is the only mechanism that syncs the
// password onto Clusters taken from a pool (where `bootstrap.initdb.secret`
// never runs). The attributes match those set by the initdb post-init SQL.
func AppRoleConfiguration(branchName string, id SQLIdentity) *apiv1ac.RoleConfigurationApplyConfiguration {
	return apiv1ac.RoleConfiguration().
		WithName(id.Login).
		WithEnsure(apiv1.EnsurePresent).
		WithPasswordSecret(corev1ac.LocalObjectReference().
			WithName(branchName + "-app")).
		WithLogin(true).
		WithInherit(true).
		WithCreateDB(true).
		WithCreateRole(true).
		WithBypassRLS(true).
		WithReplication(true).
		WithInRoles(id.PrivilegeGroup)
}
