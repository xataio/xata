package flags

import "xata/internal/openfeature"

var (
	OrganizationCreation = openfeature.FeatureFlag{
		Name:           "organizationCreation",
		DefaultEnabled: true,
	}
	// WARNING: Feature Flags should have positive names. Avoid disabled suffix in future
	BranchCreationDisabled = openfeature.FeatureFlag{
		Name:           "branchCreationDisabled",
		DefaultEnabled: false,
	}
	ChildBranchCreationDisabled = openfeature.FeatureFlag{
		Name:           "childBranchCreationDisabled",
		DefaultEnabled: false,
	}
	// ExperimentalImages flag to enable experimental PostgreSQL images (for internal users)
	ExperimentalImages = openfeature.FeatureFlag{
		Name:           "experimentalImages",
		DefaultEnabled: false,
	}
	// AnalyticsImages flag to enable analytics PostgreSQL images
	AnalyticsImages = openfeature.FeatureFlag{
		Name:           "analyticsImages",
		DefaultEnabled: false,
	}
	// LegacyPgVersions flag to enable older PostgreSQL minor versions that
	// are hidden by default (see show_only_latest in versions.yaml)
	LegacyPgVersions = openfeature.FeatureFlag{
		Name:           "legacyPgVersions",
		DefaultEnabled: false,
	}
	UseXatastor = openfeature.FeatureFlag{
		Name:           "useXatastor",
		DefaultEnabled: false,
	}
	// OrganizationViewerRole lets Viewer be granted and reported; without it a Viewer is reported as Editor.
	OrganizationViewerRole = openfeature.FeatureFlag{
		Name:           "organizationViewerRole",
		DefaultEnabled: false,
	}

	// EnterpriseSSO gates per-organization federated single sign-on.
	EnterpriseSSO = openfeature.FeatureFlag{
		Name:           "enterpriseSSO",
		DefaultEnabled: false,
	}

	// SkipChildBranchBackups creates child branches (mode=inherit) without
	// backups. Evaluate it under openfeature.WithBranchScope so it can target
	// the organizationId and projectId properties.
	SkipChildBranchBackups = openfeature.FeatureFlag{
		Name:           "skipChildBranchBackups",
		DefaultEnabled: false,
	}
	// WARNING: Feature Flags should have positive names. Avoid disabled suffix in future
)

// PgMajorFlag returns the feature flag that enables a hidden PostgreSQL major
// version (see hidden in versions.yaml) for an organization, e.g. "pgMajor14"
// for major "14". If the OpenFeature provider has no flag with this name, it
// evaluates to false and the major stays hidden.
func PgMajorFlag(major string) openfeature.FeatureFlag {
	return openfeature.FeatureFlag{
		Name:           "pgMajor" + major,
		DefaultEnabled: false,
	}
}
