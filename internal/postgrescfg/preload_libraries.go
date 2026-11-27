package postgrescfg

import (
	"fmt"
	"strings"

	"xata/internal/extensions"
	"xata/internal/postgresversions"
)

// defaultPreloadLibraries maps offering names to their default preload libraries
var defaultPreloadLibraries = map[string][]string{
	"postgres": {
		"pg_stat_statements",
		"auto_explain",
	},
	"experimental": {
		"pg_stat_statements",
		"auto_explain",
	},
	"analytics": {
		"pg_stat_statements",
		"auto_explain",
		"pg_duckdb",
	},
}

var internalPreloadLibraries = []string{
	"xatautils",
}

// GetDefaultPreloadLibraries returns the list of preload libraries that are enabled by default
// for the given image. Accepts both full image URLs (e.g.,
// "ghcr.io/xataio/postgres-images/xata-analytics:17.5") and short formats (e.g., "analytics:17").
// Returns an error if the offering is not recognized.
func GetDefaultPreloadLibraries(image string) ([]string, error) {
	shortImage := postgresversions.ShortImageName(image)

	// Extract offering from short image (format: "offering:version")
	offering := "postgres"
	if parts := strings.Split(shortImage, ":"); len(parts) >= 1 {
		offering = parts[0]
	}

	libs, ok := defaultPreloadLibraries[offering]
	if !ok {
		return nil, fmt.Errorf("unknown offering: %s", offering)
	}

	// Return a copy to prevent modification of the original slice
	return append([]string{}, libs...), nil
}

// ValidatePreloadLibraries validates that all libraries in the provided slice are valid
// for the given image (i.e., they exist as extensions with preload_required=true).
// Accepts both full image URLs (e.g., "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5")
// and short formats (e.g., "postgres:17").
func ValidatePreloadLibraries(image string, libraries []string) error {
	shortImage := postgresversions.ShortImageName(image)
	preloadExtensions := extensions.GetPreloadRequiredExtensions(shortImage)
	availableExtensions := make(map[string]bool)
	for _, ext := range preloadExtensions {
		availableExtensions[ext.Name] = true
	}

	for _, lib := range libraries {
		if !availableExtensions[lib] {
			return fmt.Errorf("invalid preload library: %s", lib)
		}
	}
	return nil
}

func GetInternalPreloadLibraries() []string {
	return append([]string{}, internalPreloadLibraries...)
}

func FilterOutInternalPreloadLibraries(libraries []string) []string {
	// pgprivs takes the place of xatautils on some branches.
	internalSet := map[string]struct{}{"pgprivs": {}}
	for _, lib := range internalPreloadLibraries {
		internalSet[lib] = struct{}{}
	}

	var filtered []string
	for _, lib := range libraries {
		if _, isInternal := internalSet[lib]; !isInternal {
			filtered = append(filtered, lib)
		}
	}
	return filtered
}
