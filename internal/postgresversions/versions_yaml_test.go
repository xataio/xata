package postgresversions

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestYAMLLoading(t *testing.T) {
	// Test that PostgreSQL versions are loaded from embedded YAML
	versions := GetVersions()
	if versions == nil {
		t.Fatal("GetVersions() returned nil")
	}

	// Test basic structure is loaded
	if len(versions.Sources) == 0 {
		t.Fatal("No sources loaded from YAML")
	}

	if versions.LastUpdated == "" {
		t.Error("LastUpdated field should not be empty")
	}

	// Test first source has expected structure
	firstSource := versions.Sources[0]
	if firstSource.Source == "" {
		t.Error("First source URL should not be empty")
	}

	if len(firstSource.MajorVersions) == 0 {
		t.Fatal("No major versions loaded from first source")
	}

	// Test specific major versions exist in first source (based on your YAML file)
	expectedMajorVersions := []string{"17"}
	for _, expectedMajor := range expectedMajorVersions {
		majorVersion, exists := firstSource.MajorVersions[expectedMajor]
		if !exists {
			t.Errorf("Expected major version %s not found in first source", expectedMajor)
			continue
		}

		// Test major version structure
		if len(majorVersion.Versions) == 0 {
			t.Errorf("Major version %s should have versions list", expectedMajor)
		}

		if majorVersion.Latest == "" {
			t.Errorf("Major version %s should have latest version", expectedMajor)
		}

		// Test that latest version is in the versions list
		latestFound := slices.Contains(majorVersion.Versions, majorVersion.Latest)
		if !latestFound {
			t.Errorf("Latest version %s not found in versions list for major %s",
				majorVersion.Latest, expectedMajor)
		}
	}

	// Test first source URL is correctly loaded
	expectedSource := "ghcr.io/xataio/postgres-images/cnpg-postgres-plus"
	if firstSource.Source != expectedSource {
		t.Errorf("First source URL: expected %s, got %s", expectedSource, firstSource.Source)
	}

	// Test package URL is loaded for first source (if present)
	if firstSource.PackageURL != "" {
		expectedPackageURL := "https://github.com/xataio/postgres-images/pkgs/container/postgres-images%2Fcnpg-postgres-plus"
		if firstSource.PackageURL != expectedPackageURL {
			t.Errorf("First source Package URL: expected %s, got %s", expectedPackageURL, firstSource.PackageURL)
		}
	}

	// Test updated_by field
	if versions.UpdatedBy == "" {
		t.Error("UpdatedBy field should not be empty")
	}
}

func TestVersionsYAMLIntegrity(t *testing.T) {
	// Test that the loaded versions pass basic integrity checks
	versions := GetVersions()

	// Test that all versions are semantically valid across all sources
	for _, source := range versions.Sources {
		for majorName, majorVersion := range source.MajorVersions {
			// Test version format (should start with the major number)
			for _, version := range majorVersion.Versions {
				if len(version) < 2 {
					t.Errorf("Version %s in major %s is too short", version, majorName)
					continue
				}

				// Basic format check - should start with major version number
				if version[0:len(majorName)] != majorName {
					t.Errorf("Version %s should start with major version %s", version, majorName)
				}
			}

			// Test that latest version is valid
			if majorVersion.Latest == "" {
				t.Errorf("Major version %s has empty latest version", majorName)
			}

			// Test that versions list is not empty for supported versions
			if majorVersion.Supported && len(majorVersion.Versions) == 0 {
				t.Errorf("Supported major version %s has no versions", majorName)
			}
		}
	}

	// Test that package functions work with loaded data
	allVersions := GetAllVersions()
	if len(allVersions) == 0 {
		t.Error("GetAllVersions() returned empty list")
	}

	supportedMajors := GetSupportedMajorVersions()
	if len(supportedMajors) == 0 {
		t.Error("GetSupportedMajorVersions() returned empty map")
	}

	// Test that BuildImageURL works with version strings
	imageURL := BuildImageURL("postgres:17.6")
	if imageURL == "" {
		t.Error("BuildImageURL() returned empty string")
	}

	// Test the structure is correct (registry/path/imageName:version)
	expectedRegistryPath := "ghcr.io/xataio/postgres-images"
	if !strings.HasPrefix(imageURL, expectedRegistryPath+"/") {
		t.Errorf("BuildImageURL() should start with %s/, got %s", expectedRegistryPath, imageURL)
	}

	// Test additional functions
	lastUpdated := GetLastUpdated()
	if lastUpdated == "" {
		t.Error("GetLastUpdated() returned empty string")
	}

	updatedBy := GetUpdatedBy()
	if updatedBy == "" {
		t.Error("GetUpdatedBy() returned empty string")
	}

	sources := GetSources()
	if len(sources) == 0 {
		t.Error("GetSources() returned empty slice")
	}
}

func TestVersionsYAMLConsistency(t *testing.T) {
	// Test that the YAML data is internally consistent
	versions := GetVersions()

	// Test that all versions returned by GetAllVersions() are valid
	allVersions := GetAllVersions()
	for _, version := range allVersions {
		if !IsVersionAvailable(version) {
			t.Errorf("Version %s from GetAllVersions() is not available according to IsVersionAvailable()", version)
		}
	}

	// Test that GetVersionsForMajor() returns consistent results across all sources
	for _, source := range versions.Sources {
		for majorName := range source.MajorVersions {
			majorVersions := GetVersionsForMajor(majorName)
			latestForMajor := GetLatestForMajor(majorName)

			if len(majorVersions) > 0 && latestForMajor == "" {
				t.Errorf("Major %s has versions but no latest version", majorName)
			}

			if latestForMajor != "" {
				// Test that latest for major is in the versions list
				found := slices.Contains(majorVersions, latestForMajor)
				if !found {
					t.Errorf("Latest version %s for major %s not found in versions list",
						latestForMajor, majorName)
				}
			}
		}
	}

	// Test that supported major versions are consistent
	supportedMajors := GetSupportedMajorVersions()
	for imageName, majors := range supportedMajors {
		if len(majors) == 0 {
			t.Errorf("Image %s has no supported major versions", imageName)
		}
	}

	// Test ValidateVersion function
	for _, version := range allVersions {
		if err := ValidateVersion(version); err != nil {
			t.Errorf("ValidateVersion(%s) returned error: %v", version, err)
		}
	}

	// Test ValidateVersion with invalid version
	if err := ValidateVersion("99.99"); err == nil {
		t.Error("ValidateVersion should return error for invalid version")
	}
}

func TestHiddenImageNames(t *testing.T) {
	hidden := HiddenImageNames()
	hiddenSet := make(map[string]bool, len(hidden))
	for _, name := range hidden {
		hiddenSet[name] = true
	}

	// Hidden images must still be valid images
	allImageNames := GetAllImageNames()
	for _, name := range hidden {
		require.Contains(t, allImageNames, name, "hidden image %s not found in GetAllImageNames()", name)
	}

	// For every show_only_latest major, all minors except the latest must be
	// hidden; everything else must be visible
	for _, source := range GetSources() {
		imageName := getDisplayName(source.Source[strings.LastIndex(source.Source, "/")+1:])
		for majorName, major := range source.MajorVersions {
			for _, version := range major.Versions {
				name := imageName + ":" + version
				wantHidden := major.Supported && major.ShowOnlyLatest && version != major.Latest
				require.Equal(t, wantHidden, hiddenSet[name],
					"HiddenImageNames(): image %s (major %s) hidden", name, majorName)
			}
		}
	}

	// Guard the current default-visibility policy: for the postgres offering,
	// only the latest minor of each major is visible by default
	for _, source := range GetSources() {
		if sourceDisplayName(source.Source) != Postgres {
			continue
		}
		for majorName, major := range source.MajorVersions {
			require.True(t, major.ShowOnlyLatest,
				"source %s major %s: show_only_latest", source.Source, majorName)
		}
	}
}

func TestHiddenMajorImages(t *testing.T) {
	hidden := HiddenMajorImages()

	// Hidden images must still be valid images
	allImageNames := GetAllImageNames()
	for name := range hidden {
		require.Contains(t, allImageNames, name, "hidden image %s not found in GetAllImageNames()", name)
	}

	// Every minor of a hidden major must be hidden, and nothing else
	for _, source := range GetSources() {
		imageName := getDisplayName(source.Source[strings.LastIndex(source.Source, "/")+1:])
		for majorName, major := range source.MajorVersions {
			for _, version := range major.Versions {
				name := imageName + ":" + version
				gotMajor, gotHidden := hidden[name]
				wantHidden := major.Supported && major.Hidden
				require.Equal(t, wantHidden, gotHidden,
					"HiddenMajorImages(): image %s (major %s) hidden", name, majorName)
				if wantHidden {
					require.Equal(t, majorName, gotMajor, "HiddenMajorImages(): image %s major", name)
				}
			}
		}
	}

	// Guard the current default-visibility policy: PG 14 and 15 are hidden.
	for _, source := range GetSources() {
		for majorName, major := range source.MajorVersions {
			if majorName == "14" || majorName == "15" {
				require.True(t, major.Hidden,
					"source %s major %s: hidden", source.Source, majorName)
			}
		}
	}
}

func TestImageNameFunctions(t *testing.T) {
	// Test GetAllImageNames function
	allImageNames := GetAllImageNames()
	if len(allImageNames) == 0 {
		t.Error("GetAllImageNames() returned empty list")
	}

	// Verify all image names have correct format (image:version)
	for _, imageName := range allImageNames {
		if !strings.Contains(imageName, ":") {
			t.Errorf("Image name %s should contain ':'", imageName)
		}
	}

	// Test BuildImageURL with postgres image names (postgres gets converted to cnpg-postgres-plus)
	for _, imageName := range allImageNames {
		if strings.HasPrefix(imageName, "postgres:") {
			fullURL := BuildImageURL(imageName)
			expectedPrefix := "ghcr.io/xataio/postgres-images/"
			if !strings.HasPrefix(fullURL, expectedPrefix) {
				t.Errorf("BuildImageURL(%s) should start with %s, got %s", imageName, expectedPrefix, fullURL)
			}

			// The URL should contain cnpg-postgres-plus, not postgres
			expectedSuffix := strings.Replace(imageName, "postgres:", "cnpg-postgres-plus:", 1)
			if !strings.HasSuffix(fullURL, expectedSuffix) {
				t.Errorf("BuildImageURL(%s) should end with %s, got %s", imageName, expectedSuffix, fullURL)
			}
		}
	}
}

func TestShortImageName(t *testing.T) {
	testCases := []struct {
		fullImageName string
		expected      string
	}{
		{"ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5", "postgres:17.5"},
		{"ghcr.io/xataio/postgres-images/cnpg-postgres-plus:16.3", "postgres:16.3"},
		{"cnpg-postgres-plus:17.6", "postgres:17.6"},
		{"postgres:17.5", "postgres:17.5"},
		{"some-other-image:1.0", "some-other-image:1.0"},
		{"registry.example.com/my-image:latest", "my-image:latest"},
		// xata-analytics -> analytics mapping
		{"ghcr.io/xataio/postgres-images/xata-analytics:17.7", "analytics:17.7"},
		{"ghcr.io/xataio/postgres-images/xata-analytics:18.1", "analytics:18.1"},
		{"xata-analytics:17.7", "analytics:17.7"},
		{"localhost:5000/pg/cnpg-postgres-plus:18.7-b1@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "postgres:18.7-b1"},
	}

	for _, tc := range testCases {
		result := ShortImageName(tc.fullImageName)
		if result != tc.expected {
			t.Errorf("ShortImageName(%s): expected %s, got %s", tc.fullImageName, tc.expected, result)
		}
	}
}

func TestGetMajorForVersion(t *testing.T) {
	testCases := []struct {
		version       string
		expectedMajor string
	}{
		{"17.6", "17"},
		{"17.5", "17"},
		{"18", "18"},
		{"18rc1", "18"},
		{"19beta3-trixie", "19"},
		{"16.4", "16"},
		{"", ""},
		{"17", "17"},         // no dot case
		{"latest", "latest"}, // no leading digits
		{"v1.2", "v1"},       // no leading digits, text before the dot
	}

	for _, tc := range testCases {
		major := GetMajorForVersion(tc.version)
		if major != tc.expectedMajor {
			t.Errorf("GetMajorForVersion(%s): expected %s, got %s", tc.version, tc.expectedMajor, major)
		}
	}
}

func TestMultipleSources(t *testing.T) {
	versions := GetVersions()

	// Test that we have multiple sources
	if len(versions.Sources) < 2 {
		t.Skip("Test requires multiple sources in versions.yaml")
	}

	// Test GetSupportedMajorVersions returns entries for each source
	supportedMajors := GetSupportedMajorVersions()
	if len(supportedMajors) != len(versions.Sources) {
		t.Errorf("GetSupportedMajorVersions() should have %d entries, got %d",
			len(versions.Sources), len(supportedMajors))
	}

	// Test that GetAllImageNames includes images from all sources
	allImageNames := GetAllImageNames()
	imageNamePrefixes := make(map[string]bool)
	for _, name := range allImageNames {
		colonIndex := strings.Index(name, ":")
		if colonIndex > 0 {
			imageNamePrefixes[name[:colonIndex]] = true
		}
	}

	// Should have more than one image name prefix if there are multiple sources
	if len(imageNamePrefixes) < 2 && len(versions.Sources) >= 2 {
		t.Errorf("GetAllImageNames() should include images from multiple sources, found prefixes: %v", imageNamePrefixes)
	}
}
