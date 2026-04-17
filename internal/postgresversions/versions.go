package postgresversions

import (
	"embed"
	"fmt"
	"log"
	"slices"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

//go:embed versions.yaml
var versionsFS embed.FS

const (
	Postgres      = "postgres"
	CNPGPostgres  = "cnpg-postgres-plus"
	Analytics     = "analytics"
	XataAnalytics = "xata-analytics"
)

// imageDisplayNames maps source image names to their display names
var imageDisplayNames = map[string]string{
	CNPGPostgres:  Postgres,
	XataAnalytics: Analytics,
}

// imageSourceNames is the reverse mapping: display names to source names
var imageSourceNames = map[string]string{
	Postgres:  CNPGPostgres,
	Analytics: XataAnalytics,
}

// getDisplayName returns the display name for a source image name
func getDisplayName(sourceImageName string) string {
	if displayName, ok := imageDisplayNames[sourceImageName]; ok {
		return displayName
	}
	return sourceImageName
}

// getSourceName returns the source image name for a display name
func getSourceName(displayName string) string {
	if sourceName, ok := imageSourceNames[displayName]; ok {
		return sourceName
	}
	return displayName
}

// PostgreSQLVersions represents the structure of the versions config
type PostgreSQLVersions struct {
	LastUpdated string   `yaml:"last_updated"`
	UpdatedBy   string   `yaml:"updated_by,omitempty"`
	Sources     []Source `yaml:"sources"`
}

// Source represents a container registry source with its available versions
type Source struct {
	Source        string                  `yaml:"source"`
	PackageURL    string                  `yaml:"package_url,omitempty"`
	MajorVersions map[string]MajorVersion `yaml:"major_versions"`
}

// MajorVersion represents a PostgreSQL major version and its available minor versions
type MajorVersion struct {
	Versions  []string `yaml:"versions"`  // ["16.3", "16.4"] or ["17.5", "17.6"]
	Latest    string   `yaml:"latest"`    // "16.4" or "17.6"
	Supported bool     `yaml:"supported"` // true/false
}

// ImageVersion represents a PostgreSQL image information (offering, major,
// minor)
type ImageVersion struct {
	Offering string `yaml:"offering"`
	Major    int    `yaml:"major"`
	Minor    int    `yaml:"minor"`
}

var postgresVersions *PostgreSQLVersions

// init loads the versions from the embedded YAML file
func init() {
	if err := loadVersions(); err != nil {
		log.Fatalf("Critical: failed to load PostgreSQL versions configuration: %v", err)
	}
}

// loadVersions loads the versions from the embedded YAML file
func loadVersions() error {
	yamlData, err := versionsFS.ReadFile("versions.yaml")
	if err != nil {
		return fmt.Errorf("failed to read embedded YAML file: %w", err)
	}

	if len(yamlData) == 0 {
		return fmt.Errorf("versions.yaml file is empty")
	}

	var versions PostgreSQLVersions
	if err := yaml.Unmarshal(yamlData, &versions); err != nil {
		return fmt.Errorf("failed to unmarshal YAML: %w", err)
	}

	if len(versions.Sources) == 0 {
		return fmt.Errorf("no image sources found in versions.yaml")
	}

	for i, source := range versions.Sources {
		if source.Source == "" {
			return fmt.Errorf("source field is required for source %d in versions.yaml", i)
		}
		if len(source.MajorVersions) == 0 {
			return fmt.Errorf("no version definitions found for source %s in versions.yaml", source.Source)
		}
	}

	postgresVersions = &versions
	return nil
}

// GetVersions returns the complete PostgreSQL version information
func GetVersions() *PostgreSQLVersions {
	return postgresVersions
}

// GetAllVersions returns all available PostgreSQL versions across all sources and major versions
func GetAllVersions() []string {
	seen := make(map[string]bool)
	var allVersions []string

	for _, source := range postgresVersions.Sources {
		for _, major := range source.MajorVersions {
			if major.Supported {
				for _, version := range major.Versions {
					if !seen[version] {
						seen[version] = true
						allVersions = append(allVersions, version)
					}
				}
			}
		}
	}

	return allVersions
}

// GetVersionsForMajor returns versions for a specific major version (e.g., "16", "17") across all sources
func GetVersionsForMajor(major string) []string {
	seen := make(map[string]bool)
	var versions []string

	for _, source := range postgresVersions.Sources {
		if majorVersion, exists := source.MajorVersions[major]; exists && majorVersion.Supported {
			for _, version := range majorVersion.Versions {
				if !seen[version] {
					seen[version] = true
					versions = append(versions, version)
				}
			}
		}
	}

	return versions
}

// GetLatestForMajor returns the latest version for a specific major version across all sources
func GetLatestForMajor(major string) string {
	var latestVersion string

	for _, source := range postgresVersions.Sources {
		if majorVersion, exists := source.MajorVersions[major]; exists && majorVersion.Supported {
			if latestVersion == "" || majorVersion.Latest > latestVersion {
				latestVersion = majorVersion.Latest
			}
		}
	}

	return latestVersion
}

// GetSupportedMajorVersions returns a map of image name to supported major versions
func GetSupportedMajorVersions() map[string][]string {
	result := make(map[string][]string)

	for _, source := range postgresVersions.Sources {
		// Extract image name from source URL
		lastSlashIndex := strings.LastIndex(source.Source, "/")
		var imageName string
		if lastSlashIndex == -1 {
			imageName = source.Source
		} else {
			imageName = source.Source[lastSlashIndex+1:]
		}
		// Convert source name to display name
		imageName = getDisplayName(imageName)

		var majors []string
		for major, info := range source.MajorVersions {
			if info.Supported {
				majors = append(majors, major)
			}
		}
		result[imageName] = majors
	}

	return result
}

// IsVersionAvailable checks if a specific version is available
func IsVersionAvailable(version string) bool {
	allVersions := GetAllVersions()
	return slices.Contains(allVersions, version)
}

// GetSources returns all container registry sources
func GetSources() []Source {
	return postgresVersions.Sources
}

// BuildImageURL constructs the full container image URL for a given image name
// e.g., for imageName "postgres:17.6" returns "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.6"
// e.g., for imageName "experimental:17.7" returns "ghcr.io/xataio/postgres-images/experimental:17.7"
func BuildImageURL(imageName string) string {
	// Extract the image name part (before the colon)
	before, _, ok := strings.Cut(imageName, ":")
	var imageNamePart string
	if !ok {
		imageNamePart = imageName
	} else {
		imageNamePart = before
	}

	// Find the source that matches this image name
	for _, source := range postgresVersions.Sources {
		lastSlashIndex := strings.LastIndex(source.Source, "/")
		var sourceImageName string
		if lastSlashIndex == -1 {
			sourceImageName = source.Source
		} else {
			sourceImageName = source.Source[lastSlashIndex+1:]
		}

		// Check if this source matches (considering display name -> source name conversion)
		if sourceImageName == imageNamePart || sourceImageName == getSourceName(imageNamePart) {
			registryPath := source.Source[:lastSlashIndex]
			convertedImageName := convertImageName(imageName)
			return fmt.Sprintf("%s/%s", registryPath, convertedImageName)
		}
	}

	// Fallback: return imageName as-is if no matching source found
	return imageName
}

// convertImageName converts display names to source names
// e.g., "postgres:17.6" -> "cnpg-postgres-plus:17.6"
// e.g., "analytics:17.7" -> "xata-analytics:17.7"
func convertImageName(imageName string) string {
	before, after, ok := strings.Cut(imageName, ":")
	if !ok {
		return getSourceName(imageName)
	}

	imageNamePart := before
	tag := after
	return fmt.Sprintf("%s:%s", getSourceName(imageNamePart), tag)
}

// ShortImageName extracts the image name and tag from a full image path.
// e.g., "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5" -> "postgres:17.5"
// e.g., "ghcr.io/xataio/postgres-images/xata-analytics:17.7" -> "analytics:17.7"
func ShortImageName(fullImageName string) string {
	// Find the last "/" to extract the image name with tag
	lastSlashIndex := strings.LastIndex(fullImageName, "/")
	var imageWithTag string
	if lastSlashIndex == -1 {
		imageWithTag = fullImageName
	} else {
		imageWithTag = fullImageName[lastSlashIndex+1:]
	}

	// Convert source names to display names
	for sourceName, displayName := range imageDisplayNames {
		if strings.HasPrefix(imageWithTag, sourceName+":") {
			return displayName + imageWithTag[len(sourceName):]
		}
	}

	return imageWithTag
}

// ValidateVersion checks if a version is valid and supported
func ValidateVersion(version string) error {
	if !IsVersionAvailable(version) {
		return fmt.Errorf("version %s is not available. Available versions: %v", version, GetAllVersions())
	}
	return nil
}

// GetLastUpdated returns when the version information was last updated
func GetLastUpdated() string {
	return postgresVersions.LastUpdated
}

// GetUpdatedBy returns who/what updated the version information
func GetUpdatedBy() string {
	return postgresVersions.UpdatedBy
}

// GetMajorForVersion extracts and returns the major version from a version string
// e.g., "17.5" -> "17"
func GetMajorForVersion(version string) string {
	if version == "" {
		return ""
	}

	// Find the first dot to separate major from minor
	before, _, ok := strings.Cut(version, ".")
	if !ok {
		// If no dot found, assume the entire string is the major version
		return version
	}

	return before
}

// ParseImageVersion parses an image string. It checks it exists in the list of
// the image string can be the short version e.g "analytics:17.5" or the long version
// "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5-08092025"
// available images and returns the (offering, major, minor) information
func ParseImageVersion(image string) (*ImageVersion, error) {
	short := ShortImageName(image)

	offering, version, ok := strings.Cut(short, ":")
	if !ok {
		return nil, fmt.Errorf("invalid image format, expected 'offering:version'")
	}

	// Strip any date suffix (e.g., "17.5-08092025" -> "17.5")
	if dashIndex := strings.Index(version, "-"); dashIndex != -1 {
		version = version[:dashIndex]
	}

	majorStr, minorStr, ok := strings.Cut(version, ".")
	if !ok {
		return nil, fmt.Errorf("invalid version format, expected 'major.minor'")
	}

	major, err := strconv.Atoi(majorStr)
	if err != nil {
		return nil, fmt.Errorf("invalid major version %q: %w", majorStr, err)
	}

	minor, err := strconv.Atoi(minorStr)
	if err != nil {
		return nil, fmt.Errorf("invalid minor version %q: %w", minorStr, err)
	}

	return &ImageVersion{
		Offering: offering,
		Major:    major,
		Minor:    minor,
	}, nil
}

// ValidateImage validates an image string. It checks that the image exists in
// the list of available images e.g., "analytics:17.5" -> ("analytics", "17",
// nil) e.g., "postgres:16.3" -> ("postgres", "16", nil)
func ValidateImage(image string) error {
	parts := strings.Split(image, ":")
	if len(parts) != 2 {
		return fmt.Errorf("invalid image format, expected 'offering:version'")
	}

	// Check if the image exists in available images
	validImages := GetAllImageNames()
	found := slices.Contains(validImages, image)
	if !found {
		return fmt.Errorf("image %s is not available", image)
	}

	return nil
}

// ExtractVersionFromImageName extracts the full version string from an image name
// e.g., "postgres:17.5" -> "17.5"
// e.g., "cnpg-postgres-plus:17.6" -> "17.6"
// e.g., "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5-08092025" -> "17.5"
func ExtractVersionFromImageName(imageName string) string {
	// Find the last colon to get the version part
	lastColonIndex := strings.LastIndex(imageName, ":")
	if lastColonIndex == -1 {
		return ""
	}

	versionPart := imageName[lastColonIndex+1:]

	// Strip any suffix after a dash (like "-08092025")
	if dashIndex := strings.Index(versionPart, "-"); dashIndex != -1 {
		versionPart = versionPart[:dashIndex]
	}

	return versionPart
}

// ExtractMajorVersionFromImage extracts the major version from a PostgreSQL image name
// e.g., "cnpg-postgres-plus:17.5" -> 17
// e.g., "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5-08092025" -> 17
func ExtractMajorVersionFromImage(imageName string) int {
	// Find the last colon to get the version part
	lastColonIndex := strings.LastIndex(imageName, ":")
	if lastColonIndex == -1 {
		return 0
	}

	versionPart := imageName[lastColonIndex+1:]

	if dashIndex := strings.Index(versionPart, "-"); dashIndex != -1 {
		versionPart = versionPart[:dashIndex]
	}

	// Extract major version
	// Handle different version formats:
	// - "17.5" -> "17"
	// - "18rc1" -> "18"
	// - "17" -> "17"

	// First, try to find a dot (for versions like "17.5")
	if dotIndex := strings.Index(versionPart, "."); dotIndex != -1 {
		versionPart = versionPart[:dotIndex]
	} else {
		// No dot found, check if it's an RC version (like "18rc1")
		// Extract only the numeric part at the beginning
		var numericPart strings.Builder
		for _, char := range versionPart {
			if char >= '0' && char <= '9' {
				numericPart.WriteRune(char)
			} else {
				break // Stop at first non-numeric character
			}
		}
		if numericPart.Len() > 0 {
			versionPart = numericPart.String()
		}
	}

	if majorVersion, err := strconv.Atoi(versionPart); err == nil {
		return majorVersion
	}

	return 0 // Return 0 if parsing fails
}

// GetAllImageNames returns all available PostgreSQL versions as image names with tags from all sources
// e.g., ["postgres:17.5", "postgres:17.6", "experimental:17.7", "analytics:17.7"]
func GetAllImageNames() []string {
	seen := make(map[string]bool)
	var allImageNames []string

	for _, source := range postgresVersions.Sources {
		// Extract the image name from the source URL
		lastSlashIndex := strings.LastIndex(source.Source, "/")
		var imageName string
		if lastSlashIndex == -1 {
			imageName = source.Source
		} else {
			imageName = source.Source[lastSlashIndex+1:]
		}
		// Convert source name to display name
		imageName = getDisplayName(imageName)

		// Build image names for all supported versions
		for _, major := range source.MajorVersions {
			if major.Supported {
				for _, version := range major.Versions {
					imageWithTag := fmt.Sprintf("%s:%s", imageName, version)
					if !seen[imageWithTag] {
						seen[imageWithTag] = true
						allImageNames = append(allImageNames, imageWithTag)
					}
				}
			}
		}
	}

	sort.Strings(allImageNames)
	return allImageNames
}
