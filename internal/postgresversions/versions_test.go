package postgresversions

import (
	"strings"
	"testing"
)

func TestExtractVersionFromImageName(t *testing.T) {
	testCases := []struct {
		name        string
		imageName   string
		expected    string
		description string
	}{
		{
			name:        "simple postgres image",
			imageName:   "postgres:17.5",
			expected:    "17.5",
			description: "Extract version from postgres image",
		},
		{
			name:        "cnpg image",
			imageName:   "cnpg-postgres-plus:17.6",
			expected:    "17.6",
			description: "Extract version from cnpg-postgres-plus image",
		},
		{
			name:        "full registry path",
			imageName:   "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
			expected:    "17.5",
			description: "Extract version from full registry path",
		},
		{
			name:        "full registry path with suffix",
			imageName:   "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5-08092025",
			expected:    "17.5",
			description: "Extract version from full registry path with date suffix",
		},
		{
			name:        "major version only",
			imageName:   "postgres:17",
			expected:    "17",
			description: "Extract version when only major version specified",
		},
		{
			name:        "pg 16",
			imageName:   "postgres:16.3",
			expected:    "16.3",
			description: "Extract PG 16 version",
		},
		{
			name:        "pg 18",
			imageName:   "postgres:18.0",
			expected:    "18.0",
			description: "Extract PG 18 version",
		},
		{
			name:        "no colon",
			imageName:   "postgres-latest",
			expected:    "",
			description: "Return empty string when no version tag found",
		},
		{
			name:        "experimental image",
			imageName:   "experimental:17.7",
			expected:    "17.7",
			description: "Extract version from experimental image",
		},
		{
			name:        "experimental full registry path",
			imageName:   "ghcr.io/xataio/postgres-images/experimental:17.7",
			expected:    "17.7",
			description: "Extract version from experimental full registry path",
		},
		{
			name:        "xata-analytics image",
			imageName:   "xata-analytics:18.1",
			expected:    "18.1",
			description: "Extract version from xata-analytics image",
		},
		{
			name:        "xata-analytics full registry path",
			imageName:   "ghcr.io/xataio/postgres-images/xata-analytics:17.7",
			expected:    "17.7",
			description: "Extract version from xata-analytics full registry path",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := ExtractVersionFromImageName(tc.imageName)
			if result != tc.expected {
				t.Errorf("ExtractVersionFromImageName(%q) = %q, want %q. %s",
					tc.imageName, result, tc.expected, tc.description)
			}
		})
	}
}

func TestExtractMajorVersionFromImage(t *testing.T) {
	testCases := []struct {
		name        string
		imageName   string
		expected    int
		description string
	}{
		{
			name:        "simple version",
			imageName:   "cnpg-postgres-plus:17.5",
			expected:    17,
			description: "Extract major version from simple image name",
		},
		{
			name:        "full registry path",
			imageName:   "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5-08092025",
			expected:    17,
			description: "Extract major version from full registry path with date suffix",
		},
		{
			name:        "version without minor",
			imageName:   "postgres:17",
			expected:    17,
			description: "Extract major version when no minor version specified",
		},
		{
			name:        "rc version",
			imageName:   "postgres:18rc1",
			expected:    18,
			description: "Extract major version from RC version",
		},
		{
			name:        "pg 14",
			imageName:   "cnpg-postgres-plus:14.10",
			expected:    14,
			description: "Extract PG 14 version",
		},
		{
			name:        "pg 15",
			imageName:   "cnpg-postgres-plus:15.4",
			expected:    15,
			description: "Extract PG 15 version",
		},
		{
			name:        "pg 16",
			imageName:   "cnpg-postgres-plus:16.2",
			expected:    16,
			description: "Extract PG 16 version",
		},
		{
			name:        "pg 18",
			imageName:   "cnpg-postgres-plus:18.0",
			expected:    18,
			description: "Extract PG 18 version",
		},
		{
			name:        "no colon",
			imageName:   "postgres-latest",
			expected:    0,
			description: "Return 0 when no version tag found",
		},
		{
			name:        "invalid version",
			imageName:   "postgres:invalid",
			expected:    0,
			description: "Return 0 when version cannot be parsed",
		},
		{
			name:        "experimental image",
			imageName:   "experimental:17.7",
			expected:    17,
			description: "Extract major version from experimental image",
		},
		{
			name:        "experimental full registry path",
			imageName:   "ghcr.io/xataio/postgres-images/experimental:17.7",
			expected:    17,
			description: "Extract major version from experimental full registry path",
		},
		{
			name:        "xata-analytics image",
			imageName:   "xata-analytics:18.1",
			expected:    18,
			description: "Extract major version from xata-analytics image",
		},
		{
			name:        "xata-analytics full registry path",
			imageName:   "ghcr.io/xataio/postgres-images/xata-analytics:17.7",
			expected:    17,
			description: "Extract major version from xata-analytics full registry path",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := ExtractMajorVersionFromImage(tc.imageName)
			if result != tc.expected {
				t.Errorf("ExtractMajorVersionFromImage(%q) = %d, want %d. %s",
					tc.imageName, result, tc.expected, tc.description)
			}
		})
	}
}

func TestBuildImageURLMultipleSources(t *testing.T) {
	// Skip if we don't have multiple sources configured
	sources := GetSources()
	if len(sources) < 2 {
		t.Skip("Test requires multiple sources in versions.yaml")
	}

	testCases := []struct {
		name           string
		imageName      string
		expectedPrefix string
		expectedSuffix string
	}{
		{
			name:           "postgres image",
			imageName:      "postgres:17.7",
			expectedPrefix: "ghcr.io/xataio/postgres-images/",
			expectedSuffix: "cnpg-postgres-plus:17.7",
		},
		{
			name:           "experimental image",
			imageName:      "experimental:17.7",
			expectedPrefix: "ghcr.io/xataio/postgres-images/",
			expectedSuffix: "experimental:17.7",
		},
		{
			name:           "analytics image",
			imageName:      "analytics:17.7",
			expectedPrefix: "ghcr.io/xataio/postgres-images/",
			expectedSuffix: "xata-analytics:17.7",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := BuildImageURL(tc.imageName)
			if result == "" {
				t.Errorf("BuildImageURL(%q) returned empty string", tc.imageName)
				return
			}
			if tc.expectedPrefix != "" && !strings.HasPrefix(result, tc.expectedPrefix) {
				t.Errorf("BuildImageURL(%q) = %q, expected prefix %q", tc.imageName, result, tc.expectedPrefix)
			}
			if tc.expectedSuffix != "" && !strings.HasSuffix(result, tc.expectedSuffix) {
				t.Errorf("BuildImageURL(%q) = %q, expected suffix %q", tc.imageName, result, tc.expectedSuffix)
			}
		})
	}
}

func TestParseImageVersion(t *testing.T) {
	testCases := map[string]struct {
		image   string
		want    *ImageVersion
		wantErr bool
	}{
		"short postgres image": {
			image: "postgres:17.5",
			want:  &ImageVersion{Offering: "postgres", Major: 17, Minor: 5},
		},
		"short analytics image": {
			image: "analytics:17.7",
			want:  &ImageVersion{Offering: "analytics", Major: 17, Minor: 7},
		},
		"short experimental image": {
			image: "experimental:17.7",
			want:  &ImageVersion{Offering: "experimental", Major: 17, Minor: 7},
		},
		"full registry path postgres": {
			image: "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
			want:  &ImageVersion{Offering: "postgres", Major: 17, Minor: 5},
		},
		"full registry path with date suffix": {
			image: "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5-08092025",
			want:  &ImageVersion{Offering: "postgres", Major: 17, Minor: 5},
		},
		"full registry path analytics": {
			image: "ghcr.io/xataio/postgres-images/xata-analytics:17.7",
			want:  &ImageVersion{Offering: "analytics", Major: 17, Minor: 7},
		},
		"pg 16": {
			image: "postgres:16.3",
			want:  &ImageVersion{Offering: "postgres", Major: 16, Minor: 3},
		},
		"pg 18": {
			image: "postgres:18.0",
			want:  &ImageVersion{Offering: "postgres", Major: 18, Minor: 0},
		},
		"double digit minor": {
			image: "postgres:14.10",
			want:  &ImageVersion{Offering: "postgres", Major: 14, Minor: 10},
		},
		"rc version": {
			image:   "postgres:18rc1",
			wantErr: true,
		},
		"no colon": {
			image:   "postgres-latest",
			wantErr: true,
		},
		"major only no minor": {
			image:   "postgres:17",
			wantErr: true,
		},
		"non-numeric major": {
			image:   "postgres:abc.5",
			wantErr: true,
		},
		"non-numeric minor": {
			image:   "postgres:17.abc",
			wantErr: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			got, err := ParseImageVersion(tc.image)
			if tc.wantErr {
				if err == nil {
					t.Errorf("ParseImageVersion(%q): expected error, got nil", tc.image)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseImageVersion(%q): unexpected error: %v", tc.image, err)
			}
			if *got != *tc.want {
				t.Errorf("ParseImageVersion(%q) = %+v, want %+v", tc.image, got, tc.want)
			}
		})
	}
}

func TestValidateImage(t *testing.T) {
	testCases := []struct {
		name        string
		image       string
		expectError bool
	}{
		{
			name:        "valid analytics image",
			image:       "analytics:17.7",
			expectError: false,
		},
		{
			name:        "valid postgres image",
			image:       "postgres:17.7",
			expectError: false,
		},
		{
			name:        "valid experimental image",
			image:       "experimental:17.7",
			expectError: false,
		},
		{
			name:        "missing colon",
			image:       "invalid-format",
			expectError: true,
		},
		{
			name:        "too many colons",
			image:       "too:many:colons",
			expectError: true,
		},
		{
			name:        "empty string",
			image:       "",
			expectError: true,
		},
		{
			name:        "non-existent version",
			image:       "analytics:17.5",
			expectError: true,
		},
		{
			name:        "non-existent offering",
			image:       "dubious-offering:17.7",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateImage(tc.image)
			if tc.expectError {
				if err == nil {
					t.Errorf("ValidateImage(%q): expected error, got nil", tc.image)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateImage(%q): unexpected error: %v", tc.image, err)
				}
			}
		})
	}
}
