package postgrescfg

import (
	"reflect"
	"testing"
)

func TestGetDefaultPreloadLibraries(t *testing.T) {
	tests := []struct {
		name     string
		image    string
		expected []string
		wantErr  bool
		errMsg   string
	}{
		{
			name:     "returns expected default libraries for postgres offering",
			image:    "postgres:17",
			expected: defaultPreloadLibraries["postgres"],
		},
		{
			name:     "returns expected default libraries for experimental offering",
			image:    "experimental:17",
			expected: defaultPreloadLibraries["experimental"],
		},
		{
			name:     "returns expected default libraries for analytics offering",
			image:    "analytics:17",
			expected: defaultPreloadLibraries["analytics"],
		},
		{
			name:     "handles full image URL for postgres",
			image:    "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
			expected: defaultPreloadLibraries["postgres"],
		},
		{
			name:     "handles full image URL for analytics",
			image:    "ghcr.io/xataio/postgres-images/xata-analytics:17.5",
			expected: defaultPreloadLibraries["analytics"],
		},
		{
			name:    "returns error for unknown offering",
			image:   "unknown:17",
			wantErr: true,
			errMsg:  "unknown offering: unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := GetDefaultPreloadLibraries(tt.image)
			if tt.wantErr {
				if err == nil {
					t.Errorf("GetDefaultPreloadLibraries(%q) expected error but got none", tt.image)
					return
				}
				if tt.errMsg != "" && err.Error() != tt.errMsg {
					t.Errorf("GetDefaultPreloadLibraries(%q) error = %v, want %v", tt.image, err.Error(), tt.errMsg)
				}
				return
			}
			if err != nil {
				t.Errorf("GetDefaultPreloadLibraries(%q) unexpected error: %v", tt.image, err)
				return
			}
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("GetDefaultPreloadLibraries(%q) = %v, want %v", tt.image, result, tt.expected)
			}
		})
	}
}

func TestValidatePreloadLibraries(t *testing.T) {
	// Using analytics:17 as the test image - these libraries have preload_required=true
	testImage := "analytics:17"

	tests := []struct {
		name      string
		image     string
		libraries []string
		wantErr   bool
		errMsg    string
	}{
		{
			name:  "validates preload-required libraries for analytics:17",
			image: testImage,
			libraries: []string{
				"pg_stat_statements",
				"auto_explain",
				"pg_prewarm",
				"pgaudit",
				"pg_cron",
				"pg_hint_plan",
			},
			wantErr: false,
		},
		{
			name:      "validates subset of available libraries",
			image:     testImage,
			libraries: []string{"pg_stat_statements", "auto_explain", "pgaudit"},
			wantErr:   false,
		},
		{
			name:      "validates default libraries for analytics",
			image:     testImage,
			libraries: defaultPreloadLibraries["analytics"],
			wantErr:   false,
		},
		{
			name:      "returns error for invalid library",
			image:     testImage,
			libraries: []string{"pg_stat_statements", "invalid_library"},
			wantErr:   true,
			errMsg:    "invalid preload library: invalid_library",
		},
		{
			name:      "returns error for multiple invalid libraries",
			image:     testImage,
			libraries: []string{"invalid1", "pg_stat_statements", "invalid2"},
			wantErr:   true,
			errMsg:    "invalid preload library: invalid1",
		},
		{
			name:      "handles empty slice",
			image:     testImage,
			libraries: []string{},
			wantErr:   false,
		},
		{
			name:      "handles nil slice",
			image:     testImage,
			libraries: nil,
			wantErr:   false,
		},
		{
			name:      "handles duplicates",
			image:     testImage,
			libraries: []string{"pg_stat_statements", "pg_stat_statements", "auto_explain"},
			wantErr:   false,
		},
		{
			name:      "is case sensitive",
			image:     testImage,
			libraries: []string{"PG_STAT_STATEMENTS"},
			wantErr:   true,
			errMsg:    "invalid preload library: PG_STAT_STATEMENTS",
		},
		{
			name:      "single valid library",
			image:     testImage,
			libraries: []string{"pg_stat_statements"},
			wantErr:   false,
		},
		{
			name:      "single invalid library",
			image:     testImage,
			libraries: []string{"nonexistent"},
			wantErr:   true,
			errMsg:    "invalid preload library: nonexistent",
		},
		{
			name:      "mixed valid and invalid libraries",
			image:     testImage,
			libraries: []string{"pg_stat_statements", "auto_explain", "invalid_lib", "pgaudit"},
			wantErr:   true,
			errMsg:    "invalid preload library: invalid_lib",
		},
		{
			name:      "all invalid libraries",
			image:     testImage,
			libraries: []string{"invalid1", "invalid2", "invalid3"},
			wantErr:   true,
			errMsg:    "invalid preload library: invalid1",
		},
		{
			name:      "rejects extension that doesn't require preload",
			image:     testImage,
			libraries: []string{"pg_stat_statements", "pg_trgm"}, // pg_trgm has preload_required=false
			wantErr:   true,
			errMsg:    "invalid preload library: pg_trgm",
		},
		// postgres image tests
		{
			name:  "validates preload-required libraries for postgres:14",
			image: "postgres:14",
			libraries: []string{
				"pg_stat_statements",
				"auto_explain",
				"pg_prewarm",
				"pg_cron",
			},
			wantErr: false,
		},
		{
			name:      "postgres:14 rejects pg_hint_plan (not available in this image)",
			image:     "postgres:14",
			libraries: []string{"pg_stat_statements", "pg_hint_plan"},
			wantErr:   true,
			errMsg:    "invalid preload library: pg_hint_plan",
		},
		{
			name:      "validates default libraries for postgres:14",
			image:     "postgres:14",
			libraries: defaultPreloadLibraries["postgres"],
			wantErr:   false,
		},
		// postgres:18 image tests
		{
			name:  "validates preload-required libraries for postgres:18",
			image: "postgres:18",
			libraries: []string{
				"pg_stat_statements",
				"auto_explain",
				"pg_prewarm",
				"pg_hint_plan",
				"pg_cron",
				"pgaudit",
			},
			wantErr: false,
		},
		{
			name:      "postgres:18 accepts pg_hint_plan (unlike postgres:14)",
			image:     "postgres:18",
			libraries: []string{"pg_stat_statements", "pg_hint_plan"},
			wantErr:   false,
		},
		{
			name:      "postgres:18 accepts pgaudit (unlike postgres:14)",
			image:     "postgres:18",
			libraries: []string{"pg_stat_statements", "pgaudit"},
			wantErr:   false,
		},
		// Full image URL tests
		{
			name:      "handles full image URL for postgres",
			image:     "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
			libraries: []string{"pg_stat_statements", "auto_explain"},
			wantErr:   false,
		},
		{
			name:      "handles full image URL for analytics",
			image:     "ghcr.io/xataio/postgres-images/xata-analytics:17.5",
			libraries: []string{"pg_stat_statements", "auto_explain", "pg_duckdb"},
			wantErr:   false,
		},
		{
			name:      "full image URL rejects invalid library",
			image:     "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5",
			libraries: []string{"pg_stat_statements", "invalid_lib"},
			wantErr:   true,
			errMsg:    "invalid preload library: invalid_lib",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePreloadLibraries(tt.image, tt.libraries)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ValidatePreloadLibraries() expected error but got none")
					return
				}
				if tt.errMsg != "" && err.Error() != tt.errMsg {
					t.Errorf("ValidatePreloadLibraries() error message = %v, want %v", err.Error(), tt.errMsg)
				}
			} else if err != nil {
				t.Errorf("ValidatePreloadLibraries() unexpected error: %v", err)
			}
		})
	}
}

func TestGetInternalPreloadLibraries(t *testing.T) {
	tests := []struct {
		name string
		want []string
	}{
		{
			name: "returns expected internal libraries",
			want: []string{"xatautils"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetInternalPreloadLibraries()
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("GetInternalPreloadLibraries() = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestFilterOutInternalPreloadLibraries(t *testing.T) {
	tests := []struct {
		name      string
		libraries []string
		want      []string
	}{
		{
			name:      "filters out internal libraries",
			libraries: []string{"pg_stat_statements", "xatautils", "auto_explain"},
			want:      []string{"pg_stat_statements", "auto_explain"},
		},
		{
			name:      "filters out pgprivs",
			libraries: []string{"pgprivs", "pg_stat_statements"},
			want:      []string{"pg_stat_statements"},
		},
		{
			name:      "handles empty slice",
			libraries: []string{},
			want:      []string(nil),
		},
		{
			name:      "handles slice with no internal libraries",
			libraries: []string{"pg_stat_statements", "auto_explain"},
			want:      []string{"pg_stat_statements", "auto_explain"},
		},
		{
			name:      "handles slice with only internal libraries",
			libraries: []string{"xatautils"},
			want:      []string(nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FilterOutInternalPreloadLibraries(tt.libraries)
			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("FilterOutInternalPreloadLibraries() = %v, want %v", result, tt.want)
			}
		})
	}
}
