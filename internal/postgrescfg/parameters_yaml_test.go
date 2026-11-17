package postgrescfg

import (
	"slices"
	"testing"
)

func TestYAMLLoading(t *testing.T) {
	// Test that configurable parameters are loaded from YAML
	params := GetConfigurableParameters(0, "", nil)

	// Check that we have the expected number of parameters
	expectedCount := 23 // Based on the YAML file
	if len(params) < expectedCount {
		t.Errorf("Expected at least %d parameters, got %d", expectedCount, len(params))
	}

	// Test a few specific parameters to ensure they're loaded correctly
	testCases := []struct {
		name     string
		expected ParameterType
	}{
		{"max_connections", ParamTypeInt},
		{"shared_buffers", ParamTypeBytes},
		{"huge_pages", ParamTypeEnum},
		{"checkpoint_timeout", ParamTypeDuration},
	}

	for _, tc := range testCases {
		param, exists := params[tc.name]
		if !exists {
			t.Errorf("Parameter %s not found", tc.name)
			continue
		}
		if param.ParameterType != tc.expected {
			t.Errorf("Parameter %s has wrong type: expected %v, got %v",
				tc.name, tc.expected, param.ParameterType)
		}
	}

	// Test enum parameter has values
	hugePages, exists := params["huge_pages"]
	if !exists {
		t.Fatal("huge_pages parameter not found")
	}
	if len(hugePages.Values) == 0 {
		t.Error("huge_pages parameter should have enum values")
	}
	expectedValues := []string{"on", "off", "try"}
	for _, expected := range expectedValues {
		found := slices.Contains(hugePages.Values, expected)
		if !found {
			t.Errorf("Expected value %s not found in huge_pages values", expected)
		}
	}

	// Test section field is loaded correctly
	testSectionCases := []struct {
		name     string
		expected string
	}{
		{"max_connections", "Connections"},
		{"shared_buffers", "Resource consumption"},
		{"effective_cache_size", "Planner"},
		{"effective_io_concurrency", "Async behavior"},
		{"wal_buffers", "WAL"},
	}

	for _, tc := range testSectionCases {
		param, exists := params[tc.name]
		if !exists {
			t.Errorf("Parameter %s not found", tc.name)
			continue
		}
		if param.Section != tc.expected {
			t.Errorf("Parameter %s has wrong section: expected %s, got %s",
				tc.name, tc.expected, param.Section)
		}
	}
}

func TestVersionFiltering(t *testing.T) {
	// Test version filtering functionality
	allParams := GetConfigurableParameters(0, "", nil)

	// Test with PostgreSQL 14 - should exclude parameters requiring version 15+
	params14 := GetConfigurableParameters(14, "", nil)

	// Test with PostgreSQL 15 - should include parameters requiring version 15
	params15 := GetConfigurableParameters(15, "", nil)

	// Test with PostgreSQL 17 - should include parameters requiring version 17
	params17 := GetConfigurableParameters(17, "", nil)

	// Verify that version filtering works
	if len(params14) >= len(allParams) {
		t.Error("PostgreSQL 14 should have fewer parameters than all parameters")
	}

	// PostgreSQL 15 should have exactly one more parameter than PostgreSQL 14:
	// wal_compression, whose lz4/zstd values (and our zstd default) require 15+.
	if len(params15) != len(params14)+1 {
		t.Errorf("PostgreSQL 15 should have one more parameter than PostgreSQL 14 (wal_compression), got %d vs %d", len(params15), len(params14))
	}

	// wal_compression should be available in PostgreSQL 15+ but not 14
	if _, exists := params14["wal_compression"]; exists {
		t.Error("wal_compression should not be available in PostgreSQL 14")
	}

	if _, exists := params15["wal_compression"]; !exists {
		t.Error("wal_compression should be available in PostgreSQL 15")
	}

	if len(params17) <= len(params15) {
		t.Error("PostgreSQL 17 should have more parameters than PostgreSQL 15")
	}

	// io_combine_limit should be available in PostgreSQL 17+ but not 15
	if _, exists := params15["io_combine_limit"]; exists {
		t.Error("io_combine_limit should not be available in PostgreSQL 15")
	}

	if _, exists := params17["io_combine_limit"]; !exists {
		t.Error("io_combine_limit should be available in PostgreSQL 17")
	}
}

func TestVersionFilteringMaximum(t *testing.T) {
	params18 := GetConfigurableParameters(18, "", nil)
	params19 := GetConfigurableParameters(19, "", nil)

	// PostgreSQL 19 replaced io_workers with io_min_workers and io_max_workers,
	// and refuses to start with the removed name in its configuration.
	for name, want18 := range map[string]bool{"io_workers": true, "io_min_workers": false, "io_max_workers": false} {
		if _, got := params18[name]; got != want18 {
			t.Errorf("%s in PostgreSQL 18 = %v, want %v", name, got, want18)
		}
		if _, got := params19[name]; got == want18 {
			t.Errorf("%s in PostgreSQL 19 = %v, want %v", name, got, !want18)
		}
	}
}
