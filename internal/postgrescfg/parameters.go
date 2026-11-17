package postgrescfg

import (
	"embed"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"xata/internal/extensions"
	"xata/internal/postgresversions"

	"gopkg.in/yaml.v3"
)

type ParameterType uint8

const (
	ParameterTypeString ParameterType = iota + 1
	ParamTypeInt
	ParamTypeFloat
	ParamTypeBytes
	ParamTypeEnum
	ParamTypeDuration
	ParamTypeBoolean
)

//go:embed parameters.yaml
var parametersFS embed.FS

// PostgresParameterSpec is the "definition" of a Postgres parameter (e.g. max_connections, shared_buffers, etc.)
// It contains things like the default value, and possible values (min/max ranges for numerics, enum values, etc.)
// It also contains information about the parameter's "documentation", including our custom recommendations.
type PostgresParameterSpec struct {
	ParameterType
	Section                string
	Description            string
	DefaultValue           string
	MinValue               string
	MaxValue               string
	Values                 []string
	DocsLink               string
	Recommendation         string
	RestartRequired        bool
	AllowEmpty             bool
	PostgresMinimumVersion *int   // Optional minimum PostgreSQL major version (e.g. 15, 17, 18)
	PostgresMaximumVersion *int   // Optional maximum PostgreSQL major version, for removed parameters
	Extension              string // Optional extension name - parameter only available if extension exists for the image
}

// YAML representation for unmarshaling
type yamlParameterSpec struct {
	Section                string   `yaml:"section"`
	ParameterType          string   `yaml:"parameter_type"`
	Description            string   `yaml:"description"`
	DefaultValue           string   `yaml:"default_value"`
	MinValue               string   `yaml:"min_value"`
	MaxValue               string   `yaml:"max_value"`
	Values                 []string `yaml:"values,omitempty"`
	DocsLink               string   `yaml:"docs_link"`
	Recommendation         string   `yaml:"recommendation,omitempty"`
	RestartRequired        bool     `yaml:"restart_required,omitempty"`
	AllowEmpty             bool     `yaml:"allow_empty,omitempty"`
	PostgresMinimumVersion *int     `yaml:"postgres_minimum_version,omitempty"`
	PostgresMaximumVersion *int     `yaml:"postgres_maximum_version,omitempty"`
	Extension              string   `yaml:"extension,omitempty"`
}

var configurableParameters map[string]PostgresParameterSpec

// init loads the configurable parameters from the embedded YAML file
func init() {
	if err := loadConfigurableParameters(); err != nil {
		panic(fmt.Sprintf("failed to load configurable parameters: %v", err))
	}
}

// loadConfigurableParameters unmarshals the embedded YAML file into the configurableParameters map
func loadConfigurableParameters() error {
	yamlData, err := parametersFS.ReadFile("parameters.yaml")
	if err != nil {
		return fmt.Errorf("failed to read embedded YAML file: %w", err)
	}

	var yamlParams map[string]yamlParameterSpec
	if err := yaml.Unmarshal(yamlData, &yamlParams); err != nil {
		return fmt.Errorf("failed to unmarshal YAML: %w", err)
	}

	configurableParameters = make(map[string]PostgresParameterSpec)
	for name, yamlSpec := range yamlParams {
		spec, err := convertYAMLSpec(yamlSpec)
		if err != nil {
			return fmt.Errorf("failed to convert parameter %s: %w", name, err)
		}
		configurableParameters[name] = spec
	}

	return nil
}

// convertYAMLSpec converts a yamlParameterSpec to PostgresParameterSpec
func convertYAMLSpec(yamlSpec yamlParameterSpec) (PostgresParameterSpec, error) {
	var paramType ParameterType
	switch yamlSpec.ParameterType {
	case "string":
		paramType = ParameterTypeString
	case "int":
		paramType = ParamTypeInt
	case "float":
		paramType = ParamTypeFloat
	case "bytes":
		paramType = ParamTypeBytes
	case "enum":
		paramType = ParamTypeEnum
	case "duration":
		paramType = ParamTypeDuration
	case "boolean":
		paramType = ParamTypeBoolean
	default:
		return PostgresParameterSpec{}, fmt.Errorf("unknown parameter type: %s", yamlSpec.ParameterType)
	}

	return PostgresParameterSpec{
		ParameterType:          paramType,
		Section:                yamlSpec.Section,
		Description:            yamlSpec.Description,
		DefaultValue:           yamlSpec.DefaultValue,
		MinValue:               yamlSpec.MinValue,
		MaxValue:               yamlSpec.MaxValue,
		Values:                 yamlSpec.Values,
		DocsLink:               yamlSpec.DocsLink,
		Recommendation:         yamlSpec.Recommendation,
		RestartRequired:        yamlSpec.RestartRequired,
		AllowEmpty:             yamlSpec.AllowEmpty,
		PostgresMinimumVersion: yamlSpec.PostgresMinimumVersion,
		PostgresMaximumVersion: yamlSpec.PostgresMaximumVersion,
		Extension:              yamlSpec.Extension,
	}, nil
}

// GetConfigurableParameters returns all configurable parameters filtered by PostgreSQL major version,
// extension availability, and preload status for the given image.
// If majorVersion is 0, version filtering is skipped.
// If image is empty, extension filtering is skipped.
// If preloadLibraries is provided, parameters for extensions that require preloading but aren't
// in the preload list will be filtered out.
// Accepts both full image URLs (e.g., "ghcr.io/xataio/postgres-images/cnpg-postgres-plus:17.5")
// and short formats (e.g., "postgres:17").
func GetConfigurableParameters(majorVersion int, image string, preloadLibraries []string) map[string]PostgresParameterSpec {
	// Build map of available extensions for the image
	var availableExtensions map[string]extensions.ExtensionSpec
	if image != "" {
		shortImage := postgresversions.ShortImageName(image)
		availableExtensions = make(map[string]extensions.ExtensionSpec)
		for _, ext := range extensions.GetExtensions(shortImage) {
			availableExtensions[ext.Name] = ext
		}
	}

	// Build set of preloaded libraries for quick lookup
	preloadedSet := make(map[string]bool, len(preloadLibraries))
	for _, lib := range preloadLibraries {
		preloadedSet[lib] = true
	}

	filtered := make(map[string]PostgresParameterSpec)
	for name, spec := range configurableParameters {
		// Filter by PostgreSQL version
		if majorVersion != 0 && spec.PostgresMinimumVersion != nil && *spec.PostgresMinimumVersion > majorVersion {
			continue
		}
		if majorVersion != 0 && spec.PostgresMaximumVersion != nil && *spec.PostgresMaximumVersion < majorVersion {
			continue
		}

		// Filter by extension availability and preload status
		if spec.Extension != "" && availableExtensions != nil {
			ext, exists := availableExtensions[spec.Extension]
			if !exists {
				continue
			}
			// Filter out parameters for extensions that require preloading but aren't preloaded
			// If preloadLibraries is nil, skip preload filtering
			if preloadLibraries != nil && ext.PreloadRequired && !preloadedSet[spec.Extension] {
				continue
			}
		}

		filtered[name] = spec
	}
	return filtered
}

// parseBytes parses a byte string like "1MB", "2GB", etc. and returns the value in bytes
// Only accepts units like this: B, kB, MB, GB, TB
func parseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)

	// Handle special case for "0B"
	if s == "0B" {
		return 0, nil
	}

	// Find the last non-digit character to determine the unit
	var unit string
	var numStr string

	for i := len(s) - 1; i >= 0; i-- {
		if s[i] >= '0' && s[i] <= '9' || s[i] == '.' || s[i] == '-' {
			numStr = s[:i+1]
			if i+1 < len(s) {
				unit = s[i+1:]
			}
			break
		}
	}

	if numStr == "" {
		return 0, fmt.Errorf("invalid byte format: %s", s)
	}

	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number in byte format: %s", s)
	}

	var multiplier int64
	switch unit {
	case "B", "":
		multiplier = 1
	case "kB":
		multiplier = 1024
	case "MB":
		multiplier = 1024 * 1024
	case "GB":
		multiplier = 1024 * 1024 * 1024
	case "TB":
		multiplier = 1024 * 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("unknown byte unit: %s (only B, kB, MB, GB, TB are supported)", unit)
	}

	return int64(num * float64(multiplier)), nil
}

// parseDuration parses a duration string like "10s", "5min", "24h" and returns the duration
func parseDuration(s string) (time.Duration, error) {
	s = strings.ToLower(strings.TrimSpace(s))

	if s == "0" {
		return 0, nil
	}

	// Try to parse as Go duration first
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}

	// Handle PostgreSQL-specific formats
	var multiplier time.Duration
	var numStr string

	// Extract number and unit
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] >= '0' && s[i] <= '9' || s[i] == '.' || s[i] == '-' {
			numStr = s[:i+1]
			if i+1 < len(s) {
				unit := s[i+1:]
				switch unit {
				case "s":
					multiplier = time.Second
				case "min":
					multiplier = time.Minute
				case "h":
					multiplier = time.Hour
				case "d":
					multiplier = 24 * time.Hour
				default:
					return 0, fmt.Errorf("unknown duration unit: %s", unit)
				}
			}
			break
		}
	}

	if numStr == "" {
		return 0, fmt.Errorf("invalid duration format: %s", s)
	}

	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number in duration format: %s", s)
	}

	return time.Duration(num * float64(multiplier)), nil
}

// ValidateParameterValue validates if a value is valid for a given PostgresParameterSpec
func ValidateParameterValue(spec PostgresParameterSpec, value string) error {
	// A control character such as a newline survives into postgresql.conf and can
	// stop the instance from starting, so reject it before trimming.
	if strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return fmt.Errorf("value must not contain control characters")
	}

	value = strings.TrimSpace(value)

	switch spec.ParameterType {
	case ParamTypeInt:
		return validateIntValue(spec, value)
	case ParamTypeFloat:
		return validateFloatValue(spec, value)
	case ParamTypeBytes:
		return validateBytesValue(spec, value)
	case ParamTypeEnum:
		return validateEnumValue(spec, value)
	case ParamTypeDuration:
		return validateDurationValue(spec, value)
	case ParamTypeBoolean:
		return validateBooleanValue(spec, value)
	case ParameterTypeString:
		if value == "" && !spec.AllowEmpty {
			return fmt.Errorf("string parameter cannot be empty")
		}
		return nil
	default:
		return fmt.Errorf("unknown parameter type: %d", spec.ParameterType)
	}
}

func validateIntValue(spec PostgresParameterSpec, value string) error {
	val, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid integer value: %s", value)
	}

	// Check min value
	if spec.MinValue != "" {
		minVal, err := strconv.ParseInt(spec.MinValue, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid min value in spec: %s", spec.MinValue)
		}
		if val < minVal {
			return fmt.Errorf("value %d is below minimum %d", val, minVal)
		}
	}

	// Check max value
	if spec.MaxValue != "" {
		maxVal, err := strconv.ParseInt(spec.MaxValue, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid max value in spec: %s", spec.MaxValue)
		}
		if val > maxVal {
			return fmt.Errorf("value %d is above maximum %d", val, maxVal)
		}
	}

	return nil
}

func validateFloatValue(spec PostgresParameterSpec, value string) error {
	val, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fmt.Errorf("invalid float value: %s", value)
	}

	// Check min value
	if spec.MinValue != "" {
		minVal, err := strconv.ParseFloat(spec.MinValue, 64)
		if err != nil {
			return fmt.Errorf("invalid min value in spec: %s", spec.MinValue)
		}
		if val < minVal {
			return fmt.Errorf("value %f is below minimum %f", val, minVal)
		}
	}

	// Check max value
	if spec.MaxValue != "" {
		maxVal, err := strconv.ParseFloat(spec.MaxValue, 64)
		if err != nil {
			return fmt.Errorf("invalid max value in spec: %s", spec.MaxValue)
		}
		if val > maxVal {
			return fmt.Errorf("value %f is above maximum %f", val, maxVal)
		}
	}

	return nil
}

func validateBytesValue(spec PostgresParameterSpec, value string) error {
	val, err := parseBytes(value)
	if err != nil {
		return fmt.Errorf("invalid bytes value: %s", value)
	}

	// Check min value
	if spec.MinValue != "" {
		minVal, err := parseBytes(spec.MinValue)
		if err != nil {
			return fmt.Errorf("invalid min value in spec: %s", spec.MinValue)
		}
		if val < minVal {
			return fmt.Errorf("value %d bytes is below minimum %d bytes", val, minVal)
		}
	}

	// Check max value
	if spec.MaxValue != "" {
		maxVal, err := parseBytes(spec.MaxValue)
		if err != nil {
			return fmt.Errorf("invalid max value in spec: %s", spec.MaxValue)
		}
		if val > maxVal {
			return fmt.Errorf("value %d bytes is above maximum %d bytes", val, maxVal)
		}
	}

	return nil
}

func validateEnumValue(spec PostgresParameterSpec, value string) error {
	if len(spec.Values) == 0 {
		return fmt.Errorf("enum parameter has no allowed values defined")
	}

	if slices.Contains(spec.Values, value) {
		return nil
	}

	return fmt.Errorf("value %s is not in allowed values: %v", value, spec.Values)
}

func validateDurationValue(spec PostgresParameterSpec, value string) error {
	val, err := parseDuration(value)
	if err != nil {
		return fmt.Errorf("invalid duration value: %s", value)
	}

	// Check min value
	if spec.MinValue != "" {
		minVal, err := parseDuration(spec.MinValue)
		if err != nil {
			return fmt.Errorf("invalid min value in spec: %s", spec.MinValue)
		}
		if val < minVal {
			return fmt.Errorf("value %v is below minimum %v", val, minVal)
		}
	}

	// Check max value
	if spec.MaxValue != "" {
		maxVal, err := parseDuration(spec.MaxValue)
		if err != nil {
			return fmt.Errorf("invalid max value in spec: %s", spec.MaxValue)
		}
		if val > maxVal {
			return fmt.Errorf("value %v is above maximum %v", val, maxVal)
		}
	}

	return nil
}

func validateBooleanValue(spec PostgresParameterSpec, value string) error {
	// Boolean parameters accept various representations
	// True values: "true", "on", "yes", "1"
	// False values: "false", "off", "no", "0"
	switch value {
	case "true", "on", "yes", "1":
		return nil
	case "false", "off", "no", "0":
		return nil
	default:
		return fmt.Errorf("boolean parameter must be one of: 'true', 'on', 'yes', '1' (for true) or 'false', 'off', 'no', '0' (for false), got: %s", value)
	}
}

// AdjustValueToBounds adjusts a value to be within the specified min/max bounds.
// If the value is outside the bounds, it returns the closest bound value.
// If no bounds are specified, it returns the original value unchanged.
func AdjustValueToBounds(value string, spec PostgresParameterSpec) string {
	// If no bounds are specified, return the original value
	if spec.MinValue == "" && spec.MaxValue == "" {
		return value
	}

	// Adjust based on the parameter type
	switch spec.ParameterType {
	case ParamTypeInt:
		if adjusted, ok := adjustIntValueToBounds(value, spec.MinValue, spec.MaxValue); ok {
			return adjusted
		}
	case ParamTypeFloat:
		if adjusted, ok := adjustFloatValueToBounds(value, spec.MinValue, spec.MaxValue); ok {
			return adjusted
		}
	case ParamTypeBytes:
		if adjusted, ok := adjustBytesValueToBounds(value, spec.MinValue, spec.MaxValue); ok {
			return adjusted
		}
	case ParamTypeDuration:
		if adjusted, ok := adjustDurationValueToBounds(value, spec.MinValue, spec.MaxValue); ok {
			return adjusted
		}
	default:
		// For specs without min/max values, we don't adjust values
		return value
	}
	return value
}

func adjustIntValueToBounds(value, minValue, maxValue string) (string, bool) {
	val, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return value, false
	}

	adjusted := value

	if minValue != "" {
		minVal, err := strconv.ParseInt(minValue, 10, 64)
		if err == nil && val < minVal {
			adjusted = minValue
		}
	}

	if maxValue != "" {
		maxVal, err := strconv.ParseInt(maxValue, 10, 64)
		if err == nil && val > maxVal {
			adjusted = maxValue
		}
	}

	return adjusted, true
}

func adjustFloatValueToBounds(value, minValue, maxValue string) (string, bool) {
	val, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return value, false
	}

	adjusted := value

	if minValue != "" {
		minVal, err := strconv.ParseFloat(minValue, 64)
		if err == nil && val < minVal {
			adjusted = minValue
		}
	}

	if maxValue != "" {
		maxVal, err := strconv.ParseFloat(maxValue, 64)
		if err == nil && val > maxVal {
			adjusted = maxValue
		}
	}

	return adjusted, true
}

func adjustBytesValueToBounds(value, minValue, maxValue string) (string, bool) {
	val, err := parseBytes(value)
	if err != nil {
		return value, false
	}

	adjusted := value

	if minValue != "" {
		minVal, err := parseBytes(minValue)
		if err == nil && val < minVal {
			adjusted = minValue
		}
	}

	if maxValue != "" {
		maxVal, err := parseBytes(maxValue)
		if err == nil && val > maxVal {
			adjusted = maxValue
		}
	}

	return adjusted, true
}

func adjustDurationValueToBounds(value, minValue, maxValue string) (string, bool) {
	val, err := parseDuration(value)
	if err != nil {
		return value, false
	}

	adjusted := value

	if minValue != "" {
		minVal, err := parseDuration(minValue)
		if err == nil && val < minVal {
			adjusted = minValue
		}
	}

	if maxValue != "" {
		maxVal, err := parseDuration(maxValue)
		if err == nil && val > maxVal {
			adjusted = maxValue
		}
	}

	return adjusted, true
}

// ValidateSettings validates if a group of settings is valid for a particular instance type.
// The settings are received as a map[string]string and the instance type as a string (e.g. "xata.micro", "xata.small", etc.).
// Returns a map of parameter names to validation errors, or nil if all validations pass.
func ValidateSettings(instanceType string, settings map[string]string, majorVersion int, image string, preloadLibraries []string) (map[string]error, error) {
	// Get the base configurable parameters
	baseParams := GetConfigurableParameters(majorVersion, image, preloadLibraries)

	// Get the instance-specific defaults/limits
	instanceParams, err := GetDefaultPostgresConfigByInstanceType(instanceType)
	if err != nil {
		return nil, fmt.Errorf("invalid instance type: %s", instanceType)
	}

	// Merge the base configuration with the instance-specific defaults/limits
	mergedParams := MergeParametersMaps(baseParams, instanceParams)

	// Validate each setting
	validationErrors := make(map[string]error)

	for paramName, paramValue := range settings {
		// Check if this is a configurable parameter
		spec, exists := mergedParams[paramName]
		if !exists {
			validationErrors[paramName] = fmt.Errorf("unknown parameter: %s", paramName)
			continue
		}

		// Validate the parameter value
		if err := ValidateParameterValue(spec, paramValue); err != nil {
			validationErrors[paramName] = err
		}
	}

	// Return nil if no validation errors, otherwise return the error map
	if len(validationErrors) == 0 {
		return nil, nil
	}

	return validationErrors, nil
}

// FilterConfigurableParameters filters a map of parameters to contain only settings
// that are in the GetConfigurableParameters() list.
func FilterConfigurableParameters(params map[string]string, majorVersion int, image string, preloadLibraries []string) map[string]string {
	configurableParams := GetConfigurableParameters(majorVersion, image, preloadLibraries)
	filtered := make(map[string]string)

	for key, value := range params {
		if _, exists := configurableParams[key]; exists {
			filtered[key] = value
		}
	}

	return filtered
}
