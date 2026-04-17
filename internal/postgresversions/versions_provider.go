package postgresversions

//go:generate go run github.com/vektra/mockery/v3 --output mocks --outpkg mocks --with-expecter --name ImageProvider

// ImageProvider defines the interface for PostgreSQL image and version operations
type ImageProvider interface {
	GetAllVersions() []string
	GetAllImageNames() []string
	GetVersionsForMajor(major string) []string
	BuildImageURL(imageName string) string
	GetMajorForVersion(version string) string
	ExtractVersionFromImageName(imageName string) string
	ParseImageVersion(image string) (*ImageVersion, error)
	ValidateImage(image string) error
}

// DefaultImageProvider is the default implementation of ImageProvider
type DefaultImageProvider struct{}

// GetAllVersions implements ImageProvider
func (p *DefaultImageProvider) GetAllVersions() []string {
	return GetAllVersions()
}

func (p *DefaultImageProvider) GetAllImageNames() []string { return GetAllImageNames() }

// GetVersionsForMajor implements ImageProvider
func (p *DefaultImageProvider) GetVersionsForMajor(major string) []string {
	return GetVersionsForMajor(major)
}

// BuildImageURL implements ImageProvider
func (p *DefaultImageProvider) BuildImageURL(imageName string) string {
	return BuildImageURL(imageName)
}

// GetMajorForVersion implements ImageProvider
func (p *DefaultImageProvider) GetMajorForVersion(version string) string {
	return GetMajorForVersion(version)
}

// ExtractVersionFromImageName implements ImageProvider
func (p *DefaultImageProvider) ExtractVersionFromImageName(imageName string) string {
	return ExtractVersionFromImageName(imageName)
}

// ParseImageVersion implements ImageProvider
func (p *DefaultImageProvider) ParseImageVersion(image string) (*ImageVersion, error) {
	return ParseImageVersion(image)
}

// ValidateImage implements ImageProvider
func (p *DefaultImageProvider) ValidateImage(image string) error {
	return ValidateImage(image)
}
