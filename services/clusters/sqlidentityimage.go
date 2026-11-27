package clusters

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/distribution/reference"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	clustersv1 "xata/gen/proto/clusters/v1"
	"xata/internal/extensions"
	"xata/internal/postgresversions"
	"xata/services/branch-operator/api/v1alpha1"
)

// validateSQLIdentityImages checks a map of Postgres major version to image.
func validateSQLIdentityImages(images map[string]string) error {
	for major, image := range images {
		n, err := strconv.Atoi(major)
		if err != nil || n <= 0 || strconv.Itoa(n) != major {
			return fmt.Errorf("sql identity images: key %q is not a Postgres major version", major)
		}
		ref, err := reference.Parse(image)
		if err != nil {
			return fmt.Errorf("sql identity images: image for %s: %w", major, err)
		}
		if _, ok := ref.(reference.Tagged); !ok {
			return fmt.Errorf("sql identity images: image for %s has no tag", major)
		}
		if _, err := postgresversions.ParseImageVersion(image); err != nil {
			return fmt.Errorf("sql identity images: image for %s: %w", major, err)
		}
		if got := postgresversions.ExtractMajorVersionFromImage(image); got != n {
			return fmt.Errorf("sql identity images: image for %s has major version %d", major, got)
		}
		if len(extensions.GetExtensions(postgresversions.ShortImageName(image))) == 0 {
			return fmt.Errorf("sql identity images: image for %s has no extension list", major)
		}
	}
	return nil
}

// imageOffering returns the offering of an image, e.g. "postgres".
func imageOffering(image string) string {
	offering, _, _ := strings.Cut(postgresversions.ShortImageName(image), ":")
	return offering
}

// sqlIdentityImage returns the image from images for the major version of
// image. The error does not repeat image, which can name the provider.
func sqlIdentityImage(images map[string]string, image string) (string, error) {
	major := postgresversions.ExtractMajorVersionFromImage(image)
	if major == 0 {
		return "", status.Errorf(codes.InvalidArgument, "cannot read the Postgres major version of the image")
	}
	mapped, ok := images[strconv.Itoa(major)]
	if !ok {
		return "", imageUnavailableError("Postgres %d is not available in this cell", major)
	}
	if imageOffering(mapped) != imageOffering(image) {
		return "", imageUnavailableError("the %s offering is not available in this cell", imageOffering(image))
	}
	return mapped, nil
}

// WithSQLIdentityImage sets the image of a new Branch with a non-legacy SQL
// identity. A child keeps the image of its parent and a restore the image of
// its source. Another Branch takes the image that images gives for the major
// version of the requested image. Set the SQL identity and the parent
// overrides first.
func (b *BranchBuilder) WithSQLIdentityImage(images map[string]string, parent, restoreSource *v1alpha1.Branch) error {
	spec := &b.branch.Spec
	if spec.SQLIdentity == v1alpha1.SQLIdentityLegacy || parent != nil {
		return nil
	}
	if restoreSource != nil {
		spec.ClusterSpec.Image = restoreSource.Spec.ClusterSpec.Image
		return nil
	}
	image, err := sqlIdentityImage(images, spec.ClusterSpec.Image)
	if err != nil {
		return err
	}
	spec.ClusterSpec.Image = image
	return nil
}

// WithSQLIdentityImageUpdate replaces an image that the update sets on a
// Branch with a non-legacy SQL identity with the image that images gives for
// its major version. It refuses an image with an older minor version than
// current, the image of the Branch before the update.
func (b *BranchBuilder) WithSQLIdentityImageUpdate(images map[string]string, req *clustersv1.UpdatePostgresClusterRequest, current string) error {
	if b.branch.Spec.SQLIdentity == v1alpha1.SQLIdentityLegacy || req.GetUpdateConfiguration().ImageName == nil {
		return nil
	}
	image, err := sqlIdentityImage(images, req.GetUpdateConfiguration().GetImageName())
	if err != nil {
		return err
	}
	if olderMinor(image, current) {
		return imageUnavailableError("this cell offers an older Postgres minor version than the branch runs")
	}
	b.branch.Spec.ClusterSpec.Image = image
	return nil
}

// olderMinor reports whether image has the major version of current and an
// older minor version.
func olderMinor(image, current string) bool {
	to, err := postgresversions.ParseImageVersion(image)
	if err != nil {
		return false
	}
	from, err := postgresversions.ParseImageVersion(current)
	if err != nil {
		return false
	}
	return to.Major == from.Major && to.Minor < from.Minor
}
