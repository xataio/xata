package wakeup

import (
	"context"
	"fmt"

	"xata/internal/postgresversions"
	"xata/services/branch-operator/api/v1alpha1"
	v1alpha1ac "xata/services/branch-operator/applyconfiguration/api/v1alpha1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// getBranch retrieves the Branch resource associated with the given name. If
// the Branch is not found, a ConditionError is returned indicating that the
// Branch was not found. Any other errors encountered during retrieval are
// returned as-is.
func (r *WakeupReconciler) getBranch(ctx context.Context, name string) (*v1alpha1.Branch, error) {
	branch := &v1alpha1.Branch{}

	// Get the Branch resource by name.
	err := r.Get(ctx, client.ObjectKey{Name: name}, branch)
	if err != nil && apierrors.IsNotFound(err) {
		err = &ConditionError{
			ConditionReason: v1alpha1.BranchNotFoundReason,
			Err:             err,
		}
		return nil, err
	}
	if err != nil {
		return nil, err
	}

	return branch, nil
}

// wakeupPoolName retrieves the name of the wakeup pool from the Branch's
// annotations. If the annotation is not present, a ConditionError is returned
// indicating that the required annotation is missing
func (r *WakeupReconciler) wakeupPoolName(branch *v1alpha1.Branch) (string, error) {
	if !branch.HasWakeupPoolAnnotation() {
		err := &ConditionError{
			ConditionReason: v1alpha1.NoPoolAnnotationReason,
			Err:             fmt.Errorf("missing %q annotation", v1alpha1.WakeupPoolAnnotation),
			Terminal:        true,
		}
		return "", err
	}

	return branch.WakeupPoolName(), nil
}

// assignClusterToBranch updates the Branch resource to set the given Cluster
// name and image in its spec and sets the awaiting wakeup annotation to
// false. The image must always be passed (normally the branch's current
// image, or the adopted pool cluster image, see adoptableImage): once this
// field manager has applied the image, omitting it from a later apply would
// make SSA remove the field from the Branch spec
func (r *WakeupReconciler) assignClusterToBranch(ctx context.Context, branch *v1alpha1.Branch, clusterName, image string) error {
	clusterSpec := v1alpha1ac.ClusterSpec().WithName(clusterName)
	if image != "" {
		clusterSpec = clusterSpec.WithImage(image)
	}

	ac := v1alpha1ac.Branch(branch.Name, "").
		WithAnnotations(map[string]string{v1alpha1.AwaitingWakeupAnnotation: "false"}).
		WithSpec(v1alpha1ac.BranchSpec().
			WithClusterSpec(clusterSpec))

	// Apply the Branch spec update using SSA
	err := r.Apply(ctx, ac, client.FieldOwner(ReconcilerName), client.ForceOwnership)
	if err != nil {
		return err
	}

	return nil
}

// adoptableImage returns the pool cluster's image when the branch should
// adopt it on wakeup: the same offering and major version as the branch's
// current image, with a newer minor. It returns "" when the branch's own
// image stays authoritative; the branch reconciler then rolls the adopted
// cluster to the branch's image, which remains the behavior for every other
// kind of mismatch. Adopting newer minors makes a pool image bump propagate
// to its branches for free on their next wakeup, instead of every wakeup
// paying an extra rollout to move the warm cluster back to the branch's
// older image.
func adoptableImage(branchImage, clusterImage string) string {
	if branchImage == "" || clusterImage == "" || branchImage == clusterImage {
		return ""
	}

	branchVersion, err := postgresversions.ParseImageVersion(branchImage)
	if err != nil {
		return ""
	}
	clusterVersion, err := postgresversions.ParseImageVersion(clusterImage)
	if err != nil {
		return ""
	}

	if clusterVersion.Offering != branchVersion.Offering ||
		clusterVersion.Major != branchVersion.Major ||
		clusterVersion.Minor <= branchVersion.Minor {
		return ""
	}

	return clusterImage
}
