package clusters

import (
	stderrors "errors"
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/errors"
)

// errBackupsNotEnabled is returned when an update sets the backup
// configuration of a branch that has no backups.
var errBackupsNotEnabled = stderrors.New("backups are not enabled on the branch")

// ClusterNotFoundError is returned when trying to retrieve/update/delete a cluster that does not exist
func ClusterNotFoundError(clusterID string) error {
	return status.Errorf(codes.NotFound, "cluster [%s] not found", clusterID)
}

// SecretNotFoundForIDError is returned when trying to retrieve/update/delete a secret that does not exist
func SecretNotFoundForIDError(id string) error {
	return status.Errorf(codes.NotFound, "secret not found for ID: [%s]", id)
}

// RestoreSourceUnsupportedReason is the ErrorInfo reason of the error that
// refuses a restore source.
const RestoreSourceUnsupportedReason = "RESTORE_SOURCE_UNSUPPORTED"

// ImageUnavailableReason is the ErrorInfo reason of the error that refuses an
// image the cell does not offer. Its message is safe to show to the user.
const ImageUnavailableReason = "IMAGE_UNAVAILABLE"

// restoreSourceUnsupportedError refuses a restore from the backups of source.
func restoreSourceUnsupportedError(source, reason string) error {
	return failedPrecondition(RestoreSourceUnsupportedReason, fmt.Sprintf("restore source %s: %s", source, reason))
}

// imageUnavailableError refuses an image the cell does not offer.
func imageUnavailableError(format string, args ...any) error {
	return failedPrecondition(ImageUnavailableReason, fmt.Sprintf(format, args...))
}

// failedPrecondition returns a FailedPrecondition error tagged with reason.
func failedPrecondition(reason, msg string) error {
	st := status.New(codes.FailedPrecondition, msg)
	withDetails, err := st.WithDetails(&errdetails.ErrorInfo{
		Reason: reason,
		Domain: "xata",
	})
	if err != nil {
		return st.Err()
	}
	return withDetails.Err()
}

// IsRestoreSourceUnsupportedError reports whether err refuses a restore source.
func IsRestoreSourceUnsupportedError(err error) bool {
	return isFailedPrecondition(err, RestoreSourceUnsupportedReason)
}

// IsImageUnavailableError reports whether err refuses an image the cell does
// not offer.
func IsImageUnavailableError(err error) bool {
	return isFailedPrecondition(err, ImageUnavailableReason)
}

// isFailedPrecondition reports whether err is a FailedPrecondition error
// tagged with reason.
func isFailedPrecondition(err error, reason string) bool {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		return false
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetReason() == reason {
			return true
		}
	}
	return false
}

// k8sErrorToGRPCError maps Kubernetes API errors to gRPC errors
func k8sErrorToGRPCError(err error) error {
	switch {
	case errors.IsNotFound(err):
		return status.Errorf(codes.NotFound, "resource not found: %v", err)
	case errors.IsAlreadyExists(err):
		return status.Errorf(codes.AlreadyExists, "resource already exists: %v", err)
	case errors.IsInvalid(err):
		return status.Errorf(codes.InvalidArgument, "invalid resource: %v", err)
	case errors.IsForbidden(err):
		return status.Errorf(codes.PermissionDenied, "forbidden: %v", err)
	case errors.IsConflict(err):
		return status.Errorf(codes.Aborted, "conflict: %v", err)
	default:
		return status.Errorf(codes.Unknown, "kubernetes error: %v", err)
	}
}
