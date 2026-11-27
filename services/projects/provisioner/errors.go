package provisioner

import "fmt"

type ErrBranchNotFound struct {
	BranchID string
}

func (e ErrBranchNotFound) Error() string {
	return fmt.Sprintf("branch %s not found", e.BranchID)
}

type ErrInvalidConfiguration struct {
	Name    string
	Message string
}

func (e ErrInvalidConfiguration) Error() string {
	return fmt.Sprintf("branch name: %s, message: %s", e.Name, e.Message)
}

type ErrParentBranchUnhealthy struct {
	ParentID string
}

func (e ErrParentBranchUnhealthy) Error() string {
	return fmt.Sprintf("parent branch %s is not healthy", e.ParentID)
}

// ErrImageUnavailable reports that the cell does not offer the requested
// image. Message comes from the cell and is safe to show to the user.
type ErrImageUnavailable struct {
	Message string
}

func (e ErrImageUnavailable) Error() string {
	return e.Message
}
