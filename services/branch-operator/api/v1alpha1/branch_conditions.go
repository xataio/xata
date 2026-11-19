package v1alpha1

// Condition types for Branch `status.conditions` fields
const (
	BranchReadyConditionType = "Ready"

	// RestoreSourceAccessConditionType records the read access of the Branch
	// to the repository of its restore source.
	RestoreSourceAccessConditionType = "RestoreSourceAccess"
)

// Reason strings for Branch conditions
const (
	// Reason strings for the `Ready` True condition
	ResourcesReadyReason = "ResourcesReady"

	// Reason strings for the `Ready` False condition
	ParentBranchNotFoundReason     = "ParentBranchNotFound"
	ParentBranchHasNoXVolReason    = "ParentBranchHasNoXVol"
	ParentClusterNotFoundReason    = "ParentClusterNotFound"
	ParentBranchHasNoClusterReason = "ParentBranchHasNoCluster"
	ParentClusterUnhealthyReason   = "ParentClusterUnhealthy"
	ParentClusterPVCNotFoundReason = "ParentClusterPVCNotFound"
	ReconciliationFailedReason     = "ReconciliationFailed"
	XVolNotFoundReason             = "XVolNotFound"
	PgBackRestCipherLostReason     = "PgBackRestCipherLost"
	// AwaitingRestoreSourceAccessReason keeps the Cluster from being applied
	// until the restore can read its source.
	AwaitingRestoreSourceAccessReason = "AwaitingRestoreSourceAccess"
	// RestoreSourceRoleNotConfiguredReason keeps the Cluster from being
	// applied because the operator has no read-only role for the restore.
	RestoreSourceRoleNotConfiguredReason = "RestoreSourceRoleNotConfigured"

	// Reason strings for the `Ready` Unknown condition
	ReconciliationPausedReason = "ReconciliationPaused"

	// Reason strings for the `RestoreSourceAccess` condition
	RestoreSourceAccessGrantedReason  = "Granted"
	RestoreSourceAccessReleasedReason = "Released"

	// Shared reason strings for multiple conditions
	AwaitingReconciliationReason = "AwaitingReconciliation"
)

// BranchConditionMessages maps condition reasons to human-readable messages
var BranchConditionMessages = map[string]string{
	// Messages for the BranchReady condition
	ResourcesReadyReason:                 "All resources created",
	ParentBranchNotFoundReason:           "The parent branch was not found",
	ParentBranchHasNoXVolReason:          "The parent branch has no XVol",
	ParentClusterNotFoundReason:          "The parent cluster for the branch was not found",
	ParentBranchHasNoClusterReason:       "The parent branch has no cluster",
	ParentClusterUnhealthyReason:         "The parent cluster for the branch is not healthy",
	ParentClusterPVCNotFoundReason:       "The PVC for the parent cluster was not found",
	AwaitingReconciliationReason:         "The branch is awaiting reconciliation",
	ReconciliationPausedReason:           "Reconciliation has been paused",
	ReconciliationFailedReason:           "An error occurred during reconciliation",
	XVolNotFoundReason:                   "No XVol found for the primary volume",
	PgBackRestCipherLostReason:           "The pgbackreset cipher secret is missing",
	AwaitingRestoreSourceAccessReason:    "Waiting for read access to the restore source",
	RestoreSourceRoleNotConfiguredReason: "The operator has no read-only role for the restore source",

	// Messages for the RestoreSourceAccess condition
	RestoreSourceAccessGrantedReason:  "Read access to the restore source is granted",
	RestoreSourceAccessReleasedReason: "The restore is done and release of read access to the restore source is requested",
}
