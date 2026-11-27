package v1alpha1

// Condition types for WakeupRequest `status.conditions` fields
const (
	WakeupSucceededConditionType = "Succeeded"
)

// Reason strings for WakeupRequest conditions
const (
	// Reason strings for the `Succeeded` True condition
	WakeupSucceededReason = "WakeupSucceeded"

	// Reason strings for the `Succeeded` False condition (terminal errors)
	PVNotAvailableReason     = "PVNotAvailable"
	CSINodePodNotFoundReason = "CSINodePodNotFound"
	PoolNotFoundReason       = "WakeupPoolNotFound"
	NoPoolAnnotationReason   = "WakeupPoolAnnotationMissing"
	PoolSQLIdentityReason    = "WakeupPoolSQLIdentityMismatch"

	// Reason strings for the `Succeeded` Unknown condition (retryable errors
	// and in-progress states)
	WakeupReconciliationFailedReason   = "ReconciliationFailed"
	WakeupInProgressReason             = "InProgress"
	WakeupAwaitingReconciliationReason = "AwaitingReconciliation"
	BranchNotFoundReason               = "BranchNotFound"
	PoolExhaustedReason                = "WakeupPoolExhausted"
	XVolNotReadyReason                 = "XVolNotReady"
)

// WakeupConditionMessages maps condition reasons to human-readable messages
var WakeupConditionMessages = map[string]string{
	WakeupSucceededReason:              "Wakeup succeeded",
	WakeupReconciliationFailedReason:   "An error occurred during reconciliation",
	WakeupAwaitingReconciliationReason: "The wakeup request is awaiting reconciliation",
	BranchNotFoundReason:               "The specified branch was not found",
	NoPoolAnnotationReason:             "The branch has no wakeup pool annotation",
	PoolNotFoundReason:                 "The specified wakeup pool was not found",
	PoolSQLIdentityReason:              "The wakeup pool does not have the SQL identity of the branch",
	PoolExhaustedReason:                "The wakeup pool has no healthy clusters",
	WakeupInProgressReason:             "Wakeup is in progress",
	PVNotAvailableReason:               "The cluster's primary PV could not be resolved",
	CSINodePodNotFoundReason:           "No CSI node plugin pod found on the primary pod's node",
	XVolNotReadyReason:                 "The XVol status indicates it is not ready for wakeup",
	XVolNotFoundReason:                 "The XVol referenced by the branch was not found",
}
