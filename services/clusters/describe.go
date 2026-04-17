package clusters

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"

	apiv1 "github.com/xataio/xata-cnpg/api/v1"

	clustersv1 "xata/gen/proto/clusters/v1"
)

const (
	StatusUnknown         = "unknown"
	InstanceStatusUnknown = "Unknown"

	hibernationAnnotation = "cnpg.io/hibernation"
	hibernationEnabled    = "on"
)

// cluster states that are not final and where the operator can navigate without
// human intervention.
var transientClusterStates = map[string]struct{}{
	apiv1.PhaseSwitchover:                    {},
	apiv1.PhaseFailOver:                      {},
	apiv1.PhaseFirstPrimary:                  {},
	apiv1.PhaseCreatingReplica:               {},
	apiv1.PhaseUpgrade:                       {},
	apiv1.PhaseUpgradeDelayed:                {},
	apiv1.PhaseInplacePrimaryRestart:         {},
	apiv1.PhaseInplaceDeletePrimaryRestart:   {},
	apiv1.PhaseWaitingForInstancesToBeActive: {},
	apiv1.PhaseOnlineUpgrading:               {},
	apiv1.PhaseApplyingConfiguration:         {},
	apiv1.PhaseReplicaClusterPromotion:       {},
}

// BuildClusterStatus creates a ClusterStatus from a CNPG Cluster.
func BuildClusterStatus(cluster *apiv1.Cluster) *clustersv1.ClusterStatus {
	clusterStatus := StatusUnknown
	statusType := clustersv1.ClusterStatus_STATUS_TYPE_TRANSIENT

	if cluster.Status.Phase != "" {
		clusterStatus = cluster.Status.Phase
		if clusterStatus == apiv1.PhaseHealthy {
			statusType = clustersv1.ClusterStatus_STATUS_TYPE_HEALTHY
		} else if isTransientClusterState(clusterStatus) {
			statusType = clustersv1.ClusterStatus_STATUS_TYPE_TRANSIENT
		} else {
			statusType = clustersv1.ClusterStatus_STATUS_TYPE_FAULT
		}
	}

	if cluster.Annotations != nil && cluster.Annotations[hibernationAnnotation] == hibernationEnabled {
		statusType = clustersv1.ClusterStatus_STATUS_TYPE_HIBERNATED
	}

	status := clustersv1.ClusterStatus{
		Status:             clusterStatus,
		StatusType:         statusType,
		InstanceCount:      int64(cluster.Status.Instances),
		InstanceReadyCount: int64(cluster.Status.ReadyInstances),
		Instances:          map[string]*clustersv1.InstanceStatus{},
	}

	for instanceStatus, instanceList := range cluster.Status.InstancesStatus {
		for _, instanceID := range instanceList {
			if status.Instances[instanceID] == nil {
				status.Instances[instanceID] = &clustersv1.InstanceStatus{}
			}
			status.Instances[instanceID].Status = string(instanceStatus)
		}
	}

	if len(cluster.Status.InstancesStatus) == 0 {
		for _, instanceName := range cluster.Status.InstanceNames {
			if status.Instances[instanceName] == nil {
				status.Instances[instanceName] = &clustersv1.InstanceStatus{Status: InstanceStatusUnknown}
			}
		}
	}

	if cluster.Status.CurrentPrimary != "" {
		if _, ok := status.Instances[cluster.Status.CurrentPrimary]; !ok {
			status.Instances[cluster.Status.CurrentPrimary] = &clustersv1.InstanceStatus{Status: InstanceStatusUnknown}
		}
		status.Instances[cluster.Status.CurrentPrimary].Primary = true
	}
	if cluster.Status.TargetPrimary != "" {
		if _, ok := status.Instances[cluster.Status.TargetPrimary]; !ok {
			status.Instances[cluster.Status.TargetPrimary] = &clustersv1.InstanceStatus{Status: InstanceStatusUnknown}
		}
		status.Instances[cluster.Status.TargetPrimary].TargetPrimary = true
	}

	return &status
}

func isTransientClusterState(value string) bool {
	_, ok := transientClusterStates[value]
	return ok
}

func formatCPUResource(milliCPUs int) string {
	if milliCPUs < 1000 {
		return fmt.Sprintf("%dm", milliCPUs)
	}
	return fmt.Sprintf("%d", milliCPUs/1000)
}

func quantityGi(q resource.Quantity) int32 {
	return int32(q.Value() / (1024 * 1024 * 1024))
}

// quantityGiStringWithPoolerReservation converts a memory quantity back to the
// user-facing GB value by adding the pooler memory reservation before converting.
// This undoes the subtraction applied in resourceRequirements so the describe
// response matches the advertised instance RAM.
func quantityGiStringWithPoolerReservation(q resource.Quantity) string {
	q.Add(poolerMemoryReservation)
	return fmt.Sprintf("%d", quantityGi(q))
}
