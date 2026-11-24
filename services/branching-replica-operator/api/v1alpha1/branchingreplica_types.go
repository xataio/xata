package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// BranchingReplicaSpec defines the desired state of a BranchingReplica.
type BranchingReplicaSpec struct{}

// BranchingReplicaStatus defines the observed state of a BranchingReplica.
type BranchingReplicaStatus struct{}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=brep
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// BranchingReplica is a permanent, never-promoted, never-connectable physical copy
// of an external PostgreSQL primary, fed only from the WAL-G archive the
// primary writes to object storage.
type BranchingReplica struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BranchingReplicaSpec   `json:"spec,omitempty"`
	Status BranchingReplicaStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// BranchingReplicaList contains a list of BranchingReplica
type BranchingReplicaList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BranchingReplica `json:"items"`
}

func init() {
	SchemeBuilder.Register(&BranchingReplica{}, &BranchingReplicaList{})
}
