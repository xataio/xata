package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BranchingReplicaSpec defines the desired state of a BranchingReplica
type BranchingReplicaSpec struct {
	// Source is the WAL-G archive the replica bootstraps from and follows
	// +kubebuilder:validation:Required
	Source SourceSpec `json:"source"`

	// Postgres configures the PostgreSQL container of the replica
	// +kubebuilder:validation:Required
	Postgres PostgresSpec `json:"postgres"`

	// Storage configures the volume that holds the replica's data directory
	// +kubebuilder:validation:Required
	Storage StorageSpec `json:"storage"`

	// WALG configures the WAL-G sidecar that fetches the base backup and the
	// WAL from the archive. The archive itself is described by Source.
	// +optional
	WALG *WALGSpec `json:"walg,omitempty"`
}

// SourceSpec describes the WAL-G archive of the source PostgreSQL instance
type SourceSpec struct {
	// S3 is the location of the archive in S3, or in an S3-compatible store,
	// and the credentials to read it
	// +kubebuilder:validation:Required
	S3 S3Spec `json:"s3"`
}

// S3Spec describes a WAL-G archive in S3, or in an S3-compatible store, and
// the credentials to read it
type S3Spec struct {
	// Prefix is the exact prefix that the source's WAL-G writes to, for
	// example s3://bucket/path/to/folder. WAL-G reads it verbatim, with no
	// version suffix appended.
	// +kubebuilder:validation:Required
	Prefix string `json:"prefix"`

	// Region is the region of the bucket
	// +optional
	Region string `json:"region,omitempty"`

	// EndpointURL is the endpoint of an S3-compatible store. If it is not
	// set, AWS S3 is used.
	// +optional
	EndpointURL string `json:"endpointUrl,omitempty"`

	// ForcePathStyle enables path-style addressing, for S3-compatible stores
	// that do not support bucket subdomains
	// +optional
	ForcePathStyle bool `json:"forcePathStyle,omitempty"`

	// AccessKeyIDRef references the access key ID of read-only credentials
	// for the archive
	// +kubebuilder:validation:Required
	AccessKeyIDRef corev1.SecretKeySelector `json:"accessKeyIdRef"`

	// AccessKeySecretRef references the secret access key of read-only
	// credentials for the archive
	// +kubebuilder:validation:Required
	AccessKeySecretRef corev1.SecretKeySelector `json:"accessKeySecretRef"`

	// SessionTokenRef references the session token of temporary credentials.
	// Set it when the access key belongs to a session. The token is read
	// when a pod starts, so pods must be restarted after the Secret changes.
	// +optional
	SessionTokenRef *corev1.SecretKeySelector `json:"sessionTokenRef,omitempty"`
}

// StorageSpec configures the volume of the replica
type StorageSpec struct {
	// Size is the size of the volume, for example 100Gi
	// +kubebuilder:validation:Required
	Size string `json:"size"`

	// StorageClass is the storage class of the volume. If it is not set, the
	// default storage class is used.
	// +optional
	StorageClass *string `json:"storageClass,omitempty"`
}

// PostgresSpec configures the PostgreSQL container of the replica
type PostgresSpec struct {
	// Image is the PostgreSQL container image to use. Its major version must
	// match the major version of the source.
	// +kubebuilder:validation:Required
	Image string `json:"image"`

	// Resources are the resource requirements of the PostgreSQL container
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Parameters are PostgreSQL configuration parameters. PostgreSQL aborts
	// recovery if max_connections, max_worker_processes, max_wal_senders,
	// max_prepared_transactions or max_locks_per_transaction is lower than on
	// the source, so those must be set to at least the source's values.
	// +optional
	Parameters map[string]string `json:"parameters,omitempty"`
}

// WALGSpec configures the WAL-G sidecar, in the recovery Job and in the
// instance pod alike
type WALGSpec struct {
	// DownloadConcurrency is the number of parallel WAL-G download streams.
	// Sidecar memory grows with it. If it is not set, WAL-G chooses at
	// runtime.
	// +kubebuilder:validation:Minimum=1
	// +optional
	DownloadConcurrency *int `json:"downloadConcurrency,omitempty"`

	// Resources are the resource requirements of the sidecar. Changing them
	// does not roll existing pods.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

// BranchingReplicaStatus defines the observed state of a BranchingReplica
type BranchingReplicaStatus struct{}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=brep
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// BranchingReplica is a permanent, never-promoted, never-connectable physical copy
// of an external PostgreSQL primary, fed only from the WAL-G archive the
// primary writes to object storage
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
