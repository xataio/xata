package v1alpha1

import (
	cnpgv1 "github.com/xataio/xata-cnpg/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClusterPoolKind is the Kind string for the ClusterPool resource.
const ClusterPoolKind = "ClusterPool"

// ClusterPoolSpec defines the desired state of a ClusterPool
// +kubebuilder:validation:XValidation:rule="has(oldSelf.sqlIdentity) == has(self.sqlIdentity)",message="sqlIdentity is immutable"
type ClusterPoolSpec struct {
	// SQLIdentity declares the SQL identity profile of the clusters that
	// ClusterSpec makes. Only a Branch with the same profile takes a cluster
	// from the pool. When empty, the clusters have the legacy names.
	// +optional
	// +kubebuilder:validation:Enum=WhiteLabel
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="sqlIdentity is immutable"
	SQLIdentity string `json:"sqlIdentity,omitempty"`

	// Clusters is the target number of clusters to maintain in the pool.
	// When a cluster is removed from the pool the controller will
	// create another cluster to replace it.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Required
	Clusters int32 `json:"clusters"`

	// ClusterSpec is an embedded CNPG cluster spec. All clusters in the
	// pool will use this spec.
	// +kubebuilder:validation:Required
	ClusterSpec cnpgv1.ClusterSpec `json:"clusterSpec"`

	// PoolerSpec, when set, pre-provisions a PgBouncer PoolerSpec for every Cluster in
	// the pool, named "<cluster>-pooler". A Branch that claims a pool cluster
	// adopts its pooler the same way it adopts the cluster, so the pooler is
	// already running when the branch wakes up.
	// +optional
	PoolerSpec *PoolerSpec `json:"poolerSpec,omitempty"`

	// PgBackRestWebIdentityRoleARN is the ARN of the IAM role that pgBackRest
	// uses for Branches with s3.keyType=web-id. If it is set, each pool cluster
	// gets the web identity token volume and the AWS_ROLE_ARN environment
	// variable.
	// +optional
	PgBackRestWebIdentityRoleARN string `json:"pgbackrestWebIdentityRoleARN,omitempty"`

	// PgBackRestGCSWebIdentity gives each pool cluster the web identity token
	// volume and the PGBACKREST_REPO1_GCS_KEY environment variable, which
	// pgBackRest uses for Branches with gcs.keyType=web-id.
	// +optional
	PgBackRestGCSWebIdentity bool `json:"pgbackrestGCSWebIdentity,omitempty"`
}

// PoolerSpec defines the desired state of a pre-provisioned PgBouncer Pooler
// for Clusters in the pool. Its fields mirror the branch-operator's
// Branch.Spec.Pooler so a pre-walmed pooler matches what the Branch applies
// when it adopts the pooler.
type PoolerSpec struct {
	// Instances is the number of PgBouncer instances. Values below 1 are
	// treated as 1 so the pooler stays warm while waiting in the pool.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=5
	// +optional
	// +kubebuilder:default:=1
	Instances int32 `json:"instances,omitempty"`

	// Mode is the PgBouncer pool mode.
	// +kubebuilder:validation:Enum=session;transaction
	// +optional
	// +kubebuilder:default:="session"
	Mode string `json:"mode,omitempty"`

	// MaxClientConn is the maximum number of client connections to PgBouncer.
	// +optional
	// +kubebuilder:default:="100"
	MaxClientConn string `json:"maxClientConn,omitempty"`

	// DefaultPoolSize overrides the PgBouncer default_pool_size parameter.
	// When empty the PgBouncer default is left in place.
	// +optional
	// +kubebuilder:validation:Pattern="^[1-9][0-9]*$"
	DefaultPoolSize string `json:"defaultPoolSize,omitempty"`
}

// ClusterPoolStatus defines the observed state of a ClusterPool
type ClusterPoolStatus struct {
	// Clusters is the current number of clusters owned by the pool
	// +optional
	Clusters int32 `json:"clusters,omitempty"`

	// ObservedGeneration reflects the generation of the most recently observed ClusterPool spec
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations of the ClusterPool's state
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// LastError contains the last error message encountered during
	// reconciliation, if any.
	// +optional
	LastError string `json:"lastError,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.clusters,statuspath=.status.clusters
// +kubebuilder:resource:shortName=cp
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Desired",type="integer",JSONPath=".spec.clusters"
// +kubebuilder:printcolumn:name="Current",type="integer",JSONPath=".status.clusters"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type ClusterPool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClusterPoolSpec   `json:"spec,omitempty"`
	Status ClusterPoolStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ClusterPoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterPool `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ClusterPool{}, &ClusterPoolList{})
}
