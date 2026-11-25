package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CassandraRoomClusterSpec is a cassandra-room cluster of one or more data
// centers, each in the Kubernetes cluster its K8sContext names
// (k8ssandra-operator's K8ssandraCluster). The cluster's controller runs in
// one Kubernetes cluster (the control plane) and creates a
// CassandraRoomDatacenter in each data center's cluster, whose operator
// (the data plane) runs it.
type CassandraRoomClusterSpec struct {
	// Image of cassandra-room.
	Image string `json:"image"`
	// AgentImage is the operator's image for the nodes' init and backup
	// containers (default: the operator's own).
	// +optional
	AgentImage string `json:"agentImage,omitempty"`
	// +optional
	ImagePullPolicy string `json:"imagePullPolicy,omitempty"`
	// +optional
	ImagePullSecrets []SecretRef `json:"imagePullSecrets,omitempty"`
	// SuperuserSecret holds the superuser's username and password (default
	// <name>-superuser, created with a random password and copied to
	// every data center's cluster).
	// +optional
	SuperuserSecret string `json:"superuserSecret,omitempty"`
	// Config applies to every data center, under its own.
	// +optional
	Config ServerConfig `json:"config,omitempty"`
	// +optional
	TLS TLSSpec `json:"tls,omitempty"`
	// +optional
	Networking NetworkingSpec `json:"networking,omitempty"`
	// Datacenters are created in order, each once the previous ones are
	// ready.
	// +kubebuilder:validation:MinItems=1
	Datacenters []DatacenterTemplate `json:"datacenters"`
	// Backup is the object store the clusters' backups go to.
	// +optional
	Backup *S3Storage `json:"backup,omitempty"`
	// Repairs are scheduled repairs, run from the first data center.
	// +optional
	Repairs []RepairTemplate `json:"repairs,omitempty"`
}

// DatacenterTemplate is a data center of a cluster.
type DatacenterTemplate struct {
	// Name of the data center in cassandra-room.
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name"`
	// K8sContext is the ClientConfig of the Kubernetes cluster running the
	// data center (empty: the control plane's own).
	// +optional
	K8sContext string `json:"k8sContext,omitempty"`
	// Namespace in that cluster (default: the cluster object's).
	// +optional
	Namespace string `json:"namespace,omitempty"`
	// +kubebuilder:validation:Minimum=0
	Size int32 `json:"size"`
	// +optional
	Racks []Rack `json:"racks,omitempty"`
	// +optional
	Storage StorageSpec `json:"storage,omitempty"`
	// +optional
	Pod PodOptions `json:"pod,omitempty"`
	// Config overrides the cluster's for this data center.
	// +optional
	Config *ServerConfig `json:"config,omitempty"`
	// Networking overrides the cluster's for this data center.
	// +optional
	Networking *NetworkingSpec `json:"networking,omitempty"`
	// +optional
	Stopped bool `json:"stopped,omitempty"`
	// +optional
	ReplaceNodes []string `json:"replaceNodes,omitempty"`
	// +optional
	RollingRestartRequestedAt string `json:"rollingRestartRequestedAt,omitempty"`
}

// RepairTemplate is a scheduled repair of the cluster.
type RepairTemplate struct {
	// Name of the RepairSchedule created (prefixed by the cluster's).
	Name string                 `json:"name"`
	Spec RepairScheduleSettings `json:"spec"`
}

// DatacenterSummary is a data center's state, seen from the control plane.
type DatacenterSummary struct {
	Name       string          `json:"name"`
	K8sContext string          `json:"k8sContext,omitempty"`
	Phase      DatacenterPhase `json:"phase,omitempty"`
	ReadyNodes int32           `json:"readyNodes"`
	Size       int32           `json:"size"`
	Seeds      []string        `json:"seeds,omitempty"`
}

// CassandraRoomClusterStatus is the observed state of a cluster.
type CassandraRoomClusterStatus struct {
	// +optional
	Phase DatacenterPhase `json:"phase,omitempty"`
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	Datacenters []DatacenterSummary `json:"datacenters,omitempty"`
	// CMSMembers are the metadata service's members the operator chose.
	// +optional
	CMSMembers []string `json:"cmsMembers,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=crc
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// CassandraRoomCluster is a cassandra-room cluster across data centers and
// Kubernetes clusters.
type CassandraRoomCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CassandraRoomClusterSpec   `json:"spec,omitempty"`
	Status CassandraRoomClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// CassandraRoomClusterList lists clusters.
type CassandraRoomClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CassandraRoomCluster `json:"items"`
}

// ClientConfigSpec gives access to another Kubernetes cluster
// (k8ssandra-operator's ClientConfig).
type ClientConfigSpec struct {
	// KubeConfigSecret holds a kubeconfig (key "kubeconfig").
	KubeConfigSecret SecretRef `json:"kubeConfigSecret"`
	// ContextName is the kubeconfig's context to use (default: its
	// current one).
	// +optional
	ContextName string `json:"contextName,omitempty"`
}

// +kubebuilder:object:root=true

// ClientConfig is a remote Kubernetes cluster that data centers run in.
type ClientConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec ClientConfigSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// ClientConfigList lists client configs.
type ClientConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClientConfig `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CassandraRoomCluster{}, &CassandraRoomClusterList{}, &ClientConfig{}, &ClientConfigList{})
}
