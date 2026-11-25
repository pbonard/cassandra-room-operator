package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CassandraRoomDatacenterSpec is one data center of a cluster, in the
// Kubernetes cluster that runs it (cass-operator's CassandraDatacenter).
// A CassandraRoomCluster creates and updates them; one can also be created
// on its own, for a cluster of a single data center.
type CassandraRoomDatacenterSpec struct {
	// ClusterName is the cassandra-room cluster's name (cluster_name).
	ClusterName string `json:"clusterName"`
	// DatacenterName is the data center's name in cassandra-room (default:
	// the object's name).
	// +optional
	DatacenterName string `json:"datacenterName,omitempty"`
	// Image of cassandra-room.
	Image string `json:"image"`
	// AgentImage is the operator's image, which initialises nodes and
	// runs the backup agent (default: the operator's own).
	// +optional
	AgentImage string `json:"agentImage,omitempty"`
	// +optional
	ImagePullPolicy string `json:"imagePullPolicy,omitempty"`
	// +optional
	ImagePullSecrets []SecretRef `json:"imagePullSecrets,omitempty"`
	// Size is the number of nodes.
	// +kubebuilder:validation:Minimum=0
	Size int32 `json:"size"`
	// Racks spread the nodes (default: one rack, "rack1").
	// +optional
	Racks []Rack `json:"racks,omitempty"`
	// +optional
	Storage StorageSpec `json:"storage,omitempty"`
	// +optional
	Pod PodOptions `json:"pod,omitempty"`
	// +optional
	Config ServerConfig `json:"config,omitempty"`
	// +optional
	TLS TLSSpec `json:"tls,omitempty"`
	// +optional
	Networking NetworkingSpec `json:"networking,omitempty"`
	// SuperuserSecret holds the username and password of the superuser the
	// operator creates and uses (default: <cluster>-superuser, created
	// with a random password).
	// +optional
	SuperuserSecret string `json:"superuserSecret,omitempty"`
	// AdditionalSeeds are internode addresses of other data centers'
	// seeds ("host:port").
	// +optional
	AdditionalSeeds []string `json:"additionalSeeds,omitempty"`
	// CMSMembers, when set, are the metadata service's members the
	// operator maintains (internode addresses). The CassandraRoomCluster
	// sets it on one data center only.
	// +optional
	CMSMembers []string `json:"cmsMembers,omitempty"`
	// ManageCMS makes this data center choose and maintain the metadata
	// service's members itself: up to three of its seeds (a cluster of
	// one data center).
	// +optional
	ManageCMS bool `json:"manageCMS,omitempty"`
	// Backup configures the backup agent run beside each node.
	// +optional
	Backup *S3Storage `json:"backup,omitempty"`
	// ReplaceNodes are pods whose node is dead and lost its data: each is
	// recreated empty and takes the dead node's place (replace_address).
	// +optional
	ReplaceNodes []string `json:"replaceNodes,omitempty"`
	// Stopped scales the data center to no pods, keeping its volumes.
	// +optional
	Stopped bool `json:"stopped,omitempty"`
	// RollingRestartRequestedAt restarts the nodes, one rack at a time, when
	// it changes (any value, typically a time).
	// +optional
	RollingRestartRequestedAt string `json:"rollingRestartRequestedAt,omitempty"`
	// ManagementPort is the management API's port (default 7199).
	// +optional
	ManagementPort int32 `json:"managementPort,omitempty"`
}

// NodeState is what the operator knows of a node.
type NodeState struct {
	Pod     string `json:"pod"`
	Rack    string `json:"rack"`
	HostID  string `json:"hostId,omitempty"`
	Address string `json:"address,omitempty"` // internode, its identity
	// Status in the ring: NORMAL, JOINING, LEAVING, LEFT.
	Status string `json:"status,omitempty"`
	Ready  bool   `json:"ready"`
}

// DatacenterPhase is a data center's state.
type DatacenterPhase string

const (
	PhaseInitializing DatacenterPhase = "Initializing"
	PhaseReady        DatacenterPhase = "Ready"
	PhaseUpdating     DatacenterPhase = "Updating"
	PhaseScaling      DatacenterPhase = "Scaling"
	PhaseStopped      DatacenterPhase = "Stopped"
)

// CassandraRoomDatacenterStatus is the observed state of a data center.
type CassandraRoomDatacenterStatus struct {
	// +optional
	Phase DatacenterPhase `json:"phase,omitempty"`
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// ReadyNodes counts nodes in the ring and serving clients.
	// +optional
	ReadyNodes int32 `json:"readyNodes"`
	// +optional
	Nodes []NodeState `json:"nodes,omitempty"`
	// SeedAddresses are the internode addresses of this data center's
	// seeds, for the other data centers.
	// +optional
	SeedAddresses []string `json:"seedAddresses,omitempty"`
	// CMSMembers are the metadata service's members, as last seen.
	// +optional
	CMSMembers []string `json:"cmsMembers,omitempty"`
	// SuperuserCreated is set once the superuser role exists.
	// +optional
	SuperuserCreated bool `json:"superuserCreated,omitempty"`
	// ReplacedNodes maps pods being replaced to the host id they replace.
	// +optional
	ReplacedNodes map[string]string `json:"replacedNodes,omitempty"`
	// LastRollingRestart is the RollingRestartRequestedAt last done.
	// +optional
	LastRollingRestart string `json:"lastRollingRestart,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=crdc
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterName`
// +kubebuilder:printcolumn:name="Size",type=integer,JSONPath=`.spec.size`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyNodes`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// CassandraRoomDatacenter is a data center of a cassandra-room cluster.
type CassandraRoomDatacenter struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CassandraRoomDatacenterSpec   `json:"spec,omitempty"`
	Status CassandraRoomDatacenterStatus `json:"status,omitempty"`
}

// DCName is the data center's name in cassandra-room.
func (dc *CassandraRoomDatacenter) DCName() string {
	if dc.Spec.DatacenterName != "" {
		return dc.Spec.DatacenterName
	}
	return dc.Name
}

// +kubebuilder:object:root=true

// CassandraRoomDatacenterList lists data centers.
type CassandraRoomDatacenterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CassandraRoomDatacenter `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CassandraRoomDatacenter{}, &CassandraRoomDatacenterList{})
}
