package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ServerConfig is the configuration of cassandra-room nodes (what
// cassandra.yaml holds for Cassandra), passed to the server as flags.
type ServerConfig struct {
	// NumTokens is the number of vnodes of each node (num_tokens).
	// +optional
	NumTokens *int32 `json:"numTokens,omitempty"`
	// CommitlogSync is "periodic" (the default) or "batch".
	// +kubebuilder:validation:Enum=periodic;batch
	// +optional
	CommitlogSync string `json:"commitlogSync,omitempty"`
	// CommitlogSyncPeriod is the fsync period in periodic mode ("10s").
	// +optional
	CommitlogSyncPeriod string `json:"commitlogSyncPeriod,omitempty"`
	// MemtableSpace bounds the memtables of all tables
	// (memtable_heap_space).
	// +optional
	MemtableSpace *resource.Quantity `json:"memtableSpace,omitempty"`
	// Authorizer is AllowAllAuthorizer or CassandraAuthorizer (the
	// default). The authenticator is always PasswordAuthenticator.
	// +kubebuilder:validation:Enum=AllowAllAuthorizer;CassandraAuthorizer
	// +optional
	Authorizer string `json:"authorizer,omitempty"`
	// ExtraArgs are further server flags ("-name=value"), after the
	// operator's.
	// +optional
	ExtraArgs []string `json:"extraArgs,omitempty"`
}

// TLSSpec configures encryption, from secrets of type kubernetes.io/tls
// holding tls.crt, tls.key and ca.crt (as cert-manager writes them). In
// a multi-cluster deployment, each secret must exist in every cluster.
type TLSSpec struct {
	// Internode encrypts the connections between nodes, mutually
	// authenticated.
	// +optional
	Internode *InternodeTLS `json:"internode,omitempty"`
	// Client encrypts the native protocol port.
	// +optional
	Client *ClientTLS `json:"client,omitempty"`
	// Management encrypts the management API; the operator and the
	// backup agent present the same secret's certificate.
	// +optional
	Management *SecretRef `json:"management,omitempty"`
}

// SecretRef names a secret.
type SecretRef struct {
	SecretName string `json:"secretName"`
}

// InternodeTLS is server_encryption_options.
type InternodeTLS struct {
	SecretName string `json:"secretName"`
	// VerifyHostname also checks peers' certificate names
	// (require_endpoint_verification).
	// +optional
	VerifyHostname bool `json:"verifyHostname,omitempty"`
}

// ClientTLS is client_encryption_options.
type ClientTLS struct {
	SecretName string `json:"secretName"`
	// RequireClientAuth requires client certificates signed by the
	// secret's ca.crt.
	// +optional
	RequireClientAuth bool `json:"requireClientAuth,omitempty"`
	// Optional also accepts unencrypted clients on the same port.
	// +optional
	Optional bool `json:"optional,omitempty"`
}

// NetworkingMode is how nodes are reached from other nodes and clients.
// +kubebuilder:validation:Enum=PodIP;HostNetwork;LoadBalancer
type NetworkingMode string

const (
	// NetworkingPodIP uses pod IPs: one Kubernetes cluster, or clusters
	// whose pod networks are routed to each other.
	NetworkingPodIP NetworkingMode = "PodIP"
	// NetworkingHostNetwork runs nodes on their hosts' network and
	// addresses (one node per host).
	NetworkingHostNetwork NetworkingMode = "HostNetwork"
	// NetworkingLoadBalancer gives each node a Service of type
	// LoadBalancer, whose address is its broadcast address.
	NetworkingLoadBalancer NetworkingMode = "LoadBalancer"
)

// NetworkingSpec configures addresses.
type NetworkingSpec struct {
	// Mode is PodIP (the default), HostNetwork or LoadBalancer.
	// +optional
	Mode NetworkingMode `json:"mode,omitempty"`
	// PreferLocal makes nodes of a data center connect to each other at
	// their pod addresses (with HostNetwork or LoadBalancer).
	// +optional
	PreferLocal bool `json:"preferLocal,omitempty"`
	// LoadBalancerAnnotations are put on the per-node Services
	// (LoadBalancer mode), for instance to make them internal.
	// +optional
	LoadBalancerAnnotations map[string]string `json:"loadBalancerAnnotations,omitempty"`
}

// StorageSpec is each node's data volume.
type StorageSpec struct {
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`
	// Size of the volume (default 10Gi).
	// +optional
	Size *resource.Quantity `json:"size,omitempty"`
}

// Rack is a failure domain of a data center: its nodes are spread evenly
// over the racks, and replicas over racks.
type Rack struct {
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name"`
	// NodeAffinityLabels place the rack's pods on nodes with these labels
	// (for instance a zone).
	// +optional
	NodeAffinityLabels map[string]string `json:"nodeAffinityLabels,omitempty"`
}

// PodOptions are scheduling and resource settings of the nodes' pods.
type PodOptions struct {
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
	// AllowMultipleNodesPerWorker lets two cassandra-room nodes share a
	// Kubernetes node (by default they repel each other).
	// +optional
	AllowMultipleNodesPerWorker bool `json:"allowMultipleNodesPerWorker,omitempty"`
	// ServiceAccountName runs the pods as this service account (default:
	// one the operator creates).
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`
}

// S3Storage is a bucket of an S3-compatible object store holding backups.
type S3Storage struct {
	Bucket string `json:"bucket"`
	// Prefix of the objects (default: none).
	// +optional
	Prefix string `json:"prefix,omitempty"`
	// +optional
	Region string `json:"region,omitempty"`
	// Endpoint of an S3-compatible store (MinIO...); empty for AWS.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`
	// ForcePathStyle addresses buckets by path (MinIO).
	// +optional
	ForcePathStyle bool `json:"forcePathStyle,omitempty"`
	// CredentialsSecret holds access_key_id and secret_access_key; without
	// it, the default AWS credential chain (IRSA, instance role) applies.
	// +optional
	CredentialsSecret *SecretRef `json:"credentialsSecret,omitempty"`
	// Concurrency is the number of files a node uploads or downloads at
	// once (default 4).
	// +optional
	Concurrency int32 `json:"concurrency,omitempty"`
	// CommitLogArchive keeps every node's commit log in the store too, so
	// that a restore can reach any point in time since the oldest backup.
	// +optional
	CommitLogArchive *CommitLogArchive `json:"commitLogArchive,omitempty"`
}

// CommitLogArchive uploads the nodes' commit logs (Medusa's point-in-time
// restores, Cassandra's commit log archiving).
type CommitLogArchive struct {
	// Interval between upload rounds (default 1m). Each round closes the
	// node's commit log segment first; a point in time can be restored once
	// a round passed it.
	// +optional
	Interval *metav1.Duration `json:"interval,omitempty"`
}

// Condition types.
const (
	ConditionReady       = "Ready"
	ConditionProgressing = "Progressing"
	ConditionFailed      = "Failed"
)
