// Package names are the names and labels of the objects the operator
// creates, and the ports and paths shared by its components.
package names

import (
	"fmt"
	"strings"

	v1 "github.com/pbonard/cassandra-room-operator/operator/api/v1alpha1"
)

const (
	Group = "cassandraroom.pbonard.github.io"

	LabelName      = "app.kubernetes.io/name"
	LabelManagedBy = "app.kubernetes.io/managed-by"
	LabelCluster   = Group + "/cluster"
	LabelDC        = Group + "/datacenter"
	LabelDCObject  = Group + "/datacenter-object"
	LabelRack      = Group + "/rack"
	LabelSeed      = Group + "/seed"

	// AnnotationRestartedAt on a pod template restarts its pods.
	AnnotationRestartedAt = Group + "/restarted-at"
	// AnnotationConfigHash on a pod template restarts its pods when the
	// configuration they read at start changes.
	AnnotationConfigHash = Group + "/config-hash"

	AppName  = "cassandra-room"
	Operator = "cassandra-room-operator"

	NativePort     = 9042
	InternodePort  = 7000
	ManagementPort = 7199
	AgentPort      = 8090

	DataDir     = "/var/lib/cassandra-room"
	ConfigDir   = "/etc/cassandra-room"
	FlagsFile   = ConfigDir + "/flags"
	TLSDir      = "/etc/cassandra-room-tls"
	DataVolume  = "data"
	ConfigVol   = "node-config"
	MaxSeedsPer = 3 // seeds of a data center
)

// DCResource names the data center object's resources:
// <cluster>-<dc>[-suffix].
func DCResource(dc *v1.CassandraRoomDatacenter, suffix string) string {
	base := Sanitize(dc.Spec.ClusterName + "-" + dc.DCName())
	if suffix == "" {
		return base
	}
	return base + "-" + suffix
}

// StatefulSet names a rack's StatefulSet.
func StatefulSet(dc *v1.CassandraRoomDatacenter, rack string) string {
	return DCResource(dc, Sanitize(rack))
}

// Pod names a rack's pod.
func Pod(dc *v1.CassandraRoomDatacenter, rack string, ordinal int) string {
	return fmt.Sprintf("%s-%d", StatefulSet(dc, rack), ordinal)
}

// PVC names a pod's data volume claim.
func PVC(pod string) string { return DataVolume + "-" + pod }

// HeadlessService has every pod, ready or not (peer discovery, DNS).
func HeadlessService(dc *v1.CassandraRoomDatacenter) string { return DCResource(dc, "all-pods") }

// ClientService has the ready pods, for clients.
func ClientService(dc *v1.CassandraRoomDatacenter) string { return DCResource(dc, "service") }

// NodeConfig is the ConfigMap node-init reads: seeds, replacements.
func NodeConfig(dc *v1.CassandraRoomDatacenter) string { return DCResource(dc, "node-config") }

// LoadBalancer names a pod's own Service (LoadBalancer networking).
func LoadBalancer(pod string) string { return pod + "-lb" }

// ServiceAccount runs the nodes (node-init reads its ConfigMap and
// Services).
func ServiceAccount(dc *v1.CassandraRoomDatacenter) string { return DCResource(dc, "node") }

// SuperuserSecret is the default superuser secret of a cluster.
func SuperuserSecret(cluster string) string { return Sanitize(cluster) + "-superuser" }

// RepairPlan is the ConfigMap holding a repair run's segments.
func RepairPlan(schedule string) string { return schedule + "-plan" }

// Labels of a data center's objects.
func Labels(dc *v1.CassandraRoomDatacenter) map[string]string {
	return map[string]string{
		LabelName:      AppName,
		LabelManagedBy: Operator,
		LabelCluster:   Sanitize(dc.Spec.ClusterName),
		LabelDC:        Sanitize(dc.DCName()),
		LabelDCObject:  dc.Name,
	}
}

// RackLabels are a rack's pods' labels.
func RackLabels(dc *v1.CassandraRoomDatacenter, rack string) map[string]string {
	l := Labels(dc)
	l[LabelRack] = Sanitize(rack)
	return l
}

// Selector selects a data center's pods.
func Selector(dc *v1.CassandraRoomDatacenter) map[string]string {
	return map[string]string{LabelDCObject: dc.Name, LabelName: AppName}
}

// Sanitize makes a string a valid name or label value.
func Sanitize(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	return strings.Trim(b.String(), "-")
}

// Racks returns the data center's racks (one, "rack1", by default).
func Racks(dc *v1.CassandraRoomDatacenter) []v1.Rack {
	if len(dc.Spec.Racks) == 0 {
		return []v1.Rack{{Name: "rack1"}}
	}
	return dc.Spec.Racks
}

// RackSizes spreads size nodes over n racks, the first ones taking the
// remainder.
func RackSizes(size int32, n int) []int32 {
	out := make([]int32, n)
	for i := range out {
		out[i] = size / int32(n)
		if int32(i) < size%int32(n) {
			out[i]++
		}
	}
	return out
}

// ManagementPort is the data center's management API port.
func MgmtPort(dc *v1.CassandraRoomDatacenter) int32 {
	if dc.Spec.ManagementPort != 0 {
		return dc.Spec.ManagementPort
	}
	return ManagementPort
}
