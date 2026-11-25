package dc

import (
	"context"
	"net"
	"sort"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/pbonard/cassandra-room-operator/operator/api/v1alpha1"
	"github.com/pbonard/cassandra-room-operator/operator/internal/names"
)

// Endpoint is how to reach a data center's node.
type Endpoint struct {
	Pod    string
	HostID string
	// Address is the node's internode address (its identity).
	Address string
	// Management is its management API's address; Agent its backup
	// agent's.
	Management, Agent string
	Ready             bool
}

// Endpoints lists a data center's nodes, by pod name.
func Endpoints(ctx context.Context, c client.Client, dc *v1.CassandraRoomDatacenter) ([]Endpoint, error) {
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods, client.InNamespace(dc.Namespace), client.MatchingLabels(names.Selector(dc))); err != nil {
		return nil, err
	}
	known := map[string]v1.NodeState{}
	for _, n := range dc.Status.Nodes {
		known[n.Pod] = n
	}
	var out []Endpoint
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.PodIP == "" || !p.DeletionTimestamp.IsZero() {
			continue
		}
		n := known[p.Name]
		out = append(out, Endpoint{Pod: p.Name, HostID: n.HostID, Address: n.Address,
			Management: net.JoinHostPort(p.Status.PodIP, strconv.Itoa(int(names.MgmtPort(dc)))),
			Agent:      net.JoinHostPort(p.Status.PodIP, strconv.Itoa(names.AgentPort)),
			Ready:      podReady(p) && n.Status == "NORMAL"})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Pod < out[j].Pod })
	return out, nil
}
