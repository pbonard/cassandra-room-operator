package cluster_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/pbonard/cassandra-room-operator/operator/api/v1alpha1"
	"github.com/pbonard/cassandra-room-operator/operator/internal/cluster"
	"github.com/pbonard/cassandra-room-operator/operator/internal/remote"
	"github.com/pbonard/cassandra-room-operator/operator/internal/testenv"
)

func kubeconfig(t *testing.T, cfg *rest.Config) []byte {
	c := clientcmdapi.NewConfig()
	c.Clusters["remote"] = &clientcmdapi.Cluster{Server: cfg.Host, CertificateAuthorityData: cfg.CAData}
	c.AuthInfos["remote"] = &clientcmdapi.AuthInfo{ClientCertificateData: cfg.CertData, ClientKeyData: cfg.KeyData, Token: cfg.BearerToken}
	c.Contexts["remote"] = &clientcmdapi.Context{Cluster: "remote", AuthInfo: "remote"}
	c.CurrentContext = "remote"
	b, err := clientcmd.Write(*c)
	require.NoError(t, err)
	return b
}

// TestMultiClusterDatacenters: a cluster of two data centers in two
// Kubernetes clusters (two API servers), created in order, linked by
// seeds, with the metadata service spread over both.
func TestMultiClusterDatacenters(t *testing.T) {
	envA, s := testenv.StartEnv(t)
	envB, _ := testenv.StartEnv(t)
	f := testenv.NewFakeCluster() // one ring across both clusters
	var cr *cluster.Reconciler
	a := testenv.StartManager(t, envA, s, f, func(mgr ctrl.Manager) error {
		cr = &cluster.Reconciler{Client: mgr.GetClient(), Scheme: s, Recorder: mgr.GetEventRecorderFor("cluster"),
			Remote: &remote.Clients{Local: mgr.GetClient(), Scheme: s, Namespace: "control"}}
		return cr.SetupWithManager(mgr)
	})
	b := testenv.StartManager(t, envB, s, f)
	ctx := context.Background()
	testenv.Namespace(t, a, "control")
	testenv.Namespace(t, b, "east")
	require.NoError(t, a.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "east-kubeconfig", Namespace: "control"},
		Data: map[string][]byte{"kubeconfig": kubeconfig(t, envB.Config)}}))
	require.NoError(t, a.Create(ctx, &v1.ClientConfig{ObjectMeta: metav1.ObjectMeta{Name: "east", Namespace: "control"},
		Spec: v1.ClientConfigSpec{KubeConfigSecret: v1.SecretRef{SecretName: "east-kubeconfig"}}}))
	cl := &v1.CassandraRoomCluster{ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "control"}, Spec: v1.CassandraRoomClusterSpec{
		Image: "cassandra-room:test",
		Datacenters: []v1.DatacenterTemplate{
			{Name: "west", Size: 2},
			{Name: "east", K8sContext: "east", Namespace: "east", Size: 2, Racks: []v1.Rack{{Name: "r1"}, {Name: "r2"}}},
		},
		Repairs: []v1.RepairTemplate{{Name: "weekly", Spec: v1.RepairScheduleSettings{Keyspaces: []string{"ks"}}}},
	}}
	require.NoError(t, a.Create(ctx, cl))

	// The second data center waits for the first.
	west := types.NamespacedName{Namespace: "control", Name: "c1-west"}
	east := types.NamespacedName{Namespace: "east", Name: "c1-east"}
	require.Eventually(t, func() bool { return a.Get(ctx, west, &v1.CassandraRoomDatacenter{}) == nil }, 10*time.Second, 50*time.Millisecond)
	sawEastEarly := false
	require.Eventually(t, func() bool {
		dc := &v1.CassandraRoomDatacenter{}
		wr := a.Get(ctx, west, dc) == nil && dc.Status.Phase == v1.PhaseReady
		if !wr && b.Get(ctx, east, &v1.CassandraRoomDatacenter{}) == nil {
			sawEastEarly = true
		}
		return wr
	}, 30*time.Second, 50*time.Millisecond)
	require.False(t, sawEastEarly, "the second data center is created once the first is ready")

	// Ready: linked by seeds, metadata service across both.
	var last string
	require.Eventually(t, func() bool {
		c := &v1.CassandraRoomCluster{}
		if a.Get(ctx, client.ObjectKeyFromObject(cl), c) != nil {
			return false
		}
		last = fmt.Sprintf("%+v", c.Status)
		return c.Status.Phase == v1.PhaseReady && len(c.Status.CMSMembers) == 3
	}, 60*time.Second, 100*time.Millisecond, "cluster ready: %s", &last)
	w, e := &v1.CassandraRoomDatacenter{}, &v1.CassandraRoomDatacenter{}
	require.NoError(t, a.Get(ctx, west, w))
	require.NoError(t, b.Get(ctx, east, e))
	require.Equal(t, "west", w.DCName())
	require.Equal(t, e.Status.SeedAddresses, sorted(w.Spec.AdditionalSeeds), "west seeds through east")
	require.Equal(t, w.Status.SeedAddresses, sorted(e.Spec.AdditionalSeeds), "and east through west")
	require.Len(t, e.Status.SeedAddresses, 2, "one seed per rack")
	require.Len(t, w.Spec.CMSMembers, 3)
	require.Empty(t, e.Spec.CMSMembers, "one data center maintains the metadata service")
	require.Eventually(t, func() bool { return slices.Equal(f.CMS(), w.Spec.CMSMembers) }, 20*time.Second, 50*time.Millisecond)
	inEast := 0
	for _, m := range w.Spec.CMSMembers {
		if slices.Contains(e.Status.SeedAddresses, m) {
			inEast++
		}
	}
	require.Equal(t, 1, inEast, "spread: two in west, one in east")
	// The superuser secret was copied to the remote cluster.
	src, dst := &corev1.Secret{}, &corev1.Secret{}
	require.NoError(t, a.Get(ctx, types.NamespacedName{Namespace: "control", Name: "c1-superuser"}, src))
	require.NoError(t, b.Get(ctx, types.NamespacedName{Namespace: "east", Name: "c1-superuser"}, dst))
	require.Equal(t, src.Data, dst.Data)
	// The repair schedule runs from the first data center.
	rs := &v1.RepairSchedule{}
	require.NoError(t, a.Get(ctx, types.NamespacedName{Namespace: "control", Name: "c1-weekly"}, rs))
	require.Equal(t, "c1-west", rs.Spec.Datacenter)

	// Scaling a remote data center through the cluster.
	require.Eventually(t, func() bool {
		c := &v1.CassandraRoomCluster{}
		if a.Get(ctx, client.ObjectKeyFromObject(cl), c) != nil {
			return false
		}
		c.Spec.Datacenters[1].Size = 3
		return a.Update(ctx, c) == nil
	}, 5*time.Second, 50*time.Millisecond)
	require.Eventually(t, func() bool {
		dc := &v1.CassandraRoomDatacenter{}
		return b.Get(ctx, east, dc) == nil && dc.Spec.Size == 3 && dc.Status.ReadyNodes == 3 && dc.Status.Phase == v1.PhaseReady
	}, 30*time.Second, 100*time.Millisecond)

	// Deleting the cluster deletes its data centers everywhere.
	require.NoError(t, a.Delete(ctx, cl))
	require.Eventually(t, func() bool {
		return a.Get(ctx, west, &v1.CassandraRoomDatacenter{}) != nil && b.Get(ctx, east, &v1.CassandraRoomDatacenter{}) != nil &&
			a.Get(ctx, client.ObjectKeyFromObject(cl), &v1.CassandraRoomCluster{}) != nil
	}, 30*time.Second, 100*time.Millisecond)
}

func sorted(s []string) []string {
	s = slices.Clone(s)
	slices.Sort(s)
	return s
}
