package dc_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/pbonard/cassandra-room-operator/operator/api/v1alpha1"
	"github.com/pbonard/cassandra-room-operator/operator/internal/names"
	"github.com/pbonard/cassandra-room-operator/operator/internal/testenv"
)

func get[T client.Object](t *testing.T, c client.Client, ns, name string, obj T) T {
	t.Helper()
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, obj))
	return obj
}

func replicas(t *testing.T, c client.Client, ns, name string) int32 {
	s := &appsv1.StatefulSet{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, s); err != nil {
		return -1
	}
	return *s.Spec.Replicas
}

func update(t *testing.T, c client.Client, ns, name string, change func(*v1.CassandraRoomDatacenter)) {
	t.Helper()
	require.Eventually(t, func() bool {
		dc := &v1.CassandraRoomDatacenter{}
		if c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, dc) != nil {
			return false
		}
		change(dc)
		return c.Update(context.Background(), dc) == nil
	}, 5*time.Second, 50*time.Millisecond)
}

func TestDatacenterLifecycle(t *testing.T) {
	env, s := testenv.StartEnv(t)
	f := testenv.NewFakeCluster()
	c := testenv.StartManager(t, env, s, f)
	ctx := context.Background()
	ns := "lifecycle"
	testenv.Namespace(t, c, ns)
	dc := &v1.CassandraRoomDatacenter{ObjectMeta: metav1.ObjectMeta{Name: "dc1", Namespace: ns}, Spec: v1.CassandraRoomDatacenterSpec{
		ClusterName: "c1", Image: "cassandra-room:test", Size: 3, ManageCMS: true,
		Racks: []v1.Rack{{Name: "r1", NodeAffinityLabels: map[string]string{"zone": "a"}}, {Name: "r2"}}}}
	require.NoError(t, c.Create(ctx, dc))

	// Scale up one node at a time, over both racks.
	maxSeen := int32(0)
	var last string
	defer func() {
		if t.Failed() {
			t.Log(last)
		}
	}()
	require.Eventually(t, func() bool {
		d := get(t, c, ns, "dc1", &v1.CassandraRoomDatacenter{})
		r1, r2 := replicas(t, c, ns, "c1-dc1-r1"), replicas(t, c, ns, "c1-dc1-r2")
		joining := int32(0)
		for _, n := range d.Status.Nodes {
			if !n.Ready {
				joining++
			}
		}
		maxSeen = max(maxSeen, joining)
		last = fmt.Sprintf("phase=%s ready=%d r1=%d r2=%d nodes=%+v", d.Status.Phase, d.Status.ReadyNodes, r1, r2, d.Status.Nodes)
		return d.Status.Phase == v1.PhaseReady && d.Status.ReadyNodes == 3 && r1 == 2 && r2 == 1
	}, 30*time.Second, 50*time.Millisecond, "last: %s", &last)
	require.LessOrEqual(t, maxSeen, int32(1), "one node joins at a time")

	d := get(t, c, ns, "dc1", &v1.CassandraRoomDatacenter{})
	require.Len(t, d.Status.SeedAddresses, 2, "the first pod of each rack")
	require.True(t, d.Status.SuperuserCreated)
	cm := get(t, c, ns, "c1-dc1-node-config", &corev1.ConfigMap{})
	require.Equal(t, "c1-dc1-r1-0", cm.Data["bootstrap-seed"])
	require.Len(t, cm.Data["seeds"], len(d.Status.SeedAddresses[0])+len(d.Status.SeedAddresses[1])+1)
	sec := get(t, c, ns, "c1-superuser", &corev1.Secret{})
	require.Len(t, sec.Data["password"], 48)
	sts := get(t, c, ns, "c1-dc1-r1", &appsv1.StatefulSet{})
	require.Contains(t, sts.Spec.Template.Spec.Containers[0].Args, "-dc=dc1")
	require.Contains(t, sts.Spec.Template.Spec.Containers[0].Args, "-rack=r1")
	require.Equal(t, "zone", sts.Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Key)
	require.Equal(t, "node-init", sts.Spec.Template.Spec.InitContainers[0].Name)
	get(t, c, ns, "c1-dc1-all-pods", &corev1.Service{})
	get(t, c, ns, "c1-dc1-service", &corev1.Service{})
	pod := get(t, c, ns, "c1-dc1-r2-0", &corev1.Pod{})
	require.Equal(t, "true", pod.Labels[names.LabelSeed])
	// Three members of the metadata service: the seeds, then another node.
	require.Eventually(t, func() bool { return len(f.CMS()) == 3 }, 10*time.Second, 50*time.Millisecond)

	// Scale down: the last node of the fullest rack leaves, then its pod
	// and volume go.
	update(t, c, ns, "dc1", func(d *v1.CassandraRoomDatacenter) { d.Spec.Size = 2 })
	require.Eventually(t, func() bool {
		d := get(t, c, ns, "dc1", &v1.CassandraRoomDatacenter{})
		err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: "data-c1-dc1-r1-1"}, &corev1.PersistentVolumeClaim{})
		return replicas(t, c, ns, "c1-dc1-r1") == 1 && apierrors.IsNotFound(err) && d.Status.Phase == v1.PhaseReady && d.Status.ReadyNodes == 2
	}, 30*time.Second, 50*time.Millisecond)

	// A rolling restart goes one rack at a time.
	f.Hold("c1-dc1-r1", true)
	hash := func(name string) string {
		return get(t, c, ns, name, &appsv1.StatefulSet{}).Annotations[names.AnnotationConfigHash]
	}
	h1, h2 := hash("c1-dc1-r1"), hash("c1-dc1-r2")
	update(t, c, ns, "dc1", func(d *v1.CassandraRoomDatacenter) { d.Spec.RollingRestartRequestedAt = "now" })
	require.Eventually(t, func() bool { return hash("c1-dc1-r1") != h1 }, 10*time.Second, 50*time.Millisecond)
	time.Sleep(500 * time.Millisecond)
	require.Equal(t, h2, hash("c1-dc1-r2"), "rack 2 waits for rack 1")
	f.Hold("c1-dc1-r1", false)
	require.Eventually(t, func() bool { return hash("c1-dc1-r2") != h2 }, 10*time.Second, 50*time.Millisecond)
	require.Eventually(t, func() bool {
		d := get(t, c, ns, "dc1", &v1.CassandraRoomDatacenter{})
		return d.Status.LastRollingRestart == "now" && d.Status.Phase == v1.PhaseReady
	}, 10*time.Second, 50*time.Millisecond)

	// Replace a dead node: its pod comes back with an empty volume and
	// replaces it.
	d = get(t, c, ns, "dc1", &v1.CassandraRoomDatacenter{})
	var dead string
	for _, n := range d.Status.Nodes {
		if n.Pod == "c1-dc1-r2-0" {
			dead = n.HostID
		}
	}
	require.NotEmpty(t, dead)
	f.Kill("c1-dc1-r2-0")
	update(t, c, ns, "dc1", func(d *v1.CassandraRoomDatacenter) { d.Spec.ReplaceNodes = []string{"c1-dc1-r2-0"} })
	require.Eventually(t, func() bool {
		cm := get(t, c, ns, "c1-dc1-node-config", &corev1.ConfigMap{})
		return cm.Data["replace.c1-dc1-r2-0"] == dead
	}, 10*time.Second, 50*time.Millisecond, "node-init is told which node to replace")
	require.Eventually(t, func() bool {
		d := get(t, c, ns, "dc1", &v1.CassandraRoomDatacenter{})
		return d.Status.ReplacedNodes["c1-dc1-r2-0"] == "done:"+dead && d.Status.ReadyNodes == 2
	}, 30*time.Second, 50*time.Millisecond)
	cm = get(t, c, ns, "c1-dc1-node-config", &corev1.ConfigMap{})
	require.NotContains(t, cm.Data, "replace.c1-dc1-r2-0")
	require.False(t, slices.Contains(f.CMS(), ""))

	// Stopped: no pods, volumes kept.
	update(t, c, ns, "dc1", func(d *v1.CassandraRoomDatacenter) { d.Spec.Stopped = true })
	require.Eventually(t, func() bool {
		d := get(t, c, ns, "dc1", &v1.CassandraRoomDatacenter{})
		return d.Status.Phase == v1.PhaseStopped && replicas(t, c, ns, "c1-dc1-r1") == 0 && replicas(t, c, ns, "c1-dc1-r2") == 0
	}, 10*time.Second, 50*time.Millisecond)
	get(t, c, ns, "data-c1-dc1-r1-0", &corev1.PersistentVolumeClaim{})
	// Restarted: every node at once (their volumes exist).
	update(t, c, ns, "dc1", func(d *v1.CassandraRoomDatacenter) { d.Spec.Stopped = false })
	require.Eventually(t, func() bool {
		return replicas(t, c, ns, "c1-dc1-r1") == 1 && replicas(t, c, ns, "c1-dc1-r2") == 1
	}, 10*time.Second, 50*time.Millisecond)
}
