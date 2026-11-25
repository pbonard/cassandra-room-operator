package nodeinit

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	v1 "github.com/pbonard/cassandra-room-operator/operator/api/v1alpha1"
)

func cm(data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "c1-dc1-node-config", Namespace: "ns"}, Data: data}
}

func run(t *testing.T, o Options, objs ...runtime.Object) (string, error) {
	kube := fake.NewClientset(objs...)
	dir := t.TempDir()
	o.Namespace, o.ConfigMap, o.Out, o.Poll = "ns", "c1-dc1-node-config", filepath.Join(dir, "flags"), 10*time.Millisecond
	if o.DataDir == "" {
		o.DataDir = filepath.Join(dir, "data")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := Run(ctx, kube, o); err != nil {
		return "", err
	}
	b, err := os.ReadFile(o.Out)
	return string(b), err
}

func TestFlags(t *testing.T) {
	// The first pod of the cluster seeds itself.
	out, err := run(t, Options{Pod: "c1-dc1-r1-0", PodIP: "10.0.0.5"}, cm(map[string]string{"bootstrap-seed": "c1-dc1-r1-0"}))
	require.NoError(t, err)
	require.Equal(t, "-listen-address=10.0.0.5:7000\n-addr=10.0.0.5:9042\n-seeds=10.0.0.5:7000\n", out)

	// The others wait for a seed.
	_, err = run(t, Options{Pod: "c1-dc1-r1-1", PodIP: "10.0.0.6"}, cm(map[string]string{"bootstrap-seed": "c1-dc1-r1-0"}))
	require.ErrorContains(t, err, "a seed")
	out, err = run(t, Options{Pod: "c1-dc1-r1-1", PodIP: "10.0.0.6"}, cm(map[string]string{"seeds": "10.0.0.5:7000,10.1.0.5:7000"}))
	require.NoError(t, err)
	require.Contains(t, out, "-seeds=10.0.0.5:7000,10.1.0.5:7000\n")

	// A replacement with an empty volume; not with data.
	data := map[string]string{"seeds": "10.0.0.5:7000", "replace.c1-dc1-r1-1": "host-1"}
	out, err = run(t, Options{Pod: "c1-dc1-r1-1", PodIP: "10.0.0.7"}, cm(data))
	require.NoError(t, err)
	require.Contains(t, out, "-replace-address=host-1\n")
	full := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(full, "schema.json"), []byte("{}"), 0o644))
	out, err = run(t, Options{Pod: "c1-dc1-r1-1", PodIP: "10.0.0.7", DataDir: full}, cm(data))
	require.NoError(t, err)
	require.NotContains(t, out, "replace")

	// LoadBalancer networking: waits for the address, then broadcasts it.
	lb := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "c1-dc1-r1-0-lb", Namespace: "ns"}}
	_, err = run(t, Options{Pod: "c1-dc1-r1-0", PodIP: "10.0.0.5", Networking: v1.NetworkingLoadBalancer}, cm(map[string]string{"bootstrap-seed": "c1-dc1-r1-0"}), lb)
	require.ErrorContains(t, err, "load balancer address")
	lb.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: "34.1.2.3"}}
	out, err = run(t, Options{Pod: "c1-dc1-r1-0", PodIP: "10.0.0.5", Networking: v1.NetworkingLoadBalancer, PreferLocal: true},
		cm(map[string]string{"bootstrap-seed": "c1-dc1-r1-0"}), lb)
	require.NoError(t, err)
	for _, f := range []string{"-listen-address=10.0.0.5:7000", "-broadcast-address=34.1.2.3:7000", "-broadcast-rpc-address=34.1.2.3",
		"-prefer-local", "-seeds=34.1.2.3:7000"} {
		require.Contains(t, strings.Split(out, "\n"), f)
	}
}
