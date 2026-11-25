// Package testenv runs controllers against a real API server (envtest) with
// fake kubelets and nodes, for tests.
package testenv

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	goruntime "runtime"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	v1 "github.com/pbonard/cassandra-room-operator/operator/api/v1alpha1"
	"github.com/pbonard/cassandra-room-operator/operator/internal/dc"
	"github.com/pbonard/cassandra-room-operator/operator/internal/mgmt"
	"github.com/pbonard/cassandra-room-operator/operator/internal/names"
)

// Test environment: a real API server (envtest), the controller, and in
// place of kubelets and nodes a fake that creates the StatefulSets' pods
// and volumes and runs a fake management API.

// StartEnv starts an API server with the CRDs; it skips the test when
// envtest's binaries are not installed (KUBEBUILDER_ASSETS).
func StartEnv(t *testing.T) (*envtest.Environment, *runtime.Scheme) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is not set (make test sets it)")
	}
	_, file, _, _ := runtimeCaller()
	env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join(filepath.Dir(file), "..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true}
	if _, err := env.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = v1.AddToScheme(s)
	return env, s
}

// fakeNode is a node behind the fake management API.
type fakeNode struct {
	info     mgmt.NodeInfo
	dead     bool // does not answer
	joinedAt time.Time
	leaving  time.Time
}

// FakeCluster is the fake kubelets and nodes.
type FakeCluster struct {
	mu        sync.Mutex
	nodes     map[string]*fakeNode // by management address
	cms       []string
	joinIn    time.Duration
	hold      map[string]bool // StatefulSets whose rollout is held
	nextIP    int
	byPod     map[string]string // pod -> management address
	podIPs    map[string]string
	repairs   []mgmt.RepairRequest
	repairErr error
	snapshots map[string][]mgmt.SnapshotTable
	imports   []string
	roles     *fakeRoles
}

func NewFakeCluster() *FakeCluster {
	return &FakeCluster{nodes: map[string]*fakeNode{}, joinIn: 300 * time.Millisecond, hold: map[string]bool{},
		byPod: map[string]string{}, podIPs: map[string]string{}, snapshots: map[string][]mgmt.SnapshotTable{},
		roles: &fakeRoles{}}
}

func (f *FakeCluster) node(addr string) (*fakeNode, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := f.nodes[addr]
	if n == nil || n.dead {
		return nil, fmt.Errorf("dial %s: connection refused", addr)
	}
	f.advanceLocked(n)
	return n, nil
}

func (f *FakeCluster) advanceLocked(n *fakeNode) {
	now := time.Now()
	if n.info.Status == "JOINING" && now.After(n.joinedAt) {
		n.info.Status = "NORMAL"
	}
	if n.info.Status == "LEAVING" && now.After(n.leaving) {
		n.info.Status = "LEFT"
	}
}

func (f *FakeCluster) Ready(ctx context.Context, addr string) (bool, error) {
	n, err := f.node(addr)
	if err != nil {
		return false, err
	}
	return n.info.Status == "NORMAL", nil
}

func (f *FakeCluster) Node(ctx context.Context, addr string) (mgmt.NodeInfo, error) {
	n, err := f.node(addr)
	if err != nil {
		return mgmt.NodeInfo{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	info := n.info
	info.CMS = slices.Contains(f.cms, info.Address)
	return info, nil
}

func (f *FakeCluster) Nodes(ctx context.Context, addr string) ([]mgmt.NodeInfo, error) {
	if _, err := f.node(addr); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []mgmt.NodeInfo
	for _, n := range f.nodes {
		f.advanceLocked(n)
		i := n.info
		i.Up = !n.dead
		i.CMS = slices.Contains(f.cms, i.Address)
		out = append(out, i)
	}
	return out, nil
}

func (f *FakeCluster) Ring(ctx context.Context, addr, keyspace string) ([]mgmt.Range, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var addrs []string
	for _, n := range f.nodes {
		if n.info.Status == "NORMAL" {
			addrs = append(addrs, n.info.Address)
		}
	}
	slices.Sort(addrs)
	// One range per node, evenly spaced, replicated on every node.
	var out []mgmt.Range
	step := uint64(1<<63) / uint64(max(len(addrs), 1)) * 2
	start := int64(-1 << 63)
	for i := range addrs {
		end := int64(uint64(start) + step)
		if i == len(addrs)-1 {
			end = -1 << 63
		}
		out = append(out, mgmt.Range{Start: start, End: end, Replicas: slices.Clone(addrs)})
		start = end
	}
	return out, nil
}

func (f *FakeCluster) Keyspaces(ctx context.Context, addr string) ([]string, error) {
	if _, err := f.node(addr); err != nil {
		return nil, err
	}
	return []string{"ks1", "ks2"}, nil
}

func (f *FakeCluster) Flush(ctx context.Context, addr string) error {
	_, err := f.node(addr)
	return err
}

func (f *FakeCluster) Decommission(ctx context.Context, addr string) error {
	n, err := f.node(addr)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if n.info.Status == "NORMAL" {
		n.info.Status, n.leaving = "LEAVING", time.Now().Add(300*time.Millisecond)
	}
	return nil
}

func (f *FakeCluster) Cleanup(ctx context.Context, addr, keyspace string) error { return nil }
func (f *FakeCluster) RemoveNode(ctx context.Context, addr, node string) error  { return nil }

func (f *FakeCluster) SetCMS(ctx context.Context, addr string, members []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cms = slices.Clone(members)
	slices.Sort(f.cms)
	return nil
}

func (f *FakeCluster) Repair(ctx context.Context, addr string, req mgmt.RepairRequest) (mgmt.RepairStats, error) {
	if _, err := f.node(addr); err != nil {
		return mgmt.RepairStats{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.repairErr != nil {
		return mgmt.RepairStats{}, f.repairErr
	}
	f.repairs = append(f.repairs, req)
	return mgmt.RepairStats{Ranges: 1, Mismatches: 1, Streamed: 10}, nil
}

func (f *FakeCluster) Snapshot(ctx context.Context, addr, name string, keyspaces []string) ([]mgmt.SnapshotTable, error) {
	if _, err := f.node(addr); err != nil {
		return nil, err
	}
	return nil, errors.New("not supported by the fake")
}

func (f *FakeCluster) ClearSnapshot(ctx context.Context, addr, name string) error { return nil }

func (f *FakeCluster) Import(ctx context.Context, addr, keyspace, table string, files []string) error {
	return errors.New("not supported by the fake")
}

// RepairsDone returns the repairs run so far.
func (f *FakeCluster) RepairsDone() []mgmt.RepairRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.repairs)
}

// SetRepairError makes repairs fail.
func (f *FakeCluster) SetRepairError(err error) {
	f.mu.Lock()
	f.repairErr = err
	f.mu.Unlock()
}

// Kill makes a pod's node stop answering.
func (f *FakeCluster) Kill(pod string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if n := f.nodes[f.byPod[pod]]; n != nil {
		n.dead = true
	}
}

// Hold stops a StatefulSet's rollout from completing.
func (f *FakeCluster) Hold(sts string, hold bool) {
	f.mu.Lock()
	f.hold[sts] = hold
	f.mu.Unlock()
}

// CMS returns the metadata service's members.
func (f *FakeCluster) CMS() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.cms)
}

// Run plays the kubelets until ctx ends: StatefulSets get their pods and
// volumes, pods their IPs, readiness from their node, nodes join.
func (f *FakeCluster) Run(ctx context.Context, c client.Client) {
	for ctx.Err() == nil {
		f.step(ctx, c)
		select {
		case <-ctx.Done():
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (f *FakeCluster) step(ctx context.Context, c client.Client) {
	sets := &appsv1.StatefulSetList{}
	if err := c.List(ctx, sets); err != nil {
		return
	}
	for i := range sets.Items {
		s := &sets.Items[i]
		replicas := int(*s.Spec.Replicas)
		revision := s.Annotations[names.AnnotationConfigHash]
		ready := 0
		for o := 0; ; o++ {
			name := fmt.Sprintf("%s-%d", s.Name, o)
			pod := &corev1.Pod{}
			err := c.Get(ctx, types.NamespacedName{Namespace: s.Namespace, Name: name}, pod)
			exists := err == nil
			if o >= replicas {
				if !exists {
					break
				}
				_ = c.Delete(ctx, pod, client.GracePeriodSeconds(0))
				continue
			}
			if !exists {
				f.createPod(ctx, c, s, name)
				continue
			}
			if !pod.DeletionTimestamp.IsZero() {
				// No kubelet: finish the deletion.
				pod.Finalizers = nil
				_ = c.Update(ctx, pod)
				continue
			}
			if f.updatePod(ctx, c, pod) {
				ready++
			}
		}
		f.mu.Lock()
		held := f.hold[s.Name]
		f.mu.Unlock()
		st := appsv1.StatefulSetStatus{ObservedGeneration: s.Generation, Replicas: int32(replicas), ReadyReplicas: int32(ready),
			UpdatedReplicas: int32(replicas), CurrentRevision: revision, UpdateRevision: revision}
		if held {
			st.UpdatedReplicas, st.CurrentRevision = 0, "old"
		}
		if !equality.Semantic.DeepEqual(s.Status, st) {
			s.Status = st
			_ = c.Status().Update(ctx, s)
		}
	}
	// PVCs whose pod was deleted with them are gone with the pod.
	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := c.List(ctx, pvcs); err == nil {
		for i := range pvcs.Items {
			p := &pvcs.Items[i]
			if !p.DeletionTimestamp.IsZero() {
				p.Finalizers = nil
				_ = c.Update(ctx, p)
			}
		}
	}
}

func (f *FakeCluster) createPod(ctx context.Context, c client.Client, s *appsv1.StatefulSet, name string) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: s.Namespace, Labels: s.Spec.Template.Labels,
		Annotations: s.Spec.Template.Annotations}, Spec: s.Spec.Template.Spec}
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: names.DataVolume, VolumeSource: corev1.VolumeSource{
		PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: names.PVC(name)}}})
	if err := c.Create(ctx, pod); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			fmt.Fprintln(os.Stderr, "fake kubelet: creating pod:", err)
		}
		return
	}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: names.PVC(name), Namespace: s.Namespace,
		Labels: s.Spec.Template.Labels}, Spec: s.Spec.VolumeClaimTemplates[0].Spec}
	newVolume := true
	if err := c.Create(ctx, pvc); apierrors.IsAlreadyExists(err) {
		newVolume = false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextIP++
	ip := fmt.Sprintf("10.0.%d.%d", f.nextIP/250, f.nextIP%250+1)
	addr := ip + ":" + strconv.Itoa(names.ManagementPort)
	old := f.byPod[name]
	info := mgmt.NodeInfo{HostID: fmt.Sprintf("host-%s-%d", name, f.nextIP), Address: ip + ":7000", Status: "JOINING",
		DataCenter: s.Labels[names.LabelDC], Rack: s.Labels[names.LabelRack], Self: true}
	if o := f.nodes[old]; o != nil {
		if !newVolume {
			info.HostID = o.info.HostID // restarted with its data
			info.Status = "NORMAL"
		}
		if i := slices.Index(f.cms, o.info.Address); i >= 0 {
			f.cms[i] = info.Address
		}
		delete(f.nodes, old)
	}
	f.nodes[addr] = &fakeNode{info: info, joinedAt: time.Now().Add(f.joinIn)}
	f.byPod[name], f.podIPs[name] = addr, ip
}

// updatePod sets a pod's IP and readiness; it reports whether it is ready.
func (f *FakeCluster) updatePod(ctx context.Context, c client.Client, pod *corev1.Pod) bool {
	f.mu.Lock()
	ip := f.podIPs[pod.Name]
	n := f.nodes[f.byPod[pod.Name]]
	ready := false
	if n != nil {
		f.advanceLocked(n)
		ready = !n.dead && n.info.Status == "NORMAL"
	}
	f.mu.Unlock()
	cond := corev1.ConditionFalse
	if ready {
		cond = corev1.ConditionTrue
	}
	st := corev1.PodStatus{Phase: corev1.PodRunning, PodIP: ip, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: cond}}}
	if pod.Status.PodIP != st.PodIP || len(pod.Status.Conditions) == 0 || pod.Status.Conditions[0].Status != cond {
		pod.Status = st
		_ = c.Status().Update(ctx, pod)
	}
	return ready
}

// fakeRoles records superuser setups.
type fakeRoles struct {
	mu         sync.Mutex
	users      []string
	truncated  []string
	onTruncate func([]string)
}

func (r *fakeRoles) EnsureSuperuser(ctx context.Context, c dc.Conn, user, password string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(c.Hosts) == 0 || password == "" {
		return errors.New("bad call")
	}
	r.users = append(r.users, user)
	return nil
}

func (r *fakeRoles) Truncate(ctx context.Context, c dc.Conn, user, password string, tables []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.truncated = append(r.truncated, tables...)
	if r.onTruncate != nil {
		r.onTruncate(tables)
	}
	return nil
}

// Truncated returns the tables truncated so far.
func (f *FakeCluster) Truncated() []string {
	f.roles.mu.Lock()
	defer f.roles.mu.Unlock()
	return slices.Clone(f.roles.truncated)
}

// OnTruncate calls fn when tables are truncated.
func (f *FakeCluster) OnTruncate(fn func([]string)) {
	f.roles.mu.Lock()
	f.roles.onTruncate = fn
	f.roles.mu.Unlock()
}

// Roles is the fake CQL role manager.
func (f *FakeCluster) Roles() dc.RoleManager { return f.roles }

// StartManager runs the data center controller (and others registered by
// setup) against the API server, with the fake cluster.
func StartManager(t *testing.T, env *envtest.Environment, s *runtime.Scheme, f *FakeCluster, setup ...func(ctrl.Manager) error) client.Client {
	if os.Getenv("TEST_LOG") != "" {
		ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	}
	skip := true // several managers in one test process
	mgr, err := ctrl.NewManager(env.Config, ctrl.Options{Scheme: s, Metrics: metricsserver.Options{BindAddress: "0"},
		Controller: config.Controller{SkipNameValidation: &skip}})
	if err != nil {
		t.Fatal(err)
	}
	r := &dc.Reconciler{Client: mgr.GetClient(), Scheme: s, Recorder: mgr.GetEventRecorderFor("test"), AgentImage: "operator:test",
		Mgmt:  func(context.Context, *v1.CassandraRoomDatacenter) (mgmt.Client, error) { return f, nil },
		Roles: f.roles}
	if err := r.SetupWithManager(mgr); err != nil {
		t.Fatal(err)
	}
	for _, fn := range setup {
		if err := fn(mgr); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = mgr.Start(ctx) }()
	c, err := client.New(env.Config, client.Options{Scheme: s})
	if err != nil {
		t.Fatal(err)
	}
	go f.Run(ctx, c)
	return c
}

// Namespace creates a namespace.
func Namespace(t *testing.T, c client.Client, name string) {
	if err := c.Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}); err != nil {
		t.Fatal(err)
	}
}

func runtimeCaller() (uintptr, string, int, bool) { return goruntime.Caller(0) }
