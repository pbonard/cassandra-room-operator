package backup_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/pbonard/cassandra-room-operator/operator/api/v1alpha1"
	"github.com/pbonard/cassandra-room-operator/operator/internal/agent"
	"github.com/pbonard/cassandra-room-operator/operator/internal/backup"
	"github.com/pbonard/cassandra-room-operator/operator/internal/catalog"
	"github.com/pbonard/cassandra-room-operator/operator/internal/cluster"
	"github.com/pbonard/cassandra-room-operator/operator/internal/mgmt"
	"github.com/pbonard/cassandra-room-operator/operator/internal/objstore"
	"github.com/pbonard/cassandra-room-operator/operator/internal/remote"
	"github.com/pbonard/cassandra-room-operator/operator/internal/testenv"
)

// node is a node's data for the agents: its sstables, and what it
// imported.
type node struct {
	mgmt.Client
	dir      string
	mu       sync.Mutex
	imported []string
}

func (n *node) Node(context.Context, string) (mgmt.NodeInfo, error) {
	return mgmt.NodeInfo{HostID: "h-" + filepath.Base(n.dir), Status: "NORMAL"}, nil
}

func (n *node) Snapshot(_ context.Context, _, _ string, _ []string) ([]mgmt.SnapshotTable, error) {
	files, _ := filepath.Glob(filepath.Join(n.dir, "*.sst"))
	return []mgmt.SnapshotTable{{Keyspace: "ks", Table: "t1", ID: "x", Files: files}}, nil
}

func (n *node) ClearSnapshot(context.Context, string, string) error { return nil }

func (n *node) Import(_ context.Context, _, keyspace, table string, files []string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, f := range files {
		b, _ := os.ReadFile(f)
		n.imported = append(n.imported, keyspace+"."+table+":"+string(b))
	}
	return nil
}

// agents creates an agent per address on first use, with its own data.
type agents struct {
	agent.Local
	mu    sync.Mutex
	store objstore.Store
	root  string
	nodes map[string]*node
}

func (a *agents) ensure(addr string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.nodes[addr] != nil {
		return
	}
	dir := filepath.Join(a.root, strings.ReplaceAll(addr, ":", "_"))
	_ = os.MkdirAll(dir, 0o755)
	_ = os.WriteFile(filepath.Join(dir, "1.sst"), []byte("data of "+addr), 0o644)
	n := &node{dir: dir}
	a.nodes[addr] = n
	a.Set(addr, &agent.Agent{Store: a.store, Mgmt: n, MgmtAddr: addr, DataDir: filepath.Join(dir, "agent")})
}

func (a *agents) StartBackup(ctx context.Context, addr string, req agent.BackupRequest) (agent.Status, error) {
	a.ensure(addr)
	return a.Local.StartBackup(ctx, addr, req)
}

func (a *agents) BackupStatus(ctx context.Context, addr, backup string) (agent.Status, error) {
	a.ensure(addr)
	return a.Local.BackupStatus(ctx, addr, backup)
}

func (a *agents) RestoreStatus(ctx context.Context, addr, backup string) (agent.Status, error) {
	a.ensure(addr)
	return a.Local.RestoreStatus(ctx, addr, backup)
}

func (a *agents) StartRestore(ctx context.Context, addr string, req agent.RestoreRequest) (agent.Status, error) {
	a.ensure(addr)
	return a.Local.StartRestore(ctx, addr, req)
}

func TestBackupRestoreSchedule(t *testing.T) {
	env, s := testenv.StartEnv(t)
	f := testenv.NewFakeCluster()
	store := objstore.NewMemory()
	ag := &agents{store: store, root: t.TempDir(), nodes: map[string]*node{}}
	c := testenv.StartManager(t, env, s, f, func(mgr ctrl.Manager) error {
		rem := &remote.Clients{Local: mgr.GetClient(), Scheme: s, Namespace: "bk"}
		cp := backup.ControlPlane{Client: mgr.GetClient(), Scheme: s, Recorder: mgr.GetEventRecorderFor("backup"), Remote: rem}
		common := backup.Common{Client: mgr.GetClient(), Scheme: s, Recorder: mgr.GetEventRecorderFor("backup"), Agents: ag,
			Store: func(context.Context, *v1.CassandraRoomDatacenter) (objstore.Store, error) { return store, nil }, Roles: f.Roles(),
			CollectGrace: time.Nanosecond}
		for _, r := range []interface{ SetupWithManager(ctrl.Manager) error }{
			&cluster.Reconciler{Client: mgr.GetClient(), Scheme: s, Recorder: mgr.GetEventRecorderFor("cluster"), Remote: rem},
			&backup.BackupReconciler{ControlPlane: cp}, &backup.ScheduleReconciler{ControlPlane: cp}, &backup.RestoreReconciler{ControlPlane: cp},
			&backup.DatacenterBackupReconciler{Common: common}, &backup.DatacenterRestoreReconciler{Common: common},
		} {
			if err := r.SetupWithManager(mgr); err != nil {
				return err
			}
		}
		return nil
	})
	ctx := context.Background()
	ns := "bk"
	testenv.Namespace(t, c, ns)
	require.NoError(t, c.Create(ctx, &v1.CassandraRoomCluster{ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: ns}, Spec: v1.CassandraRoomClusterSpec{
		Image: "x", Datacenters: []v1.DatacenterTemplate{{Name: "dc1", Size: 2}}, Backup: &v1.S3Storage{Bucket: "b"}}}))
	require.Eventually(t, func() bool {
		cl := &v1.CassandraRoomCluster{}
		return c.Get(ctx, types.NamespacedName{Namespace: ns, Name: "c1"}, cl) == nil && cl.Status.Phase == v1.PhaseReady
	}, 30*time.Second, 100*time.Millisecond)

	// Backup.
	require.NoError(t, c.Create(ctx, &v1.CassandraRoomBackup{ObjectMeta: metav1.ObjectMeta{Name: "b1", Namespace: ns}, Spec: v1.CassandraRoomBackupSpec{Cluster: "c1"}}))
	waitPhase(t, c, &v1.CassandraRoomBackup{}, ns, "b1", v1.BackupSucceeded, func(o client.Object) v1.BackupPhase { return o.(*v1.CassandraRoomBackup).Status.Phase })
	b := &v1.CassandraRoomBackup{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: ns, Name: "b1"}, b))
	require.Len(t, b.Status.Datacenters, 1)
	require.Equal(t, int32(2), b.Status.Datacenters[0].Done)
	var idx catalog.Index
	require.NoError(t, catalog.GetJSON(ctx, store, catalog.IndexKey("c1", "b1", "dc1"), &idx))
	require.ElementsMatch(t, []string{"c1-dc1-rack1-0", "c1-dc1-rack1-1"}, idx.Pods)

	// Restore: truncate, then each node imports its own files.
	require.NoError(t, c.Create(ctx, &v1.CassandraRoomRestore{ObjectMeta: metav1.ObjectMeta{Name: "r1", Namespace: ns},
		Spec: v1.CassandraRoomRestoreSpec{Cluster: "c1", Backup: "b1"}}))
	waitPhase(t, c, &v1.CassandraRoomRestore{}, ns, "r1", v1.BackupSucceeded, func(o client.Object) v1.BackupPhase { return o.(*v1.CassandraRoomRestore).Status.Phase })
	require.Equal(t, []string{"ks.t1"}, f.Truncated())
	ag.mu.Lock()
	require.Len(t, ag.nodes, 2)
	for addr, n := range ag.nodes {
		require.Equal(t, []string{"ks.t1:data of " + addr}, n.imported, "%s restored its own data", addr)
	}
	ag.mu.Unlock()

	// A restore of a backup that does not exist fails.
	require.NoError(t, c.Create(ctx, &v1.CassandraRoomRestore{ObjectMeta: metav1.ObjectMeta{Name: "r2", Namespace: ns},
		Spec: v1.CassandraRoomRestoreSpec{Cluster: "c1", Backup: "nope"}}))
	waitPhase(t, c, &v1.CassandraRoomRestore{}, ns, "r2", v1.BackupFailed, func(o client.Object) v1.BackupPhase { return o.(*v1.CassandraRoomRestore).Status.Phase })

	// A schedule keeping one backup.
	require.NoError(t, c.Create(ctx, &v1.CassandraRoomBackupSchedule{ObjectMeta: metav1.ObjectMeta{Name: "every", Namespace: ns},
		Spec: v1.CassandraRoomBackupScheduleSpec{Cluster: "c1", Schedule: "@every 2s", Keep: 1}}))
	var scheduled []string
	require.Eventually(t, func() bool {
		list := &v1.CassandraRoomBackupList{}
		if c.List(ctx, list, client.InNamespace(ns), client.MatchingLabels{backup.LabelSchedule: "every"}) != nil {
			return false
		}
		for _, x := range list.Items {
			if !contains(scheduled, x.Name) {
				scheduled = append(scheduled, x.Name)
			}
		}
		live := 0
		for _, x := range list.Items {
			if x.DeletionTimestamp.IsZero() {
				live++
			}
		}
		return len(scheduled) >= 3 && live <= 2
	}, 40*time.Second, 200*time.Millisecond, "scheduled: %v", &scheduled)
	// The deleted ones are gone from the store too.
	require.Eventually(t, func() bool {
		backups, err := catalog.Backups(ctx, store, "c1")
		return err == nil && len(backups) <= 3
	}, 20*time.Second, 200*time.Millisecond)
	backups, _ := catalog.Backups(ctx, store, "c1")
	require.Contains(t, backups, "b1", "backups not of the schedule are kept")
	fmt.Println("backups in the store:", backups)
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func waitPhase(t *testing.T, c client.Client, obj client.Object, ns, name string, want v1.BackupPhase, phase func(client.Object) v1.BackupPhase) {
	t.Helper()
	ok := assert.Eventually(t, func() bool {
		if c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, obj) != nil {
			return false
		}
		return phase(obj) == want
	}, 30*time.Second, 100*time.Millisecond)
	if !ok {
		b, _ := json.Marshal(obj)
		parts := &v1.DatacenterBackupList{}
		_ = c.List(context.Background(), parts)
		p, _ := json.Marshal(parts.Items)
		t.Fatalf("%s: %s\nparts: %s", name, b, p)
	}
}
