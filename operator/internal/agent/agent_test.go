package agent

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pbonard/cassandra-room-operator/operator/internal/catalog"
	"github.com/pbonard/cassandra-room-operator/operator/internal/mgmt"
	"github.com/pbonard/cassandra-room-operator/operator/internal/objstore"
)

// fakeNode snapshots the files of a directory per table.
type fakeNode struct {
	mgmt.Client
	dir      string
	mu       sync.Mutex
	imported map[string][]string // keyspace.table -> file contents
	loaded   map[string][]string
	cleared  []string
	archive  string // the commit log archive's directory
	replays  []mgmt.ReplayRequest
	segments []string // contents of the replayed segments
}

func (n *fakeNode) Load(_ context.Context, _, keyspace, table string, files []string, consistency string) (mgmt.LoadResult, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.loaded == nil {
		n.loaded = map[string][]string{}
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return mgmt.LoadResult{}, err
		}
		n.loaded[keyspace+"."+table] = append(n.loaded[keyspace+"."+table], string(b))
	}
	return mgmt.LoadResult{Entries: int64(len(files))}, nil
}

func (n *fakeNode) ArchiveCommitLog(context.Context, string) ([]string, error) {
	files, _ := filepath.Glob(filepath.Join(n.archive, "*.wal"))
	sort.Strings(files)
	return files, nil
}

func (n *fakeNode) ReplayCommitLog(_ context.Context, _ string, req mgmt.ReplayRequest) (mgmt.ReplayResult, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.replays = append(n.replays, req)
	for _, f := range req.Files {
		b, err := os.ReadFile(f)
		if err != nil {
			return mgmt.ReplayResult{}, err
		}
		n.segments = append(n.segments, string(b))
	}
	return mgmt.ReplayResult{Applied: 1}, nil
}

func (n *fakeNode) Node(context.Context, string) (mgmt.NodeInfo, error) {
	return mgmt.NodeInfo{HostID: "h1", Address: "10.0.0.1:7000", Status: "NORMAL"}, nil
}

func (n *fakeNode) Snapshot(_ context.Context, _, name string, keyspaces []string) ([]mgmt.SnapshotTable, error) {
	var out []mgmt.SnapshotTable
	for _, t := range []string{"ks.a", "ks.b"} {
		files, _ := filepath.Glob(filepath.Join(n.dir, t, "*"))
		sort.Strings(files)
		out = append(out, mgmt.SnapshotTable{Keyspace: "ks", Table: filepath.Ext(t)[1:], ID: "id-" + t, Storage: uint64(len(t) + len(out)), Files: files})
	}
	return out, nil
}

func (n *fakeNode) ClearSnapshot(_ context.Context, _, name string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cleared = append(n.cleared, name)
	return nil
}

func (n *fakeNode) Import(_ context.Context, _, keyspace, table string, files []string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.imported == nil {
		n.imported = map[string][]string{}
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		n.imported[keyspace+"."+table] = append(n.imported[keyspace+"."+table], string(b))
	}
	return nil
}

func write(t *testing.T, dir, table, name, content string) {
	require.NoError(t, os.MkdirAll(filepath.Join(dir, table), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, table, name), []byte(content), 0o644))
}

func wait(t *testing.T, f func() (Status, bool), phase string) Status {
	var st Status
	require.Eventually(t, func() bool {
		var ok bool
		st, ok = f()
		return ok && (st.Phase == phase || st.Phase == Failed)
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, phase, st.Phase, st.Error)
	return st
}

func TestBackupRestore(t *testing.T) {
	ctx := context.Background()
	data := t.TempDir()
	write(t, data, "ks.a", "1.sst", "aaaa")
	write(t, data, "ks.a", "2.sst", "bbbbbb")
	write(t, data, "ks.b", "3.sst", "cc")
	node := &fakeNode{dir: data}
	store := objstore.NewMemory()
	a := &Agent{Store: store, Mgmt: node, MgmtAddr: "x", DataDir: t.TempDir()}

	a.StartBackup(BackupRequest{Cluster: "c1", Datacenter: "dc1", Backup: "b1", Pod: "p0"})
	st := wait(t, func() (Status, bool) { return a.BackupStatus("b1") }, Succeeded)
	require.Equal(t, 3, st.Files)
	require.Equal(t, int64(12), st.Bytes)
	require.Equal(t, int64(12), st.Transferred)
	require.Equal(t, "h1", st.HostID)
	require.Equal(t, []string{"backup-b1"}, node.cleared, "the snapshot is cleared")

	// The next backup uploads only the new file.
	require.NoError(t, os.Remove(filepath.Join(data, "ks.a", "1.sst")))
	write(t, data, "ks.a", "4.sst", "ddddddd")
	a.StartBackup(BackupRequest{Cluster: "c1", Datacenter: "dc1", Backup: "b2", Pod: "p0"})
	st = wait(t, func() (Status, bool) { return a.BackupStatus("b2") }, Succeeded)
	require.Equal(t, int64(15), st.Bytes)
	require.Equal(t, int64(7), st.Transferred)
	var m catalog.Manifest
	require.NoError(t, catalog.GetJSON(ctx, store, catalog.ManifestKey("c1", "b2", "dc1", "p0"), &m))
	require.Len(t, m.Tables, 2)
	require.Equal(t, "10.0.0.1:7000", m.Address)

	// Restore b1: download, then import.
	a.StartRestore(RestoreRequest{Cluster: "c1", Datacenter: "dc1", Backup: "b1", Pod: "p0", Keyspaces: []string{"ks"}})
	st = wait(t, func() (Status, bool) { return a.RestoreStatus("b1") }, Downloaded)
	require.Equal(t, int64(12), st.Transferred)
	require.NoError(t, a.Import("b1"))
	wait(t, func() (Status, bool) { return a.RestoreStatus("b1") }, Succeeded)
	require.ElementsMatch(t, []string{"aaaa", "bbbbbb"}, node.imported["ks.a"])
	require.Equal(t, []string{"cc"}, node.imported["ks.b"])
	_, err := os.Stat(a.restoreDir("b1"))
	require.True(t, os.IsNotExist(err), "the downloaded files are removed")

	// Deleting b1 frees the file only it had.
	require.NoError(t, catalog.DeleteDatacenter(ctx, store, "c1", "b1", "dc1"))
	n, err := catalog.Collect(ctx, store, "c1", time.Hour, time.Now())
	require.NoError(t, err)
	require.Zero(t, n, "too recent")
	n, err = catalog.Collect(ctx, store, "c1", 0, time.Now().Add(time.Second))
	require.NoError(t, err)
	require.Equal(t, 1, n)
	objs, _ := store.List(ctx, "c1/data/")
	require.Len(t, objs, 3)

	// A corrupted object fails the restore.
	for _, o := range objs {
		require.NoError(t, store.Put(ctx, o.Key, strings.NewReader("x"), 1))
	}
	a.StartRestore(RestoreRequest{Cluster: "c1", Datacenter: "dc1", Backup: "b2", Pod: "p0"})
	var failed Status
	require.Eventually(t, func() bool { failed, _ = a.RestoreStatus("b2"); return failed.Phase == Failed }, 5*time.Second, 10*time.Millisecond)
	require.Contains(t, failed.Error, "does not match")
}

// TestLoadIntoOtherTopology: a node loads the files of two other nodes'
// manifests through itself.
func TestLoadIntoOtherTopology(t *testing.T) {
	store := objstore.NewMemory()
	for i, pod := range []string{"p0", "p1"} {
		data := t.TempDir()
		write(t, data, "ks.a", "1.sst", "a-"+pod)
		write(t, data, "ks.b", "2.sst", "b-"+pod)
		a := &Agent{Store: store, Mgmt: &fakeNode{dir: data}, MgmtAddr: "x", DataDir: t.TempDir()}
		a.StartBackup(BackupRequest{Cluster: "old", Datacenter: "dc1", Backup: "b1", Pod: pod})
		wait(t, func() (Status, bool) { return a.BackupStatus("b1") }, Succeeded)
		_ = i
	}
	node := &fakeNode{}
	a := &Agent{Store: store, Mgmt: node, MgmtAddr: "x", DataDir: t.TempDir()}
	a.StartRestore(RestoreRequest{Cluster: "old", Datacenter: "dc1", Backup: "b1", Pod: "new-0", Load: true, Sources: []string{"p0", "p1"}})
	st := wait(t, func() (Status, bool) { return a.RestoreStatus("b1") }, Downloaded)
	require.Equal(t, 4, st.Files)
	require.NoError(t, a.Import("b1"))
	wait(t, func() (Status, bool) { return a.RestoreStatus("b1") }, Succeeded)
	require.ElementsMatch(t, []string{"a-p0", "a-p1"}, node.loaded["ks.a"])
	require.ElementsMatch(t, []string{"b-p0", "b-p1"}, node.loaded["ks.b"])
	require.Empty(t, node.imported)
}

// TestPointInTime: the node's commit log goes up in rounds; a restore to a
// point in time imports the backup, then replays the rounds from the
// backup up to the first round after the point in time.
func TestPointInTime(t *testing.T) {
	ctx := context.Background()
	data, archive := t.TempDir(), t.TempDir()
	write(t, data, "ks.a", "1.sst", "aaaa")
	node := &fakeNode{dir: data, archive: archive}
	store := objstore.NewMemory()
	a := &Agent{Store: store, Mgmt: node, MgmtAddr: "x", DataDir: t.TempDir(), Cluster: "c1", Datacenter: "dc1", Pod: "p0"}
	segment := func(name, content string) {
		require.NoError(t, os.WriteFile(filepath.Join(archive, name), []byte(content), 0o644))
	}
	round := func() {
		require.NoError(t, a.ArchiveCommitLog(ctx))
		time.Sleep(5 * time.Millisecond)
	}
	segment("000001.wal", "before the backup")
	round()
	a.StartBackup(BackupRequest{Cluster: "c1", Datacenter: "dc1", Backup: "b1", Pod: "p0"})
	wait(t, func() (Status, bool) { return a.BackupStatus("b1") }, Succeeded)
	segment("000002.wal", "after")
	round()
	pit := time.Now()
	time.Sleep(5 * time.Millisecond)
	segment("000003.wal", "spans the point in time")
	segment("000004.wal", "same round")
	round()
	segment("000005.wal", "later")
	round()
	entries, _ := os.ReadDir(archive)
	require.Empty(t, entries, "uploaded segments are removed")
	segs, err := catalog.CommitLog(ctx, store, "c1", "dc1", "p0")
	require.NoError(t, err)
	require.Len(t, segs, 5)

	a.StartRestore(RestoreRequest{Cluster: "c1", Datacenter: "dc1", Backup: "b1", Pod: "p0", PointInTime: &pit})
	wait(t, func() (Status, bool) { return a.RestoreStatus("b1") }, Downloaded)
	require.NoError(t, a.Import("b1"))
	wait(t, func() (Status, bool) { return a.RestoreStatus("b1") }, Succeeded)
	require.Equal(t, []string{"aaaa"}, node.imported["ks.a"])
	require.Equal(t, []string{"after", "spans the point in time", "same round"}, node.segments)
	require.Len(t, node.replays, 1)
	require.Equal(t, pit.UnixMicro(), node.replays[0].Until)
	require.Equal(t, map[string]string{"4": "ks.a", "5": "ks.b"}, node.replays[0].Tables)

	// Pruning before the backup leaves the rounds since.
	n, err := catalog.PruneCommitLog(ctx, store, "c1", segs[1].At)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// A point in time beyond what was uploaded is refused... after a round
	// that reaches now, so only the future fails.
	future := time.Now().Add(time.Hour)
	a2 := &Agent{Store: store, Mgmt: node, MgmtAddr: "x", DataDir: t.TempDir(), Cluster: "c1", Datacenter: "dc1", Pod: "p0"}
	a2.StartRestore(RestoreRequest{Cluster: "c1", Datacenter: "dc1", Backup: "b1", Pod: "p0", PointInTime: &future})
	var failed Status
	require.Eventually(t, func() bool { failed, _ = a2.RestoreStatus("b1"); return failed.Phase == Failed }, 5*time.Second, 10*time.Millisecond)
	require.Contains(t, failed.Error, "does not reach")
}
