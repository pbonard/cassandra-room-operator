// Package backup runs backups and restores (Medusa's job): per data center
// through the nodes' backup agents (the data plane), and across a
// cluster's data centers (the control plane).
package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1 "github.com/pbonard/cassandra-room-operator/operator/api/v1alpha1"
	"github.com/pbonard/cassandra-room-operator/operator/internal/agent"
	"github.com/pbonard/cassandra-room-operator/operator/internal/catalog"
	"github.com/pbonard/cassandra-room-operator/operator/internal/dc"
	"github.com/pbonard/cassandra-room-operator/operator/internal/names"
	"github.com/pbonard/cassandra-room-operator/operator/internal/objstore"
)

const finalizer = names.Group + "/backup"

// StoreFunc opens a data center's object store.
type StoreFunc func(ctx context.Context, d *v1.CassandraRoomDatacenter) (objstore.Store, error)

// Common are what the data plane's backup controllers use.
type Common struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	Agents   agent.Client
	Store    StoreFunc
	Roles    dc.RoleManager
	// CollectGrace protects files younger than this from collection.
	CollectGrace time.Duration
}

func (c *Common) datacenter(ctx context.Context, ns, name string) (*v1.CassandraRoomDatacenter, error) {
	d := &v1.CassandraRoomDatacenter{}
	return d, c.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, d)
}

func saveStatus(ctx context.Context, c client.Client, obj client.Object, before, after any) error {
	a, _ := json.Marshal(before)
	b, _ := json.Marshal(after)
	if string(a) == string(b) {
		return nil
	}
	return c.Status().Update(ctx, obj)
}

// DatacenterBackupReconciler runs DatacenterBackups.
type DatacenterBackupReconciler struct{ Common }

// +kubebuilder:rbac:groups=cassandraroom.pbonard.github.io,resources=datacenterbackups;datacenterbackups/status;datacenterbackups/finalizers;datacenterrestores;datacenterrestores/status;datacenterrestores/finalizers,verbs=get;list;watch;create;update;patch;delete

func (r *DatacenterBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&v1.DatacenterBackup{}).Named("datacenterbackup").Complete(r)
}

func (r *DatacenterBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	b := &v1.DatacenterBackup{}
	if err := r.Get(ctx, req.NamespacedName, b); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	d, err := r.datacenter(ctx, b.Namespace, b.Spec.Datacenter)
	if !b.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, r.delete(ctx, b, d, err)
	}
	if err != nil {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, client.IgnoreNotFound(err)
	}
	if !controllerutil.ContainsFinalizer(b, finalizer) {
		controllerutil.AddFinalizer(b, finalizer)
		if err := r.Update(ctx, b); err != nil {
			return ctrl.Result{}, err
		}
	}
	before := b.Status.DeepCopy()
	st := &b.Status
	if st.Phase == v1.BackupSucceeded || st.Phase == v1.BackupFailed {
		return ctrl.Result{}, nil
	}
	if d.Spec.Backup == nil {
		st.Phase, st.Message = v1.BackupFailed, "the data center has no backup storage"
		return ctrl.Result{}, saveStatus(ctx, r.Client, b, before, st)
	}
	eps, err := dc.Endpoints(ctx, r.Client, d)
	if err != nil {
		return ctrl.Result{}, err
	}
	byPod := map[string]dc.Endpoint{}
	for _, e := range eps {
		byPod[e.Pod] = e
	}
	if st.Phase == "" || st.Phase == v1.BackupPending {
		// Every node backs up: the data center must be ready.
		if d.Status.Phase != v1.PhaseReady || len(eps) == 0 {
			st.Phase, st.Message = v1.BackupPending, "waiting for the data center to be ready"
			return ctrl.Result{RequeueAfter: 10 * time.Second}, saveStatus(ctx, r.Client, b, before, st)
		}
		st.Phase, st.Message, st.StartedAt = v1.BackupRunning, "", &metav1.Time{Time: time.Now()}
		st.Nodes = nil
		for _, e := range eps {
			st.Nodes = append(st.Nodes, v1.NodeTransfer{Pod: e.Pod, HostID: e.HostID, Phase: v1.BackupPending})
		}
	}
	done, failed := 0, 0
	for i := range st.Nodes {
		n := &st.Nodes[i]
		if n.Phase == v1.BackupSucceeded || n.Phase == v1.BackupFailed {
			if n.Phase == v1.BackupSucceeded {
				done++
			} else {
				failed++
			}
			continue
		}
		e, ok := byPod[n.Pod]
		if !ok {
			continue // the pod is restarting
		}
		s, err := r.Agents.BackupStatus(ctx, e.Agent, b.BackupName())
		if errors.Is(err, agent.ErrNoJob) {
			s, err = r.Agents.StartBackup(ctx, e.Agent, agent.BackupRequest{Cluster: d.Spec.ClusterName, Datacenter: d.DCName(),
				Backup: b.BackupName(), Pod: n.Pod, Keyspaces: b.Spec.Keyspaces})
		}
		if err != nil {
			n.Error = err.Error()
			continue
		}
		n.Files, n.Bytes, n.Transferred, n.Error = int32(s.Files), s.Bytes, s.Transferred, s.Error
		if s.HostID != "" {
			n.HostID = s.HostID
		}
		switch s.Phase {
		case agent.Succeeded:
			n.Phase = v1.BackupSucceeded
			done++
		case agent.Failed:
			n.Phase = v1.BackupFailed
			failed++
		default:
			n.Phase = v1.BackupRunning
		}
	}
	switch {
	case done+failed < len(st.Nodes):
		return ctrl.Result{RequeueAfter: 2 * time.Second}, saveStatus(ctx, r.Client, b, before, st)
	case failed > 0:
		st.Phase, st.Message = v1.BackupFailed, fmt.Sprintf("%d nodes failed", failed)
		r.Recorder.Event(b, corev1.EventTypeWarning, "BackupFailed", st.Message)
	default:
		store, err := r.Store(ctx, d)
		if err != nil {
			return ctrl.Result{}, err
		}
		idx := catalog.Index{Datacenter: d.DCName(), Keyspaces: b.Spec.Keyspaces, FinishedAt: time.Now().UTC()}
		for _, n := range st.Nodes {
			idx.Pods = append(idx.Pods, n.Pod)
		}
		if err := catalog.PutJSON(ctx, store, catalog.IndexKey(d.Spec.ClusterName, b.BackupName(), d.DCName()), &idx); err != nil {
			return ctrl.Result{}, err
		}
		st.Phase = v1.BackupSucceeded
		r.Recorder.Eventf(b, corev1.EventTypeNormal, "BackupSucceeded", "%d nodes backed up", len(st.Nodes))
	}
	st.FinishedAt = &metav1.Time{Time: time.Now()}
	return ctrl.Result{}, saveStatus(ctx, r.Client, b, before, st)
}

// delete removes the data center's part of the backup from the store,
// then the files no backup uses any more.
func (r *DatacenterBackupReconciler) delete(ctx context.Context, b *v1.DatacenterBackup, d *v1.CassandraRoomDatacenter, dcErr error) error {
	if !controllerutil.ContainsFinalizer(b, finalizer) {
		return nil
	}
	if dcErr == nil && d.Spec.Backup != nil {
		store, err := r.Store(ctx, d)
		if err != nil {
			return err
		}
		if err := catalog.DeleteDatacenter(ctx, store, d.Spec.ClusterName, b.BackupName(), d.DCName()); err != nil {
			return err
		}
		grace := r.CollectGrace
		if grace == 0 {
			grace = 6 * time.Hour
		}
		if _, err := catalog.Collect(ctx, store, d.Spec.ClusterName, grace, time.Now()); err != nil {
			return err
		}
		// The commit log is needed only from the oldest backup on.
		if d.Spec.Backup.CommitLogArchive != nil {
			oldest, err := catalog.OldestBackup(ctx, store, d.Spec.ClusterName)
			if err != nil {
				return err
			}
			if !oldest.IsZero() {
				if _, err := catalog.PruneCommitLog(ctx, store, d.Spec.ClusterName, oldest); err != nil {
					return err
				}
			}
		}
	}
	controllerutil.RemoveFinalizer(b, finalizer)
	return r.Update(ctx, b)
}

// DatacenterRestoreReconciler runs DatacenterRestores: every node
// downloads its files as soon as the restore starts; the tables are
// truncated (by one data center of the cluster); then the nodes import.
type DatacenterRestoreReconciler struct{ Common }

func (r *DatacenterRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&v1.DatacenterRestore{}).Named("datacenterrestore").Complete(r)
}

func (r *DatacenterRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	rs := &v1.DatacenterRestore{}
	if err := r.Get(ctx, req.NamespacedName, rs); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	before := rs.Status.DeepCopy()
	st := &rs.Status
	if st.Phase == v1.BackupSucceeded || st.Phase == v1.BackupFailed {
		return ctrl.Result{}, nil
	}
	d, err := r.datacenter(ctx, rs.Namespace, rs.Spec.Datacenter)
	if err != nil {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, client.IgnoreNotFound(err)
	}
	fail := func(msg string) (ctrl.Result, error) {
		st.Phase, st.Message, st.FinishedAt = v1.BackupFailed, msg, &metav1.Time{Time: time.Now()}
		r.Recorder.Event(rs, corev1.EventTypeWarning, "RestoreFailed", msg)
		return ctrl.Result{}, saveStatus(ctx, r.Client, rs, before, st)
	}
	if d.Spec.Backup == nil {
		return fail("the data center has no backup storage")
	}
	store, err := r.Store(ctx, d)
	if err != nil {
		return ctrl.Result{}, err
	}
	cluster, dcName := d.Spec.ClusterName, d.DCName()
	// Where the files come from: this data center's own part, or (Load)
	// any data center of any cluster's backup.
	srcCluster, srcDC := cluster, dcName
	if rs.Spec.Load {
		if rs.Spec.SourceCluster != "" {
			srcCluster = rs.Spec.SourceCluster
		}
		srcDC = rs.Spec.SourceDatacenter
		if srcDC == "" {
			backups, err := catalog.Backups(ctx, store, srcCluster)
			if err != nil {
				return ctrl.Result{}, err
			}
			if len(backups[rs.Spec.Backup]) == 0 {
				return fail(fmt.Sprintf("cluster %s has no complete backup %q", srcCluster, rs.Spec.Backup))
			}
			srcDC = backups[rs.Spec.Backup][0]
		}
	} else if rs.Spec.SourceCluster != "" && rs.Spec.SourceCluster != cluster {
		return fail("restoring another cluster's backup needs load")
	}
	if rs.Spec.PointInTime != nil && (rs.Spec.Load || d.Spec.Backup.CommitLogArchive == nil) {
		return fail("a point in time needs each node's own backup (import) and commit log archiving")
	}
	var idx catalog.Index
	if err := catalog.GetJSON(ctx, store, catalog.IndexKey(srcCluster, rs.Spec.Backup, srcDC), &idx); errors.Is(err, objstore.ErrNotFound) {
		return fail(fmt.Sprintf("backup %q has no complete part for data center %s", rs.Spec.Backup, srcDC))
	} else if err != nil {
		return ctrl.Result{}, err
	}
	eps, err := dc.Endpoints(ctx, r.Client, d)
	if err != nil {
		return ctrl.Result{}, err
	}
	if st.Phase == "" || st.Phase == v1.BackupPending {
		if d.Status.Phase != v1.PhaseReady {
			st.Phase, st.Message = v1.BackupPending, "waiting for the data center to be ready"
			return ctrl.Result{RequeueAfter: 10 * time.Second}, saveStatus(ctx, r.Client, rs, before, st)
		}
		pods := make([]string, 0, len(eps))
		for _, e := range eps {
			pods = append(pods, e.Pod)
		}
		sort.Strings(pods)
		want := slices.Clone(idx.Pods)
		sort.Strings(want)
		if !rs.Spec.Load && !slices.Equal(pods, want) {
			return fail(fmt.Sprintf("the data center's pods %v are not the backup's %v: importing needs the same topology (load does not)", pods, want))
		}
		st.Phase, st.StartedAt, st.Nodes = v1.BackupRunning, &metav1.Time{Time: time.Now()}, nil
		for _, p := range pods {
			st.Nodes = append(st.Nodes, v1.NodeTransfer{Pod: p, Phase: v1.BackupPending})
		}
	}
	// Load: the backup's nodes, spread over this data center's.
	sources := map[string][]string{}
	if rs.Spec.Load && len(st.Nodes) > 0 {
		want := slices.Clone(idx.Pods)
		sort.Strings(want)
		for i, p := range want {
			n := st.Nodes[i%len(st.Nodes)].Pod
			sources[n] = append(sources[n], p)
		}
	}
	byPod := map[string]dc.Endpoint{}
	var hosts []string
	for _, e := range eps {
		byPod[e.Pod] = e
		if e.Ready {
			hosts = append(hosts, ipOf(e.Management))
		}
	}
	// Downloads, then imports once allowed.
	downloaded, done := 0, 0
	for i := range st.Nodes {
		n := &st.Nodes[i]
		e, ok := byPod[n.Pod]
		if !ok || n.Phase == v1.BackupSucceeded {
			if n.Phase == v1.BackupSucceeded {
				done++
				downloaded++
			}
			continue
		}
		s, err := r.Agents.RestoreStatus(ctx, e.Agent, rs.Spec.Backup)
		if errors.Is(err, agent.ErrNoJob) {
			req := agent.RestoreRequest{Cluster: srcCluster, Datacenter: srcDC,
				Backup: rs.Spec.Backup, Pod: n.Pod, Keyspaces: rs.Spec.Keyspaces,
				Load: rs.Spec.Load, Sources: sources[n.Pod], Consistency: rs.Spec.Consistency}
			if rs.Spec.PointInTime != nil {
				req.PointInTime = &rs.Spec.PointInTime.Time
			}
			s, err = r.Agents.StartRestore(ctx, e.Agent, req)
		}
		if err != nil {
			n.Error = err.Error()
			continue
		}
		n.Files, n.Bytes, n.Transferred, n.Error = int32(s.Files), s.Bytes, s.Transferred, s.Error
		switch s.Phase {
		case agent.Failed:
			return fail(fmt.Sprintf("%s: %s", n.Pod, s.Error))
		case agent.Downloaded:
			downloaded++
			n.Phase = v1.BackupRunning
			if rs.Spec.Import {
				if err := r.Agents.Import(ctx, e.Agent, rs.Spec.Backup); err != nil {
					n.Error = err.Error()
				}
			}
		case agent.Importing:
			downloaded++
			n.Phase = v1.RestoreImporting
		case agent.Succeeded:
			downloaded++
			done++
			n.Phase = v1.BackupSucceeded
		default:
			n.Phase = v1.BackupRunning
		}
	}
	if rs.Spec.Truncate && !st.Truncated {
		tables, err := backupTables(ctx, store, srcCluster, rs.Spec.Backup, srcDC, idx.Pods, rs.Spec.Keyspaces)
		if err != nil {
			return ctrl.Result{}, err
		}
		st.Phase = v1.RestoreTruncating
		conn, user, pass, err := dc.SuperuserConn(ctx, r.Client, d, hosts)
		if err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Roles.Truncate(ctx, conn, user, pass, tables); err != nil {
			st.Message = err.Error()
			return ctrl.Result{RequeueAfter: 5 * time.Second}, saveStatus(ctx, r.Client, rs, before, st)
		}
		st.Truncated, st.Message = true, ""
		r.Recorder.Eventf(rs, corev1.EventTypeNormal, "Truncated", "truncated %d tables", len(tables))
	}
	if rs.Spec.Import {
		st.Phase = v1.RestoreImporting
	}
	if done == len(st.Nodes) {
		st.Phase, st.FinishedAt = v1.BackupSucceeded, &metav1.Time{Time: time.Now()}
		r.Recorder.Eventf(rs, corev1.EventTypeNormal, "RestoreSucceeded", "%d nodes restored", done)
		return ctrl.Result{}, saveStatus(ctx, r.Client, rs, before, st)
	}
	return ctrl.Result{RequeueAfter: 2 * time.Second}, saveStatus(ctx, r.Client, rs, before, st)
}

// backupTables are the tables ("keyspace.table") of a backup.
func backupTables(ctx context.Context, store objstore.Store, cluster, backup, dcName string, pods, keyspaces []string) ([]string, error) {
	seen := map[string]bool{}
	for _, p := range pods {
		var m catalog.Manifest
		if err := catalog.GetJSON(ctx, store, catalog.ManifestKey(cluster, backup, dcName, p), &m); err != nil {
			return nil, err
		}
		for _, t := range m.Tables {
			if len(keyspaces) == 0 || slices.Contains(keyspaces, t.Keyspace) {
				seen[t.Keyspace+"."+t.Table] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	sort.Strings(out)
	return out, nil
}

func ipOf(addr string) string {
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return addr[:i]
		}
	}
	return addr
}
