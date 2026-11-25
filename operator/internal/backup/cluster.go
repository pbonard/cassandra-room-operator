package backup

import (
	"context"
	"sort"
	"time"

	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	v1 "github.com/pbonard/cassandra-room-operator/operator/api/v1alpha1"
	"github.com/pbonard/cassandra-room-operator/operator/internal/cluster"
	"github.com/pbonard/cassandra-room-operator/operator/internal/names"
	"github.com/pbonard/cassandra-room-operator/operator/internal/remote"
)

// Control plane: a cluster's backups and restores are one per data center,
// in the data centers' Kubernetes clusters.

// ControlPlane are what the control plane's controllers use.
type ControlPlane struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	Remote   *remote.Clients
	Now      func() time.Time
}

// LabelBackup marks a data center's part of a cluster backup or restore;
// LabelSchedule a schedule's backups.
const (
	LabelBackup   = names.Group + "/backup"
	LabelRestore  = names.Group + "/restore"
	LabelSchedule = names.Group + "/schedule"
)

// dcTarget is where a data center's part goes.
type dcTarget struct {
	name       string
	k8sContext string
	c          client.Client
	ns         string
	obj        string // the CassandraRoomDatacenter
}

func (cp *ControlPlane) targets(ctx context.Context, clusterNs, clusterName string) ([]dcTarget, error) {
	cl := &v1.CassandraRoomCluster{}
	if err := cp.Get(ctx, types.NamespacedName{Namespace: clusterNs, Name: clusterName}, cl); err != nil {
		return nil, err
	}
	var out []dcTarget
	for _, t := range cl.Spec.Datacenters {
		c, err := cp.Remote.Get(ctx, t.K8sContext)
		if err != nil {
			return nil, err
		}
		ns := t.Namespace
		if ns == "" {
			ns = cl.Namespace
		}
		out = append(out, dcTarget{name: t.Name, k8sContext: t.K8sContext, c: c, ns: ns, obj: cluster.DatacenterObject(cl.Name, t.Name)})
	}
	return out, nil
}

func partName(parent, dc string) string { return names.Sanitize(parent + "-" + dc) }

// BackupReconciler runs CassandraRoomBackups.
type BackupReconciler struct {
	ControlPlane
	ctrl controller.Controller
}

// +kubebuilder:rbac:groups=cassandraroom.pbonard.github.io,resources=cassandraroombackups;cassandraroombackups/status;cassandraroombackups/finalizers;cassandraroombackupschedules;cassandraroombackupschedules/status;cassandraroomrestores;cassandraroomrestores/status,verbs=get;list;watch;create;update;patch;delete

func (r *BackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	c, err := ctrl.NewControllerManagedBy(mgr).For(&v1.CassandraRoomBackup{}).
		Watches(&v1.DatacenterBackup{}, handler.EnqueueRequestsFromMapFunc(ownerOf)).
		Named("cassandraroombackup").Build(r)
	r.ctrl = c
	return err
}

func ownerOf(_ context.Context, o client.Object) []reconcile.Request { return remote.Owner(o) }

// watchParts watches the parts of type obj in the data centers' remote
// clusters.
func watchParts[T client.Object](ctx context.Context, cp *ControlPlane, c controller.Controller, id string, obj T, targets []dcTarget) error {
	for _, t := range targets {
		err := cp.Remote.Watch(ctx, t.k8sContext, id, func(ch cache.Cache) error {
			return c.Watch(source.Kind(ch, obj, handler.TypedEnqueueRequestsFromMapFunc(func(_ context.Context, o T) []reconcile.Request {
				return remote.Owner(o)
			})))
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *BackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	b := &v1.CassandraRoomBackup{}
	if err := r.Get(ctx, req.NamespacedName, b); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	targets, err := r.targets(ctx, b.Namespace, b.Spec.Cluster)
	if !b.DeletionTimestamp.IsZero() {
		return r.delete(ctx, b, targets, err)
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
	if err := watchParts(ctx, &r.ControlPlane, r.ctrl, "backup", &v1.DatacenterBackup{}, targets); err != nil {
		return ctrl.Result{}, err
	}
	if st.StartedAt == nil {
		st.StartedAt, st.Phase = &metav1.Time{Time: time.Now()}, v1.BackupRunning
	}
	st.Datacenters = nil
	done, failed := 0, 0
	for _, t := range targets {
		part := &v1.DatacenterBackup{}
		key := types.NamespacedName{Namespace: t.ns, Name: partName(b.Name, t.name)}
		err := t.c.Get(ctx, key, part)
		if apierrors.IsNotFound(err) {
			part = &v1.DatacenterBackup{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace,
				Labels: map[string]string{LabelBackup: b.Name, names.LabelCluster: names.Sanitize(b.Spec.Cluster)}},
				Spec: v1.DatacenterBackupSpec{Datacenter: t.obj, Backup: b.Name, Keyspaces: b.Spec.Keyspaces}}
			remote.SetOwner(part, b)
			if err := t.c.Create(ctx, part); err != nil {
				return ctrl.Result{}, err
			}
		} else if err != nil {
			return ctrl.Result{}, err
		}
		p := v1.DatacenterProgress{Name: t.name, Phase: part.Status.Phase, Nodes: int32(len(part.Status.Nodes))}
		for _, n := range part.Status.Nodes {
			if n.Phase == v1.BackupSucceeded {
				p.Done++
			}
			p.Bytes += n.Bytes
		}
		switch part.Status.Phase {
		case v1.BackupSucceeded:
			done++
		case v1.BackupFailed:
			failed++
			st.Message = t.name + ": " + part.Status.Message
		}
		st.Datacenters = append(st.Datacenters, p)
	}
	switch {
	case failed > 0 && done+failed == len(targets):
		st.Phase, st.FinishedAt = v1.BackupFailed, &metav1.Time{Time: time.Now()}
		r.Recorder.Event(b, corev1.EventTypeWarning, "BackupFailed", st.Message)
	case done == len(targets):
		st.Phase, st.FinishedAt = v1.BackupSucceeded, &metav1.Time{Time: time.Now()}
		r.Recorder.Event(b, corev1.EventTypeNormal, "BackupSucceeded", "every data center backed up")
	}
	if err := saveStatus(ctx, r.Client, b, before, st); err != nil {
		return ctrl.Result{}, err
	}
	if st.Phase == v1.BackupRunning {
		// The parts' changes come through watches; this is a safety resync.
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}
	return ctrl.Result{}, nil
}

// delete deletes the data centers' parts (which delete their files).
func (r *BackupReconciler) delete(ctx context.Context, b *v1.CassandraRoomBackup, targets []dcTarget, terr error) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(b, finalizer) {
		return ctrl.Result{}, nil
	}
	if terr == nil {
		remaining := false
		for _, t := range targets {
			part := &v1.DatacenterBackup{ObjectMeta: metav1.ObjectMeta{Namespace: t.ns, Name: partName(b.Name, t.name)}}
			if err := t.c.Delete(ctx, part); err == nil {
				remaining = true
			} else if !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		}
		if remaining {
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
	}
	controllerutil.RemoveFinalizer(b, finalizer)
	return ctrl.Result{}, r.Update(ctx, b)
}

// ScheduleReconciler runs CassandraRoomBackupSchedules.
type ScheduleReconciler struct{ ControlPlane }

func (r *ScheduleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Now == nil {
		r.Now = time.Now
	}
	return ctrl.NewControllerManagedBy(mgr).For(&v1.CassandraRoomBackupSchedule{}).Named("cassandraroombackupschedule").Complete(r)
}

var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

func (r *ScheduleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	s := &v1.CassandraRoomBackupSchedule{}
	if err := r.Get(ctx, req.NamespacedName, s); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	sched, err := cronParser.Parse(s.Spec.Schedule)
	if err != nil {
		r.Recorder.Eventf(s, corev1.EventTypeWarning, "BadSchedule", "%v", err)
		return ctrl.Result{}, nil
	}
	before := s.Status.DeepCopy()
	now := r.Now()
	if s.Status.NextRunAt == nil {
		s.Status.NextRunAt = &metav1.Time{Time: sched.Next(now)}
	}
	if !s.Spec.Suspend && !now.Before(s.Status.NextRunAt.Time) {
		name := names.Sanitize(s.Name + "-" + now.UTC().Format("20060102-150405"))
		b := &v1.CassandraRoomBackup{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: s.Namespace,
			Labels: map[string]string{LabelSchedule: s.Name}}, Spec: v1.CassandraRoomBackupSpec{Cluster: s.Spec.Cluster, Keyspaces: s.Spec.Keyspaces}}
		if err := r.Create(ctx, b); err != nil && !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, err
		}
		s.Status.LastBackup, s.Status.LastScheduleTime = name, &metav1.Time{Time: now}
		s.Status.NextRunAt = &metav1.Time{Time: sched.Next(now)}
	}
	if err := r.retain(ctx, s); err != nil {
		return ctrl.Result{}, err
	}
	if err := saveStatus(ctx, r.Client, s, before, &s.Status); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: max(s.Status.NextRunAt.Sub(now), time.Second)}, nil
}

// retain deletes the schedule's oldest successful backups beyond Keep.
func (r *ScheduleReconciler) retain(ctx context.Context, s *v1.CassandraRoomBackupSchedule) error {
	if s.Spec.Keep <= 0 {
		return nil
	}
	list := &v1.CassandraRoomBackupList{}
	if err := r.List(ctx, list, client.InNamespace(s.Namespace), client.MatchingLabels{LabelSchedule: s.Name}); err != nil {
		return err
	}
	var ok []v1.CassandraRoomBackup
	for _, b := range list.Items {
		if b.Status.Phase == v1.BackupSucceeded && b.DeletionTimestamp.IsZero() {
			ok = append(ok, b)
		}
	}
	sort.Slice(ok, func(i, j int) bool { return ok[i].Status.StartedAt.After(ok[j].Status.StartedAt.Time) })
	for i := int(s.Spec.Keep); i < len(ok); i++ {
		if err := r.Delete(ctx, &ok[i]); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		r.Recorder.Eventf(s, corev1.EventTypeNormal, "BackupDeleted", "deleted %s (keeping %d)", ok[i].Name, s.Spec.Keep)
	}
	return nil
}

// RestoreReconciler runs CassandraRoomRestores: every data center
// downloads; the first truncates the tables; then every one imports.
type RestoreReconciler struct {
	ControlPlane
	ctrl controller.Controller
}

func (r *RestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	c, err := ctrl.NewControllerManagedBy(mgr).For(&v1.CassandraRoomRestore{}).
		Watches(&v1.DatacenterRestore{}, handler.EnqueueRequestsFromMapFunc(ownerOf)).
		Named("cassandraroomrestore").Build(r)
	r.ctrl = c
	return err
}

func (r *RestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	rs := &v1.CassandraRoomRestore{}
	if err := r.Get(ctx, req.NamespacedName, rs); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	before := rs.Status.DeepCopy()
	st := &rs.Status
	if st.Phase == v1.BackupSucceeded || st.Phase == v1.BackupFailed {
		return ctrl.Result{}, nil
	}
	b := &v1.CassandraRoomBackup{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: rs.Namespace, Name: rs.Spec.Backup}, b); err != nil || b.Status.Phase != v1.BackupSucceeded {
		st.Phase, st.Message = v1.BackupFailed, "backup "+rs.Spec.Backup+" does not exist or did not succeed"
		return ctrl.Result{}, saveStatus(ctx, r.Client, rs, before, st)
	}
	targets, err := r.targets(ctx, rs.Namespace, rs.Spec.Cluster)
	if err != nil {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, client.IgnoreNotFound(err)
	}
	if err := watchParts(ctx, &r.ControlPlane, r.ctrl, "restore", &v1.DatacenterRestore{}, targets); err != nil {
		return ctrl.Result{}, err
	}
	if st.StartedAt == nil {
		st.StartedAt, st.Phase = &metav1.Time{Time: time.Now()}, v1.BackupRunning
	}
	parts := make([]*v1.DatacenterRestore, len(targets))
	for i, t := range targets {
		part := &v1.DatacenterRestore{}
		key := types.NamespacedName{Namespace: t.ns, Name: partName(rs.Name, t.name)}
		err := t.c.Get(ctx, key, part)
		if apierrors.IsNotFound(err) {
			part = &v1.DatacenterRestore{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace,
				Labels: map[string]string{LabelRestore: rs.Name, names.LabelCluster: names.Sanitize(rs.Spec.Cluster)}},
				Spec: v1.DatacenterRestoreSpec{Datacenter: t.obj, Backup: rs.Spec.Backup, Keyspaces: rs.Spec.Keyspaces, Truncate: i == 0}}
			remote.SetOwner(part, rs)
			if err := t.c.Create(ctx, part); err != nil {
				return ctrl.Result{}, err
			}
		} else if err != nil {
			return ctrl.Result{}, err
		}
		parts[i] = part
	}
	truncated := len(parts) > 0 && parts[0].Status.Truncated
	st.Datacenters = nil
	done, failed := 0, 0
	for i, part := range parts {
		if truncated && !part.Spec.Import {
			part.Spec.Import = true
			if err := targets[i].c.Update(ctx, part); err != nil {
				return ctrl.Result{}, err
			}
		}
		p := v1.DatacenterProgress{Name: targets[i].name, Phase: part.Status.Phase, Nodes: int32(len(part.Status.Nodes))}
		for _, n := range part.Status.Nodes {
			if n.Phase == v1.BackupSucceeded {
				p.Done++
			}
			p.Bytes += n.Bytes
		}
		switch part.Status.Phase {
		case v1.BackupSucceeded:
			done++
		case v1.BackupFailed:
			failed++
			st.Message = targets[i].name + ": " + part.Status.Message
		}
		st.Datacenters = append(st.Datacenters, p)
	}
	switch {
	case failed > 0:
		st.Phase, st.FinishedAt = v1.BackupFailed, &metav1.Time{Time: time.Now()}
		r.Recorder.Event(rs, corev1.EventTypeWarning, "RestoreFailed", st.Message)
	case done == len(parts):
		st.Phase, st.FinishedAt = v1.BackupSucceeded, &metav1.Time{Time: time.Now()}
		r.Recorder.Event(rs, corev1.EventTypeNormal, "RestoreSucceeded", "every data center restored")
	case truncated:
		st.Phase = v1.RestoreImporting
	default:
		st.Phase = v1.RestoreTruncating
	}
	if err := saveStatus(ctx, r.Client, rs, before, st); err != nil {
		return ctrl.Result{}, err
	}
	if st.Phase == v1.BackupSucceeded || st.Phase == v1.BackupFailed {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: time.Minute}, nil // a safety resync: the parts are watched
}
