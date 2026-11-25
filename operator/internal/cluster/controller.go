// Package cluster runs CassandraRoomClusters: the control plane
// (k8ssandra-operator's job). It creates the data centers in the
// Kubernetes clusters they run in, in order, links them (seeds), chooses
// the metadata service's members across them, copies the secrets they
// need, and creates the cluster's repair schedules.
package cluster

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
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
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	v1 "github.com/pbonard/cassandra-room-operator/operator/api/v1alpha1"
	"github.com/pbonard/cassandra-room-operator/operator/internal/names"
	"github.com/pbonard/cassandra-room-operator/operator/internal/remote"
)

const finalizer = names.Group + "/cluster"

// Reconciler runs CassandraRoomClusters.
type Reconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	Remote   *remote.Clients

	ctrl controller.Controller
}

// +kubebuilder:rbac:groups=cassandraroom.pbonard.github.io,resources=cassandraroomclusters;cassandraroomclusters/status;cassandraroomclusters/finalizers;clientconfigs;repairschedules,verbs=get;list;watch;create;update;patch;delete

// SetupWithManager registers the controller.
// Its data centers' changes reach it by watches: of the local cluster
// through the manager, of remote clusters through their caches (see
// watchRemote).
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	c, err := ctrl.NewControllerManagedBy(mgr).For(&v1.CassandraRoomCluster{}).
		Watches(&v1.CassandraRoomDatacenter{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []reconcile.Request {
			return remote.Owner(o)
		})).
		Named("cassandraroomcluster").Build(r)
	if err != nil {
		return err
	}
	r.ctrl = c
	return mgr.Add(r.Remote.Stopper())
}

// watchRemote watches the data centers of a remote cluster.
func (r *Reconciler) watchRemote(ctx context.Context, k8sContext string) error {
	return r.Remote.Watch(ctx, k8sContext, "cluster", func(c cache.Cache) error {
		return r.ctrl.Watch(source.Kind(c, &v1.CassandraRoomDatacenter{},
			handler.TypedEnqueueRequestsFromMapFunc(func(_ context.Context, o *v1.CassandraRoomDatacenter) []reconcile.Request {
				return remote.Owner(o)
			})))
	})
}

// dcRef is a data center of the cluster, in its Kubernetes cluster.
type dcRef struct {
	tmpl *v1.DatacenterTemplate
	c    client.Client
	key  types.NamespacedName
	obj  *v1.CassandraRoomDatacenter // nil if not created yet
}

// DatacenterObject names a cluster's data center object.
func DatacenterObject(cluster, dc string) string { return names.Sanitize(cluster + "-" + dc) }

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	cl := &v1.CassandraRoomCluster{}
	if err := r.Get(ctx, req.NamespacedName, cl); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !cl.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, cl)
	}
	if !controllerutil.ContainsFinalizer(cl, finalizer) {
		controllerutil.AddFinalizer(cl, finalizer)
		if err := r.Update(ctx, cl); err != nil {
			return ctrl.Result{}, err
		}
	}
	status := cl.Status.DeepCopy()
	superuser, err := r.ensureSuperuser(ctx, cl)
	if err != nil {
		return ctrl.Result{}, err
	}
	refs := make([]*dcRef, len(cl.Spec.Datacenters))
	for i := range cl.Spec.Datacenters {
		t := &cl.Spec.Datacenters[i]
		c, err := r.Remote.Get(ctx, t.K8sContext)
		if err != nil {
			return ctrl.Result{}, err
		}
		if err := r.watchRemote(ctx, t.K8sContext); err != nil {
			return ctrl.Result{}, err
		}
		ns := t.Namespace
		if ns == "" {
			ns = cl.Namespace
		}
		ref := &dcRef{tmpl: t, c: c, key: types.NamespacedName{Namespace: ns, Name: DatacenterObject(cl.Name, t.Name)}}
		obj := &v1.CassandraRoomDatacenter{}
		if err := c.Get(ctx, ref.key, obj); err == nil {
			ref.obj = obj
		} else if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		refs[i] = ref
	}

	// Seeds of each data center, for the others.
	seedsOf := map[string][]string{}
	for _, ref := range refs {
		if ref.obj != nil {
			seedsOf[ref.tmpl.Name] = ref.obj.Status.SeedAddresses
		}
	}
	allReady := true
	for _, ref := range refs {
		if ref.obj == nil || ref.obj.Status.Phase != v1.PhaseReady || ref.obj.Status.ObservedGeneration != ref.obj.Generation {
			allReady = false
		}
	}
	cms := status.CMSMembers
	if allReady {
		cms = chooseCMS(refs)
	}
	status.Datacenters = nil
	previousReady := true
	for i, ref := range refs {
		var additional []string
		for _, other := range refs {
			if other != ref {
				additional = append(additional, seedsOf[other.tmpl.Name]...)
			}
		}
		spec := datacenterSpec(cl, ref.tmpl, superuser, additional)
		if i == 0 {
			spec.CMSMembers = cms
		}
		if err := r.copySecrets(ctx, cl, ref, superuser); err != nil {
			return ctrl.Result{}, err
		}
		switch {
		case ref.obj == nil && previousReady && (i == 0 || len(additional) > 0):
			// Data centers start in order: each joins through the ready
			// ones' seeds.
			ref.obj = &v1.CassandraRoomDatacenter{ObjectMeta: metav1.ObjectMeta{Name: ref.key.Name, Namespace: ref.key.Namespace,
				Labels: map[string]string{names.LabelCluster: names.Sanitize(cl.Name), names.LabelName: names.AppName}}, Spec: spec}
			remote.SetOwner(ref.obj, cl)
			if err := ref.c.Create(ctx, ref.obj); err != nil {
				return ctrl.Result{}, err
			}
			r.Recorder.Eventf(cl, corev1.EventTypeNormal, "DatacenterCreated", "created data center %s", ref.tmpl.Name)
		case ref.obj != nil && (!equalJSON(ref.obj.Spec, spec) || remote.SetOwner(ref.obj, cl)):
			ref.obj.Spec = spec
			if err := ref.c.Update(ctx, ref.obj); err != nil {
				return ctrl.Result{}, err
			}
		}
		sum := v1.DatacenterSummary{Name: ref.tmpl.Name, K8sContext: ref.tmpl.K8sContext, Size: ref.tmpl.Size}
		if ref.obj != nil {
			sum.Phase, sum.ReadyNodes, sum.Seeds = ref.obj.Status.Phase, ref.obj.Status.ReadyNodes, ref.obj.Status.SeedAddresses
		}
		if ref.obj == nil || ref.obj.Status.Phase != v1.PhaseReady {
			previousReady = false
		}
		status.Datacenters = append(status.Datacenters, sum)
	}
	if err := r.removeDatacenters(ctx, cl, refs); err != nil {
		return ctrl.Result{}, err
	}
	if len(refs) > 0 && refs[0].obj != nil {
		if err := r.ensureRepairs(ctx, cl, refs[0]); err != nil {
			return ctrl.Result{}, err
		}
	}
	status.CMSMembers = cms
	status.ObservedGeneration = cl.Generation
	status.Phase = v1.PhaseReady
	if !allReady {
		status.Phase = v1.PhaseUpdating
		if !previousReady && slices.ContainsFunc(refs, func(r *dcRef) bool { return r.obj == nil }) {
			status.Phase = v1.PhaseInitializing
		}
	}
	cond := metav1.Condition{Type: v1.ConditionReady, Status: metav1.ConditionFalse, Reason: string(status.Phase), ObservedGeneration: cl.Generation}
	if allReady {
		cond.Status = metav1.ConditionTrue
	}
	meta.SetStatusCondition(&status.Conditions, cond)
	if !equalJSON(cl.Status, *status) {
		cl.Status = *status
		if err := r.Status().Update(ctx, cl); err != nil {
			return ctrl.Result{}, err
		}
	}
	logger.V(1).Info("reconciled", "phase", status.Phase)
	// Data centers' changes come through watches; this is a safety resync.
	if !allReady {
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}
	return ctrl.Result{RequeueAfter: 10 * time.Minute}, nil
}

func equalJSON(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

// datacenterSpec is a data center's spec from the cluster's.
func datacenterSpec(cl *v1.CassandraRoomCluster, t *v1.DatacenterTemplate, superuser string, seeds []string) v1.CassandraRoomDatacenterSpec {
	config := cl.Spec.Config
	if t.Config != nil {
		config = mergeConfig(config, *t.Config)
	}
	networking := cl.Spec.Networking
	if t.Networking != nil {
		networking = *t.Networking
	}
	sort.Strings(seeds)
	return v1.CassandraRoomDatacenterSpec{
		ClusterName: cl.Name, DatacenterName: t.Name,
		Image: cl.Spec.Image, AgentImage: cl.Spec.AgentImage, ImagePullPolicy: cl.Spec.ImagePullPolicy, ImagePullSecrets: cl.Spec.ImagePullSecrets,
		Size: t.Size, Racks: t.Racks, Storage: t.Storage, Pod: t.Pod, Config: config, TLS: cl.Spec.TLS, Networking: networking,
		SuperuserSecret: superuser, AdditionalSeeds: seeds, Backup: cl.Spec.Backup,
		ReplaceNodes: t.ReplaceNodes, Stopped: t.Stopped, RollingRestartRequestedAt: t.RollingRestartRequestedAt,
	}
}

// mergeConfig overrides base's settings with those set in o.
func mergeConfig(base, o v1.ServerConfig) v1.ServerConfig {
	if o.NumTokens != nil {
		base.NumTokens = o.NumTokens
	}
	if o.CommitlogSync != "" {
		base.CommitlogSync = o.CommitlogSync
	}
	if o.CommitlogSyncPeriod != "" {
		base.CommitlogSyncPeriod = o.CommitlogSyncPeriod
	}
	if o.MemtableSpace != nil {
		base.MemtableSpace = o.MemtableSpace
	}
	if o.Authorizer != "" {
		base.Authorizer = o.Authorizer
	}
	base.ExtraArgs = append(slices.Clone(base.ExtraArgs), o.ExtraArgs...)
	return base
}

// chooseCMS picks the metadata service's members: as many data centers as
// possible, an odd number, at least three, at most seven; seeds first.
func chooseCMS(refs []*dcRef) []string {
	n := len(refs)
	if n < 3 {
		n = 3
	} else if n%2 == 0 {
		n++
	}
	n = min(n, 7)
	candidates := make([][]string, len(refs))
	total := 0
	for i, ref := range refs {
		c := slices.Clone(ref.obj.Status.SeedAddresses)
		for _, node := range ref.obj.Status.Nodes {
			if node.Ready && node.Address != "" && !slices.Contains(c, node.Address) {
				c = append(c, node.Address)
			}
		}
		candidates[i] = c
		total += len(c)
	}
	n = min(n, total)
	var out []string
	for round := 0; len(out) < n; round++ {
		for _, c := range candidates {
			if round < len(c) && len(out) < n {
				out = append(out, c[round])
			}
		}
	}
	sort.Strings(out)
	return out
}

// ensureSuperuser creates the superuser secret if missing; it returns its
// name.
func (r *Reconciler) ensureSuperuser(ctx context.Context, cl *v1.CassandraRoomCluster) (string, error) {
	name := cl.Spec.SuperuserSecret
	if name == "" {
		name = names.SuperuserSecret(cl.Name)
	}
	s := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Namespace: cl.Namespace, Name: name}, s)
	if apierrors.IsNotFound(err) {
		s = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cl.Namespace},
			Data: map[string][]byte{"username": []byte(names.Sanitize(cl.Name) + "-superuser"), "password": []byte(randomPassword())}}
		if err := controllerutil.SetControllerReference(cl, s, r.Scheme); err != nil {
			return "", err
		}
		return name, r.Create(ctx, s)
	}
	return name, err
}

// copySecrets copies the secrets a data center needs into its namespace
// (and cluster) when they are not there: the superuser's, and the TLS and
// backup credentials ones the control plane has.
func (r *Reconciler) copySecrets(ctx context.Context, cl *v1.CassandraRoomCluster, ref *dcRef, superuser string) error {
	wanted := []string{superuser}
	t := cl.Spec.TLS
	if t.Internode != nil {
		wanted = append(wanted, t.Internode.SecretName)
	}
	if t.Client != nil {
		wanted = append(wanted, t.Client.SecretName)
	}
	if t.Management != nil {
		wanted = append(wanted, t.Management.SecretName)
	}
	if b := cl.Spec.Backup; b != nil && b.CredentialsSecret != nil {
		wanted = append(wanted, b.CredentialsSecret.SecretName)
	}
	for _, name := range wanted {
		if ref.c == r.Client && ref.key.Namespace == cl.Namespace {
			continue // the same secret
		}
		src := &corev1.Secret{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: cl.Namespace, Name: name}, src); apierrors.IsNotFound(err) {
			continue // to be created there by the user
		} else if err != nil {
			return err
		}
		dst := &corev1.Secret{}
		err := ref.c.Get(ctx, types.NamespacedName{Namespace: ref.key.Namespace, Name: name}, dst)
		switch {
		case apierrors.IsNotFound(err):
			dst = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ref.key.Namespace,
				Labels: map[string]string{names.LabelCluster: names.Sanitize(cl.Name)}}, Type: src.Type, Data: src.Data}
			if err := ref.c.Create(ctx, dst); err != nil {
				return err
			}
		case err != nil:
			return err
		case dst.Labels[names.LabelCluster] == names.Sanitize(cl.Name) && !equalJSON(dst.Data, src.Data):
			dst.Data = src.Data // a copy: kept up to date
			if err := ref.c.Update(ctx, dst); err != nil {
				return err
			}
		}
	}
	return nil
}

// ensureRepairs creates the cluster's repair schedules beside its first
// data center.
func (r *Reconciler) ensureRepairs(ctx context.Context, cl *v1.CassandraRoomCluster, first *dcRef) error {
	for _, rt := range cl.Spec.Repairs {
		name := names.Sanitize(cl.Name + "-" + rt.Name)
		want := v1.RepairScheduleSpec{Datacenter: first.key.Name, RepairScheduleSettings: rt.Spec}
		rs := &v1.RepairSchedule{}
		err := first.c.Get(ctx, types.NamespacedName{Namespace: first.key.Namespace, Name: name}, rs)
		switch {
		case apierrors.IsNotFound(err):
			rs = &v1.RepairSchedule{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: first.key.Namespace,
				Labels: map[string]string{names.LabelCluster: names.Sanitize(cl.Name)}}, Spec: want}
			if err := first.c.Create(ctx, rs); err != nil {
				return err
			}
		case err != nil:
			return err
		case !equalJSON(rs.Spec, want):
			rs.Spec = want
			if err := first.c.Update(ctx, rs); err != nil {
				return err
			}
		}
	}
	return nil
}

// removeDatacenters retires the data centers no longer in the spec: their
// nodes leave the ring one by one (size 0), then the object goes. The
// keyspaces must no longer replicate to them.
func (r *Reconciler) removeDatacenters(ctx context.Context, cl *v1.CassandraRoomCluster, refs []*dcRef) error {
	for _, sum := range cl.Status.Datacenters {
		if slices.ContainsFunc(refs, func(r *dcRef) bool { return r.tmpl.Name == sum.Name }) {
			continue
		}
		c, err := r.Remote.Get(ctx, sum.K8sContext)
		if err != nil {
			return err
		}
		list := &v1.CassandraRoomDatacenterList{}
		if err := c.List(ctx, list, client.MatchingLabels{names.LabelCluster: names.Sanitize(cl.Name)}); err != nil {
			return err
		}
		for i := range list.Items {
			dc := &list.Items[i]
			if dc.Spec.DatacenterName != sum.Name {
				continue
			}
			if dc.Spec.Size != 0 {
				dc.Spec.Size = 0
				if err := c.Update(ctx, dc); err != nil {
					return err
				}
				r.Recorder.Eventf(cl, corev1.EventTypeNormal, "RemovingDatacenter", "decommissioning the nodes of %s", sum.Name)
			} else if len(dc.Status.Nodes) == 0 {
				if err := c.Delete(ctx, dc); err != nil && !apierrors.IsNotFound(err) {
					return err
				}
			}
		}
	}
	return nil
}

// finalize deletes the data centers in every cluster.
func (r *Reconciler) finalize(ctx context.Context, cl *v1.CassandraRoomCluster) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(cl, finalizer) {
		return ctrl.Result{}, nil
	}
	remaining := false
	for _, t := range cl.Spec.Datacenters {
		c, err := r.Remote.Get(ctx, t.K8sContext)
		if err != nil {
			return ctrl.Result{}, err
		}
		ns := t.Namespace
		if ns == "" {
			ns = cl.Namespace
		}
		dc := &v1.CassandraRoomDatacenter{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: DatacenterObject(cl.Name, t.Name)}}
		if err := c.Delete(ctx, dc); err == nil {
			remaining = true
		} else if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
	}
	if remaining {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	controllerutil.RemoveFinalizer(cl, finalizer)
	return ctrl.Result{}, r.Update(ctx, cl)
}

func randomPassword() string {
	return fmt.Sprintf("%x", randomBytes(24))
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}
