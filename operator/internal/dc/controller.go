// Package dc runs data centers: the CassandraRoomDatacenter controller (the
// data plane, cass-operator's job).
package dc

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "github.com/pbonard/cassandra-room-operator/operator/api/v1alpha1"
	"github.com/pbonard/cassandra-room-operator/operator/internal/mgmt"
	"github.com/pbonard/cassandra-room-operator/operator/internal/names"
)

const finalizer = names.Group + "/datacenter"

// Reconciler runs CassandraRoomDatacenters.
type Reconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	// Mgmt returns the management API client of a data center.
	Mgmt func(ctx context.Context, dc *v1.CassandraRoomDatacenter) (mgmt.Client, error)
	// Roles sets up the superuser role through CQL.
	Roles RoleManager
	// AgentImage is the default image of node-init and the backup agent.
	AgentImage string
}

// +kubebuilder:rbac:groups=cassandraroom.pbonard.github.io,resources=cassandraroomdatacenters;cassandraroomdatacenters/status;cassandraroomdatacenters/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods;services;configmaps;secrets;persistentvolumeclaims;serviceaccounts;events,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch;delete;bind;escalate
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete

// SetupWithManager registers the controller.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1.CassandraRoomDatacenter{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Named("cassandraroomdatacenter").
		Complete(r)
}

// node is a pod and what its management API says.
type node struct {
	pod     *corev1.Pod
	rack    string
	ordinal int
	info    *mgmt.NodeInfo // nil when it does not answer
	ready   bool           // pod ready and in the ring
	addr    string         // management API address
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	dc := &v1.CassandraRoomDatacenter{}
	if err := r.Get(ctx, req.NamespacedName, dc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !dc.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, r.finalize(ctx, dc)
	}
	if !controllerutil.ContainsFinalizer(dc, finalizer) {
		controllerutil.AddFinalizer(dc, finalizer)
		if err := r.Update(ctx, dc); err != nil {
			return ctrl.Result{}, err
		}
	}
	agentImage := dc.Spec.AgentImage
	if agentImage == "" {
		agentImage = r.AgentImage
	}
	status := dc.Status.DeepCopy()
	if err := r.ensureSuperuserSecret(ctx, dc); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureCommon(ctx, dc); err != nil {
		return ctrl.Result{}, err
	}
	cl, err := r.Mgmt(ctx, dc)
	if err != nil {
		return ctrl.Result{}, err
	}
	nodes, err := r.observe(ctx, dc, cl)
	if err != nil {
		return ctrl.Result{}, err
	}
	racks := names.Racks(dc)
	if dc.Spec.Networking.Mode == v1.NetworkingLoadBalancer {
		if err := r.ensureLoadBalancers(ctx, dc, racks); err != nil {
			return ctrl.Result{}, err
		}
	}
	seeds, err := r.seeds(ctx, dc, nodes)
	if err != nil {
		return ctrl.Result{}, err
	}
	r.replacements(ctx, dc, status, nodes)
	if err := r.ensureNodeConfig(ctx, dc, seeds, status); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.labelSeeds(ctx, dc, nodes); err != nil {
		return ctrl.Result{}, err
	}
	phase, wait, err := r.reconcileRacks(ctx, dc, racks, nodes, cl, agentImage, status)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.replace(ctx, dc, status, nodes); err != nil {
		return ctrl.Result{}, err
	}
	ready := readyNodes(nodes)
	if ready > 0 && !status.SuperuserCreated && r.Roles != nil {
		if err := r.setupSuperuser(ctx, dc, nodes); err != nil {
			logger.Info("superuser not set up yet", "err", err.Error())
			wait = true
		} else {
			status.SuperuserCreated = true
			r.Recorder.Event(dc, corev1.EventTypeNormal, "SuperuserCreated", "created the superuser role")
		}
	}
	if phase == v1.PhaseReady {
		if err := r.reconcileCMS(ctx, dc, cl, nodes, seeds, status); err != nil {
			logger.Info("metadata service not reconfigured yet", "err", err.Error())
			wait = true
		}
	}
	if dc.Spec.RollingRestartRequestedAt != status.LastRollingRestart && phase == v1.PhaseReady {
		status.LastRollingRestart = dc.Spec.RollingRestartRequestedAt
	}

	// Status.
	status.Phase, status.ReadyNodes, status.ObservedGeneration = phase, ready, dc.Generation
	status.SeedAddresses = seeds
	status.Nodes = nodeStates(nodes, status.Nodes)
	cond := metav1.Condition{Type: v1.ConditionReady, Status: metav1.ConditionFalse, Reason: string(phase), ObservedGeneration: dc.Generation}
	if phase == v1.PhaseReady {
		cond.Status = metav1.ConditionTrue
	}
	meta.SetStatusCondition(&status.Conditions, cond)
	if !equalStatus(&dc.Status, status) {
		dc.Status = *status
		if err := r.Status().Update(ctx, dc); err != nil {
			return ctrl.Result{}, err
		}
	}
	if wait || phase != v1.PhaseReady {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func equalStatus(a, b *v1.CassandraRoomDatacenterStatus) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

// finalize deletes the volumes of a deleted data center.
func (r *Reconciler) finalize(ctx context.Context, dc *v1.CassandraRoomDatacenter) error {
	if !controllerutil.ContainsFinalizer(dc, finalizer) {
		return nil
	}
	if err := r.DeleteAllOf(ctx, &corev1.PersistentVolumeClaim{}, client.InNamespace(dc.Namespace),
		client.MatchingLabels{names.LabelDCObject: dc.Name}); err != nil {
		return err
	}
	controllerutil.RemoveFinalizer(dc, finalizer)
	return r.Update(ctx, dc)
}

// ensureSuperuserSecret creates the superuser secret with a random
// password if it does not exist.
func (r *Reconciler) ensureSuperuserSecret(ctx context.Context, dc *v1.CassandraRoomDatacenter) error {
	name := superuserSecret(dc)
	s := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Namespace: dc.Namespace, Name: name}, s)
	if !apierrors.IsNotFound(err) {
		return err
	}
	s = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: dc.Namespace, Labels: names.Labels(dc)},
		Data: map[string][]byte{"username": []byte(names.Sanitize(dc.Spec.ClusterName) + "-superuser"), "password": []byte(randomPassword())}}
	return r.Create(ctx, s)
}

func superuserSecret(dc *v1.CassandraRoomDatacenter) string {
	if dc.Spec.SuperuserSecret != "" {
		return dc.Spec.SuperuserSecret
	}
	return names.SuperuserSecret(dc.Spec.ClusterName)
}

func randomPassword() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// ensureCommon creates the objects that do not depend on the nodes.
func (r *Reconciler) ensureCommon(ctx context.Context, dc *v1.CassandraRoomDatacenter) error {
	sa, role, rb := nodeRBAC(dc)
	if err := r.apply(ctx, dc, sa, func(existing client.Object) {}); err != nil {
		return err
	}
	if err := r.apply(ctx, dc, role, func(existing client.Object) { existing.(*rbacv1.Role).Rules = role.Rules }); err != nil {
		return err
	}
	if err := r.apply(ctx, dc, rb, func(existing client.Object) {}); err != nil {
		return err
	}
	for _, svc := range []*corev1.Service{headlessService(dc), clientService(dc)} {
		if err := r.apply(ctx, dc, svc, func(existing client.Object) {
			e := existing.(*corev1.Service)
			e.Spec.Ports, e.Spec.Selector, e.Spec.PublishNotReadyAddresses = svc.Spec.Ports, svc.Spec.Selector, svc.Spec.PublishNotReadyAddresses
		}); err != nil {
			return err
		}
	}
	pdb := podDisruptionBudget(dc)
	return r.apply(ctx, dc, pdb, func(existing client.Object) { existing.(*policyv1.PodDisruptionBudget).Spec = pdb.Spec })
}

// apply creates obj, owned by the data center, or updates the existing one
// with update.
func (r *Reconciler) apply(ctx context.Context, dc *v1.CassandraRoomDatacenter, obj client.Object, update func(existing client.Object)) error {
	existing := obj.DeepCopyObject().(client.Object)
	err := r.Get(ctx, client.ObjectKeyFromObject(obj), existing)
	if apierrors.IsNotFound(err) {
		if err := controllerutil.SetControllerReference(dc, obj, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, obj)
	} else if err != nil {
		return err
	}
	before := existing.DeepCopyObject()
	update(existing)
	labels := existing.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	for k, v := range obj.GetLabels() {
		labels[k] = v
	}
	existing.SetLabels(labels)
	if equalObjects(before, existing) {
		return nil
	}
	return r.Update(ctx, existing)
}

func equalObjects(a, b runtime.Object) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

// observe lists the data center's pods and asks each running one about
// its node.
func (r *Reconciler) observe(ctx context.Context, dc *v1.CassandraRoomDatacenter, cl mgmt.Client) ([]*node, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(dc.Namespace), client.MatchingLabels(names.Selector(dc))); err != nil {
		return nil, err
	}
	var out []*node
	for i := range pods.Items {
		p := &pods.Items[i]
		rack := p.Labels[names.LabelRack]
		ord, err := strconv.Atoi(p.Name[strings.LastIndex(p.Name, "-")+1:])
		if err != nil {
			continue
		}
		n := &node{pod: p, rack: rack, ordinal: ord}
		if p.Status.PodIP != "" && p.DeletionTimestamp.IsZero() {
			n.addr = net.JoinHostPort(p.Status.PodIP, strconv.Itoa(int(names.MgmtPort(dc))))
			qctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			if info, err := cl.Node(qctx, n.addr); err == nil {
				n.info = &info
			}
			cancel()
		}
		n.ready = podReady(p) && n.info != nil && n.info.Status == "NORMAL"
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].pod.Name < out[j].pod.Name })
	return out, nil
}

func podReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func readyNodes(nodes []*node) int32 {
	var n int32
	for _, x := range nodes {
		if x.ready {
			n++
		}
	}
	return n
}

func nodeStates(nodes []*node, prev []v1.NodeState) []v1.NodeState {
	known := map[string]v1.NodeState{}
	for _, s := range prev {
		known[s.Pod] = s
	}
	var out []v1.NodeState
	for _, n := range nodes {
		s := v1.NodeState{Pod: n.pod.Name, Rack: n.rack, Ready: n.ready}
		if p, ok := known[n.pod.Name]; ok { // keep what a node no longer answering said
			s.HostID, s.Address, s.Status = p.HostID, p.Address, p.Status
		}
		if n.info != nil {
			s.HostID, s.Address, s.Status = n.info.HostID, n.info.Address, n.info.Status
		}
		out = append(out, s)
	}
	return out
}

// nodeAddress is a pod's internode address: the node's own report, else
// what node-init gives it.
func (r *Reconciler) nodeAddress(ctx context.Context, dc *v1.CassandraRoomDatacenter, n *node) string {
	if n.info != nil && n.info.Address != "" {
		return n.info.Address
	}
	host := n.pod.Status.PodIP
	if dc.Spec.Networking.Mode == v1.NetworkingLoadBalancer {
		host = r.loadBalancerIP(ctx, dc, n.pod.Name)
	}
	if host == "" {
		return ""
	}
	return net.JoinHostPort(host, strconv.Itoa(names.InternodePort))
}

func (r *Reconciler) loadBalancerIP(ctx context.Context, dc *v1.CassandraRoomDatacenter, pod string) string {
	svc := &corev1.Service{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: dc.Namespace, Name: names.LoadBalancer(pod)}, svc); err != nil {
		return ""
	}
	for _, in := range svc.Status.LoadBalancer.Ingress {
		if in.IP != "" {
			return in.IP
		}
		if in.Hostname != "" {
			if ips, err := net.LookupHost(in.Hostname); err == nil && len(ips) > 0 {
				return ips[0]
			}
		}
	}
	return ""
}

// isSeed reports whether a pod is a seed: the first pod of each of the
// first racks (at most three).
func isSeed(dc *v1.CassandraRoomDatacenter, rack string, ordinal int) bool {
	if ordinal != 0 {
		return false
	}
	for i, r := range names.Racks(dc) {
		if i >= names.MaxSeedsPer {
			break
		}
		if names.Sanitize(r.Name) == rack {
			return true
		}
	}
	return false
}

// seeds are the internode addresses of the data center's seeds that
// have one.
func (r *Reconciler) seeds(ctx context.Context, dc *v1.CassandraRoomDatacenter, nodes []*node) ([]string, error) {
	var out []string
	for _, n := range nodes {
		if isSeed(dc, n.rack, n.ordinal) {
			if a := r.nodeAddress(ctx, dc, n); a != "" {
				out = append(out, a)
			}
		}
	}
	return out, nil
}

func (r *Reconciler) labelSeeds(ctx context.Context, dc *v1.CassandraRoomDatacenter, nodes []*node) error {
	for _, n := range nodes {
		want := isSeed(dc, n.rack, n.ordinal)
		if (n.pod.Labels[names.LabelSeed] == "true") == want {
			continue
		}
		patch := client.MergeFrom(n.pod.DeepCopy())
		if want {
			n.pod.Labels[names.LabelSeed] = "true"
		} else {
			delete(n.pod.Labels, names.LabelSeed)
		}
		if err := r.Patch(ctx, n.pod, patch); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// ensureNodeConfig writes what node-init reads: the seeds, which pod may
// start the cluster alone, and the nodes pods replace.
func (r *Reconciler) ensureNodeConfig(ctx context.Context, dc *v1.CassandraRoomDatacenter, seeds []string, status *v1.CassandraRoomDatacenterStatus) error {
	all := append(slices.Clone(dc.Spec.AdditionalSeeds), seeds...)
	data := map[string]string{"seeds": strings.Join(all, ",")}
	if len(dc.Spec.AdditionalSeeds) == 0 {
		// The first pod of the first rack starts the cluster when there is
		// no other seed.
		data["bootstrap-seed"] = names.Pod(dc, names.Racks(dc)[0].Name, 0)
	}
	for pod, host := range status.ReplacedNodes {
		if !strings.HasPrefix(host, "done:") {
			data["replace."+pod] = host
		}
	}
	cm := &corev1.ConfigMap{ObjectMeta: objMeta(dc, names.NodeConfig(dc)), Data: data}
	return r.apply(ctx, dc, cm, func(existing client.Object) { existing.(*corev1.ConfigMap).Data = data })
}

func (r *Reconciler) ensureLoadBalancers(ctx context.Context, dc *v1.CassandraRoomDatacenter, racks []v1.Rack) error {
	sizes := names.RackSizes(dc.Spec.Size, len(racks))
	for i, rack := range racks {
		for o := 0; o < int(sizes[i]); o++ {
			svc := loadBalancerService(dc, names.Pod(dc, rack.Name, o))
			if err := r.apply(ctx, dc, svc, func(existing client.Object) {
				e := existing.(*corev1.Service)
				e.Spec.Ports, e.Spec.Selector, e.Annotations = svc.Spec.Ports, svc.Spec.Selector, svc.Annotations
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// reconcileRacks scales and updates the racks' StatefulSets: one node joins
// or leaves at a time, and changes roll out one rack at a time.
func (r *Reconciler) reconcileRacks(ctx context.Context, dc *v1.CassandraRoomDatacenter, racks []v1.Rack, nodes []*node,
	cl mgmt.Client, agentImage string, status *v1.CassandraRoomDatacenterStatus) (v1.DatacenterPhase, bool, error) {
	size := dc.Spec.Size
	if dc.Spec.Stopped {
		size = 0
	}
	desired := names.RackSizes(size, len(racks))
	sets := make([]*appsv1.StatefulSet, len(racks))
	current := make([]int32, len(racks))
	for i, rack := range racks {
		s := &appsv1.StatefulSet{}
		err := r.Get(ctx, types.NamespacedName{Namespace: dc.Namespace, Name: names.StatefulSet(dc, rack.Name)}, s)
		if apierrors.IsNotFound(err) {
			s = nil
		} else if err != nil {
			return "", false, err
		} else if s.Spec.Replicas != nil {
			current[i] = *s.Spec.Replicas
		}
		sets[i] = s
	}
	allReady := true
	for _, n := range nodes {
		if !n.ready && !dc.Spec.Stopped {
			allReady = false
		}
	}
	var total, want int32
	for i := range racks {
		total += current[i]
		want += desired[i]
	}
	target := slices.Clone(current)
	phase := v1.PhaseReady
	wait := false
	switch {
	case dc.Spec.Stopped:
		for i := range target {
			target[i] = 0
		}
		phase = v1.PhaseStopped
	case total < want:
		phase = v1.PhaseScaling
		if total == 0 && status.Phase == "" {
			phase = v1.PhaseInitializing
		}
		// Nodes that had joined already (their volume exists) restart at
		// once; a new one joins once every node is ready.
		for i, rack := range racks {
			for target[i] < desired[i] && r.pvcExists(ctx, dc, names.Pod(dc, rack.Name, int(target[i]))) {
				target[i]++
			}
		}
		if slices.Equal(target, current) && allReady && int32(len(nodes)) == total {
			best := -1
			for i := range racks {
				if target[i] < desired[i] && (best < 0 || target[i] < target[best]) {
					best = i
				}
			}
			target[best]++
		}
	case total > want:
		phase = v1.PhaseScaling
		done, err := r.scaleDown(ctx, dc, racks, current, desired, nodes, cl, allReady)
		if err != nil {
			return "", false, err
		}
		if done >= 0 {
			target[done]--
		}
	}
	// Roll changes out rack by rack: a rack's template changes once the
	// racks before it run theirs.
	rolledOut := true
	for i, rack := range racks {
		want := statefulSet(dc, rack, target[i], agentImage)
		hash := templateHash(want)
		want.Annotations = map[string]string{names.AnnotationConfigHash: hash}
		if sets[i] == nil {
			if err := controllerutil.SetControllerReference(dc, want, r.Scheme); err != nil {
				return "", false, err
			}
			if err := r.Create(ctx, want); err != nil {
				return "", false, err
			}
			rolledOut = false
			continue
		}
		s := sets[i]
		changed := s.Annotations[names.AnnotationConfigHash] != hash
		if changed && rolledOut {
			s.Spec.Template, s.Spec.UpdateStrategy = want.Spec.Template, want.Spec.UpdateStrategy
			if s.Annotations == nil {
				s.Annotations = map[string]string{}
			}
			s.Annotations[names.AnnotationConfigHash] = hash
			if phase == v1.PhaseReady {
				phase = v1.PhaseUpdating
			}
		}
		if *s.Spec.Replicas != target[i] || changed && rolledOut {
			s.Spec.Replicas = &target[i]
			if err := r.Update(ctx, s); err != nil {
				return "", false, err
			}
		}
		if changed || !rackRolledOut(s) {
			rolledOut = false
			if phase == v1.PhaseReady {
				phase = v1.PhaseUpdating
			}
		}
	}
	if phase == v1.PhaseReady && !allReady {
		phase = v1.PhaseUpdating
	}
	if phase == v1.PhaseReady && int32(len(nodes)) != want {
		phase, wait = v1.PhaseScaling, true
	}
	return phase, wait, nil
}

// rackRolledOut reports whether a StatefulSet's pods all run its template
// and are ready.
func rackRolledOut(s *appsv1.StatefulSet) bool {
	n := *s.Spec.Replicas
	st := s.Status
	return st.ObservedGeneration >= s.Generation && st.ReadyReplicas == n && st.UpdatedReplicas == n &&
		(st.UpdateRevision == "" || st.CurrentRevision == st.UpdateRevision)
}

func templateHash(s *appsv1.StatefulSet) string {
	b, _ := json.Marshal(struct {
		T any
		V any
		U any
	}{s.Spec.Template, s.Spec.VolumeClaimTemplates[0].Spec.Resources, s.Spec.UpdateStrategy})
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:8])
}

func (r *Reconciler) pvcExists(ctx context.Context, dc *v1.CassandraRoomDatacenter, pod string) bool {
	err := r.Get(ctx, types.NamespacedName{Namespace: dc.Namespace, Name: names.PVC(pod)}, &corev1.PersistentVolumeClaim{})
	return err == nil
}

// scaleDown decommissions the last node of the fullest rack; once it left
// the ring, its pod and volume go (it returns the rack to shrink, or -1).
func (r *Reconciler) scaleDown(ctx context.Context, dc *v1.CassandraRoomDatacenter, racks []v1.Rack, current, desired []int32,
	nodes []*node, cl mgmt.Client, allReady bool) (int, error) {
	rack := -1
	for i := range racks {
		if current[i] > desired[i] && (rack < 0 || current[i] >= current[rack]) {
			rack = i
		}
	}
	if rack < 0 {
		return -1, nil
	}
	pod := names.Pod(dc, racks[rack].Name, int(current[rack]-1))
	var n *node
	for _, x := range nodes {
		if x.pod.Name == pod {
			n = x
		}
	}
	switch {
	case n == nil || n.info != nil && n.info.Status == "LEFT":
		// Gone from the ring (or never started): remove the pod and volume.
		pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: dc.Namespace, Name: names.PVC(pod)}}
		if err := r.Delete(ctx, pvc); err != nil && !apierrors.IsNotFound(err) {
			return -1, err
		}
		r.Recorder.Eventf(dc, corev1.EventTypeNormal, "NodeRemoved", "%s left the ring; removing its pod and volume", pod)
		return rack, nil
	case n.info == nil:
		return -1, nil // not answering: wait
	case n.info.Status == "NORMAL" && allReady:
		if err := cl.Decommission(ctx, n.addr); err != nil {
			return -1, err
		}
		r.Recorder.Eventf(dc, corev1.EventTypeNormal, "Decommissioning", "decommissioning %s", pod)
	}
	return -1, nil
}

// replacements records the host id of the nodes pods are to replace.
func (r *Reconciler) replacements(ctx context.Context, dc *v1.CassandraRoomDatacenter, status *v1.CassandraRoomDatacenterStatus, nodes []*node) {
	for _, pod := range dc.Spec.ReplaceNodes {
		if _, ok := status.ReplacedNodes[pod]; ok {
			continue
		}
		var host string
		for _, s := range status.Nodes {
			if s.Pod == pod {
				host = s.HostID
			}
		}
		if host == "" {
			r.Recorder.Eventf(dc, corev1.EventTypeWarning, "ReplaceUnknown", "cannot replace %s: its node's host id is unknown", pod)
			continue
		}
		if status.ReplacedNodes == nil {
			status.ReplacedNodes = map[string]string{}
		}
		status.ReplacedNodes[pod] = host
	}
	// Done once the pod's new node is in the ring.
	for pod, host := range status.ReplacedNodes {
		if strings.HasPrefix(host, "done:") {
			if !slices.Contains(dc.Spec.ReplaceNodes, pod) {
				delete(status.ReplacedNodes, pod) // forgotten once no longer asked
			}
			continue
		}
		for _, n := range nodes {
			if n.pod.Name == pod && n.ready && n.info.HostID != host {
				status.ReplacedNodes[pod] = "done:" + host
			}
		}
	}
}

// replace recreates the pods of nodes to replace with empty volumes; their
// node-init then starts them replacing the dead node.
func (r *Reconciler) replace(ctx context.Context, dc *v1.CassandraRoomDatacenter, status *v1.CassandraRoomDatacenterStatus, nodes []*node) error {
	for pod, host := range status.ReplacedNodes {
		if strings.HasPrefix(host, "done:") {
			continue
		}
		var n *node
		for _, x := range nodes {
			if x.pod.Name == pod {
				n = x
			}
		}
		if n == nil || n.info != nil && n.info.HostID != host || !n.pod.DeletionTimestamp.IsZero() {
			continue // recreated already (or being)
		}
		if n.info != nil && n.ready {
			continue // the node is alive after all
		}
		// The replacement must start empty: the volume goes with the pod.
		pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: dc.Namespace, Name: names.PVC(pod)}}
		if err := r.Delete(ctx, pvc); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		if err := r.Delete(ctx, n.pod); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		r.Recorder.Eventf(dc, corev1.EventTypeNormal, "ReplacingNode", "recreating %s to replace node %s", pod, host)
	}
	return nil
}

// setupSuperuser creates the superuser role from its secret.
func (r *Reconciler) setupSuperuser(ctx context.Context, dc *v1.CassandraRoomDatacenter, nodes []*node) error {
	s := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: dc.Namespace, Name: superuserSecret(dc)}, s); err != nil {
		return err
	}
	var hosts []string
	for _, n := range nodes {
		if n.ready {
			hosts = append(hosts, n.pod.Status.PodIP)
		}
	}
	tlsCfg, err := clientTLS(ctx, r.Client, dc)
	if err != nil {
		return err
	}
	return r.Roles.EnsureSuperuser(ctx, Conn{Hosts: hosts, Port: names.NativePort, TLS: tlsCfg},
		string(s.Data["username"]), string(s.Data["password"]))
}

// reconcileCMS keeps the metadata service's members as wanted: those the
// cluster chose (spec.cmsMembers), or with manageCMS three of this data
// center's nodes, seeds first.
func (r *Reconciler) reconcileCMS(ctx context.Context, dc *v1.CassandraRoomDatacenter, cl mgmt.Client, nodes []*node,
	seeds []string, status *v1.CassandraRoomDatacenterStatus) error {
	var any *node
	for _, n := range nodes {
		if n.ready {
			any = n
			break
		}
	}
	if any == nil {
		return nil
	}
	all, err := cl.Nodes(ctx, any.addr)
	if err != nil {
		return err
	}
	var current []string
	up := map[string]bool{}
	for _, n := range all {
		if n.CMS {
			current = append(current, n.Address)
		}
		up[n.Address] = n.Up && n.Status == "NORMAL"
	}
	sort.Strings(current)
	status.CMSMembers = current
	want := slices.Clone(dc.Spec.CMSMembers)
	if len(want) == 0 && dc.Spec.ManageCMS {
		want = slices.Clone(seeds)
		for _, n := range nodes {
			if len(want) >= 3 {
				break
			}
			if a := r.nodeAddress(ctx, dc, n); n.ready && a != "" && !slices.Contains(want, a) {
				want = append(want, a)
			}
		}
	}
	if len(want) == 0 {
		return nil
	}
	sort.Strings(want)
	if slices.Equal(want, current) {
		return nil
	}
	for _, m := range want {
		if !up[m] {
			return fmt.Errorf("metadata service member %s is not up", m)
		}
	}
	if err := cl.SetCMS(ctx, any.addr, want); err != nil {
		return err
	}
	r.Recorder.Eventf(dc, corev1.EventTypeNormal, "MetadataServiceReconfigured", "metadata service members: %s", strings.Join(want, ", "))
	status.CMSMembers = want
	return nil
}
