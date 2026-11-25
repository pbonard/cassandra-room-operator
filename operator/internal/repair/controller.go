// Package repair runs RepairSchedules (Cassandra Reaper's job): each run
// splits every keyspace's ring into segments and repairs them through the
// nodes' management API, a few at a time on disjoint replicas, pausing
// between segments as the intensity asks, retrying failures, and resuming
// where it stopped after a pause or an operator restart.
package repair

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
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
	"github.com/pbonard/cassandra-room-operator/operator/internal/dc"
	"github.com/pbonard/cassandra-room-operator/operator/internal/mgmt"
	"github.com/pbonard/cassandra-room-operator/operator/internal/names"
)

// Segment is a range of a keyspace to repair, and its progress.
type Segment struct {
	Keyspace string   `json:"keyspace"`
	Start    int64    `json:"start,string"`
	End      int64    `json:"end,string"`
	Replicas []string `json:"replicas"`
	// State is "", "done" or "failed".
	State    string `json:"state,omitempty"`
	Attempts int32  `json:"attempts,omitempty"`
	Error    string `json:"error,omitempty"`
}

// plan is a run's segments, kept in a ConfigMap.
type plan struct {
	Run      string    `json:"run"`
	Segments []Segment `json:"segments"`
}

// result is a segment's repair outcome, from its worker.
type result struct {
	index    int
	stats    mgmt.RepairStats
	err      error
	duration time.Duration
}

// runState is a run's in-memory state: its segments in progress.
type runState struct {
	running   map[int]bool
	results   []result
	notBefore time.Time // the intensity's pause
}

// Reconciler runs RepairSchedules.
type Reconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	Mgmt     func(ctx context.Context, d *v1.CassandraRoomDatacenter) (mgmt.Client, error)
	Now      func() time.Time

	mu   sync.Mutex
	runs map[string]*runState // by namespace/name/run
}

// +kubebuilder:rbac:groups=cassandraroom.pbonard.github.io,resources=repairschedules;repairschedules/status,verbs=get;list;watch;create;update;patch;delete

// SetupWithManager registers the controller.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Now == nil {
		r.Now = time.Now
	}
	return ctrl.NewControllerManagedBy(mgr).For(&v1.RepairSchedule{}).Named("repairschedule").Complete(r)
}

var parser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	rs := &v1.RepairSchedule{}
	if err := r.Get(ctx, req.NamespacedName, rs); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	now := r.Now()
	status := rs.Status.DeepCopy()
	sched, err := parser.Parse(orDefault(rs.Spec.Schedule, "@weekly"))
	if err != nil {
		meta.SetStatusCondition(&status.Conditions, metav1.Condition{Type: v1.ConditionFailed, Status: metav1.ConditionTrue,
			Reason: "BadSchedule", Message: err.Error()})
		return ctrl.Result{}, r.saveStatus(ctx, rs, status)
	}
	meta.RemoveStatusCondition(&status.Conditions, v1.ConditionFailed)
	d := &v1.CassandraRoomDatacenter{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: rs.Namespace, Name: rs.Spec.Datacenter}, d); err != nil {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, client.IgnoreNotFound(err)
	}
	var requeue time.Duration
	if status.Current == nil {
		requested := rs.Spec.RunRequestedAt != "" && rs.Spec.RunRequestedAt != status.LastRunRequest
		if status.NextRunAt == nil {
			status.NextRunAt = &metav1.Time{Time: sched.Next(now)}
		}
		due := !now.Before(status.NextRunAt.Time)
		switch {
		case (requested || due) && d.Status.Phase == v1.PhaseReady:
			status.LastRunRequest = rs.Spec.RunRequestedAt
			if err := r.start(ctx, rs, d, status, now); err != nil {
				return ctrl.Result{}, err
			}
			requeue = time.Second
		case requested || due:
			requeue = 30 * time.Second // the data center is not ready
		default:
			requeue = status.NextRunAt.Sub(now)
		}
	} else {
		requeue, err = r.step(ctx, rs, d, status, sched, now)
		if err != nil {
			return ctrl.Result{}, err
		}
	}
	if err := r.saveStatus(ctx, rs, status); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: max(requeue, 100*time.Millisecond)}, nil
}

func (r *Reconciler) saveStatus(ctx context.Context, rs *v1.RepairSchedule, status *v1.RepairScheduleStatus) error {
	a, _ := json.Marshal(rs.Status)
	b, _ := json.Marshal(status)
	if string(a) == string(b) {
		return nil
	}
	rs.Status = *status
	return r.Status().Update(ctx, rs)
}

func orDefault(s, d string) string {
	if s == "" {
		return d
	}
	return s
}

// start plans a run: the keyspaces' ranges, each split so that the ring
// has about segmentsPerNode segments per node.
func (r *Reconciler) start(ctx context.Context, rs *v1.RepairSchedule, d *v1.CassandraRoomDatacenter, status *v1.RepairScheduleStatus, now time.Time) error {
	cl, err := r.Mgmt(ctx, d)
	if err != nil {
		return err
	}
	eps, err := dc.Endpoints(ctx, r.Client, d)
	if err != nil {
		return err
	}
	var coordinator string
	for _, e := range eps {
		if e.Ready {
			coordinator = e.Management
			break
		}
	}
	if coordinator == "" {
		return fmt.Errorf("no ready node in %s", d.Name)
	}
	keyspaces := rs.Spec.Keyspaces
	if len(keyspaces) == 0 {
		if keyspaces, err = cl.Keyspaces(ctx, coordinator); err != nil {
			return err
		}
	}
	perNode := int(rs.Spec.SegmentsPerNode)
	if perNode <= 0 {
		perNode = 16
	}
	p := plan{Run: now.UTC().Format("20060102-150405")}
	for _, ks := range keyspaces {
		ring, err := cl.Ring(ctx, coordinator, ks)
		if err != nil {
			return fmt.Errorf("ring of %s: %w", ks, err)
		}
		nodes := map[string]bool{}
		for _, rg := range ring {
			for _, n := range rg.Replicas {
				nodes[n] = true
			}
		}
		split := max(1, (perNode*len(nodes)+len(ring)-1)/max(len(ring), 1))
		for _, rg := range ring {
			for _, sub := range SplitRange(rg.Start, rg.End, split) {
				p.Segments = append(p.Segments, Segment{Keyspace: ks, Start: sub[0], End: sub[1], Replicas: rg.Replicas})
			}
		}
	}
	if err := r.savePlan(ctx, rs, &p); err != nil {
		return err
	}
	status.Current = &v1.RepairRun{ID: p.Run, State: v1.RepairRunning, StartedAt: metav1.Time{Time: now}, Segments: int32(len(p.Segments))}
	r.Recorder.Eventf(rs, corev1.EventTypeNormal, "RepairStarted", "run %s: %d segments of %v", p.Run, len(p.Segments), keyspaces)
	return nil
}

// SplitRange splits (start, end] into n contiguous ranges (end <= start
// wraps around the ring).
func SplitRange(start, end int64, n int) [][2]int64 {
	width := new(big.Int).Sub(big.NewInt(end), big.NewInt(start))
	if width.Sign() <= 0 {
		width.Add(width, new(big.Int).Lsh(big.NewInt(1), 64))
	}
	if big.NewInt(int64(n)).Cmp(width) > 0 {
		n = int(width.Int64())
	}
	out := make([][2]int64, 0, n)
	prev := start
	for i := 1; i <= n; i++ {
		var next int64
		if i == n {
			next = end
		} else {
			off := new(big.Int).Div(new(big.Int).Mul(width, big.NewInt(int64(i))), big.NewInt(int64(n)))
			v := new(big.Int).Add(big.NewInt(start), off)
			next = wrap(v)
		}
		out = append(out, [2]int64{prev, next})
		prev = next
	}
	return out
}

// wrap brings a token computed past the end of the ring back into it.
func wrap(v *big.Int) int64 {
	m := new(big.Int).Lsh(big.NewInt(1), 64)
	min := new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 63))
	v = new(big.Int).Sub(v, min)
	v.Mod(v, m)
	v.Add(v, min)
	return v.Int64()
}

func (r *Reconciler) savePlan(ctx context.Context, rs *v1.RepairSchedule, p *plan) error {
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	cm := &corev1.ConfigMap{}
	key := types.NamespacedName{Namespace: rs.Namespace, Name: names.RepairPlan(rs.Name)}
	err = r.Get(ctx, key, cm)
	if apierrors.IsNotFound(err) {
		cm = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}, Data: map[string]string{"plan": string(data)}}
		if err := controllerutil.SetControllerReference(rs, cm, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, cm)
	} else if err != nil {
		return err
	}
	cm.Data = map[string]string{"plan": string(data)}
	return r.Update(ctx, cm)
}

func (r *Reconciler) loadPlan(ctx context.Context, rs *v1.RepairSchedule) (*plan, error) {
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: rs.Namespace, Name: names.RepairPlan(rs.Name)}, cm); err != nil {
		return nil, err
	}
	p := &plan{}
	return p, json.Unmarshal([]byte(cm.Data["plan"]), p)
}

// step folds finished segments into the plan, starts segments as
// parallelism and intensity allow, and finishes the run when none is left.
func (r *Reconciler) step(ctx context.Context, rs *v1.RepairSchedule, d *v1.CassandraRoomDatacenter, status *v1.RepairScheduleStatus,
	sched cron.Schedule, now time.Time) (time.Duration, error) {
	logger := log.FromContext(ctx)
	cur := status.Current
	p, err := r.loadPlan(ctx, rs)
	if apierrors.IsNotFound(err) || err == nil && p.Run != cur.ID {
		// The plan is gone: the run cannot go on.
		r.finish(rs, status, sched, now, v1.RepairAborted, "the run's plan was lost")
		return time.Second, nil
	} else if err != nil {
		return 0, err
	}
	key := rs.Namespace + "/" + rs.Name + "/" + cur.ID
	r.mu.Lock()
	if r.runs == nil {
		r.runs = map[string]*runState{}
	}
	st := r.runs[key]
	if st == nil {
		st = &runState{running: map[int]bool{}}
		r.runs[key] = st
	}
	results := st.results
	st.results = nil
	r.mu.Unlock()

	maxRetries := int32(3)
	if rs.Spec.MaxRetries != nil {
		maxRetries = *rs.Spec.MaxRetries
	}
	intensity := 1.0
	if rs.Spec.Intensity != "" {
		if v, err := strconv.ParseFloat(rs.Spec.Intensity, 64); err == nil && v > 0 && v <= 1 {
			intensity = v
		}
	}
	changed := false
	for _, res := range results {
		s := &p.Segments[res.index]
		s.Attempts++
		changed = true
		if res.err == nil {
			s.State, s.Error = "done", ""
			cur.Done++
			cur.Mismatches += int64(res.stats.Mismatches)
			cur.StreamedBytes += res.stats.Streamed
		} else {
			s.Error = res.err.Error()
			logger.Info("segment failed", "keyspace", s.Keyspace, "start", s.Start, "end", s.End, "err", s.Error)
			if s.Attempts >= maxRetries {
				s.State = "failed"
				cur.Failed++
			}
		}
		pause := time.Duration(float64(res.duration) * (1/intensity - 1))
		r.mu.Lock()
		st.notBefore = maxTime(st.notBefore, now.Add(pause))
		r.mu.Unlock()
	}
	if changed {
		if err := r.savePlan(ctx, rs, p); err != nil {
			return 0, err
		}
	}
	r.mu.Lock()
	running := len(st.running)
	r.mu.Unlock()
	if cur.Done+cur.Failed == cur.Segments && running == 0 {
		state := v1.RepairDone
		if cur.Failed > 0 {
			state = v1.RepairErrored
		}
		r.finish(rs, status, sched, now, state, "")
		r.mu.Lock()
		delete(r.runs, key)
		r.mu.Unlock()
		_ = r.Delete(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: rs.Namespace, Name: names.RepairPlan(rs.Name)}})
		return time.Second, nil
	}
	if rs.Spec.Paused {
		return 10 * time.Second, nil
	}
	r.mu.Lock()
	wait := st.notBefore.Sub(now)
	r.mu.Unlock()
	if wait > 0 {
		return wait, nil
	}
	return time.Second, r.launch(ctx, rs, d, p, st, key)
}

// launch starts segments while parallelism allows, each on replicas no
// running segment uses, coordinated by a ready replica of the data center.
func (r *Reconciler) launch(ctx context.Context, rs *v1.RepairSchedule, d *v1.CassandraRoomDatacenter, p *plan, st *runState, key string) error {
	parallelism := int(max(rs.Spec.Parallelism, 1))
	cl, err := r.Mgmt(ctx, d)
	if err != nil {
		return err
	}
	eps, err := dc.Endpoints(ctx, r.Client, d)
	if err != nil {
		return err
	}
	byAddr := map[string]dc.Endpoint{}
	for _, e := range eps {
		if e.Ready && e.Address != "" {
			byAddr[e.Address] = e
		}
	}
	timeout := 30 * time.Minute
	if rs.Spec.SegmentTimeout != nil {
		timeout = rs.Spec.SegmentTimeout.Duration
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	busy := map[string]bool{}
	for i := range st.running {
		for _, n := range p.Segments[i].Replicas {
			busy[n] = true
		}
	}
	for i := range p.Segments {
		if len(st.running) >= parallelism {
			break
		}
		s := p.Segments[i]
		if s.State != "" || st.running[i] || slices.ContainsFunc(s.Replicas, func(n string) bool { return busy[n] }) {
			continue
		}
		var coord *dc.Endpoint
		for _, n := range s.Replicas {
			if e, ok := byAddr[n]; ok {
				coord = &e
				break
			}
		}
		if coord == nil {
			continue // no replica of this data center ready
		}
		st.running[i] = true
		for _, n := range s.Replicas {
			busy[n] = true
		}
		go func(i int, s Segment, addr string) {
			sctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			began := time.Now()
			stats, err := cl.Repair(sctx, addr, mgmt.RepairRequest{Keyspace: s.Keyspace, Start: mgmt.Token(s.Start), End: mgmt.Token(s.End)})
			r.mu.Lock()
			defer r.mu.Unlock()
			if cur := r.runs[key]; cur == st {
				delete(st.running, i)
				st.results = append(st.results, result{index: i, stats: stats, err: err, duration: time.Since(began)})
			}
		}(i, s, coord.Management)
	}
	return nil
}

// finish ends the run and schedules the next.
func (r *Reconciler) finish(rs *v1.RepairSchedule, status *v1.RepairScheduleStatus, sched cron.Schedule, now time.Time, state v1.RepairRunState, msg string) {
	run := *status.Current
	run.State, run.Message = state, msg
	run.FinishedAt = &metav1.Time{Time: now}
	keep := int(rs.Spec.History)
	if keep <= 0 {
		keep = 10
	}
	status.History = append([]v1.RepairRun{run}, status.History...)
	if len(status.History) > keep {
		status.History = status.History[:keep]
	}
	status.Current = nil
	status.NextRunAt = &metav1.Time{Time: sched.Next(now)}
	kind := corev1.EventTypeNormal
	if state != v1.RepairDone {
		kind = corev1.EventTypeWarning
	}
	r.Recorder.Eventf(rs, kind, "Repair"+string(state), "run %s: %d of %d segments repaired, %d failed", run.ID, run.Done, run.Segments, run.Failed)
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
