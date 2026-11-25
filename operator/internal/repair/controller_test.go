package repair_test

import (
	"context"
	"errors"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	v1 "github.com/pbonard/cassandra-room-operator/operator/api/v1alpha1"
	"github.com/pbonard/cassandra-room-operator/operator/internal/mgmt"
	"github.com/pbonard/cassandra-room-operator/operator/internal/repair"
	"github.com/pbonard/cassandra-room-operator/operator/internal/testenv"
)

func TestSplitRange(t *testing.T) {
	// The whole ring (a wrapping range) in four.
	parts := repair.SplitRange(math.MinInt64, math.MinInt64, 4)
	require.Len(t, parts, 4)
	require.Equal(t, int64(math.MinInt64), parts[0][0])
	require.Equal(t, int64(math.MinInt64), parts[3][1])
	for i := 1; i < 4; i++ {
		require.Equal(t, parts[i-1][1], parts[i][0], "contiguous")
	}
	require.Equal(t, int64(0), parts[2][0], "evenly")
	// A wrapping range.
	parts = repair.SplitRange(math.MaxInt64-10, math.MinInt64+9, 2)
	require.Equal(t, [][2]int64{{math.MaxInt64 - 10, math.MaxInt64}, {math.MaxInt64, math.MinInt64 + 9}}, parts)
	// Narrower than asked.
	require.Len(t, repair.SplitRange(10, 12, 5), 2)
}

func TestRepairSchedule(t *testing.T) {
	env, s := testenv.StartEnv(t)
	f := testenv.NewFakeCluster()
	c := testenv.StartManager(t, env, s, f, func(mgr ctrl.Manager) error {
		return (&repair.Reconciler{Client: mgr.GetClient(), Scheme: s, Recorder: mgr.GetEventRecorderFor("repair"),
			Mgmt: func(context.Context, *v1.CassandraRoomDatacenter) (mgmt.Client, error) { return f, nil }}).SetupWithManager(mgr)
	})
	ctx := context.Background()
	ns := "repair"
	testenv.Namespace(t, c, ns)
	require.NoError(t, c.Create(ctx, &v1.CassandraRoomDatacenter{ObjectMeta: metav1.ObjectMeta{Name: "dc1", Namespace: ns},
		Spec: v1.CassandraRoomDatacenterSpec{ClusterName: "c1", Image: "x", Size: 3}}))
	rs := &v1.RepairSchedule{ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: ns}, Spec: v1.RepairScheduleSpec{
		Datacenter: "dc1", RepairScheduleSettings: v1.RepairScheduleSettings{
			Schedule: "0 3 * * *", SegmentsPerNode: 2, Intensity: "0.9", RunRequestedAt: "first"}}}
	require.NoError(t, c.Create(ctx, rs))
	key := types.NamespacedName{Namespace: ns, Name: "nightly"}
	get := func() *v1.RepairSchedule {
		x := &v1.RepairSchedule{}
		require.NoError(t, c.Get(ctx, key, x))
		return x
	}
	// A run of both keyspaces (every user keyspace): 3 ranges × 2 per
	// node... = 6 segments each.
	require.Eventually(t, func() bool {
		x := get()
		return len(x.Status.History) == 1 && x.Status.Current == nil
	}, 60*time.Second, 100*time.Millisecond)
	run := get().Status.History[0]
	require.Equal(t, v1.RepairDone, run.State)
	require.Equal(t, int32(12), run.Segments)
	require.Equal(t, int32(12), run.Done)
	require.Equal(t, int64(12), run.Mismatches)
	done := f.RepairsDone()
	require.Len(t, done, 12)
	// The segments of a keyspace cover the ring exactly.
	byKs := map[string][][2]int64{}
	for _, r := range done {
		var st, en int64
		_, _ = fmtSscan(r.Start, &st), fmtSscan(r.End, &en)
		byKs[r.Keyspace] = append(byKs[r.Keyspace], [2]int64{st, en})
	}
	require.Len(t, byKs, 2)
	for ks, segs := range byKs {
		require.True(t, coversRing(segs), "%s: %v", ks, segs)
	}
	next := get().Status.NextRunAt
	require.NotNil(t, next)
	require.Equal(t, 3, next.UTC().Hour())
	err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: "nightly-plan"}, &corev1.ConfigMap{})
	require.Error(t, err, "the plan is deleted with the run")

	// A failing run: segments retried, then given up.
	f.SetRepairError(errors.New("stream failed"))
	x := get()
	x.Spec.RunRequestedAt, x.Spec.Keyspaces = "second", []string{"ks1"}
	one := int32(2)
	x.Spec.MaxRetries = &one
	require.NoError(t, c.Update(ctx, x))
	require.Eventually(t, func() bool { return len(get().Status.History) == 2 }, 60*time.Second, 100*time.Millisecond)
	run = get().Status.History[0]
	require.Equal(t, v1.RepairErrored, run.State)
	require.Equal(t, int32(6), run.Failed)
	require.Len(t, f.RepairsDone(), 12, "no more repairs succeeded")
}

func fmtSscan(s string, v *int64) error {
	var err error
	*v, err = strconv.ParseInt(s, 10, 64)
	return err
}

// coversRing reports whether ranges (start, end] tile the ring.
func coversRing(segs [][2]int64) bool {
	next := map[int64]int64{}
	for _, s := range segs {
		if _, dup := next[s[0]]; dup {
			return false
		}
		next[s[0]] = s[1]
	}
	// Walk from any start; it must come back after visiting every segment.
	start := segs[0][0]
	cur, n := start, 0
	for {
		e, ok := next[cur]
		if !ok {
			return false
		}
		n++
		cur = e
		if cur == start {
			return n == len(segs)
		}
		if n > len(segs) {
			return false
		}
	}
}
