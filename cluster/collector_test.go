package cluster

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/beholdrapp/stalkr/k8s"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakeSource implements Source with canned data and injectable per-call
// errors, so collect() failure/recovery paths can be exercised without a
// real cluster.
type fakeSource struct {
	nodes        []corev1.Node
	pods         []corev1.Pod
	deployments  []appsv1.Deployment
	statefulSets []appsv1.StatefulSet
	daemonSets   []appsv1.DaemonSet
	hpas         []autoscalingv1.HorizontalPodAutoscaler
	nodeUsage    map[string]k8s.Usage
	podUsage     map[string]k8s.Usage

	nodesErr       error
	podsErr        error
	depsErr        error
	nodeMetricsErr error
	podMetricsErr  error
}

func (f *fakeSource) Nodes(context.Context) ([]corev1.Node, error) { return f.nodes, f.nodesErr }
func (f *fakeSource) Pods(context.Context) ([]corev1.Pod, error)   { return f.pods, f.podsErr }
func (f *fakeSource) Deployments(context.Context) ([]appsv1.Deployment, error) {
	return f.deployments, f.depsErr
}
func (f *fakeSource) StatefulSets(context.Context) ([]appsv1.StatefulSet, error) {
	return f.statefulSets, nil
}
func (f *fakeSource) DaemonSets(context.Context) ([]appsv1.DaemonSet, error) {
	return f.daemonSets, nil
}
func (f *fakeSource) HPAs(context.Context) ([]autoscalingv1.HorizontalPodAutoscaler, error) {
	return f.hpas, nil
}
func (f *fakeSource) NodeMetrics(context.Context) (map[string]k8s.Usage, error) {
	if f.nodeMetricsErr != nil {
		return nil, f.nodeMetricsErr
	}
	return f.nodeUsage, nil
}

func (f *fakeSource) PodMetrics(context.Context) (map[string]k8s.Usage, error) {
	if f.podMetricsErr != nil {
		return nil, f.podMetricsErr
	}
	return f.podUsage, nil
}

func qty(s string) resource.Quantity { return resource.MustParse(s) }

func int32Ptr(v int32) *int32 { return &v }

// oneNodeOnePodFixture is a minimal, internally-consistent cluster: one ready
// node running one pod owned by a Deployment.
func oneNodeOnePodFixture() *fakeSource {
	node := corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceCPU:    qty("4"),
				corev1.ResourceMemory: qty("8Gi"),
			},
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    qty("3800m"),
				corev1.ResourceMemory: qty("7500Mi"),
			},
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
			NodeInfo:   corev1.NodeSystemInfo{KubeletVersion: "v1.30.0"},
		},
	}
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "web-abc123xy",
			Namespace:       "default",
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "web-abc123xy"}},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Containers: []corev1.Container{{
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    qty("100m"),
						corev1.ResourceMemory: qty("128Mi"),
					},
				},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	dep := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default"},
		Spec:       appsv1.DeploymentSpec{Replicas: int32Ptr(1)},
		Status:     appsv1.DeploymentStatus{ReadyReplicas: 1},
	}
	return &fakeSource{
		nodes:       []corev1.Node{node},
		pods:        []corev1.Pod{pod},
		deployments: []appsv1.Deployment{dep},
		nodeUsage:   map[string]k8s.Usage{"node-1": {CPUMilli: 500, MemBytes: 1 << 30}},
		podUsage:    map[string]k8s.Usage{"default/web-abc123xy": {CPUMilli: 50, MemBytes: 64 << 20}},
	}
}

func TestCollectSuccessUpdatesSnapshotAndHealth(t *testing.T) {
	src := oneNodeOnePodFixture()
	c := New(src, time.Hour, 5*time.Second, testLogger())

	c.collect(context.Background())

	snap := c.Snapshot()
	if !snap.Ready {
		t.Fatal("snapshot should be ready after a successful collection")
	}
	if snap.Cluster.NodesTotal != 1 || snap.Cluster.NodesReady != 1 {
		t.Errorf("cluster nodes: got total=%d ready=%d", snap.Cluster.NodesTotal, snap.Cluster.NodesReady)
	}
	if snap.Cluster.PodsTotal != 1 {
		t.Errorf("want 1 pod, got %d", snap.Cluster.PodsTotal)
	}
	if len(snap.Microservices) != 1 || snap.Microservices[0].Name != "web" {
		t.Fatalf("want microservice %q, got %+v", "web", snap.Microservices)
	}
	if got := snap.Microservices[0]; got.ReadyReplicas != 1 || got.DesiredReplica != 1 {
		t.Errorf("microservice replicas: want ready=1 desired=1, got %+v", got)
	}

	hs := c.Health()
	if !hs.Ready {
		t.Fatal("collector should report ready after a successful collection")
	}
	if hs.LastError != "" {
		t.Errorf("want no error, got %q", hs.LastError)
	}
}

func TestCollectFailureLeavesHealthNotReady(t *testing.T) {
	src := oneNodeOnePodFixture()
	src.nodesErr = errors.New("api-server unreachable")
	c := New(src, time.Hour, 5*time.Second, testLogger())

	c.collect(context.Background())

	hs := c.Health()
	if hs.Ready {
		t.Fatal("collector should not be ready when the poll failed")
	}
	if hs.LastError == "" {
		t.Error("want the last error to be recorded")
	}
	if c.Snapshot().Ready {
		t.Error("snapshot should stay un-ready when no collection has ever succeeded")
	}
}

func TestCollectRecoversAfterFailure(t *testing.T) {
	src := oneNodeOnePodFixture()
	c := New(src, time.Hour, 5*time.Second, testLogger())

	src.podsErr = errors.New("timeout")
	c.collect(context.Background())
	if c.Health().Ready {
		t.Fatal("should not be ready after a failed poll")
	}

	src.podsErr = nil
	c.collect(context.Background())
	hs := c.Health()
	if !hs.Ready {
		t.Fatal("should recover to ready after a subsequent successful poll")
	}
	if hs.LastError != "" {
		t.Errorf("want error cleared after recovery, got %q", hs.LastError)
	}
	if !c.Snapshot().Ready {
		t.Error("snapshot should be ready once a collection has succeeded")
	}
}

func TestHealthGoesStaleWithoutRecentSuccess(t *testing.T) {
	src := oneNodeOnePodFixture()
	c := New(src, time.Hour, 5*time.Second, testLogger())
	c.collect(context.Background())
	if !c.Health().Ready {
		t.Fatal("expected ready right after a successful collection")
	}

	// Same package: reach in directly to simulate the passage of time
	// without a real sleep.
	c.mu.Lock()
	c.lastSuccess = time.Now().Add(-c.staleAfter - time.Second)
	c.mu.Unlock()

	if c.Health().Ready {
		t.Fatal("readiness should fail once the last success is older than staleAfter")
	}
}

// A failed metrics read must not outlive the collection that saw it: the
// availability flag is derived per cycle, so a later successful read recovers
// without a restart. The old shared-flag implementation latched false forever
// and failed this test's third phase.
func TestMetricsAvailabilityRecoversAfterFailure(t *testing.T) {
	src := oneNodeOnePodFixture()
	c := New(src, time.Hour, 5*time.Second, testLogger())

	c.collect(context.Background())
	if !c.Snapshot().MetricsAvailable {
		t.Fatal("want metrics available on a clean read")
	}

	src.nodeMetricsErr = errors.New("metrics-server unreachable")
	c.collect(context.Background())
	snap := c.Snapshot()
	if snap.MetricsAvailable {
		t.Fatal("want metrics unavailable while the node metrics read is failing")
	}
	if snap.Metrics.NodesAvailable {
		t.Error("want nodes_available false")
	}
	if !snap.Metrics.PodsAvailable {
		t.Error("want pods_available true: only the node read failed")
	}
	if snap.Metrics.Error == "" {
		t.Error("want the read error reported for diagnostics")
	}

	src.nodeMetricsErr = nil
	c.collect(context.Background())
	if !c.Snapshot().MetricsAvailable {
		t.Fatal("want metrics available again after a later successful read")
	}
	if got := c.Snapshot().Metrics.Error; got != "" {
		t.Errorf("want the stale error cleared, got %q", got)
	}
}

// A failed read leaves every object without a sample, and that must be
// reported as missing rather than published as a measured zero.
func TestFailedMetricsReadMarksEveryObjectMissing(t *testing.T) {
	src := oneNodeOnePodFixture()
	src.nodeMetricsErr = errors.New("boom")
	src.podMetricsErr = errors.New("boom")
	c := New(src, time.Hour, 5*time.Second, testLogger())
	c.collect(context.Background())

	snap := c.Snapshot()
	if snap.Metrics.NodesMissing != 1 || snap.Metrics.PodsMissing != 1 {
		t.Fatalf("want 1 node and 1 pod missing metrics, got %d/%d",
			snap.Metrics.NodesMissing, snap.Metrics.PodsMissing)
	}
	if !snap.Cluster.MetricsMissing {
		t.Error("want the cluster marked as missing metrics")
	}
	if !snap.Nodes[0].MetricsMissing {
		t.Error("want the node marked as missing metrics")
	}
	if !snap.Pods[0].MetricsMissing {
		t.Error("want the pod marked as missing metrics")
	}
	if !snap.Microservices[0].MetricsMissing {
		t.Error("want the workload marked as missing metrics: its sum is an undercount")
	}
	if snap.Metrics.Partial() {
		t.Error("a failed read is unavailable, not partial")
	}
}

// The metrics API can answer without having a sample for every object — a
// node or pod scheduled since the last scrape. That is partial data, not
// unavailability, and zero usage for those objects is still not a
// measurement.
func TestPartialMetricsAreReportedSeparatelyFromUnavailability(t *testing.T) {
	src := oneNodeOnePodFixture()
	src.nodeUsage = map[string]k8s.Usage{}
	c := New(src, time.Hour, 5*time.Second, testLogger())
	c.collect(context.Background())

	snap := c.Snapshot()
	if !snap.MetricsAvailable {
		t.Fatal("want metrics available: the read succeeded, it was just incomplete")
	}
	if !snap.Metrics.Partial() {
		t.Fatal("want partial metrics reported")
	}
	if snap.Metrics.NodesMissing != 1 {
		t.Errorf("want 1 node missing a sample, got %d", snap.Metrics.NodesMissing)
	}
	if snap.Metrics.PodsMissing != 0 {
		t.Errorf("want pod metrics complete, got %d missing", snap.Metrics.PodsMissing)
	}
	if !snap.Nodes[0].MetricsMissing {
		t.Error("want the unsampled node flagged so the UI does not render 0% as measured")
	}
	if snap.Pods[0].MetricsMissing {
		t.Error("want the sampled pod not flagged")
	}
}

// Availability used to live on the shared k8s client, written by the
// collector goroutine and read by HTTP handlers with no synchronization. It
// now travels inside the snapshot, which is guarded by the collector's mutex.
// Under -race this fails on the old design and passes on the new one.
func TestSnapshotIsRaceFreeAgainstConcurrentCollection(t *testing.T) {
	src := oneNodeOnePodFixture()
	c := New(src, time.Hour, 5*time.Second, testLogger())

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = c.Snapshot().MetricsAvailable
				_ = c.Health()
			}
		}
	}()
	for i := range 50 {
		if i%2 == 0 {
			src.nodeMetricsErr = errors.New("flapping")
		} else {
			src.nodeMetricsErr = nil
		}
		c.collect(context.Background())
	}
	close(stop)
	wg.Wait()
}

func TestWorkloadOf(t *testing.T) {
	cases := []struct {
		name string
		pod  corev1.Pod
		want string
	}{
		{
			"replicaset owner strips the hash suffix",
			corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name:            "web-abc123xy",
				OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "web-abc123xy"}},
			}},
			"web",
		},
		{
			"statefulset owner keeps its name",
			corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				OwnerReferences: []metav1.OwnerReference{{Kind: "StatefulSet", Name: "db"}},
			}},
			"db",
		},
		{
			"no owner falls back to the app label",
			corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "legacy"}}},
			"legacy",
		},
		{
			"no owner or label falls back to the pod name",
			corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "standalone"}},
			"standalone",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := workloadOf(&c.pod); got != c.want {
				t.Errorf("want %q, got %q", c.want, got)
			}
		})
	}
}

func TestPctZeroCapacityIsZeroNotNaN(t *testing.T) {
	if got := pct(100, 0); got != 0 {
		t.Errorf("pct with zero capacity: want 0, got %v", got)
	}
}

func podOwnedBy(ns, name, ownerKind, ownerName string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       ns,
			OwnerReferences: []metav1.OwnerReference{{Kind: ownerKind, Name: ownerName}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func findMS(t *testing.T, ms map[string]Microservice, ns, kind, name string) Microservice {
	t.Helper()
	m, ok := ms[msKey(ns, kind, name)]
	if !ok {
		t.Fatalf("want microservice %s/%s/%s, have keys %v", ns, kind, name, mapKeys(ms))
	}
	return m
}

func mapKeys[K comparable, V any](m map[K]V) []K {
	out := make([]K, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestStatefulSetAndDaemonSetReportAuthoritativeReplicas guards the core bug
// in #32: desired/ready for StatefulSets and DaemonSets must come from their
// own status, not be derived from how many pods happen to be observed (a pod
// down for a moment must not silently shrink "desired").
func TestStatefulSetAndDaemonSetReportAuthoritativeReplicas(t *testing.T) {
	sts := appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: int32Ptr(3)},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 2},
	}
	ds := appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: "default"},
		Status:     appsv1.DaemonSetStatus{DesiredNumberScheduled: 3, NumberReady: 2},
	}
	pods := []corev1.Pod{
		podOwnedBy("default", "db-0", "StatefulSet", "db"),
		podOwnedBy("default", "db-1", "StatefulSet", "db"),
		// db-2 is down/missing entirely: only 2 of 3 pods observed.
		podOwnedBy("default", "agent-a", "DaemonSet", "agent"),
		podOwnedBy("default", "agent-b", "DaemonSet", "agent"),
	}
	src := &fakeSource{
		nodes:        []corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}},
		pods:         pods,
		statefulSets: []appsv1.StatefulSet{sts},
		daemonSets:   []appsv1.DaemonSet{ds},
	}
	c := New(src, time.Hour, 5*time.Second, testLogger())
	c.collect(context.Background())

	msMap := map[string]Microservice{}
	for _, m := range c.Snapshot().Microservices {
		msMap[m.Key] = m
	}

	db := findMS(t, msMap, "default", "StatefulSet", "db")
	if db.DesiredReplica != 3 {
		t.Errorf("statefulset desired: want 3 (from spec, not pod count), got %d", db.DesiredReplica)
	}
	if db.ReadyReplicas != 2 {
		t.Errorf("statefulset ready: want 2 (from status), got %d", db.ReadyReplicas)
	}

	agent := findMS(t, msMap, "default", "DaemonSet", "agent")
	if agent.DesiredReplica != 3 {
		t.Errorf("daemonset desired: want 3 (from DesiredNumberScheduled), got %d", agent.DesiredReplica)
	}
	if agent.ReadyReplicas != 2 {
		t.Errorf("daemonset ready: want 2 (from NumberReady), got %d", agent.ReadyReplicas)
	}
}

// TestZeroPodWorkloadStaysVisible guards against a StatefulSet/DaemonSet
// with no currently-observed pods disappearing from the microservice list
// entirely, per #32's acceptance criteria.
func TestZeroPodWorkloadStaysVisible(t *testing.T) {
	sts := appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: int32Ptr(3)},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 0},
	}
	src := &fakeSource{statefulSets: []appsv1.StatefulSet{sts}}
	c := New(src, time.Hour, 5*time.Second, testLogger())
	c.collect(context.Background())

	snap := c.Snapshot()
	if len(snap.Microservices) != 1 {
		t.Fatalf("want the zero-pod statefulset to remain visible, got %+v", snap.Microservices)
	}
	if got := snap.Microservices[0]; got.Kind != "StatefulSet" || got.DesiredReplica != 3 || got.ReadyReplicas != 0 {
		t.Errorf("want StatefulSet desired=3 ready=0, got %+v", got)
	}
}

// TestSameNameDifferentKindWorkloadsDoNotCollide guards against a Deployment
// and a StatefulSet sharing a name in the same namespace merging into one
// entry and losing data, per #32's acceptance criteria.
func TestSameNameDifferentKindWorkloadsDoNotCollide(t *testing.T) {
	dep := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "default"},
		Spec:       appsv1.DeploymentSpec{Replicas: int32Ptr(2)},
		Status:     appsv1.DeploymentStatus{ReadyReplicas: 2},
	}
	sts := appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: int32Ptr(1)},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 1},
	}
	pods := []corev1.Pod{
		podOwnedBy("default", "shared-rs1-abc12345", "ReplicaSet", "shared-abc12345"),
		podOwnedBy("default", "shared-rs2-abc12345", "ReplicaSet", "shared-abc12345"),
		podOwnedBy("default", "shared-0", "StatefulSet", "shared"),
	}
	src := &fakeSource{
		pods:         pods,
		deployments:  []appsv1.Deployment{dep},
		statefulSets: []appsv1.StatefulSet{sts},
	}
	c := New(src, time.Hour, 5*time.Second, testLogger())
	c.collect(context.Background())

	snap := c.Snapshot()
	if len(snap.Microservices) != 2 {
		t.Fatalf("want 2 distinct microservices for same-name different-kind workloads, got %+v", snap.Microservices)
	}
	msMap := map[string]Microservice{}
	for _, m := range snap.Microservices {
		msMap[m.Key] = m
	}
	d := findMS(t, msMap, "default", "Deployment", "shared")
	if d.DesiredReplica != 2 || d.RunningPods != 2 {
		t.Errorf("deployment entry corrupted by collision: %+v", d)
	}
	s := findMS(t, msMap, "default", "StatefulSet", "shared")
	if s.DesiredReplica != 1 || s.RunningPods != 1 {
		t.Errorf("statefulset entry corrupted by collision: %+v", s)
	}
}

// TestHPAMatchesTargetKind guards against an HPA targeting a Deployment
// being attached to a same-named StatefulSet (or vice versa).
func TestHPAMatchesTargetKind(t *testing.T) {
	sts := appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: int32Ptr(1)},
	}
	dep := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "default"},
		Spec:       appsv1.DeploymentSpec{Replicas: int32Ptr(1)},
	}
	hpa := autoscalingv1.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: "shared-hpa", Namespace: "default"},
		Spec: autoscalingv1.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv1.CrossVersionObjectReference{Kind: "StatefulSet", Name: "shared"},
			MaxReplicas:    int32(5),
		},
	}
	src := &fakeSource{
		deployments:  []appsv1.Deployment{dep},
		statefulSets: []appsv1.StatefulSet{sts},
		hpas:         []autoscalingv1.HorizontalPodAutoscaler{hpa},
	}
	c := New(src, time.Hour, 5*time.Second, testLogger())
	c.collect(context.Background())

	msMap := map[string]Microservice{}
	for _, m := range c.Snapshot().Microservices {
		msMap[m.Key] = m
	}
	if findMS(t, msMap, "default", "StatefulSet", "shared").HPA == nil {
		t.Error("want the HPA attached to the StatefulSet it targets")
	}
	if findMS(t, msMap, "default", "Deployment", "shared").HPA != nil {
		t.Error("HPA targeting the StatefulSet must not attach to the same-named Deployment")
	}
}

// TestCronJobRunsCollapseIntoOneWorkload guards the Job/CronJob handling
// required by #32: successive scheduled Job runs (whose generated names
// carry a timestamp suffix) should collapse into a single "Job" workload
// rather than one entry per run.
func TestCronJobRunsCollapseIntoOneWorkload(t *testing.T) {
	pods := []corev1.Pod{
		podOwnedBy("default", "backup-1758000000-abc", "Job", "backup-1758000000"),
		podOwnedBy("default", "backup-1758003600-def", "Job", "backup-1758003600"),
	}
	src := &fakeSource{pods: pods}
	c := New(src, time.Hour, 5*time.Second, testLogger())
	c.collect(context.Background())

	snap := c.Snapshot()
	if len(snap.Microservices) != 1 {
		t.Fatalf("want cronjob runs collapsed into one workload, got %+v", snap.Microservices)
	}
	m := snap.Microservices[0]
	if m.Kind != "Job" || m.Name != "backup" {
		t.Errorf("want kind=Job name=backup, got kind=%s name=%s", m.Kind, m.Name)
	}
	if m.RunningPods != 2 {
		t.Errorf("want both job runs' pods counted, got %d", m.RunningPods)
	}
}
