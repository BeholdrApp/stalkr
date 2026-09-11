// Package cluster polls and aggregates read-only Kubernetes observations.
// It retains only the latest snapshot; history belongs to the control plane.
package cluster

import (
	"context"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/beholdrapp/stalkr/k8s"
)

// rsHash strips the hash suffix Kubernetes appends to a ReplicaSet name
// (itself derived from the Deployment) so pods can be attributed back to
// their Deployment by name.
var rsHash = regexp.MustCompile(`-[a-z0-9]{8,10}$`)

// jobSuffix strips the timestamp suffix a CronJob appends to the Jobs it
// creates, so successive scheduled runs collapse into one workload instead
// of a new entry per run.
var jobSuffix = regexp.MustCompile(`-\d{8,10}$`)

// Source is the subset of the k8s client the collector consumes (interface
// keeps the collector testable).
type Source interface {
	Nodes(context.Context) ([]corev1.Node, error)
	Pods(context.Context) ([]corev1.Pod, error)
	Deployments(context.Context) ([]appsv1.Deployment, error)
	StatefulSets(context.Context) ([]appsv1.StatefulSet, error)
	DaemonSets(context.Context) ([]appsv1.DaemonSet, error)
	HPAs(context.Context) ([]autoscalingv1.HorizontalPodAutoscaler, error)
	NodeMetrics(context.Context) (map[string]k8s.Usage, error)
	PodMetrics(context.Context) (map[string]k8s.Usage, error)
}

type Collector struct {
	src      Source
	interval time.Duration
	timeout  time.Duration
	// staleAfter bounds how old the last successful collection may be before
	// Health().Ready reports false — readiness must fail on stale data, not
	// just on a collection that has never succeeded.
	staleAfter time.Duration
	log        *slog.Logger

	// OnSnapshot is called synchronously after a successful poll. Set before Run.
	// Consumers must not mutate the snapshot and should enqueue work without blocking.
	OnSnapshot func(Snapshot)

	mu   sync.RWMutex
	snap Snapshot

	lastSuccess time.Time
	lastErr     error
	lastErrAt   time.Time
}

func New(src Source, interval, timeout time.Duration, log *slog.Logger) *Collector {
	staleAfter := interval * 3
	if staleAfter < 30*time.Second {
		staleAfter = 30 * time.Second
	}
	return &Collector{
		src:        src,
		interval:   interval,
		timeout:    timeout,
		staleAfter: staleAfter,
		log:        log,
	}
}

// HealthStatus reports the collector's own liveness/readiness signal,
// independent of whatever data happens to be cached in Snapshot: it reflects
// whether the *most recent* poll succeeded recently, not merely whether one
// has ever succeeded. Timestamps are unix seconds, 0 meaning "never".
type HealthStatus struct {
	Ready       bool    `json:"ready"`
	LastSuccess float64 `json:"last_success"`
	LastError   string  `json:"last_error,omitempty"`
	LastErrorAt float64 `json:"last_error_at,omitempty"`
}

// Health reports whether the collector has completed a collection recently
// enough to be trusted, plus the most recent error (if any) for diagnostics.
func (c *Collector) Health() HealthStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	hs := HealthStatus{LastSuccess: unixSeconds(c.lastSuccess), LastErrorAt: unixSeconds(c.lastErrAt)}
	if c.lastErr != nil {
		hs.LastError = c.lastErr.Error()
	}
	hs.Ready = !c.lastSuccess.IsZero() && time.Since(c.lastSuccess) < c.staleAfter
	return hs
}

func unixSeconds(t time.Time) float64 {
	if t.IsZero() {
		return 0
	}
	return float64(t.UnixNano()) / 1e9
}

func (c *Collector) Snapshot() Snapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.snap
}

// Run polls until ctx is cancelled.
func (c *Collector) Run(ctx context.Context) {
	c.collect(ctx) // prime immediately
	t := time.NewTicker(c.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.collect(ctx)
		}
	}
}

// Poll performs one bounded collection. Do not call concurrently with Run.
func (c *Collector) Poll(ctx context.Context) { c.collect(ctx) }

func (c *Collector) collect(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()

	nodes, err := c.src.Nodes(ctx)
	if err != nil {
		c.log.Error("list nodes", "err", err)
		c.recordErr(err)
		return
	}
	pods, err := c.src.Pods(ctx)
	if err != nil {
		c.log.Error("list pods", "err", err)
		c.recordErr(err)
		return
	}
	deployments, err := c.src.Deployments(ctx)
	if err != nil {
		c.log.Error("list deployments", "err", err)
		c.recordErr(err)
		return
	}
	statefulSets, err := c.src.StatefulSets(ctx)
	if err != nil {
		c.log.Error("list statefulsets", "err", err)
		c.recordErr(err)
		return
	}
	daemonSets, err := c.src.DaemonSets(ctx)
	if err != nil {
		c.log.Error("list daemonsets", "err", err)
		c.recordErr(err)
		return
	}
	hpas, _ := c.src.HPAs(ctx)

	// Metrics availability is derived from this cycle's reads alone. A failed
	// read leaves the usage map empty and is reported through metrics; it is
	// never latched onto shared state, so the next successful read recovers
	// on its own.
	var metrics MetricsStatus
	nodeUsage, nodeMetricsErr := c.src.NodeMetrics(ctx)
	if nodeMetricsErr != nil {
		c.log.Warn("node metrics read failed", "err", nodeMetricsErr)
		metrics.Error = nodeMetricsErr.Error()
	}
	metrics.NodesAvailable = nodeMetricsErr == nil
	podUsage, podMetricsErr := c.src.PodMetrics(ctx)
	if podMetricsErr != nil {
		c.log.Warn("pod metrics read failed", "err", podMetricsErr)
		metrics.Error = podMetricsErr.Error()
	}
	metrics.PodsAvailable = podMetricsErr == nil

	ts := float64(time.Now().UnixNano()) / 1e9

	nodeMap, nodesMissing := buildNodes(nodes, nodeUsage)
	podList, byNode, byMS, podsMissing := buildPods(pods, podUsage)
	metrics.NodesMissing, metrics.PodsMissing = nodesMissing, podsMissing
	msMap := buildMicroservices(deployments, statefulSets, daemonSets, hpas, byMS)

	for name, n := range nodeMap {
		np := byNode[name]
		n.Pods = np
		n.PodCount = len(np)
		n.Workloads = distinctWorkloads(np)
		nodeMap[name] = n
	}

	cluster := buildCluster(nodeMap, podList, msMap)
	cluster.MetricsMissing = nodesMissing > 0

	snap := Snapshot{
		Ready:            true,
		UpdatedAt:        ts,
		MetricsAvailable: metrics.Available(),
		Metrics:          metrics,
		Cluster:          cluster,
		Nodes:            sortedNodes(nodeMap),
		Microservices:    sortedMS(msMap),
		Pods:             podList,
	}
	c.mu.Lock()
	c.snap = snap
	c.lastSuccess = time.Now()
	c.lastErr = nil
	c.mu.Unlock()
	if c.OnSnapshot != nil {
		c.OnSnapshot(snap)
	}
	c.log.Info("collected",
		"nodes", len(nodeMap), "pods", len(podList), "microservices", len(msMap),
		"metrics_available", metrics.Available(),
		"nodes_missing_metrics", metrics.NodesMissing, "pods_missing_metrics", metrics.PodsMissing)
}

func (c *Collector) recordErr(err error) {
	c.mu.Lock()
	c.lastErr = err
	c.lastErrAt = time.Now()
	c.mu.Unlock()
}

// --- builders ---------------------------------------------------------------

// buildNodes returns the node map plus the number of nodes the metrics API
// had no sample for, so the caller can tell "measured zero" from "not
// measured" instead of publishing the two as the same number.
func buildNodes(nodes []corev1.Node, usage map[string]k8s.Usage) (map[string]Node, int) {
	out := make(map[string]Node, len(nodes))
	missing := 0
	for _, n := range nodes {
		cpuCap := n.Status.Capacity.Cpu().MilliValue()
		memCap := n.Status.Capacity.Memory().Value()
		u, haveUsage := usage[n.Name]
		if !haveUsage {
			missing++
		}
		conditions := map[string]string{}
		ready := false
		for _, cond := range n.Status.Conditions {
			conditions[string(cond.Type)] = strings.ToLower(string(cond.Status))
			if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionTrue {
				ready = true
			}
		}
		roles := []string{}
		for k := range n.Labels {
			const p = "node-role.kubernetes.io/"
			if len(k) > len(p) && k[:len(p)] == p {
				roles = append(roles, k[len(p):])
			}
		}
		if len(roles) == 0 {
			roles = []string{"worker"}
		}
		out[n.Name] = Node{
			Name:           n.Name,
			Conditions:     conditions,
			Ready:          ready,
			Roles:          roles,
			KubeletVersion: n.Status.NodeInfo.KubeletVersion,
			CPUCapacity:    cpuCap,
			MemCapacity:    memCap,
			CPUAllocatable: n.Status.Allocatable.Cpu().MilliValue(),
			MemAllocatable: n.Status.Allocatable.Memory().Value(),
			CPUUsed:        u.CPUMilli,
			MemUsed:        u.MemBytes,
			CPUPct:         pct(u.CPUMilli, cpuCap),
			MemPct:         pct(u.MemBytes, memCap),
			MetricsMissing: !haveUsage,
		}
	}
	return out, missing
}

// buildPods returns the pod list, the by-node and by-workload groupings, and
// the number of pods with no usage sample — see buildNodes for why that count
// is tracked rather than folded into a zero.
func buildPods(pods []corev1.Pod, usage map[string]k8s.Usage) ([]Pod, map[string][]Pod, map[string][]Pod, int) {
	list := make([]Pod, 0, len(pods))
	byNode := map[string][]Pod{}
	byMS := map[string][]Pod{}
	missing := 0
	for i := range pods {
		p := &pods[i]
		kind, workload := ownerOf(p)
		reqCPU, reqMem := podRequests(p)
		u, haveUsage := usage[p.Namespace+"/"+p.Name]
		if !haveUsage {
			missing++
		}
		var restarts int32
		for _, cs := range p.Status.ContainerStatuses {
			restarts += cs.RestartCount
		}
		e := Pod{
			Namespace:      p.Namespace,
			Name:           p.Name,
			Node:           p.Spec.NodeName,
			Workload:       workload,
			WorkloadKind:   kind,
			Phase:          string(p.Status.Phase),
			StatusReason:   podStatusReason(p),
			Restarts:       restarts,
			CPUUsed:        u.CPUMilli,
			MemUsed:        u.MemBytes,
			CPURequest:     reqCPU,
			MemRequest:     reqMem,
			MetricsMissing: !haveUsage,
		}
		list = append(list, e)
		if e.Node != "" {
			byNode[e.Node] = append(byNode[e.Node], e)
		}
		key := msKey(p.Namespace, kind, workload)
		byMS[key] = append(byMS[key], e)
	}
	return list, byNode, byMS, missing
}

// A pod can remain in Running phase while a container is crash-looping.
// Keep the Kubernetes phase and surface the current reason separately.
func podStatusReason(p *corev1.Pod) string {
	for _, statuses := range [][]corev1.ContainerStatus{p.Status.InitContainerStatuses, p.Status.ContainerStatuses} {
		for _, cs := range statuses {
			if cs.State.Waiting != nil && cs.State.Waiting.Reason != "" {
				return cs.State.Waiting.Reason
			}
			if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 && cs.State.Terminated.Reason != "" {
				return cs.State.Terminated.Reason
			}
		}
	}
	return p.Status.Reason
}

// msKey identifies a workload by namespace, controller kind and name, so a
// Deployment and a StatefulSet that happen to share a name in the same
// namespace are tracked as distinct microservices instead of colliding.
func msKey(ns, kind, name string) string { return ns + "/" + kind + "/" + name }

func splitMSKey(k string) (ns, kind, name string) {
	ns, rest, _ := strings.Cut(k, "/")
	kind, name, _ = strings.Cut(rest, "/")
	return
}

func buildMicroservices(deps []appsv1.Deployment, stss []appsv1.StatefulSet, dss []appsv1.DaemonSet, hpas []autoscalingv1.HorizontalPodAutoscaler, byMS map[string][]Pod) map[string]Microservice {
	hpaByTarget := map[string]autoscalingv1.HorizontalPodAutoscaler{}
	for _, h := range hpas {
		kind := h.Spec.ScaleTargetRef.Kind
		if kind == "" {
			kind = "Deployment"
		}
		hpaByTarget[msKey(h.Namespace, kind, h.Spec.ScaleTargetRef.Name)] = h
	}

	out := map[string]Microservice{}
	seen := map[string]bool{}
	addController := func(ns, name, kind string, desired, ready int32) {
		key := msKey(ns, kind, name)
		seen[key] = true
		pods := byMS[key]
		var hpa *autoscalingv1.HorizontalPodAutoscaler
		if h, ok := hpaByTarget[key]; ok {
			hpa = &h
		}
		out[key] = msEntry(ns, name, kind, desired, ready, pods, hpa)
	}

	for i := range deps {
		d := &deps[i]
		desired := int32(len(byMS[msKey(d.Namespace, "Deployment", d.Name)]))
		if d.Spec.Replicas != nil {
			desired = *d.Spec.Replicas
		}
		addController(d.Namespace, d.Name, "Deployment", desired, d.Status.ReadyReplicas)
		entry := out[msKey(d.Namespace, "Deployment", d.Name)]
		entry.AvailableReplicas = d.Status.AvailableReplicas
		out[entry.Key] = entry
	}
	for i := range stss {
		s := &stss[i]
		desired := int32(1)
		if s.Spec.Replicas != nil {
			desired = *s.Spec.Replicas
		}
		addController(s.Namespace, s.Name, "StatefulSet", desired, s.Status.ReadyReplicas)
	}
	for i := range dss {
		ds := &dss[i]
		addController(ds.Namespace, ds.Name, "DaemonSet", ds.Status.DesiredNumberScheduled, ds.Status.NumberReady)
	}

	// Workloads with pods but no matching controller object above (bare
	// Jobs, CronJob-created Jobs, or unmanaged pods) stay visible under
	// whatever kind their owner reference reported, keyed so they never
	// collide with a controller of a different kind sharing the same name.
	for key, pods := range byMS {
		if seen[key] {
			continue
		}
		ns, kind, name := splitMSKey(key)
		var hpa *autoscalingv1.HorizontalPodAutoscaler
		if h, ok := hpaByTarget[key]; ok {
			hpa = &h
		}
		// ready=-1 tells msEntry to derive it from the pods it's already
		// about to scan for running/CPU/mem, instead of counting them twice.
		out[key] = msEntry(ns, name, kind, int32(len(pods)), -1, pods, hpa)
	}
	return out
}

func msEntry(ns, name, kind string, desired, ready int32, pods []Pod, hpa *autoscalingv1.HorizontalPodAutoscaler) Microservice {
	var cpuUsed, memUsed, cpuReq, memReq int64
	var restarts int32
	running := 0
	nodeSet := map[string]struct{}{}
	metricsMissing := false
	for _, p := range pods {
		cpuUsed += p.CPUUsed
		memUsed += p.MemUsed
		cpuReq += p.CPURequest
		memReq += p.MemRequest
		restarts += p.Restarts
		if p.MetricsMissing {
			metricsMissing = true
		}
		if p.Phase == "Running" {
			running++
		}
		if p.Node != "" {
			nodeSet[p.Node] = struct{}{}
		}
	}
	if ready < 0 {
		ready = int32(running)
	}
	nodes := make([]string, 0, len(nodeSet))
	for n := range nodeSet {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)

	var util *float64
	if cpuReq > 0 {
		v := round1(100 * float64(cpuUsed) / float64(cpuReq))
		util = &v
	}
	m := Microservice{
		Key: msKey(ns, kind, name), Namespace: ns, Name: name, Kind: kind,
		DesiredReplica: desired, ReadyReplicas: ready, RunningPods: running,
		Restarts: restarts, Nodes: nodes,
		CPUUsed: cpuUsed, MemUsed: memUsed, CPURequest: cpuReq, MemRequest: memReq,
		CPUUtilPct: util, MetricsMissing: metricsMissing,
	}
	if hpa != nil {
		h := HPA{
			Max:           hpa.Spec.MaxReplicas,
			Current:       hpa.Status.CurrentReplicas,
			Desired:       hpa.Status.DesiredReplicas,
			TargetCPUPct:  hpa.Spec.TargetCPUUtilizationPercentage,
			CurrentCPUPct: hpa.Status.CurrentCPUUtilizationPercentage,
		}
		if hpa.Spec.MinReplicas != nil {
			h.Min = *hpa.Spec.MinReplicas
		}
		m.HPA = &h
	}
	return m
}

func buildCluster(nodeMap map[string]Node, pods []Pod, ms map[string]Microservice) Cluster {
	var cpuCap, memCap, cpuUsed, memUsed int64
	ready := 0
	for _, n := range nodeMap {
		cpuCap += n.CPUCapacity
		memCap += n.MemCapacity
		cpuUsed += n.CPUUsed
		memUsed += n.MemUsed
		if n.Ready {
			ready++
		}
	}
	byPhase := map[string]int{}
	for _, p := range pods {
		ph := p.Phase
		if ph == "" {
			ph = "Unknown"
		}
		byPhase[ph]++
	}
	return Cluster{
		NodesTotal: len(nodeMap), NodesReady: ready,
		MicroservicesTotal: len(ms), PodsTotal: len(pods), PodsByPhase: byPhase,
		CPUCapacity: cpuCap, MemCapacity: memCap, CPUUsed: cpuUsed, MemUsed: memUsed,
		CPUPct: pct(cpuUsed, cpuCap), MemPct: pct(memUsed, memCap),
	}
}

// --- helpers ----------------------------------------------------------------

// ownerOf identifies the controller kind and name a pod belongs to.
// ReplicaSet ownership resolves to the owning Deployment's name (the hash
// suffix Kubernetes appends to the ReplicaSet is stripped); Job ownership
// resolves to the owning CronJob's name when the Job carries the
// scheduler's timestamp suffix, so successive scheduled runs collapse into
// one workload instead of a new entry per run.
func ownerOf(p *corev1.Pod) (kind, name string) {
	for _, ref := range p.OwnerReferences {
		switch ref.Kind {
		case "ReplicaSet":
			return "Deployment", rsHash.ReplaceAllString(ref.Name, "")
		case "StatefulSet":
			return "StatefulSet", ref.Name
		case "DaemonSet":
			return "DaemonSet", ref.Name
		case "Job":
			return "Job", jobSuffix.ReplaceAllString(ref.Name, "")
		}
	}
	if v, ok := p.Labels["app"]; ok {
		return "Other", v
	}
	if v, ok := p.Labels["app.kubernetes.io/name"]; ok {
		return "Other", v
	}
	return "Other", p.Name
}

func workloadOf(p *corev1.Pod) string {
	_, name := ownerOf(p)
	return name
}

func podRequests(p *corev1.Pod) (cpu, mem int64) {
	for _, c := range p.Spec.Containers {
		if r := c.Resources.Requests; r != nil {
			cpu += r.Cpu().MilliValue()
			mem += r.Memory().Value()
		}
	}
	return
}

func distinctWorkloads(pods []Pod) []string {
	set := map[string]struct{}{}
	for _, p := range pods {
		set[p.Workload] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for w := range set {
		out = append(out, w)
	}
	sort.Strings(out)
	return out
}

func sortedNodes(m map[string]Node) []Node {
	out := make([]Node, 0, len(m))
	for _, n := range m {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func sortedMS(m map[string]Microservice) []Microservice {
	out := make([]Microservice, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CPUUsed > out[j].CPUUsed })
	return out
}

func pct(used, cap int64) float64 {
	if cap <= 0 {
		return 0
	}
	return round1(100 * float64(used) / float64(cap))
}

func round1(f float64) float64 {
	return float64(int64(f*10+0.5)) / 10
}
