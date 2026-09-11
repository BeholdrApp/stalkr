package cluster

// JSON shapes served by the API. Field names are the stable contract the UI
// depends on.

type Cluster struct {
	NodesTotal         int            `json:"nodes_total"`
	NodesReady         int            `json:"nodes_ready"`
	MicroservicesTotal int            `json:"microservices_total"`
	PodsTotal          int            `json:"pods_total"`
	PodsByPhase        map[string]int `json:"pods_by_phase"`
	CPUCapacity        int64          `json:"cpu_capacity"`
	MemCapacity        int64          `json:"mem_capacity"`
	CPUUsed            int64          `json:"cpu_used"`
	MemUsed            int64          `json:"mem_used"`
	CPUPct             float64        `json:"cpu_pct"`
	MemPct             float64        `json:"mem_pct"`
	// MetricsMissing is true when at least one node contributed no usage
	// sample, so CPUUsed/MemUsed and the percentages below them are an
	// undercount of the cluster rather than a measurement of it.
	MetricsMissing bool `json:"metrics_missing"`
}

type Node struct {
	Conditions     map[string]string `json:"conditions,omitempty"`
	Name           string            `json:"name"`
	Ready          bool              `json:"ready"`
	Roles          []string          `json:"roles"`
	KubeletVersion string            `json:"kubelet_version"`
	CPUCapacity    int64             `json:"cpu_capacity"`
	MemCapacity    int64             `json:"mem_capacity"`
	CPUAllocatable int64             `json:"cpu_allocatable"`
	MemAllocatable int64             `json:"mem_allocatable"`
	CPUUsed        int64             `json:"cpu_used"`
	MemUsed        int64             `json:"mem_used"`
	CPUPct         float64           `json:"cpu_pct"`
	MemPct         float64           `json:"mem_pct"`
	PodCount       int               `json:"pod_count"`
	Workloads      []string          `json:"workloads"`
	Pods           []Pod             `json:"pods,omitempty"`
	// MetricsMissing is true when metrics.k8s.io returned no sample for this
	// node. The usage fields above are then zero because nothing was
	// measured, not because the node is idle.
	MetricsMissing bool `json:"metrics_missing"`
}

type Pod struct {
	Namespace    string `json:"namespace"`
	Name         string `json:"name"`
	Node         string `json:"node"`
	Workload     string `json:"workload"`
	WorkloadKind string `json:"workload_kind"`
	Phase        string `json:"phase"`
	StatusReason string `json:"status_reason,omitempty"`
	Restarts     int32  `json:"restarts"`
	CPUUsed      int64  `json:"cpu_used"`
	MemUsed      int64  `json:"mem_used"`
	CPURequest   int64  `json:"cpu_request"`
	MemRequest   int64  `json:"mem_request"`
	// MetricsMissing carries the same meaning as on Node: no usage sample
	// existed for this pod, so CPUUsed/MemUsed are unmeasured, not zero.
	MetricsMissing bool `json:"metrics_missing"`
}

type HPA struct {
	Min           int32  `json:"min"`
	Max           int32  `json:"max"`
	Current       int32  `json:"current"`
	Desired       int32  `json:"desired"`
	TargetCPUPct  *int32 `json:"target_cpu_pct"`
	CurrentCPUPct *int32 `json:"current_cpu_pct"`
}

type Microservice struct {
	AvailableReplicas int32    `json:"available_replicas"`
	Key               string   `json:"key"`
	Namespace         string   `json:"namespace"`
	Name              string   `json:"name"`
	Kind              string   `json:"kind"`
	DesiredReplica    int32    `json:"desired_replicas"`
	ReadyReplicas     int32    `json:"ready_replicas"`
	RunningPods       int      `json:"running_pods"`
	Restarts          int32    `json:"restarts"`
	Nodes             []string `json:"nodes"`
	CPUUsed           int64    `json:"cpu_used"`
	MemUsed           int64    `json:"mem_used"`
	CPURequest        int64    `json:"cpu_request"`
	MemRequest        int64    `json:"mem_request"`
	CPUUtilPct        *float64 `json:"cpu_util_pct"`
	HPA               *HPA     `json:"hpa"`
	// MetricsMissing is true when any of this workload's pods had no usage
	// sample, making the summed usage — and CPUUtilPct derived from it — an
	// undercount.
	MetricsMissing bool `json:"metrics_missing"`
}

// MetricsStatus describes what the metrics.k8s.io reads produced during one
// collection. It is recomputed every cycle and never carried forward, so a
// transient metrics-server outage clears itself on the next successful read.
//
// Availability and completeness are separate: the API can answer (available)
// while still having no sample for a node or pod that was only just
// scheduled, which is what the Missing counts report.
type MetricsStatus struct {
	NodesAvailable bool `json:"nodes_available"`
	PodsAvailable  bool `json:"pods_available"`
	// NodesMissing and PodsMissing count objects the collector saw for which
	// the metrics API returned no sample. When the corresponding read failed
	// outright, every object counts as missing.
	NodesMissing int `json:"nodes_missing"`
	PodsMissing  int `json:"pods_missing"`
	// Error is the most recent read failure of this cycle, for diagnostics.
	Error string `json:"error,omitempty"`
}

// Available reports whether both metrics reads succeeded. It says nothing
// about completeness — check the Missing counts for that.
func (m MetricsStatus) Available() bool { return m.NodesAvailable && m.PodsAvailable }

// Partial reports whether metrics were readable but incomplete: some objects
// exist that the metrics API had no sample for.
func (m MetricsStatus) Partial() bool {
	return m.Available() && (m.NodesMissing > 0 || m.PodsMissing > 0)
}

// Snapshot is the full consolidated view produced each poll cycle.
type Snapshot struct {
	Ready     bool    `json:"ready"`
	UpdatedAt float64 `json:"updated_at"`
	// MetricsAvailable is Metrics.Available(), kept as its own field because
	// it is the stable name the UI already consumes.
	MetricsAvailable bool           `json:"metrics_available"`
	Metrics          MetricsStatus  `json:"metrics"`
	Cluster          Cluster        `json:"cluster"`
	Nodes            []Node         `json:"nodes"`
	Microservices    []Microservice `json:"microservices"`
	Pods             []Pod          `json:"pods"`
}
