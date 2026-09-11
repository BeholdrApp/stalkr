// Package telemetry maps observations to standard OTLP metrics. No snapshot JSON
// or Beholdr-specific envelope is sent across the repository boundary.
package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/beholdrapp/stalkr/cluster"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

type Exporter interface {
	Export(context.Context, *metricdata.ResourceMetrics) error
}

// Forwarder retains one pending observation, replacing it with the newest on
// overload. Gauges describe current state: replaying old snapshots after an
// outage would be misleading. There is no disk spool or unbounded retry queue.
type Forwarder struct {
	queue       chan cluster.Snapshot
	exporter    Exporter
	clusterName string
	log         *slog.Logger
	Dropped     atomic.Uint64
}

func New(exporter Exporter, clusterName string, log *slog.Logger) *Forwarder {
	return &Forwarder{queue: make(chan cluster.Snapshot, 1), exporter: exporter, clusterName: clusterName, log: log}
}

// Offer has a single producer (the collector's OnSnapshot callback).
func (f *Forwarder) Offer(s cluster.Snapshot) {
	select {
	case f.queue <- s:
		return
	default:
	}
	select {
	case <-f.queue:
		f.Dropped.Add(1)
	default:
	}
	select {
	case f.queue <- s:
	default:
		f.Dropped.Add(1)
	}
}

func (f *Forwarder) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case s := <-f.queue:
			exportCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := f.Export(exportCtx, s)
			cancel()
			if err != nil {
				f.log.Warn("telemetry export failed; next poll retries fresh state", "error", err, "replaced_snapshots", f.Dropped.Load())
			}
		}
	}
}

func (f *Forwarder) Export(ctx context.Context, s cluster.Snapshot) error {
	if !s.Ready {
		return fmt.Errorf("no successful observation to export")
	}
	if len(s.Nodes)+len(s.Pods)+len(s.Microservices) > 10000 {
		return fmt.Errorf("observation exceeds 10000 entity export limit")
	}
	at := time.Unix(0, int64(s.UpdatedAt*1e9))
	send := func(attrs []attribute.KeyValue, metrics ...metricdata.Metrics) error {
		attrs = append(attrs, attribute.String("k8s.cluster.name", f.clusterName), attribute.String("service.name", "stalkr"))
		rm := metricdata.ResourceMetrics{Resource: resource.NewSchemaless(attrs...), ScopeMetrics: []metricdata.ScopeMetrics{{Scope: instrumentation.Scope{Name: "github.com/beholdrapp/stalkr", Version: "0.1.0-preview.1"}, Metrics: metrics}}}
		return f.exporter.Export(ctx, &rm)
	}
	for _, n := range s.Nodes {
		var metrics []metricdata.Metrics
		points := []metricdata.DataPoint[float64]{}
		for kind, current := range n.Conditions {
			for _, status := range []string{"true", "false", "unknown"} {
				points = append(points, metricdata.DataPoint[float64]{Time: at, Value: boolNumber(current == status), Attributes: attribute.NewSet(attribute.String("k8s.node.condition.type", kind), attribute.String("k8s.node.condition.status", status))})
			}
		}
		if len(points) > 0 {
			metrics = append(metrics, metricdata.Metrics{Name: "k8s.node.condition.status", Unit: "{node}", Data: metricdata.Sum[float64]{Temporality: metricdata.CumulativeTemporality, DataPoints: points}})
		}
		if !n.MetricsMissing {
			metrics = append(metrics, gauge("k8s.node.cpu.usage", "{cpu}", float64(n.CPUUsed)/1000, at), currentSum("k8s.node.memory.working_set", "By", float64(n.MemUsed), at))
		}
		if err := send([]attribute.KeyValue{attribute.String("k8s.node.name", n.Name)}, metrics...); err != nil {
			return err
		}
	}
	for _, p := range s.Pods {
		attrs := []attribute.KeyValue{attribute.String("k8s.namespace.name", p.Namespace), attribute.String("k8s.pod.name", p.Name), attribute.String("k8s.node.name", p.Node)}
		if key := workloadAttribute(p.WorkloadKind); key != "" {
			attrs = append(attrs, attribute.String(key, p.Workload))
		}
		points := []metricdata.DataPoint[float64]{}
		phase := p.Phase
		if phase == "" {
			phase = "Unknown"
		}
		for _, value := range []string{"Pending", "Running", "Succeeded", "Failed", "Unknown"} {
			points = append(points, metricdata.DataPoint[float64]{Time: at, Value: boolNumber(phase == value), Attributes: attribute.NewSet(attribute.String("k8s.pod.status.phase", value))})
		}
		metrics := []metricdata.Metrics{{Name: "k8s.pod.status.phase", Unit: "{pod}", Data: metricdata.Sum[float64]{Temporality: metricdata.CumulativeTemporality, DataPoints: points}}}
		if !p.MetricsMissing {
			metrics = append(metrics, gauge("k8s.pod.cpu.usage", "{cpu}", float64(p.CPUUsed)/1000, at), currentSum("k8s.pod.memory.working_set", "By", float64(p.MemUsed), at))
		}
		if err := send(attrs, metrics...); err != nil {
			return err
		}
	}
	for _, m := range s.Microservices {
		key := workloadAttribute(m.Kind)
		var prefix, desired, ready string
		readyCount, unit := m.ReadyReplicas, "{pod}"
		switch m.Kind {
		case "Deployment":
			prefix, desired, ready = "k8s.deployment.pod.", "desired", "available"
			readyCount = m.AvailableReplicas
		case "StatefulSet":
			prefix, desired, ready = "k8s.statefulset.pod.", "desired", "ready"
		case "DaemonSet":
			prefix, desired, ready = "k8s.daemonset.node.", "desired_scheduled", "ready"
			unit = "{node}"
		default:
			continue // Job pod ownership remains in the observations, not invented replica metrics.
		}
		if err := send([]attribute.KeyValue{attribute.String("k8s.namespace.name", m.Namespace), attribute.String(key, m.Name)}, currentSum(prefix+desired, unit, float64(m.DesiredReplica), at), currentSum(prefix+ready, unit, float64(readyCount), at)); err != nil {
			return err
		}
	}
	return nil
}

func workloadAttribute(kind string) string {
	switch kind {
	case "Deployment":
		return "k8s.deployment.name"
	case "StatefulSet":
		return "k8s.statefulset.name"
	case "DaemonSet":
		return "k8s.daemonset.name"

	}
	return ""
}

func gauge(name, unit string, value float64, at time.Time, attrs ...attribute.KeyValue) metricdata.Metrics {
	return metricdata.Metrics{Name: name, Unit: unit, Data: metricdata.Gauge[float64]{DataPoints: []metricdata.DataPoint[float64]{{Time: at, Value: value, Attributes: attribute.NewSet(attrs...)}}}}
}

func boolNumber(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

func currentSum(name, unit string, value float64, at time.Time, attrs ...attribute.KeyValue) metricdata.Metrics {
	return metricdata.Metrics{Name: name, Unit: unit, Data: metricdata.Sum[float64]{Temporality: metricdata.CumulativeTemporality, DataPoints: []metricdata.DataPoint[float64]{{Time: at, Value: value, Attributes: attribute.NewSet(attrs...)}}}}
}
