package telemetry

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/beholdrapp/stalkr/cluster"
	"github.com/beholdrapp/stalkr/demo"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	collector "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/protobuf/proto"
)

func TestExportsStandardOTLPAndOmitsUnmeasuredUsage(t *testing.T) {
	var mu sync.Mutex
	names := map[string]int{}
	resources := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/metrics" || r.Header.Get("Content-Type") != "application/x-protobuf" {
			t.Errorf("not OTLP/HTTP: %s %s", r.URL, r.Header)
		}
		body, _ := io.ReadAll(r.Body)
		var request collector.ExportMetricsServiceRequest
		if err := proto.Unmarshal(body, &request); err != nil {
			t.Error(err)
		}
		mu.Lock()
		defer mu.Unlock()
		for _, rm := range request.ResourceMetrics {
			resources++
			found := false
			for _, attr := range rm.Resource.Attributes {
				if attr.Key == "k8s.cluster.name" && attr.Value.GetStringValue() == "test-cluster" {
					found = true
				}
			}
			if !found {
				t.Error("missing cluster identity")
			}
			for _, scope := range rm.ScopeMetrics {
				for _, m := range scope.Metrics {
					names[m.Name]++
				}
			}
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
	}))
	defer server.Close()
	exp, err := otlpmetrichttp.New(context.Background(), otlpmetrichttp.WithEndpointURL(server.URL+"/v1/metrics"))
	if err != nil {
		t.Fatal(err)
	}
	defer exp.Shutdown(context.Background())
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := cluster.New(demo.Source{}, time.Second, time.Second, log)
	c.Poll(context.Background())
	s := c.Snapshot()
	s.Nodes[0].MetricsMissing = true
	s.Pods[0].MetricsMissing = true
	if err := New(exp, "test-cluster", log).Export(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if resources != 23 {
		t.Fatalf("expected 3 nodes, 14 pods, 6 workloads; got %d", resources)
	}
	for name, want := range map[string]int{"k8s.node.cpu.usage": 2, "k8s.pod.cpu.usage": 13, "k8s.deployment.pod.desired": 4, "k8s.statefulset.pod.ready": 1, "k8s.daemonset.node.ready": 1} {
		if names[name] != want {
			t.Errorf("%s: got %d, want %d", name, names[name], want)
		}
	}
}

type blockedExporter struct{ started chan struct{} }

func (e blockedExporter) Export(ctx context.Context, _ *metricdata.ResourceMetrics) error {
	select {
	case e.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestBackpressureKeepsOnlyNewestSnapshotAndCancels(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	f := New(blockedExporter{make(chan struct{}, 1)}, "test", log)
	for i := 0; i < 100; i++ {
		f.Offer(cluster.Snapshot{UpdatedAt: float64(i)})
	}
	if f.Dropped.Load() != 99 {
		t.Fatalf("dropped %d", f.Dropped.Load())
	}
	if got := <-f.queue; got.UpdatedAt != 99 {
		t.Fatalf("kept stale snapshot: %v", got.UpdatedAt)
	}
	c := cluster.New(demo.Source{}, time.Second, time.Second, log)
	c.Poll(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { f.Run(ctx); close(done) }()
	f.Offer(c.Snapshot())
	select {
	case <-f.exporter.(blockedExporter).started:
	case <-time.After(time.Second):
		t.Fatal("export did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("blocked export ignored cancellation")
	}
}
