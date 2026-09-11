package k8s

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
)

func TestNamespacedReads(t *testing.T) {
	for _, namespaces := range [][]string{nil, {"shop"}, {"shop", "platform"}} {
		t.Run(fmt.Sprint(namespaces), func(t *testing.T) {
			var paths []string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				paths = append(paths, r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				switch {
				case strings.HasSuffix(r.URL.Path, "horizontalpodautoscalers"):
					fmt.Fprint(w, `{"apiVersion":"autoscaling/v1","kind":"HorizontalPodAutoscalerList","items":[]}`)
				case strings.HasPrefix(r.URL.Path, "/apis/metrics.k8s.io/"):
					fmt.Fprint(w, `{"apiVersion":"metrics.k8s.io/v1beta1","kind":"PodMetricsList","items":[{"metadata":{"namespace":"shop","name":"checkout-0"},"containers":[{"name":"app","usage":{"cpu":"25m","memory":"32Mi"}}]}]}`)
				default:
					t.Errorf("unexpected request: %s", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer srv.Close()
			cfg := &rest.Config{Host: srv.URL}
			cs, err := kubernetes.NewForConfig(cfg)
			if err != nil {
				t.Fatal(err)
			}
			mc, err := metricsclient.NewForConfig(cfg)
			if err != nil {
				t.Fatal(err)
			}
			c := &Client{cs: cs, metrics: mc, namespaces: namespaces, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
			if _, err := c.HPAs(context.Background()); err != nil {
				t.Fatal(err)
			}
			usage, err := c.PodMetrics(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if usage["shop/checkout-0"] != (Usage{CPUMilli: 25, MemBytes: 32 * 1024 * 1024}) {
				t.Fatalf("usage: %v", usage)
			}
			var want []string
			scopes := namespaces
			if len(scopes) == 0 {
				scopes = []string{""}
			}
			for _, api := range []string{"autoscaling/v1", "metrics.k8s.io/v1beta1"} {
				resource := "horizontalpodautoscalers"
				if api == "metrics.k8s.io/v1beta1" {
					resource = "pods"
				}
				for _, ns := range scopes {
					prefix := "/apis/" + api
					if ns != "" {
						prefix += "/namespaces/" + ns
					}
					want = append(want, prefix+"/"+resource)
				}
			}
			if !reflect.DeepEqual(paths, want) {
				t.Fatalf("read paths: %v; want %v", paths, want)
			}
		})
	}
}
