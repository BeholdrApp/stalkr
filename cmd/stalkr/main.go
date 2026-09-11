// Command stalkr is an egress-only, read-only Kubernetes observer.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/beholdrapp/stalkr/cluster"
	"github.com/beholdrapp/stalkr/demo"
	"github.com/beholdrapp/stalkr/internal/telemetry"
	"github.com/beholdrapp/stalkr/k8s"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
)

func main() {
	if err := run(); err != nil {
		slog.Error("stalkr stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	demoMode := flag.Bool("demo", false, "synthetic Kubernetes input; only literal loopback OTLP endpoints allowed")
	endpoint := flag.String("endpoint", "", "OTLP/HTTP metrics URL, including /v1/metrics; overrides OTEL exporter endpoint")
	name := flag.String("cluster", os.Getenv("K8S_CLUSTER_NAME"), "stable cluster name (required outside demo)")
	mode := flag.String("kube-mode", "in-cluster", "in-cluster or kubeconfig (explicit opt-in)")
	kubeconfig := flag.String("kubeconfig", "", "explicit kubeconfig file for kubeconfig mode")
	namespaces := flag.String("namespaces", "", "comma-separated namespace allowlist; empty observes all namespaces")
	interval := flag.Duration("interval", 15*time.Second, "collection interval (minimum 1s)")
	once := flag.Bool("once", false, "collect and export once; fail on collection/export error")
	flag.Parse()
	if *interval < time.Second {
		return errors.New("interval must be at least 1s")
	}
	if *demoMode {
		*name = "beholdr-demo"
		if *endpoint == "" {
			*endpoint = "http://127.0.0.1:4318/v1/metrics"
		}
	}
	if strings.TrimSpace(*name) == "" {
		return errors.New("set -cluster or K8S_CLUSTER_NAME")
	}
	if *endpoint != "" {
		if err := validateEndpoint(*endpoint, *demoMode); err != nil {
			return err
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	var src cluster.Source
	if *demoMode {
		src = demo.Source{}
	} else {
		if *mode != "in-cluster" && (*mode != "kubeconfig" || *kubeconfig == "") {
			return errors.New("use in-cluster or explicitly supply -kube-mode kubeconfig -kubeconfig FILE")
		}
		var ns []string
		for _, n := range strings.Split(*namespaces, ",") {
			if n = strings.TrimSpace(n); n != "" {
				ns = append(ns, n)
			}
		}
		client, err := k8s.New(*mode, *kubeconfig, ns, log)
		if err != nil {
			return err
		}
		src = client
	}
	opts := []otlpmetrichttp.Option{otlpmetrichttp.WithTimeout(5 * time.Second)}
	if *endpoint != "" {
		opts = append(opts, otlpmetrichttp.WithEndpointURL(*endpoint))
	}
	if *demoMode {
		// Explicit transport and headers prevent ambient proxies, credentials,
		// redirects or OTEL_* settings from escaping the isolated demo.
		opts = append(opts, otlpmetrichttp.WithHeaders(map[string]string{}), otlpmetrichttp.WithHTTPClient(&http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}))
	}
	exporter, err := otlpmetrichttp.New(ctx, opts...)
	if err != nil {
		return err
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = exporter.Shutdown(shutdown)
	}()
	forwarder := telemetry.New(exporter, *name, log)
	collector := cluster.New(src, *interval, 5*time.Second, log)
	if *once {
		var exportErr error
		collector.OnSnapshot = func(s cluster.Snapshot) {
			exportCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			exportErr = forwarder.Export(exportCtx, s)
		}
		collector.Poll(ctx)
		if !collector.Snapshot().Ready {
			return errors.New("initial collection failed")
		}
		return exportErr
	}
	collector.OnSnapshot = forwarder.Offer
	done := make(chan struct{})
	go func() { forwarder.Run(ctx); close(done) }()
	collector.Run(ctx)
	<-done
	return nil
}

func validateEndpoint(raw string, isolated bool) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("endpoint must be an HTTP(S) URL without credentials, query or fragment")
	}
	if isolated {
		ip := net.ParseIP(u.Hostname())
		if ip == nil || !ip.IsLoopback() {
			return errors.New("demo endpoint must use a literal loopback IP")
		}
	}
	return nil
}
