# Development and delivery status

The Kubernetes source, aggregation, workload identity and regression tests were
extracted from Beholdr rather than rewritten. Beholdr consumes this Go module;
its rolling chart history remains in Beholdr. The agent retains one current
snapshot and one pending export. During an outage, newer observations replace
pending old ones; export attempts share a ten-second deadline. Drops and failures
are logged. There is no disk spool and no claim of lossless delivery.

```sh
go test ./...
go run ./cmd/stalkr -demo -once
```

The second command requires a local OTLP receiver at
`http://127.0.0.1:4318/v1/metrics` and fails if no observation is delivered.
The complete [Beholdr agents demo](https://github.com/BeholdrApp/Beholdr/blob/main/docs/agents-demo.md)
starts the receiver and shows telemetry in Beholdr.

Outside demo, `-cluster` (or `K8S_CLUSTER_NAME`) is required and Kubernetes auth
defaults to in-cluster. Reading a kubeconfig requires both `-kube-mode kubeconfig`
and `-kubeconfig FILE`. The binary has no inbound server. `-namespaces` filters
namespace reads; it is not an authorization boundary. Set standard
`OTEL_EXPORTER_OTLP_ENDPOINT` (base URL) or
`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` (full metrics URL), headers and TLS
settings for the upstream HTTP exporter. `-endpoint` overrides the full metrics
URL. HTTP/protobuf is the supported protocol for this initial agent.

The Go collector still provides pod/container status reasons and collapsed
CronJob views to Beholdr. The initial OTLP profile exports a subset of those
observations; collapsed Job names are not mislabeled as concrete Job resources.
Inventory synchronization, Prometheus federation and richer event export are
tracked follow-ups.

`charts/stalkr` has read-only RBAC and defaults to local `gaze` Service DNS. Use
the same chart/binary with a different endpoint for a remote receiver. CI renders
and lints the chart, runs race tests/vet, scans the built container and emits a
CycloneDX SBOM. No installation occurs. Container/chart publication and signing
are not configured yet; the default image tag is a planned preview artifact,
so supply a built image when testing manifests.

The canonical [telemetry contract](https://github.com/BeholdrApp/Beholdr/blob/main/contracts/telemetry/README.md)
uses standard OTLP and resource conventions. Formal agent/platform release-skew
guarantees, deployment profile CI and enrollment/tenant authentication remain
open. No version handshake rejects standard producers.
