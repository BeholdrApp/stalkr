# stalkr

**The Beholdr observer that runs inside a monitored Kubernetes cluster.**

`stalkr` is the cluster-side agent for [Beholdr](https://github.com/BeholdrApp/Beholdr).
It discovers what is running in the cluster it lives in, collects health and
resource state, scrapes or federates Prometheus-compatible metrics, and forwards
everything to a Beholdr control plane over OTLP.

## Why it is a separate repository

`stalkr` runs in clusters that Beholdr's operators do not control, and it is
upgraded on the cluster owner's schedule rather than the control plane's. That
means it needs its own release cadence, its own compatibility guarantees, and
its own security review — which is exactly the bar for a separate repository.
Everything that ships to the Beholdr management cluster lives in the
[Beholdr monorepo](https://github.com/BeholdrApp/Beholdr) instead.

## Design constraints

- **Egress only.** `stalkr` dials out to the control plane. Nothing dials in.
  No inbound firewall rules, no kubeconfig held centrally, no cluster API
  exposed to the management plane.
- **Read only.** It observes; it never mutates workloads or scaling settings.
- **Standards first.** Telemetry leaves over OTLP. Metrics are read through
  the Prometheus HTTP API. There is no bespoke Beholdr wire protocol.
- **N=1 is a supported topology.** A single cluster running both `stalkr` and
  the control plane is the same code path as a fleet, with a different chart.

## Status

Early. The collection logic currently lives in the Beholdr monorepo under
`internal/collect` and `internal/k8s` and is being extracted here. See the
[issues](https://github.com/BeholdrApp/stalkr/issues) and the
[Beholdr roadmap](https://github.com/BeholdrApp/Beholdr/blob/main/ROADMAP.md).

## Compatibility

`stalkr` and the Beholdr control plane version independently. The supported
version skew and the wire contract between them are tracked in
[#3](https://github.com/BeholdrApp/stalkr/issues/3).

## License

All rights reserved. See [LICENSE](LICENSE).
