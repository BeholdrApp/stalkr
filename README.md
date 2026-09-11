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

## Run the preview

The observer, Kubernetes source and tests now live here. `stalkr` exports
standard OTLP/HTTP Kubernetes metrics, with bounded pending work and explicit
synthetic demo inputs. Beholdr imports the extracted collector; chart history
stays in the control plane.

```sh
go test ./...
go run ./cmd/stalkr -demo -once
```

The demo command needs a loopback OTLP receiver. Use the complete
[agents demo](https://github.com/BeholdrApp/Beholdr/blob/main/docs/agents-demo.md)
to start every component and inspect results in Beholdr. See
[development and delivery status](docs/development.md) for configuration,
exported coverage and remaining release work.

## Compatibility

`stalkr` and the Beholdr control plane version independently. The supported
version skew and the wire contract between them are tracked in
[#3](https://github.com/BeholdrApp/stalkr/issues/3).

## License

Released under the [MIT License](LICENSE).
