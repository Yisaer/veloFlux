# Deployment Security

## Trust Model

Manager is a fully trusted control plane and has no built-in authentication or
per-user authorization. Rule authors are authorized to use the filesystem,
network access, and configured secrets available to the VeloFlux process. File
sources and sinks can access paths permitted by the process's operating-system
permissions, and HTTP sinks can target internal services.

Ordinary stream payloads are data, not control-plane configuration. Keep Manager
and runtime property updates inaccessible to untrusted data publishers.

## Listener Defaults

Manager defaults to `127.0.0.1:8080`. Profiling, when enabled, defaults to
`127.0.0.1:6060`. Configuration files and supported environment variables can
override these addresses. Profiling is disabled by both the built-in and example
configurations. For diagnostics, explicitly set `profiling.enabled: true` or
`VELOFLUX_PROFILING__ENABLED=true` before startup. The repository does not ship a
root `config.yaml`, so running from the repository without an explicit config or
environment override uses the disabled profiling default. Load
`etc/config.example.yaml` explicitly or copy it to a local `config.yaml` to use
the template settings.

Loopback limits remote access but does not authenticate local processes. Treat
other processes sharing the network namespace as part of this boundary.

## Gateway Isolation

- A gateway in the same network namespace can reach Manager on loopback. Expose
  only the authenticated gateway to external clients.
- A gateway in another container needs an explicit Manager bind address, such as
  `0.0.0.0:8080`. Use a dedicated container network with trusted members and do
  not publish Manager's port externally. If the gateway runs on the host, publish
  Manager's container port only on host loopback.
- For a gateway on another host, bind to a management interface and enforce
  default-deny ingress with an explicit allowance for the gateway. Use an
  authenticated encrypted transport over shared or untrusted networks.
- In Kubernetes, avoid external exposure of the Manager service and enforce a
  default-deny ingress NetworkPolicy allowing only gateway workloads. The
  cluster's network plugin must enforce NetworkPolicy.

Authenticate every Manager route and HTTP method at the gateway, including
upload, import, and export. Reject requests when authentication fails or is
unavailable, and strip client-supplied identity headers before setting trusted
ones. CORS is not an authentication boundary. Restrict profiling separately;
protecting Manager alone does not protect another listening port.

Verify from outside the trusted network that direct Manager access fails,
unauthenticated gateway requests fail, and authenticated requests succeed.
Also verify that an unrelated container or Pod cannot bypass the gateway.

## Runtime Properties

The optional `property_registry.mqtt` provider consumes updates from the local
broker at `127.0.0.1:1883`. It is disabled unless explicitly configured. Updates
can replace already declared properties used in connector templates, including
HTTP header values, MQTT topics, and file naming patterns.

The property topic is a control-plane input. Use broker ACLs to allow publishing
only by trusted administrators or configuration services. A local broker address
does not guarantee that all publishers are trusted: the broker may also accept
remote clients or bridge messages from another broker. Keep ordinary ingestion
publishers and bridged untrusted topics outside the property update namespace.
If this separation cannot be enforced, leave the provider disabled.

HTTP destination URLs and secret-store reference selection are configured by
rule authors; ordinary stream payloads do not select them. Dynamic properties
used in ordinary HTTP headers are resolved for each delivery and therefore need
the same trust as static configuration.

## HTTP Redirects

HTTP sinks do not follow redirects. A 3xx response is an `Uncertain` delivery
failure subject to the existing sink retry policy; a retry targets the original
configured URL. Configure the final endpoint URL directly. This prevents a
redirect response from forwarding custom secret headers or delivery payloads
to another destination, including for 307 and 308 responses.
