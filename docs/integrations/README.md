# Integrations

This section contains designs for external integrations and developer-facing integration workflows.

- [VSS and Kuksa Integration](vss-kuksa/README.md): decode packed CAN/GBF MQTT input, process DBC-backed signals, and publish MQTT or Kuksa/VSS output.
- [FFI Embedding](ffi/README.md): embed veloFlux into a host process through a thin C ABI shim while keeping HTTP/REST as the runtime control plane.
