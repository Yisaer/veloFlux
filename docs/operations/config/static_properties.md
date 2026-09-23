# Process Properties

## Configuration

Properties are deployment-provided strings loaded when the process starts:

```yaml
properties:
  vin: "L123456789"
  site_code: "cn"
```

Keys must be at most 128 bytes and match `[a-z][a-z0-9_]*`. Values must be YAML
strings; numbers, booleans, lists, and maps are rejected rather than converted
implicitly. Empty strings are valid.

Each property can be added or overridden with an environment variable:

```bash
export VELOFLUX_PROPERTIES__VIN=L987654321
export VELOFLUX_PROPERTIES__SITE_CODE=de
```

The suffix is normalized to lowercase. File values are merged per key first,
then environment values override matching keys. An empty environment value is
an explicit empty string. File and environment changes require a process
restart.

All in-process flow instances share the same process property registry.
Properties are not manager resources and are not included in metadata
export/import.

## Optional Local MQTT Overrides

Runtime overrides can be enabled with one topic filter:

```yaml
property_registry:
  mqtt:
    topic: "$local/notification/+"
```

The provider always connects to the local broker at `127.0.0.1:1883` using
MQTT 3.1.1, QoS 1, and client id `VeloFluxIntPropRegistry`. The configured
filter must be at most 1024 bytes and end in `+`; no other wildcard is allowed.
The final topic level is the property key. For example, a publish to
`$local/notification/vin` updates `vin`.

Only keys declared under `properties` may be updated. Publishers must send all
property updates as retained messages so the broker always holds the latest
value for replay after a reconnect or VeloFlux restart. A non-retained update
may take effect temporarily but is not guaranteed to survive a reconnect.

The payload must be non-empty UTF-8 and no larger than 4096 bytes. It becomes
the value exactly as published, without trimming or recursive expansion. Empty
payloads are rejected; there is no MQTT operation that clears an override or
falls back to the static base.

The provider starts asynchronously and never blocks pipeline startup while the
broker or a retained value is unavailable. Until an override arrives,
consumers use the static base. A valid override remains effective across broker
disconnects; a retained publish can restore it after a VeloFlux restart.

## Security Boundary

Configuration files and environment variables contain plaintext strings.
After loading, property values and connector strings rendered from them use
redacted, zeroizing runtime string types. Their `Debug` and `Display`
representations do not reveal the value.

Pipeline GET, storage, and export preserve the original connector template.
They do not contain the rendered value or the process property map. Logs and
errors may include a property key and field path, but must not include the
property value or complete rendered connector string.

MQTT override payloads are sensitive input and must not be logged, emitted in
metrics, or included in update errors. The value is necessarily exposed as plaintext at the MQTT or HTTP client call
boundary and in generated file names when used by a file sink affix. When
When `prop()` is used by a template encoder, it can also enter the encoded
payload bytes. Transport confidentiality depends on the connector's TLS and
authentication configuration; file-name confidentiality depends on filesystem
access controls.

See [connector property templates](../../syntax/connectors/property_templates.md)
for the supported fields and grammar.
