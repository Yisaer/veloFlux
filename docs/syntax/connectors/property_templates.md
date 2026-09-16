# Connector Property Templates

Connector property templates use the same Upon engine as JSON encoder transform
templates. They are compiled when a pipeline is applied:

```text
vehicle/{{ prop("vin") }}/telemetry
```

The connector profile registers `prop("key")`, where the key must be at most
128 bytes and match `[a-z][a-z0-9_]*`. It otherwise follows Upon's native
template syntax. It does not provide `.row` or the encoder-only `json()`
function. References to missing context values and calls to unregistered
functions fail during pipeline apply.

Literal text, Upon expressions, filters, and control blocks use Upon's normal
semantics. VeloFlux does not add a separate delimiter escape or connector
expression parser.

Property values are inserted exactly as configured. They are not trimmed,
converted, encoded, parsed recursively, or expanded a second time.

The initial template-enabled connector fields are:

- MQTT sink `props.topic`;
- MQTT sink `props.user_properties[].value`;
- HTTP sink `props.headers.*` values;
- HTTP multipart `props.body.fields.*` values.
- File sink `props.filename_pattern`.

Other connector fields are not interpreted as templates and retain their
literal configuration value. In particular, HTTP `auth` and `secret_headers`
remain `SecretRef` paths: template-looking text there is handled as literal
secret input by the existing secret policy.

MQTT User Property keys remain literal. Their values use the process property
profile described above. They do
not provide `.row` or access to incoming MQTT 5 User Properties. Declaration
order and duplicate keys are preserved after rendering.

Without a runtime provider, `prop()` values are rendered and fixed in the plan.
With the local MQTT provider enabled, literal-only templates remain fixed while
templates using `prop()` are resolved from one immutable snapshot at the
consumer boundary:

- JSON transforms snapshot at encoder delivery start;
- MQTT and HTTP sinks snapshot after receiving a complete encoded delivery and
  before the first external attempt;
- file sinks use that connector snapshot for the final filename pattern after
  the upstream batch or roll has completed.

Connector retries reuse the original snapshot. A later property update affects
only a later delivery. Encoder and connector boundaries take snapshots
independently, so they are not guaranteed to observe the same revision.
If a delivery is restored from a checkpoint after process restart, its encoded
payload is replayed unchanged, while connector templates use a new property
snapshot taken just before the first replay attempt. Subsequent retries of that
restored delivery reuse the replay snapshot.

Missing properties and invalid Upon syntax reject only the affected pipeline
during apply or hydration. A dynamically rendered invalid protocol field fails
that delivery under the connector retry policy; it does not stop other
pipelines or the service.
