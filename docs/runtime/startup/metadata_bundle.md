# Resource Directory Format

Export, import, incremental Apply, and startup initialization share one canonical directory:

```text
manifest.json
schemas/<type>/<name>/
wasm_files/<sha256>.wasm
```

`manifest.json` uses `format_version: 1` and contains the resource collections
for memory topics, shared MQTT clients, schemas, streams, tables, pipelines, and
UDFs. Models are supported by full storage import/export, but startup init and
incremental Apply accept only an empty `models` collection.
Every resource or deletion requires a `revision` in `1..=9007199254740991`.
`resources`, optional `deletions`, and each collection may be omitted; omitted
collections are empty, while `null` and non-array collections are invalid.

```json
{
  "format_version": 1,
  "bundle_version": "2026.07.24-1",
  "resources": {
    "memory_topics": [
      {
        "topic": "decoded_frames",
        "revision": 1721797200000,
        "kind": "collection",
        "capacity": 1024
      }
    ],
    "shared_mqtt_clients": [
      {
        "key": "vehicle_broker",
        "revision": 1721797200001,
        "broker_url": "tcp://127.0.0.1:1883",
        "topic": "vehicle/telemetry",
        "client_id": "vehicle_reader",
        "qos": 1,
        "protocol_version": "v5"
      }
    ],
    "schemas": [
      {
        "name": "telemetry_schema",
        "revision": 1721797200002,
        "type": "json",
        "props": {
          "columns": [
            { "name": "speed", "data_type": "float64" }
          ]
        }
      }
    ],
    "streams": [
      {
        "name": "telemetry",
        "revision": 1721797200003,
        "type": "mqtt",
        "schema": { "ref": "telemetry_schema" },
        "props": { "connector_key": "vehicle_broker" },
        "decoder": { "type": "json", "props": {} }
      }
    ],
    "tables": [
      {
        "name": "vehicle_history",
        "revision": 1721797200004,
        "type": "history",
        "schema": { "ref": "telemetry_schema" },
        "props": {
          "datasource": "/var/lib/nanomq/history",
          "topic": "vehicle",
          "time_column": "ts"
        },
        "decoder": { "type": "json", "props": {} }
      }
    ],
    "pipelines": [
      {
        "id": "speed_pipeline",
        "revision": 1721797200005,
        "sql": "SELECT speed FROM telemetry",
        "sources": [],
        "sinks": [],
        "run_state": "Running"
      }
    ],
    "udfs": [
      {
        "name": "normalize_speed",
        "revision": 1721797200006,
        "wasm_sha256": "<64 lowercase hex characters>"
      }
    ]
  }
}
```

Pipeline desired run state is inline. Schema files and WASM modules are present
only when referenced. Ordinary uploads, config, secrets, checkpoints, offsets,
and runtime state are outside this format.

The physical format does not select write semantics:

- HTTP export writes a ZIP and includes stored revisions without generating or
  comparing them.
- Startup `--init-dir` reads an extracted directory and performs revision-based,
  best-effort Apply. It retains resources absent from the directory.
- Online `POST /storage/apply` accepts manifest JSON or a ZIP and applies a
  complete validated delta atomically, then reports runtime application separately.
  It retains absent resources, rejects lower revisions and equal-revision content
  conflicts, and acknowledges equal normalized deployment declarations without
  undoing operational start/stop. Omitted pipeline `run_state` is `Stopped`.
- HTTP `POST /storage/import` validates the complete ZIP and performs full-snapshot Sync in one
  metadata transaction. It does not compare revisions, so an imported lower
  revision replaces a stored higher revision and absent resources are deleted.
  Import remains storage-only and reports `applied_to_runtime: false`.

`bundle_version` identifies one complete producer directory and is compared only
for equality during init. It is not ordered and is distinct from per-resource
`revision`.

## Export, Edit, and Initialize

```shell
curl -sS -o veloflux-export.zip \
  'http://127.0.0.1:8080/storage/export?bundle_version=2026.07.24-1'
unzip veloflux-export.zip -d ./init

# Increase revision for every resource whose definition or pipeline run state
# is changed, and choose a new bundle_version for the edited directory.

veloflux --config ./config.yaml --data-dir ./data --init-dir ./init
```

Keep file-backed schema sources under `schemas/<type>/<name>/`. After changing a
WASM module, recompute its SHA-256, rename it to
`wasm_files/<sha256>.wasm`, update `wasm_sha256`, and increase the UDF revision.

Use startup Apply for selective revision-based deployment. Use HTTP import when
the artifact must replace the complete persisted resource set.

## Entry Contracts

Format version 1 supports optional revisioned `deletions`. JSON Apply permits inline resources and already
installed, validated managed attachments; new files require ZIP.

Online Apply defaults to `cascade=false`, preserving unsubmitted
stream/table definitions and pipeline graphs. `cascade=true` coordinates cascading reconstruction
around the complete dependency batch; submitted newer pipelines always use their
incoming `run_state`. Upsert replay remains idempotent with either query value; deletion replay retries cleanup.

Pipeline deployment declarations are stored independently of operational
Running/Stopped intent. Export preserves its existing snapshot behavior: inline
`run_state` reflects current persisted operational desired state. Import treats
that snapshot value as the restored declaration; online replay compares against
the last accepted deployment declaration. Start/stop does not change revisions.

Read the [Apply API](../../api/storage_apply.md),
[Import API](../../api/storage_import.md), and
[error comparison](../../api/storage_errors.md) before choosing an entrance.

## Deletion Records

`deletions` contains any of the same seven collection names. Each record has
only its identity and revision:

| Collection | Identity | Example |
| --- | --- | --- |
| `memory_topics` | `topic` | `{"topic":"frames","revision":12}` |
| `shared_mqtt_clients` | `key` | `{"key":"broker","revision":12}` |
| `schemas` | `name` | `{"name":"payload","revision":12}` |
| `streams` | `name` | `{"name":"telemetry","revision":12}` |
| `tables` | `name` | `{"name":"history","revision":12}` |
| `pipelines` | `id` | `{"id":"processor","revision":12}` |
| `udfs` | `name` | `{"name":"transform","revision":12}` |

Within one kind, an identity must be unique across both resources and deletions.
UDF names are case-insensitive. Array order does not decide which operation wins.

Export includes tombstones using this object. Import restores the complete
resource and tombstone snapshot without comparing existing revisions. An old
snapshot lacking a tombstone removes that protection; subsequent old upserts
can recreate the identity. Import rejects live/deleted duplicate identities.
Init skips entries whose revision does not exceed the live record or tombstone;
its successful subset must preserve retained owners' dependencies. Online
Apply uses the [revision and dependency rules](../../api/storage_apply.md).
