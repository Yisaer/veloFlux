# Apply REST API (Manager)

This document describes the **Manager** REST API for incremental deployment of persisted metadata.

Base URL depends on your deployment (examples use `http://127.0.0.1:8080`).

> **Resource IDs.** The same id grammar as the create APIs and the
> [Import](import.md) API applies: stream names, pipeline ids, sink ids,
> schema names, table names, memory topics, shared MQTT client keys, UDF names, and
> `flow_instance_id` references must match `` `[A-Za-z][A-Za-z0-9_]{0,127}` ``.
> Invalid ids reject the whole batch with `400 Bad Request`.

## Endpoint

`POST /storage/apply`

Applies a validated manifest delta atomically: it creates, updates, or explicitly
deletes the declared resources and retains resources absent from the manifest.
For full-snapshot replacement (backup restore), use
`POST /storage/import` ([Import](import.md)) instead.

Send either `application/json` with a manifest, or `multipart/form-data` with
exactly one non-empty `file` field whose filename ends in `.zip`. The optional
query parameter `cascade` defaults to `false` for both JSON and ZIP.

```sh
curl -H 'Content-Type: application/json' \
  --data-binary @manifest.json http://127.0.0.1:8080/storage/apply
curl -F 'file=@deployment.zip' 'http://127.0.0.1:8080/storage/apply?cascade=true'
```

## Request Shape

`ResourceManifestV1`:

- `format_version: number` (currently `1`)
- `bundle_version: string` (producer artifact identity, echoed in the result)
- `resources: ExportResources` (all seven collections optional, omitted means empty)
- `deletions`: optional revisioned tombstones, same seven collection names

Each resource or deletion entry requires a `revision` in `1..=9007199254740991`
(JSON-safe positive integer). Streams use the `CreateStreamRequest` shape, pipelines use the
`CreatePipelineRequest` shape plus an optional inline `run_state`
(`Stopped` when omitted).

> **Revision must keep increasing, or updates cannot overwrite.** A resource is only
> replaced when the incoming `revision` is greater than the stored one: a lower
> revision rejects the entire batch with `409 revision_conflict`, and an equal
> revision is accepted only when the normalized content is unchanged (different
> content at the same revision is also `409`). Every change to a resource must
> therefore bump its `revision`. The simplest practice is to use the current
> timestamp (e.g. milliseconds since epoch, as in the example below) or a
> per-resource monotonic counter that you increment yourself. Never reuse the
> same revision for different content.

Key semantics:

- The batch is validated and committed atomically. Array order does not decide
  dependency order.
- Dependencies are checked against the effective view (persisted resources plus
  accepted upserts minus accepted deletions). A missing dependency rejects the
  entire batch before anything is committed.
- A greater revision creates, replaces, or deletes the resource. A lower revision
  rejects the entire batch. An equal revision is idempotent only when the
  normalized content is unchanged; otherwise it is rejected.
- With the default `cascade=false`, unsubmitted streams/tables and pipeline graphs
  keep their runtime state. Submitted newer pipelines are installed according to
  their incoming `run_state` in either mode.

## Example: New Stream Plus Dependent Pipeline In One Request

Declare the new stream and the pipeline that reads from it in a single manifest
(`manifest.json`):

```json
{
  "format_version": 1,
  "bundle_version": "deployment-001",
  "resources": {
    "streams": [
      {
        "name": "telemetry",
        "revision": 1721797200000,
        "type": "mqtt",
        "schema": {
          "type": "json",
          "props": {
            "columns": [
              { "name": "speed", "data_type": "float64" }
            ]
          }
        },
        "props": {
          "broker_url": "tcp://127.0.0.1:1883",
          "topic": "vehicle/telemetry"
        },
        "decoder": { "type": "json", "props": {} }
      }
    ],
    "pipelines": [
      {
        "id": "speed_pipeline",
        "revision": 1721797200001,
        "sql": "SELECT speed FROM telemetry",
        "sinks": [
          {
            "type": "mqtt",
            "props": {
              "broker_url": "tcp://127.0.0.1:1883",
              "topic": "vehicle/speed_out"
            },
            "encoder": { "type": "json", "props": {} }
          }
        ],
        "run_state": "Stopped"
      }
    ]
  }
}
```

```sh
curl -H 'Content-Type: application/json' \
  --data-binary @manifest.json http://127.0.0.1:8080/storage/apply
```

Because both resources travel in one atomic batch, the pipeline's `FROM telemetry`
reference resolves against the same batch. There is no ordering hazard: putting
`pipelines` before `streams` in the JSON arrays works the same way. Do not split
this into two steps (`POST /pipelines` first, then `POST /streams`); the first
call would fail because the stream does not exist yet.

The example revisions (`1721797200000`, `1721797200001`) are timestamps in
milliseconds. When you change either resource later, send the same manifest with a
greater revision (e.g. a newer timestamp); reusing the old revision leaves the
update unapplied or rejected.

## Response

A processed configuration commit returns `200`, including when runtime
application partially fails. Inspect the per-resource entries:

- `result`: `created`, `updated`, `deleted`, or `idempotent`.
- `operation`: `upsert` or `delete`.
- `runtime_applied`: `true` when confirmed, `false` on confirmed failure or
  unavailable runtime, `null` when this request did not apply the resource.
- Aggregate `applied_to_runtime` is `false` if any resource reports `false`.

Precommit failures return `4xx`/`5xx` with `metadata_committed: false` and none
of the batch committed. Common codes: `400` invalid manifest/resources,
`409` revision conflict / referenced resource / busy, `413` body too large,
`415` wrong media type, `500` preparation/commit failure.

## Notes

- Apply is incremental (retains absent resources). Import is full replacement
  (removes absent resources and is storage-only).
- Repeating the same declaration is safe: equal revision with equal content is
  idempotent and does not restart resources.
- New file attachments (schema sources, WASM modules) require the ZIP form. JSON
  can only reference already installed managed files.
- See the [detailed contract](../../docs/api/storage_apply.md) and the
  [error comparison](../../docs/api/storage_errors.md).
