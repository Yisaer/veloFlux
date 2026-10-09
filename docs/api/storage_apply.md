# Incremental Storage Apply

`POST /storage/apply` accepts the [public resource manifest](../runtime/startup/metadata_bundle.md).
It creates, updates, or explicitly deletes declared resources and retains absent
resources. Format version 1 accepts an optional `deletions` object.
Use `POST /storage/import` for full-snapshot replacement.

## Requests

Send either `application/json` with a manifest, or `multipart/form-data` with
exactly one non-empty `file` field whose filename ends in `.zip`. ZIP contents
are `manifest.json`, `schemas/<type>/<name>/`, and `wasm_files/<sha256>.wasm`.
The optional query parameter `cascade` defaults to `false` for both
JSON and ZIP. It controls cascading refresh of unsubmitted dependent resources
and reconstruction of existing pipelines. Explicitly submitted newer resources
are applied directly; submitted newer pipelines use their incoming `run_state`.

The manifest limit is 8 MiB for both JSON and ZIP.
The compressed file limit is 512 MiB; the multipart body limit is 513 MiB.
ZIPs permit at most 4096 entries and 512 MiB of total uncompressed data; unsafe
paths, duplicate paths, symlinks, special files, and inconsistent sizes fail.

JSON can carry inline schemas. File-backed schemas must reference sources already
installed for that schema identity; WASM hashes must name existing managed files.
New attachments require ZIP. JSON never interprets arbitrary server file paths
as uploaded attachments. ZIP and JSON use the same definitions and revision rules.

```sh
curl -H 'Content-Type: application/json' \
  --data-binary @manifest.json http://127.0.0.1:8080/storage/apply
curl -F 'file=@deployment.zip' 'http://127.0.0.1:8080/storage/apply?cascade=true'
```

`resources`, `deletions`, and each of their seven arrays can be omitted; omitted
collections are empty. `null` is invalid. Each resource or deletion requires a
revision in `1..=9007199254740991`.
`bundle_version` identifies the producer artifact and is echoed in the result;
it does not order changes or provide a separate online deduplication key.
The producer must not assign different contents to the same bundle identity.

## Configuration Commit

The manager normalizes definitions, rejects duplicate identities, and builds
an effective view from persisted resources plus accepted upserts minus accepted
deletions. An identity cannot appear in both operations or more than once. Resource ordering
in the arrays does not determine dependency ordering.

- A greater revision creates, replaces, or deletes the resource. Deletion records
  a persistent tombstone even when the resource does not exist. A newer upsert
  recreates the resource and removes its tombstone.
- An equal-revision delete of a tombstone is idempotent. An equal-revision upsert
  of a tombstone, or delete of a live resource, is `409 revision_conflict`.
- Retained references to a deleted identity reject the batch with
  `409 resource_referenced`. Delete the owners in the same batch or submit newer
  owner definitions that remove those references.
- A lower revision rejects the entire batch with `409 revision_conflict`.
- Equal revision and equal normalized deployment content is idempotent.
- Equal revision with different content rejects the entire batch with `409`.
  For UDFs, this includes a different declared WASM hash. Uploaded bytes that do
  not match the declared/accepted hash are invalid attachments and return `400`.
- Missing dependencies, invalid schemas/WASM, invalid SQL or planning errors
  in submitted pipelines reject the entire batch before runtime installation.
- If stored SQL cannot be parsed while identifying affected pipelines, the
  manager logs a warning and uses actual runtime source bindings. Without known
  bindings, source changes conservatively include that pipeline in the affected
  scope. This does not reject an unrelated configuration commit.
- Only submitted pipelines are planned before commit. An upstream update is
  not rejected because an unsubmitted pipeline might become incompatible.

The latest pipeline deployment `run_state` is stored in the pipeline definition,
separately from operational desired state. Omitted `run_state` means `Stopped`.
A greater revision applies that declaration; an idempotent replay does not undo
manual start/stop, including after restart. Definitions without an accepted
deployment `run_state` use `Stopped` for this comparison.

A metadata transaction commits the complete selected delta, pipeline deployment
and operational states, and the attachment commit marker together. Ordinary HTTP
mutations and scheduler patrol coordinate with Apply/import/export and revisioned
DELETE. A busy HTTP mutation
returns `409`; ordinary mutations otherwise retain their per-resource locks
and concurrency. Scheduler patrol evaluates pipelines without this boundary when
no lifecycle operation is needed. Before a scheduled start/stop it waits for a
shared permit, rereads the latest configuration and operational intent, and
reevaluates the current window. It holds the permit only for that pipeline's
operation, not the complete scan. A pending operation resumes after the boundary
without waiting for another patrol interval; a configuration change after an idle
check is handled by a later scan. Elapsed windows are not replayed, and long-held
boundaries can still delay scheduled starts and stops. Missed patrol ticks are
skipped instead of triggering a burst of catch-up scans.
Apply and import acquire the boundary after receiving uploads.
Ordinary GET queries remain available during Apply, but runtime queries can
observe intermediate reconstruction states, including temporarily absent
pipelines. Storage export acquires the exclusive boundary to read consistent
metadata and attachments and can return `409 metadata_busy`. It also performs
attachment recovery when needed. This is a manager HTTP boundary, not a lock for
external code directly using the storage crate.

## Runtime Application And Recovery

After commit the manager updates explicitly submitted newer resources.

- With `cascade=false` (the default), unsubmitted stream/table definitions
  and pipeline graphs retain their runtime state and resolved dependencies.
  Updating a named schema does not refresh its existing consumers. New or
  explicitly updated streams/tables resolve the accepted named schema. New
  pipelines use their bound streams' schemas; a retained shared runtime supplies
  its previously resolved version.
  A submitted identical resource remains idempotent.
- With `cascade=true`, the manager collects affected existing pipelines,
  stops them once, applies the complete dependency batch including refresh of
  retained dependent streams/tables, then rebuilds the pipelines and
  restores their persisted operational desired state. UDF changes participate in
  this cascading reconstruction.
- Submitted newer pipelines are installed according to their incoming `run_state`
  in either mode. This direct replacement is independent of the cascade option.

Multiple schema versions can intentionally coexist after a non-cascading update.
Converge consumers by submitting a newer schema revision with `cascade=true`, or
by restarting the manager. Same-revision upsert replay remains observation-only.

Runtime objects that cannot be safely replaced while retained pipelines use them
are deferred. In particular, a shared MQTT client or shared stream used by a
pipeline outside the update keeps its old runtime; the accepted new resource
reports `runtime_applied: false` with a deferral reason. The check uses MQTT keys
bound to existing processor graphs, including bindings older than the catalog.
Sources depending on a deferred MQTT client are also left unchanged. Submitted
newer pipelines with unavailable runtime dependencies report failure instead of
being installed against the old configuration.

Selected pipelines are stopped before connector replacement. A stop failure
prevents runtime replacement and is reported without rolling back metadata.
Runtime updates are not atomic across resources or instances: one instance can
accept a replacement before another fails. Other runtime failures are reported
per resource or dependency; accepted metadata is not rolled back. Correct the
cause and use the recovery actions below to converge runtime to that metadata.
Connector reconnection keeps its existing behavior. Scheduled Running intent is governed
by the schedule window and patrol; it does not force execution outside a window.

A repeated same-revision upsert preserves manual start/stop and does not reinstall
or restart resources, even when replay adds `cascade=true`. Same-revision deletions
retry cleanup of the deleted resources; this does not restart idempotent upserts. To execute
cascading reconstruction after a deferred update, submit a newer revision with
that option or restart the manager. Its runtime results come from current observable state,
not a persisted history of previous attempts. Schema versions, stream resource
and bound schema revisions, memory topic kind/capacity, shared MQTT client
configuration, and pipeline definition revisions
can be inspected. A healthy retained pipeline's revision alone does not establish
whether it was rebound after an upstream change. Table resource revisions
and loaded UDF hashes are not exposed by the current runtime APIs. Results that
cannot be confirmed are `null`, rather than success or failure.

Runtime failures are reported for the current attempt and are not persisted.
Restart hydration retries the committed configuration according to operational
desired state; a previous Apply failure does not block the retry. A manually
stopped pipeline stays stopped. Within the current process, existing failed
pipeline lifecycle behavior still applies: explicit
`POST /pipelines/<id>/start` retries a failed or absent runtime from the committed
definition, and scheduled starts remain governed by patrol and the active window.

For a failed global resource, correct the cause and restart the manager, or
submit a corrected higher revision. In particular, an existing memory topic's
kind/capacity cannot be changed in place: the new metadata is committed, online
redeclaring it reports failure, and restarting constructs the new topic. A replay
compares the current topic configuration without attempting replacement.

## Response And Errors

A processed configuration commit returns `200`, including runtime failure:

```json
{
  "bundle_version": "deployment-002",
  "metadata_committed": true,
  "applied_to_runtime": false,
  "resources": [{
    "kind": "memory_topics", "identity": "frames",
    "incoming_revision": 2, "current_revision": 1,
    "operation": "upsert", "result": "updated",
    "runtime_applied": false, "error": "topic capacity mismatch"
  }],
  "affected_resources": []
}
```

`result` is `created`, `updated`, `deleted`, or `idempotent`. `operation` is
`upsert` or `delete`. With `cascade=false`,
`affected_resources` is empty. With `cascade=true`, it reports
runtime rebinding of retained resources as `rebound`, or their observed state
as `current` on replay; neither is another metadata update.
`runtime_applied` is `true` for confirmed application, `false` for confirmed
failure or an unavailable/mismatched runtime, and `null` when this request did
not apply the resource and current runtime information cannot confirm application.
The aggregate `applied_to_runtime` is `false` if any resource reports `false`,
`true` if every resource reports `true`, and otherwise `null`.
An empty resource batch makes no changes and returns `applied_to_runtime: true`.
The result describes this attempt and current observations, not a permanent health guarantee.
An idempotent-only request reports `metadata_committed: true` to acknowledge the
already accepted configuration; it does not issue another metadata transaction.

Precommit failures return JSON such as:

```json
{
  "metadata_committed": false, "applied_to_runtime": false,
  "error": {"code": "revision_conflict", "message": "older_revision: streams telemetry: incoming 3, current 4"}
}
```

| Status | Codes/meaning | Committed configuration |
| --- | --- | --- |
| 400 | `invalid_manifest`, `invalid_upload`, `invalid_archive`, `invalid_resources` | None of this batch |
| 409 | `revision_conflict`, `resource_referenced`, `metadata_busy` | None of this batch |
| 413 | Body/upload size exceeded | None of this batch |
| 415 | `unsupported_media_type` | None of this batch |
| 500 | `preparation_failed`, `commit_failed`, `attachment_recovery_failed` | None of this batch; recovery errors can concern an earlier committed publication |
| 200 | Configuration accepted; inspect runtime fields | Entire selected delta, or already accepted idempotent declarations |

Errors identify the first failing resource in the message; this API does not
return an exhaustive list of every static error. Preparation/commit failures
include rollback failures in the message. Metadata not committed does not mean
all filesystem effects were successfully undone.

Attachments use a durable publication journal and a transaction id committed
with metadata. Recovery rolls back uncommitted publication or cleans up a
committed publication. A rollback failure preserves the journal/backups and
blocks ordinary manager mutations with `409 attachment_recovery_required` until
a storage request or startup recovers it. Apply performs recovery itself; its
recovery failure returns `500 attachment_recovery_failed`, not that ordinary
mutation error.
Cleanup after a committed publication logs a warning and does not turn it into
an uncommitted response. Upsert-only Apply retains unreferenced WASM files.
Explicit UDF deletion and import reclaim unreferenced managed WASM files. Schema hydration reconciles source trees
with the committed schema identities.

A timeout, disconnect, or lost response does not establish whether metadata
committed. Replay the same declaration to re-observe current runtime application;
upsert runtime recovery still requires an explicit action; deletion replay retries cleanup. There is no exactly-once guarantee
or Apply background retry loop. See the [three-entry error comparison](storage_errors.md).

## Revisioned Deletion

```json
{
  "format_version": 1,
  "bundle_version": "remove-telemetry",
  "deletions": {
    "pipelines": [{"id": "telemetry_pipeline", "revision": 12}],
    "streams": [{"name": "telemetry", "revision": 8}]
  }
}
```

The manager validates the final dependency graph before committing live-record
removal, pipeline operational intent removal, and tombstones in one transaction.
After stopping selected pipeline replacements, cleanup runs in reverse dependency
order: pipelines, streams/tables, connectors/topics/UDFs, then schemas. Metadata
is not rolled back if runtime or file cleanup fails; inspect `runtime_applied`
and `error`, then replay the same deletion to retry. A late older deletion cannot
remove a resource recreated at a newer revision.

Cleanup uses the ordinary DELETE behavior: pipeline checkpoints and managed
schema attachments are removed; unreferenced managed WASM files are reclaimed.
Business data and user uploads are retained. UDF DELETE retains the registered
runtime function until restart; Apply reports this as `runtime_applied: false`.
Tombstones survive restart and are not automatically garbage-collected.
The `cascade` option controls updates; deletion never implicitly deletes retained
owners. See [single-resource deletion](storage_delete.md).
