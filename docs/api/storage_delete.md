# Resource Deletion

Managed resource DELETE endpoints accept optional `?revision=N`:

| Resource | Endpoint |
| --- | --- |
| Pipeline | `/pipelines/:id` |
| Stream | `/streams/:name` |
| Table | `/tables/:name` |
| Schema | `/schemas/:name` |
| Shared MQTT client | `/mqtt/clients/:key` |
| Memory topic | `/memory/topics/:topic` |
| WASM UDF | `/udfs/:name` (requires `wasm_udf`) |

A revision must be an integer in `1..=9007199254740991`. Versioned deletion uses
[Apply](storage_apply.md)'s final dependency checks and atomic metadata commit.
A missing resource can be deleted: its tombstone prevents older upserts from
creating it. Lower revisions and an equal-revision delete of a live record are
409 conflicts. An equal-revision delete of a tombstone retries cleanup.
Only an upsert with a greater revision can recreate the resource.

Versioned success returns `200` with the Apply response: `metadata_committed`,
`applied_to_runtime`, and the per-resource deletion result. Cleanup failures
remain committed and appear in the runtime fields; they are not an HTTP proof
of rollback. A late older DELETE cannot delete a newer recreation.

Without a revision, existing endpoint response and deletion behavior apply.
Deleting a live resource does not create a tombstone or allocate a revision.
Deleting an already absent resource returns 404 and preserves any existing
marker. Old upserts can recreate an identity after an unversioned deletion;
a late unversioned DELETE can delete its new incarnation.

Unversioned memory topic deletion returns 409 while a publisher or subscriber
holds the topic in any instance, preserving its metadata and runtime declarations.
It checks all instances before cleanup, rechecks usage during removal, and removes
metadata only after runtime cleanup succeeds. If cleanup fails midway, it restores
previously removed declarations. A restoration failure returns 500 with details;
metadata remains available. Versioned deletion retains the committed-tombstone and
retryable-cleanup behavior described above.

Cleanup follows ordinary deletion: remove pipeline checkpoints and managed
schema attachments, reclaim unreferenced managed WASM, and retain business data
and user files. UDF runtime registration is retained until restart. Versioned
operations coordinate with other manager mutations and can return 409 when busy.
Tombstones are included in export, replaced by full import, and have no automatic
expiry. Full import of an older snapshot can intentionally reset this protection.
