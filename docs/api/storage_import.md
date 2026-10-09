# Full Storage Import

`POST /storage/import` replaces the complete persisted resource snapshot.
See the
[request and response reference](../../user_docs/api/import.md).

Import accepts exactly one multipart ZIP `file`; JSON and raw `application/zip`
requests are unsupported. It validates the manifest, duplicate identities,
revisions, dependency presence, schemas and WASM before publishing attachments.
It does not compare revisions with storage: an old snapshot can replace newer
configuration, and absent resources are removed. Repeating an import performs
another replacement, rather than the idempotent Apply protocol.

Success is `200` with `applied_to_runtime: false`, `imported_resource_counts`,
and `previous_resources`. Import refreshes the process-wide named schema store,
but does not rebuild or start pipelines, stop removed runtime resources, or
reconcile global connectors. Restart is required for complete runtime hydration.
Deployment declarations and operational desired states are restored from inline
pipeline `run_state`. Import completion does not set the init bundle completion record.

| Failure | Status | Effect and recovery |
| --- | --- | --- |
| Unknown/missing/repeated `file`, non-ZIP filename, malformed ZIP, invalid manifest/resource | 400 | No metadata replacement; correct the artifact |
| Malformed multipart/body | Framework status, normally 400; size rejection 413 | No metadata replacement |
| Uploaded ZIP over 512 MiB or multipart body over 513 MiB | 413 | No metadata replacement |
| Non-multipart request | 415 | No metadata replacement |
| Concurrent manager metadata command, import/export operation | 409 | No metadata replacement; retry after completion |
| Temporary directory/staging I/O, attachment publication, old-resource read, metadata transaction failure | 500 | No metadata replacement; publication attempts rollback |
| Attachment recovery failure | 500 | Preserve journal and backups; an earlier publication may already have committed |
| Unreferenced WASM cleanup failure after commit | 200 | Replacement remains committed; warning in logs |

The previous snapshot is read before publishing files. Publication is shared
with init/Apply: it journals same-filesystem schema backup/activation and newly
installed WASM files, then commits metadata and the publication id in one
transaction. On failure, recovery removes new WASM files and restores old schema
sources. Rollback errors are appended to the error response and recovery data
is retained. It is incorrect to interpret a `500` as proof that every filesystem
side effect was undone.

At startup, the journal distinguishes an uncommitted publication to roll back
from a committed publication to finish cleaning up. Committed cleanup failures
are warnings. Import's subsequent unreferenced-WASM cleanup is also best effort;
it does not erase the successful metadata commit. HTTP failure bodies from the
import handler remain text; metadata coordination errors use structured JSON.

Example: import successfully commits stream `new_stream`, then an old unused
WASM file cannot be removed. The response remains `200`, with counts for the
new snapshot and `applied_to_runtime: false`; logs contain the cleanup warning.
The caller must not repeat import under the assumption that configuration failed.

If the response is lost, query/export storage or repeat the complete replacement
with awareness that it replaces current configuration again. Runtime success
and incremental revision ordering are not part of this endpoint's contract.
