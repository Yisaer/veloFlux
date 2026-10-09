# Init, Apply, And Import Failure Contracts

All three entrances use the [resource directory](../runtime/startup/metadata_bundle.md),
shared revision selection where applicable, validation and attachment publication.
Their acceptance policies remain different.

| Event | Init | Online Apply | Import |
| --- | --- | --- | --- |
| Bad envelope/version/bundle identity | Skip package, log, continue startup | Reject whole request | Reject whole snapshot |
| Bad resource/revision | Fail entry; independent valid entries continue | Reject whole batch | Reject whole snapshot |
| Duplicate identity | Fail all entries of that identity | Reject whole batch | Reject whole snapshot |
| Lower revision | Ignore, retain current record | 409; nothing commits | Replace without comparing old revisions |
| Equal revision | Ignore without comparing content | Delete of a tombstone is idempotent; switching live/deleted operation is 409; equal normalized declaration is idempotent; changed declaration/hash is 409; attachment bytes inconsistent with the declared hash are 400 | Full replacement |
| Retained reference to deletion | Reject that deletion and preserve dependency | 409; reject whole batch | Reject inconsistent snapshot |
| Deletion cleanup fails after commit | Keep tombstone, leave package incomplete | 200 with failed cleanup result; same delete can retry | Storage-only |
| Dependency fails | Use retained valid dependency where available; fail unsatisfied dependants | Reject batch before commit | Validate only snapshot dependencies |
| Invalid submitted SQL/schema/WASM | Fail entry and unsatisfied dependants | Reject whole batch | Reject snapshot for static import validation |
| Upstream update breaks retained pipeline | No additional retained-pipeline planning before commit | Commit; report failed runtime rebind | Old absent pipelines are removed from metadata |
| Publication/metadata commit fails | Selected subset does not commit; log rollback result; continue | Batch does not commit; report rollback result | Snapshot does not replace; report rollback result |
| Runtime build/start fails after commit | Hydration logs errors; committed configuration stays | 200 with false runtime result; configuration stays | No online runtime reconciliation |
| Committed attachment cleanup fails | Keep commit, warning | Keep commit, warning | Keep commit, warning |
| Partial processing | Do not record bundle completion | No static partial commit | No partial snapshot commit |
| Retry | Next startup skips accepted revisions and retries unresolved entries and matching deletion cleanup | Same upsert observes without runtime retry; same delete retries cleanup | Replaces full snapshot again |

Apply reports a failed attachment recovery as `500 attachment_recovery_failed`.
An unresolved journal blocks ordinary manager mutations with
`409 attachment_recovery_required`; Apply performs recovery itself.

Attachment journal recovery runs before init. If recovery cannot establish a
usable file state, startup reports an error rather than hydrating against an
unresolved publication. Init resource/package errors themselves remain
best effort; unrelated storage corruption or unavailable base directories do
not gain a guarantee that the complete process can start.

## Init Partial Success

The directory contains new memory topic `frames` at revision 2 and pipeline
`bad` with malformed SQL. The topic commits; the pipeline fails validation and
its old definition, if any, stays. Startup continues into hydration. Structured
logs identify `bad` and its failure; the init bundle is not marked complete.
Next startup ignores the accepted topic revision and retries `bad`. Fixing the
SQL requires a valid artifact; no current-process polling repairs it.

## Online Precommit Failure

One batch contains stream `s`, malformed pipeline `p`, and independent valid
pipeline `q`. Parsing/planning `p` returns `400 invalid_resources`. No resource
from the batch is committed or installed into the current runtime. Correct the
manifest and resubmit. A stale revision or equal-revision content conflict
similarly returns `409` and prevents every other batch entry from committing.

## Online Postcommit Runtime Failure

A topic `frames` already runs with capacity 16. Apply accepts revision 2 with
capacity 32. Metadata commits revision 2, but the runtime registry cannot change
an existing topic's capacity in place. The response is `200`, with
`metadata_committed: true` and `runtime_applied: false`. With
`cascade=true`, pipelines selected for reconstruction also report
unavailable dependencies; by default unsubmitted pipelines keep running. Replaying revision 2 observes the current topic
configuration without restarting resources. Restart reconstructs global resources
and retries desired-running pipelines from their committed definitions. Manual
pipeline start can also retry a failed or absent runtime in the current process.

With `cascade=true`, a retained pipeline can also fail after a valid
schema update removes one of its
columns. Its metadata and operational intent remain; the new schema is not
rolled back. Correct the pipeline with a greater revision, or repair the cause
and explicitly retry start. This differs from invalid SQL in a submitted pipeline,
which is detected before metadata commit.

## Import Commit Followed By Cleanup Failure

The complete ZIP validates and replaces metadata. Removing an old unused WASM
file then fails due to a filesystem permission error. Import returns `200`, its
resource counts, previous resources, and `applied_to_runtime: false`; the cleanup
error is a warning in logs. The configuration has been replaced. Import does not
attempt online pipeline hydration, and replay would replace the snapshot again.

See [Apply](storage_apply.md), [Import](storage_import.md), and
[init](../runtime/startup/init_process.md) for detailed fields and recovery paths.
