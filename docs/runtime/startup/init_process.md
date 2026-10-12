# Startup Resource Directory

VeloFlux can apply an optional resource directory before runtime hydration:

```shell
veloflux --config ./config.yaml --data-dir ./data --init-dir ./init
```

The directory contains `manifest.json`, optional installed schema sources under
`schemas/`, and referenced WASM modules under `wasm_files/`. It must not be
inside `data_dir`. Startup reads the extracted directory directly; HTTP export
and import use the same directory inside a ZIP envelope.

## Bundle and Resource Revisions

`bundle_version` and `revision` have separate purposes:

- `bundle_version` is an opaque identity for one complete directory. VeloFlux
  compares it only for equality with the last completely applied init bundle.
- `revision` is a required positive JSON safe integer on every schema, UDF,
  memory topic, shared MQTT client, stream, table, and pipeline.
- Model entries are reserved for full storage import/export and are not supported by startup init directories. An init manifest may contain an empty `models` collection for format compatibility, but non-empty model entries make the init bundle incomplete and prevent its bundle version from advancing.
- A resource from init replaces the stored resource only when its revision is
  strictly greater. Equal and lower revisions are `ignored_not_newer` without
  comparing content.
- A stored resource absent from init remains stored. Explicit `deletions` can
  remove it and record a persistent revision watermark, including for absent
  resources. Equal/lower changes cannot resurrect a deleted identity.

The supported range is `1..=9007199254740991`. Timestamp revisions in seconds,
milliseconds, or microseconds fit this range. Producers are responsible for
monotonic assignment and clock rollback handling.

Pipeline revision covers its definition and inline `run_state`. The inline pipeline
`run_state` contains only the user's desired `Running` or `Stopped` intent. When an incoming
pipeline wins, both values apply. When it loses, both are ignored. Operational
start and stop requests change desired run state without changing revision.

## Best-Effort Algorithm

Init failures never abort process startup. The process continues from the usable
state in `data_dir`. This guarantee applies to init processing; unrelated
configuration errors or corrupted persisted metadata can still fail startup.

The manifest is parsed in two stages. First, VeloFlux parses the envelope and
resource arrays as raw JSON entries. If the envelope is unsafe or cannot be
parsed, the complete directory is skipped. Each entry is then decoded
independently. A missing or invalid revision therefore rejects only that entry.

Duplicate `(kind, identity)` entries are ambiguous. Every duplicate for that
identity is marked `failed_validation`; VeloFlux does not select the first,
last, or greatest entry. Other identities continue. An invalid entry without a
usable identity is reported by kind and array index.

Eligible entries are processed in dependency order:

```text
schemas / UDFs / memory topics / shared MQTT clients
                            |
                            v
                     streams / tables
                            |
                            v
                         pipelines
```

Validation uses the effective state: retained live resources plus incoming
resources that already succeeded. If an incoming dependency fails but a usable
live resource with the same identity remains, dependants use that live
resource. If no usable dependency exists, only the dependant is marked
`failed_dependency`.

A valid upstream replacement is not blocked by a stored downstream resource
that is absent from init. For example, a schema revision may remove a column
used by a retained pipeline. The schema is applied; if the retained pipeline
cannot be hydrated later, its runtime status exposes that failure.

The decision flow is equivalent to:

```text
parse envelope
if envelope invalid:
    log bundle failure and continue process startup

decode each raw entry
mark malformed and duplicate entries failed_validation

for entry in dependency order:
    if live revision >= incoming revision:
        keep live and mark ignored_not_newer
        continue
    validate entry against effective static dependencies
    prepare its managed files, if any
    if dependency unavailable:
        mark failed_dependency
    else if spec invalid:
        mark failed_validation
    else if file preparation fails:
        mark failed_install
    else:
        add entry to effective state and successful metadata subset
        mark created or updated

install the successful managed-file subset with rollback support
commit the successful metadata subset in one transaction
record bundle_version only when every entry succeeded or was ignored
continue process startup regardless of the init result
```

Logical application is partial, but the successful metadata subset is committed
in one storage transaction. A commit failure exposes none of that subset.
Managed schema and WASM files are staged under `<data_dir>/.init-staging/` and
use the shared durable attachment publication journal. Startup first recovers
an interrupted publication by its metadata transaction id.

## Static and Runtime Failures

Init checks static resource shape, identifiers, dependency presence, schemas,
SQL planning inputs, and managed files. Runtime availability is not an
application precondition. An unreachable broker, unavailable device, connector
start failure, or later processor failure does not roll back a statically valid
artifact. The resource, revision, and pipeline desired run state remain stored;
runtime status reports the real error.

Hydration and online Apply failures are held in the current runtime, separately
from persisted `run_state`. Startup hydration creates fresh runtime state from the
committed pipeline definition and its desired state. A desired-running pipeline is retried; a new failure records
its reason and timestamp in the current runtime.

Every resource result is one of:

- `created`
- `updated`
- `ignored_not_newer`
- `failed_validation`
- `failed_dependency`
- `failed_install`

Structured logs include `bundle_version`, `resource_kind`, `resource_id`,
`incoming_revision`, `current_revision`, `result`, and `reason` when available.

`bundle_version` advances only when every entry is `created`, `updated`, or
`ignored_not_newer`. On a partial static, dependency, file, or commit failure it
does not advance. At the next startup, previously successful entries are equal
and become `ignored_not_newer`, while failed entries are attempted again.
Runtime start failures do not prevent advancement because artifact application
already succeeded.

## Worked Examples

Two independent pipelines can have different outcomes. If pipeline A has
invalid SQL and pipeline B is valid, A is `failed_validation` and B is applied.

If incoming schema revision 2 is invalid while live schema revision 1 is usable,
the schema update fails but a downstream incoming stream may validate against
live revision 1. If no live schema exists, the stream is `failed_dependency`.

After a partial apply, a restart retries the same bundle. Resources committed on
the first attempt are now `ignored_not_newer`; failed resources are decoded,
validated, and prepared again.

If valid schema revision 2 removes a column used only by a retained pipeline,
the schema update is committed. The retained pipeline may later report a
hydration or planning failure; it does not roll back the schema.

If a pipeline and its `run_state: "Running"` are applied but its connector
cannot reach a broker, the pipeline artifact, revision, and desired state remain
stored. Runtime status reports the connector failure; failure is not rewritten into
the two-value desired state. On a later startup, hydration creates a fresh runtime
and retries pipelines whose desired state is `running`. A failure of that new
attempt records its own error in memory.

## Policy Boundary

Init and online Apply share revision selection, resource validation, metadata
submission, and attachment publication. Init deliberately retains best-effort
acceptance and does not add same-revision content comparison. Accepted independent
resources can therefore combine new and retained old configuration; dependency
presence does not guarantee business-level release consistency.

Online Apply rejects a batch if any submitted static configuration is invalid.
Neither entrance replans retained pipelines as a precondition for an upstream
update. No Apply background retry loop is introduced. See the
[three-entrance error examples](../../api/storage_errors.md).

## Revisioned Deletions

Init accepts the optional [manifest deletions](metadata_bundle.md#deletion-records).
It skips equal/lower revisions against either a live record or a tombstone.
Higher upserts recreate deleted identities. Duplicate upsert/delete identities
fail every entry of that identity; independent entries can still commit.

Deletion selection uses the actually accepted upserts and deletions. If deleting
or updating an owner fails, its retained definition protects its dependencies.
Rejected dependency deletion leaves both its live record and attachments intact.
The selected live-record changes and tombstones commit together before hydration.
Pipeline checkpoints, schema attachments, and unreferenced managed WASM files
are cleaned as with ordinary DELETE. Cleanup failure leaves the package
incomplete; the next startup retries cleanup only for the matching tombstone,
never for a newer recreated resource.
