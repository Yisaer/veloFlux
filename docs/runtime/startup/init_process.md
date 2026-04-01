# Init Process

## Background

veloFlux may load an optional `init.json` file from the configured data directory during process
startup.

This mechanism is intended for bootstrap-time metadata initialization. Its responsibility is limited
to writing resources into storage before the normal runtime hydration flow starts.

## Goals

- Allow operators to place an `init.json` file in the data directory for startup-time bootstrap.
- Avoid re-applying the same file when it has not changed.
- Fail fast when the bootstrap file conflicts with existing stored resources.

## Non-Goals

- Runtime hot reload of `init.json`.
- Partial apply with best-effort error recovery.
- Silent overwrite of existing resources with the same name.

## Startup Behavior

On startup, after storage is opened and before runtime resources are hydrated from storage, veloFlux
checks whether `<data_dir>/init.json` exists.

If the file does not exist:

- skip the init process
- continue normal startup

If the file exists:

1. Read the file's last modified time from the filesystem.
2. Read the persisted init-apply metadata from storage.
3. Compare the current file modified time with the stored metadata.

If the current file modified time is not newer than the stored metadata:

- skip the init process
- continue normal startup

If the current file modified time is newer than the stored metadata:

- execute the init apply flow once during this startup

## Apply Semantics

The init process is a storage write flow.

It does not introduce a separate runtime-only resource model. After the init apply succeeds, the
normal storage hydration flow is responsible for loading the stored resources into the runtime.

Before writing any resource from `init.json`, veloFlux performs a duplicate-name check against the
existing storage state.

If any resource in `init.json` has the same identity as an existing stored resource of the same
kind, startup must fail immediately.

Examples include:

- a stream with the same name
- a pipeline with the same id
- a shared MQTT client config with the same key
- a memory topic with the same topic name

The init process does not overwrite or merge existing resources.

## Apply Metadata

After a successful init apply, veloFlux persists init-apply metadata in storage.

The metadata includes at least:

- `last_applied_at`
- `last_init_json_modified_time`

`last_init_json_modified_time` is the field used for the startup skip/apply decision.

`last_applied_at` is retained for observability and troubleshooting.

## Failure Semantics

The init process is strict.

If `init.json` is selected for apply and any step fails, veloFlux startup must fail.

Examples include:

- the file cannot be read
- the file content is invalid
- duplicate-name conflicts are detected before apply
- a storage write fails

When apply fails:

- the init-apply metadata must not be advanced
- the process exits instead of continuing with partial bootstrap state

## Update Behavior

The init process is driven by file modification time.

If `init.json` is updated before the next veloFlux startup, and its modified time becomes newer
than `last_init_json_modified_time`, veloFlux attempts the init apply flow again during that
startup.

If `init.json` is unchanged, veloFlux skips the init apply flow.

Because duplicate-name conflicts are fatal, updating `init.json` does not imply in-place update of
existing resources with the same identity. Such a file still fails the duplicate-name check and
causes startup failure.
