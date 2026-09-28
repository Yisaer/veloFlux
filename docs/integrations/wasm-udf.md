# WASM UDF Design

## Overview

This document describes the design for user-defined functions (UDFs) executed inside a
WebAssembly (WASM) sandbox. Users compile their logic into `.wasm` files, upload them
through the manager API, and reference them directly in SQL (e.g. `SELECT my_udf(col) FROM stream`).

At startup, all persisted WASM UDFs are loaded into the `CustomFuncRegistry`, so they are
available to the flow planner and all processor pipelines without additional configuration.

### Design assumptions

- **Single-process deployment only.** All `FlowInstance`s run in the same OS process as
the manager. There is no remote worker mode. This means wasm files and metadata are
always locally accessible; no file distribution or remote synchronization is needed.
- **Trusted UDF source.** Uploaded WASM modules are trusted application artifacts. The
runtime provides sandboxing and bounded linear memory, but does not attempt to recover
from arbitrary hostile module state after an interrupted call.
- **Upload hot-registers for new pipelines.** Uploaded UDFs are persisted to storage and
immediately registered into the running `CustomFuncRegistry` via `clone_and_add`. New
pipelines created after upload can reference the UDF in SQL. Startup reloads all
persisted UDFs before restoring pipelines.
- **DELETE is restart-only.** Deleting a UDF removes it from persistent storage but the
function remains registered in the running instance until the next restart.
Hot-unregister is deferred to future work.

## Architecture

```
                          ┌─────────────────────────┐
                          │   Manager REST API       │
                          │                          │
                          │ POST   /udfs/upload      │
                          │ GET    /udfs             │
                          │ GET    /udfs/:name       │
                          │ DELETE /udfs/:name       │
                          └──────────┬───────────────┘
                                     │
                    ┌────────────────┼────────────────┐
                    │                │                 │
                    ▼                ▼                 ▼
           ┌──────────────┐ ┌──────────────┐ ┌──────────────────┐
           │  wasm_files/ │ │  metadata    │ │  export/import   │
           │  (on-disk)   │ │  (redb)      │ │  (bundle json)   │
           └──────┬───────┘ └──────┬───────┘ └──────────────────┘
                  │                │
                  │    ┌───────────┘
                  │    │
                  ▼    ▼
        ┌─────────────────────────┐
        │   Startup / Bootstrap    │
        │                         │
        │ 1. List StoredUdf from  │
        │    metadata redb         │
        │ 2. Load .wasm from disk │
        │ 3. Instantiate via      │
        │    wasmtime             │
        │ 4. Register into        │
        │    CustomFuncRegistry   │
        └───────────┬─────────────┘
                    │
                    ▼
        ┌─────────────────────────┐
        │   Flow Module           │
        │                         │
        │  ScalarExpr / Physical │
        │  UDF call definition   │◀── immutable UDF definition
        │  args: [...]           │
        │                         │
        │  Processor creates      │
        │  processor-owned runtime│
        └─────────────────────────┘
```

### Key design decisions

1. **WASM UDFs integrate with the scalar expression system.** The registry stores a
   shareable UDF definition, while the planner materializes batch-capable UDF subexpressions
   into temporary columns. Processor-owned runtimes execute those columns without changing
   the scalar semantics visible to SQL.

2. **WASM runtime: wasmtime.** wasmtime is a pure-Rust, production-grade WASM runtime
   with JIT compilation. It provides sandbox isolation, epoch-based interruption for
   execution-timeout protection, and a safe Rust API (no `unsafe` in call sites).

3. **WASM files stored on-disk, metadata in redb.** The WASM module binary is written to
   a `wasm_files/` directory under `base_dir`. A `udfs` table in `MetadataStorage` holds
   the serialized `StoredUdf` record (name, revision, file hash, metadata). This follows the same
   pattern as existing persisted resources (streams, pipelines, mqtt configs, memory topics).

4. **Explicit execution mode.** Each WASM UDF declares exactly one execution mode in
   metadata: `row` or `batch`. Row-mode UDFs use the row JSON ABI; batch-mode UDFs use
   the vectorized batch JSON ABI and process multiple rows in one call. The host does
   not infer or switch modes from the exports.

## Storage Design

### New storage table

Add a `udfs` table to `MetadataStorage` alongside the existing tables:

```rust
// In src/storage/src/lib.rs

const UDFS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("udfs");
```

### StoredUdf record

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredUdf {
    /// Canonical function name (case-insensitive in SQL, stored lowercase).
    pub name: String,
    pub revision: u64,
    /// SHA-256 hex digest of the WASM file contents.
    pub wasm_sha256: String,
    /// Original upload metadata serialized without identity revision.
    pub raw_json: String,
}
```

The `raw_json` field holds a `UdfUploadRequest` as JSON — the same struct accepted by
the upload API. This keeps import/export semantics consistent with streams and pipelines.

### On-disk WASM files

WASM binaries are stored under `<base_dir>/wasm_files/<sha256>.wasm`.

**Why SHA-256 content-addressable storage?**

1. **Deduplication.** If the same `.wasm` binary is uploaded twice (e.g. two different
   UDF names wrapping the same logic, or a re-upload by mistake), only one copy exists
   on disk. The `StoredUdf` records both point to the same file via the hash.
2. **Integrity verification on load.** At startup, the system re-hashes the file on disk
   and compares it to the stored `wasm_sha256`. This detects silent disk corruption or
   tampering before the module is instantiated.
3. **Safe GC on delete.** When a UDF is deleted, the system checks if any other
   `StoredUdf` references the same SHA-256. The on-disk file is only removed when the
   reference count drops to zero.

## Manager API

### Routes

| Method   | Path              | Description                        |
|----------|-------------------|------------------------------------|
| `POST`   | `/udfs/upload`    | Upload a new WASM UDF              |
| `GET`    | `/udfs`           | List all registered UDFs           |
| `GET`    | `/udfs/:name`     | Get UDF metadata                   |
| `DELETE` | `/udfs/:name`     | Delete a UDF                       |

### POST `/udfs/upload`

Request: `multipart/form-data`
- Field `name` (text): canonical function name (e.g. `my_udf`). The name is
  canonicalized to lowercase, then must match the resource-id grammar
  `` `[A-Za-z][A-Za-z0-9_]{0,127}` `` (a letter followed by letters, digits, or
  underscores). `GET`/`DELETE /udfs/:name` apply the same lowercase
  canonicalization before lookup. Invalid names return `400 Bad Request`.
- Field `revision` (text): positive JSON-safe resource revision.
- Field `wasm_file` (binary): the `.wasm` file

Response `200 OK`:
```json
{
  "abi_version": 1,
  "name": "my_udf",
  "revision": 1,
  "wasm_sha256": "a1b2c3...",
  "status": "registered",
  "metadata": {
    "abi_version": 1,
    "description": "optional description from wasm metadata",
    "args": [{"name": "input", "type": "string"}],
    "return_type": "int64",
    "execution_mode": "row"
  }
}
```

Server-side behaviour:
1. Compute SHA-256 of the WASM binary.
2. Check for name collision with built-in functions and existing UDFs; return
   `409 Conflict` if already registered.
3. Validate the WASM module: instantiate it with wasmtime, validate the metadata JSON,
   verify the row execution export (`udf_row`) together with `udf_metadata_ptr`,
   `udf_metadata_len`, and `memory`, verify `abi_version == 1`, and verify the memory
   cap. Batch metadata is reserved for the later batch execution phase and is rejected
   by the current row-only implementation.
4. Write `<sha256>.wasm` to `wasm_files/`.
5. Insert `StoredUdf` into the `udfs` redb table.
6. Instantiate and hot-register the UDF into the running `CustomFuncRegistry` via
   `clone_and_add`. New pipelines created after this point can reference the UDF.
   Startup reload remains the recovery path after restart.
   If hot-registration fails, the response includes `status: "persisted (hot-register
   failed: ...)"` — the UDF is stored but not yet callable.

### GET `/udfs`

Returns a list of all stored UDFs with their metadata (excluding WASM binary).

### GET `/udfs/:name`

Returns the full metadata for a single UDF.

### DELETE `/udfs/:name`

1. Remove the `StoredUdf` record from redb.
2. Garbage-collect the WASM file on disk if no other UDF references the same SHA-256.

The UDF remains registered in the running `CustomFuncRegistry` until the next
restart. Hot-unregister and pipeline-reference checking are deferred to future
work; for now, only persist a new pipeline if you intend to keep the UDF.

### Integration with import/export

Export produces a **ZIP archive** containing:

```
veloflux-export-<timestamp>.zip
├── manifest.json          # ResourceManifestV1 (schemas, streams, tables, pipelines, mqtt, memory_topics, udfs)
└── wasm_files/            # WASM UDF binaries (one .wasm per function)
    ├── <sha256_1>.wasm
    └── <sha256_2>.wasm
```

**Why an archive instead of base64-inline JSON?**

- Large WASM binaries don't bloat the JSON payload.
- The archive is a single file, easy to transfer and archive.
- Each `.wasm` file can be independently SHA-256 verified against the stored hash.
- Users can unpack, inspect, and selectively modify before re-importing.

**Export flow**

1. Serialize `ResourceManifestV1` to `manifest.json`. The `udfs` field contains
   `ExportUdf` records with required `name`, `revision`, and `wasm_sha256` (not
   the binary).
2. Copy referenced `.wasm` files from `<base_dir>/wasm_files/` into the archive's
   `wasm_files/` directory.
3. Return as `application/zip` with `Content-Disposition: attachment`.

**Import flow**

1. Safely extract the ZIP to a temporary directory.
2. Parse and validate `manifest.json` (streams, pipelines, etc.).
3. For each UDF in the `udfs` list:
   - Require `revision` in `1..=9007199254740991`.
   - Require `<sha256>.wasm` to exist in the archive.
   - Recomputed SHA-256 and compare to declared value.
   - Validate the WASM module via `WasmEngine::validate`.
   - Confirm metadata name matches the declared name.
4. Copy `.wasm` files to `<base_dir>/wasm_files/` (skip if already present).
5. Apply the `MetadataExportSnapshot` to redb.

**Current behavior**

| Method | Path              | Content-Type       |
|--------|-------------------|--------------------|
| `GET`  | `/storage/export?bundle_version=<version>` | `application/zip` |
| `POST` | `/import`         | `multipart/form-data` with a ZIP `file` field |

The previous JSON-only format is no longer supported.

`POST /udfs/upload` requires a multipart `revision` text field. UDF get/list
responses and export include the persisted revision. UDF create remains
create-only; startup init is the entry point that compares UDF revisions.

## WASM Runtime Integration (wasmtime)

### Crate structure

A new crate `src/udf/` (sibling to `src/flow/`, `src/storage/`, `src/manager/`):

```
src/udf/
  Cargo.toml
  src/
    lib.rs          # public API
    wasm_func.rs    # WasmUdfDefinition and WasmUdfEvaluator
    engine.rs       # WasmEngine (wasmtime Engine + Linker pool)
    abi.rs          # WASM <-> Value type mapping
```

### WASM ABI specification

Each UDF `.wasm` file must export the metadata functions and exactly one execution
function selected by `execution_mode`:

```wat
(module
  ;; For execution_mode: "row"
  ;; Parameters: i32 pointer to input JSON bytes, i32 byte length
  ;; Returns: i32 pointer to output JSON bytes. The 4-byte little-endian output length
  ;; is stored immediately before the returned data pointer.
  (func (export "udf_row") (param i32 i32) (result i32))

  ;; For execution_mode: "batch", use this instead of udf_row.
  ;; The JSON input contains column-oriented argument vectors.
  ;; The JSON output contains one result value per input row.
  ;; (func (export "udf_batch") (param i32 i32) (result i32))

  ;; Required memory export
  (memory (export "memory") 1)

  ;; Allocate memory from the WASM linear memory (host-provided)
  (import "host" "alloc" (func (param i32) (result i32)))

  ;; Metadata export (required): returns a pointer+length to JSON metadata blob
  (func (export "udf_metadata_ptr") (result i32))
  (func (export "udf_metadata_len") (result i32))
)
```

**Why JSON as the ABI wire format?**

The parameters and return value are serialized as JSON strings. This is a deliberate
trade-off between performance and interoperability:

1. **Universal compatibility.** JSON serialization is available in every language that
   compiles to WASM (Rust via `serde_json`, C via `cJSON`, Go via `encoding/json`,
   AssemblyScript via built-in `JSON`). A struct-based C ABI would require per-language
   binding generators and struggle with dynamic types like `Value::List` or `Value::Struct`.
2. **Matches veloFlux's type system.** The `Value` enum already has round-trip JSON
   conversion (`value_to_json_value` / `json_value_to_value` in `helpers.rs`). The host
   side of the ABI is essentially `serde_json::to_vec(&args) -> call wasm ->
   serde_json::from_slice(&result)`.
3. **Simplifies versioning.** Adding new types (e.g. a `Interval` type in the future)
   does not break the ABI. The JSON schema simply gains a new variant.
4. **Batch amortization.** Row-level JSON conversion is avoided on the selected hot path.
   The host sends column-oriented batches such as

   ```json
   {
     "row_count": 3,
     "args": [[1, 2, 3], [10, 20, 30]]
   }
   ```

   and receives a result vector such as `{ "values": [11, 22, 33] }`. The UDF remains
   scalar from SQL's perspective: output element `i` corresponds to input row `i`.

If a specific UDF is found to be JSON-bound (i.e. the serialization cost dominates),
a future optimization could add a raw pointer-based ABI variant (`udf_row_raw`) that
passes typed arrays. But this adds complexity and should be driven by profiling data,
not premature optimization.

### WASM UDF definition and processor-bound function

The registry must not store a mutable `Store` or `Instance` behind the existing
`Arc<dyn CustomFunc>` interface. The target design separates immutable definitions from
processor-bound implementations:

```rust
// Shareable and stored in the global UDF registry.
pub struct WasmUdfDefinition {
    name: String,
    metadata: UdfMetadata,
    execution_mode: ExecutionMode, // Row or Batch
    engine: wasmtime::Engine,
    module: wasmtime::Module,
    instance_pre: wasmtime::InstancePre<()>,
}

// Created during processor construction and never placed in the global registry.
pub struct BoundWasmUdf {
    store: wasmtime::Store<()>,
    instance: wasmtime::Instance,
    memory: wasmtime::Memory,
    entrypoint: WasmEntrypoint,
}
```

`WasmUdfDefinition` is referenced by the physical plan through a stable UDF identity.
When a processor is built, it replaces each WASM definition in its
physical expressions with a private processor-bound function. That function owns the
mutable wasmtime state and is never shared between pipelines or workers. Evaluation then
uses the same function call path as built-ins; it does not pass a runtime parameter.

Built-in functions continue to use the existing immutable `CustomFunc` path. WASM calls
are bound to their processor-owned runtime during physical processor construction, so
`WasmUdf::eval_row` is not an execution path.

### Definition and runtime ownership

A single `wasmtime::Engine` is shared across the process, and compiled module definitions
are shared by all pipelines. Execution state must not be shared between pipelines. The
target ownership model is:

```
Global UDF Registry
  └── WasmUdfDefinition
        ├── Engine
        ├── Module
        ├── InstancePre
        └── metadata

Pipeline A
  ├── Processor A
  │     └── WasmRuntimeSet
  │           └── wasm_udf
  │                 ├── Store
  │                 ├── Instance
  │                 ├── Memory
  │                 └── TypedFunc
  └── Processor B
        └── WasmRuntimeSet
              └── wasm_udf
                    ├── Store
                    ├── Instance
                    ├── Memory
                    └── TypedFunc

Pipeline B
  └── Processor C
        └── WasmRuntimeSet
              └── wasm_udf
                    ├── Store
                    ├── Instance
                    ├── Memory
                    └── TypedFunc
```

`WasmUdfDefinition` contains immutable, shareable data: the `Engine`, compiled `Module`,
`InstancePre`, execution mode, and validated metadata. Each processor creates one private
bound runtime for each distinct UDF definition used by that processor. The runtime owns
the mutable execution state and is reused for all rows or micro-batches processed by
that processor. Different processors do not share a mutable `Instance`.

For a stateless scalar UDF, multiple call sites of the same function inside one processor
share the same processor-owned bound runtime. For example, both
`wasm_udf(a) + wasm_udf(b)` and `wasm_udf(wasm_udf(a))` reuse the same `Store` and
`Instance`; the nested call executes the inner and outer calls sequentially. Different
UDFs have different runtimes, and the same UDF used by two processors has one runtime per
processor. This design avoids a per-row `Store` and `Instance` while retaining
linear-memory isolation between processors and pipelines.

Scalar UDFs using this model must be stateless: their result must depend only on the
current arguments, not on mutable module state left by an earlier row or call site.
Evaluator memory uses a scratch arena with a reusable high-water mark. Each row or
batch starts allocation from the arena base; if the existing linear memory is large
enough, the allocation is reused, and if a larger input or output is needed, only the
additional pages are grown. Linear memory is not shrunk during normal execution.
After every invocation, including serialization failures, WASM traps, timeouts, and
result decoding failures, the host restores the bump pointer to the arena base. This
allows the same linear memory pages to be reused without accumulating allocations.
Periodic reclamation by replacing an evaluator is optional deferred work.

### Safety limits

- **WASM invocation timeout**: each row invocation or micro-batch has an execution
  deadline. Epoch-based interruption terminates a call that exceeds its deadline and
  returns a timeout error. The timeout is a best-effort execution deadline rather than
  an exact wall-clock guarantee.
- **Memory limit**: module-declared memory maximums and host allocator checks provide
  the initial memory boundary. A separate store-level resource limiter is not required
  for trusted UDFs.
- **No `unsafe`** in the call site. wasmtime's safe API handles all FFI.

## Flow Module Integration

### CustomFuncRegistry extension

`CustomFuncRegistry` is built at startup with all built-in functions plus all
persisted WASM UDFs. After startup, hot-loaded UDFs are added via `clone_and_add`
which atomically replaces the `Arc`; the hot path (`compute_processor`,
`filter_processor`) reads from the current `Arc` lock-free — no lock is taken per
expression evaluation.

```rust
impl CustomFuncRegistry {
    /// Build a registry containing built-in functions and the given WASM UDFs.
    /// Returns an error on name collision.
    pub fn with_builtins_and_wasm(
        wasm_udfs: Vec<Arc<WasmUdfDefinition>>,
    ) -> Result<Arc<Self>, String> {
        let mut registry = Self::builtins();
        for udf in wasm_udfs {
            let name = udf.name().to_lowercase();
            if registry.functions.contains_key(&name) {
                return Err(format!("UDF '{name}' conflicts with an existing function"));
            }
            registry.register(udf);
        }
        Ok(Arc::new(registry))
    }

    fn register(&mut self, definition: Arc<WasmUdfDefinition>) {
        self.definitions
            .insert(definition.name().to_lowercase(), Arc::clone(&definition));
    }
}
```

### Startup loading sequence

UDF loading happens during FlowInstance construction, before the manager boots
its HTTP server. The sequence is:

```
FlowInstance::new()
  ├── Build CustomFuncRegistry (built-ins only)
  └── Rebuild registry with builtins + WASM UDFs (read lock on startup only)

Manager bootstrap
  ├── hydrate WASM UDFs from storage              ← NEW
  │    1. List all StoredUdf records
  │    2. For each: load <sha256>.wasm from disk
  │    3. Re-hash and verify against stored wasm_sha256
  │    4. Compile and validate WasmUdfDefinition via wasmtime
  │    5. Collect validated immutable definitions
  │    6. Rebuild the registry with built-ins + UDF definitions
  │    7. Replace the FlowInstance's registry with the new one
  ├── hydrate memory topics
  ├── hydrate shared MQTT configs
  ├── hydrate streams
  └── hydrate pipelines
```

UDFs must be loaded **before** pipelines, because pipeline SQL may reference them.
After startup, hot-load replaces the immutable definition registry via `clone_and_add`
(write lock on upload only). Pipeline workers create their private evaluators when they
are built; the registry never owns those mutable evaluators.

### Batch execution and column materialization

Batch execution is implemented above the row-level expression evaluator. A single-row
`eval_row` call cannot discover neighboring rows, so batching is not hidden inside
`eval_row`. Instead, the planner extracts batch-capable WASM UDF subexpressions into
ordered temporary columns and the compute processor evaluates those columns in batches.

For example:

```sql
SELECT wasm_udf(wasm_udf(a, b), c) + 1 AS result FROM stream
```

is materialized as:

```text
col_1 = wasm_udf(a, b)
col_2 = wasm_udf(col_1, c)
result = col_2 + 1
```

The inner UDF produces a vector first; that vector is then used as an input column for
the outer UDF. Temporary columns are internal affiliate columns and are not included in
the final output layout.

The processor-owned evaluator interface is conceptually:

```rust
pub trait WasmUdfEvaluator {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError>;

    fn eval_row(&mut self, args: &[Value]) -> Result<Value, EvalError>;

    /// Each argument vector has row_count elements. The result vector must also
    /// have row_count elements and preserve row alignment.
    fn eval_batch(&mut self, batch_args: &[Vec<Value>])
        -> Vec<Result<Value, EvalError>>;
}
```

Batch-mode definitions implement `eval_batch` through a `udf_batch` export.
The WASM function receives vectors for all scalar arguments and returns a vector. It
must preserve row alignment: output element `i` depends only on input elements at
index `i`. This is vectorized scalar evaluation, not cross-row aggregation. Row-mode
definitions implement `eval_row` through `udf_row` and are evaluated one row at a time.

The `ComputeProcessor` evaluates temporary columns in dependency order. A batch is
bounded (a micro-batch) so JSON size, scratch memory, latency, and timeout budget stay
within configured limits. Batch-mode definitions must be materialized through this path;
row-mode definitions remain on the row-level path. The processor creates and binds its
runtime set when it is built; no mutable runtime is acquired from the global registry
during row evaluation.

Processor construction follows this sequence:

1. Inspect the physical expressions and collect referenced WASM UDF definition IDs.
2. Create one processor-owned `WasmRuntimeSet` and instantiate one runtime per distinct
   definition used by that processor.
3. Replace the physical expression's WASM function references with the bound runtime
   functions.
4. Reuse those runtimes for all rows or micro-batches handled by the processor.

Built-in `CustomFunc` calls can continue through the existing scalar evaluator. A WASM
call is resolved through its definition ID and runtime set entry, so the global registry
never becomes the owner of mutable execution state.

## WASM UDF Authorship

### Required exports

Every WASM UDF module must export:

1. `memory`: WebAssembly linear memory
2. `udf_metadata_ptr() -> i32` and `udf_metadata_len() -> i32`: pointers to a
   JSON metadata blob describing the function
3. Exactly one execution export, selected by metadata:
   - `udf_row(ptr: i32, len: i32) -> i32` for `execution_mode: "row"`;
   - `udf_batch(ptr: i32, len: i32) -> i32` for `execution_mode: "batch"`.
   A module must not declare one mode while exporting the other mode's entry point.

### Metadata format

The `udf_metadata` JSON blob:

```json
{
  "abi_version": 1,
  "name": "my_udf",
  "description": "Computes a custom score from sensor data",
  "args": [
    {"name": "sensor_value", "type": "float"},
    {"name": "threshold", "type": "float"}
  ],
  "return_type": "float",
  "execution_mode": "batch"
}
```

Supported types: `"int"`, `"float"`, `"string"`, `"bool"`, `"timestamp"`,
`"object"`, `"array"`, `"any"`.

`abi_version` is part of the WASM artifact metadata and is the single source of truth
for ABI selection. The current host accepts only version `1`; unsupported versions are
rejected before the module is persisted or registered. The HTTP response, `StoredUdf`,
and export/import records expose this validated metadata; they do not define a separate
ABI version.

### ABI version 1 execution contract

The host passes a pointer and byte length for one JSON input envelope to the
mode-selected execution export. The export returns a pointer to JSON result bytes.
The four bytes immediately before the returned data pointer contain the result byte
length as a little-endian `i32`. The host reads the result immediately and owns the
result bytes only for that invocation; a UDF must not retain input or output pointers
after returning.

For row mode, the input envelope contains one argument list and the result envelope
contains one `Value`. For batch mode, the input is column-oriented:

```json
{
  "row_count": 3,
  "args": [[1, 2, 3], [10, 20, 30]]
}
```

The batch result is:

```json
{"values": [11, 22, 33]}
```

Every argument vector and the result vector must contain exactly `row_count` elements.
Output element `i` corresponds to input row `i`; batch mode is vectorized scalar
evaluation, not cross-row aggregation.

The host-provided allocator is used for invocation scratch buffers. Before each row or
batch, the allocator reuses the scratch arena from its base. A larger request grows
linear memory only by the required additional pages; a smaller later request reuses the
existing capacity. The host resets the bump pointer after every invocation, including
error paths. The module must not rely on scratch pointers remaining valid across
invocations. Periodic evaluator replacement is optional deferred work for returning
unused linear memory to the operating system.

### Example: Rust UDF (with `veloflux-udf-sdk`)

We provide `veloflux-udf-sdk`, a helper crate that hides the entire WASM ABI
behind a `define_udf!` macro. Users write plain Rust functions with zero
boilerplate:

```rust
use veloflux_udf_sdk::define_udf;

define_udf! {
    name = "my_udf",
    description = "Multiplies sensor value by threshold",
    return_type = "float",
    fn my_udf(sensor_value: f64, threshold: f64) -> f64 {
        sensor_value * threshold
    }
}
```

The macro automatically generates the execution export selected by the UDF mode:
- `udf_row(input_ptr, input_len)` for row mode, or `udf_batch(input_ptr, input_len)` for
  batch mode — JSON deserialization → call user fn → serialize result(s)
- `udf_metadata_ptr()` / `udf_metadata_len()` — metadata JSON blob
- `memory` — WASM linear memory for the bump allocator

Compile with:
```
cargo build --target wasm32-unknown-unknown --release
```

The JSON ABI (`Value` ↔ JSON ↔ WASM bytes) is a transparent implementation detail;
users of `veloflux-udf-sdk` never interact with it directly.

## Performance

**Current implementation limitation (MVP):** Row-mode execution still serializes and
deserializes JSON for every row. Store creation and module instantiation are not part of
the row-level hot path: the processor binds one runtime per distinct UDF and reuses it.
Batch ABI and column materialization remain a later optimization.

**Selected optimization:** Move `Store`, `Instance`, `Memory`, and typed exports into a
processor-owned bound runtime created by a processor-local `WasmRuntimeSet`. Share the
compiled `Module` and `InstancePre` through a global `WasmUdfDefinition`, but instantiate
the module only once per processor. The future batch runtime can then execute `udf_batch`
for a bounded batch of rows,
so each batch performs one JSON serialization, one WASM call, and one JSON
deserialization instead of repeating those operations for every row.

The planner materializes nested UDF calls into dependency-ordered temporary columns. For
`wasm_udf(wasm_udf(a, b), c)`, the inner call produces `col_1` in one or more batches,
and the outer call consumes `col_1` in subsequent batches. The final projection reads
the materialized result columns and performs any remaining scalar operations.

The runtime must reuse its scratch arena before each batch. A trap or timeout is
reported as an evaluation error and the scratch arena is reset; the trusted-UDF model
does not discard and recreate the evaluator. A global
`Instance` protected by a mutex is not the target design because it would serialize
otherwise independent pipelines and allow execution state to leak across them.

## Security

1. **Sandbox**: Each UDF runs in a separate `wasmtime::Instance` with isolated
   linear memory. No access to filesystem, network, or environment variables.

2. **Execution safeguards**: A periodic engine epoch ticker and per-call epoch deadline
   provide a bounded execution timeout for running UDF code. Module-declared memory
   maximums and host allocator checks bound the initial linear-memory model. This design
   assumes trusted UDF artifacts and does not claim complete recovery from arbitrary
   hostile module state.

3. **Input validation**: Argument count and types are validated against metadata
   before calling into WASM.

4. **No `unsafe`**: The wasmtime safe API is used throughout. All pointer reads/writes
   go through wasmtime's checked `Memory::read`/`Memory::write` methods.

## Future Extensions

### gRPC remote UDFs

A `RemoteUdf` could implement `CustomFunc` to call an external UDF service via gRPC.
This would follow the same registration pattern and be transparent to the flow module.
This is appropriate for UDFs that:
- Require languages that don't compile to WASM (Python, JavaScript)
- Are computationally expensive (seconds per call)
- Need access to external services or databases

### Stateful WASM UDFs

WASM UDFs could maintain state across rows by exporting `udf_init`/`udf_eval`/`udf_close`,
mapping to the existing `StatefulFunction` trait. This would enable windowed aggregations
and custom stateful transformations in WASM.

### WASI support

If use cases demand it, wasmtime's WASI support could selectively enable file system or
network access for UDFs that need it, with explicit opt-in and capability controls.

## Deployment contract

The selected `abi_version` and `execution_mode` are required for every new WASM UDF.
The host rejects modules whose metadata mode does not match the single execution export,
whose metadata ABI version is not `1`, or whose input/output envelope is malformed. Row-mode
and batch-mode modules are separate deployment artifacts; the host does not silently
translate one mode into the other. Under the trusted-UDF model, a timeout or trap is
reported as an evaluation error and the evaluator is reused after scratch reset.

The UDF registry and processor runtime set are separate lifecycle domains. Updating the
registry affects newly built pipelines; existing workers retain their private evaluators
until they stop. A hot-loaded definition is validated before it becomes visible to new
pipeline construction.
