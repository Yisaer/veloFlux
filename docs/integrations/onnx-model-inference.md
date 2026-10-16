# ONNX Model Inference Design

## Status

This document records the design for ONNX model inference in veloFlux. The Cargo feature gate,
model upload/storage slice, shared prepared-model registry, SQL registration, planner binding, and
row-based custom-function execution are implemented. The first runtime target is CPU inference through tract
and does not include TFLite or NCNN execution.

The product requirements are recorded in
[`tmp/车端计算引擎AI模型接入要求.md`](../../tmp/%E8%BD%A6%E7%AB%AF%E8%AE%A1%E7%AE%97%E5%BC%95%E6%93%8EAI%E6%A8%A1%E5%9E%8B%E6%8E%A5%E5%85%A5%E8%A6%81%E6%B1%82.md).

## Background

The vehicle-side engine receives decoded, flat signal columns from MQTT and GBF processing. Model
execution must remain independent from collection and decoding. SQL is the contract that connects
the two sides:

```sql
SELECT
  ts,
  onnx('acme.collision_v2.1', speed, distance, rel_speed) AS infer_out
FROM vehicle_stream
WHERE speed IS NOT NULL
  AND distance IS NOT NULL
  AND rel_speed IS NOT NULL
```

The result has one struct field per ONNX graph output. Individual outputs are accessed with the
existing struct field operator:

```sql
SELECT
  onnx('acme.collision_v2.1', speed, distance, rel_speed) -> output_0 AS probability
FROM vehicle_stream
```

The initial model contract is deliberately narrow:

- one `.onnx` artifact per model version;
- graph-derived input and output signatures exposed through veloFlux scalar/list values;
- no external data files or custom operators;
- no `Loop`, `If`, or `Scan` control-flow operators;
- opset 11 through 18;
- a `model.yaml` file containing the model name; input and output signatures are extracted from the
  ONNX graph;
- at least three input/output test-vector pairs;
- model versions are immutable and are identified by the model name used in SQL.

The engine must not infer units, ranges, or normalization. The rule author provides arguments in
the ONNX graph input order and filters `NULL` values before model execution.

The model file does not repeat the tensor schema. For example:

```yaml
name: acme.collision_v2.1
```

Each graph input/output is exposed to veloFlux as a scalar for a rank-0 tensor or a flat
`list<scalar>` for every other tensor. Tensor rank, fixed dimensions, symbolic dimensions, and layout
remain internal ONNX metadata used to construct and decode tensors. An ONNX graph may have multiple
outputs; the SQL function returns one struct value containing all graph outputs rather than expanding
one invocation into multiple SQL columns.

## Terminology and scope

ONNX is a portable model representation. It is not an inference engine. tract is the Rust
inference engine selected for the first implementation; it can load and execute ONNX models on
CPU. TFLite is another model format and runtime family. NCNN is a separate mobile and embedded
inference engine with its own model format and native C++ integration requirements.

The first scope is therefore:

```text
ONNX model -> tract runtime -> veloFlux SQL function
```

The SQL function is named `onnx`, because the function exposes the supported model format rather
than the current runtime implementation. `tract` remains an internal implementation detail.

## Architectural principles

### Models are first-class resources

An AI model is not a WASM UDF. A model has a graph, weights, an input/output contract, test
vectors, a version, and a runtime lifecycle. Model resources must therefore have their own
metadata and API while reusing the existing storage attachment, revision, import, export, and
rollback infrastructure where appropriate.

### Parser and planner responsibilities are separate

The parser should parse `onnx(...)` as an ordinary function expression. It must not know whether
ONNX support is compiled into the binary.

The planner decides whether the function is available and whether the pipeline is valid. A model
name must be a string literal. Once the literal is resolved, the planner specializes the generic
SQL call into a model-specific invocation with a fixed input and output schema. It should perform
static checks such as:

- the `onnx` function is enabled in the build;
- the first argument is a model-name string literal;
- the referenced model exists and is usable;
- the number and types of arguments match the graph-derived input signatures;
- the function result has the graph-derived `StructType` output schema;
- every referenced output field exists when struct field access is used.

The first implementation should reject a dynamic model expression such as
`onnx(model_column, speed)`. A literal model name allows the planner to bind the model before
runtime, construct the fixed output `StructType`, and make model dependencies explicit.

### Row-based custom-function execution

ONNX is an ordinary row-based `CustomFunc`, not a `BatchFunctionCompute` implementation. Each row
is evaluated through the normal scalar expression path and produces one row-aligned `Value::Struct`
containing all declared model outputs. An inference failure therefore affects the current row
without compacting or reindexing the surrounding collection.

The planner specializes the generic SQL call using the literal model name and model metadata. It
validates the argument count and veloFlux-native input schema, and fixes the return type to the
metadata-declared output struct. Nested expressions use the normal scalar dependency order.

Row-based execution does not imply loading the model for every row. After the physical plan has
been built, the runtime collects the literal model names referenced by the plan and acquires the
prepared models before creating the processors. A processor-bound ONNX custom-function instance
reuses its acquired prepared model for every row. `eval_row` performs inference only; it must not
perform first-use graph loading or preparation. The first implementation invokes the prepared
model once per row, while a future optimization may use tensor-level batching for models whose
ONNX input graph supports a batch dimension.

### Model and pipeline ownership

Model metadata is a storage resource, while a prepared ONNX graph is a runtime resource. Startup
loads model metadata and graph-derived signatures so planning can resolve literal model names without
preparing every executable graph in memory. When a pipeline runtime is built, after planning and before processor creation,
it acquires the prepared models referenced by that pipeline.

The runtime cache shares immutable prepared graphs and weights across pipelines. Two pipelines
using the same model revision hold two `Arc` references to one prepared graph; they do not prepare
two copies. Each pipeline still owns its row execution context and any runtime state that is not
safe to share. The relationship is:

```text
StoredModel
  └── model name, revision, schema, bundle path

ModelRuntimeCache
  └── (model name, revision, SHA-256)
        └── shared PreparedOnnxModel
              ├── Pipeline A runtime / Arc handle
              └── Pipeline B runtime / Arc handle
```

The registry cache should not permanently own prepared models with a strong `Arc`. The cache uses
the model key for de-duplication and retains a `Weak<PreparedOnnxModel>` entry, while running
pipeline runtimes hold the strong `Arc` handles. When the last runtime is stopped, deleted, or
rebuilt, its processor pipeline and model handles are dropped and the prepared graph is released.
This also releases old revisions after the last pipeline using them exits.

Stopped pipelines retain their definitions and model dependencies for validation, but do not retain
a processor runtime or a prepared graph. Starting them builds a new runtime and acquires the
current model revision.

`PreparedOnnxModel` must be immutable. The current tract implementation shares one immutable
`TypedSimplePlan` through `Arc` when multiple pipeline runtimes use the same cache key. Concurrent
inference on that shared plan is covered by a runtime unit test. A model's mutable execution state
must remain local to its processor-bound scalar function or to a narrower invocation scope. This
prevents one pipeline's row-level errors from affecting another pipeline. If a future backend
requires isolated execution state, that state may be made pipeline-local without duplicating the
prepared graph and weights.

The cache key includes the model revision and content hash. Because model versions are immutable,
loading a new model version cannot reuse a prepared graph for an older version. A running pipeline
continues to use the immutable revision that it acquired; a newly built or restarted runtime uses
the current metadata revision. A model referenced by a stored pipeline cannot be deleted until
the dependency is removed, even when that pipeline is stopped and no prepared graph is resident.

Model release is ownership-driven rather than dependent on a periodic scan. The runtime `Arc`
handles provide the authoritative memory lifetime across stop, delete, rebuild, and failed-start
cleanup paths. An optional patrol task may remove dead weak cache entries and expose diagnostics,
but it is not responsible for deciding whether an in-flight runtime still owns a model.

The model API exposes `GET /models/:name/status` for runtime observability. Its `prepared` field
indicates whether the current model cache key still has a strong runtime reference, and
`prepare_count` verifies that concurrent pipelines share one preparation. This endpoint describes
runtime ownership; it is not an operating-system memory or RSS measurement.

## Cargo feature design

The public build capability is one top-level `onnx` feature. It must not be enabled by default.
The feature is propagated to the planner/function registration, manager model validation, and the
inference runtime:

```text
root: onnx
  ├── flow/onnx
  ├── manager/onnx
  └── model-runtime/onnx
        └── tract
```

The intended behavior is:

| Build | Planner | Processor | Manager | tract in binary |
|---|---|---|---|---|
| without `onnx` | `onnx(...)` is unavailable | ONNX runtime is absent | ONNX validation is unavailable | No |
| with `onnx` | Registers and validates `onnx(...)` | Compiles the tract-backed runtime | Validates and tests ONNX models | Yes |

The internal runtime crate should keep tract optional and have no default features:

```toml
[features]
default = []
onnx = ["dep:tract"]
```

The public feature should be named `onnx`, not `tract`. This describes the product capability and
allows the implementation to change later without changing the SQL or deployment interface.

Cargo feature propagation must be atomic from the user's perspective. The build must not allow a
planner with ONNX support and a processor without the corresponding runtime. The root feature must
enable all required subcrate features together.

## Model resource and API design

The public manager API should expose models as a separate resource, for example:

```text
POST   /models
GET    /models
GET    /models/:name
DELETE /models/:name
```

The upload should be an atomic model bundle rather than three unrelated uploads:

```text
acme.collision_v2.1/
├── model.onnx
├── model.yaml
└── test_vectors/
    ├── input-001.json
    ├── expected_output-001.json
    ├── input-002.json
    ├── expected_output-002.json
    ├── input-003.json
    └── expected_output-003.json
```

The storage layer should add a model resource type and persist model metadata, revision, and
content hashes. The current implementation has a dedicated model metadata table, publishes the
extracted bundle below the storage base directory, and includes model metadata and files in the
full storage import/export bundle. Model resources also participate in tombstone snapshots;
storage does not depend on tract.

The manager layer owns ONNX-specific validation and lifecycle orchestration:

1. stage the upload in a temporary directory;
2. validate the bundle layout and extract the graph-derived input/output signatures;
3. calculate content hashes;
4. publish the bundle and persist model metadata atomically;
5. prepare the model through the enabled runtime when the pipeline runtime is created, after
   planning and before processor creation;
6. validate the ONNX graph's tensor types, dimensions, batch behavior, opset, and operator
   restrictions;
7. run all supplied test vectors.

The first four steps are implemented for upload and full storage import. The manager parses and
optimizes the ONNX graph during upload to extract the signatures and reject malformed or unsupported
graph metadata. Test-vector execution remains deferred. Runtime binding prepares the executable graph
during pipeline runtime creation, after planning and before processor creation; it does not defer
preparation until the first input row. The planner consumes the persisted graph-derived signatures.
Processor execution supports bool, signed/unsigned integer, floating-point, and string scalar values.
Non-scalar tensor values are supplied as flat lists and reconstructed using the graph rank and fixed
dimensions. A single symbolic input dimension can be derived from the flat value length; ambiguous
multiple-symbolic-dimension inputs are rejected. Tensor elements are returned as flat lists. All
model outputs are returned as one struct value.

This restriction is caused by the veloFlux boundary rather than by ONNX. A flat list provides only
the total number of tensor elements. For `[?, 3]`, six elements uniquely determine `[2, 3]` because
the second dimension is fixed. For `[?, ?]` or `[?, ?, 3]`, the same flat length has multiple valid
rank-preserving shapes, so veloFlux cannot reconstruct the tensor without an additional shape input
or a nested value representation. Such input signatures are rejected during upload instead of being
silently reshaped incorrectly.
Numeric conversion at the runtime boundary is strict. Fractional integer inputs, out-of-range
values, non-finite values, and conversions that lose integer or floating-point precision are
rejected rather than silently cast.

If ONNX support is disabled, model storage metadata may remain available for persistence and
import/export, but ONNX validation, execution, and ONNX-specific upload handling are unavailable.
Persisted models must not be silently deleted merely because a binary was built without the
feature.

Model files must be stored under system-generated or content-addressed paths. User-provided model
names must never become unchecked filesystem paths.

## Runtime dependency and lifecycle

The intended dependency direction is:

```text
storage
  └── model metadata and attachments

manager
  ├── model API and lifecycle
  └── optional model-runtime validation

flow
  ├── SQL function registration and planner checks
  └── optional model-runtime execution binding

model-runtime
  └── optional tract implementation
```

The model runtime must expose a small application-facing abstraction instead of leaking tract
types into planner and storage code. The abstraction should provide model metadata, prepared-model
acquisition, and inference operations; the tract implementation owns model loading, optimization,
and tensor conversion. Prepared-model acquisition occurs after planning and before processor
creation, is shared through the runtime cache, and is owned by active pipeline runtimes while
execution contexts remain pipeline-local.

Models are immutable. Replacing the graph, input order, or input meaning requires a new versioned
model name. A pipeline that references a model creates a model dependency. Deleting a referenced
model should be rejected or require the dependent pipelines to be stopped first.

## Deferred backends

TFLite and NCNN are intentionally outside the first implementation:

- TFLite may later use tract's TFLite loader if the required models and operators are supported.
- A native TensorFlow Lite runtime should only be added if tract compatibility is insufficient.
- NCNN requires a separate backend, model-file pair management, native library packaging, FFI
  safety, and platform-specific build support.

The model abstraction should leave room for these backends, but their Cargo features and runtime
implementations should not be introduced until there is a concrete model and performance need.

## Implementation phases

### Phase 1: ONNX custom function

- Add the optional runtime crate and the top-level `onnx` feature. **Implemented.**
- Add model metadata, model upload/storage, and full import/export integration. **Implemented.**
- Register `onnx(...)` only when the feature is enabled.
- Validate literal model names and input contracts during planning.
- Evaluate `onnx(...)` through the ordinary scalar custom-function path.
- Acquire and optimize only referenced ONNX models with tract before processor creation, sharing
  immutable prepared graphs across active pipeline runtimes. **Implemented.**
- Convert flat veloFlux list values to ONNX tensors using graph rank and dimensions, and decode
  tensor outputs into flat veloFlux lists.
- Validate the supplied test vectors during upload.

### Phase 2: operational hardening

- Extend model dependency tracking to pipeline update/delete lifecycle operations.
- Add inference counters, failures, and latency metrics.
- Add model load and execution resource limits.
- Add feature-on and feature-off CI coverage.

### Phase 3: additional formats/backends

- Add TFLite only after confirming required operator coverage.
- Add NCNN only with a concrete deployment requirement and an explicit native-library strategy.
