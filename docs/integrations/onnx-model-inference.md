# ONNX Model Inference Design

## Status

This document records the design for ONNX model inference in veloFlux. The Cargo feature gate and
the first model upload/storage slice are implemented. SQL inference, tract graph validation, and
runtime execution are not implemented yet. The first runtime target is CPU inference through tract
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

The initial model contract is deliberately narrow:

- one `.onnx` artifact per model version;
- `float32` inputs and outputs;
- static shapes with `batch=1`;
- no external data files or custom operators;
- no `Loop`, `If`, or `Scan` control-flow operators;
- opset 11 through 18;
- a `model.yaml` model card;
- at least three input/output test-vector pairs;
- model versions are immutable and are identified by the model name used in SQL.

The engine must not infer units, ranges, normalization, or input ordering beyond the model card.
The rule author provides arguments in the model-card order and filters `NULL` values before model
execution.

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

The planner decides whether the function is available and whether the pipeline is valid. It should
perform static checks such as:

- the `onnx` function is enabled in the build;
- the first argument is a model-name string literal;
- the referenced model exists and is usable;
- the number and types of arguments match the model card;
- the function result has the declared list-of-`float32` shape.

The first implementation should reject a dynamic model expression such as
`onnx(model_column, speed)`. A literal model name allows the planner to bind the model before
runtime and makes model dependencies explicit.

### Shared batch-function execution

ONNX execution follows the same collection-level batch-function mechanism as WASM UDF execution.
The SQL function is represented as a custom function, but it is not evaluated through the ordinary
row-level `ScalarExpr::CallFunc` path. Physical planning lowers batch functions into one generic
batch-function compute node, whose processor owns the execution instances for all batch functions
in that expression group.

The distinction between a function call and a runtime instance is important:

- every function occurrence is a separate evaluation with its own argument expressions and output
  column;
- evaluations with the same function type and runtime binding configuration share one runtime
  instance;
- different model references or runtime configurations create different instances even when the
  SQL function name is the same.

For example:

```sql
onnx(
  wasm_udf1(a),
  wasm_udf1(a),
  wasm_udf2(a)
)
```

is evaluated as four ordered evaluations:

```text
wasm_udf1(a)       -> __vf_batch_1
wasm_udf1(a)       -> __vf_batch_2
wasm_udf2(a)       -> __vf_batch_3
onnx(__vf_batch_1, __vf_batch_2, __vf_batch_3)
```

The processor owns three runtime instances: one for `wasm_udf1`, one for `wasm_udf2`, and one for
the ONNX model. The two `wasm_udf1` evaluations reuse the same instance. Runtime instances are
bound once during processor initialization and are reused for all input collections.

For each evaluation, the processor:

1. evaluates ordinary argument expressions, such as `a + 1`, row by row;
2. collects the aligned argument columns for the current collection;
3. invokes the bound batch runtime instance;
4. appends the row-aligned result to the planned affiliate output column.

Nested calls are executed in dependency order. `wasm_udf(onnx(a))` evaluates ONNX first and then
WASM; `onnx(wasm_udf(a))` evaluates WASM first and then ONNX. The processor creation order does
not define this semantic order; the planner-generated evaluation order does.

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
2. validate the bundle layout and model-card schema;
3. calculate content hashes;
4. publish the bundle and persist model metadata atomically;
5. load the model through the enabled runtime;
6. validate dtype, static shape, batch, opset, and operator restrictions;
7. run all supplied test vectors.

The first four steps are implemented for upload and full storage import. The final three runtime
validation steps are deferred.

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
types into planner and storage code. The abstraction should provide model metadata and inference
operations; the tract implementation owns model loading, optimization, and tensor conversion.

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

### Phase 1: ONNX batch function

- Add the optional runtime crate and the top-level `onnx` feature. **Implemented.**
- Add model metadata, model upload/storage, and full import/export integration. **Implemented.**
- Register `onnx(...)` only when the feature is enabled.
- Validate literal model names and input contracts during planning.
- Lower `onnx(...)` into the shared batch-function compute path.
- Load and optimize ONNX models with tract when the ONNX runtime instance is bound.
- Return `List<Float32>` values.
- Validate the supplied test vectors during upload.

### Phase 2: operational hardening

- Add model dependency tracking to pipeline lifecycle operations.
- Add inference counters, failures, and latency metrics.
- Add model load and execution resource limits.
- Add feature-on and feature-off CI coverage.

### Phase 3: additional formats/backends

- Add TFLite only after confirming required operator coverage.
- Add NCNN only with a concrete deployment requirement and an explicit native-library strategy.
