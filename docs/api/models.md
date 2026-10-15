# ONNX Model Management API

## Availability

These endpoints are compiled only when veloFlux is built with the `onnx` Cargo feature. A
binary built without that feature does not expose model management routes.

The current implementation manages immutable ONNX model bundles, registers the `onnx(...)` SQL
function, binds it through the scalar custom-function planner path, and executes row-based tensor
inference through the shared lazy tract-backed runtime. Runtime conversion supports the primitive
schema types and recursively flattens list/struct input values; model outputs are decoded back into
the declared recursive schema.

Numeric conversion is strict at the runtime boundary: floating-point values used for integer
tensors must be finite, integral, and within range; integer-to-floating-point and `float64` to
`float32` conversions must be exact. Lossy or out-of-range conversions fail inference.

## Bundle format

`POST /models` accepts a `multipart/form-data` request with one ZIP field named `file` and an
optional `revision` field. The ZIP must contain exactly one top-level directory:

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

The upload validator currently requires:

- a non-empty `model.onnx` file;
- a YAML mapping in `model.yaml` with string `name`, plus `inputs` and `outputs` fields;
- a model name beginning with an ASCII letter and containing only letters, digits, `_`, and `.`;
- at least three matching `input-*.json` and `expected_output-*.json` pairs.

The model-card contract is expressed in veloFlux's stream-column schema. Each input and output must
declare a unique column name and a supported `data_type`. Composite values use the same recursive
schema as stream columns: a `list` declares its element column in `element`, and a `struct` declares
its child columns in `fields`. Model schemas additionally support an optional `cardinality` on list
columns to preserve boundaries for nested or composite tensor values. Input arguments in SQL are
positional and follow the order declared by `inputs`.

For a composite list element, or for a list field inside a struct, `cardinality` is required so
the runtime can preserve element boundaries. Model upload rejects recursive schemas whose boundaries
cannot be determined from the tensor values and schema.

For example, a model that accepts a scalar speed and a list of float samples can declare:

```yaml
name: acme.collision_v2.1
inputs:
  - name: speed
    data_type: float32
  - name: samples
    data_type: list
    cardinality: 10
    element:
      name: element
      data_type: float32
outputs:
  - name: probability
    data_type: float32
  - name: labels
    data_type: struct
    fields:
      - name: safe
        data_type: bool
```

The model card describes values from veloFlux's perspective. ONNX tensor rank, dimensions, and
runtime tensor layout are implementation details and are not fields in this API. The ONNX runtime
validates the declared input values against the prepared graph when inference is executed.

A model may declare multiple outputs; all outputs are returned as one struct value whose fields
follow the declared output names and order.

The model name is immutable. Uploading another bundle with the same name returns `409 Conflict`;
publishers must use a new versioned name for a new graph or input/output contract.

## Endpoints

| Method | Path | Behavior |
| --- | --- | --- |
| `POST` | `/models` | Validate and publish one immutable model bundle. |
| `GET` | `/models` | List persisted model metadata. |
| `GET` | `/models/:name` | Read one model's metadata. |
| `DELETE` | `/models/:name` | Delete model metadata and its bundle when no stored pipeline references it. |

The response metadata includes the model name, revision, ONNX SHA-256, model size, input schema,
output schema, and test-vector count. Model files are stored below the storage base directory's `models/` directory; upload names
are validated before they are used in filesystem paths. Upload and delete operations acquire the
exclusive metadata and storage-operation guards and use a crash-recoverable publication journal.

Deleting a model referenced by a stored pipeline returns `409 Conflict`. The dependency check scans
the persisted pipeline SQL before removing the model attachment or metadata.

Uploading a model persists its metadata and bundle but does not require every model to remain
loaded in memory. The process loads model metadata at startup. When a pipeline references a model,
the ONNX graph is prepared on demand during runtime initialization and placed in a shared cache
keyed by model name, revision, and SHA-256. Pipelines share the immutable prepared graph and
weights, but keep their own batch execution context and temporary buffers.

## Validation boundary

The manager performs bundle structure and model-card validation. The storage layer persists model
metadata in its own table and publishes the extracted bundle directory. The storage layer does
not depend on tract or parse ONNX graphs.

The following checks are intentionally deferred:

- upload-time ONNX graph validation and operator/dtype/shape/opset validation through tract;
- executing test vectors against the loaded model;

When an ONNX function binds a model at runtime, the registry loads and prepares the graph on demand.
The persisted metadata remains the planner contract; the planner must not infer a model's signature
dynamically during row execution. A prepared graph is shared only when it is immutable; mutable
execution state remains owned by the pipeline's batch-function runtime.

## Import and export

Models are included in the full storage import/export bundle. Exported archives contain the model
metadata in `manifest.json` and the complete bundle below `models/<name>/`. Import validates the
model name, metadata, model SHA-256, model size, model card, and test-vector pair count before
publishing the model directory and metadata together.

The incremental `POST /storage/apply` endpoint does not accept model changes yet. Model bundles
must be transferred through `POST /storage/import`, because the full bundle is required for an
atomic file and metadata publication.
