# ONNX Model Management API

## Availability

These endpoints are compiled only when veloFlux is built with the `onnx` Cargo feature. A
binary built without that feature does not expose model management routes.

The current implementation manages immutable ONNX model bundles, registers the `onnx(...)` SQL
function, binds it through the scalar custom-function planner path, and executes row-based tensor
inference through the shared tract-backed runtime.

The model API treats every ONNX input and output tensor as one veloFlux parameter whose
public value is either a scalar or a flat one-dimensional list of scalars. Tensor rank, dimensions,
batch/feature interpretation, and tensor layout remain ONNX runtime metadata. They are used to build
and decode the actual tensor, but are not exposed as nested veloFlux model schema.

Multiple ONNX outputs continue to be returned as one veloFlux `struct`. Each struct field represents
one ONNX output and has either a scalar type or a flat list type. The model loader extracts these
signatures from the ONNX graph before the model is registered.

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
- a YAML mapping in `model.yaml` with a string `name`; `inputs` and `outputs` declarations are not
  required;
- a model name beginning with an ASCII letter and containing only letters, digits, `_`, and `.`;
- at least three matching `input-*.json` and `expected_output-*.json` pairs.

The model contract is derived from the ONNX graph when the model is loaded. Each ONNX input
and output contributes one tensor signature containing its name, scalar element type, and runtime
shape. The veloFlux-facing type is a scalar for a single scalar value or a flat one-dimensional list
for tensor values. Input arguments in SQL remain positional and follow the ONNX input order.

```yaml
name: acme.collision_v2.1
```

For example, a graph input such as `float32[1,1,28,28]` is exposed as a
`list<float32>`. The runtime flattens the veloFlux value and uses the ONNX shape to construct the
actual tensor; exposing the value as one-dimensional does not change the model's multidimensional
computation.

A model may have multiple outputs. All outputs are returned as one struct value whose fields follow
the ONNX output names and order. Each field is independently a scalar or flat list.

For input tensors, at most one dimension may remain symbolic. The flat veloFlux list carries only
the total element count, so `[?, 3]` with six elements can be resolved to `[2, 3]`, while `[?, ?]`
or `[?, ?, 3]` cannot be resolved uniquely. Models with ambiguous symbolic input dimensions are
rejected during upload because silently choosing a shape could change the model computation. This
is a veloFlux boundary limitation, not a general ONNX limitation.

The model name is immutable. Uploading another bundle with the same name returns `409 Conflict`;
publishers must use a new versioned name for a new graph or input/output contract.

## Endpoints

| Method | Path | Behavior |
| --- | --- | --- |
| `POST` | `/models` | Validate and publish one immutable model bundle. |
| `GET` | `/models` | List persisted model metadata. |
| `GET` | `/models/:name` | Read one model's metadata. |
| `GET` | `/models/:name/status` | Read whether the current model revision has a prepared runtime graph. |
| `DELETE` | `/models/:name` | Delete model metadata and its bundle when no stored pipeline references it. |

The response metadata includes the model name, revision, ONNX SHA-256, model size, generated input
and output signatures, and test-vector count. Model files are stored below the storage base directory's `models/` directory; upload names
are validated before they are used in filesystem paths. Upload and delete operations acquire the
exclusive metadata and storage-operation guards and use a crash-recoverable publication journal.

`GET /models/:name/status` reports runtime state separately from persisted metadata:

```json
{
  "name": "mnist_8",
  "revision": 1,
  "model_sha256": "...",
  "prepared": true,
  "prepare_count": 1
}
```

`prepared` is true while at least one active pipeline runtime holds a strong reference to the
prepared graph. `prepare_count` counts successful preparations for the current model cache key
since process startup or the last metadata replacement. It is useful for verifying cache sharing;
two pipelines using the same model should keep this value at one. The endpoint does not report
process RSS or claim to measure allocator-level memory reclamation.

Deleting a model referenced by a stored pipeline returns `409 Conflict`. The dependency check scans
the persisted pipeline SQL before removing the model attachment or metadata.

Uploading a model persists its metadata and bundle but does not require every model to remain
loaded in memory. The process loads model metadata at startup. When a pipeline runtime has finished
planning and is about to create processors, it acquires and prepares only the models referenced by
that pipeline. If preparation fails, runtime creation/start fails before the first input row is
processed.

The prepared graph cache is keyed by model name, revision, and SHA-256. Pipelines share the
immutable prepared graph and weights: two pipelines using the same key hold multiple `Arc`
references to one prepared model, not multiple prepared graphs. Pipeline runtimes keep their own
execution context and temporary buffers. The registry stores only a `Weak` cache entry; strong
references are owned by active runtimes. This allows the graph to be released as soon as the last
runtime stops, is deleted, or is rebuilt. A stopped pipeline keeps its stored definition but does
not keep a prepared graph in memory.

Concurrent preparation is deduplicated by the complete model cache key. The first runtime prepares
the graph outside the cache lock, while other runtimes requesting the same key wait for that result.
Requests for different model keys do not wait for the preparation of another model. A failed
preparation is returned to the callers waiting for that attempt and can be retried by a later
runtime.

## Validation boundary

The manager performs bundle structure validation and ONNX signature extraction. The storage layer persists
model metadata in its own table and publishes the extracted bundle directory. The storage layer does
not depend on tract or parse ONNX graphs.

Model loading inspects the ONNX graph and adds the generated input/output tensor signatures to
`ModelMetadata`. The planner and the `onnx` function consume this same metadata. Signature extraction
happens before planning; it is not deferred to the first input row.

The manager parses and optimizes the ONNX graph during upload to extract the signature and reject
unsupported or malformed graph metadata. Execution of test vectors is still intentionally deferred;
the files are currently validated only as matching input/expected-output pairs.

When an ONNX pipeline runtime is built, the registry loads and prepares the graph before processor
creation. The persisted metadata remains the planner contract; the planner must not infer a model's
signature dynamically during row execution. `eval_row` performs inference against the already
prepared model and must not load the graph on the first row. A prepared graph is shared only when
it is immutable; mutable execution state remains owned by the pipeline's runtime.

Prepared-model release is driven by runtime ownership rather than by scanning all pipelines. The
`Arc` held by each active runtime is the source of truth for memory lifetime. A periodic patrol may
clean up dead weak cache entries and report diagnostics, but must not be required for correctness
or be used to release a model that an in-flight runtime still owns.

## Import and export

Models are included in the full storage import/export bundle. Exported archives contain the model
metadata in `manifest.json` and the complete bundle below `models/<name>/`. Import validates the
model name, metadata, model SHA-256, model size, model bundle metadata, and test-vector pair count before
publishing the model directory and metadata together.

The incremental `POST /storage/apply` endpoint does not accept model changes yet. Model bundles
must be transferred through `POST /storage/import`, because the full bundle is required for an
atomic file and metadata publication.
