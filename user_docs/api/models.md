# Model API

The model API manages immutable ONNX model bundles. It is available when veloFlux is built with
the `onnx` feature.

## Upload a model

Send a ZIP archive to `POST /models` as the multipart field `file`. The archive contains one model
directory with `model.onnx`, `model.yaml`, and at least three input/expected-output test-vector
pairs:

```text
model_name/
├── model.onnx
├── model.yaml
└── test_vectors/
    ├── input-001.json
    └── expected_output-001.json
```

The `model.yaml` file only declares the model name. Input and output schemas are extracted from the
ONNX graph during upload and are not declared by the caller:

```yaml
name: acme.collision_v2.1
```

Each ONNX input and output is exposed to veloFlux as one positional parameter. A scalar tensor is
exposed as a scalar value; every other tensor is exposed as a flat one-dimensional list of scalar
values. The ONNX element type and tensor shape remain model metadata used to construct and decode
the actual tensor. Flattening does not change the model's multidimensional computation.

SQL arguments follow the ONNX input order. Multiple outputs are returned by the `onnx(...)` SQL
function as one struct value, with one field per ONNX output in output order.

The generated model metadata includes each input/output name, scalar element type, and tensor shape.
The persisted API representation uses scalar columns or `list<scalar>` columns and does not expose
nested tensor rank or dimensions as veloFlux schema.

An input tensor may contain at most one symbolic dimension. Because veloFlux receives a non-scalar
input as a flat list, it knows the total number of elements but not the individual dynamic
dimensions. Therefore `[?, 3]` with six values can be reconstructed as `[2, 3]`, but `[?, ?]` or
`[?, ?, 3]` is ambiguous. Such models are rejected during upload rather than being reshaped
silently. This restriction comes from the flat veloFlux model boundary, not from ONNX itself.

## Read and delete models

- `GET /models` lists model metadata.
- `GET /models/:name` reads one model's metadata.
- `DELETE /models/:name` deletes the model bundle and metadata when no stored pipeline references it.

Deleting a model referenced by a stored pipeline returns `409 Conflict`.

Model names are immutable. Uploading another bundle with the same name returns `409 Conflict`; use
a new name for a new model version or contract.

Model metadata includes the name, revision, ONNX SHA-256, model size, generated `inputs`, `outputs`,
and the test-vector count. The manager validates the bundle structure and extracts the ONNX
signature during upload, maintains the model metadata registry, and exposes lazy graph preparation
and row-based inference through `onnx(...)`.

When inference is enabled, model metadata and the ONNX signature are loaded during startup, while
the optimized executable graph is prepared on demand when a pipeline using the model starts.
Multiple pipelines can share the same immutable prepared graph and model weights through the runtime
cache. Each pipeline keeps its own inference row execution context, so errors from one pipeline do
not affect another. The cache is keyed by model name, revision, and SHA-256.

The runtime registry keeps only a weak cache entry for a prepared graph. Running pipelines own the
strong references, so a graph remains in memory while at least one pipeline runtime uses it and is
released after the last such runtime stops. Stopped pipelines retain their stored definitions but
do not retain prepared model graphs. Concurrent pipelines requesting the same model preparation
share one preparation attempt; preparation of different models is independent.

Runtime conversion supports bool, signed/unsigned integer, floating-point, and string scalar values.
Flat list values are converted to tensor elements for inputs and tensor elements are returned as flat
lists for outputs. Scalar tensors remain scalar values. All model outputs are returned as one struct
value.
Numeric conversions are strict: fractional, out-of-range, non-finite, or precision-losing values
are rejected instead of being silently cast.
