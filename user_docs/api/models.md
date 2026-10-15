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

The `model.yaml` schema uses the same column definition as a stream schema. Scalar columns specify
`name` and `data_type`. A list uses `element`, an optional `cardinality` defines its fixed length,
and a struct uses `fields`:

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
  - name: status
    data_type: struct
    fields:
      - name: safe
        data_type: bool
```

Composite list elements and list fields inside structs must declare `cardinality`; this lets the
runtime preserve boundaries when decoding tensor values. Ambiguous recursive schemas are rejected
when the model is uploaded.

`inputs` and `outputs` are ordered. SQL arguments follow the input order. Multiple outputs are
returned by the `onnx(...)` SQL function as one struct value.

The schema is defined from veloFlux's data perspective. ONNX tensor shape and rank are not model
API fields; the runtime validates the input values against the prepared graph when inference runs.

## Read and delete models

- `GET /models` lists model metadata.
- `GET /models/:name` reads one model's metadata.
- `DELETE /models/:name` deletes the model bundle and metadata when no stored pipeline references it.

Deleting a model referenced by a stored pipeline returns `409 Conflict`.

Model names are immutable. Uploading another bundle with the same name returns `409 Conflict`; use
a new name for a new model version or contract.

Model metadata includes the name, revision, ONNX SHA-256, model size, `inputs`, `outputs`, and the
test-vector count. The manager validates the bundle structure and schema, maintains the model
metadata registry, and exposes lazy model preparation and row-based inference through `onnx(...)`.

When inference is enabled, model metadata is loaded during startup, while ONNX graphs are loaded
on demand when a pipeline using the model starts. Multiple pipelines can share the same immutable
prepared graph and model weights through the runtime cache. Each pipeline keeps its own inference
row execution context, so errors from one pipeline do not affect another. The cache is keyed by
model name, revision, and SHA-256.

Runtime conversion supports bool, signed/unsigned integer, floating-point, and string scalar values.
List and struct values are recursively flattened into tensor leaves for inputs and decoded back into
the declared recursive schema for outputs. All model outputs are returned as one struct value.
Numeric conversions are strict: fractional, out-of-range, non-finite, or precision-losing values
are rejected instead of being silently cast.
