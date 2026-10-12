# ONNX Model Management API

## Availability

These endpoints are compiled only when veloFlux is built with the `onnx` Cargo feature. A
binary built without that feature does not expose model management routes.

The current implementation manages immutable ONNX model bundles. It does not execute inference
yet; SQL registration, planner binding, and tract-backed runtime execution are deferred to the
next implementation phase.

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

The model name is immutable. Uploading another bundle with the same name returns `409 Conflict`;
publishers must use a new versioned name for a new graph or input/output contract.

## Endpoints

| Method | Path | Behavior |
| --- | --- | --- |
| `POST` | `/models` | Validate and publish one immutable model bundle. |
| `GET` | `/models` | List persisted model metadata. |
| `GET` | `/models/:name` | Read one model's metadata. |
| `DELETE` | `/models/:name` | Delete model metadata and its bundle. |

The response metadata includes the model name, revision, ONNX SHA-256, model size, and test-vector
count. Model files are stored below the storage base directory's `models/` directory; upload names
are validated before they are used in filesystem paths. Upload and delete operations acquire the
exclusive metadata and storage-operation guards and use a crash-recoverable publication journal.

## Validation boundary

The manager performs bundle structure and model-card validation. The storage layer persists model
metadata in its own table and publishes the extracted bundle directory. The storage layer does
not depend on tract or parse ONNX graphs.

The following checks are intentionally deferred:

- ONNX graph loading and operator/dtype/shape/opset validation through tract;
- executing test vectors against the loaded model;
- model dependency checks when pipelines are deleted or changed;

## Import and export

Models are included in the full storage import/export bundle. Exported archives contain the model
metadata in `manifest.json` and the complete bundle below `models/<name>/`. Import validates the
model name, metadata, model SHA-256, model size, model card, and test-vector pair count before
publishing the model directory and metadata together.

The incremental `POST /storage/apply` endpoint does not accept model changes yet. Model bundles
must be transferred through `POST /storage/import`, because the full bundle is required for an
atomic file and metadata publication.
