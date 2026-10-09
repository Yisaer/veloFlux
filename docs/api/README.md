# API Docs

This section contains runtime-facing HTTP and introspection contracts.

- `capabilities/`: Capability catalogs exposed by the manager.
- `streams/`: Stream-related schema and runtime inspection APIs.
- `pipelines.md`: Pipeline lifecycle and schedule API contract.
- `schema_registry.md`: Schema registry design and proto schema parser reference.
- `status.md`: Runtime health and status endpoint (`GET /status`).

- [Incremental Apply](storage_apply.md): `POST /storage/apply`, revisions and runtime results.
- [Full Import](storage_import.md): `POST /storage/import`, replacement and publication rollback.
- [Storage error comparison](storage_errors.md): init/Apply/import examples and retry contracts.
