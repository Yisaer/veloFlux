# Manager REST API

## Trust and deployment model

VeloFlux runs in a single trust domain on a private LAN. Management API callers
must be trusted administrators authorized to control the engine and use its
server-side file access capabilities. Authentication, authorization, and API
access protection belong to the deployment environment and are outside the
scope of the open-source engine. LAN membership alone does not establish trust.

Server-local file access uses the service process's filesystem permissions.
Operators control the accessible files through service accounts, filesystem
permissions, container mounts, or OS access controls. There is no general
data-directory sandbox; endpoint-specific restrictions, including upload name
and archive entry validation, still apply.

Read the [full trust and deployment model](../../docs/api/README.md#trust-and-deployment-model)
before exposing or integrating the API.

## Resource write methods

Resource PUT endpoints create when absent and replace when present: creation
returns `201`, while replacement or identical replay returns `200`. PUT carries
the complete writable definition and requires a resource revision. A lower
revision or the same revision with changed content returns `409`. An identical
revision has no runtime side effects. Resource create POST endpoints remain
create-only.

Read the [resource write method principles](../../docs/api/README.md#resource-write-methods)
for defaults, immutable fields, tombstones, cascade, and the distinction from
snapshot import.

## Endpoints

- [Streams](stream.md)
- [Tables](table.md)
- [Pipelines](pipeline.md)
- [Schemas](schema.md)
- [Files](upload.md)
- [Functions](function.md)
- [WASM UDFs](udf.md)
- [Models](models.md)
- [Incremental Apply](apply.md)
- [Export](export.md)
- [Import](import.md)
