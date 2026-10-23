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
