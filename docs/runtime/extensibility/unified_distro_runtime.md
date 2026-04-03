# Single Entrypoint Distro Architecture

## Background

veloFlux needs to support multiple customer-oriented distributions while keeping
the core runtime maintainable in a single repository. The existing runtime
already exposes registry-based extension points for decoders, encoders,
mergers, schema parsers, and related capabilities. That extension boundary is
the correct place for distro-specific behavior.

The remaining design question is not whether distros should exist, but how they
should integrate with startup, worker-process mode, testing, and release
selection.

This document defines the target architecture for distro support in the
runtime. It is written from the perspective of the desired steady-state design,
not as a patch description for any specific branch.

The intended product model is:

- `main` contains the core runtime plus the SDV base product.
- customer products are derived from `main` through long-lived branches such as
  `custom_1`.
- code may remain separated into `src/` and `distros/` for maintainability, but
  the branch as a whole still represents one product.

## Problem Statement

We want a monorepo that can host the core engine and multiple customer-specific
runtime extensions without forcing each customer variant to maintain a separate
copy of the core startup sequence.

The core problem is boundary placement:

- Customer differences should be expressed through registry registration.
- Runtime role switching between manager mode and worker mode should remain a
  core runtime concern.
- Product identity should be chosen once per branch and reflected through the
  root runtime configuration, not by introducing multiple service boot paths
  that each reimplement startup logic.

If distro-specific service binaries own their own `main()` and normal startup
flow, the repository accumulates duplicated boot logic, duplicated signal
handling, duplicated worker integration, and a higher risk that normal mode and
worker mode diverge in behavior.

## Design Goals

- Keep a single service entrypoint in the root crate.
- Keep distro-specific behavior outside the core startup sequence.
- Use the same distro registration path in both normal mode and worker mode.
- Keep exactly one active product registration path per branch.
- Allow distro-specific code, tests, docs, benchmarks, and utility tools to
  live in distro-owned crates.
- Preserve the existing registry-based extension model as the primary boundary
  for customer customization.

## Non-Goals

- This document does not introduce new plugin traits or registries.
- This document does not define dynamic plugin loading.
- This document does not require all distro-specific code to move into the core
  crate.
- This document does not prescribe release packaging details beyond the runtime
  selection model.
- This document does not assume that all products must coexist as selectable
  variants within the same branch.

## Design Principles

### A distro is a runtime extension, not an alternative service boot path

A distro adds runtime capabilities such as decoders, encoders, mergers, schema
parsers, or similar registry-based integrations. Those capabilities modify what
the runtime can resolve and instantiate. They should not redefine how the
runtime enters manager mode or worker mode.

### Runtime role selection is not distro selection

The `--worker` path exists because the runtime supports multiple execution
roles. That is orthogonal to customer differentiation. A distro must not need a
different worker boot sequence from the core runtime. It only needs its
registrations to be present inside both the manager process and worker process.

### Product registration must be explicit

Each branch should expose one active product registration path. The root runtime
must know which registration function to call, and that decision must not be
ambiguous at build time.

This does not require every branch to expose multiple distro choices through a
shared feature matrix. In the common case, a branch simply builds one product.

### Startup logic must stay centralized

Bootstrap, signal handling, manager startup, worker startup, shared-registry
construction, metrics initialization, and profiling initialization are runtime
infrastructure. They should remain in the root runtime crate so changes to
startup semantics are implemented and reviewed in one place.

## Architecture Overview

The target layout is:

```text
root package: veloflux
├── src/main.rs
│   └── the only service entrypoint
├── src/worker.rs
│   └── shared worker-process runtime
├── src/distro.rs
│   └── branch-local product registration dispatch
├── src/bootstrap.rs
│   └── shared bootstrap
├── src/server.rs
│   └── shared manager/server startup
└── distros/
    ├── sdv/
    │   ├── src/lib.rs
    │   │   └── register(instance) for the base SDV product
    │   ├── tests/
    │   └── benches/
    └── custom_<n>/
        └── src/lib.rs
            └── register(instance) for product-specific extensions layered on SDV
```

The root package owns service startup. Distro crates provide registration
functions and optional tooling, but they do not own the service boot sequence.
The branch as a whole still represents one product, even if its code is split
across multiple crates for management purposes.

## Startup Model

### Normal mode

The root `src/main.rs` is the only service entrypoint. In normal mode it:

1. Parses CLI arguments.
2. Loads config and initializes logging through shared bootstrap code.
3. Prepares the default `FlowInstance` and its registries.
4. Applies the branch's active product registration to that instance.
5. Starts the shared server runtime.

This keeps service startup semantics identical across distros.

### Worker mode

When the same service binary is invoked with `--worker`, the root runtime
enters worker mode through the shared worker path. In worker mode it:

1. Parses worker-specific arguments such as instance id and config path.
2. Loads config and validates the worker process spec.
3. Constructs the default instance used to source shared registries.
4. Applies the same active product registration to that default instance.
5. Clones the resulting shared registries into the dedicated worker instance.
6. Starts the worker server.

The requirement is strict: the same product registration function must be used
in both normal mode and worker mode. This is a correctness requirement, not an
implementation detail.

If normal mode and worker mode do not share the exact same registration path,
the manager process may accept a pipeline definition that the worker process
cannot instantiate.

## Distro Contract

Each distro crate is a library-first runtime extension crate.

In the expected branch model:

- `distros/sdv` provides the base product capabilities that ship on `main`;
- a customer crate such as `distros/custom_1` may depend on `distros/sdv` and
  append customer-specific registrations;
- the customer branch then selects `custom_1` as its active product
  registration, while `main` selects `sdv`.

It should:

- expose a registration function such as `register(instance: &flow::FlowInstance)`;
- register decoders, encoders, mergers, schema parsers, and other
  registry-based extensions;
- keep distro-specific tests, docs, benchmarks, and utility code close to the
  distro implementation;
- optionally expose additional non-service binaries for tooling workflows.

It should not:

- own the service `main()` entrypoint;
- duplicate bootstrap, signal handling, manager startup, or worker startup;
- redefine manager and worker semantics;
- require a separate service startup path to activate its capabilities.

This contract keeps distro code focused on capability registration and allows
the core runtime to remain the single owner of startup behavior.

## Cargo Selection Model

The root `Cargo.toml` should describe the active product for the current
branch.

For the base product branch, the simplest model is direct dependency wiring:

```toml
[dependencies]
veloflux_sdv = { package = "veloflux-sdv", path = "distros/sdv" }
```

Then `src/distro.rs` in `main` can simply call:

```rust
pub fn register_selected_distro(instance: &flow::FlowInstance) {
    veloflux_sdv::register(instance);
}
```

For a customer branch such as `custom_1`, the branch may add:

```toml
[dependencies]
veloflux_sdv = { package = "veloflux-sdv", path = "distros/sdv" }
veloflux_custom_1 = { package = "veloflux-custom-1", path = "distros/custom_1" }
```

and `src/distro.rs` becomes:

```rust
pub fn register_selected_distro(instance: &flow::FlowInstance) {
    veloflux_custom_1::register(instance);
}
```

Inside `veloflux_custom_1::register(instance)`, the customer crate should call
`veloflux_sdv::register(instance)` first, then append its own registrations.

This keeps product identity branch-local and explicit without forcing the
repository to maintain a long-lived multi-product feature matrix in `main`.

## Branch and Release Model

The repository should follow a single-product-per-branch model.

Examples:

- `main` contains `core + sdv` and represents the base product;
- `custom_1` is derived from `main` and represents `core + sdv + custom_1`;
- `custom_2` is derived from `main` and represents `core + sdv + custom_2`.

This model has two important properties:

- product code can remain separated into crates for maintainability;
- source delivery remains simple because a customer branch is already the exact
  product to hand over.

Under this model, branches may differ in:

- which distro crate is selected by `src/distro.rs`;
- whether additional distro crates such as `distros/custom_1` exist;
- packaging, tests, and branch-local release workflow details.

They should not differ in service boot code. The root `src/main.rs` and shared
worker startup path remain the same shape across branches.

## Testing and CI Model

The testing model follows the same separation of concerns:

- core unit and integration tests run on every branch;
- SDV tests run on `main` and also on customer branches that derive from SDV;
- customer-specific tests run on the customer branch that introduces them;
- distro-specific e2e tests remain colocated with distro code;
- startup behavior is validated once through the shared root runtime path.

The CI advantage is that startup and worker behavior are no longer duplicated
across distros. Changes to runtime startup code affect all branches uniformly
because they all build the same root service entrypoint shape.

## Tradeoffs

This design has clear advantages:

- one service startup path;
- less duplicated runtime glue code;
- stronger consistency between manager mode and worker mode;
- clearer separation between runtime roles and customer capabilities;
- easier review of startup changes and safer long-term maintenance.

It also introduces explicit constraints:

- the root crate must know which product registration to call on each branch;
- customer crates should layer on top of the SDV base crate instead of copying
  SDV code;
- distro crates become library-first rather than service-binary-first;
- customer delivery still depends on keeping branch-local product wiring clean.

These tradeoffs are acceptable because they reduce operational divergence in the
part of the codebase that is hardest to keep consistent: startup behavior.

## Recommended Repository Shape

The repository should treat distro code as an extension surface around a shared
runtime core:

- core startup remains under `src/`;
- SDV registration code remains under `distros/sdv/`;
- customer-specific registration code remains under `distros/custom_<n>/`;
- root `src/main.rs` remains the only service entrypoint;
- worker mode remains part of the shared runtime implementation;
- distro crates may still ship helper tools when needed.

This shape achieves the original monorepo and CI-sharing goals without turning
product identity into a boot-path concern.

## Appendix: Migration from the current distro-oriented implementation

If the repository already contains a distro-oriented implementation that gives a
distro crate ownership of the service binary, the migration should move toward
the target design above.

The migration direction is:

1. Restore the root `src/main.rs` as the only service entrypoint.
2. Keep the shared worker runtime in the root crate.
3. Keep `distros/sdv` as a library-first base product crate.
4. Remove distro-owned service binaries such as `distros/<name>/src/bin/veloflux.rs`.
5. Keep distro-owned tooling binaries when they serve non-service workflows.
6. Introduce a root-level product registration layer in `src/distro.rs`.
7. On `main`, let that layer dispatch to the SDV base registration.
8. On customer branches, let that layer dispatch to the customer crate, which
   itself reuses SDV registration and appends customer-specific registration.
9. Ensure both normal mode and worker mode call that same product registration
   path.

This migration preserves the monorepo and registry-extension goals while
re-centering startup behavior in the shared runtime.
