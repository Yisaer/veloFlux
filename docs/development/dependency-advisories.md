# Dependency advisory review

Reviewed on 2026-10-27 using cargo-deny 0.20.2, the workspace lockfile, all
workspace members, all features and all target dependencies. Re-run
`cargo deny --workspace --locked check` against the current RustSec database;
this review is not a substitute for future scans.

Dependabot security updates and grouping are managed in the GitHub repository
settings. The version-update configuration file is removed to stop scheduled
ordinary version update PRs. The cargo-deny workflow continues to check the
full dependency graph against RustSec independently of Dependabot.

## Updated dependencies

- Wasmtime moves from 24 to the patched 36.0.19 release line. The existing UDF
  ABI, memory/table limits and epoch interruption remain unchanged. Persisted
  UDFs contain WebAssembly bytes, not version-specific compiled Wasmtime caches.
- Wasmtime's unused disk compilation cache is disabled at build time. The
  explicit feature list preserves the other defaults from 36.0.19; engine and
  module reuse in memory are unchanged. This removes the Wasmtime cache's zstd
  dependency path.
- Flow uses zstd 0.14.3 or newer, with zstd-safe 8.2.0 or newer and zstd-sys
  2.1.2 or newer, addressing the zstd advisories reported by the security job
  on 2026-10-27. Arrow and Parquet move together to 60.0.0 so Parquet also uses
  the patched zstd release line. Existing history schemas and compression
  support are retained.
- ZIP moves to 9.x. Entry names are now decoded through a fallible API; import
  and schema extraction propagate decoding errors before writing files. Existing
  path, symlink, duplicate-entry and size validation remains in place.
- pprof moves to 0.15 and Prometheus to 0.14, eliminating protobuf 2.x. Profile
  and metrics endpoints retain their existing wire formats.
- Compatible lockfile updates address advisories in anyhow, crossbeam-epoch,
  h2, lz4_flex, memmap2, rand, rustls, rustls-webpki, Tokio and root certificates.
  Yanked chacha20, lz4_flex and spin versions are replaced.
- The unused `tract` command-line package is removed; ONNX execution continues
  to use `tract-onnx`. The test-only rumqttd dependency disables TLS/WebSocket
  defaults because the embedded test brokers use plain TCP. Production MQTT TLS
  remains enabled through rumqttc and rustls.

The SDV test dependencies also use ZIP 9.x and serial_test 4.x, removing the
affected scc 2.x dependency. Unpublished workspace crates are excluded only from
license checking; their dependencies remain subject to every policy.

## Unreachable WebPKI advisories

The following advisories still match rustls-webpki 0.102.8 in the lockfile:

| Advisory | Affected operation |
| --- | --- |
| [RUSTSEC-2026-0049](https://rustsec.org/advisories/RUSTSEC-2026-0049.html) | CRL distribution-point matching |
| [RUSTSEC-2026-0098](https://rustsec.org/advisories/RUSTSEC-2026-0098.html) | URI name constraints |
| [RUSTSEC-2026-0099](https://rustsec.org/advisories/RUSTSEC-2026-0099.html) | Wildcard name constraints |
| [RUSTSEC-2026-0104](https://rustsec.org/advisories/RUSTSEC-2026-0104.html) | CRL parsing |
| [RUSTSEC-2026-0382](https://rustsec.org/advisories/RUSTSEC-2026-0382.html) | Distrusted intermediate verification |

`cargo tree --workspace --all-features -i rustls-webpki@0.102.8` identifies
rumqttc 0.25.1 as its only direct dependent. Inspecting that published crate's
Rust sources finds only the `webpki::Error` type in
[`tls::Error::WebPki`](https://github.com/bytebeamio/rumqtt/blob/rumqttc-0.25.1/rumqttc/src/tls.rs).
There are no calls into that version's certificate verification or CRL parser.
The TLS connector uses tokio-rustls and rustls 0.23.46, whose actual verifier
uses patched rustls-webpki 0.103.16. These five affected paths are therefore
unreachable in the reviewed dependency graph.

The five IDs are explicitly ignored, with reasons, in `deny.toml`. Because
cargo-deny 0.20.2 ignores IDs globally, a separate crate ban rejects every direct
consumer of WebPKI below 0.103.16 except rumqttc. Additional bans require rumqttc
to remain at the reviewed 0.25.1 version until this exception is reassessed.
Thus downgrading the real verifier or adding another user of the old verifier
fails the dependency policy check. Both advisory and policy jobs must pass.
Remove these exceptions when an upstream rumqttc release drops or upgrades its
unused WebPKI dependency; do not extend them to new call paths.

## Deferred maintenance replacements

These are maintenance advisories with no patched version, rather than reports
of a specific exploitable defect. They remain explicit maintenance debt. Their
IDs are individually exempted and crate bans limit them to the reviewed direct
dependents. New vulnerability IDs are not ignored.

| Advisory / dependency | Reason for retaining it | Follow-up |
| --- | --- | --- |
| [RUSTSEC-2025-0141](https://rustsec.org/advisories/RUSTSEC-2025-0141.html), bincode 1.3.3 | Storage records and checkpoint manifests use its serialized format. | Replace with a separately validated persistence-format migration. |
| [RUSTSEC-2025-0057](https://rustsec.org/advisories/RUSTSEC-2025-0057.html), fxhash 0.2.1 | Required by Wasmtime's fxprof-processed-profile dependency. | Track an upstream replacement; reassess when updating Wasmtime/profiling. |
| [RUSTSEC-2024-0436](https://rustsec.org/advisories/RUSTSEC-2024-0436.html), paste 1.0.15 | Compile-time macro dependency of GStreamer, option-operations and jemalloc control. | Adopt upstream replacements without changing these libraries solely to rename the macro package. |
| [RUSTSEC-2025-0134](https://rustsec.org/advisories/RUSTSEC-2025-0134.html), rustls-pemfile 2.2.0 | Latest published rumqttc requires this wrapper around rustls-pki-types PEM parsing. | Upgrade when rumqttc adopts `PemObject` directly. |
| [RUSTSEC-2024-0449](https://rustsec.org/advisories/RUSTSEC-2024-0449.html), unsafe-libyaml 0.2.11 | serde_yaml uses it for configuration and import/export. | Migrate to a maintained safe-Rust YAML implementation with configuration and export/import compatibility checks. |

Re-review these exceptions when their dependency paths or versions change.
`unused-ignored-advisory = "deny"` requires removing obsolete ignores. New
unmaintained dependencies, new vulnerabilities and yanked versions still fail CI.

## Validation and performance scope

Use the existing UDF, ZIP import/export, schema extraction, model-runtime and
MQTT integration tests when validating updates. Check that CPU profiles remain
readable and metrics retain their text format. No persisted storage format is
changed in this update.

Removing unused dependencies reduces build scope. Dependency updates can change
code generation, allocation and runtime performance; functional checks do not
establish throughput or latency equivalence. Existing benchmark and daily
performance workflows remain responsible for measuring those effects.
