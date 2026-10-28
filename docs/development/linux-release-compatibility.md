# Linux Release Compatibility

GNU/Linux release artifacts target **glibc 2.28 or newer**. This applies to
x86_64 release/debug/thin/video binaries, arm64 release/video binaries, and the
x86_64 FFI shared library. Android uses Bionic and QNX uses its own runtime;
neither is covered by this glibc policy. CentOS 7's glibc 2.17 is not supported.

## Build Environment

`.github/linux-release/Dockerfile` defines a native builder and a separate runtime
stage on a digest-pinned AlmaLinux 8.10 base. Rust 1.97.1 and protoc 3.20.3 archives
are pinned and checksum-verified for x86_64 and arm64. GCC Toolset 14 supplies a
modern compiler against the old userland. Distribution packages come from the
AlmaLinux 8 repositories; this is a compatibility baseline, not a claim of
bit-for-bit reproducible package resolution.

The native architecture is preserved: x86_64 jobs use Ubuntu 24.04 runners and
arm64 jobs use Ubuntu 24.04 ARM runners. There is no QEMU compilation. The host
runner's glibc is not linked into container builds.

The composite action in `.github/actions/build-linux-release` caches the builder
layers and the Cargo registry, Git dependency database, and target directory.
Compiler cache keys include architecture, builder recipe, lockfile, variant, and
source revision. Download caches are shared across variants; compiler output
caches stay separate so debug jobs do not restore unused release outputs. Compiler outputs from the previous Ubuntu/Bullseye environments are
not reused. The first build needs a new cache; later builds can restore compatible
dependencies. Runtime smoke images are derived from the cached runtime stage.

## Artifact Gates

`scripts/ci/check_linux_abi.py` examines ELF version requirements and rejects:

- GLIBC versions above 2.28 or private/non-numeric GLIBC requirements.
- GLIBCXX versions above 3.4.25 and CXXABI versions above 1.3.11, if present.
- Artifacts without the expected dynamic GLIBC version requirements.

It also prints dynamic dependencies. Video packages still require compatible
GStreamer and plugin libraries on the destination system. The glibc version
alone does not guarantee that these optional libraries are installed.

`scripts/ci/smoke_linux_release.py` launches the actual artifact in the clean
runtime stage, creates a file source and file sink through Manager, verifies a
row reaches the output file, and checks shutdown. The FFI package uses a small C
host linked to the produced library to exercise start, REST operations, and stop;
the host is a CI helper and is not included in the release package.

The runtime stage has no compiler or Rust toolchain. It has glibc 2.28, Python for
the test driver, CA certificates, and libgcc. Video artifacts use a separate
`video-runtime` stage with GStreamer runtime libraries. This
checks userland compatibility using the host kernel. It does **not** establish a
minimum kernel version or replace native hardware/performance testing.

`Linux Release Compatibility` runs all supported native x86_64, native arm64,
video, thin/debug, and FFI variants when the release infrastructure or lockfile changes. Every
Linux release variant also runs its ABI and runtime gates before packaging.

## Local Validation

From the repository root on x86_64 or arm64:

```bash
docker buildx build --load --target builder -t vf-linux-builder .github/linux-release
docker buildx build --load --target runtime -t vf-linux-runtime .github/linux-release
docker run --rm -v "$PWD:/workspace" -w /workspace \
  -e CARGO_HOME=/workspace/target/linux-release-cargo \
  -e CARGO_TARGET_DIR=/workspace/target/linux-release \
  vf-linux-builder bash scripts/ci/build_linux_release.sh release target/release-artifacts/local
docker run --rm -v "$PWD:/workspace:ro" -w /tmp vf-linux-runtime \
  python3 /workspace/scripts/ci/smoke_linux_release.py \
  /workspace/target/release-artifacts/local/veloflux
```

Replace `release` with `debug`, `thin`, `video`, or `ffi` as needed. For video,
build the `video-runtime` stage instead of `runtime`. For FFI, pass
`/workspace/target/release-artifacts/local/ffi-smoke-host --ffi` to the smoke driver.
Container-created cache/output files may need their ownership restored for local
use; the GitHub composite action handles ownership before saving caches.

Keep the ABI limit independent of runner updates. A dependency that cannot build
or run on the baseline must be investigated before raising the published minimum.
