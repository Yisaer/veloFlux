#!/usr/bin/env bash
set -euo pipefail

variant="${1:?expected release, debug, thin, video, or ffi}"
output="${2:?expected output directory}"
case "$(uname -m)" in
  x86_64) rust_target=x86_64-unknown-linux-gnu ;;
  aarch64) rust_target=aarch64-unknown-linux-gnu ;;
  *) echo 'unsupported native architecture' >&2; exit 1 ;;
esac
test "$(getconf GNU_LIBC_VERSION)" = 'glibc 2.28'
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target/linux-release}"
profile=release
args=(build --locked --target "$rust_target")
case "$variant" in
  release) args+=(--release --bin veloflux) ;;
  debug) profile=debug; args+=(--features deadlock_detection --bin veloflux) ;;
  thin) profile=release-thin; args+=(--profile release-thin --no-default-features --bin veloflux) ;;
  video) args+=(--release --features video_gstreamer --bin veloflux) ;;
  ffi) args+=(--release -p veloflux-ffi) ;;
  *) echo "unsupported variant: $variant" >&2; exit 1 ;;
esac
git config --global --add safe.directory "$PWD"
cargo "${args[@]}"
mkdir -p "$output"
if [ "$variant" = ffi ]; then
  cp "$CARGO_TARGET_DIR/$rust_target/$profile/libveloflux_ffi.so" "$output/"
  cc -std=c11 -Wall -Wextra -Werror -Isrc/ffi/include \
    scripts/ci/linux_release_ffi_host.c -L"$output" -lveloflux_ffi \
    -Wl,-rpath,\$ORIGIN -o "$output/ffi-smoke-host"
  python3 scripts/ci/check_linux_abi.py "$output/libveloflux_ffi.so" "$output/ffi-smoke-host"
else
  cp "$CARGO_TARGET_DIR/$rust_target/$profile/veloflux" "$output/veloflux"
  python3 scripts/ci/check_linux_abi.py "$output/veloflux"
fi
