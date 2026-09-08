#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

cargo fetch
nng_cmake=$(find "$HOME/.cargo/registry/src" \
  -path '*/nng-sys-*/nng/CMakeLists.txt' -print -quit)
test -n "$nng_cmake"
nng_source=$(dirname "$nng_cmake")

cmake -S "$nng_source" -B target/nng-build \
  -DBUILD_SHARED_LIBS=ON \
  -DNNG_TESTS=OFF \
  -DNNG_TOOLS=OFF \
  -DNNG_ENABLE_TLS=OFF \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_INSTALL_LIBDIR=lib \
  -DCMAKE_INSTALL_PREFIX="$PWD/target/nng"
cmake --build target/nng-build --parallel
cmake --install target/nng-build

NNG_NO_VENDOR=1 \
NNG_STATIC=0 \
NNG_DIR="$PWD/target/nng" \
  cargo build -p veloflux-ffi --features nng_pubsub

cc -std=c11 -Wall -Wextra -Werror \
  -Itarget/nng/include \
  -Isrc/ffi/include \
  tests/ffi_nng_inproc_host.c \
  -Ltarget/debug -lveloflux_ffi \
  -Ltarget/nng/lib -lnng \
  -Wl,-rpath,"$PWD/target/debug:$PWD/target/nng/lib" \
  -o target/ffi-nng-inproc-host

ldd target/debug/libveloflux_ffi.so
ldd target/ffi-nng-inproc-host

cat > target/ffi-nng-config.yaml <<'EOF'
properties: {}
server:
  manager_addr: "127.0.0.1:18080"
  flow_instances:
    - id: default
      backend: in_process
EOF

target/ffi-nng-inproc-host \
  target/ffi-nng-config.yaml \
  inproc://velo-flux-ci-input \
  inproc://velo-flux-ci-output \
  > target/ffi-nng-host.log 2>&1 &
host_pid=$!

cleanup() {
  kill "$host_pid" 2>/dev/null || true
  wait "$host_pid" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 60); do
  if curl --fail --silent http://127.0.0.1:18080/streams >/dev/null; then
    break
  fi
  if ! kill -0 "$host_pid" 2>/dev/null; then
    cat target/ffi-nng-host.log
    exit 1
  fi
  sleep 1
done

curl --fail --silent -X POST http://127.0.0.1:18080/streams \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "ffi_nng_input",
    "revision": 1,
    "type": "nng_pubsub",
    "schema": {
      "type": "json",
      "props": {
        "columns": [{"name": "v", "data_type": "int64"}]
      }
    },
    "props": {
      "url": "inproc://velo-flux-ci-input",
      "topic": "topic/can"
    },
    "decoder": {"type": "json", "props": {}}
  }'

curl --fail --silent -X POST http://127.0.0.1:18080/pipelines \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "ffi_nng_inproc",
    "revision": 1,
    "sql": "SELECT v FROM ffi_nng_input",
    "sinks": [{
      "id": "nng_output",
      "type": "nng_pubsub",
      "props": {
        "url": "inproc://velo-flux-ci-output",
        "topic": "topic/can"
      },
      "encoder": {"type": "json", "props": {}}
    }]
  }'

curl --fail --silent -X POST \
  http://127.0.0.1:18080/pipelines/ffi_nng_inproc/start

set +e
wait "$host_pid"
host_status=$?
set -e
trap - EXIT
cat target/ffi-nng-host.log
exit "$host_status"
