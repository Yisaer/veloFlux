#!/usr/bin/env bash
set -euo pipefail

ROOT="${CGROUP_ROOT:-/sys/fs/cgroup/veloflux-runtime-demo}"
DEFAULT_BIN="./target/release/runtime_thread_cgroup_demo"
BIN_PATH="${DEFAULT_BIN}"

usage() {
  cat <<'EOF'
Usage: run_demo.sh [--skip-setup] [--bin <path>] [--] [demo args...]

Examples:
  sudo ./run_demo.sh --bin ./target/release/runtime_thread_cgroup_demo -- --duration-secs 20
  sudo ./run_demo.sh -- --duration-secs 20 --worker-threads 1

Environment variables:
  CGROUP_ROOT   Threaded domain root. Default: /sys/fs/cgroup/veloflux-runtime-demo
EOF
}

require_linux() {
  if [[ "$(uname -s)" != "Linux" ]]; then
    echo "This demo requires Linux." >&2
    exit 1
  fi
}

SKIP_SETUP=0
DEMO_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-setup)
      SKIP_SETUP=1
      shift
      ;;
    --bin)
      BIN_PATH="${2:-}"
      if [[ -z "${BIN_PATH}" ]]; then
        echo "missing value for --bin" >&2
        exit 1
      fi
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    --)
      shift
      DEMO_ARGS=("$@")
      break
      ;;
    *)
      DEMO_ARGS+=("$1")
      shift
      ;;
  esac
done

require_linux

if [[ ${EUID} -ne 0 ]]; then
  echo "run_demo.sh should be executed as root or via sudo." >&2
  exit 1
fi

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

if [[ ${SKIP_SETUP} -eq 0 ]]; then
  ./setup_cgroup.sh setup
fi

if [[ ! -x "${BIN_PATH}" ]]; then
  echo "demo binary is not executable: ${BIN_PATH}" >&2
  echo "build it first with: cargo build --release" >&2
  exit 1
fi

cleanup() {
  local rc=$?
  if [[ ${SKIP_SETUP} -eq 0 ]]; then
    ./setup_cgroup.sh status || true
  fi
  exit ${rc}
}

trap cleanup EXIT

echo $$ > "${ROOT}/cgroup.procs"
"${BIN_PATH}" "${DEMO_ARGS[@]}"
