#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  ./scripts/perf_pr_host/start_veloflux.sh \
    --main-cgroup /veloflux-ci/perf-pr-host-001/veloflux/manager \
    [--veloflux-bin ./veloflux-linux-x86_64] \
    [--config ./scripts/perf_pr_host/config.yaml] \
    [--data-dir ./data] \
    [-- <extra veloflux args>]

Starts the main veloflux process after joining the specified main-process cgroup.

Examples:
  worker-process mode:
    --main-cgroup /veloflux-ci/perf-pr-host-001/veloflux/manager

  thread-level mode:
    --main-cgroup /veloflux-ci/perf-pr-host-001/veloflux/main

Worker-process instances are still bound by
server.flow_instances[].cgroup.process_path.
In-process instances with dedicated runtimes are bound by
server.flow_instances[].cgroup.thread_path.
USAGE
}

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

VELOFLUX_BIN="${ROOT_DIR}/veloflux-linux-x86_64"
CONFIG_PATH="${ROOT_DIR}/scripts/perf_pr_host/config.yaml"
DATA_DIR="${ROOT_DIR}/data"
MAIN_CGROUP=""
EXTRA_ARGS=()

while [ $# -gt 0 ]; do
  case "$1" in
    --main-cgroup|--manager-cgroup)
      MAIN_CGROUP="${2:-}"; shift 2;;
    --veloflux-bin)
      VELOFLUX_BIN="${2:-}"; shift 2;;
    --config)
      CONFIG_PATH="${2:-}"; shift 2;;
    --data-dir)
      DATA_DIR="${2:-}"; shift 2;;
    --)
      shift
      EXTRA_ARGS=("$@")
      break;;
    -h|--help)
      usage; exit 0;;
    *)
      echo "unknown arg: $1" >&2
      usage
      exit 2;;
  esac
done

if [ -z "${MAIN_CGROUP}" ]; then
  echo "--main-cgroup is required" >&2
  usage
  exit 2
fi
if [ "${MAIN_CGROUP:0:1}" != "/" ]; then
  echo "--main-cgroup must be an absolute cgroup path like /veloflux-ci/..." >&2
  exit 2
fi

if [ "$(uname -s)" != "Linux" ]; then
  echo "cgroup binding is Linux-only" >&2
  exit 1
fi

if [ ! -x "${VELOFLUX_BIN}" ]; then
  echo "veloflux binary is not executable: ${VELOFLUX_BIN}" >&2
  exit 1
fi
if [ ! -f "${CONFIG_PATH}" ]; then
  echo "config file not found: ${CONFIG_PATH}" >&2
  exit 1
fi
mkdir -p "${DATA_DIR}"

CGROUP_PROCS="/sys/fs/cgroup${MAIN_CGROUP}/cgroup.procs"
if [ ! -f "${CGROUP_PROCS}" ]; then
  echo "main cgroup not found: ${CGROUP_PROCS}" >&2
  echo "run setup_cgroup.sh first and confirm --main-cgroup value" >&2
  exit 1
fi

echo "[start] joining main cgroup: ${MAIN_CGROUP}"
echo $$ > "${CGROUP_PROCS}"
echo "[start] exec veloflux: bin=${VELOFLUX_BIN} config=${CONFIG_PATH} data_dir=${DATA_DIR}"
exec "${VELOFLUX_BIN}" --data-dir "${DATA_DIR}" --config "${CONFIG_PATH}" "${EXTRA_ARGS[@]}"
