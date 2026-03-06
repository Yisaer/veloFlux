#!/usr/bin/env bash
set -euo pipefail

ROOT="${CGROUP_ROOT:-/sys/fs/cgroup/veloflux-runtime-demo}"
CPU_A="${CPU_MAX_A:-20000 100000}"
CPU_B="${CPU_MAX_B:-80000 100000}"

usage() {
  cat <<'EOF'
Usage: setup_cgroup.sh <setup|status|cleanup>

Environment variables:
  CGROUP_ROOT   Root cgroup path. Default: /sys/fs/cgroup/veloflux-runtime-demo
  CPU_MAX_A     cpu.max for rt_a. Default: 20000 100000
  CPU_MAX_B     cpu.max for rt_b. Default: 80000 100000
EOF
}

require_linux() {
  if [[ "$(uname -s)" != "Linux" ]]; then
    echo "This demo requires Linux." >&2
    exit 1
  fi
}

require_cgroup_v2() {
  local fs_type
  fs_type="$(stat -fc %T /sys/fs/cgroup)"
  if [[ "${fs_type}" != "cgroup2fs" ]]; then
    echo "This demo requires cgroup v2. Found: ${fs_type}" >&2
    exit 1
  fi
}

require_root_path() {
  if [[ "${ROOT}" != /sys/fs/cgroup/* ]]; then
    echo "CGROUP_ROOT must stay under /sys/fs/cgroup. Got: ${ROOT}" >&2
    exit 1
  fi
}

parent_dir() {
  dirname "${ROOT}"
}

move_threads() {
  local from_path="$1"
  local to_path="$2"
  [[ -f "${from_path}/cgroup.threads" ]] || return 0

  mapfile -t tids < <(grep -E '^[0-9]+$' "${from_path}/cgroup.threads" || true)
  for tid in "${tids[@]}"; do
    echo "${tid}" > "${to_path}/cgroup.threads"
  done
}

move_processes() {
  local from_path="$1"
  local to_path="$2"
  [[ -f "${from_path}/cgroup.procs" ]] || return 0

  mapfile -t pids < <(grep -E '^[0-9]+$' "${from_path}/cgroup.procs" || true)
  for pid in "${pids[@]}"; do
    echo "${pid}" > "${to_path}/cgroup.procs"
  done
}

assert_empty_file() {
  local path="$1"
  local label="$2"
  if [[ -f "${path}" ]] && grep -Eq '^[0-9]+$' "${path}"; then
    echo "${label} is still busy: ${path}" >&2
    cat "${path}" >&2
    exit 1
  fi
}

ensure_controller() {
  local parent
  parent="$(parent_dir)"
  if ! grep -qw cpu "${parent}/cgroup.controllers"; then
    echo "cpu controller is unavailable under ${parent}" >&2
    exit 1
  fi
  if ! grep -qw cpu "${parent}/cgroup.subtree_control"; then
    echo "+cpu" > "${parent}/cgroup.subtree_control"
  fi
}

ensure_child_controller() {
  local path="$1"
  if ! grep -qw cpu "${path}/cgroup.controllers"; then
    echo "cpu controller is unavailable under ${path}" >&2
    exit 1
  fi
  if ! grep -qw cpu "${path}/cgroup.subtree_control"; then
    echo +cpu > "${path}/cgroup.subtree_control"
  fi
}

ensure_threaded_child() {
  local path="$1"
  mkdir -p "${path}"
  local current_type
  current_type="$(<"${path}/cgroup.type")"
  if [[ "${current_type}" == "domain" || "${current_type}" == "domain invalid" ]]; then
    echo threaded > "${path}/cgroup.type"
  fi
}

setup() {
  require_linux
  require_cgroup_v2
  require_root_path
  ensure_controller

  mkdir -p "${ROOT}"
  ensure_child_controller "${ROOT}"
  mkdir -p "${ROOT}/rt_a" "${ROOT}/rt_b"
  ensure_threaded_child "${ROOT}/rt_a"
  ensure_threaded_child "${ROOT}/rt_b"

  echo "${CPU_A}" > "${ROOT}/rt_a/cpu.max"
  echo "${CPU_B}" > "${ROOT}/rt_b/cpu.max"

  echo "configured ${ROOT}"
  status
}

status() {
  require_linux
  require_cgroup_v2
  require_root_path

  if [[ ! -d "${ROOT}" ]]; then
    echo "cgroup root does not exist: ${ROOT}" >&2
    exit 1
  fi

  for path in "${ROOT}" "${ROOT}/rt_a" "${ROOT}/rt_b"; do
    echo "== ${path} =="
    [[ -f "${path}/cgroup.type" ]] && echo "type=$(<"${path}/cgroup.type")"
    [[ -f "${path}/cpu.max" ]] && echo "cpu.max=$(<"${path}/cpu.max")"
    [[ -f "${path}/cpu.stat" ]] && cat "${path}/cpu.stat"
    [[ -f "${path}/cgroup.threads" ]] && {
      echo "threads=$(tr '\n' ' ' < "${path}/cgroup.threads")"
      echo
    }
    echo
  done
}

cleanup() {
  require_linux
  require_cgroup_v2
  require_root_path

  if [[ -d "${ROOT}/rt_a" ]]; then
    move_threads "${ROOT}/rt_a" "${ROOT}"
  fi
  if [[ -d "${ROOT}/rt_b" ]]; then
    move_threads "${ROOT}/rt_b" "${ROOT}"
  fi
  if [[ -d "${ROOT}" ]]; then
    move_processes "${ROOT}" "$(parent_dir)"
  fi

  for path in "${ROOT}/rt_a" "${ROOT}/rt_b"; do
    if [[ -d "${path}" ]]; then
      assert_empty_file "${path}/cgroup.threads" "threaded child cgroup"
      rmdir "${path}"
    fi
  done

  if [[ -d "${ROOT}" ]]; then
    assert_empty_file "${ROOT}/cgroup.procs" "threaded root cgroup procs"
    assert_empty_file "${ROOT}/cgroup.threads" "threaded root cgroup threads"
    rmdir "${ROOT}"
  fi

  echo "cleaned ${ROOT}"
}

main() {
  local command="${1:-}"
  case "${command}" in
    setup) setup ;;
    status) status ;;
    cleanup) cleanup ;;
    *)
      usage
      exit 1
      ;;
  esac
}

main "$@"
