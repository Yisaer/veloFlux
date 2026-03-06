#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  sudo ./scripts/perf_pr_host/cleanup_cgroup.sh --base-cg /veloflux-ci/perf-pr-host-<id>/veloflux

Moves remaining pids/tids back toward the parent cgroup and removes the cgroup tree.
This works for both worker_process and thread_level perf topologies.
USAGE
}

BASE_CG=""
while [ $# -gt 0 ]; do
  case "$1" in
    --base-cg)
      BASE_CG="${2:-}"; shift 2;;
    -h|--help)
      usage; exit 0;;
    *)
      echo "unknown arg: $1" >&2
      usage
      exit 2;;
  esac
done

if [ -z "${BASE_CG}" ]; then
  echo "--base-cg is required" >&2
  usage
  exit 2
fi
if [ "${BASE_CG:0:1}" != "/" ]; then
  echo "--base-cg must be an absolute cgroup path like /veloflux-ci/..." >&2
  exit 2
fi
if [ "$(id -u)" -ne 0 ]; then
  echo "this script must be run as root (use sudo)" >&2
  exit 1
fi

move_procs_to_root() {
  local cg="$1"
  local procs="/sys/fs/cgroup${cg}/cgroup.procs"
  if [ ! -f "${procs}" ]; then
    return 0
  fi
  local moved=0
  while read -r pid; do
    [ -z "${pid}" ] && continue
    if echo "${pid}" > /sys/fs/cgroup/cgroup.procs 2>/dev/null; then
      moved=$((moved + 1))
    fi
  done < "${procs}"
  if [ "${moved}" -gt 0 ]; then
    echo "[cleanup] ${cg}: moved ${moved} pid(s) to /sys/fs/cgroup"
  fi
}

move_threads_to_parent() {
  local cg="$1"
  local cgfs="/sys/fs/cgroup${cg}"
  local type_file="${cgfs}/cgroup.type"
  local threads_file="${cgfs}/cgroup.threads"
  local parent_threads="$(dirname "${cgfs}")/cgroup.threads"

  if [ ! -f "${type_file}" ] || [ ! -f "${threads_file}" ] || [ ! -f "${parent_threads}" ]; then
    return 0
  fi

  local cg_type
  cg_type="$(cat "${type_file}" 2>/dev/null || true)"
  case "${cg_type}" in
    *threaded*) ;;
    *) return 0;;
  esac

  local moved=0
  while read -r tid; do
    [ -z "${tid}" ] && continue
    if echo "${tid}" > "${parent_threads}" 2>/dev/null; then
      moved=$((moved + 1))
    fi
  done < "${threads_file}"
  if [ "${moved}" -gt 0 ]; then
    echo "[cleanup] ${cg}: moved ${moved} tid(s) to $(dirname "${cg}")"
  fi
}

remove_cgroup_dir() {
  local cg="$1"
  if rmdir "/sys/fs/cgroup${cg}" 2>/dev/null; then
    echo "[cleanup] removed ${cg}"
  elif [ -d "/sys/fs/cgroup${cg}" ]; then
    echo "[cleanup] keep ${cg} (not empty or busy)"
  else
    echo "[cleanup] ${cg}: already removed"
  fi
}

ROOT="/sys/fs/cgroup${BASE_CG}"
if [ ! -d "${ROOT}" ]; then
  echo "[cleanup] ${BASE_CG}: not found"
  exit 0
fi

echo "[cleanup] target cgroup tree: ${BASE_CG}"
while IFS= read -r cgfs; do
  cg="${cgfs#/sys/fs/cgroup}"
  move_threads_to_parent "${cg}"
  move_procs_to_root "${cg}"
  remove_cgroup_dir "${cg}"
done < <(find "${ROOT}" -mindepth 1 -type d | sort -r)

move_procs_to_root "${BASE_CG}"
remove_cgroup_dir "${BASE_CG}"

echo "[cleanup] final status:"
if [ -d "${ROOT}" ]; then
  find "${ROOT}" -mindepth 0 -type d | sort | sed 's#^#[cleanup] keep #' 
else
  echo "[cleanup] removed ${BASE_CG}"
fi
