#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  sudo ./scripts/perf_pr_host/cleanup_cgroup.sh --base-cg /veloflux-ci/perf-pr-host-<id>/veloflux

Moves any remaining pids back to cgroup root and removes the cgroup directories.
EOF
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
  while read -r pid; do
    [ -z "${pid}" ] && continue
    # Ignore errors if pid already exited.
    echo "${pid}" > /sys/fs/cgroup/cgroup.procs 2>/dev/null || true
  done < "${procs}"
}

CG_MANAGER="${BASE_CG}/manager"
CG_FI_CRITICAL="${BASE_CG}/fi_critical"
CG_FI_BEST="${BASE_CG}/fi_best"

move_procs_to_root "${CG_MANAGER}"
move_procs_to_root "${CG_FI_CRITICAL}"
move_procs_to_root "${CG_FI_BEST}"
move_procs_to_root "${BASE_CG}"

# Remove leaves first.
rmdir "/sys/fs/cgroup${CG_MANAGER}" 2>/dev/null || true
rmdir "/sys/fs/cgroup${CG_FI_CRITICAL}" 2>/dev/null || true
rmdir "/sys/fs/cgroup${CG_FI_BEST}" 2>/dev/null || true
rmdir "/sys/fs/cgroup${BASE_CG}" 2>/dev/null || true

