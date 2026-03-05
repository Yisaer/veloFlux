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
    echo "[cleanup] ${cg}: not found (skip)"
    return 0
  fi
  local moved=0
  while read -r pid; do
    [ -z "${pid}" ] && continue
    # Ignore errors if pid already exited.
    if echo "${pid}" > /sys/fs/cgroup/cgroup.procs 2>/dev/null; then
      moved=$((moved + 1))
    fi
  done < "${procs}"
  echo "[cleanup] ${cg}: moved ${moved} pid(s) to /sys/fs/cgroup"
}

remove_cgroup_dir() {
  local cg="$1"
  if rmdir "/sys/fs/cgroup${cg}" 2>/dev/null; then
    echo "[cleanup] removed ${cg}"
  else
    if [ -d "/sys/fs/cgroup${cg}" ]; then
      echo "[cleanup] keep ${cg} (not empty or busy)"
    else
      echo "[cleanup] ${cg}: already removed"
    fi
  fi
}

cleanup_descendants() {
  local root="/sys/fs/cgroup${BASE_CG}"
  if [ ! -d "${root}" ]; then
    return 0
  fi
  while IFS= read -r cgfs; do
    local cg="${cgfs#/sys/fs/cgroup}"
    [ "${cg}" = "${BASE_CG}" ] && continue
    move_procs_to_root "${cg}"
    remove_cgroup_dir "${cg}"
  done < <(find "${root}" -mindepth 1 -type d | sort -r)
}

CG_MANAGER="${BASE_CG}/manager"
CG_FI_CRITICAL="${BASE_CG}/fi_critical"
CG_FI_BEST="${BASE_CG}/fi_best"

echo "[cleanup] target cgroups:"
echo "[cleanup]   ${BASE_CG}"
echo "[cleanup]   ${CG_MANAGER}"
echo "[cleanup]   ${CG_FI_CRITICAL}"
echo "[cleanup]   ${CG_FI_BEST}"

move_procs_to_root "${CG_MANAGER}"
move_procs_to_root "${CG_FI_CRITICAL}"
move_procs_to_root "${CG_FI_BEST}"
move_procs_to_root "${BASE_CG}"
cleanup_descendants

# Remove leaves first.
remove_cgroup_dir "${CG_MANAGER}"
remove_cgroup_dir "${CG_FI_CRITICAL}"
remove_cgroup_dir "${CG_FI_BEST}"
remove_cgroup_dir "${BASE_CG}"

echo "[cleanup] final status:"
for cg in "${CG_MANAGER}" "${CG_FI_CRITICAL}" "${CG_FI_BEST}" "${BASE_CG}"; do
  if [ -d "/sys/fs/cgroup${cg}" ]; then
    echo "[cleanup] keep ${cg}"
  else
    echo "[cleanup] removed ${cg}"
  fi
done
