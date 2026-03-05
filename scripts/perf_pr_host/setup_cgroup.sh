#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  sudo ./scripts/perf_pr_host/setup_cgroup.sh --base-cg /veloflux-ci/perf-pr-host-<id>/veloflux

Creates a cgroup v2 tree for veloflux CPU isolation:
  <base>            (total veloflux CPU budget group)
  <base>/manager
  <base>/fi_critical
  <base>/fi_best

Defaults:
  base cpu.max      = 100000 100000   (1 CPU)
  critical cpu.max  =  90000 100000   (90%)
  best cpu.max      =  25000 100000   (25%)
  critical cpu.weight = 10000
  manager cpu.weight= 300
  best cpu.weight   = 1

Outputs env lines to stdout (CG_BASE/CG_MANAGER/CG_FI_CRITICAL/CG_FI_BEST) for sourcing.
EOF
}

BASE_CG=""
BASE_MAX_QUOTA_US="100000"
BASE_MAX_PERIOD_US="100000"
CRITICAL_MAX_QUOTA_US="90000"
CRITICAL_MAX_PERIOD_US="100000"
BEST_MAX_QUOTA_US="25000"
BEST_MAX_PERIOD_US="100000"
MANAGER_WEIGHT="300"
CRITICAL_WEIGHT="10000"
BEST_WEIGHT="1"

while [ $# -gt 0 ]; do
  case "$1" in
    --base-cg)
      BASE_CG="${2:-}"; shift 2;;
    --base-max)
      BASE_MAX_QUOTA_US="${2:-}"; shift 2;;
    --base-period)
      BASE_MAX_PERIOD_US="${2:-}"; shift 2;;
    --critical-max)
      CRITICAL_MAX_QUOTA_US="${2:-}"; shift 2;;
    --critical-period)
      CRITICAL_MAX_PERIOD_US="${2:-}"; shift 2;;
    --best-max)
      BEST_MAX_QUOTA_US="${2:-}"; shift 2;;
    --best-period)
      BEST_MAX_PERIOD_US="${2:-}"; shift 2;;
    --manager-weight)
      MANAGER_WEIGHT="${2:-}"; shift 2;;
    --critical-weight)
      CRITICAL_WEIGHT="${2:-}"; shift 2;;
    --best-weight)
      BEST_WEIGHT="${2:-}"; shift 2;;
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

if [ ! -f /sys/fs/cgroup/cgroup.controllers ]; then
  echo "cgroup v2 is not mounted at /sys/fs/cgroup" >&2
  exit 1
fi
if ! grep -qw cpu /sys/fs/cgroup/cgroup.controllers; then
  echo "cpu controller is not available: $(cat /sys/fs/cgroup/cgroup.controllers)" >&2
  exit 1
fi

CG_BASE="${BASE_CG}"
CG_MANAGER="${CG_BASE}/manager"
CG_FI_CRITICAL="${CG_BASE}/fi_critical"
CG_FI_BEST="${CG_BASE}/fi_best"

move_procs_to_root_if_any() {
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
    echo "[setup] moved ${moved} pid(s) from ${cg} to /sys/fs/cgroup"
  fi
}

remove_cgroup_dir_if_possible() {
  local cg="$1"
  if rmdir "/sys/fs/cgroup${cg}" 2>/dev/null; then
    echo "[setup] removed stale ${cg}"
  fi
}

cleanup_descendants_if_any() {
  local root="/sys/fs/cgroup${CG_BASE}"
  if [ ! -d "${root}" ]; then
    return 0
  fi
  while IFS= read -r cgfs; do
    local cg="${cgfs#/sys/fs/cgroup}"
    move_procs_to_root_if_any "${cg}"
    remove_cgroup_dir_if_possible "${cg}"
  done < <(find "${root}" -mindepth 1 -type d | sort -r)
}

disable_all_subtree_controllers() {
  local cgfs="$1"
  local st="${cgfs}/cgroup.subtree_control"
  if [ ! -f "${st}" ]; then
    return 0
  fi
  local controllers
  controllers="$(tr ' ' '\n' < "${st}" | sed '/^$/d' || true)"
  if [ -z "${controllers}" ]; then
    return 0
  fi
  while read -r ctrl; do
    [ -z "${ctrl}" ] && continue
    echo "-${ctrl}" > "${st}" 2>/dev/null || true
  done <<< "${controllers}"
}

init_cpuset_if_available() {
  local cgfs="$1"
  local parentfs="$2"

  local mems_file="${cgfs}/cpuset.mems"
  local cpus_file="${cgfs}/cpuset.cpus"
  local parent_mems_eff="${parentfs}/cpuset.mems.effective"
  local parent_cpus_eff="${parentfs}/cpuset.cpus.effective"

  if [ -f "${mems_file}" ]; then
    local mems
    mems="$(cat "${mems_file}" 2>/dev/null || true)"
    if [ -z "${mems}" ] && [ -f "${parent_mems_eff}" ]; then
      local v
      v="$(cat "${parent_mems_eff}" 2>/dev/null || true)"
      if [ -n "${v}" ]; then
        echo "${v}" > "${mems_file}" 2>/dev/null || true
      fi
    fi
  fi

  if [ -f "${cpus_file}" ]; then
    local cpus
    cpus="$(cat "${cpus_file}" 2>/dev/null || true)"
    if [ -z "${cpus}" ] && [ -f "${parent_cpus_eff}" ]; then
      local v
      v="$(cat "${parent_cpus_eff}" 2>/dev/null || true)"
      if [ -n "${v}" ]; then
        echo "${v}" > "${cpus_file}" 2>/dev/null || true
      fi
    fi
  fi
}

prepare_leaf_cgroup() {
  local cg="$1"
  local cgfs="/sys/fs/cgroup${cg}"
  mkdir -p "${cgfs}"
  while IFS= read -r child; do
    local child_cg="${child#/sys/fs/cgroup}"
    move_procs_to_root_if_any "${child_cg}"
    remove_cgroup_dir_if_possible "${child_cg}"
  done < <(find "${cgfs}" -mindepth 1 -type d | sort -r)
  move_procs_to_root_if_any "${cg}"
  disable_all_subtree_controllers "${cgfs}"
  local parentfs
  parentfs="$(dirname "${cgfs}")"
  init_cpuset_if_available "${cgfs}" "${parentfs}"
  echo "[setup] prepared leaf ${cg}"
}

echo "[setup] target cgroups:"
echo "[setup]   ${CG_BASE}"
echo "[setup]   ${CG_MANAGER}"
echo "[setup]   ${CG_FI_CRITICAL}"
echo "[setup]   ${CG_FI_BEST}"

enable_cpu_controller() {
  local cgfs="$1"
  local ctrls="${cgfs}/cgroup.controllers"
  local st="${cgfs}/cgroup.subtree_control"

  if [ ! -f "${ctrls}" ]; then
    echo "[setup] missing ${ctrls}" >&2
    exit 1
  fi
  if ! grep -qw cpu "${ctrls}"; then
    echo "[setup] cpu controller is not available in ${cgfs}; check parent delegation" >&2
    exit 1
  fi
  if [ -f "${st}" ] && ! grep -qw cpu "${st}"; then
    if ! echo "+cpu" > "${st}" 2>/dev/null; then
      # Retry once after moving tasks out (no-internal-process constraint).
      local cg="${cgfs#/sys/fs/cgroup}"
      move_procs_to_root_if_any "${cg}"
      if ! echo "+cpu" > "${st}" 2>/dev/null; then
        echo "[setup] failed to enable +cpu in ${st}" >&2
        exit 1
      fi
    fi
  fi
}

# Best-effort reset of stale tree so rerun can converge.
if [ -d "/sys/fs/cgroup${CG_BASE}" ]; then
  echo "[setup] detected existing tree, cleanup stale cgroups before setup"
  cleanup_descendants_if_any
  move_procs_to_root_if_any "${CG_MANAGER}"
  move_procs_to_root_if_any "${CG_FI_CRITICAL}"
  move_procs_to_root_if_any "${CG_FI_BEST}"
  move_procs_to_root_if_any "${CG_BASE}"
  remove_cgroup_dir_if_possible "${CG_MANAGER}"
  remove_cgroup_dir_if_possible "${CG_FI_CRITICAL}"
  remove_cgroup_dir_if_possible "${CG_FI_BEST}"
  remove_cgroup_dir_if_possible "${CG_BASE}"
fi

# cgroup v2 needs controller delegation step-by-step:
# parent.cgroup.subtree_control must include +cpu before child can use cpu controller.
enable_cpu_controller "/sys/fs/cgroup"

CURRENT="/sys/fs/cgroup"
BASE_TRIMMED="${CG_BASE#/}"
IFS='/' read -r -a BASE_PARTS <<< "${BASE_TRIMMED}"
for part in "${BASE_PARTS[@]}"; do
  local_prev="${CURRENT}"
  CURRENT="${CURRENT}/${part}"
  mkdir -p "${CURRENT}"
  init_cpuset_if_available "${CURRENT}" "${local_prev}"
  enable_cpu_controller "${CURRENT}"
  echo "[setup] ensured ${CURRENT#/sys/fs/cgroup}"
done

echo "${BASE_MAX_QUOTA_US} ${BASE_MAX_PERIOD_US}" > "/sys/fs/cgroup${CG_BASE}/cpu.max"
echo "[setup] set ${CG_BASE}/cpu.max = $(cat "/sys/fs/cgroup${CG_BASE}/cpu.max")"

prepare_leaf_cgroup "${CG_MANAGER}"
prepare_leaf_cgroup "${CG_FI_CRITICAL}"
prepare_leaf_cgroup "${CG_FI_BEST}"

echo "${CRITICAL_MAX_QUOTA_US} ${CRITICAL_MAX_PERIOD_US}" > "/sys/fs/cgroup${CG_FI_CRITICAL}/cpu.max"
echo "${BEST_MAX_QUOTA_US} ${BEST_MAX_PERIOD_US}" > "/sys/fs/cgroup${CG_FI_BEST}/cpu.max"
echo "${CRITICAL_WEIGHT}" > "/sys/fs/cgroup${CG_FI_CRITICAL}/cpu.weight"
echo "${MANAGER_WEIGHT}" > "/sys/fs/cgroup${CG_MANAGER}/cpu.weight"
echo "${BEST_WEIGHT}" > "/sys/fs/cgroup${CG_FI_BEST}/cpu.weight"
echo "[setup] set ${CG_FI_CRITICAL}/cpu.max = $(cat "/sys/fs/cgroup${CG_FI_CRITICAL}/cpu.max")"
echo "[setup] set ${CG_FI_BEST}/cpu.max = $(cat "/sys/fs/cgroup${CG_FI_BEST}/cpu.max")"
echo "[setup] set ${CG_FI_CRITICAL}/cpu.weight = $(cat "/sys/fs/cgroup${CG_FI_CRITICAL}/cpu.weight")"
echo "[setup] set ${CG_MANAGER}/cpu.weight = $(cat "/sys/fs/cgroup${CG_MANAGER}/cpu.weight")"
echo "[setup] set ${CG_FI_BEST}/cpu.weight = $(cat "/sys/fs/cgroup${CG_FI_BEST}/cpu.weight")"

# Allow the invoking user to join cgroups (main process + workers join at runtime).
OWNER_UID="${SUDO_UID:-0}"
OWNER_GID="${SUDO_GID:-0}"
chown -R "${OWNER_UID}:${OWNER_GID}" "/sys/fs/cgroup${CG_BASE}"
chmod -R ug+rwX "/sys/fs/cgroup${CG_BASE}"
echo "[setup] set ownership to ${OWNER_UID}:${OWNER_GID} on ${CG_BASE}"

show_cgroup_config() {
  local cg="$1"
  local cgfs="/sys/fs/cgroup${cg}"
  if [ ! -d "${cgfs}" ]; then
    echo "[setup] ${cg}: missing"
    return 0
  fi
  local type_v
  local cpu_max_v
  local cpu_weight_v
  type_v="$(cat "${cgfs}/cgroup.type" 2>/dev/null || echo "<n/a>")"
  cpu_max_v="$(cat "${cgfs}/cpu.max" 2>/dev/null || echo "<n/a>")"
  cpu_weight_v="$(cat "${cgfs}/cpu.weight" 2>/dev/null || echo "<n/a>")"
  echo "[setup] ${cg}: type=${type_v}, cpu.max=${cpu_max_v}, cpu.weight=${cpu_weight_v}"
}

echo "[setup] effective cgroup settings:"
show_cgroup_config "${CG_BASE}"
show_cgroup_config "${CG_MANAGER}"
show_cgroup_config "${CG_FI_CRITICAL}"
show_cgroup_config "${CG_FI_BEST}"

cat <<EOF
CG_BASE=${CG_BASE}
CG_MANAGER=${CG_MANAGER}
CG_FI_CRITICAL=${CG_FI_CRITICAL}
CG_FI_BEST=${CG_FI_BEST}
CG_MANAGER_PROCS=/sys/fs/cgroup${CG_MANAGER}/cgroup.procs
EOF
