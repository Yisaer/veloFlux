#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  sudo ./scripts/perf_pr_host/setup_cgroup.sh \
    --base-cg /veloflux-ci/perf-pr-host-<id>/veloflux \
    [--mode worker_process|thread_level]

Creates a cgroup v2 tree for perf validation.

worker_process mode topology:
  <base>            (total veloflux CPU budget group)
  <base>/manager
  <base>/fi_critical
  <base>/fi_best

thread_level mode topology:
  <base>            (process-level parent)
  <base>/main       (threaded-domain root for main veloflux process)
  <base>/main/fi_critical
  <base>/main/fi_best

Defaults:
  base cpu.max        = 100000 100000   (1 CPU)
  critical cpu.max    =  90000 100000   (90%)
  best cpu.max        =  25000 100000   (25%)
  critical cpu.weight = 10000
  main cpu.weight     = 300
  best cpu.weight     = 1

Outputs env lines to stdout (CG_MODE/CG_BASE/CG_MAIN/CG_FI_CRITICAL/CG_FI_BEST).
USAGE
}

MODE="worker_process"
BASE_CG=""
BASE_MAX_QUOTA_US="100000"
BASE_MAX_PERIOD_US="100000"
CRITICAL_MAX_QUOTA_US="90000"
CRITICAL_MAX_PERIOD_US="100000"
BEST_MAX_QUOTA_US="25000"
BEST_MAX_PERIOD_US="100000"
MAIN_WEIGHT="300"
CRITICAL_WEIGHT="10000"
BEST_WEIGHT="1"

while [ $# -gt 0 ]; do
  case "$1" in
    --mode)
      MODE="${2:-}"; shift 2;;
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
    --main-weight)
      MAIN_WEIGHT="${2:-}"; shift 2;;
    --manager-weight)
      MAIN_WEIGHT="${2:-}"; shift 2;;
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
if [ "${MODE}" != "worker_process" ] && [ "${MODE}" != "thread_level" ]; then
  echo "--mode must be worker_process or thread_level" >&2
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
if [ "${MODE}" = "worker_process" ]; then
  CG_MAIN="${CG_BASE}/manager"
  CG_FI_CRITICAL="${CG_BASE}/fi_critical"
  CG_FI_BEST="${CG_BASE}/fi_best"
else
  CG_MAIN="${CG_BASE}/main"
  CG_FI_CRITICAL="${CG_MAIN}/fi_critical"
  CG_FI_BEST="${CG_MAIN}/fi_best"
fi

read_cgroup_type() {
  local cgfs="$1"
  local type_file="${cgfs}/cgroup.type"
  if [ ! -f "${type_file}" ]; then
    return 1
  fi
  tr -d '\n' < "${type_file}" 2>/dev/null || true
}

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

move_threads_to_parent_if_any() {
  local cg="$1"
  local cgfs="/sys/fs/cgroup${cg}"
  local type_file="${cgfs}/cgroup.type"
  local threads_file="${cgfs}/cgroup.threads"
  local parent_threads="$(dirname "${cgfs}")/cgroup.threads"

  if [ ! -f "${type_file}" ] || [ ! -f "${threads_file}" ] || [ ! -f "${parent_threads}" ]; then
    return 0
  fi

  local cg_type
  cg_type="$(read_cgroup_type "${cgfs}" || true)"
  case "${cg_type}" in
    *threaded*) ;;
    *) return 0 ;;
  esac

  local moved=0
  while read -r tid; do
    [ -z "${tid}" ] && continue
    if echo "${tid}" > "${parent_threads}" 2>/dev/null; then
      moved=$((moved + 1))
    fi
  done < "${threads_file}"
  if [ "${moved}" -gt 0 ]; then
    echo "[setup] moved ${moved} tid(s) from ${cg} to $(dirname "${cg}")"
  fi
}

remove_cgroup_dir_if_possible() {
  local cg="$1"
  if rmdir "/sys/fs/cgroup${cg}" 2>/dev/null; then
    echo "[setup] removed stale ${cg}"
  fi
}

disable_all_subtree_controllers() {
  local cgfs="$1"
  local st="${cgfs}/cgroup.subtree_control"
  if [ ! -f "${st}" ]; then
    return 0
  fi
  local controllers
  controllers="$(cat "${st}" 2>/dev/null || true)"
  for ctrl in ${controllers}; do
    [ -z "${ctrl}" ] && continue
    echo "-${ctrl}" > "${st}" 2>/dev/null || true
  done
}

cleanup_descendants_if_any() {
  local root="/sys/fs/cgroup${1}"
  if [ ! -d "${root}" ]; then
    return 0
  fi
  local child_fs
  local child_cg
  while IFS= read -r child_fs; do
    child_cg="${child_fs#/sys/fs/cgroup}"
    move_threads_to_parent_if_any "${child_cg}"
    move_procs_to_root_if_any "${child_cg}"
    disable_all_subtree_controllers "${child_fs}"
    remove_cgroup_dir_if_possible "${child_cg}"
  done < <(find "${root}" -mindepth 1 -type d | sort -r)
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
      local value
      value="$(cat "${parent_mems_eff}" 2>/dev/null || true)"
      if [ -n "${value}" ]; then
        echo "${value}" > "${mems_file}" 2>/dev/null || true
      fi
    fi
  fi

  if [ -f "${cpus_file}" ]; then
    local cpus
    cpus="$(cat "${cpus_file}" 2>/dev/null || true)"
    if [ -z "${cpus}" ] && [ -f "${parent_cpus_eff}" ]; then
      local value
      value="$(cat "${parent_cpus_eff}" 2>/dev/null || true)"
      if [ -n "${value}" ]; then
        echo "${value}" > "${cpus_file}" 2>/dev/null || true
      fi
    fi
  fi
}

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
      local cg="${cgfs#/sys/fs/cgroup}"
      move_procs_to_root_if_any "${cg}"
      if ! echo "+cpu" > "${st}" 2>/dev/null; then
        echo "[setup] failed to enable +cpu in ${st}" >&2
        exit 1
      fi
    fi
  fi
}

prepare_leaf_cgroup() {
  local cg="$1"
  local cgfs="/sys/fs/cgroup${cg}"
  mkdir -p "${cgfs}"
  cleanup_descendants_if_any "${cg}"
  move_threads_to_parent_if_any "${cg}"
  move_procs_to_root_if_any "${cg}"
  disable_all_subtree_controllers "${cgfs}"
  local parentfs
  parentfs="$(dirname "${cgfs}")"
  init_cpuset_if_available "${cgfs}" "${parentfs}"
  echo "[setup] prepared leaf ${cg}"
}

write_threaded_type() {
  local cg="$1"
  local cgfs="/sys/fs/cgroup${cg}"
  local tmp_err
  tmp_err="$(mktemp)"
  if ! echo threaded > "${cgfs}/cgroup.type" 2>"${tmp_err}"; then
    local err current_type
    err="$(tr -d '\n' < "${tmp_err}" 2>/dev/null || true)"
    current_type="$(read_cgroup_type "${cgfs}" || true)"
    rm -f "${tmp_err}"
    echo "[setup] failed to mark ${cg} as threaded: type=${current_type:-<empty>} error=${err:-<none>}" >&2
    exit 1
  fi
  rm -f "${tmp_err}"
}

prepare_threaded_domain_root() {
  local cg="$1"
  local cgfs="/sys/fs/cgroup${cg}"
  mkdir -p "${cgfs}"
  cleanup_descendants_if_any "${cg}"
  move_procs_to_root_if_any "${cg}"
  local parentfs
  parentfs="$(dirname "${cgfs}")"
  init_cpuset_if_available "${cgfs}" "${parentfs}"
  enable_cpu_controller "${cgfs}"
  local current_type
  current_type="$(read_cgroup_type "${cgfs}" || true)"
  case "${current_type}" in
    "domain"|"domain threaded") ;;
    *)
      echo "[setup] unexpected threaded-domain root type for ${cg}: ${current_type:-<empty>}" >&2
      exit 1 ;;
  esac
  echo "[setup] prepared threaded-domain root ${cg} (type=${current_type})"
}

prepare_threaded_leaf_cgroup() {
  local cg="$1"
  local cgfs="/sys/fs/cgroup${cg}"
  prepare_leaf_cgroup "${cg}"
  local current_type
  current_type="$(read_cgroup_type "${cgfs}" || true)"
  case "${current_type}" in
    threaded)
      echo "[setup] prepared threaded leaf ${cg} (type=${current_type})"
      return 0 ;;
    "domain"|"domain invalid"|"")
      write_threaded_type "${cg}"
      current_type="$(read_cgroup_type "${cgfs}" || true)" ;;
  esac
  if [ "${current_type}" != "threaded" ]; then
    echo "[setup] failed to prepare threaded leaf ${cg}: type=${current_type:-<empty>}" >&2
    exit 1
  fi
  echo "[setup] prepared threaded leaf ${cg} (type=${current_type})"
}

verify_thread_level_tree() {
  local main_fs="/sys/fs/cgroup${CG_MAIN}"
  local critical_fs="/sys/fs/cgroup${CG_FI_CRITICAL}"
  local best_fs="/sys/fs/cgroup${CG_FI_BEST}"
  local main_type critical_type best_type
  local verify_fs

  for verify_fs in "${main_fs}" "${critical_fs}" "${best_fs}"; do
    if [ ! -d "${verify_fs}" ]; then
      echo "[setup] verification failed: missing ${verify_fs}" >&2
      exit 1
    fi
  done

  main_type="$(read_cgroup_type "${main_fs}" || true)"
  critical_type="$(read_cgroup_type "${critical_fs}" || true)"
  best_type="$(read_cgroup_type "${best_fs}" || true)"

  if [ "${main_type}" != "domain threaded" ]; then
    echo "[setup] verification failed: ${CG_MAIN} type=${main_type:-<empty>} expected=domain threaded" >&2
    exit 1
  fi
  if [ "${critical_type}" != "threaded" ]; then
    echo "[setup] verification failed: ${CG_FI_CRITICAL} type=${critical_type:-<empty>} expected=threaded" >&2
    exit 1
  fi
  if [ "${best_type}" != "threaded" ]; then
    echo "[setup] verification failed: ${CG_FI_BEST} type=${best_type:-<empty>} expected=threaded" >&2
    exit 1
  fi

  for path in \
    "${main_fs}/cgroup.procs" \
    "${main_fs}/cgroup.threads" \
    "${critical_fs}/cgroup.threads" \
    "${best_fs}/cgroup.threads"; do
    if [ ! -f "${path}" ]; then
      echo "[setup] verification failed: missing ${path}" >&2
      exit 1
    fi
  done

  echo "[setup] verified thread_level tree: main=${main_type}, fi_critical=${critical_type}, fi_best=${best_type}"
}

show_cgroup_config() {
  local cg="$1"
  local cgfs="/sys/fs/cgroup${cg}"
  if [ ! -d "${cgfs}" ]; then
    echo "[setup] ${cg}: missing"
    return 0
  fi
  local type_v cpu_max_v cpu_weight_v
  type_v="$(read_cgroup_type "${cgfs}" || echo "<n/a>")"
  cpu_max_v="$(cat "${cgfs}/cpu.max" 2>/dev/null || echo "<n/a>")"
  cpu_weight_v="$(cat "${cgfs}/cpu.weight" 2>/dev/null || echo "<n/a>")"
  echo "[setup] ${cg}: type=${type_v}, cpu.max=${cpu_max_v}, cpu.weight=${cpu_weight_v}"
}

echo "[setup] mode=${MODE}"
echo "[setup] target cgroups:"
echo "[setup]   ${CG_BASE}"
echo "[setup]   ${CG_MAIN}"
echo "[setup]   ${CG_FI_CRITICAL}"
echo "[setup]   ${CG_FI_BEST}"

if [ -d "/sys/fs/cgroup${CG_BASE}" ]; then
  echo "[setup] detected existing tree, cleanup stale cgroups before setup"
  cleanup_descendants_if_any "${CG_BASE}"
  move_threads_to_parent_if_any "${CG_FI_CRITICAL}"
  move_threads_to_parent_if_any "${CG_FI_BEST}"
  move_threads_to_parent_if_any "${CG_MAIN}"
  move_procs_to_root_if_any "${CG_FI_CRITICAL}"
  move_procs_to_root_if_any "${CG_FI_BEST}"
  move_procs_to_root_if_any "${CG_MAIN}"
  move_procs_to_root_if_any "${CG_BASE}"
  remove_cgroup_dir_if_possible "${CG_FI_CRITICAL}"
  remove_cgroup_dir_if_possible "${CG_FI_BEST}"
  remove_cgroup_dir_if_possible "${CG_MAIN}"
  remove_cgroup_dir_if_possible "${CG_BASE}"
fi

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

if [ "${MODE}" = "worker_process" ]; then
  prepare_leaf_cgroup "${CG_MAIN}"
  prepare_leaf_cgroup "${CG_FI_CRITICAL}"
  prepare_leaf_cgroup "${CG_FI_BEST}"
else
  prepare_threaded_domain_root "${CG_MAIN}"
  prepare_threaded_leaf_cgroup "${CG_FI_CRITICAL}"
  prepare_threaded_leaf_cgroup "${CG_FI_BEST}"
  verify_thread_level_tree
fi

echo "${CRITICAL_MAX_QUOTA_US} ${CRITICAL_MAX_PERIOD_US}" > "/sys/fs/cgroup${CG_FI_CRITICAL}/cpu.max"
echo "${BEST_MAX_QUOTA_US} ${BEST_MAX_PERIOD_US}" > "/sys/fs/cgroup${CG_FI_BEST}/cpu.max"
echo "${MAIN_WEIGHT}" > "/sys/fs/cgroup${CG_MAIN}/cpu.weight"
echo "${CRITICAL_WEIGHT}" > "/sys/fs/cgroup${CG_FI_CRITICAL}/cpu.weight"
echo "${BEST_WEIGHT}" > "/sys/fs/cgroup${CG_FI_BEST}/cpu.weight"

echo "[setup] set ${CG_FI_CRITICAL}/cpu.max = $(cat "/sys/fs/cgroup${CG_FI_CRITICAL}/cpu.max")"
echo "[setup] set ${CG_FI_BEST}/cpu.max = $(cat "/sys/fs/cgroup${CG_FI_BEST}/cpu.max")"
echo "[setup] set ${CG_MAIN}/cpu.weight = $(cat "/sys/fs/cgroup${CG_MAIN}/cpu.weight")"
echo "[setup] set ${CG_FI_CRITICAL}/cpu.weight = $(cat "/sys/fs/cgroup${CG_FI_CRITICAL}/cpu.weight")"
echo "[setup] set ${CG_FI_BEST}/cpu.weight = $(cat "/sys/fs/cgroup${CG_FI_BEST}/cpu.weight")"

OWNER_UID="${SUDO_UID:-0}"
OWNER_GID="${SUDO_GID:-0}"
chown -R "${OWNER_UID}:${OWNER_GID}" "/sys/fs/cgroup${CG_BASE}"
chmod -R ug+rwX "/sys/fs/cgroup${CG_BASE}"
echo "[setup] set ownership to ${OWNER_UID}:${OWNER_GID} on ${CG_BASE}"

echo "[setup] effective cgroup settings:"
show_cgroup_config "${CG_BASE}"
show_cgroup_config "${CG_MAIN}"
show_cgroup_config "${CG_FI_CRITICAL}"
show_cgroup_config "${CG_FI_BEST}"

cat <<ENVVARS
CG_MODE=${MODE}
CG_BASE=${CG_BASE}
CG_MAIN=${CG_MAIN}
CG_FI_CRITICAL=${CG_FI_CRITICAL}
CG_FI_BEST=${CG_FI_BEST}
CG_MAIN_PROCS=/sys/fs/cgroup${CG_MAIN}/cgroup.procs
ENVVARS
