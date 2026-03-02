#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  sudo ./scripts/perf_pr_host/setup_cgroup.sh --base-cg /veloflux-ci/perf-pr-host-<id>/veloflux

Creates a cgroup v2 tree for veloflux CPU isolation:
  <base>/manager
  <base>/fi_critical
  <base>/fi_best

Defaults:
  base cpu.max      = 100000 100000   (1 CPU)
  critical cpu.max  =  95000 100000   (95%)
  manager cpu.weight= 200
  best cpu.weight   = 100

Outputs env lines to stdout (CG_BASE/CG_MANAGER/CG_FI_CRITICAL/CG_FI_BEST) for sourcing.
EOF
}

BASE_CG=""
BASE_MAX_QUOTA_US="100000"
BASE_MAX_PERIOD_US="100000"
CRITICAL_MAX_QUOTA_US="95000"
CRITICAL_MAX_PERIOD_US="100000"
MANAGER_WEIGHT="200"
BEST_WEIGHT="100"

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
    --manager-weight)
      MANAGER_WEIGHT="${2:-}"; shift 2;;
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

enable_cpu_controller() {
  local cgfs="$1"
  local st="${cgfs}/cgroup.subtree_control"
  if [ -f "${st}" ] && ! grep -qw cpu "${st}"; then
    echo "+cpu" > "${st}" || true
  fi
}

enable_cpu_controller "/sys/fs/cgroup"

mkdir -p "/sys/fs/cgroup${CG_BASE}"
enable_cpu_controller "/sys/fs/cgroup${CG_BASE}"

echo "${BASE_MAX_QUOTA_US} ${BASE_MAX_PERIOD_US}" > "/sys/fs/cgroup${CG_BASE}/cpu.max"

mkdir -p "/sys/fs/cgroup${CG_MANAGER}" "/sys/fs/cgroup${CG_FI_CRITICAL}" "/sys/fs/cgroup${CG_FI_BEST}"

echo "${CRITICAL_MAX_QUOTA_US} ${CRITICAL_MAX_PERIOD_US}" > "/sys/fs/cgroup${CG_FI_CRITICAL}/cpu.max"
echo "${MANAGER_WEIGHT}" > "/sys/fs/cgroup${CG_MANAGER}/cpu.weight"
echo "${BEST_WEIGHT}" > "/sys/fs/cgroup${CG_FI_BEST}/cpu.weight"

# Allow the invoking user to join cgroups (veloflux does the join itself at runtime).
OWNER_UID="${SUDO_UID:-0}"
OWNER_GID="${SUDO_GID:-0}"
chown -R "${OWNER_UID}:${OWNER_GID}" "/sys/fs/cgroup${CG_BASE}"
chmod -R ug+rwX "/sys/fs/cgroup${CG_BASE}"

cat <<EOF
CG_BASE=${CG_BASE}
CG_MANAGER=${CG_MANAGER}
CG_FI_CRITICAL=${CG_FI_CRITICAL}
CG_FI_BEST=${CG_FI_BEST}
EOF

