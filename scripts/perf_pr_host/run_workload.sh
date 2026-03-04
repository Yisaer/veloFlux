#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
One-click run workload for perf PR wide MQTT scenario.

This script calls Go workload binary:
  scripts/perf_pr_host/go_workload/bin/go_workload

It does NOT start veloflux/emqx/prometheus/grafana.

Default behavior:
  phase A: rate_critical=120, rate_best=120, duration=60s
  phase B: rate_critical=10,  rate_best=120, duration=60s

Usage:
  ./scripts/perf_pr_host/run_workload.sh

Options:
  --phase <a|b|both>              default: both
  --broker-url <url>              default: tcp://127.0.0.1:1883
  --topic-critical <topic>        default: /perf/pr/critical
  --topic-best <topic>            default: /perf/pr/best
  --columns <n>                   default: 15000
  --cases <n>                     default: 20
  --str-len <n>                   default: 10
  --qos <n>                       default: 1
  --phase-a-duration-secs <n>     default: 60
  --phase-a-rate-critical <n>     default: 120
  --phase-a-rate-best <n>         default: 120
  --phase-b-duration-secs <n>     default: 60
  --phase-b-rate-critical <n>     default: 10
  --phase-b-rate-best <n>         default: 120
  --workload-bin <path>           default: scripts/perf_pr_host/go_workload/bin/go_workload
  --client-prefix <prefix>        default: perf-pr-go
  --taskset-cpu <cpu-list>        optional, e.g. 1
EOF
}

PHASE="both"
BROKER_URL="tcp://127.0.0.1:1883"
TOPIC_CRITICAL="/perf/pr/critical"
TOPIC_BEST="/perf/pr/best"
COLS="15000"
CASES="20"
STR_LEN="10"
QOS="1"
PHASE_A_SECS="60"
PHASE_A_RATE_CRIT="120"
PHASE_A_RATE_BEST="120"
PHASE_B_SECS="60"
PHASE_B_RATE_CRIT="10"
PHASE_B_RATE_BEST="120"
CLIENT_PREFIX="perf-pr-go"
TASKSET_CPU=""
WORKLOAD_BIN=""

while [ $# -gt 0 ]; do
  case "$1" in
    --phase) PHASE="${2:-}"; shift 2;;
    --broker-url) BROKER_URL="${2:-}"; shift 2;;
    --topic-critical) TOPIC_CRITICAL="${2:-}"; shift 2;;
    --topic-best) TOPIC_BEST="${2:-}"; shift 2;;
    --columns) COLS="${2:-}"; shift 2;;
    --cases) CASES="${2:-}"; shift 2;;
    --str-len) STR_LEN="${2:-}"; shift 2;;
    --qos) QOS="${2:-}"; shift 2;;
    --phase-a-duration-secs) PHASE_A_SECS="${2:-}"; shift 2;;
    --phase-a-rate-critical) PHASE_A_RATE_CRIT="${2:-}"; shift 2;;
    --phase-a-rate-best) PHASE_A_RATE_BEST="${2:-}"; shift 2;;
    --phase-b-duration-secs) PHASE_B_SECS="${2:-}"; shift 2;;
    --phase-b-rate-critical) PHASE_B_RATE_CRIT="${2:-}"; shift 2;;
    --phase-b-rate-best) PHASE_B_RATE_BEST="${2:-}"; shift 2;;
    --client-prefix) CLIENT_PREFIX="${2:-}"; shift 2;;
    --workload-bin) WORKLOAD_BIN="${2:-}"; shift 2;;
    --taskset-cpu) TASKSET_CPU="${2:-}"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "unknown arg: $1" >&2; usage; exit 2;;
  esac
done

case "${PHASE}" in
  a|b|both) ;;
  *)
    echo "--phase must be one of: a | b | both" >&2
    exit 2
    ;;
esac

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

if [ -z "${WORKLOAD_BIN}" ]; then
  WORKLOAD_BIN="${ROOT_DIR}/scripts/perf_pr_host/go_workload/bin/go_workload"
fi

if [ ! -x "${WORKLOAD_BIN}" ]; then
  echo "[workload] binary not found, building: ${WORKLOAD_BIN}"
  mkdir -p "$(dirname "${WORKLOAD_BIN}")"
  GOPROXY="${GOPROXY:-https://proxy.golang.org,direct}" \
    go -C "${ROOT_DIR}/scripts/perf_pr_host/go_workload" build -o "${WORKLOAD_BIN}" .
fi

CMD=(
  "${WORKLOAD_BIN}"
  --phase "${PHASE}"
  --broker-url "${BROKER_URL}"
  --topic-critical "${TOPIC_CRITICAL}"
  --topic-best "${TOPIC_BEST}"
  --columns "${COLS}"
  --cases "${CASES}"
  --str-len "${STR_LEN}"
  --qos "${QOS}"
  --phase-a-duration-secs "${PHASE_A_SECS}"
  --phase-a-rate-critical "${PHASE_A_RATE_CRIT}"
  --phase-a-rate-best "${PHASE_A_RATE_BEST}"
  --phase-b-duration-secs "${PHASE_B_SECS}"
  --phase-b-rate-critical "${PHASE_B_RATE_CRIT}"
  --phase-b-rate-best "${PHASE_B_RATE_BEST}"
  --client-prefix "${CLIENT_PREFIX}"
)

if [ -n "${TASKSET_CPU}" ]; then
  exec taskset -c "${TASKSET_CPU}" "${CMD[@]}"
fi

exec "${CMD[@]}"
