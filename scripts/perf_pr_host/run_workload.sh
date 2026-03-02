#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
One-click run workload for perf PR wide MQTT scenario.

This script only calls:
  tests/perf_daily/perf_pr_multi_instance.py ... publish

It does NOT start veloflux/emqx/prometheus/grafana.

Default behavior:
  phase A: rate_critical=100, rate_best=100, duration=60s
  phase B: rate_critical=5,   rate_best=100, duration=60s

Usage:
  ./scripts/perf_pr_host/run_workload.sh

Options:
  --phase <a|b|both>              default: both
  --base-url <url>                default: http://127.0.0.1:8080
  --broker-url <url>              default: tcp://127.0.0.1:1883
  --topic-critical <topic>        default: /perf/pr/critical
  --topic-best <topic>            default: /perf/pr/best
  --pipeline-id-critical <id>     default: perf_pr_pipeline_critical
  --pipeline-id-best <id>         default: perf_pr_pipeline_best
  --critical-instance-id <id>     default: fi_critical
  --best-instance-id <id>         default: fi_best
  --columns <n>                   default: 15000
  --cases <n>                     default: 20
  --str-len <n>                   default: 10
  --qos <n>                       default: 1
  --sql-mode <explicit|star>      default: explicit
  --timeout-secs <n>              default: 60
  --phase-a-duration-secs <n>     default: 60
  --phase-a-rate-critical <n>     default: 100
  --phase-a-rate-best <n>         default: 100
  --phase-b-duration-secs <n>     default: 60
  --phase-b-rate-critical <n>     default: 5
  --phase-b-rate-best <n>         default: 100
  --taskset-cpu <cpu-list>        optional, e.g. 1
EOF
}

PHASE="both"
BASE_URL="http://127.0.0.1:8080"
BROKER_URL="tcp://127.0.0.1:1883"
TOPIC_CRITICAL="/perf/pr/critical"
TOPIC_BEST="/perf/pr/best"
PIPELINE_ID_CRITICAL="perf_pr_pipeline_critical"
PIPELINE_ID_BEST="perf_pr_pipeline_best"
CRITICAL_INSTANCE_ID="fi_critical"
BEST_INSTANCE_ID="fi_best"
COLS="15000"
CASES="20"
STR_LEN="10"
QOS="1"
SQL_MODE="explicit"
TIMEOUT_SECS="60"
PHASE_A_SECS="60"
PHASE_A_RATE_CRIT="100"
PHASE_A_RATE_BEST="100"
PHASE_B_SECS="60"
PHASE_B_RATE_CRIT="5"
PHASE_B_RATE_BEST="100"
TASKSET_CPU=""

while [ $# -gt 0 ]; do
  case "$1" in
    --phase) PHASE="${2:-}"; shift 2;;
    --base-url) BASE_URL="${2:-}"; shift 2;;
    --broker-url) BROKER_URL="${2:-}"; shift 2;;
    --topic-critical) TOPIC_CRITICAL="${2:-}"; shift 2;;
    --topic-best) TOPIC_BEST="${2:-}"; shift 2;;
    --pipeline-id-critical) PIPELINE_ID_CRITICAL="${2:-}"; shift 2;;
    --pipeline-id-best) PIPELINE_ID_BEST="${2:-}"; shift 2;;
    --critical-instance-id) CRITICAL_INSTANCE_ID="${2:-}"; shift 2;;
    --best-instance-id) BEST_INSTANCE_ID="${2:-}"; shift 2;;
    --columns) COLS="${2:-}"; shift 2;;
    --cases) CASES="${2:-}"; shift 2;;
    --str-len) STR_LEN="${2:-}"; shift 2;;
    --qos) QOS="${2:-}"; shift 2;;
    --sql-mode) SQL_MODE="${2:-}"; shift 2;;
    --timeout-secs) TIMEOUT_SECS="${2:-}"; shift 2;;
    --phase-a-duration-secs) PHASE_A_SECS="${2:-}"; shift 2;;
    --phase-a-rate-critical) PHASE_A_RATE_CRIT="${2:-}"; shift 2;;
    --phase-a-rate-best) PHASE_A_RATE_BEST="${2:-}"; shift 2;;
    --phase-b-duration-secs) PHASE_B_SECS="${2:-}"; shift 2;;
    --phase-b-rate-critical) PHASE_B_RATE_CRIT="${2:-}"; shift 2;;
    --phase-b-rate-best) PHASE_B_RATE_BEST="${2:-}"; shift 2;;
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

run_phase() {
  local phase_name="$1"
  local duration_secs="$2"
  local rate_critical="$3"
  local rate_best="$4"
  echo "running phase ${phase_name}: duration=${duration_secs}s, rate_critical=${rate_critical}, rate_best=${rate_best}"

  local cmd=(
    python3 tests/perf_daily/perf_pr_multi_instance.py
    --base-url "${BASE_URL}"
    --timeout-secs "${TIMEOUT_SECS}"
    --critical-instance-id "${CRITICAL_INSTANCE_ID}"
    --best-instance-id "${BEST_INSTANCE_ID}"
    --broker-url "${BROKER_URL}"
    --topic-critical "${TOPIC_CRITICAL}"
    --topic-best "${TOPIC_BEST}"
    --qos "${QOS}"
    --columns "${COLS}"
    --sql-mode "${SQL_MODE}"
    --pipeline-id-critical "${PIPELINE_ID_CRITICAL}"
    --pipeline-id-best "${PIPELINE_ID_BEST}"
    --cases "${CASES}"
    --str-len "${STR_LEN}"
    --duration-secs "${duration_secs}"
    --rate-critical "${rate_critical}"
    --rate-best "${rate_best}"
    --no-metrics
  )
  cmd+=(publish)

  if [ -n "${TASKSET_CPU}" ]; then
    env PYTHONPYCACHEPREFIX=tmp/pycache taskset -c "${TASKSET_CPU}" "${cmd[@]}"
  else
    env PYTHONPYCACHEPREFIX=tmp/pycache "${cmd[@]}"
  fi
}

if [ "${PHASE}" = "a" ] || [ "${PHASE}" = "both" ]; then
  run_phase "a" "${PHASE_A_SECS}" "${PHASE_A_RATE_CRIT}" "${PHASE_A_RATE_BEST}"
fi
if [ "${PHASE}" = "b" ] || [ "${PHASE}" = "both" ]; then
  run_phase "b" "${PHASE_B_SECS}" "${PHASE_B_RATE_CRIT}" "${PHASE_B_RATE_BEST}"
fi

echo "workload done"
