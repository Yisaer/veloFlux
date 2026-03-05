#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
One-click run workload for perf PR wide MQTT scenario.

This script calls Go workload binary:
  scripts/perf_pr_host/go_workload/bin/go_workload

It does NOT start veloflux/emqx/prometheus/grafana.

Default behavior:
  qps=20, columns=15000, str-len=10, duration=60s

Usage:
  ./scripts/perf_pr_host/run_workload.sh

Options:
  --broker-url <url>              default: tcp://127.0.0.1:1883
  --topic-critical <topic>        default: /perf/pr/critical
  --topic-best <topic>            default: /perf/pr/best
  --columns <n>                   default: 15000
  --cases <n>                     default: 20
  --str-len <n>                   default: 10
  --qos <n>                       default: 1
  --duration-secs <n>             default: 60
  --qps <n>                       default: 20
  --qps-critical <n>              optional, override critical qps
  --qps-best <n>                  optional, override best qps
  --workload-bin <path>           default: scripts/perf_pr_host/go_workload/bin/go_workload
  --client-prefix <prefix>        default: perf-pr-go
  --taskset-cpu <cpu-list>        optional, e.g. 1
EOF
}

BROKER_URL="tcp://127.0.0.1:1883"
TOPIC_CRITICAL="/perf/pr/critical"
TOPIC_BEST="/perf/pr/best"
COLS="15000"
CASES="20"
STR_LEN="10"
QOS="1"
DURATION_SECS="60"
QPS="20"
QPS_CRITICAL=""
QPS_BEST=""
CLIENT_PREFIX="perf-pr-go"
TASKSET_CPU=""
WORKLOAD_BIN=""

while [ $# -gt 0 ]; do
  case "$1" in
    --broker-url) BROKER_URL="${2:-}"; shift 2;;
    --topic-critical) TOPIC_CRITICAL="${2:-}"; shift 2;;
    --topic-best) TOPIC_BEST="${2:-}"; shift 2;;
    --columns) COLS="${2:-}"; shift 2;;
    --cases) CASES="${2:-}"; shift 2;;
    --str-len) STR_LEN="${2:-}"; shift 2;;
    --qos) QOS="${2:-}"; shift 2;;
    --duration-secs) DURATION_SECS="${2:-}"; shift 2;;
    --qps) QPS="${2:-}"; shift 2;;
    --qps-critical) QPS_CRITICAL="${2:-}"; shift 2;;
    --qps-best) QPS_BEST="${2:-}"; shift 2;;
    --client-prefix) CLIENT_PREFIX="${2:-}"; shift 2;;
    --workload-bin) WORKLOAD_BIN="${2:-}"; shift 2;;
    --taskset-cpu) TASKSET_CPU="${2:-}"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "unknown arg: $1" >&2; usage; exit 2;;
  esac
done

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
  run
  --broker-url "${BROKER_URL}"
  --topic-critical "${TOPIC_CRITICAL}"
  --topic-best "${TOPIC_BEST}"
  --columns "${COLS}"
  --cases "${CASES}"
  --str-len "${STR_LEN}"
  --qos "${QOS}"
  --duration-secs "${DURATION_SECS}"
  --qps "${QPS}"
  --client-prefix "${CLIENT_PREFIX}"
)

if [ -n "${QPS_CRITICAL}" ]; then
  CMD+=(--qps-critical "${QPS_CRITICAL}")
fi
if [ -n "${QPS_BEST}" ]; then
  CMD+=(--qps-best "${QPS_BEST}")
fi

if [ -n "${TASKSET_CPU}" ]; then
  exec taskset -c "${TASKSET_CPU}" "${CMD[@]}"
fi

exec "${CMD[@]}"
