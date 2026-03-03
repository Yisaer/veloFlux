#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
One-click resource manager for 2 streams + 2 pipelines (critical/best).

It does NOT start veloflux/emqx/prometheus/grafana.

Usage:
  ./scripts/perf_pr_host/deploy_stream_pipeline.sh <command>

Commands:
  create   create/reconcile streams+pipelines (idempotent), do NOT start pipelines
  delete   delete pipelines then streams (idempotent)
  start    start both pipelines (idempotent)
  pause    stop both pipelines (idempotent, graceful)

Options:
  --base-url <url>                 default: http://127.0.0.1:8080
  --broker-url <url>               default: tcp://127.0.0.1:1883
  --stream-name-critical <name>    default: perf_pr_stream_critical
  --stream-name-best <name>        default: perf_pr_stream_best
  --topic-critical <topic>         default: /perf/pr/critical
  --topic-best <topic>             default: /perf/pr/best
  --pipeline-id-critical <id>      default: perf_pr_pipeline_critical
  --pipeline-id-best <id>          default: perf_pr_pipeline_best
  --critical-instance-id <id>      default: fi_critical
  --best-instance-id <id>          default: fi_best
  --columns <n>                    default: 15000
  --cases <n>                      default: 20
  --str-len <n>                    default: 10
  --qos <n>                        default: 1
  --sql-mode <explicit|star>       default: explicit
  --timeout-secs <n>               default: 60
  --stop-mode <quick|graceful>     default: graceful (for pause/delete)
  --stop-timeout-ms <ms>           default: 5000 (for pause/delete)
  --taskset-cpu <cpu-list>         optional, e.g. 1
EOF
}

COMMAND=""
BASE_URL="http://127.0.0.1:8080"
BROKER_URL="tcp://127.0.0.1:1883"
STREAM_NAME_CRITICAL="perf_pr_stream_critical"
STREAM_NAME_BEST="perf_pr_stream_best"
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
STOP_MODE="graceful"
STOP_TIMEOUT_MS="5000"
TASKSET_CPU=""
REQ_CODE=""
REQ_BODY=""

urlencode() {
  python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1"
}

request() {
  local method="$1"
  local path="$2"
  local data="${3:-}"
  local tmp
  tmp="$(mktemp)"
  if [ -n "${data}" ]; then
    REQ_CODE="$(curl -sS -o "${tmp}" -w '%{http_code}' -X "${method}" \
      -H 'Content-Type: application/json' -H 'Accept: application/json, text/plain, */*' \
      --data "${data}" \
      "${BASE_URL}${path}" || true)"
  else
    REQ_CODE="$(curl -sS -o "${tmp}" -w '%{http_code}' -X "${method}" \
      -H 'Accept: application/json, text/plain, */*' \
      "${BASE_URL}${path}" || true)"
  fi
  REQ_BODY="$(cat "${tmp}")"
  rm -f "${tmp}"
}

pause_pipeline_idempotent() {
  local pipeline_id="$1"
  local encoded_id
  encoded_id="$(urlencode "${pipeline_id}")"
  request POST "/pipelines/${encoded_id}/stop?mode=${STOP_MODE}&timeout_ms=${STOP_TIMEOUT_MS}"
  if [ "${REQ_CODE}" = "200" ]; then
    echo "[pause] ${pipeline_id}: stopped"
    return 0
  fi
  if [ "${REQ_CODE}" = "404" ]; then
    echo "[pause] ${pipeline_id}: not found (skip)"
    return 0
  fi
  echo "[ERROR] pause ${pipeline_id}: HTTP ${REQ_CODE}: ${REQ_BODY}" >&2
  exit 1
}

start_pipeline_idempotent() {
  local pipeline_id="$1"
  local encoded_id
  encoded_id="$(urlencode "${pipeline_id}")"
  request POST "/pipelines/${encoded_id}/start"
  if [ "${REQ_CODE}" = "200" ]; then
    echo "[start] ${pipeline_id}: started"
    return 0
  fi
  if [ "${REQ_CODE}" = "404" ]; then
    echo "[start] ${pipeline_id}: not found (skip)"
    return 0
  fi
  echo "[ERROR] start ${pipeline_id}: HTTP ${REQ_CODE}: ${REQ_BODY}" >&2
  exit 1
}

delete_pipeline_idempotent() {
  local pipeline_id="$1"
  local encoded_id
  encoded_id="$(urlencode "${pipeline_id}")"
  request DELETE "/pipelines/${encoded_id}"
  if [ "${REQ_CODE}" = "200" ]; then
    echo "[delete pipeline] ${pipeline_id}: deleted"
    return 0
  fi
  if [ "${REQ_CODE}" = "404" ]; then
    echo "[delete pipeline] ${pipeline_id}: not found (skip)"
    return 0
  fi
  echo "[ERROR] delete pipeline ${pipeline_id}: HTTP ${REQ_CODE}: ${REQ_BODY}" >&2
  exit 1
}

delete_stream_idempotent() {
  local stream_name="$1"
  local encoded_name
  encoded_name="$(urlencode "${stream_name}")"
  local attempts=5
  local i
  for i in $(seq 1 "${attempts}"); do
    request DELETE "/streams/${encoded_name}"
    if [ "${REQ_CODE}" = "200" ]; then
      echo "[delete stream] ${stream_name}: deleted"
      return 0
    fi
    if [ "${REQ_CODE}" = "404" ]; then
      echo "[delete stream] ${stream_name}: not found (skip)"
      return 0
    fi
    if [ "${REQ_CODE}" = "409" ] && [ "${i}" -lt "${attempts}" ]; then
      echo "[delete stream] ${stream_name}: still referenced, retry ${i}/${attempts}"
      sleep 1
      continue
    fi
    echo "[ERROR] delete stream ${stream_name}: HTTP ${REQ_CODE}: ${REQ_BODY}" >&2
    exit 1
  done
}

if [ $# -eq 0 ]; then
  usage
  exit 2
fi

COMMAND="$1"
shift

while [ $# -gt 0 ]; do
  case "$1" in
    --base-url) BASE_URL="${2:-}"; shift 2;;
    --broker-url) BROKER_URL="${2:-}"; shift 2;;
    --stream-name-critical) STREAM_NAME_CRITICAL="${2:-}"; shift 2;;
    --stream-name-best) STREAM_NAME_BEST="${2:-}"; shift 2;;
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
    --stop-mode) STOP_MODE="${2:-}"; shift 2;;
    --stop-timeout-ms) STOP_TIMEOUT_MS="${2:-}"; shift 2;;
    --taskset-cpu) TASKSET_CPU="${2:-}"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "unknown arg: $1" >&2; usage; exit 2;;
  esac
done

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

case "${COMMAND}" in
  create)
    CMD=(
      python3 tests/perf_daily/perf_pr_multi_instance.py
      --base-url "${BASE_URL}"
      --timeout-secs "${TIMEOUT_SECS}"
      --stream-name-critical "${STREAM_NAME_CRITICAL}"
      --stream-name-best "${STREAM_NAME_BEST}"
      --broker-url "${BROKER_URL}"
      --topic-critical "${TOPIC_CRITICAL}"
      --topic-best "${TOPIC_BEST}"
      --qos "${QOS}"
      --columns "${COLS}"
      --sql-mode "${SQL_MODE}"
      --pipeline-id-critical "${PIPELINE_ID_CRITICAL}"
      --pipeline-id-best "${PIPELINE_ID_BEST}"
      --critical-instance-id "${CRITICAL_INSTANCE_ID}"
      --best-instance-id "${BEST_INSTANCE_ID}"
      --cases "${CASES}"
      --str-len "${STR_LEN}"
      --no-start
      provision
    )
    if [ -n "${TASKSET_CPU}" ]; then
      exec env PYTHONPYCACHEPREFIX=tmp/pycache taskset -c "${TASKSET_CPU}" "${CMD[@]}"
    fi
    exec env PYTHONPYCACHEPREFIX=tmp/pycache "${CMD[@]}"
    ;;
  delete)
    pause_pipeline_idempotent "${PIPELINE_ID_CRITICAL}"
    pause_pipeline_idempotent "${PIPELINE_ID_BEST}"
    delete_pipeline_idempotent "${PIPELINE_ID_CRITICAL}"
    delete_pipeline_idempotent "${PIPELINE_ID_BEST}"
    delete_stream_idempotent "${STREAM_NAME_CRITICAL}"
    delete_stream_idempotent "${STREAM_NAME_BEST}"
    ;;
  start)
    start_pipeline_idempotent "${PIPELINE_ID_CRITICAL}"
    start_pipeline_idempotent "${PIPELINE_ID_BEST}"
    ;;
  pause)
    pause_pipeline_idempotent "${PIPELINE_ID_CRITICAL}"
    pause_pipeline_idempotent "${PIPELINE_ID_BEST}"
    ;;
  *)
    echo "unknown command: ${COMMAND}" >&2
    usage
    exit 2
    ;;
esac
