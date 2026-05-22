#!/usr/bin/env bash
set -euo pipefail

URL="${1:-rtsp://127.0.0.1:8554/camera_1}"
SIZE="${VIDEO_TESTSRC_SIZE:-1280x720}"
RATE="${VIDEO_TESTSRC_RATE:-30}"

if ! command -v ffmpeg >/dev/null 2>&1; then
  echo "ffmpeg is required to publish the RTSP test source" >&2
  exit 127
fi

exec ffmpeg -re -f lavfi -i "testsrc=size=${SIZE}:rate=${RATE}" \
  -c:v libx264 -tune zerolatency -g "${RATE}" -pix_fmt yuv420p -f rtsp "${URL}"
