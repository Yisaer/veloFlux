#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  cat <<'EOF'
Install tools for converting jemalloc heap dump (heap_v2) to flamegraph SVG on Ubuntu.

Installs:
  - python3
  - libjemalloc-dev (provides jeprof on most Ubuntu repos)
  - flamegraph.pl (via apt "flamegraph" if available, otherwise via GitHub clone)

Usage:
  scripts/setup_heap_tools_ubuntu.sh
EOF
  exit 0
fi

SUDO=""
if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  if command -v sudo >/dev/null 2>&1; then
    SUDO="sudo"
  else
    echo "error: need root privileges (run as root or install sudo)" >&2
    exit 1
  fi
fi

echo "[setup] updating apt indexes"
$SUDO apt-get update -y

echo "[setup] ensuring 'universe' repository is enabled"
$SUDO apt-get install -y software-properties-common
$SUDO add-apt-repository -y universe >/dev/null 2>&1 || true
$SUDO apt-get update -y

echo "[setup] installing base packages"
$SUDO apt-get install -y python3 git perl curl libjemalloc-dev

if ! command -v jeprof >/dev/null 2>&1; then
  echo "warning: 'jeprof' not found in PATH after installing libjemalloc-dev." >&2
  echo "warning: check 'dpkg -L libjemalloc-dev | grep jeprof' and adjust PATH if needed." >&2
else
  echo "[setup] jeprof: $(command -v jeprof)"
fi

if command -v flamegraph.pl >/dev/null 2>&1; then
  echo "[setup] flamegraph.pl already available: $(command -v flamegraph.pl)"
  exit 0
fi

echo "[setup] installing flamegraph.pl"
if $SUDO apt-get install -y flamegraph >/dev/null 2>&1; then
  if command -v flamegraph.pl >/dev/null 2>&1; then
    echo "[setup] flamegraph.pl: $(command -v flamegraph.pl)"
    exit 0
  fi
fi

install_dir="/opt/FlameGraph"
if [[ -d "${install_dir}" ]]; then
  echo "[setup] using existing ${install_dir} (skip update)"
  if [[ ! -f "${install_dir}/flamegraph.pl" ]]; then
    echo "error: ${install_dir} exists but flamegraph.pl not found" >&2
    echo "error: remove ${install_dir} and rerun, or point install_dir to a valid FlameGraph checkout" >&2
    exit 1
  fi
else
  echo "[setup] cloning FlameGraph to ${install_dir}"
  $SUDO mkdir -p "$(dirname "${install_dir}")"
  $SUDO rm -rf "${install_dir}"
  $SUDO git clone --depth 1 https://github.com/brendangregg/FlameGraph.git "${install_dir}"
fi

$SUDO mkdir -p /usr/local/bin

bin_dir="/usr/local/bin"
case ":${PATH:-}:" in
  *":/usr/local/bin:"*) bin_dir="/usr/local/bin" ;;
  *":/usr/bin:"*) bin_dir="/usr/bin" ;;
  *)
    echo "warning: /usr/local/bin is not in PATH; installing to /usr/bin for easier access" >&2
    bin_dir="/usr/bin"
    ;;
esac

$SUDO mkdir -p "${bin_dir}"
$SUDO ln -sf "${install_dir}/flamegraph.pl" "${bin_dir}/flamegraph.pl"
$SUDO chmod +x "${install_dir}/flamegraph.pl" || true

echo "[setup] flamegraph.pl installed at: ${bin_dir}/flamegraph.pl"
