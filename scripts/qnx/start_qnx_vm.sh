#!/usr/bin/env bash
set -euo pipefail

log_dir="tmp/qnx-qemu"
work_dir="tmp/qnx-qemu-work"
timeout_secs=300
hold_secs=10
hostname="veloflux-qnx"
arch="x86_64"
host_ssh_port=2222
host_qconn_port=8000
host_manager_port=18080
qnx_cpus=2
qnx_ram="1024M"
qnx_install_dir="${QNX_INSTALL_DIR:-/opt/qnx710}"
qnx_qsc_dir="${QNX_QSC_DIR:-/opt/qnxsoftwarecenter}"
qnx_mirror_baseline="${QNX_MIRROR_BASELINE:-qnx710}"
qemuvirt_package="${QNX_QEMU_VIRT_PACKAGE:-com.qnx.qnx710.target.driver.virtio.startup}"
virtio_driver_package="${QNX_VIRTIO_PACKAGE:-com.qnx.qnx710.target.driver.virtio}"
deactivate_retries="${QNX_DEACTIVATE_RETRIES:-20}"
deactivate_timeout_secs="${QNX_DEACTIVATE_TIMEOUT_SECS:-180}"
deactivate_sleep_secs="${QNX_DEACTIVATE_SLEEP_SECS:-10}"
run_pid=""
key_dir=""
ssh_key=""
delete_key_dir=0
skip_build=0
build_only=0
qnx_license_active=0

usage() {
  cat <<'USAGE'
Usage: start_qnx_vm.sh [options]

Options:
  --log-dir PATH        Directory for startup and console logs.
  --work-dir PATH       Working directory used by mkqnximage.
  --timeout-secs SECS   Maximum time to wait for the forwarded SSH port.
  --hold-secs SECS      Time to keep the VM running after SSH is reachable.
  --hostname NAME       QNX VM hostname.
  --arch ARCH           QNX target architecture.
  --host-ssh-port PORT  Host TCP port forwarded to QNX port 22.
  --host-qconn-port PORT Host TCP port forwarded to QNX port 8000.
  --host-manager-port PORT Host TCP port forwarded to QNX port 8080.
  --cpus COUNT          QEMU vCPU count.
  --ram SIZE            QEMU RAM size.
  --ssh-key PATH        SSH private key used for QNX root login.
  --build-only          Generate the VM image without starting QEMU.
  --skip-build          Start an existing VM image without QNX SDP setup.
USAGE
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --log-dir) log_dir="$2"; shift 2 ;;
    --work-dir) work_dir="$2"; shift 2 ;;
    --timeout-secs) timeout_secs="$2"; shift 2 ;;
    --hold-secs) hold_secs="$2"; shift 2 ;;
    --hostname) hostname="$2"; shift 2 ;;
    --arch) arch="$2"; shift 2 ;;
    --host-ssh-port) host_ssh_port="$2"; shift 2 ;;
    --host-qconn-port) host_qconn_port="$2"; shift 2 ;;
    --host-manager-port) host_manager_port="$2"; shift 2 ;;
    --cpus) qnx_cpus="$2"; shift 2 ;;
    --ram) qnx_ram="$2"; shift 2 ;;
    --ssh-key) ssh_key="$2"; shift 2 ;;
    --build-only) build_only=1; shift ;;
    --skip-build) skip_build=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

mkdir -p "$log_dir" "$work_dir"
log_dir="$(cd "$log_dir" && pwd -P)"
work_dir="$(cd "$work_dir" && pwd -P)"
startup_log="$log_dir/start_qnx_vm.log"
console_log="$log_dir/qnx_console.log"
qemu_pidfile="$log_dir/qemu.pid"

if [ -z "$ssh_key" ]; then
  key_dir="$(mktemp -d)"
  ssh_key="$key_dir/qnx_vm_ed25519"
  delete_key_dir=1
else
  mkdir -p "$(dirname "$ssh_key")"
  ssh_key="$(cd "$(dirname "$ssh_key")" && pwd -P)/$(basename "$ssh_key")"
fi

exec > >(tee -a "$startup_log") 2>&1

deactivate_qnx() {
  [ "$qnx_license_active" -eq 1 ] || return 0
  local qsc_bin="$qnx_qsc_dir/qnxsoftwarecenter/qnxsoftwarecenter_clt"
  local deactivated=0
  set +e
  for attempt in $(seq 1 "$deactivate_retries"); do
    echo "Attempt $attempt/$deactivate_retries to deactivate QNX 7.1 licenses"
    if timeout "${deactivate_timeout_secs}s" "$qsc_bin" \
      -deactivateAll \
      -myqnx.user "${QNX_MYQNX_USER:-}" \
      -myqnx.password "${QNX_MYQNX_PASSWORD:-}" >/dev/null 2>&1; then
      deactivated=1
      echo "QNX license deactivation succeeded on attempt $attempt/$deactivate_retries"
      break
    fi
    echo "QNX license deactivation failed on attempt $attempt/$deactivate_retries" >&2
    [ "$attempt" -lt "$deactivate_retries" ] && sleep "$deactivate_sleep_secs"
  done
  if [ "$deactivated" -ne 1 ]; then
    echo "QNX 7.1 license deactivation failed after $deactivate_retries attempts." >&2
    return 1
  fi
  qnx_license_active=0
  return 0
}

cleanup() {
  local status=$?
  set +e
  trap - EXIT INT TERM
  if [ -n "$run_pid" ] && kill -0 "$run_pid" >/dev/null 2>&1; then
    kill "$run_pid" >/dev/null 2>&1
    wait "$run_pid" >/dev/null 2>&1
  fi
  deactivate_qnx || status=1
  if [ -n "${QNX_LICENSE_KEY:-}" ] && [ -f "$startup_log" ]; then
    sanitized_log="$(mktemp)"
    while IFS= read -r line; do
      printf '%s\n' "${line//${QNX_LICENSE_KEY}/[REDACTED]}"
    done < "$startup_log" > "$sanitized_log"
    mv "$sanitized_log" "$startup_log"
  fi
  rm -rf "$qnx_qsc_dir"/../.qnx/license "$HOME/.qnx/license" "$HOME/.qnx/swupdate/session" "$HOME/.qnx/swupdate/server-cache"
  [ "$delete_key_dir" -eq 1 ] && [ -n "$key_dir" ] && rm -rf "$key_dir"
  exit "$status"
}
trap cleanup EXIT INT TERM

if [ "$skip_build" -eq 0 ]; then
  source "$qnx_install_dir/qnxsdp-env.sh"
  [ -n "${QNX_LICENSE_KEY:-}" ] || { echo "QNX_LICENSE_KEY is required." >&2; exit 1; }
  qsc_bin="$qnx_qsc_dir/qnxsoftwarecenter/qnxsoftwarecenter_clt"
  [ -x "$qsc_bin" ] || qsc_bin="$(find "$qnx_qsc_dir" -type f -name qnxsoftwarecenter_clt -perm -111 | head -n 1)"
  [ -x "$qsc_bin" ] || { echo "qnxsoftwarecenter_clt was not found." >&2; exit 1; }
  qsc() {
    local qsc_output qsc_status
    qsc_output="$(mktemp)"
    qsc_status=0
    "$qsc_bin" "$@" >"$qsc_output" 2>&1 || qsc_status=$?
    if [ "$qsc_status" -ne 0 ]; then
      grep -E '(^| )(Error|ERROR|error):' "$qsc_output" || true
      echo "QNX Software Center command failed with exit code $qsc_status." >&2
    fi
    rm -f "$qsc_output"
    return "$qsc_status"
  }
  list_virtualization_units() {
    local qsc_output
    qsc_output="$(mktemp)"
    "$qsc_bin" -list \
      -myqnx.user "${QNX_MYQNX_USER:-}" \
      -myqnx.password "${QNX_MYQNX_PASSWORD:-}" >"$qsc_output" 2>&1 || true
    echo "Available QNX virtualization-related units:"
    grep -iE 'hypervisor|virtualization|qemu|virtio' "$qsc_output" || echo "<none>"
    rm -f "$qsc_output"
  }
  qsc -addLicenseKey "$QNX_LICENSE_KEY" -listLicenseKeys \
    -myqnx.user "${QNX_MYQNX_USER:-}" -myqnx.password "${QNX_MYQNX_PASSWORD:-}"
  qnx_license_active=1

  if ! find "$QNX_TARGET" -type f -name startup-qemu-virt | grep -q .; then
    qsc -setDebugSymbolsEnabled=false -mirrorBaseline "$qnx_mirror_baseline" \
      -myqnx.user "${QNX_MYQNX_USER:-}" -myqnx.password "${QNX_MYQNX_PASSWORD:-}"
    if ! qsc -setDebugSymbolsEnabled=false -destination "$qnx_install_dir" \
      -installPackage "$qemuvirt_package" -listLicenseKeys \
      -myqnx.user "${QNX_MYQNX_USER:-}" -myqnx.password "${QNX_MYQNX_PASSWORD:-}"; then
      list_virtualization_units
      exit 1
    fi
  fi
  if [ "$arch" != "x86_64" ] && ! find "$QNX_TARGET" -type f -path '*/sbin/devb-virtio' | grep -q .; then
    qsc -setDebugSymbolsEnabled=false -destination "$qnx_install_dir" \
      -installPackage "$virtio_driver_package" -listLicenseKeys \
      -myqnx.user "${QNX_MYQNX_USER:-}" -myqnx.password "${QNX_MYQNX_PASSWORD:-}"
  fi
  command -v mkqnximage >/dev/null 2>&1 || export PATH="$(dirname "$(find "$qnx_install_dir" -type f -name mkqnximage -perm -111 | head -n 1)"):$PATH"
  command -v mkqnximage >/dev/null 2>&1 || { echo "mkqnximage was not found." >&2; exit 1; }
  cd "$work_dir"
  [ -f "$ssh_key" ] || ssh-keygen -q -t ed25519 -N '' -f "$ssh_key"
  mkqnximage --type=qemu --arch="$arch" --hostname="$hostname" \
    --ssh-ident="${ssh_key}.pub" --sshd-pregen=yes --force --build
else
  [ -f "$ssh_key" ] || { echo "QNX SSH key was not found: $ssh_key" >&2; exit 1; }
  cd "$work_dir"
fi

ifs_image="$work_dir/output/ifs.bin"
disk_image="$work_dir/output/disk-qemu"
[ -f "$ifs_image" ] || { echo "QNX IFS image was not generated." >&2; exit 1; }
[ -f "$disk_image" ] || { echo "QNX disk image was not generated." >&2; exit 1; }

if [ "$build_only" -eq 1 ]; then
  echo "QNX QEMU image generated without starting QEMU."
  exit 0
fi

if [ "$arch" = "aarch64" ] || [ "$arch" = "aarch64le" ]; then
  qemu_bin="qemu-system-aarch64"
  qemu_machine="virt"
  qemu_device="virtio-net-pci"
else
  qemu_bin="qemu-system-x86_64"
  qemu_machine="pc"
  qemu_device="e1000"
fi
command -v "$qemu_bin" >/dev/null 2>&1 || {
  echo "QEMU binary was not found: $qemu_bin" >&2
  exit 1
}
"$qemu_bin" -machine "$qemu_machine" -smp "$qnx_cpus" -m "$qnx_ram" \
  -hda "$disk_image" \
  -netdev "user,id=net0,hostfwd=tcp:127.0.0.1:${host_ssh_port}-:22,hostfwd=tcp:127.0.0.1:${host_qconn_port}-:8000,hostfwd=tcp:127.0.0.1:${host_manager_port}-:8080" \
  -device "${qemu_device},netdev=net0,mac=52:54:00:0f:f0:9d" \
  -pidfile "$qemu_pidfile" -kernel "$ifs_image" -nographic >"$console_log" 2>&1 &
run_pid=$!

deadline=$((SECONDS + timeout_secs))
while [ "$SECONDS" -lt "$deadline" ]; do
  kill -0 "$run_pid" >/dev/null 2>&1 || { tail -n 200 "$console_log" >&2 || true; exit 1; }
  if timeout 5 bash -c ": </dev/tcp/127.0.0.1/$host_qconn_port" >/dev/null 2>&1; then
    printf 'QNX_VM_HOST=127.0.0.1\nQNX_VM_SSH_PORT=%s\nQNX_VM_QCONN_PORT=%s\nQNX_VM_MANAGER_PORT=%s\nQNX_VM_SSH_KEY=%s\n' \
      "$host_ssh_port" "$host_qconn_port" "$host_manager_port" "$ssh_key" > "$log_dir/qnx-vm.env"
    break
  fi
  sleep 5
done
[ -f "$log_dir/qnx-vm.env" ] || { tail -n 200 "$console_log" >&2 || true; exit 1; }
sleep "$hold_secs"
