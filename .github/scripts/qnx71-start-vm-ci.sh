#!/usr/bin/env bash

set -euo pipefail

run_dir="${RUN_DIR:-/workspace/qnx710-aarch64-run}"
vm_output_dir="${VM_OUTPUT_DIR:-/vm/image}"
qemu_bin="${QEMU_BIN:-qemu-system-aarch64}"
host_ssh_addr="${HOST_SSH_ADDR:-127.0.0.1}"
host_ssh_port="${HOST_SSH_PORT:-2223}"
host_qconn_addr="${HOST_QCONN_ADDR:-127.0.0.1}"
host_qconn_port="${HOST_QCONN_PORT:-8001}"
qnx_ram="${QNX_RAM:-2G}"
qnx_mac="${QNX_MAC:-52:54:00:0f:f0:b0}"
kernel_image="${KERNEL_IMAGE:-${vm_output_dir}/ifs.bin}"
disk_image="${DISK_IMAGE:-${vm_output_dir}/disk-qemu}"
pidfile="${PIDFILE:-${run_dir}/qemu.pid}"

mkdir -p "$run_dir"
for file in "$kernel_image" "$disk_image"; do
  test -f "$file"
done

echo "Starting QNX 7.1 aarch64 VM (SSH ${host_ssh_addr}:${host_ssh_port})"
echo "  machine: vexpress-a15 / cortex-a57 / virtio"
echo "  serial: stdio (QEMU monitor disabled)"

exec "$qemu_bin" \
  -machine vexpress-a15 \
  -cpu cortex-a57 \
  -smp 2 \
  -m "$qnx_ram" \
  -kernel "$kernel_image" \
  -drive "file=${disk_image},format=raw,if=none,id=drv0" \
  -device virtio-blk-device,drive=drv0 \
  -netdev "user,id=net0,hostfwd=tcp:${host_ssh_addr}:${host_ssh_port}-:22,hostfwd=tcp:${host_qconn_addr}:${host_qconn_port}-:8000" \
  -device "virtio-net-device,netdev=net0,mac=${qnx_mac}" \
  -object rng-random,filename=/dev/urandom,id=rng0 \
  -device virtio-rng-device,rng=rng0 \
  -pidfile "$pidfile" \
  -monitor none \
  -serial stdio \
  -nographic
