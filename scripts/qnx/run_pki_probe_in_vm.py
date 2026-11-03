#!/usr/bin/env python3
"""Run the PKI probe in the distributed QNX VM using an ephemeral SSH key."""

import os
import pty
import re
import secrets
import select
import shlex
import subprocess
import threading
import time
import tty
from pathlib import Path


ROOT = Path.cwd() / "tmp/qnx-pki"
VM = ROOT / "vm"
KEY = ROOT / "vm-ssh-key"
START_LOG = ROOT / "start.log"
PROBE_LOG = ROOT / "probe.log"
SSH_PORT = "2223"


class SerialConsole:
    def __init__(self, master_fd: int, log_path: Path):
        self.master_fd = master_fd
        self.output = ""
        self.condition = threading.Condition()
        self.log_path = log_path
        self.thread = threading.Thread(target=self._read, daemon=True)
        self.thread.start()

    def _read(self):
        with self.log_path.open("wb") as log:
            while True:
                try:
                    readable, _, _ = select.select([self.master_fd], [], [], 1)
                    if not readable:
                        continue
                    data = os.read(self.master_fd, 4096)
                    if not data:
                        break
                except OSError:
                    break
                log.write(data)
                log.flush()
                with self.condition:
                    self.output = (self.output + data.decode("utf-8", "replace"))[-65536:]
                    self.condition.notify_all()

    def wait_for(self, pattern: str, timeout: int, process: subprocess.Popen):
        deadline = time.monotonic() + timeout
        with self.condition:
            while True:
                if re.search(pattern, self.output.replace("\r", ""), re.MULTILINE):
                    return
                if process.poll() is not None:
                    raise RuntimeError(f"QEMU exited with status {process.returncode}")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"Serial console did not show {pattern!r} within {timeout}s")
                self.condition.wait(min(remaining, 2))

    def send(self, command: str):
        os.write(self.master_fd, (command + "\r").encode())


def run(command, *, timeout=120, capture=False):
    result = subprocess.run(
        command,
        check=False,
        timeout=timeout,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
    )
    if result.returncode != 0:
        if capture and result.stdout:
            print(result.stdout, flush=True)
        raise RuntimeError(f"Command failed with status {result.returncode}: {command[0]}")
    return result.stdout if capture else None


def main():
    run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(KEY)])
    public_key = (KEY.with_suffix(KEY.suffix + ".pub")).read_text().strip()

    env = os.environ.copy()
    env.update(
        RUN_DIR=str(ROOT / "run"),
        VM_OUTPUT_DIR=str(VM / "image"),
        HOST_SSH_PORT=SSH_PORT,
        LOG_FILE=str(ROOT / "console.log"),
        PIDFILE=str(ROOT / "run/qemu.pid"),
    )
    master_fd, slave_fd = pty.openpty()
    tty.setraw(slave_fd)
    process = None
    try:
        process = subprocess.Popen(
            [str(VM / "scripts/start-vm.sh")],
            stdin=slave_fd,
            stdout=slave_fd,
            stderr=slave_fd,
            env=env,
            start_new_session=True,
        )
        os.close(slave_fd)
        slave_fd = -1
        console = SerialConsole(master_fd, START_LOG)
        print("Waiting for QNX startup on the serial console", flush=True)
        console.wait_for(r"^Startup complete$", 600, process)

        ready = "QNX_SERIAL_READY_" + secrets.token_hex(8)
        console.send("")
        console.send(f"echo {ready}")
        console.wait_for(rf"^{ready}$", 60, process)
        print("QNX serial console accepts shell commands", flush=True)

        installed = "QNX_SSH_KEY_INSTALLED_" + secrets.token_hex(8)
        command = (
            "mkdir -p /data/home/root/.ssh && "
            f"printf '%s\\n' {shlex.quote(public_key)} >> /data/home/root/.ssh/authorized_keys && "
            "chmod 700 /data/home/root/.ssh && "
            "chmod 600 /data/home/root/.ssh/authorized_keys && "
            f"echo {installed}"
        )
        console.send(command)
        console.wait_for(rf"^{installed}$", 60, process)
        print("Installed ephemeral SSH public key through QNX serial console", flush=True)

        ssh_options = [
            "-i", str(KEY),
            "-o", "IdentitiesOnly=yes",
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=5",
            "-o", "ConnectionAttempts=1",
        ]
        ssh = ["ssh", *ssh_options, "-p", SSH_PORT]
        scp = ["scp", "-O", *ssh_options, "-P", SSH_PORT]
        deadline = time.monotonic() + 120
        while True:
            result = subprocess.run(
                [*ssh, "root@127.0.0.1", "true"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
                timeout=15,
            )
            if result.returncode == 0:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError(f"QNX SSH login failed: {result.stderr.strip()}")
            time.sleep(5)
        print("QNX SSH login succeeded", flush=True)

        remote = "root@127.0.0.1"
        run([
            *ssh,
            remote,
            "mkdir -p /data/home/root/apps /data/home/root/pki-mnt/etc && "
            "ln -sP /data/home/root/pki-mnt /mnt && test -d /mnt/etc",
        ])
        run([
            *scp,
            str(ROOT / "sdk/lib/libcn_x509_common.so"),
            str(ROOT / "sdk/lib/libcn_x509_crypto.so"),
            str(ROOT / "sdk/lib/libcn_x509_proxy.so"),
            str(ROOT / "pki-crypto-probe"),
            f"{remote}:/data/home/root/apps/",
        ])
        secureclient = ROOT / "libsecureclient.so.1"
        if secureclient.is_file():
            run([*scp, str(secureclient), f"{remote}:/data/home/root/apps/"])
        run([
            *scp,
            str(ROOT / "sdk/conf/cn_x509_config.ini"),
            f"{remote}:/mnt/etc/cn_x509_config.ini",
        ])
        dependencies = subprocess.run(
            [
                *ssh,
                remote,
                "LD_LIBRARY_PATH=/data/home/root/apps "
                "ldd /data/home/root/apps/libcn_x509_proxy.so",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
            timeout=30,
        )
        print("QNX vendor library dependencies:\n" + dependencies.stdout, flush=True)
        output = run([
            *ssh,
            remote,
            "LD_LIBRARY_PATH=/data/home/root/apps PKI_KEY_TYPE=1 "
            "/data/home/root/apps/pki-crypto-probe",
        ], capture=True)
        PROBE_LOG.write_text(output)
        print(output, end="", flush=True)
        for expected in (
            "PASS: empty AAD encryption and decryption round trip",
            "PASS: nonempty AAD encryption and decryption round trip",
        ):
            if expected not in output:
                raise RuntimeError(f"Probe output did not contain: {expected}")
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        if slave_fd >= 0:
            os.close(slave_fd)
        os.close(master_fd)


if __name__ == "__main__":
    main()
