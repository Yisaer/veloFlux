#!/usr/bin/env python3
"""Run an actual release artifact in the clean glibc 2.28 runtime image."""
import argparse
import json
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


def smoke(executable, ffi):
    if subprocess.check_output(["getconf", "GNU_LIBC_VERSION"]).strip() != b"glibc 2.28":
        raise RuntimeError("smoke test requires the glibc 2.28 runtime")
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    base = "http://127.0.0.1:{}".format(port)

    def request(path, body=None):
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(base + path, data=data, headers={"Content-Type": "application/json"})
        try:
            with opener.open(req, timeout=3) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            raise RuntimeError("{} returned {}: {}".format(path, error.code, error.read().decode()))


    with tempfile.TemporaryDirectory(prefix="vf-release-smoke-") as tmp:
        work = Path(tmp)
        config = work / "config.yaml"
        config.write_text("profiling:\n  enabled: false\nserver:\n  manager_addr: '127.0.0.1:{}'\n".format(port))
        source = work / "input.txt"
        source.write_text("glibc-baseline-smoke\n")
        output = work / "output"
        args = [str(executable), str(config)] if ffi else [str(executable), "--config", str(config), "--data-dir", str(work / "data")]
        with (work / "server.log").open("w+") as log:
            process = subprocess.Popen(args, cwd=str(work), stdout=log, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 60
                while True:
                    if process.poll() is not None:
                        raise RuntimeError("artifact exited before Manager became ready")
                    try:
                        request("/status")
                        break
                    except (OSError, urllib.error.URLError):
                        if time.monotonic() >= deadline:
                            raise RuntimeError("Manager readiness timed out")
                        time.sleep(0.1)
                request("/streams", {
                    "name": "ReleaseInput", "revision": 1, "type": "file",
                    "props": {"path": str(source)}, "decoder": {"type": "file_line"},
                })
                request("/pipelines", {
                    "id": "ReleaseSmoke", "revision": 1, "sql": "SELECT * FROM ReleaseInput",
                    "sinks": [{"id": "FileOutput", "type": "file",
                               "props": {"path": str(output), "filename_pattern": "result_{seq}.json"},
                               "encoder": {"type": "json"}}],
                })
                request("/pipelines/ReleaseSmoke/start", {})
                deadline = time.monotonic() + 30
                while not output.exists() or not list(output.glob("result_*.json")):
                    if process.poll() is not None or time.monotonic() >= deadline:
                        raise RuntimeError("file pipeline produced no output")
                    time.sleep(0.1)
                payload = json.loads(next(output.glob("result_*.json")).read_text())
                rows = payload if isinstance(payload, list) else [payload]
                if not rows or rows[0]["line"].strip() != "glibc-baseline-smoke":
                    raise RuntimeError("unexpected file pipeline output: " + repr(payload))
                request("/pipelines/ReleaseSmoke/stop?mode=graceful&timeout_ms=5000", {})
            finally:
                process.terminate()
                try:
                    result = process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise RuntimeError("artifact did not shut down")
                finally:
                    log.seek(0)
                    print(log.read())
            if result != 0:
                raise RuntimeError("artifact exited with status {}".format(result))
    print("glibc 2.28 startup, REST, file pipeline, and shutdown passed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("executable", type=Path)
    parser.add_argument("--ffi", action="store_true")
    arguments = parser.parse_args()
    smoke(arguments.executable.resolve(), arguments.ffi)
