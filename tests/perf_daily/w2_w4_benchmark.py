#!/usr/bin/env python3
"""Run the W2/W4 MQTT aggregation benchmark against one local VeloFlux process."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.request

from mqtt_qos0_pub import (
    _build_connect_packet,
    _build_publish_packet,
    _read_connack,
)
from perf_daily import Client, wait_for_manager, wait_for_tcp


RATE = 5_000
DURATION = 30
PROFILE_SECONDS = 20
METRIC_NAMES = (
    "veloflux_runtime_cpu_usage_percent",
    "veloflux_runtime_memory_usage_bytes",
    "veloflux_mqtt_source_messages_in_total",
    "veloflux_processor_records_in_total",
    "veloflux_processor_records_out_total",
    "veloflux_processor_errors_total",
)
SAMPLE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([-+\d.eE]+)")
LABEL = re.compile(r'([a-zA-Z_][a-zA-Z_0-9]*)="((?:\\.|[^"\\])*)"')


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=("w2", "w4"), required=True)
    parser.add_argument("--mode", choices=("cpu", "profile"), required=True)
    parser.add_argument("--rep", type=int, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--config", type=Path, default=Path("etc/config.example.yaml"))
    parser.add_argument("--rate", type=int, default=RATE)
    parser.add_argument("--duration", type=int, default=DURATION)
    return parser.parse_args()


def metrics() -> list[dict]:
    with urllib.request.urlopen("http://127.0.0.1:8080/metrics", timeout=5) as response:
        body = response.read().decode("utf-8")
    result = []
    for line in body.splitlines():
        match = SAMPLE.match(line)
        if match and match.group(1) in METRIC_NAMES:
            result.append({
                "name": match.group(1),
                "labels": dict(LABEL.findall(match.group(2) or "")),
                "value": float(match.group(3)),
            })
    return result


def total(samples: list[dict], name: str, pipeline: str | None = None) -> float:
    return sum(
        sample["value"]
        for sample in samples
        if sample["name"] == name
        and (pipeline is None or sample["labels"].get("pipeline_id") == pipeline)
    )


def process_ticks(pid: int) -> int:
    stat = Path(f"/proc/{pid}/stat").read_text()
    fields = stat.rsplit(") ", 1)[1].split()
    return int(fields[11]) + int(fields[12])


def capture_profile(path: Path, errors: list[str], seconds: int) -> None:
    try:
        url = f"http://127.0.0.1:6060/debug/pprof/profile?seconds={seconds}&freq=99"
        with urllib.request.urlopen(url, timeout=seconds + 20) as response:
            profile = response.read()
        if len(profile) < 100:
            raise RuntimeError("CPU profile is empty")
        path.write_bytes(profile)
    except Exception as error:
        errors.append(str(error))


def sample_metrics(path: Path, stop: threading.Event, errors: list[str]) -> None:
    try:
        with path.open("w") as output:
            while not stop.is_set():
                timestamp = int(time.time() * 1000)
                for sample in metrics():
                    labels = ",".join(
                        f'{key}="{value}"' for key, value in sample["labels"].items()
                    )
                    suffix = "{" + labels + "}" if labels else ""
                    output.write(
                        f'{sample["name"]}{suffix} {sample["value"]} {timestamp}\n'
                    )
                output.flush()
                stop.wait(1)
    except Exception as error:
        errors.append(f"metrics sampling: {error}")


def publish(case: str, rate: int, duration: int) -> dict:
    devices = 1_000 if case == "w2" else 10_000
    topic = "bench/telemetry" if case == "w2" else "bench/vehicles/{device}/telemetry"
    with socket.create_connection(("127.0.0.1", 1883), timeout=10) as conn:
        conn.sendall(_build_connect_packet(f"veloflux-{case}-{os.getpid()}"))
        _read_connack(conn)
        start = time.monotonic()
        sent = 0
        while sent < rate * duration:
            due = min(rate * duration, int((time.monotonic() - start) * rate) + 1)
            if due <= sent:
                time.sleep(max(0, min(0.001, start + sent / rate - time.monotonic())))
                continue
            batch_end = min(due, sent + 1_000)
            batch = bytearray()
            for sequence in range(sent, batch_end):
                device = f"dev_{sequence % devices}"
                message = {
                    "id": sequence,
                    "device": device,
                    "temp": 20.0 + sequence % 100 / 10,
                    "speed": 30.0 + sequence % 200 / 10,
                    "ts": int(time.time() * 1000),
                }
                payload = json.dumps(message, separators=(",", ":")).encode()
                batch.extend(_build_publish_packet(topic.replace("{device}", device), payload))
            conn.sendall(batch)
            sent = batch_end
        elapsed = time.monotonic() - start
    return {"sent": sent, "duration_seconds": elapsed, "rate": sent / elapsed}


def provision(case: str, pipeline: str) -> None:
    client = Client("http://127.0.0.1:8080", timeout_secs=30)
    topic = "bench/telemetry" if case == "w2" else "bench/vehicles/+/telemetry"
    stream = {
        "name": "telem", "revision": 1, "type": "mqtt",
        "props": {"broker_url": "tcp://127.0.0.1:1883", "topic": topic, "qos": 0},
        "schema": {"type": "json", "props": {"columns": [
            {"name": "id", "data_type": "int64"},
            {"name": "device", "data_type": "string"},
            {"name": "temp", "data_type": "float64"},
            {"name": "speed", "data_type": "float64"},
            {"name": "ts", "data_type": "int64"},
        ]}},
        "decoder": {"type": "json", "props": {}},
        "shared": False,
    }
    sql = (
        "SELECT device, count(*) AS n, avg(temp) AS avg_temp, "
        "max(speed) AS max_speed FROM telem "
        "GROUP BY device, tumblingwindow('ss', 1)"
        if case == "w2" else
        "SELECT device, count(*) AS n, avg(speed) AS avg_speed, "
        "max(temp) AS max_temp FROM telem "
        "GROUP BY device, tumblingwindow('ss', 1)"
    )
    definition = {
        "id": pipeline, "revision": 1, "sql": sql,
        "sinks": [{
            "type": "nop", "props": {"log": False},
            "encoder": {"type": "none", "props": {}},
        }],
        "options": {"eventtime": {"enabled": False}},
    }
    client.request_json("POST", "/streams", stream)
    client.request_json("POST", "/pipelines", definition)
    client.request_text("POST", f"/pipelines/{pipeline}/start")


def run(args: argparse.Namespace) -> dict:
    if args.rate <= 0 or args.duration <= 0 or args.rep <= 0:
        raise ValueError("rate, duration, and rep must be positive")
    if args.mode == "profile" and args.duration < PROFILE_SECONDS:
        raise ValueError("profile duration must fit within the publish interval")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    tag = f"{args.case}-{args.mode}-{args.rep}"
    pipeline = f"bench_{tag.replace('-', '_')}"
    log_file = output / f"{tag}.veloflux.log"
    result_file = output / f"{tag}.json"
    profile_file = output / f"{tag}.pb"
    metrics_file = output / f"{tag}.openmetrics"
    data_dir = output / f"{tag}.data"
    errors: list[str] = []
    result = {"case": args.case, "mode": args.mode, "rep": args.rep,
              "pipeline": pipeline, "sink": "nop", "window_seconds": 1,
              "target_rate": args.rate, "target_duration": args.duration}
    with log_file.open("wb") as log:
        process = subprocess.Popen(
            [str(args.binary.resolve()), "--config", str(args.config.resolve()),
             "--data-dir", str(data_dir)], stdout=log, stderr=subprocess.STDOUT,
            env={**os.environ, "VELOFLUX_PROFILING__ENABLED": "true"},
        )
        try:
            wait_for_tcp("127.0.0.1", 1883, wait_secs=30)
            wait_for_manager("http://127.0.0.1:8080", timeout_secs=5, wait_secs=60)
            provision(args.case, pipeline)
            time.sleep(2)
            before = metrics()
            ticks_before = process_ticks(process.pid)
            started = time.monotonic()
            stop_sampling = threading.Event()
            metrics_thread = threading.Thread(
                target=sample_metrics, args=(metrics_file, stop_sampling, errors)
            )
            metrics_thread.start()
            profile_thread = None
            try:
                if args.mode == "profile":
                    profile_thread = threading.Thread(
                        target=capture_profile,
                        args=(profile_file, errors, PROFILE_SECONDS),
                    )
                    profile_thread.start()
                result["publisher"] = publish(args.case, args.rate, args.duration)
                ticks_after = process_ticks(process.pid)
            finally:
                stop_sampling.set()
                metrics_thread.join(timeout=10)
            elapsed = time.monotonic() - started
            if profile_thread:
                profile_thread.join(timeout=PROFILE_SECONDS + 25)
                if profile_thread.is_alive():
                    errors.append("profile capture did not finish")
            # One additional window boundary allows the final aggregate to flush.
            time.sleep(3)
            after = metrics()
            clock_ticks = os.sysconf("SC_CLK_TCK")
            result["cpu_mean_cores"] = (ticks_after - ticks_before) / clock_ticks / elapsed
            result["metrics_before"] = before
            result["metrics_after"] = after
            result["source_received"] = total(after, "veloflux_mqtt_source_messages_in_total") - total(
                before, "veloflux_mqtt_source_messages_in_total")
            result["processor_input"] = total(after, "veloflux_processor_records_in_total", pipeline) - total(
                before, "veloflux_processor_records_in_total", pipeline)
            result["processor_output"] = total(after, "veloflux_processor_records_out_total", pipeline) - total(
                before, "veloflux_processor_records_out_total", pipeline)
            result["processor_errors"] = total(after, "veloflux_processor_errors_total", pipeline) - total(
                before, "veloflux_processor_errors_total", pipeline)
            result["profile_ok"] = args.mode != "profile" or profile_file.is_file()
            result["metrics_samples"] = sum(
                1 for line in metrics_file.read_text().splitlines()
                if line.startswith("veloflux_runtime_cpu_usage_percent")
            )
            result["complete"] = (
                process.poll() is None
                and result["publisher"]["sent"] == args.rate * args.duration
                and result["publisher"]["rate"] >= args.rate * 0.95
                and result["source_received"] >= result["publisher"]["sent"] * 0.99
                and result["processor_input"] > 0
                and result["processor_output"] > 0
                and result["processor_errors"] == 0
                and result["metrics_samples"] >= 2
                and result["profile_ok"] and not errors
            )
        except Exception as error:
            errors.append(str(error))
            result["complete"] = False
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
    result["errors"] = errors
    result_file.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: value for key, value in result.items()
                      if key not in ("metrics_before", "metrics_after")}, indent=2))
    return result


if __name__ == "__main__":
    sys.exit(0 if run(arguments())["complete"] else 1)
