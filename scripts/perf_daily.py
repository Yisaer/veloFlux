#!/usr/bin/env python3

import argparse
import json
import os
import random
import string
import sys
import re
import threading
import time
import urllib.parse
import urllib.request
import socket
from typing import Any, Dict, List, Optional

import sf_rest_tool
from mqtt_qos0_pub import MqttError, publish_qos0


class PerfDailyError(RuntimeError):
    pass


DEFAULT_STREAM_NAME = "perf_daily_stream"
DEFAULT_PIPELINE_ID = "perf_daily_pipeline"
DEFAULT_METRICS_URL = "http://127.0.0.1:9898/metrics"
DEFAULT_OUT_DIR = "tmp/perf_daily"

# Minimal set for step 7 (process-level trend metrics).
TRACKED_METRICS = (
    "cpu_usage",
    "memory_usage_bytes",
    "heap_in_use_bytes",
    "heap_in_allocator_bytes",
)


def _ignore_not_found(fn) -> None:
    try:
        fn()
    except sf_rest_tool.ApiError as e:
        if "HTTP 404" in str(e):
            return
        raise


def _alnum_rand_str(rng: random.Random, length: int) -> str:
    alphabet = string.ascii_letters + string.digits
    # random.choices is fast enough here and keeps code straightforward.
    return "".join(rng.choices(alphabet, k=length))


def generate_payload_cases(column_count: int, cases: int, str_len: int) -> List[bytes]:
    if cases <= 0:
        return []
    if column_count <= 0:
        raise PerfDailyError("column_count must be > 0")
    if str_len <= 0:
        raise PerfDailyError("str_len must be > 0")

    # Precompute keys once.
    keys = [f"a{i}" for i in range(1, column_count + 1)]
    rng = random.Random()

    payloads: List[bytes] = []
    for _ in range(cases):
        obj: Dict[str, Any] = {}
        for k in keys:
            obj[k] = _alnum_rand_str(rng, str_len)
        payloads.append(json.dumps(obj, separators=(",", ":")).encode("utf-8"))
    return payloads


def scrape_prometheus_text(url: str, timeout_secs: float) -> str:
    req = urllib.request.Request(url=url, method="GET", headers={"Accept": "text/plain"})
    with urllib.request.urlopen(req, timeout=timeout_secs) as resp:
        raw = resp.read()
        return raw.decode("utf-8", errors="replace") if raw else ""


def parse_prometheus_gauges(text: str) -> Dict[str, float]:
    # Only parse simple gauge samples without labels; ignore counters/histograms.
    # Format: <name>[{labels}] <value> [timestamp]
    out: Dict[str, float] = {}
    for line in text.splitlines():
        if not line or line[0] == "#":
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        name = parts[0].split("{", 1)[0]
        if name not in TRACKED_METRICS:
            continue
        try:
            out[name] = float(parts[1])
        except ValueError:
            continue
    return out


_SAMPLE_RE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([-+]?(?:\d+\.?\d*|\d*\.?\d+)(?:[eE][-+]?\d+)?)(?:\s+\d+)?$")


def _ensure_instance_label(labels: Optional[str], instance: str) -> str:
    if labels is None:
        return f'{{instance="{instance}"}}'
    if "instance=" in labels:
        return labels
    # Insert instance label at the beginning.
    inner = labels[1:-1].strip()
    if not inner:
        return f'{{instance="{instance}"}}'
    return f'{{instance="{instance}",{inner}}}'


def extract_openmetrics_block(text: str, ts_ms: int, instance: str) -> str:
    # Keep HELP/TYPE for tracked metrics; rewrite sample lines to include instance label + timestamp.
    out_lines: List[str] = []
    for line in text.splitlines():
        if not line:
            continue
        if line.startswith("# HELP ") or line.startswith("# TYPE "):
            parts = line.split(" ", 3)
            if len(parts) >= 3 and parts[2] in TRACKED_METRICS:
                out_lines.append(line)
            continue
        m = _SAMPLE_RE.match(line)
        if not m:
            continue
        name = m.group(1)
        if name not in TRACKED_METRICS:
            continue
        labels = _ensure_instance_label(m.group(2), instance=instance)
        value = m.group(3)
        out_lines.append(f"{name}{labels} {value} {ts_ms}")
    return "\n".join(out_lines) + ("\n" if out_lines else "")


class OpenMetricsDumper:
    def __init__(
        self,
        metrics_url: str,
        timeout_secs: float,
        interval_ms: int,
        out_path: str,
        instance: str,
    ) -> None:
        self.metrics_url = metrics_url
        self.timeout_secs = timeout_secs
        self.interval_ms = interval_ms
        self.out_path = out_path
        self.instance = instance
        self.stop = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.samples = 0
        self.errors = 0

    def start(self) -> None:
        os.makedirs(os.path.dirname(self.out_path), exist_ok=True)
        t = threading.Thread(target=self._run, name="metrics-dumper", daemon=True)
        self.thread = t
        t.start()

    def finish(self) -> None:
        self.stop.set()
        if self.thread is not None:
            self.thread.join(timeout=10.0)
        # Append OpenMetrics EOF marker once.
        try:
            with open(self.out_path, "a", encoding="utf-8") as f:
                f.write("# EOF\n")
        except Exception:
            pass

    def _run(self) -> None:
        interval = max(0.05, self.interval_ms / 1000.0)
        with open(self.out_path, "a", encoding="utf-8") as f:
            while not self.stop.is_set():
                try:
                    ts_ms = int(time.time() * 1000)
                    text = scrape_prometheus_text(self.metrics_url, timeout_secs=self.timeout_secs)
                    block = extract_openmetrics_block(text, ts_ms=ts_ms, instance=self.instance)
                    if block:
                        f.write(block)
                        f.flush()
                    self.samples += 1
                except Exception:
                    self.errors += 1
                time.sleep(interval)

    def result(self) -> Dict[str, Any]:
        return {
            "metrics_url": self.metrics_url,
            "scrape_interval_ms": self.interval_ms,
            "samples": self.samples,
            "errors": self.errors,
            "openmetrics_path": self.out_path,
            "instance": self.instance,
        }

def print_human_report(result: Dict[str, Any], result_path: str) -> None:
    cfg = result.get("config", {})
    metrics = result.get("metrics", {})

    print("perf_daily run complete")
    print(f"result_json: {result_path}")
    print(
        "config: stream_name={stream_name} pipeline_id={pipeline_id} columns={columns} cases={cases} str_len={str_len} duration_secs={duration_secs} publish_count={publish_count}".format(
            stream_name=cfg.get("stream_name"),
            pipeline_id=cfg.get("pipeline_id"),
            columns=cfg.get("columns"),
            cases=cfg.get("cases"),
            str_len=cfg.get("str_len"),
            duration_secs=cfg.get("duration_secs", "n/a"),
            publish_count=cfg.get("publish_count"),
        )
    )
    print(
        "metrics: url={url} samples={samples} errors={errors} interval_ms={interval_ms}".format(
            url=metrics.get("metrics_url"),
            samples=metrics.get("samples"),
            errors=metrics.get("errors"),
            interval_ms=metrics.get("scrape_interval_ms"),
        )
    )
    print(f"metrics_openmetrics: {metrics.get('openmetrics_path')}")


def provision(
    base_url: str,
    timeout_secs: float,
    stream_name: str,
    pipeline_id: str,
    column_count: int,
    broker_url: str,
    topic: str,
    qos: int,
    force: bool,
    no_start: bool,
    dry_run: bool,
) -> None:
    client = sf_rest_tool.Client(base_url, timeout_secs)
    sql = sf_rest_tool.build_select_sql(stream_name, column_count)
    stream_req = sf_rest_tool.create_stream_body(stream_name, column_count, broker_url, topic, qos)
    pipeline_req = sf_rest_tool.create_pipeline_body(pipeline_id, sql)
    # Be explicit about JSON encoder/decoder to match the perf scenario contract.
    if pipeline_req.get("sinks"):
        pipeline_req["sinks"][0]["encoder"] = {"type": "json", "props": {}}

    if dry_run:
        print(f"sql bytes: {len(sql.encode('utf-8'))}", file=sys.stderr)
        print(
            f"stream body bytes: {len(json.dumps(stream_req, separators=(',', ':')).encode('utf-8'))}",
            file=sys.stderr,
        )
        print(
            f"pipeline body bytes: {len(json.dumps(pipeline_req, separators=(',', ':')).encode('utf-8'))}",
            file=sys.stderr,
        )
        return

    if force:
        _ignore_not_found(
            lambda: client.request_text(
                "DELETE", f"/pipelines/{urllib.parse.quote(pipeline_id)}"
            )
        )
        _ignore_not_found(
            lambda: client.request_text(
                "DELETE", f"/streams/{urllib.parse.quote(stream_name)}"
            )
        )

    try:
        client.request_json("POST", "/streams", stream_req)
    except sf_rest_tool.ApiError as e:
        # Allow reruns with fixed resource names.
        if "HTTP 409" not in str(e):
            raise

    # Prefer recreate semantics for the pipeline.
    try:
        client.request_text("DELETE", f"/pipelines/{urllib.parse.quote(pipeline_id)}")
    except sf_rest_tool.ApiError:
        pass
    try:
        client.request_json("POST", "/pipelines", pipeline_req)
    except sf_rest_tool.ApiError as e:
        if "HTTP 409" not in str(e):
            raise
    if not no_start:
        client.request_text("POST", f"/pipelines/{urllib.parse.quote(pipeline_id)}/start")


def wait_for_manager(base_url: str, timeout_secs: float, wait_secs: float = 60.0) -> None:
    client = sf_rest_tool.Client(base_url, timeout_secs)
    deadline = time.time() + wait_secs
    last_err: Optional[str] = None
    while time.time() < deadline:
        try:
            client.request_json("GET", "/streams", None)
            return
        except Exception as e:
            last_err = str(e)
            time.sleep(1.0)
    raise PerfDailyError(f"manager not ready after {wait_secs}s: {last_err}")


def wait_for_tcp(host: str, port: int, wait_secs: float = 60.0) -> None:
    deadline = time.time() + wait_secs
    last_err: Optional[str] = None
    while time.time() < deadline:
        s = socket.socket()
        try:
            s.settimeout(2.0)
            s.connect((host, port))
            return
        except Exception as e:
            last_err = str(e)
            time.sleep(1.0)
        finally:
            try:
                s.close()
            except Exception:
                pass
    raise PerfDailyError(f"broker {host}:{port} not ready after {wait_secs}s: {last_err}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Daily perf scenario: 15k-column MQTT stream + nop pipeline.")
    p.add_argument("--base-url", default="http://127.0.0.1:8080")
    p.add_argument("--timeout-secs", type=float, default=60.0)

    p.add_argument("--stream-name", default=DEFAULT_STREAM_NAME)
    p.add_argument("--pipeline-id", default=DEFAULT_PIPELINE_ID)
    p.add_argument("--columns", type=int, default=15000)

    p.add_argument("--broker-url", default="tcp://127.0.0.1:1883")
    p.add_argument("--topic", default="/perf/daily")
    p.add_argument("--qos", type=int, default=0)

    p.add_argument("--cases", type=int, default=10)
    p.add_argument("--str-len", type=int, default=16)
    p.add_argument("--publish-count", type=int, default=0)
    p.add_argument("--publish-interval-ms", type=int, default=0)
    p.add_argument(
        "--duration-secs",
        type=int,
        default=300,
        help="Publish duration; overrides --publish-count when > 0",
    )

    p.add_argument("--metrics-url", default=DEFAULT_METRICS_URL)
    p.add_argument("--scrape-interval-ms", type=int, default=5000)
    p.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    p.add_argument("--no-metrics", action="store_true")

    p.add_argument("--force", action="store_true")
    p.add_argument("--no-start", action="store_true")
    p.add_argument("--dry-run", action="store_true")

    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("provision")
    sub.add_parser("publish")
    sub.add_parser("run")
    report = sub.add_parser("report", help="Render a human-friendly summary from an existing result.json")
    report.add_argument("--input", required=True, help="Path to result.json produced by `run`")
    return p


def main(argv: List[str]) -> int:
    args = build_parser().parse_args(argv)

    if args.command == "report":
        with open(args.input, "r", encoding="utf-8") as f:
            data = json.load(f)
        print_human_report(data, result_path=args.input)
        return 0

    if args.columns <= 0:
        raise PerfDailyError("--columns must be > 0")
    if args.cases < 0:
        raise PerfDailyError("--cases must be >= 0")
    if args.str_len <= 0:
        raise PerfDailyError("--str-len must be > 0")
    if args.publish_count < 0:
        raise PerfDailyError("--publish-count must be >= 0")
    if args.duration_secs < 0:
        raise PerfDailyError("--duration-secs must be >= 0")
    if args.qos != 0:
        # Our stdlib MQTT publisher only supports QoS0 for now.
        raise PerfDailyError("--qos must be 0 for publish (QoS0 only)")

    if args.command in ("provision", "run"):
        wait_for_manager(args.base_url, timeout_secs=args.timeout_secs, wait_secs=60.0)
        provision(
            base_url=args.base_url,
            timeout_secs=args.timeout_secs,
            stream_name=args.stream_name,
            pipeline_id=args.pipeline_id,
            column_count=args.columns,
            broker_url=args.broker_url,
            topic=args.topic,
            qos=args.qos,
            force=args.force,
            no_start=args.no_start,
            dry_run=args.dry_run,
        )

    if args.command in ("publish", "run"):
        if args.broker_url.startswith("tcp://") and ":" in args.broker_url[len("tcp://") :]:
            host, port_s = args.broker_url[len("tcp://") :].rsplit(":", 1)
            try:
                wait_for_tcp(host, int(port_s), wait_secs=60.0)
            except ValueError:
                pass
        if args.dry_run:
            payloads = generate_payload_cases(5, min(args.cases, 1), args.str_len)
            print(f"sample payload bytes: {len(payloads[0]) if payloads else 0}", file=sys.stderr)
            return 0

        dumper = None
        start_ts = time.time()
        if args.command == "run" and not args.no_metrics:
            out_path = os.path.join(args.out_dir, "metrics.openmetrics")
            dumper = OpenMetricsDumper(
                metrics_url=args.metrics_url,
                timeout_secs=args.timeout_secs,
                interval_ms=args.scrape_interval_ms,
                out_path=out_path,
                instance="local",
            )
            dumper.start()

        payloads = generate_payload_cases(args.columns, args.cases, args.str_len)
        try:
            publish_qos0(
                broker_url=args.broker_url,
                topic=args.topic,
                payloads=payloads,
                publish_count=args.publish_count,
                publish_interval_ms=args.publish_interval_ms,
                client_id=f"perf-daily-{os.getpid()}",
                keepalive_secs=60,
                duration_secs=args.duration_secs,
            )
        except MqttError as e:
            raise PerfDailyError(str(e)) from None
        finally:
            if dumper is not None:
                dumper.finish()

        if dumper is not None:
            end_ts = time.time()
            os.makedirs(args.out_dir, exist_ok=True)
            result_path = os.path.join(args.out_dir, "result.json")
            result = {
                "ts_start": start_ts,
                "ts_end": end_ts,
                "duration_secs": end_ts - start_ts,
                "config": {
                    "base_url": args.base_url,
                    "stream_name": args.stream_name,
                    "pipeline_id": args.pipeline_id,
                    "columns": args.columns,
                    "broker_url": args.broker_url,
                    "topic": args.topic,
                    "qos": args.qos,
                    "cases": args.cases,
                    "str_len": args.str_len,
                    "duration_secs": args.duration_secs,
                    "publish_count": args.publish_count,
                    "publish_interval_ms": args.publish_interval_ms,
                },
                "metrics": dumper.result(),
            }
            with open(result_path, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            print_human_report(result, result_path=result_path)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except (PerfDailyError, sf_rest_tool.ApiError) as e:
        print(f"error: {e}", file=sys.stderr)
        raise SystemExit(2)
