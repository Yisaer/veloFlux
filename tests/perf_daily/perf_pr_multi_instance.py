#!/usr/bin/env python3

import argparse
import json
import os
import sys
import threading
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

from mqtt_qos0_pub import MqttError, parse_tcp_broker_url, publish_qos1_with_timing

import perf_daily


class PerfPrError(RuntimeError):
    pass


def _encode_json(obj: Any) -> bytes:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _labels_have_kv(labels: str, key: str, value: str) -> bool:
    return f'{key}="{value}"' in labels


def extract_openmetrics_block_multi_instance(
    text: str,
    ts_ms: int,
    instance: str,
    include_meta: bool,
) -> str:
    out_lines: List[str] = []
    for line in text.splitlines():
        if not line:
            continue
        if include_meta and (line.startswith("# HELP ") or line.startswith("# TYPE ")):
            parts = line.split(" ", 3)
            if len(parts) >= 3 and parts[2] in perf_daily.TRACKED_METRICS:
                out_lines.append(line)
            continue

        m = perf_daily._SAMPLE_RE.match(line)
        if not m:
            continue

        name = m.group(1)
        if name not in perf_daily.TRACKED_METRICS:
            continue

        labels = perf_daily._ensure_instance_label(m.group(2), instance=instance)
        if name == "processor_records_in_total" and not _labels_have_kv(labels, "kind", "datasource"):
            continue
        value = m.group(3)
        out_lines.append(f"{name}{labels} {value} {ts_ms}")
    return "\n".join(out_lines) + ("\n" if out_lines else "")


class MultiOpenMetricsDumper:
    def __init__(
        self,
        metrics_urls: List[Tuple[str, str]],
        timeout_secs: float,
        interval_ms: int,
        out_path: str,
    ) -> None:
        self.metrics_urls = metrics_urls  # [(instance_label, metrics_url)]
        self.timeout_secs = timeout_secs
        self.interval_ms = interval_ms
        self.out_path = out_path
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
        try:
            with open(self.out_path, "a", encoding="utf-8") as f:
                f.write("# EOF\n")
        except Exception:
            pass

    def _run(self) -> None:
        interval = max(0.05, self.interval_ms / 1000.0)
        include_meta = True
        with open(self.out_path, "w", encoding="utf-8") as f:
            while not self.stop.is_set():
                try:
                    ts_ms = int(time.time() * 1000)
                    any_block = False
                    for idx, (instance, url) in enumerate(self.metrics_urls):
                        text = perf_daily.scrape_prometheus_text(url, timeout_secs=self.timeout_secs)
                        block = extract_openmetrics_block_multi_instance(
                            text,
                            ts_ms=ts_ms,
                            instance=instance,
                            include_meta=include_meta and idx == 0,
                        )
                        if block:
                            f.write(block)
                            any_block = True
                    if any_block:
                        f.flush()
                    include_meta = False
                    self.samples += 1
                except Exception:
                    self.errors += 1
                time.sleep(interval)

    def result(self) -> Dict[str, Any]:
        return {
            "metrics_urls": [{"instance": inst, "url": url} for inst, url in self.metrics_urls],
            "scrape_interval_ms": self.interval_ms,
            "samples": self.samples,
            "errors": self.errors,
            "openmetrics_path": self.out_path,
        }


def create_pipeline_body(pipeline_id: str, sql: str, flow_instance_id: Optional[str]) -> Dict[str, Any]:
    body = perf_daily.create_pipeline_body(pipeline_id, sql)
    if flow_instance_id is not None and flow_instance_id.strip():
        body["flow_instance_id"] = flow_instance_id
    return body


def provision_two_pipelines(
    base_url: str,
    timeout_secs: float,
    stream_name: str,
    pipeline_id_default: str,
    pipeline_id_extra: str,
    extra_instance_id: str,
    columns: int,
    broker_url: str,
    topic: str,
    qos: int,
    force: bool,
    no_start: bool,
    sql_mode: str,
) -> None:
    client = perf_daily.Client(base_url, timeout_secs)
    sql = perf_daily.build_select_sql_by_mode(stream_name, columns, sql_mode=sql_mode)
    stream_req = perf_daily.create_stream_body(stream_name, columns, broker_url, topic, qos)

    if force:
        perf_daily.delete_all_pipelines(client)

    try:
        client.request_json("POST", "/streams", stream_req)
    except perf_daily.ApiError as e:
        if e.status_code != 409:
            raise

    default_req = create_pipeline_body(pipeline_id_default, sql, flow_instance_id=None)
    extra_req = create_pipeline_body(pipeline_id_extra, sql, flow_instance_id=extra_instance_id)

    perf_daily._ignore_not_found(
        lambda: client.request_text("DELETE", f"/pipelines/{urllib.parse.quote(pipeline_id_default)}")
    )
    perf_daily._ignore_not_found(
        lambda: client.request_text("DELETE", f"/pipelines/{urllib.parse.quote(pipeline_id_extra)}")
    )

    client.request_json("POST", "/pipelines", default_req)
    client.request_json("POST", "/pipelines", extra_req)

    if not no_start:
        client.request_text("POST", f"/pipelines/{urllib.parse.quote(pipeline_id_default)}/start", None)
        client.request_text("POST", f"/pipelines/{urllib.parse.quote(pipeline_id_extra)}/start", None)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="PR perf scenario: Wide MQTT with 2 pipelines (default + extra worker)."
    )
    p.add_argument("--base-url", default="http://127.0.0.1:8080")
    p.add_argument("--timeout-secs", type=float, default=60.0)

    p.add_argument("--stream-name", default=perf_daily.DEFAULT_STREAM_NAME)
    p.add_argument("--pipeline-id-default", default="perf_pr_pipeline_default")
    p.add_argument("--pipeline-id-extra", default="perf_pr_pipeline_extra")
    p.add_argument("--extra-instance-id", default="fi_1")
    p.add_argument("--columns", type=int, default=15000)
    p.add_argument("--sql-mode", choices=("explicit", "star"), default="explicit")

    p.add_argument("--broker-url", default="tcp://127.0.0.1:1883")
    p.add_argument("--topic", default="/perf/daily")
    p.add_argument("--qos", type=int, default=1)

    p.add_argument("--cases", type=int, default=20)
    p.add_argument("--str-len", type=int, default=10)
    p.add_argument("--duration-secs", type=int, default=120)
    p.add_argument("--rate", type=int, default=25)

    p.add_argument("--metrics-url-default", default="http://127.0.0.1:9898/metrics")
    p.add_argument("--metrics-url-extra", default="http://127.0.0.1:19891/metrics")
    p.add_argument("--scrape-interval-ms", type=int, default=15000)
    p.add_argument("--out-dir", default="tmp/perf_pr_ci/out")
    p.add_argument("--no-metrics", action="store_true")

    p.add_argument("--force", action="store_true")
    p.add_argument("--no-start", action="store_true")
    p.add_argument("--dry-run", action="store_true")

    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("provision")
    sub.add_parser("publish")
    sub.add_parser("run")
    return p


def main(argv: List[str]) -> int:
    args = build_parser().parse_args(argv)

    if args.columns <= 0:
        raise PerfPrError("--columns must be > 0")
    if args.cases < 0:
        raise PerfPrError("--cases must be >= 0")
    if args.str_len <= 0:
        raise PerfPrError("--str-len must be > 0")
    if args.duration_secs < 0:
        raise PerfPrError("--duration-secs must be >= 0")
    if args.rate < 0:
        raise PerfPrError("--rate must be >= 0")
    if args.qos != 1:
        raise PerfPrError("--qos must be 1 for publish (QoS1 only)")

    perf_daily.wait_for_manager(args.base_url, timeout_secs=args.timeout_secs, wait_secs=60.0)
    broker = parse_tcp_broker_url(args.broker_url)
    perf_daily.wait_for_tcp(broker.host, broker.port, wait_secs=60.0)

    if args.command in ("provision", "run"):
        provision_two_pipelines(
            base_url=args.base_url,
            timeout_secs=args.timeout_secs,
            stream_name=args.stream_name,
            pipeline_id_default=args.pipeline_id_default,
            pipeline_id_extra=args.pipeline_id_extra,
            extra_instance_id=args.extra_instance_id,
            columns=args.columns,
            broker_url=args.broker_url,
            topic=args.topic,
            qos=args.qos,
            force=args.force,
            no_start=args.no_start,
            sql_mode=args.sql_mode,
        )

    if args.command in ("publish", "run"):
        if args.dry_run:
            payloads = perf_daily.generate_payload_cases(5, min(args.cases, 1), args.str_len)
            print(f"sample payload bytes: {len(payloads[0]) if payloads else 0}", file=sys.stderr)
            return 0

        payloads = perf_daily.generate_payload_cases(args.columns, args.cases, args.str_len)
        payload_bytes = len(payloads[0]) if payloads else 0

        dumper = None
        os.makedirs(args.out_dir, exist_ok=True)
        openmetrics_path = os.path.join(args.out_dir, "metrics.openmetrics")

        publish_start_ts_ms = 0
        publish_end_ts_ms = 0
        if args.command == "run" and not args.no_metrics:
            dumper = MultiOpenMetricsDumper(
                metrics_urls=[
                    ("default", args.metrics_url_default),
                    (args.extra_instance_id, args.metrics_url_extra),
                ],
                timeout_secs=args.timeout_secs,
                interval_ms=args.scrape_interval_ms,
                out_path=openmetrics_path,
            )
            dumper.start()

        pub = None
        try:
            pub = publish_qos1_with_timing(
                broker_url=args.broker_url,
                topic=args.topic,
                payloads=payloads,
                duration_secs=args.duration_secs,
                rate_per_sec=args.rate,
                client_id=f"perf-pr-{os.getpid()}",
                keepalive_secs=60,
            )
        except MqttError as e:
            raise PerfPrError(str(e)) from None
        finally:
            if pub is None:
                publish_start_ts_ms = int(time.time() * 1000)
                publish_end_ts_ms = publish_start_ts_ms
            else:
                publish_start_ts_ms = pub.start_ts_ms
                publish_end_ts_ms = pub.end_ts_ms
            if dumper is not None:
                dumper.finish()

        if dumper is not None:
            perf_daily.trim_openmetrics_to_window(
                openmetrics_path, start_ms=publish_start_ts_ms, end_ms=publish_end_ts_ms
            )
            duration_s = max(0.001, (publish_end_ts_ms - publish_start_ts_ms) / 1000.0)
            result_path = os.path.join(args.out_dir, "result.json")
            result = {
                "publish_start_ts_ms": publish_start_ts_ms,
                "publish_end_ts_ms": publish_end_ts_ms,
                "publish_duration_secs": duration_s,
                "sent_messages": pub.sent if pub is not None else 0,
                "effective_rate_mps": (pub.sent / duration_s) if pub is not None else 0.0,
                "payload_bytes": payload_bytes,
                "config": {
                    "base_url": args.base_url,
                    "stream_name": args.stream_name,
                    "pipeline_id": args.pipeline_id_default,
                    "pipeline_ids": [args.pipeline_id_default, args.pipeline_id_extra],
                    "extra_instance_id": args.extra_instance_id,
                    "columns": args.columns,
                    "sql_mode": args.sql_mode,
                    "broker_url": args.broker_url,
                    "topic": args.topic,
                    "qos": args.qos,
                    "cases": args.cases,
                    "str_len": args.str_len,
                    "duration_secs": args.duration_secs,
                    "rate": args.rate,
                },
                "metrics": dumper.result(),
            }
            with open(result_path, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            print(f"result_json: {result_path}")
            print(f"openmetrics: {openmetrics_path}")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except (PerfPrError, perf_daily.ApiError) as e:
        print(f"error: {e}", file=sys.stderr)
        raise SystemExit(2)

