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
    stream_name_critical: str,
    stream_name_best: str,
    pipeline_id_critical: str,
    pipeline_id_best: str,
    critical_instance_id: str,
    best_instance_id: str,
    columns: int,
    broker_url: str,
    topic_critical: str,
    topic_best: str,
    qos: int,
    force: bool,
    no_start: bool,
    sql_mode: str,
) -> None:
    client = perf_daily.Client(base_url, timeout_secs)
    sql_critical = perf_daily.build_select_sql_by_mode(stream_name_critical, columns, sql_mode=sql_mode)
    sql_best = perf_daily.build_select_sql_by_mode(stream_name_best, columns, sql_mode=sql_mode)
    stream_critical_req = perf_daily.create_stream_body(stream_name_critical, columns, broker_url, topic_critical, qos)
    stream_best_req = perf_daily.create_stream_body(stream_name_best, columns, broker_url, topic_best, qos)

    if force:
        perf_daily.delete_all_pipelines(client)

    try:
        client.request_json("POST", "/streams", stream_critical_req)
    except perf_daily.ApiError as e:
        if e.status_code != 409:
            raise

    try:
        client.request_json("POST", "/streams", stream_best_req)
    except perf_daily.ApiError as e:
        if e.status_code != 409:
            raise

    critical_req = create_pipeline_body(pipeline_id_critical, sql_critical, flow_instance_id=critical_instance_id)
    best_req = create_pipeline_body(pipeline_id_best, sql_best, flow_instance_id=best_instance_id)

    perf_daily._ignore_not_found(
        lambda: client.request_text("DELETE", f"/pipelines/{urllib.parse.quote(pipeline_id_critical)}")
    )
    perf_daily._ignore_not_found(
        lambda: client.request_text("DELETE", f"/pipelines/{urllib.parse.quote(pipeline_id_best)}")
    )

    client.request_json("POST", "/pipelines", critical_req)
    client.request_json("POST", "/pipelines", best_req)

    if not no_start:
        client.request_text("POST", f"/pipelines/{urllib.parse.quote(pipeline_id_critical)}/start", None)
        client.request_text("POST", f"/pipelines/{urllib.parse.quote(pipeline_id_best)}/start", None)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="PR perf scenario: Wide MQTT with 2 pipelines (critical + best).")
    p.add_argument("--base-url", default="http://127.0.0.1:8080")
    p.add_argument("--timeout-secs", type=float, default=60.0)

    p.add_argument("--stream-name-critical", default="perf_pr_stream_critical")
    p.add_argument("--stream-name-best", default="perf_pr_stream_best")
    p.add_argument("--pipeline-id-critical", default="perf_pr_pipeline_critical")
    p.add_argument("--pipeline-id-best", default="perf_pr_pipeline_best")
    p.add_argument("--critical-instance-id", default="fi_critical")
    p.add_argument("--best-instance-id", default="fi_best")
    p.add_argument("--columns", type=int, default=15000)
    p.add_argument("--sql-mode", choices=("explicit", "star"), default="explicit")

    p.add_argument("--broker-url", default="tcp://127.0.0.1:1883")
    p.add_argument("--topic-critical", default="/perf/pr/critical")
    p.add_argument("--topic-best", default="/perf/pr/best")
    p.add_argument("--qos", type=int, default=1)

    p.add_argument("--cases", type=int, default=20)
    p.add_argument("--str-len", type=int, default=10)
    p.add_argument("--duration-secs", type=int, default=120)
    p.add_argument("--rate-critical", type=int, default=25)
    p.add_argument("--rate-best", type=int, default=25)

    p.add_argument("--metrics-url-critical", default="http://127.0.0.1:19891/metrics")
    p.add_argument("--metrics-url-best", default="http://127.0.0.1:19892/metrics")
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
    if args.rate_critical < 0:
        raise PerfPrError("--rate-critical must be >= 0")
    if args.rate_best < 0:
        raise PerfPrError("--rate-best must be >= 0")
    if args.qos != 1:
        raise PerfPrError("--qos must be 1 for publish (QoS1 only)")

    perf_daily.wait_for_manager(args.base_url, timeout_secs=args.timeout_secs, wait_secs=60.0)
    broker = parse_tcp_broker_url(args.broker_url)
    perf_daily.wait_for_tcp(broker.host, broker.port, wait_secs=60.0)

    if args.command in ("provision", "run"):
        provision_two_pipelines(
            base_url=args.base_url,
            timeout_secs=args.timeout_secs,
            stream_name_critical=args.stream_name_critical,
            stream_name_best=args.stream_name_best,
            pipeline_id_critical=args.pipeline_id_critical,
            pipeline_id_best=args.pipeline_id_best,
            critical_instance_id=args.critical_instance_id,
            best_instance_id=args.best_instance_id,
            columns=args.columns,
            broker_url=args.broker_url,
            topic_critical=args.topic_critical,
            topic_best=args.topic_best,
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
        if not args.no_metrics:
            dumper = MultiOpenMetricsDumper(
                metrics_urls=[
                    (args.critical_instance_id, args.metrics_url_critical),
                    (args.best_instance_id, args.metrics_url_best),
                ],
                timeout_secs=args.timeout_secs,
                interval_ms=args.scrape_interval_ms,
                out_path=openmetrics_path,
            )
            dumper.start()

        pubs: Dict[str, Any] = {}

        def _pub_worker(label: str, topic: str, rate_per_sec: int) -> None:
            try:
                pubs[label] = publish_qos1_with_timing(
                    broker_url=args.broker_url,
                    topic=topic,
                    payloads=payloads,
                    duration_secs=args.duration_secs,
                    rate_per_sec=rate_per_sec,
                    client_id=f"perf-pr-{label}-{os.getpid()}",
                    keepalive_secs=60,
                )
            except Exception as e:
                pubs[label] = e

        try:
            threads = [
                threading.Thread(
                    target=_pub_worker,
                    kwargs={
                        "label": "critical",
                        "topic": args.topic_critical,
                        "rate_per_sec": args.rate_critical,
                    },
                    name="mqtt-pub-critical",
                ),
                threading.Thread(
                    target=_pub_worker,
                    kwargs={
                        "label": "best",
                        "topic": args.topic_best,
                        "rate_per_sec": args.rate_best,
                    },
                    name="mqtt-pub-best",
                ),
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            errors = [v for v in pubs.values() if isinstance(v, Exception)]
            if errors:
                # Prefer MQTT errors for more actionable messages.
                mqtt_errs = [e for e in errors if isinstance(e, MqttError)]
                if mqtt_errs:
                    raise PerfPrError(str(mqtt_errs[0])) from None
                raise PerfPrError(str(errors[0])) from None
        finally:
            results = [v for v in pubs.values() if hasattr(v, "start_ts_ms") and hasattr(v, "end_ts_ms")]
            if not results:
                publish_start_ts_ms = int(time.time() * 1000)
                publish_end_ts_ms = publish_start_ts_ms
            else:
                publish_start_ts_ms = min(int(r.start_ts_ms) for r in results)
                publish_end_ts_ms = max(int(r.end_ts_ms) for r in results)
            if dumper is not None:
                dumper.finish()

        if dumper is not None:
            perf_daily.trim_openmetrics_to_window(
                openmetrics_path, start_ms=publish_start_ts_ms, end_ms=publish_end_ts_ms
            )
            duration_s = max(0.001, (publish_end_ts_ms - publish_start_ts_ms) / 1000.0)
            result_path = os.path.join(args.out_dir, "result.json")
            sent_total = sum(int(v.sent) for v in results)
            result = {
                "publish_start_ts_ms": publish_start_ts_ms,
                "publish_end_ts_ms": publish_end_ts_ms,
                "publish_duration_secs": duration_s,
                "sent_messages": sent_total,
                "effective_rate_mps": (sent_total / duration_s) if sent_total else 0.0,
                "payload_bytes": payload_bytes,
                "config": {
                    "base_url": args.base_url,
                    "stream_name_critical": args.stream_name_critical,
                    "stream_name_best": args.stream_name_best,
                    "pipeline_id": args.pipeline_id_critical,
                    "pipeline_ids": [args.pipeline_id_critical, args.pipeline_id_best],
                    "critical_instance_id": args.critical_instance_id,
                    "best_instance_id": args.best_instance_id,
                    "columns": args.columns,
                    "sql_mode": args.sql_mode,
                    "broker_url": args.broker_url,
                    "topic_critical": args.topic_critical,
                    "topic_best": args.topic_best,
                    "qos": args.qos,
                    "cases": args.cases,
                    "str_len": args.str_len,
                    "duration_secs": args.duration_secs,
                    "rate": args.rate_critical + args.rate_best,
                    "rate_critical": args.rate_critical,
                    "rate_best": args.rate_best,
                },
                "metrics": dumper.result(),
                "publish": {
                    "critical": {
                        "topic": args.topic_critical,
                        "rate_per_sec": args.rate_critical,
                        "sent_messages": int(pubs["critical"].sent) if "critical" in pubs else 0,
                    },
                    "best": {
                        "topic": args.topic_best,
                        "rate_per_sec": args.rate_best,
                        "sent_messages": int(pubs["best"].sent) if "best" in pubs else 0,
                    },
                },
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
