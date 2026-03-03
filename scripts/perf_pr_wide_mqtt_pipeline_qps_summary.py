#!/usr/bin/env python3

"""
Append per-pipeline records-out QPS to GitHub Actions step summary.

Reads:
  - result.json: publish window + pipeline ids
  - metrics.openmetrics: counter samples with timestamps

Computes average QPS over the publish window for:
  processor_records_out_total{kind="result_collect", pipeline_id="...", instance="..."}

Stdlib only.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple


_SAMPLE_RE = re.compile(
    r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([-+]?(?:\d+\.?\d*|\d*\.?\d+)(?:[eE][-+]?\d+)?)(?:\s+(\d+))?$"
)
_LABEL_KV_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="([^"\\\\]*(?:\\\\.[^"\\\\]*)*)"')


def _read_json(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _parse_labels(raw: str) -> Dict[str, str]:
    if not raw:
        return {}
    s = raw.strip()
    if s.startswith("{") and s.endswith("}"):
        s = s[1:-1]
    out: Dict[str, str] = {}
    for k, v in _LABEL_KV_RE.findall(s):
        out[k] = bytes(v, "utf-8").decode("unicode_escape")
    return out


@dataclass
class CounterWindow:
    first_ts_ms: int
    first_val: float
    last_ts_ms: int
    last_val: float

    def update(self, ts_ms: int, val: float) -> None:
        if ts_ms < self.first_ts_ms:
            self.first_ts_ms = ts_ms
            self.first_val = val
        if ts_ms > self.last_ts_ms:
            self.last_ts_ms = ts_ms
            self.last_val = val


def _iter_samples(openmetrics_path: str) -> Iterable[Tuple[str, Dict[str, str], float, int]]:
    with open(openmetrics_path, "r", encoding="utf-8", errors="replace") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            m = _SAMPLE_RE.match(line)
            if not m:
                continue
            name = m.group(1)
            labels = _parse_labels(m.group(2) or "")
            try:
                val = float(m.group(3))
            except ValueError:
                continue
            ts_ms_s = m.group(4)
            if not ts_ms_s:
                continue
            try:
                ts_ms = int(ts_ms_s)
            except ValueError:
                continue
            yield name, labels, val, ts_ms


def build_markdown(
    phase: str,
    result_json_path: str,
    openmetrics_path: str,
    duration_secs: float,
    rows: List[Tuple[str, str, int, float]],
) -> str:
    lines: List[str] = []
    lines.append(f"## Phase {phase} - pipeline records out QPS")
    lines.append("")
    lines.append(f"- result.json: `{result_json_path}`")
    lines.append(f"- openmetrics: `{openmetrics_path}`")
    lines.append(f"- publish_duration_secs: {duration_secs:.3f}")
    lines.append("")
    lines.append("| flow_instance | pipeline_id | records_out Δ | avg qps |")
    lines.append("|---|---|---:|---:|")
    for inst, pipeline_id, delta, qps in rows:
        lines.append(f"| `{inst}` | `{pipeline_id}` | {delta} | {qps:.2f} |")
    lines.append("")
    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Append per-pipeline records-out QPS to step summary.")
    p.add_argument("--phase", required=True)
    p.add_argument(
        "--summary-path",
        default=os.environ.get("GITHUB_STEP_SUMMARY", ""),
        help="Path to GitHub Actions step summary markdown file.",
    )
    p.add_argument("--result-json", required=True)
    p.add_argument("--openmetrics", required=True)
    return p


def main(argv: List[str]) -> int:
    args = build_parser().parse_args(argv)
    result = _read_json(args.result_json)

    duration_secs = float(result.get("publish_duration_secs") or 0.0)
    if duration_secs <= 0:
        raise SystemExit(f"invalid publish_duration_secs in {args.result_json}: {duration_secs}")

    cfg = result.get("config", {}) or {}
    pipeline_ids = cfg.get("pipeline_ids")
    pipeline_id_set: Optional[set] = set(pipeline_ids) if isinstance(pipeline_ids, list) else None

    windows: Dict[Tuple[str, str], CounterWindow] = {}
    for name, labels, val, ts_ms in _iter_samples(args.openmetrics):
        if name != "processor_records_out_total":
            continue
        if labels.get("kind") != "result_collect":
            continue
        inst = labels.get("instance") or labels.get("flow_instance") or ""
        pipeline_id = labels.get("pipeline_id") or ""
        if not inst or not pipeline_id:
            continue
        if pipeline_id_set is not None and pipeline_id not in pipeline_id_set:
            continue
        key = (inst, pipeline_id)
        w = windows.get(key)
        if w is None:
            windows[key] = CounterWindow(first_ts_ms=ts_ms, first_val=val, last_ts_ms=ts_ms, last_val=val)
        else:
            w.update(ts_ms, val)

    rows: List[Tuple[str, str, int, float]] = []
    for (inst, pipeline_id), w in sorted(windows.items()):
        delta = w.last_val - w.first_val
        if delta < 0:
            continue
        qps = float(delta) / duration_secs
        rows.append((inst, pipeline_id, int(delta), qps))

    if not rows:
        rows = [("<none>", "<none>", 0, 0.0)]

    md = build_markdown(
        phase=args.phase,
        result_json_path=args.result_json,
        openmetrics_path=args.openmetrics,
        duration_secs=duration_secs,
        rows=rows,
    )

    if args.summary_path:
        os.makedirs(os.path.dirname(args.summary_path) or ".", exist_ok=True)
        with open(args.summary_path, "a", encoding="utf-8") as f:
            f.write(md)
    else:
        sys.stdout.write(md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

