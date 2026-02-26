#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class Estimate:
    benchmark: str
    mean_ns: float
    mean_ci_lower_ns: float | None
    mean_ci_upper_ns: float | None
    std_dev_ns: float | None


def _format_ns(ns: float) -> str:
    if ns < 1_000:
        return f"{ns:.2f} ns"
    if ns < 1_000_000:
        return f"{ns / 1_000:.2f} µs"
    if ns < 1_000_000_000:
        return f"{ns / 1_000_000:.2f} ms"
    return f"{ns / 1_000_000_000:.2f} s"


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _extract_estimate(benchmark: str, payload: dict[str, Any]) -> Estimate:
    mean = payload.get("mean") or {}
    std_dev = payload.get("std_dev") or {}
    ci = mean.get("confidence_interval") or {}
    return Estimate(
        benchmark=benchmark,
        mean_ns=float(mean.get("point_estimate", 0.0)),
        mean_ci_lower_ns=float(ci["lower_bound"]) if "lower_bound" in ci else None,
        mean_ci_upper_ns=float(ci["upper_bound"]) if "upper_bound" in ci else None,
        std_dev_ns=float(std_dev["point_estimate"]) if "point_estimate" in std_dev else None,
    )


def _discover_estimates(criterion_dir: Path) -> list[Estimate]:
    estimates: list[Estimate] = []
    for estimates_path in criterion_dir.glob("**/new/estimates.json"):
        try:
            rel = estimates_path.relative_to(criterion_dir)
        except ValueError:
            continue

        # rel: <bench path>/new/estimates.json
        benchmark = str(rel.parent.parent).replace("\\", "/")
        payload = _read_json(estimates_path)
        estimates.append(_extract_estimate(benchmark, payload))

    estimates.sort(key=lambda e: e.benchmark)
    return estimates


def _render_markdown(estimates: Iterable[Estimate]) -> str:
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")

    rows: list[str] = []
    rows.append("# Criterion Bench Summary\n")
    rows.append(f"Generated at: `{generated_at}`\n")
    rows.append("| Benchmark | Mean | 95% CI | Std Dev |")
    rows.append("|---|---:|---:|---:|")

    count = 0
    for e in estimates:
        count += 1
        mean = _format_ns(e.mean_ns)
        if e.mean_ci_lower_ns is not None and e.mean_ci_upper_ns is not None:
            ci = f"{_format_ns(e.mean_ci_lower_ns)} – {_format_ns(e.mean_ci_upper_ns)}"
        else:
            ci = "-"
        std_dev = _format_ns(e.std_dev_ns) if e.std_dev_ns is not None else "-"
        rows.append(f"| `{e.benchmark}` | {mean} | {ci} | {std_dev} |")

    if count == 0:
        rows.append("\nNo Criterion results found under `target/criterion/`.\n")

    rows.append("")
    return "\n".join(rows)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Render a markdown summary from Criterion estimates.json files."
    )
    parser.add_argument(
        "--criterion-dir",
        default="target/criterion",
        help="Criterion output directory (default: target/criterion).",
    )
    parser.add_argument(
        "--output",
        default="bench-summary.md",
        help="Output markdown path (default: bench-summary.md).",
    )
    args = parser.parse_args()

    criterion_dir = Path(args.criterion_dir)
    output_path = Path(args.output)

    estimates = _discover_estimates(criterion_dir) if criterion_dir.exists() else []
    output_path.write_text(_render_markdown(estimates), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

