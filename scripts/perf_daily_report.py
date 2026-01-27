#!/usr/bin/env python3

"""
Render perf_daily OpenMetrics dump into:
1) GitHub Actions Job Summary (ASCII "charts")
2) A standalone HTML report (SVG charts)

No third-party deps; stdlib only.
"""

from __future__ import annotations

import argparse
import datetime as dt
import html
import json
import os
import re
from typing import Dict, List, Optional, Tuple


Sample = Tuple[int, float]  # (ts_ms, value)

TRACKED_METRICS = (
    "cpu_usage",
    "memory_usage_bytes",
    "heap_in_use_bytes",
    "heap_in_allocator_bytes",
)

_SAMPLE_RE = re.compile(
    r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([-+]?(?:\d+\.?\d*|\d*\.?\d+)(?:[eE][-+]?\d+)?)(?:\s+(\d+))?$"
)


def _read_json(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _find_openmetrics_path(result_json_path: str) -> Optional[str]:
    data = _read_json(result_json_path)
    metrics = data.get("metrics", {})
    p = metrics.get("openmetrics_path")
    if not p:
        return None
    # If path is relative, interpret it relative to repo root (same as execution cwd).
    return str(p)


def parse_openmetrics(path: str, instance: Optional[str]) -> Dict[str, List[Sample]]:
    series: Dict[str, List[Sample]] = {k: [] for k in TRACKED_METRICS}
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            m = _SAMPLE_RE.match(line)
            if not m:
                continue
            name = m.group(1)
            if name not in series:
                continue
            labels = m.group(2) or ""
            if instance is not None and f'instance="{instance}"' not in labels:
                continue
            try:
                val = float(m.group(3))
            except ValueError:
                continue
            ts_ms = m.group(4)
            if ts_ms is None:
                # perf_daily should always include timestamps, but tolerate missing ones.
                continue
            try:
                ts = int(ts_ms)
            except ValueError:
                continue
            series[name].append((ts, val))

    # Sort + de-dup by timestamp (keep last value for same ts).
    for k, items in series.items():
        items.sort(key=lambda x: x[0])
        dedup: List[Sample] = []
        for ts, v in items:
            if dedup and dedup[-1][0] == ts:
                dedup[-1] = (ts, v)
            else:
                dedup.append((ts, v))
        series[k] = dedup
    return series


def _to_mib(v: float) -> float:
    return v / (1024.0 * 1024.0)


def _format_ts_ms(ts_ms: int) -> str:
    return dt.datetime.fromtimestamp(ts_ms / 1000.0).isoformat(timespec="seconds")


def _downsample(values: List[float], width: int) -> List[float]:
    if width <= 0 or len(values) <= width:
        return values
    out: List[float] = []
    n = len(values)
    for i in range(width):
        idx = int(i * n / width)
        out.append(values[idx])
    return out


def sparkline(values: List[float], width: int = 60) -> str:
    chars = " .:-=+*#%@"
    if not values:
        return "(no data)"
    vs = _downsample(values, width=width)
    vmin = min(vs)
    vmax = max(vs)
    if vmax <= vmin:
        mid = len(chars) // 2
        return chars[mid] * len(vs)
    out = []
    for v in vs:
        t = (v - vmin) / (vmax - vmin)
        idx = int(round(t * (len(chars) - 1)))
        idx = max(0, min(len(chars) - 1, idx))
        out.append(chars[idx])
    return "".join(out)


def _metric_unit(name: str) -> str:
    if name == "cpu_usage":
        return "pct"
    return "MiB"


def _metric_values_for_display(name: str, items: List[Sample]) -> Tuple[List[float], Optional[float], Optional[float], Optional[float]]:
    vals = [v for _, v in items]
    if name != "cpu_usage":
        vals = [_to_mib(v) for v in vals]
    if not vals:
        return vals, None, None, None
    return vals, min(vals), max(vals), vals[-1]


def build_summary_markdown(
    series: Dict[str, List[Sample]],
    openmetrics_path: str,
    result_json_path: Optional[str],
    spark_width: int,
) -> str:
    lines: List[str] = []
    lines.append("## perf_daily")
    if result_json_path:
        lines.append(f"- result.json: `{result_json_path}`")
    lines.append(f"- openmetrics: `{openmetrics_path}`")
    lines.append("")
    lines.append("### Metrics (ASCII)")
    lines.append("```")
    for name in TRACKED_METRICS:
        vals, vmin, vmax, vlast = _metric_values_for_display(name, series.get(name, []))
        unit = _metric_unit(name)
        sp = sparkline(vals, width=spark_width)
        lines.append(f"{name} [{unit}]")
        if vlast is None:
            lines.append(sp)
        else:
            lines.append(f"{sp}  last={vlast:.3f} min={vmin:.3f} max={vmax:.3f} (n={len(vals)})")
        lines.append("")
    lines.append("```")
    return "\n".join(lines) + "\n"


def _svg_polyline_points(xs: List[float], ys: List[float]) -> str:
    return " ".join(f"{x:.2f},{y:.2f}" for x, y in zip(xs, ys))


def svg_line_chart(
    title: str,
    unit: str,
    items: List[Sample],
    width: int = 900,
    height: int = 220,
    padding: int = 30,
) -> str:
    if not items:
        return f"<div class='chart'><h3>{html.escape(title)}</h3><pre>(no data)</pre></div>"

    ts0 = items[0][0]
    xs_raw = [(ts - ts0) / 1000.0 for ts, _ in items]
    ys_raw = [v for _, v in items]
    if unit == "MiB":
        ys_raw = [_to_mib(v) for v in ys_raw]

    xmin, xmax = min(xs_raw), max(xs_raw)
    ymin, ymax = min(ys_raw), max(ys_raw)
    if xmax <= xmin:
        xmax = xmin + 1.0
    if ymax <= ymin:
        ymax = ymin + 1.0

    w = width - padding * 2
    h = height - padding * 2
    xs = [padding + (x - xmin) / (xmax - xmin) * w for x in xs_raw]
    ys = [padding + (1.0 - (y - ymin) / (ymax - ymin)) * h for y in ys_raw]

    points = _svg_polyline_points(xs, ys)
    start_iso = _format_ts_ms(items[0][0])
    end_iso = _format_ts_ms(items[-1][0])

    return f"""
<div class="chart">
  <h3>{html.escape(title)} <span class="meta">({html.escape(unit)})</span></h3>
  <div class="meta">n={len(items)} range_x={xs_raw[-1]:.1f}s range_y=[{ymin:.3f},{ymax:.3f}] start={html.escape(start_iso)} end={html.escape(end_iso)}</div>
  <svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">
    <rect x="0" y="0" width="{width}" height="{height}" fill="white" stroke="#ddd"/>
    <polyline fill="none" stroke="#2f6fdb" stroke-width="2" points="{points}"/>
  </svg>
</div>
""".strip()


def build_html_report(series: Dict[str, List[Sample]], openmetrics_path: str, title: str) -> str:
    charts: List[str] = []
    charts.append(svg_line_chart("cpu_usage", "pct", series.get("cpu_usage", [])))
    charts.append(svg_line_chart("memory_usage_bytes", "MiB", series.get("memory_usage_bytes", [])))
    charts.append(svg_line_chart("heap_in_use_bytes", "MiB", series.get("heap_in_use_bytes", [])))
    charts.append(
        svg_line_chart(
            "heap_in_allocator_bytes",
            "MiB",
            series.get("heap_in_allocator_bytes", []),
        )
    )

    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{html.escape(title)}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Arial, sans-serif; margin: 20px; }}
    code, pre {{ background: #f6f8fa; padding: 2px 4px; border-radius: 4px; }}
    .meta {{ color: #555; font-size: 12px; }}
    .chart {{ margin: 18px 0; }}
    h1 {{ margin: 0 0 6px 0; }}
    h3 {{ margin: 0 0 6px 0; }}
  </style>
</head>
<body>
  <h1>{html.escape(title)}</h1>
  <div class="meta">openmetrics: <code>{html.escape(openmetrics_path)}</code></div>
  {''.join(charts)}
</body>
</html>
"""


def main(argv: List[str]) -> int:
    p = argparse.ArgumentParser(description="Render perf_daily OpenMetrics into summary + html.")
    p.add_argument("--openmetrics", help="Path to metrics.openmetrics")
    p.add_argument("--result-json", help="Path to result.json (used to locate openmetrics if --openmetrics is omitted)")
    p.add_argument("--instance", default="local", help="Filter instance label (default: local)")
    p.add_argument("--spark-width", type=int, default=60)
    p.add_argument("--out-html", help="Write HTML report to this path")
    p.add_argument("--summary-path", help="Append markdown to this path (e.g. $GITHUB_STEP_SUMMARY)")
    p.add_argument("--title", default="perf_daily report")
    args = p.parse_args(argv)

    openmetrics_path = args.openmetrics
    if not openmetrics_path and args.result_json:
        openmetrics_path = _find_openmetrics_path(args.result_json)
    if not openmetrics_path:
        raise SystemExit("error: missing --openmetrics (or --result-json with openmetrics_path)")

    series = parse_openmetrics(openmetrics_path, instance=args.instance)

    if args.out_html:
        os.makedirs(os.path.dirname(args.out_html) or ".", exist_ok=True)
        html_doc = build_html_report(series, openmetrics_path=openmetrics_path, title=args.title)
        with open(args.out_html, "w", encoding="utf-8") as f:
            f.write(html_doc)

    if args.summary_path:
        md = build_summary_markdown(
            series,
            openmetrics_path=openmetrics_path,
            result_json_path=args.result_json,
            spark_width=args.spark_width,
        )
        with open(args.summary_path, "a", encoding="utf-8") as f:
            f.write(md)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(os.sys.argv[1:]))

