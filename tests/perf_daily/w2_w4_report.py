#!/usr/bin/env python3
"""Render VeloFlux W2/W4 process and Prometheus time series.

Adapted from rekuiper's W2/W4 benchmark report (MIT/Apache-2.0).
"""

import argparse
import html
import json
import os
import re
from pathlib import Path

from w2_w4_benchmark import LABEL, ROLE_METRICS


METRIC_RE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([-+\d.eE]+)\s+(\d+)$")
COLORS = ("#3178c6", "#d44c42", "#299d68")
ROLE_LABELS = {
    "source_in": "source records in",
    "window_in": "window records in",
    "window_out": "window records out",
    "sink_out": "sink records out",
}


def steps(evidence, mode):
    reps = (1, 2) if mode == "cpu" else (1,)
    return [json.loads((evidence / f"{case}-{mode}-{rep}.json").read_text())
            for rep in reps for case in ("w2", "w4")]


def metric_role(name, labels, pipeline):
    for role, (metric_name, processor_prefix) in ROLE_METRICS.items():
        if name != metric_name:
            continue
        if processor_prefix is None:
            return role
        if (labels.get("pipeline_id") == pipeline
                and labels.get("processor_id", "").startswith(processor_prefix)):
            return role
    return None


def metric_counters(path, pipeline):
    counters = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        match = METRIC_RE.match(line)
        if not match:
            continue
        role = metric_role(match.group(1), dict(LABEL.findall(match.group(2) or "")), pipeline)
        if role:
            counters.setdefault(role, {}).setdefault(int(match.group(4)), 0.0)
            counters[role][int(match.group(4))] += float(match.group(3))
    return {role: sorted(values.items()) for role, values in counters.items()}


def records_per_second(path, start_ms, end_ms, pipeline):
    counters = metric_counters(path, pipeline)
    rates = {}
    for role, samples in counters.items():
        points = []
        for (t0, v0), (t1, v1) in zip(samples, samples[1:]):
            if t0 >= start_ms and t1 <= end_ms and t1 > t0 and v1 >= v0:
                points.append(((t1 - start_ms) / 1000, (v1 - v0) * 1000 / (t1 - t0)))
        rates[ROLE_LABELS[role]] = points
    return rates


def source_operator_progress(path, start_ms, end_ms, pipeline):
    counters = metric_counters(path, pipeline)
    progress = {}
    for role, label in (("source_in", "source received from MQTT"),
                        ("window_in", "window input consumed")):
        samples = counters.get(role, [])
        if samples:
            baseline = samples[0][1]
            progress[label] = [((t - start_ms) / 1000, value - baseline)
                               for t, value in samples if start_ms <= t <= end_ms]
    return progress


def svg_chart(title, unit, series):
    points = [(x, y) for values in series.values() for x, y in values]
    if not points:
        return f"<p>{html.escape(title)}: no samples</p>"
    xmin, xmax = min(x for x, _ in points), max(x for x, _ in points)
    ymin, ymax = min(y for _, y in points), max(y for _, y in points)
    xmax = max(xmax, xmin + 1)
    ymax = max(ymax, ymin + 1)
    left, top, width, height = 65, 25, 820, 235
    plot_width, plot_height = width - left - 20, height - top - 35
    lines = [f'<section><h3>{html.escape(title)} ({html.escape(unit)})</h3>',
             f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(title)}">',
             f'<path d="M {left} {top} V {top + plot_height} H {left + plot_width}" fill="none" stroke="#777"/>',
             f'<text x="4" y="{top + 5}">{ymax:.1f}</text>',
             f'<text x="4" y="{top + plot_height}">{ymin:.1f}</text>',
             f'<text x="{left}" y="{height - 3}">{xmin:.0f}s</text>',
             f'<text x="{left + plot_width - 40}" y="{height - 3}">{xmax:.0f}s</text>']
    for index, (label, values) in enumerate(series.items()):
        if not values:
            continue
        coords = " ".join(
            f"{left + (x - xmin) / (xmax - xmin) * plot_width:.1f},"
            f"{top + (1 - (y - ymin) / (ymax - ymin)) * plot_height:.1f}"
            for x, y in values
        )
        color = COLORS[index % len(COLORS)]
        lines.append(f'<polyline points="{coords}" fill="none" stroke="{color}" stroke-width="2"/>')
        lines.append(f'<text x="{left + index * 210}" y="16" fill="{color}">{html.escape(label)}</text>')
    lines.append("</svg></section>")
    return "\n".join(lines)


def time_points(values, limit):
    return [(index + 1, float(value)) for index, value in enumerate(values[:limit])]


def step_charts(step, evidence):
    cpu = step.get("cpu", {})
    duration = int(step["target_duration"])
    cpu_series = {"CPU (% of one core)": time_points(cpu.get("cpu_per_sec", []), duration)}
    memory_series = {
        "RSS": time_points(cpu.get("rss_per_sec_mb", []), duration),
        "anonymous": time_points(cpu.get("anon_per_sec_mb", []), duration),
    }
    path = evidence / step["metrics_series_path"]
    rate_series = records_per_second(path, int(step["send_start_ms"]), int(step["send_end_ms"]), step["pipeline"])
    progress_series = source_operator_progress(path, int(step["send_start_ms"]), int(step["send_end_ms"]), step["pipeline"])
    return (svg_chart("Engine CPU", "%", cpu_series)
            + svg_chart("Engine memory", "MiB", memory_series)
            + svg_chart("Input and output rate", "records/s", rate_series)
            + svg_chart("Source versus window progress", "records", progress_series))


def rates_for_step(step, evidence):
    path = evidence / step["metrics_series_path"]
    return records_per_second(path, int(step["send_start_ms"]), int(step["send_end_ms"]), step["pipeline"])


def mean_rate(points):
    return f"{sum(value for _, value in points) / len(points):,.0f}" if points else "n/a"


def rate_ratio(numerator, denominator):
    if not numerator or not denominator:
        return "n/a"
    num = sum(value for _, value in numerator)
    den = sum(value for _, value in denominator)
    return f"{num / den * 100:.1f}%" if den else "n/a"


def summary_table(measured, profiled, evidence):
    rows = ["| Run | Case | Target msg/s | CPU (% core) | Sent | Source in/s | Window in/s | Window/source | Window out/s | Sink outputs | Proof |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|"]
    for name, group in (("CPU", measured), ("profile", profiled)):
        for step in group:
            rates = rates_for_step(step, evidence)
            rows.append(f"| {name} #{step['rep']} | {step['case']} | {step['target_rate']:,} | "
                        f"{step['cpu']['cpu_mean_send_window']} | {step['publisher']['sent']:,} | "
                        f"{mean_rate(rates.get('source records in', []))} | "
                        f"{mean_rate(rates.get('window records in', []))} | "
                        f"{rate_ratio(rates.get('window records in', []), rates.get('source records in', []))} | "
                        f"{mean_rate(rates.get('window records out', []))} | "
                        f"{step['sink_outputs']:,.0f} | Counters only |")
    return "\n".join(rows)


def per_second_table(step, evidence):
    rates = rates_for_step(step, evidence)
    series = [rates.get(name, []) for name in
              ("source records in", "window records in", "window records out", "sink records out")]
    if not all(series):
        return "Per-second operator metrics unavailable."
    rows = ["| Send elapsed (s) | Source records_in/s | Window records_in/s | Window records_out/s | Sink records_out/s |",
            "|---:|---:|---:|---:|---:|"]
    for samples in zip(*series):
        rows.append(f"| {samples[0][0]:.1f} | "
                    + " | ".join(f"{value:,.0f}" for _, value in samples) + " |")
    return "\n".join(rows)


def mermaid_cpu(measured):
    selected = [step for step in measured if step["rep"] == 1]
    if len(selected) != 2:
        return ""
    selected.sort(key=lambda step: step["case"])
    length = min(30, *(len(step["cpu"]["cpu_per_sec"]) for step in selected))
    if length < 2:
        return ""
    lines = ["```mermaid", "xychart-beta", f'  title "W2 then W4 engine CPU at {selected[0]["target_rate"]:,} msg/s"',
             "  x-axis \"send second\" [" + ", ".join(str(i) for i in range(1, length + 1)) + "]",
             '  y-axis "one-core CPU %" 0 --> 100']
    for step in selected:
        values = ", ".join(str(v) for v in step["cpu"]["cpu_per_sec"][:length])
        lines.append(f'  line [{values}]')
    lines.append("```")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--html", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    args = parser.parse_args()
    measured = steps(args.input, "cpu")
    profiled = steps(args.input, "profile")
    window_s = measured[0]["window_seconds"]
    table = summary_table(measured, profiled, args.input)
    chart = mermaid_cpu(measured)
    with args.summary.open("a", encoding="utf-8") as output:
        output.write(f"## W2/W4 VeloFlux CPU and profile ({window_s}s tumbling windows)\n\n"
                     + table + "\n\n")
        if window_s != 10:
            output.write("This is a window-length variant; the documented W2/W4 cases use 10-second windows.\n\n")
        output.write("CPU comparison uses only the unsampled CPU runs. The profile runs include sampling overhead.\n\n")
        output.write(f"Each workload sends for {measured[0]['target_duration']} seconds; "
                     f"each CPU profile samples {profiled[0]['profile_seconds']} seconds.\n\n")
        output.write("CPU and memory samples come from the VeloFlux process in /proc; "
                     "rekuiper reports engine cgroup samples.\n\n")
        if chart:
            output.write(chart + "\n\n")
        output.write("Source and window input should track the publisher. Window output is aggregated "
                     "per tumbling window, so it is not expected to equal input msg/s. "
                     "Both W2 and W4 use nop sinks: counters show progress but cannot verify "
                     "aggregate values or device coverage. Rates are differences between roughly "
                     "one-second Prometheus scrapes. Sink outputs count rows entering the nop sink.\n\n")
        for step in measured:
            if step["rep"] == 1:
                output.write(f"<details><summary>{step['case'].upper()} operator records_in/out per second "
                             f"({step['target_rate']:,} msg/s)</summary>\n\n"
                             + per_second_table(step, args.input) + "\n\n</details>\n\n")
        for step in profiled:
            flame = Path(step["profile_path"]).stem + ".flame.svg"
            preview = args.input / (Path(step["profile_path"]).stem + ".flame.txt")
            output.write(f"### {step['case'].upper()} CPU flamegraph\n\n")
            if preview.exists():
                output.write("```text\n" + preview.read_text(encoding="utf-8") + "\n```\n\n")
            artifact = (f"{os.environ.get('GITHUB_SERVER_URL', 'https://github.com')}/"
                        f"{os.environ.get('GITHUB_REPOSITORY', 'emqx/VeloFlux')}/actions/runs/"
                        f"{os.environ.get('GITHUB_RUN_ID', '')}#artifacts")
            output.write(f"[Full SVG flamegraph: {flame}]({artifact}) (download the artifact).\n\n")
            top_path = args.input / (Path(step["profile_path"]).stem + ".top.txt")
            if top_path.exists():
                excerpt = "\n".join(top_path.read_text(encoding="utf-8").splitlines()[:18])
                output.write(f"### {step['case']} CPU hotspots\n\n```text\n{excerpt}\n```\n\n")
        output.write("Download the workflow artifact for the HTML plots, full pprof call graphs, and raw metrics.\n")
    sections = []
    for kind, group in (("CPU", measured), ("profile", profiled)):
        for step in group:
            label = f"{kind} {step['case']} {step['target_rate']} msg/s repetition {step['rep']}"
            profile_link = ""
            if kind == "profile":
                profile = Path(step["profile_path"]).stem
                profile_link = (f'<p><a href="{html.escape(profile)}.flame.svg">CPU flamegraph</a> · '
                                f'<a href="{html.escape(profile)}.svg">CPU call graph</a> · '
                                f'<a href="{html.escape(profile)}.top.txt">Top functions</a></p>'
                                f'<img src="{html.escape(profile)}.flame.svg" alt="{html.escape(step["case"])} CPU flamegraph">')
            sections.append(f"<details><summary>{html.escape(label)}</summary>{profile_link}"
                            + step_charts(step, args.input) + "</details>")
    page = ("<!doctype html><html><head><meta charset='utf-8'><title>W2/W4 CPU report</title>"
            "<style>body{font:16px system-ui;max-width:1000px;margin:24px auto;padding:0 16px}"
            "table{border-collapse:collapse}td,th{border:1px solid #ccc;padding:6px}"
            "details{margin:12px 0;border:1px solid #ddd;padding:12px}svg{width:100%;height:auto}"
            f"section{{margin:20px 0}}</style></head><body><h1>W2/W4 CPU and profile ({window_s}s windows)</h1>"
            "<p>CPU comparisons use unsampled runs; diagnostic runs include profiling overhead.</p>"
            + "".join(sections) + "</body></html>")
    args.html.write_text(page, encoding="utf-8")


if __name__ == "__main__":
    main()
