#!/usr/bin/env python3
"""Summarize W2/W4 benchmark evidence for GitHub Actions."""

import argparse
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    args = parser.parse_args()
    lines = [
        "# W2/W4 benchmark",
        "",
        "Both cases use a nop sink and a one-second tumbling window. CPU rounds are",
        "unprofiled; the profile round has sampling overhead. Counts do not prove",
        "aggregate values because the sink discards output.",
        "",
        "| Case | Round | Sent | Effective msg/s | Source in | Processor in | Processor out | CPU cores | Complete |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    missing = []
    for case in ("w2", "w4"):
        for mode, reps in (("cpu", (1, 2)), ("profile", (1,))):
            for rep in reps:
                path = args.input / f"{case}-{mode}-{rep}.json"
                if not path.is_file():
                    missing.append(str(path))
                    continue
                data = json.loads(path.read_text())
                publisher = data.get("publisher", {})
                lines.append(
                    f"| {case.upper()} | {mode} {rep} | {publisher.get('sent', 0)} "
                    f"| {publisher.get('rate', 0):.0f} | {data.get('source_received', 0):.0f} "
                    f"| {data.get('processor_input', 0):.0f} "
                    f"| {data.get('processor_output', 0):.0f} "
                    f"| {data.get('cpu_mean_cores', 0):.2f} "
                    f"| {'yes' if data.get('complete') else 'no'} |"
                )
                if data.get("errors"):
                    lines.append(f"\n{case.upper()} {mode} {rep}: `{data['errors']}`\n")
    if missing:
        lines.extend(("", "Missing results: " + ", ".join(missing)))
    lines.extend(("", "Download the workflow artifact for raw OpenMetrics samples, JSON, logs, and pprof files."))
    args.summary.write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    return 1 if missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
