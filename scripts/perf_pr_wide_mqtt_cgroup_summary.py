#!/usr/bin/env python3

"""
Append cgroup v2 cpu.stat deltas to GitHub Actions step summary.

Stdlib only.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class CgroupRow:
    name: str
    path: str
    usage_usec0: int
    usage_usec1: int


def _cpu_pct(usage_usec0: int, usage_usec1: int, t0_ns: int, t1_ns: int) -> float:
    du = usage_usec1 - usage_usec0
    dt_us = max(1, (t1_ns - t0_ns) // 1000)
    return (du / dt_us) * 100.0


def _read_int(name: str, v: Optional[str]) -> Optional[int]:
    if v is None:
        return None
    v = v.strip()
    if not v:
        return None
    try:
        return int(v)
    except ValueError as e:
        raise SystemExit(f"invalid int for {name}: {v}") from e


def build_markdown(
    phase: str,
    t0_ns: int,
    t1_ns: int,
    rows: List[CgroupRow],
    critical_throttled_usec0: Optional[int],
    critical_throttled_usec1: Optional[int],
    critical_nr_throttled0: Optional[int],
    critical_nr_throttled1: Optional[int],
) -> str:
    lines: List[str] = []
    lines.append(f"## Phase {phase} - cgroup cpu.stat")
    lines.append("")
    lines.append("| cgroup | path | usage_usec (Δ) | cpu% |")
    lines.append("|---|---|---:|---:|")

    for r in rows:
        du = r.usage_usec1 - r.usage_usec0
        cpu_pct = _cpu_pct(r.usage_usec0, r.usage_usec1, t0_ns=t0_ns, t1_ns=t1_ns)
        lines.append(f"| {r.name} | `{r.path}` | {du} | {cpu_pct:.1f}% |")

    lines.append("")

    if critical_throttled_usec0 is not None and critical_throttled_usec1 is not None:
        lines.append(f"- fi_critical throttled_usec Δ: {critical_throttled_usec1 - critical_throttled_usec0}")
    if critical_nr_throttled0 is not None and critical_nr_throttled1 is not None:
        lines.append(f"- fi_critical nr_throttled Δ: {critical_nr_throttled1 - critical_nr_throttled0}")

    lines.append("")
    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Append cgroup cpu.stat usage deltas to GitHub Actions step summary."
    )
    p.add_argument("--phase", required=True)
    p.add_argument(
        "--summary-path",
        default=os.environ.get("GITHUB_STEP_SUMMARY", ""),
        help="Path to GitHub Actions step summary markdown file.",
    )
    p.add_argument("--t0-ns", type=int, required=True)
    p.add_argument("--t1-ns", type=int, required=True)
    p.add_argument(
        "--row",
        action="append",
        nargs=4,
        metavar=("NAME", "PATH", "USAGE_USEC0", "USAGE_USEC1"),
        required=True,
        help="A row to render: (name, cgroup_path, usage_usec0, usage_usec1).",
    )

    p.add_argument("--critical-throttled-usec0")
    p.add_argument("--critical-throttled-usec1")
    p.add_argument("--critical-nr-throttled0")
    p.add_argument("--critical-nr-throttled1")
    return p


def main(argv: List[str]) -> int:
    args = build_parser().parse_args(argv)

    rows: List[CgroupRow] = []
    for name, path, u0, u1 in args.row:
        rows.append(CgroupRow(name=name, path=path, usage_usec0=int(u0), usage_usec1=int(u1)))

    md = build_markdown(
        phase=args.phase,
        t0_ns=args.t0_ns,
        t1_ns=args.t1_ns,
        rows=rows,
        critical_throttled_usec0=_read_int("--critical-throttled-usec0", args.critical_throttled_usec0),
        critical_throttled_usec1=_read_int("--critical-throttled-usec1", args.critical_throttled_usec1),
        critical_nr_throttled0=_read_int("--critical-nr-throttled0", args.critical_nr_throttled0),
        critical_nr_throttled1=_read_int("--critical-nr-throttled1", args.critical_nr_throttled1),
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

