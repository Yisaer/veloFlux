#!/usr/bin/env python3

import sys
from pathlib import Path

if __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from agents.nl2pipeline.cli.langgraph import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

