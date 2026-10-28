#!/usr/bin/env python3
"""Reject Linux release ELFs that exceed the supported userspace ABI baseline."""
import re
import subprocess
import sys

LIMITS = {"GLIBC": (2, 28), "GLIBCXX": (3, 4, 25), "CXXABI": (1, 3, 11)}


def check(path):
    info = subprocess.check_output(
        ["readelf", "--version-info", "--wide", path], universal_newlines=True
    )
    # Version needs, rather than exported definitions, determine runtime requirements.
    needs = info.split("Version needs section", 1)
    if len(needs) != 2:
        raise ValueError("expected a dynamically linked ELF with version requirements")
    versions = {}
    for name in re.findall(r"\bName: ((?:GLIBC|GLIBCXX|CXXABI)_[^\s]+)", needs[1]):
        family, version = name.split("_", 1)
        if not re.fullmatch(r"[0-9]+(?:\.[0-9]+)*", version):
            raise ValueError("unsupported private or non-numeric ABI: " + name)
        value = tuple(int(part) for part in version.split("."))
        versions[family] = max(versions.get(family, ()), value)
    if "GLIBC" not in versions:
        raise ValueError("missing GLIBC requirements")
    for family, value in versions.items():
        if value > LIMITS[family]:
            raise ValueError("{} requires {}_{}, baseline is {}".format(
                path, family, ".".join(map(str, value)), ".".join(map(str, LIMITS[family]))))
    print("{}: {}".format(path, versions), flush=True)
    subprocess.check_call(["readelf", "--dynamic", "--wide", path])


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("usage: check_linux_abi.py ELF [ELF ...]")
    try:
        for artifact in sys.argv[1:]:
            check(artifact)
    except (ValueError, subprocess.CalledProcessError) as error:
        sys.exit(str(error))
