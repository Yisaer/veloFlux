#!/usr/bin/env python3
"""Check ELF64 LOAD and RELRO layout for Android's 16 KB pages."""

import argparse
import struct
from collections import namedtuple


PAGE_SIZE = 16384
PT_LOAD = 1
PT_GNU_RELRO = 0x6474E552
PF_W = 2
ProgramHeader = namedtuple(
    "ProgramHeader", "kind flags offset vaddr paddr filesz memsz align"
)


def check(path):
    with open(path, "rb") as elf:
        header = struct.unpack("<16sHHIQQQIHHHHHH", elf.read(64))
        if header[0][:6] != b"\x7fELF\x02\x01":
            raise ValueError("expected a little-endian ELF64 file")
        phoff, phentsize, phnum = header[5], header[9], header[10]
        if phentsize != 56 or phnum == 0 or phnum == 0xFFFF:
            raise ValueError("unsupported or missing program header table")
        elf.seek(phoff)
        segments = [
            ProgramHeader(*struct.unpack("<IIQQQQQQ", elf.read(phentsize)))
            for _ in range(phnum)
        ]

    loads = [segment for segment in segments if segment.kind == PT_LOAD]
    if not loads:
        raise ValueError("no LOAD segments")
    for segment in loads:
        if segment.align < PAGE_SIZE or segment.align & (segment.align - 1):
            raise ValueError(f"LOAD at {segment.vaddr:#x}: invalid alignment {segment.align:#x}")
        if (segment.vaddr - segment.offset) % PAGE_SIZE:
            raise ValueError(f"LOAD at {segment.vaddr:#x}: offset is not page-congruent")

    for segment in segments:
        if segment.kind != PT_GNU_RELRO or not segment.memsz:
            continue
        start, end = segment.vaddr, segment.vaddr + segment.memsz
        page_start = start // PAGE_SIZE * PAGE_SIZE
        page_end = (end + PAGE_SIZE - 1) // PAGE_SIZE * PAGE_SIZE
        # An isolated RELRO LOAD may end mid-page if the remaining page
        # contains no writable data. Check both rounded boundaries.
        for load in loads:
            if not load.flags & PF_W:
                continue
            load_end = load.vaddr + load.memsz
            for lower, upper in ((page_start, start), (end, page_end)):
                if max(lower, load.vaddr) < min(upper, load_end):
                    raise ValueError(
                        f"RELRO at {start:#x}: 16 KB page protection overlaps writable data"
                    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary")
    args = parser.parse_args()
    try:
        check(args.binary)
    except (OSError, ValueError, struct.error) as error:
        parser.exit(1, f"{args.binary}: {error}\n")
    print(f"{args.binary}: LOAD and RELRO layout supports 16 KB pages")


if __name__ == "__main__":
    main()
