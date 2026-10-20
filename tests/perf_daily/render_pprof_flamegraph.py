#!/usr/bin/env python3
"""Render a pprof CPU protobuf as an SVG flamegraph and a summary-safe text preview.

Uses only the Python standard library. A profile with samples but no symbolized
locations is rejected instead of publishing an empty, misleading graph.
Adapted from rekuiper's W2/W4 benchmark renderer (MIT/Apache-2.0).
"""

import argparse
import gzip
import hashlib
import html
from pathlib import Path


def varint(data, offset):
    value = 0
    shift = 0
    while offset < len(data):
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7f) << shift
        if byte < 0x80:
            return value, offset
        shift += 7
        if shift >= 70:
            break
    raise ValueError("invalid protobuf varint")


def fields(data):
    offset = 0
    while offset < len(data):
        tag, offset = varint(data, offset)
        number, wire = tag >> 3, tag & 7
        if wire == 0:
            value, offset = varint(data, offset)
        elif wire == 2:
            length, offset = varint(data, offset)
            value = data[offset:offset + length]
            offset += length
            if len(value) != length:
                raise ValueError("truncated protobuf field")
        elif wire == 1:
            value, offset = data[offset:offset + 8], offset + 8
        elif wire == 5:
            value, offset = data[offset:offset + 4], offset + 4
        else:
            raise ValueError(f"unsupported protobuf wire type {wire}")
        yield number, value


def repeated_int(value):
    if isinstance(value, int):
        return [value]
    result = []
    offset = 0
    while offset < len(value):
        number, offset = varint(value, offset)
        result.append(number)
    return result


def int_field(data, number, default=0):
    return next((value for key, value in fields(data) if key == number), default)


def profile_stacks(data):
    if data[:2] == b"\x1f\x8b":
        data = gzip.decompress(data)
    entries = list(fields(data))
    strings = [value.decode("utf-8", "replace") for key, value in entries if key == 6]
    functions = {}
    for key, value in entries:
        if key == 5:
            function_id = int_field(value, 1)
            name_id = int_field(value, 2)
            functions[function_id] = strings[name_id] if name_id < len(strings) else "[unknown]"
    locations = {}
    for key, value in entries:
        if key == 4:
            location_id = int_field(value, 1)
            lines = [item for field, item in fields(value) if field == 4]
            locations[location_id] = [functions.get(int_field(line, 1), "[unknown]") for line in lines]
    stacks = []
    missing = 0
    for key, value in entries:
        if key != 2:
            continue
        sample_fields = list(fields(value))
        ids = [item for field, raw in sample_fields if field == 1 for item in repeated_int(raw)]
        weights = [item for field, raw in sample_fields if field == 2 for item in repeated_int(raw)]
        if not ids:
            missing += 1
            continue
        names = [name for location_id in reversed(ids)
                 for name in reversed(locations.get(location_id, []))]
        if names and weights:
            stacks.append((names, weights[-1]))
        else:
            missing += 1
    if not stacks:
        raise ValueError(f"profile contains no symbolized stack samples ({missing} empty samples)")
    return stacks, missing


def tree(stacks):
    root = {"weight": 0, "children": {}}
    for names, weight in stacks:
        if weight <= 0:
            continue
        node = root
        node["weight"] += weight
        for name in names:
            node = node["children"].setdefault(name, {"weight": 0, "children": {}})
            node["weight"] += weight
    if not root["weight"]:
        raise ValueError("profile has no positive CPU samples")
    return root


def children(node):
    return sorted(node["children"].items(), key=lambda item: (-item[1]["weight"], item[0]))


def text_preview(root, missing):
    total = root["weight"]
    result = [f"Sampled CPU: {total / 1e6:.1f} ms; empty samples: {missing}"]

    def visit(node, level):
        if level >= 4 or len(result) >= 32:
            return
        for name, child in children(node)[:5]:
            if len(result) >= 32:
                break
            fraction = child["weight"] / total
            bar = "█" * max(1, round(32 * fraction))
            result.append(f"{'  ' * level}{bar:<32} {fraction * 100:5.1f}% {name[:95]}")
            visit(child, level + 1)

    visit(root, 0)
    return "\n".join(result) + "\n"


def svg_flamegraph(root):
    total = root["weight"]
    width, margin, row = 1200, 12, 22
    plot_width = width - margin * 2

    def depth(node):
        return 1 + max((depth(child) for child in node["children"].values()), default=0)

    max_depth = min(depth(root), 48)
    height = 65 + row * max_depth
    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
             f'width="{width}" height="{height}">',
             '<style>text{font:12px sans-serif;fill:#222}rect{stroke:#fff;stroke-width:.6}</style>',
             '<rect width="100%" height="100%" fill="white"/>',
             f'<text x="{margin}" y="22">CPU flamegraph · sampled {total / 1e6:.1f} ms '
             '(root at bottom; width = cumulative CPU)</text>']

    def draw(node, start, level):
        if level >= max_depth:
            return
        x = start
        for name, child in children(node):
            span = plot_width * child["weight"] / total
            if span < 0.6:
                x += span
                continue
            y = height - 25 - row * (level + 1)
            digest = hashlib.sha1(name.encode()).digest()
            color = f"hsl({8 + digest[0] % 48},80%,{62 + digest[1] % 16}%)"
            percent = child["weight"] / total * 100
            parts.append(f'<g><title>{html.escape(name)} · {percent:.1f}%</title>'
                         f'<rect x="{margin + x:.2f}" y="{y}" width="{span:.2f}" '
                         f'height="{row - 2}" fill="{color}"/>')
            if span > 65:
                capacity = max(1, int((span - 9) / 7))
                label = name if len(name) <= capacity else name[:capacity - 1] + "…"
                parts.append(f'<text x="{margin + x + 4:.2f}" y="{y + 14}">'
                             f'{html.escape(label)}</text>')
            parts.append('</g>')
            draw(child, x, level + 1)
            x += span

    draw(root, 0, 0)
    parts.append('</svg>')
    return "\n".join(parts) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", type=Path)
    parser.add_argument("--svg", type=Path, required=True)
    parser.add_argument("--text", type=Path, required=True)
    args = parser.parse_args()
    stacks, missing = profile_stacks(args.profile.read_bytes())
    root = tree(stacks)
    args.svg.write_text(svg_flamegraph(root), encoding="utf-8")
    args.text.write_text(text_preview(root, missing), encoding="utf-8")


if __name__ == "__main__":
    main()
