# ARXML Decode Performance Validation

The standalone `decode_profile` example measures repeated converter decoding of
BAQ service `0xAB04`, event `0x8003`. It uses the existing parking frame from
`tests/e2e/someip_explain.rs` and extracts the declared 840-byte GBF payload.
The frame literal contains additional trailing bytes, which are excluded using
the declared payload length.

Run from the repository root:

```sh
python3 - <<'PYTHON'
import re
from pathlib import Path
source = Path("tests/e2e/someip_explain.rs").read_text()
source = source.split("const SIP_HEX: &str = concat!(", 1)[1].split(");", 1)[0]
frame = bytes.fromhex("".join(re.findall(r'"([0-9a-fA-F]+)"', source)))
size = int.from_bytes(frame[21:25], "big")
assert size == 840 and len(frame) >= 25 + size
Path("/tmp/veloflux-parking-payload.bin").write_bytes(frame[25:25 + size])
PYTHON
cargo run -p arxml_converter --release --example decode_profile -- \
  distros/sdv/arxml_converter/tests/test_data/baq.arxml \
  /tmp/veloflux-parking-payload.bin 2000
```

The example loads the codec before measurement and warms up 64 decodes. It counts
allocation/reallocation calls for one decode, disables counting for timing, and
prints five samples of 2,000 decodes. The allocator delegates to `System` in every
version. Output construction and destruction are timed; ARXML parsing, plan
compilation, I/O, output formatting, and checksum calculation are excluded.
The example is tied to this BAQ message, not a general message-selection CLI.

For a historical comparison, build this same example against each converter
revision in isolated temporary directories with separate target directories.
Use the same lockfile, Rust toolchain, release profile, ARXML, and payload. Stop
other builds before running the binaries sequentially. This is a standalone
example, not a `cargo bench` target or a CI timing gate.

## Local Comparison

Local release runs on macOS x86_64, after other builds and tests finished,
produced the following results. Timings are medians of five sequential samples;
each sample decodes the same payload 2,000 times.

| Converter revision | Allocation/reallocation calls per decode | Median, 2,000 decodes | Sample range |
| --- | ---: | ---: | ---: |
| Main, `74f4174` | 2,896 | 859.841 ms | 853.789–965.862 ms |
| PR before these fixes, `836c6366` | 4,515 | 1,743.021 ms | 1,627.914–1,774.294 ms |
| Compiled decode plans | 528 | 152.875 ms | 151.664–169.579 ms |

All three produced output fingerprint `6d76d787c157b7c5` (FNV-1a over the debug
representation of the decoded value). The fingerprint is a diagnostic comparison,
not the sole correctness oracle; IO regressions assert concrete values and byte
counts, and existing SOME/IP E2E tests verify pipeline output.

These results concern converter-only repeated decoding of one fixed payload.
They do not measure pipeline throughput, codec load time, retained plan memory,
or all ARXML layouts. Plans move resolution work to loading and retain nodes per
message. References share compiled nodes within a message; separate messages
currently retain separate plans. Public stateless `Decoder::decode` recompiles
from the supplied registry; repeated direct callers should use `Decoder::compile`
and reuse the returned immutable `DecodePlan`.
