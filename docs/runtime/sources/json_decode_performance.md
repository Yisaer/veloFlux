# JSON Decode Cost Comparison

This local microbenchmark compares the retained DOM reference, streaming decode that prepares its
projection for every payload, and streaming decode bound to one prepared projection. It uses the
current execution-region baseline from main at `6de5e211`, with the JSON changes in this stack.
It measures elapsed decode time, including output row allocation and destruction. It does not
measure end-to-end MQTT CPU, network delivery, or allocation counts.

## Reproduction

```sh
cargo test --release -p flow --lib json_decode_cost_comparison -- --ignored --nocapture
```

The ignored test checks output equivalence before timing. Each case decodes 2,048 rows per sample,
rotates the order of the three modes, and reports the median of five samples. Schemas contain
integer columns; inputs contain one object or an array of 32 objects. Projections emit either all
columns or three columns. No performance assertion is used in CI.

## Local Results

Recorded on 2026-10-20, x86_64 Linux, Intel Core i9-14900HX, rustc 1.96.0, release profile.
Times are nanoseconds per payload; batch rows are included in each payload.

| Schema columns | Output columns | Rows | DOM reference | Per-call plan | Prepared |
| --- | --- | --- | --- | --- | --- |
| 8 | 8 | 1 | 914 | 744 | 441 |
| 8 | 8 | 32 | 30178 | 15998 | 15698 |
| 8 | 3 | 1 | 819 | 591 | 402 |
| 8 | 3 | 32 | 24574 | 14227 | 13834 |
| 64 | 64 | 1 | 9458 | 6080 | 3036 |
| 64 | 64 | 32 | 321039 | 103608 | 102302 |
| 64 | 3 | 1 | 6443 | 3654 | 2673 |
| 64 | 3 | 32 | 200545 | 78105 | 78558 |
| 256 | 256 | 1 | 55941 | 23706 | 11868 |
| 256 | 256 | 32 | 1661157 | 391406 | 378324 |
| 256 | 3 | 1 | 33237 | 12129 | 9049 |
| 256 | 3 | 32 | 963012 | 291735 | 297256 |

Preparation materially reduces single-row decode cost. For 32-row payloads the plan cost is
amortized and the two streaming modes are close, with small timing variation in either direction.
The DOM reference shares the new scalar conversion functions; it is not a separately built
historical binary. These results do not replace end-to-end measurements or justify adding the
pre-fusion CPU deltas reported in the original PR.
