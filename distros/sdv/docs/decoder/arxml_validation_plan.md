# ARXML Decoder Validation Plan

Validate the SDV ARXML decoder against two external libraries: `autosar-data-py`
for ARXML loading and reference inspection, and `someipy` for SOME/IP payload
serialization and deserialization. The objective is to find reproducible
differences in message selection, type resolution, binary decoding, and the
resulting veloFlux output.

The initial `autosar-data-py` input/output framework is implemented in
[`arxml_converter/tests/reference`](../../arxml_converter/tests/reference/README.md).
Its 17 cases cover repository samples, generated adaptations of an upstream
example, and a locally authored adaptation of an official structure illustration.
Both the Rust regression and the live reference comparison have been executed.
Fourteen cases match, one is rejected by both implementations, and two record
known differences. A complete type inventory now supplements those selected
cases: it enumerates every type in each reference-loadable document, compares
definitions, retained references, and declared base traits, and reports partial
coverage separately. Status follows the current computation contract: short
references for unqualified lookup and resolved mapping normalization are expected,
and retained maximum
lengths do not require runtime enforcement. These are not partial support. The
current inventory has 2,798 matching rows, no partial rows, 15 unrepresented rows,
and no Rust parser errors. Supported native builtin names provide their traits
without repeated SW base declarations. Qualified references require exact loaded
identities; neither short-name lookup nor builtin name guessing can recover a
missing path. CP application and AP definitions register their actual paths,
and application fields and array elements retain scoped references. All 16 explicit SW base definitions are
also compared independently by full-path lookup, name, runtime category, bit
width, and encoding. See the [inventory findings](arxml_type_inventory.md) and
framework documentation for counts, commands, and coverage limits.
The signed 8-bit and float64 resolver issues found by that inventory are fixed;
the arbitrary-base-name compatibility issue is also fixed by resolving declared
base widths and encodings. The refreshed inventory has no Rust parser errors or
audited property mismatches. Unrepresented definitions remain explicitly recorded.
CP implementation structures, arrays, aliases, and inline members now use the
existing shared type model and scoped runtime entries. The original 544 missing
implementation composites match the reference definitions. Fixed-layout decode
and compatibility checks are implemented. Dynamic implementation-array binary
decoding remains deferred and returns an explicit length-policy error. The error
propagates through enclosing application arrays and structures using an internal
layout/payload distinction. Unused unsupported implementation categories are
retained as diagnostic nodes and do not prevent supported messages from loading;
using one reports its source path and category. Unknown/cyclic references and
malformed supported definitions retain their existing parsing errors. Schema
inference enumerates all configured entries and propagates resolution/layout
errors with message IDs, including top-level payloads and array elements. CP requests now model IN/INOUT arguments in order and represent OUT-only
getters as empty requests; a single input keeps its original shape. Empty requests
reject nonempty payloads. Service Discovery `(0xFFFF, 0x8100)` is excluded as
protocol control traffic. The unchanged complete BAQ schema and root SOME/IP E2E
tests use these semantics. Response payload selection remains deferred because
the current GBF adapter does not pass message_type into the payload decoder.
CP application array/string maxima are retained as converter-only metadata for
inspection and future policies, without changing SQL types or decoder length
behavior. Mapping and short-name coverage flags are distinguished from SQL
projection and confirmed decoding defects in the inventory findings.
Binary payload checks with `someipy` and the later steps below remain
planned. The Go original and `esrlabs/someip-payload` are outside this investigation.

## Reference Libraries and Boundaries

| Library | Role | Boundary |
| --- | --- | --- |
| [autosar-data-py](https://github.com/DanielT/autosar-data-py) (`autosar_data`) | Load the same ARXML files, inspect full AUTOSAR paths and reference targets, and independently trace deployed messages to their types. | Python bindings to a Rust library. It provides an ARXML model, not an automatic SOME/IP payload decoder. Domain-specific traversal still needs review. |
| [someipy](https://github.com/chrizog/someipy) | Independently define equivalent payload types and compare serialization and deserialization of identical bytes. | Its documented workflow defines payload types in Python. Direct ARXML import is not assumed. Verify each required datatype and wire-format option in the pinned version before using it as a reference. |

Record the exact library versions or commits, Python version, and options used.
Use an isolated Python environment without changing production dependencies.
Start with local serialization APIs; network services and discovery are not
required for payload comparison.

Agreement between libraries is evidence of consistency, not proof of conformance.
Resolve disputed wire behavior against the applicable AUTOSAR release and its
serialization configuration. Missing reference-library features must be recorded
as comparison gaps rather than decoder failures or passing checks.

## Code and Existing Inputs

- [GBF schema contract](../schema/gbf.md) and [GBF decoder](gbf.md).
- [ARXML codec](../../arxml_converter/src/lib.rs): CP/AP loading, message lookup,
  and public decoding entry points.
- [Binary decoder](../../arxml_converter/src/decoder/mod.rs) and
  [basic type resolution](../../arxml_converter/src/ast/resolver.rs).
- [Schema compilation](../../src/schema/arxml.rs) and
  [SOME/IP output adapter](../../src/decoder/someip.rs).
- [Existing integration cases](../../arxml_converter/tests/integration_test.rs).
- Existing fixtures in `arxml_converter/tests/test_data/`: `minimal_cp.arxml`,
  `s1_cp_test.xml`, `s1_ap_test.xml`, and `baq.arxml`.
- Existing GBF input and assertions in `src/decoder/someip.rs`, including the
  parking-slot message identified by service `0xAB04` and event `0x8003`.

## Comparison Procedure

### 1. Check Reference Capabilities and Fixture Validity

Inspect the pinned libraries' APIs, implementation, and examples. Establish which
numeric types, string formats, array boundaries, and byte-order options can be
compared directly with `someipy`.

Load existing ARXML fixtures with `autosar-data-py` in strict mode first and
record warnings and failures. If non-strict mode is needed, preserve that result
separately. Do not silently edit the input until a reference library accepts it.
Simplified fixtures may omit schema metadata or definitions; their rejection
alone does not establish a binary-decoder defect. Loading successfully also does
not establish that every deployment or cross reference is semantically valid.

If an existing fixture cannot support a valid comparison, propose a minimal,
schema-valid fixture with explicit deployment settings before implementing it.

### 2. Independently Trace ARXML Message and Type Definitions

Use `autosar-data-py` paths and reference targets to trace each selected service
and event/message to its payload type. Compare with `ArxmlCodec::resolve`,
`configured_entries`, filtered `known_entries`, and the schema inferred by veloFlux.

Compare numeric identifiers, full reference paths, application-to-implementation
type mappings, primitive signedness and width, member order, array dimensions,
and applicable serialization properties. Include CP and AP examples separately.
Keep raw CP header IDs distinct from the unified service/event ID combination;
do not assume every fixture uses the same identifier relationship.

Inspect same-short-name types in different packages and renamed packages to
detect name-based lookup assumptions. Distinguish unsupported input layouts from
cases that resolve successfully to the wrong type.

The Python traversal must follow ARXML references independently rather than copy
the Rust lookup algorithm or consume its resolved AST as the expected answer.

### 3. Compare Identical Binary Payloads

Construct equivalent `someipy` types from the independently inspected ARXML
definitions and deployment settings. Review these definitions manually for the
initial small examples. Do not generate reference types from our decoder's AST.

Feed exactly the same payload bytes to both implementations. Do not remove length
fields, BOMs, terminators, or padding on only one side. Distinguish the GBF
envelope, SOME/IP header, and serialized application payload when extracting
bytes from an existing capture.

Use both manually calculable payloads and payloads serialized by `someipy` from
known values. A reference-library round trip checks the sample setup, while the
cross-library decode checks our implementation. Preserve externally captured
bytes unchanged for real-world examples.

Compare success/failure, typed values, field and element order, array lengths,
and consumed bytes where exposed. The internal Rust decoder returns consumed
bytes even though the high-level codec discards them. If a public API does not
expose consumption, record that limitation and use a following sentinel field
where possible to detect boundary errors.

### 4. Check veloFlux Output Separately

For a selected real-world message, compare the complete `ArxmlCodec` result
before checking the inferred schema and final GBF-decoded Tuple. Verify column
naming, runtime value types, nested values, and preservation of array elements
under the intended output contract. Record intentional projection separately
from unexpected data loss.

Keep output-adapter behavior separate from payload conformance, including
repeated-frame selection and handling of unknown or malformed frames.

## Proposed Initial Cases and Expected Results

These payload cases remain the proposed set for confirmation before implementing
the `someipy` tests; the initial reference framework checks ARXML type outputs.
Run a case through a reference library only after verifying that it can represent
the required wire configuration. Record unavailable comparisons explicitly.

| Case | Input and configuration | Expected result |
| --- | --- | --- |
| Unsigned integer | Big-endian `uint16`, bytes `12 34`. | Value `4660`, consuming two bytes. |
| Signed integer | Signed 8-bit type, byte `FF`. | Value `-1` with a signed runtime type matching the inferred schema. |
| Byte order | Little-endian `uint16`, bytes `34 12`, with explicit deployment settings. | Value `4660`; unsupported configuration must not silently produce a different value. |
| Member order | Packed structure `{a: uint8, b: uint16}`, big-endian, bytes `01 02 03`, with no intervening padding. | Ordered values `{a: 1, b: 515}`, consuming three bytes. |
| Fixed array | Big-endian `uint16[2]`, bytes `00 01 00 02`. | Exactly two elements `[1, 2]`, consuming four bytes. |
| Dynamic array boundary | Four-byte big-endian byte-length prefix, `uint16` elements, followed by a `uint8` sentinel; bytes `00 00 00 04 00 01 00 02 7F`. | Array `[1, 2]` and sentinel `127`; the array must not consume the sentinel. |
| String framing | UTF-8 with four-byte length, BOM, and terminator; bytes `00 00 00 08 EF BB BF 54 65 73 74 00`. | Application string `Test`, with framing excluded from the value and consumption confined to the string. |
| Empty dynamic value | Zero-length array and a valid empty string in the selected string format. | Empty values; a valid empty string still includes any framing required by that format. |
| Truncation | Scalar cut short, declared length beyond available bytes, or an incomplete array element. | Explicit failure without a successful partial value. Compare error categories rather than exact text. |
| Type identity | Different packages contain types with the same short name but different widths. | Resolve the full referenced path and decode using the referenced width. |
| Real-world composite | An existing parking-slot payload with nested fields and multiple elements. | Compare every decoded field and array element, then validate the separate veloFlux output contract. |

After the initial cases, extend to remaining integer widths and boundaries,
floating-point values, nested composites, and additional CP/AP messages according
to findings. Union, TLV, extra encodings, transport reassembly, and other features
are deferred until their relevance and reference-library capabilities are known.

## Evidence and Difference Classification

For every case, preserve the source ARXML and its hash, AUTOSAR version, message
identifiers, full type paths, deployment settings, exact payload bytes, independent
expected values, reference versions, options, and both results. Record any default
assumptions explicitly. Keep full payload coverage in the evidence; checking only
a few fields is insufficient for composite messages.

Normalize results without losing signedness, integer width, 64-bit precision,
array order, or bytes. Compare floating-point bit patterns where available;
handle NaN and signed zero explicitly rather than applying a blanket tolerance.
If JSON is used, encode large integers and non-finite floats without precision
loss. Do not strip framing or coerce types during normalization to hide a mismatch.

Classify differences as ARXML reference/type resolution, serialization settings,
payload decoding, veloFlux output adaptation, invalid/incomplete fixture,
reference limitation, or unresolved specification interpretation. Link each
finding to code locations and a minimal reproducible example. Use official
requirements to adjudicate disagreements rather than treating either library as
infallible.

The investigation deliverables are a code-logic comparison, per-case results,
reproducible inputs, confirmed defects, and explicit coverage gaps. The
selected-case and complete-inventory harnesses and the signed 8-bit/float64
resolver fixes and CP implementation-composite support are implemented.
Additional payload fixtures, `someipy`
comparisons, and other runtime fixes remain follow-up work after the required
design confirmation.

## References

- [autosar-data-py API](https://danielt.github.io/autosar-data-py/).
- [someipy documentation](https://someipy.readthedocs.io/en/latest/).
- [someipy serialization implementation](https://github.com/chrizog/someipy/blob/master/src/someipy/serialization.py).
- [AUTOSAR SOME/IP Protocol R24-11](https://www.autosar.org/fileadmin/standards/R24-11/FO/AUTOSAR_FO_PRS_SOMEIPProtocol.pdf).
- [AUTOSAR SOME/IP Transformer R24-11](https://www.autosar.org/fileadmin/standards/R24-11/CP/AUTOSAR_CP_SWS_SOMEIPTransformer.pdf).

The R24-11 documents are starting references; use the release applicable to each
ARXML fixture when determining conformance.
