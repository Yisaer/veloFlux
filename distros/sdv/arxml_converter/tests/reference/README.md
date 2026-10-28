# ARXML Input Output Reference Tests

The table in `cases.json` drives both the Rust regression test and an opt-in live
comparison with `autosar-data-py`. Add inputs and explicit expected outputs to
the table to extend coverage without adding one Rust test function per example.
The framework checks production parsing and decoding behavior, including exact
lookup of qualified type paths.

## Running the Tests

From the repository root, run the Rust input/output regression without Python:

```bash
cargo test -p arxml_converter --test arxml_reference_tests
```

Prepare an isolated reference environment and execute both tests:

```bash
python3 -m venv /tmp/veloflux-arxml-reference
/tmp/veloflux-arxml-reference/bin/python -m pip install -r distros/sdv/arxml_converter/tests/reference/requirements.txt
ARXML_REFERENCE_PYTHON=/tmp/veloflux-arxml-reference/bin/python cargo test -p arxml_converter --test arxml_reference_tests -- --include-ignored --nocapture
```

The live test is explicitly ignored in ordinary `cargo test` because it requires
Python and a separate installed dependency. Once selected, missing dependencies,
an incorrect dependency version, or a failing reference process fail the test;
they are not silently skipped. Reference execution performs no downloads.

To retain the full Python report, including fixture SHA-256 hashes, versions,
load warnings, and full reference paths:

```bash
PYTHONDONTWRITEBYTECODE=1 /tmp/veloflux-arxml-reference/bin/python distros/sdv/arxml_converter/tests/reference/autosar_reference.py distros/sdv/arxml_converter/tests/reference/cases.json > /tmp/veloflux-arxml-reference.json
```

## Case Contract

Each case has a unique `id`, provenance in `source`, an `input`, a `comparison`
mode, and explicit `expected` results. Input paths are relative to this directory.
Cases use strict reference loading unless `input.strict` is explicitly false.

- `resolve`: select a payload using the unified service/event identifiers.
- `resolve_cp`: select a CP payload using a service ID and a raw 32-bit header ID.
- `type`: inspect a type through the loaded codec.
- `ap_type`: inspect an AP type package without requiring service deployment.
- `cp_type`: inspect a standalone CP application or implementation type, including application mappings.
- `implementation_type`: compatibility operation for standalone CP implementation types.

The standalone CP and AP operations exercise existing type parsers directly.
Their success does not establish that `ArxmlCodec::load` accepts those standalone documents.

An example table entry is:

```json
{
  "id": "ap_wifi_vector",
  "source": {
    "kind": "repository",
    "url": "https://github.com/Yisaer/veloFlux"
  },
  "input": {
    "file": "../test_data/s1_ap_test.xml",
    "strict": true,
    "query": {
      "operation": "type",
      "type_path": "/dataTypes/WiFiApArray"
    }
  },
  "comparison": "equal",
  "expected": {
    "both": {
      "ok": {
        "name": "WiFiApArray",
        "category": "VECTOR",
        "definition": { "kind": "vector", "element": "WiFiApInfo" }
      }
    }
  }
}
```

`expected.both` specifies a shared result. A case can instead provide separate
`expected.rust` and `expected.python` results. Successful outputs are compared
exactly, including all ordered fields in the selected type. Error expectations
use `error_contains` to avoid comparing platform-specific filenames or wording.

The comparison modes are:

- `equal`: both outputs must match the shared expected value and each other.
- `both_error`: both must fail and satisfy their respective error expectations.
- `known_difference`: both outputs must satisfy separate expectations and remain
  different. A nonempty `reason` is required. Unexpected changes, including a
  resolved difference, require updating the case rather than silently passing.

These modes do not make known differences equivalent to conformance. The current
30 cases contain 27 matching outputs, one shared rejection, and two explicitly
recorded differences. The suite passes when those expectations hold.

## Selected Case Traversal and Coverage

The Python adapter uses `AutosarModel`, full paths, `reference_target`, and reverse
reference lookup. It does not receive the Rust AST or its output. For CP messages,
it traverses header/PDU/signal references and reverse system-signal mappings; for
AP messages it follows event or notifier deployment references. Multiple CP
routes are accepted only if they converge on one payload type.

The 30 selected cases cover type names, categories, base reference names,
ordered structure fields, array sizes, vector elements, string sizes, and
in-place flags. Each selected type is inspected one level deep; nested types
need their own cases. The reference validates the traversed targets and records
full paths separately. The shared normalized output uses short target names,
while the inventory audit compares retained full references independently. CP
DATA-TYPE-MAP inputs preserve both full paths; the Rust inventory adapter looks
up definitions by exact identity. Implementation IO regressions also check
same-named application and implementation types in different packages, both
declaration orders, and missing or ambiguous mapping references.

Dynamic CP array size zero follows the current Rust AST representation. The
source's maximum element count is not compared by this initial adapter. Base
reference names also do not establish correct signed binary decoding. Payload
bytes, serialization properties, final veloFlux Tuple output, and `someipy`
comparisons remain separate follow-up work for these selected cases. The complete
inventory below additionally audits retained paths, constraints, and base traits.

## Complete Type Inventory

`inventory.json` selects documents, not individual type names. The reference
adapter enumerates every named `*-DATA-TYPE` and every `SW-BASE-TYPE` in each
loaded document by full AUTOSAR path. New type names are included automatically.
An unfamiliar type element or category becomes an explicit adapter gap rather
than disappearing from the results. All four original `test_data` documents,
the implementation-composite fixture, and thirteen reference fixtures are selected.
Ten of those reference fixtures implement the two upstream migration batches below.

Run the inventory with the same reference environment:

```bash
ARXML_REFERENCE_PYTHON=/tmp/veloflux-arxml-reference/bin/python cargo test -p arxml_converter --test arxml_reference_tests type_inventory::all_arxml_types -- --ignored --nocapture
```

The full per-type JSON report is written to
`target/arxml-reference/type_inventory.json` at the workspace root. Set
`ARXML_INVENTORY_REPORT` to choose another output path. The report includes every
source definition, full reference paths, constraints, the actual Rust definition
where represented, inferred runtime base traits, differences, coverage gaps, and expected normalizations.
It also records file hashes, parser mode, strict-loading options, reference
versions, document-load failures, and counts by element/category/status.

The Rust side reads each document once and uses the existing CP or AP datatype
parser. CP mapping inputs are extracted from the source XML independently of the
Python result. `cp` selects the production `DataTypes` package; `cp_types`
explicitly checks standalone upstream type fixtures without claiming complete
codec loading. Only the production AP `dataTypes` package is parsed; standard
types in other packages are not artificially added to the registry. Missing
registry entries are not replaced with synthesized builtin types. A registry
without a definition can still support a runtime builtin fallback; this report
does not equate those two capabilities.

| Status | Meaning |
| --- | --- |
| `matched` | The normalized definition and required audited facts match under the current contract. Expected short paths, resolved mappings, and metadata-only length limits do not reduce status. |
| `partial` | Compared properties agree, but required facts remain unverifiable, such as native types outside the builtin contract or ambiguous short-name identity. This is not a complete match. |
| `different` | At least one audited property differs. Any additional gaps remain attached to the same row. |
| `unrepresented` | No independently extracted source definition in the production parser registry. |
| `rust_error` | The Rust datatype parser rejects the document. |
| `adapter_gap` | The Python comparison adapter has not implemented this source construct. |
| `reference_error` | The reference could load the document but could not inspect this definition or resolve a required reference. |

Document load errors are reported separately; no types from such a file are
claimed to have been checked. The sum of per-type status counts must equal the
inventory size for each successfully loaded document.

Absolute references retained in the Rust AST are compared directly. Short
references are compared in the normalized definition. Runtime compatibility
lookup applies only to unqualified names; supplied qualified references require
an exact registered identity and cannot fall back to a basename. CP application
fields and array elements preserve their supplied reference paths. Resolved application mappings and short
references appear in `expected_normalizations`, not `coverage_gaps`. Ambiguous
short-name collisions remain gaps when scoped identity is unavailable. Retained
maximum lengths are compared as metadata; enforcing them is deferred and does
not reduce status. Variable strings are checked against their existing
`string_size = 0` representation. Supported native types (`int32_t`, `uint16_t`,
`uint32_t`, `float`, and `String` in the current samples) are checked against an
explicit builtin-trait table independent of the Rust resolver. Repeated SW base
declarations are not required; unknown native types retain a verification gap.
All 26 explicit `SW-BASE-TYPE` definitions are compared independently using exact
full-path registry lookup, name, terminal runtime category, bit width, and
encoding. Expected traits come from Python model declarations; actual traits
come from stored Rust primitives. Source-category preservation, byte order, native
declarations, and memory alignment are outside this comparison. A missing base
registry entry remains unrepresented; builtin fallback is not used to fill it. CP implementation ARRAY and STRUCTURE
definitions, including inline nested members, are compared recursively against
the existing Rust `DataType` model and its scoped internal entries. Explicit
array bounds retain fixed/variable semantics, the declared count, and handling
policy. The original 441 implementation structures and 103 arrays in `baq.arxml`
now match their reference definitions.
Source base bit widths and encodings are compared with the existing
`resolve_basic_type` result; the expected traits come from `SW-BASE-TYPE` fields,
not from type-name inference. Native C++ definitions without those declarations
are marked as such instead of inventing expected widths.

CP application array/string limits are compared through the converter's
separate `application_constraints` metadata, including fixed-array limits that
also appear in the lowered length. The report emits these under
`application_constraints`; they are not decoder enforcement policies. Mapping
and short-reference provenance gaps remain distinct from type/value mismatches
and SQL output projection. See the inventory findings for that distinction.

`inventory_baseline.json` preserves category/status counts, source SHA-256 hashes,
and a deterministic regression checksum of every full per-type result. New,
removed, or changed types therefore fail the regression even if aggregate counts
happen to remain equal. The checksum is not a security mechanism. The baseline
records known failures and coverage gaps; passing it does not mean every type
matches. For an explicit conformance gate, add `ARXML_REQUIRE_FULL_MATCH=1`; this
requires every document to load and every row to be `matched` and currently fails.

After reviewing changes in the full report, deliberately update the baseline:

```bash
ARXML_REFERENCE_PYTHON=/tmp/veloflux-arxml-reference/bin/python ARXML_UPDATE_INVENTORY=1 cargo test -p arxml_converter --test arxml_reference_tests type_inventory::all_arxml_types -- --ignored --nocapture
```

This command records expectations; rerun without `ARXML_UPDATE_INVENTORY` to
validate them. See the [inventory findings](../../../docs/decoder/arxml_type_inventory.md)
for current counts, known differences, and their scope. Payload byte decoding,
deployment serialization settings, and veloFlux output adaptation remain outside
the inventory.

## Sample Provenance

Repository inputs are reused from `../test_data/`. The minimal CP fixture has
its type references corrected to actual package paths; its missing header is
preserved. Other original repository inputs are unchanged. These include the AP
Wi-Fi events and CP parking-slot message and its constituent types. Two inputs
record reference-loading limitations:

- `minimal_cp.arxml` lacks the ARXML header required by the reference library.
- `s1_cp_test.xml` is rejected in strict mode because `HAS-INTERNAL-STATE` is not
  allowed at that position in its declared AUTOSAR 4.2.2 version.

`../test_data/implementation_types.arxml` is a locally authored, strictly
reference-loadable fixture. Its 13 definitions cover implementation aliases,
structures, fixed and empty arrays, nested inline members, dynamic bounds, and
same-short-name types in separate packages. It is included in the complete
inventory. Run its binary input/output and compatibility checks without Python:

```bash
cargo test -p arxml_converter --test implementation_types
```

The checks also cover truncated fixed data, following-field alignment,
application short-name precedence, invalid/cyclic references, and a service
interface referencing an implementation composite through an alias. Dynamic
implementation definitions are inspected, but decoding them explicitly requires
a serialization length policy; a member named `sizeIndicator` is not interpreted
as that policy. Internal layout errors propagate through enclosing application
arrays, structures, and aliases; legacy stopping on a payload error remains
unchanged. Cases include direct and nested variable implementation arrays with
three-byte inputs, unresolved nested references, unsupported UNION elements, and
truncated uint16 elements. Unused UNION definitions, aliases, and containing
structures preserve successful decoding of the original message; selecting them
returns the retained source diagnostic. Unsupported fields also fail SDV schema
inference instead of silently becoming string columns.

`../test_data/schema_message_entries.arxml` is a locally authored two-message CP
fixture based on the minimal fixture. It is used for configuration enumeration
and SDV schema regressions, separately from the Python type inventory. Tests
verify that `configured_entries()` includes unresolved payloads while
`known_entries()` remains filtered; AP events and field notifiers follow the same
API distinction. Schema tests reject direct unsupported payloads, aliases, array
elements, nested arrays, unsupported fields sharing an existing column name,
and missing references in mixed configurations. Unused
UNION definitions still permit a normal schema. Public inference of the complete
BAQ sample now succeeds: OUT-only getters are supported empty requests and the
protocol Service Discovery ID is excluded from application inference. The BAQ
column-shape test and root E2E tests use the complete original input. Request
cases check missing/empty argument lists, OUT-only getters, IN/OUT order, INOUT,
multiple inputs, truncation, and rejection of nonempty empty-request payloads.
Single inputs preserve the original shape. Response decoding remains deferred.
The same target also checks the retained application limits (`5` array elements
and text size `128`), exact metadata lookup, and unchanged decoding behavior.

`fixtures/upstream_value.arxml` is generated with the pinned reference library
from the VALUE construction pattern in upstream
[`test_implementation_data_type_value`](https://github.com/DanielT/autosar-data-py/blob/1e064d751eadcb184961f025754e0605099b56eb/test/abstraction/implementation_data_type_test.py).
The optional computation method and constraint are omitted. It preserves the
`ImplValue` and `SwBaseType` names and the 32-bit two's-complement base definition.
Rust resolves the declared 32-bit two's-complement base independently of its
name; this case now matches. Binary regressions also check declaration precedence,
legacy fallback, and unsupported declarations.
`upstream_uint32.arxml` adapts the same pattern to a base named `uint32` with
`NoEncoding`, providing a matching supported-name case. These are generated
adaptations, not verbatim upstream fixture files.

`fixtures/autosar_struct_prefix.arxml` is locally authored from the finite portion
of the structure illustration in
[AUTOSAR SOME/IP Protocol R24-11, section 4.1.4.2, Figure 4.1, page 29](https://www.autosar.org/fileadmin/standards/R24-11/FO/AUTOSAR_FO_PRS_SOMEIPProtocol.pdf).
It represents the ordered members and two-element float arrays. The diagram's
unspecified `Struct_3` continuation is excluded and the types are named
`Struct1Prefix` and `Struct2Prefix` to make that boundary explicit. The file is
an AP ARXML representation of the example, not an official distributed ARXML
file. Strict loading validates its declared ARXML structure; these tests do not
claim to validate the illustration's binary serialization.

## Upstream Case Migration

Batch 1 is implemented with six generated fixtures, eight selected cases, and
23 independently compared type definitions. All new rows match. The original
eight inventory document results, including their checksums, remain unchanged.
Batch 2 is implemented with four additional fixtures, five happy-path cases, and
14 matching type definitions. All fourteen previous inventory document results
retain their exact checksums.

### Sources and Scope

The generator uses `autosar-data` 0.17.0 and upstream commit
`1e064d751eadcb184961f025754e0605099b56eb`, matching the existing VALUE fixture
provenance. The upstream tree at this commit has no standalone `.arxml` or `.xml`
fixtures. Its Python tests construct models that can be serialized into ARXML.
The relevant sources are:

- [Implementation datatype tests](https://github.com/DanielT/autosar-data-py/blob/1e064d751eadcb184961f025754e0605099b56eb/test/abstraction/implementation_data_type_test.py): VALUE, ARRAY, STRUCTURE, TYPE_REFERENCE, unsupported categories, and mixed inline members.
- [Application datatype and mapping tests](https://github.com/DanielT/autosar-data-py/blob/1e064d751eadcb184961f025754e0605099b56eb/test/abstraction/datatype_test.py): application arrays and records, array size modes, mappings, and SW base declarations.

The existing `upstream_value.arxml` and `upstream_uint32.arxml` already cover the
VALUE construction pattern. Keep them unchanged and add complementary examples.
The goal is independent source coverage beyond BAQ and locally authored fixtures;
some constructs intentionally overlap existing implementation tests.

This migration concerns ARXML definitions and reference resolution. These
upstream tests provide neither SOME/IP payload bytes nor independent decoded
payload values. Service deployment, complete codec loading, binary decoding,
SQL output, and performance are outside this migration. The separate validation
plan retains the proposed `someipy` payload work.

### Batch 1: Supported Definitions and Reference Chains (Implemented)

The following IDs are entries in `cases.json`. All six fixtures strictly reload
with the pinned Python library and have no unresolved references. They use
`AUTOSAR_00054.xsd` (R25-11). The selected cases compare one-level normalized
outputs; the inventory additionally audits inline definitions and full references.

| Case | Upstream construction | Verified expected result |
| --- | --- | --- |
| `upstream_impl_array_fixed` | `test_implmentation_data_type_array` | ARRAY of 10 values described by one inline VALUE element; the element references `SwBaseType`, declared as signed 32-bit. Preserve fixed bounds and the full base path. |
| `upstream_impl_structure` | `test_implmentation_data_type_structure` | STRUCTURE containing the ordered inline member `ImplValue`, resolving to the declared signed 32-bit base. |
| `upstream_impl_type_reference` | `test_implmentation_data_type_type_reference` | `ImplRef` references the exact `ImplValue` path; runtime base traits resolve through the alias to signed 32-bit. |
| `upstream_application_mapping` | `test_data_type_mapping_set` | The application VALUE maps to the exact implementation VALUE, which references the exact SW base. Lowering preserves the application identity and yields signed 32-bit traits. |
| `upstream_application_array_fixed` | `test_application_array_data_type` | Fixed application array with 10 elements and retained maximum 10; its element reference resolves exactly. |
| `upstream_application_record` | `test_application_record_data_type` | Record members remain ordered as `Element1`, `Element2`, `Element3`, targeting the primitive, nested record, and fixed array respectively. Preserve the empty nested record. |
| `upstream_application_array_of_records` | `test_application_array_data_type` | Fixed array of 10 records, retaining the referenced record identity, including its empty member list. |
| `upstream_application_array_of_arrays` | `test_application_array_data_type` | Fixed outer length 10 referencing a fixed inner array of length 10; retain both array definitions and their full reference paths. |

The upstream application array and record examples create application VALUE
types without implementation mappings. Their adaptations add explicit signed
32-bit implementation VALUE and SW base definitions plus DATA-TYPE-MAP entries,
following the upstream mapping example. Empty records receive explicit empty
`ELEMENTS` containers for the current parser and reference adapter. No members
are added. These changes are recorded in the provenance manifest. The mapping
case preserves the upstream mapping construction. Compu methods, units, and
constraints are retained where the source constructs them, but their
physical-value semantics are outside the comparison.

### Batch 2: Supported-Type Happy Paths (Implemented)

All five selected cases strictly load in Python, parse successfully in Rust, and
match their explicit expected outputs. All 14 new inventory definitions match.
Each fixture uses the same pinned dependency, upstream commit, and R25-11 schema
as Batch 1. The following cases cover the confirmed supported-type scope:

| Case | Upstream construction | Verified expected result |
| --- | --- | --- |
| `upstream_application_array_variable` | `test_application_array_size`, selecting `VariableLinear(20)` | Variable application array with lowered size zero, retained maximum 20, and exact element reference to `ApplicationPrimitiveDataType`. The adaptation adds an explicit signed 32-bit implementation mapping. |
| `upstream_impl_mixed_members` | Adapt `test_implmentation_data_type_element` | The four retained members remain ordered as `ImplValue`, `ImplStruct`, `ImplArray`, `ImplTypeRef`. The inventory recursively checks supported nested members, fixed array length 10, and the exact alias target `OtherImplType`. The selected output marks inline STRUCTURE and ARRAY fields as in-place. |
| `upstream_impl_array_of_structures` | Compose the upstream ARRAY and STRUCTURE patterns | `ImplArray` has fixed count 10 and inline element `ImplStruct`, which contains VALUE member `ImplValue` referencing the exact signed 32-bit SW base. |
| `upstream_impl_array_alias` | Apply the upstream TYPE_REFERENCE pattern to a fixed array | `ImplArrayRef` retains the exact target `/package/ImplArray`; the target's count 10 and inline VALUE definition match independently. |
| `upstream_impl_structure_alias` | Apply the upstream TYPE_REFERENCE pattern to a structure | `ImplStructRef` retains the exact target `/package/ImplStruct`; the target's ordered VALUE member and full base reference match independently. |

The array and structure alias cases share
`upstream_impl_composite_aliases.arxml`; the other cases each have their own
fixture. Mixed-member selection, composed layouts, changed alias targets, and the
variable application's added mapping are explicit local adaptations recorded in
[`upstream_batch2_provenance.json`](fixtures/upstream_batch2_provenance.json).

UNION, DATA_REFERENCE, FUNCTION_REFERENCE, unsupported encodings, and expected
rejection cases are excluded from this migration. Square, rectangular, and fully
flexible array modes are also excluded because their dimensional semantics are
not established by the current type contract. Existing rejection tests elsewhere
remain unchanged.

Byte order, alignment, native declarations, compu-method interpretation,
client/server deployment, and Ethernet routing remain outside this batch.
Successful comparison of a variable-array definition does not establish dynamic
implementation-array payload decoding or maximum-length enforcement. The separate
payload validation plan retains the `someipy` work.

### Fixture Generation and Framework Integration

[`generate_upstream_fixtures.py`](generate_upstream_fixtures.py) contains the
reviewable construction patterns. It performs no downloads and does not use Rust
output. It selects the constructed state before upstream name/settings mutations
and omits API round-trip assertions. Arrays of records and arrays of arrays share
`upstream_application_arrays.arxml` with the fixed application-array case; the
remaining five Batch 1 cases each use a separate fixture. The same generator
constructs the four Batch 2 fixtures and selects the linear variable-array state
without generating other size modes.

[`upstream_batch1_provenance.json`](fixtures/upstream_batch1_provenance.json)
and [`upstream_batch2_provenance.json`](fixtures/upstream_batch2_provenance.json)
record the source functions, commit, selected states, local adaptations, schema
version, and generated SHA-256 hashes. The generator and fixtures use the upstream
MIT license, retained in [`UPSTREAM-LICENSE-MIT`](fixtures/UPSTREAM-LICENSE-MIT).
From the repository root, regenerate with the pinned environment:

```bash
PYTHONDONTWRITEBYTECODE=1 /tmp/veloflux-arxml-reference/bin/python distros/sdv/arxml_converter/tests/reference/generate_upstream_fixtures.py
```

Check byte-for-byte reproducibility, including provenance, without writing:

```bash
PYTHONDONTWRITEBYTECODE=1 /tmp/veloflux-arxml-reference/bin/python distros/sdv/arxml_converter/tests/reference/generate_upstream_fixtures.py --check
```

For each batch, generation strictly reloads every document and checks that all
model references resolve before writing its fixtures. Ordinary comparison runs only read committed
files. Explicit fixture regeneration and inventory baseline updates remain
separate operations.

The new inventory documents use `parser: cp_types`, exercising production type
parsing from standalone `/package` definitions without claiming complete CP codec
loading. The selected `cp_type` operation uses the same source mapping extraction
as the inventory. The Python selected-case adapter distinguishes CP implementation
ARRAY, STRUCTURE, and TYPE_REFERENCE layouts from AP layouts. CP structure and
array outputs name their inline members; the inventory recursively checks those
members' actual definitions and references. Production parser behavior is
unchanged.

### Validation and Results

The Rust regression, live Python differential comparison, complete inventory, and
fixture reproducibility check pass for both batches. Batch 1 documents have
2, 2, 3, 3, 7, and 6 definitions respectively; Batch 2 documents have 4, 3, 2, and 5.
All 37 migrated definitions are `matched`. The full inventory contains 2,850
definitions: 2,835 matched and the existing 15 unrepresented AP definitions. The
two existing reference-load failures remain recorded separately. All fourteen
inventory documents predating Batch 2 retain their exact checksums.

Run the Rust and live reference commands above to check all 30 selected cases
and the complete inventory. Review `target/arxml-reference/type_inventory.json`
before deliberately updating a baseline, then rerun without
`ARXML_UPDATE_INVENTORY`. All new Batch 2 rows match; adapter gaps or unexpected
differences must be investigated rather than accepted into the baseline as
successful coverage. No payload or timing success is claimed by this migration.

## Adding Cases

1. Record a precise source and distinguish copied, generated, and adapted data.
2. Select an operation and verify its input identifiers or full type path.
3. Inspect the source and reference output before writing expected values. Do
   not use Rust output as the independent reference answer.
4. Specify exact output or a narrow error expectation. Explain known differences.
5. Run both tests with the pinned reference library. Review printed inputs,
   outputs, and traversed reference paths for the new case.

For the broader investigation, see the
[ARXML Decoder Validation Plan](../../../docs/decoder/arxml_validation_plan.md).
