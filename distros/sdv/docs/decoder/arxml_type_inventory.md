# ARXML Type Inventory Findings

The complete inventory checks every type definition in the documents selected by
[`inventory.json`](../../arxml_converter/tests/reference/inventory.json) against
`autosar-data-py` 0.17.0. It extends the original 17 selected input/output cases
without replacing them. The current run enumerates 2,813 definitions across six
strictly loaded documents. Two additional existing CP fixtures fail reference
loading and are reported separately.

## Results by Document

| Document | Types | Matched | Partial | Different | Unrepresented | Rust errors |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `s1_ap_test.xml` | 27 | 14 | 0 | 0 | 13 | 0 |
| `baq.arxml` | 2,762 | 2,762 | 0 | 0 | 0 | 0 |
| Implementation composite fixture | 13 | 13 | 0 | 0 | 0 | 0 |
| Official structure adaptation | 7 | 5 | 0 | 0 | 2 | 0 |
| Upstream unsigned VALUE adaptation | 2 | 2 | 0 | 0 | 0 | 0 |
| Upstream arbitrary-base-name adaptation | 2 | 2 | 0 | 0 | 0 | 0 |
| Total | 2,813 | 2,798 | 0 | 0 | 15 | 0 |

There are no reference-adapter gaps or per-type reference errors in these six
documents. This does not imply coverage of constructs absent from the inputs.
`matched` means agreement under the current converter contract, not full
AUTOSAR conformance or complete payload verification. Expected short-reference
normalization and metadata-only maximum lengths do not make a type partial.
`partial` is reserved for unresolved verification gaps and is not counted as a
match. There are currently no partial rows. Supported native builtin names
provide the required traits without repeated SW base declarations.

The reference rejects `minimal_cp.arxml` because it lacks the required ARXML file
header. It rejects `s1_cp_test.xml` in strict mode because `HAS-INTERNAL-STATE` is
not allowed at its position in the declared AUTOSAR 4.2.2 version. Neither file contributes types to the 2,813-definition total. The minimal CP
fixture's type references were corrected to their actual package paths for exact
lookup; its missing header remains. The legacy CP fixture is unchanged.

## CP Implementation Composite Support

All 441 implementation structures and 103 implementation arrays in `baq.arxml`
now match their reference definitions, including inline nested members, full
reference targets, and array bounds. That implementation-composite change left
the other 2,256 original per-type results unchanged. The additional
13-definition fixture checks aliases, nesting, fixed and empty arrays, variable
bounds, and collisions between short names.

The implementation reuses `DataType`, `StructureField`, and `ArrayType`.
Top-level and inline implementation definitions are registered by their actual
AUTOSAR paths. Inline entries do not acquire global short-name aliases; unique
top-level implementation aliases supplement existing application entries without
overwriting them. Qualified references require exact paths; only unqualified
references use legacy short-name lookup. Stored user-type aliases are
followed while preserving fixed-string attributes. CP interfaces may reference
implementation composites directly or through aliases.

Unsupported implementation categories (including inline categories such as
`UNION`) are retained as `DataTypeKind::Unsupported` nodes with the source path
and diagnostic. Their presence, including unused aliases and structures that
contain them, does not prevent supported messages from loading. Selecting a
direct unsupported type or alias fails resolution; encountering one in a nested
layout fails decoding. Schema inference rejects unsupported fields instead of
assigning them a string datatype. Schema compilation enumerates
`ArxmlCodec::configured_entries()` independently of successful type resolution.
Every configured payload is validated before extracting or deduplicating columns,
so direct unsupported payloads, aliases, array elements, and unresolved references
return errors with service/event IDs and the original diagnostic.
`known_entries()` retains its filtered behavior for existing decoder consumers.
Unused unsupported definitions remain loadable. This does not implement UNION
serialization.
Unknown and cyclic implementation references still fail parsing; malformed
supported definitions and invalid referenced base declarations remain errors.
Fixed-layout composites built from supported primitives can be decoded, including
zero-length fixed arrays and nested fixed arrays/structures. New `ArrayBounds`
metadata records fixed/variable semantics, the declared count or maximum, and
`ARRAY-SIZE-HANDLING` while leaving the existing application/AP representation
unchanged.

Dynamic implementation-array definitions are supported, but their binary decode
requires a serialization length policy. Decoding such an array currently returns
an explicit error instead of guessing from a sibling named `sizeIndicator` or
consuming the remainder of the payload. The 179 inline variable arrays in the
existing implementation structures are therefore definition checks, not claims
of correct dynamic payload decoding. Decoder internals distinguish layout errors
from payload errors without changing the public string-error API. An enclosing
variable-length application array propagates dynamic-length-policy errors,
unsupported-type errors, and unresolved-reference errors, including through
aliases and structures. It cannot turn these errors into an empty or partial
success. The legacy stop-at-last-complete-element behavior on payload errors
(such as trailing truncated data) is preserved; strict payload validation remains
part of the separate validation plan. Empty outer arrays with no elements do not
attempt element decoding.

## Client/Server Request Layouts and Strict Schema Inference

CP operation requests serialize IN and INOUT arguments in declaration order;
OUT arguments belong to responses. See [AUTOSAR SOME/IP R24-11, section 4.2.2,
PRS_SOMEIP_00921 and PRS_SOMEIP_00923](https://www.autosar.org/fileadmin/standards/R24-11/FO/AUTOSAR_FO_PRS_SOMEIPProtocol.pdf).
A getter with only OUT arguments therefore has a supported empty request, not a
missing operation mapping. Operations without ARGUMENTS and empty ARGUMENTS are
handled the same way. Invalid argument directions fail parsing.

A single IN/INOUT argument retains the existing flat datatype and decoded value.
Empty and multi-input requests use derived structures, registered only at the
operation's actual source path with category `REQUEST`. Multi-input fields keep
argument names and order. Empty requests consume zero bytes and reject nonempty
payloads; OUT bytes cannot be decoded as request inputs. The runtime map includes
these derived layouts without adding global short-name aliases.

Schema inference still validates every configured application payload, preserving
unsupported-type and unresolved-reference diagnostics. Only the protocol-defined
SOME/IP Service Discovery identity `(0xFFFF, 0x8100)` is excluded from application
schema inference; adjacent IDs are not exempt. See [AUTOSAR SOME/IP-SD R24-11,
PRS_SOMEIPSD_00151/00152](https://www.autosar.org/fileadmin/standards/R24-11/FO/AUTOSAR_FO_PRS_SOMEIPServiceDiscoveryProtocol.pdf).
The unmodified complete BAQ sample now compiles successfully, including getter
`(43529, 1)`. Tests use the public complete-document schema API and retain the
original SOME/IP EXPLAIN and runtime-output E2E assertions.

This follows the existing call/request scope. Separate response plans, return
signal mappings, and passing the GBF message_type into payload selection remain
deferred; this change does not interpret OUT arguments as requests or claim
response decoding support. Dynamic implementation-array decoding remains deferred
as described above.

## Resolved Base Trait Differences

The inventory compares bit widths and encodings declared by the independent
ARXML model with the result of the Rust
[`resolve_basic_type`](../../arxml_converter/src/ast/resolver.rs) function.

| Source base | Declared traits | Corrected Rust resolver result | Previously affected rows in `baq.arxml` |
| --- | --- | --- | ---: |
| `sint8` | 8 bits, two's complement | Signed 8-bit, encoding `2C` | 38 |
| `float64` | 64 bits, IEEE 754 | 64-bit double, IEEE 754 | 24 |

The original 62 mismatching rows consisted of 31 application definitions and
their 31 implementation definitions. The shared resolver now checks `uint8`
separately before the signed `int8` substring and checks `float64` before the
generic `float` alias. `int8`, `sint8`, and `int8_t` produce `BasicType::Int8`;
`float64`, `float64_t`, and `double` produce `BasicType::Double`. Unsigned 8-bit
aliases and `float`/`float32` retain their existing meanings.

All 62 former base-trait mismatches now agree with the reference. Both the 31
implementation rows and the 31 application rows are `matched`; application mapping
normalization is expected and does not reduce their status. Example corrected
types include
`/DataTypes/ApplicationDataTypes/ADT_ADAS_val_FSRACruiseSpdOffsetPercentFb`
and `/DataTypes/ApplicationDataTypes/ADT_ADAS_val_FusionMetaData`.
The refreshed full report and regression baseline record the corrected traits.
The inventory itself remains a definition-level comparison. Separate decoder
regressions verify signed negative values, double precision beyond float32,
consumed byte counts, and alignment of a following field.

The upstream arbitrary-base-name example now matches the reference.
CP VALUE references resolve `SW-BASE-TYPE` declarations by full path, including
bases outside the selected DataTypes package. Declared bit widths and encodings
select existing integer (8/16/32/64-bit), IEEE754 (32/64-bit), 8-bit Boolean,
and 8-bit UTF-8 primitives, regardless of the source name. Original VALUE
reference paths are preserved; the merged runtime registry contains terminal
base entries with recognized primitive names. Names that already agree with
the declaration (such as `sint32` and `float64`) and existing VALUE identities
are preserved, keeping `entry_fields` output compatible. Terminal bases do not follow
short-name aliases, even if an implementation type has the canonical name.

Unsupported or incomplete referenced declarations return errors instead of
falling back to their names. Missing base declarations are compatible only for
unqualified builtin names or explicitly referenced primitives in the legacy
`/BaseTypes`, `/PlatformTypes`, and `/AUTOSAR/StdTypes` namespaces; these exact
paths are registered during parsing. Runtime lookup never guesses a primitive
from an unknown qualified path. Unreferenced unsupported declarations
do not reject the document. The comparison follows runtime base resolution
separately from source-reference comparison. Binary regressions verify `FF FF FF
FE` as signed `-2`, four consumed bytes, truncated input rejection, declaration
precedence over an unsigned-looking name, and legacy fallback.

## Status Policy and Remaining Partial Verification

Status follows the current veloFlux computation contract. Unqualified references
may use short names. Supplied qualified references retain their scoped identity
and require exact lookup; a path miss must not resolve to an unrelated short
name, including one resembling a builtin. CP application fields and array
elements now retain their supplied paths. CP application and AP definitions
register actual full paths in addition to compatibility names. Normalized
definitions still compare logical short targets and member order, while retained
full reference paths are compared separately.
CP DATA-TYPE-MAP source and target references retain their full paths. Mapping
lookup uses the exact application identity and exact implementation identity;
legacy unqualified mappings require unique names. Ambiguous application or
implementation names have no compatibility alias. Regression cases cover both
declaration orders, same-named types with different widths, missing qualified
targets, and ambiguous unqualified mappings.
Application-to-implementation mapping is executed by `resolve_value_type` before
schema compilation; not retaining the intermediate mapping path is expected type
normalization. Neither behavior is classified as partial support or verification.
The report records them in `expected_normalizations`, separately from
`coverage_gaps`.

The SOME/IP adapter decodes the payload before selecting SQL output slots;
reference normalization occurs in the converter. The current `baq.arxml` has
1,374 application types with no short-name collisions among top-level application
and implementation definitions. These rows now match under the current contract.
Ambiguous short-name collisions still count as verification gaps where scoped
identity is unavailable; using short references does not waive ambiguity checks.

The former 12 native-alias partial rows now match the supported builtin
contract: `int32_t` means signed 32-bit, `uint16_t` and `uint32_t` mean unsigned
16/32-bit, `float` uses the converter's IEEE754 32-bit rule, and `String` uses
its UTF-8 rule. The WiFi string's fixed size of 64 is also compared in the
normalized definition. Repeated SW base declarations are not required. The
comparison uses an explicit expected-trait table independent of the Rust
resolver; unknown native types still leave a verification gap. No partial rows
remain in these samples. The 15 unrepresented rows are tracked separately below.

## Retained Application Limits

CP application array and string limits are now retained separately in
`DataTypesParser::application_constraints`, keyed by full AUTOSAR type path.
`ArxmlCodec::application_type_constraints(path)` exposes this metadata through
`ApplicationTypeConstraints`. Array entries preserve `MAX-NUMBER-OF-ELEMENTS`;
string entries preserve the declared numeric `SW-MAX-TEXT-SIZE` without treating
it as a serialized byte count. Missing declarations have no entry, and AP codecs
return None through this CP-specific inspection API.

For example, `ADT_ApnArray` retains `max_elements = 5` and `ADT_UUIDCode` retains
`max_text_size = 128`. All 207 application-array limits and 75 string limits in
`baq.arxml` are included in the inventory comparison. These limits are no longer
reported as missing constraints. Maximum-length enforcement is deferred by
design and does not make these types partial. Variable strings retain their
variable-size semantics through `string_size = 0`, which the inventory checks
against `ARRAY-SIZE-SEMANTICS` instead of requiring a second metadata field.

This metadata is available for future policies inside the converter. It does not
add constraints to veloFlux SQL datatypes, change `ArrayType`/`TypeReference`,
activate implementation-array length handling, or enforce a maximum during
decoding. Regression coverage checks the example limits, unchanged lowered
types, and acceptance of a 129-byte ASCII value by the existing string decoder
despite the retained 128 limit. Retaining the declared maximum and enforcing it
are separate capabilities; only retention is required by the current contract.
This policy does not claim dynamic implementation-array binary decoding is
implemented: its serialization length policy remains deferred as described above.

## Independent SW Base Comparison

All 16 `SW-BASE-TYPE` definitions now have independent comparison rows: 12 in
`baq.arxml`, two in the implementation fixture, and one in each upstream
adaptation. The Rust side looks up the exact full path in the production merged
registry; no builtin fallback synthesizes a missing definition. Each row checks
the source name, terminal runtime category (`SW-BASE-TYPE`), bit width, and
encoding against the Python model. The independent Python expected values are
read from `BASE-TYPE-SIZE` and `BASE-TYPE-ENCODING`; actual traits come from the
stored Rust primitive. This includes signed/unsigned integers, float32/float64,
Boolean, UTF-8, and the arbitrary name `SwBaseType`.

The source category (such as `FIXED_LENGTH`) is lowered to a terminal primitive
and recorded as an expected normalization. This comparison does not audit
source-category preservation, byte-order attributes, native declarations, memory
alignment, or binary payload decoding. All 16 rows match on the audited traits.

## Remaining Unrepresented Definitions

The remaining 15 rows are AP standard-type definitions outside the production
parser's `dataTypes` package: 13 in the AP sample and two in the official
adaptation. They have no independently extracted Rust source definition in this
comparison. References to supported builtins are checked using the builtin
contract, so an unrepresented source definition does not itself imply a runtime
decoding failure. SW base definitions are no longer in this group.

## Reproducing and Reviewing Results

Follow the [reference test instructions](../../arxml_converter/tests/reference/README.md)
to install the pinned dependency and run `type_inventory::all_arxml_types`.
The full per-type report is generated at
`target/arxml-reference/type_inventory.json` at the workspace root. It includes
definitions, source paths and traits, Rust outputs, differences, coverage gaps,
expected normalizations,
load results, versions, and source SHA-256 hashes.

The committed [baseline](../../arxml_converter/tests/reference/inventory_baseline.json)
checks every current result through a deterministic checksum as well as explicit
counts. Passing that regression means the recorded results have not changed;
it does not turn known differences or missing coverage into successful matches.
`ARXML_REQUIRE_FULL_MATCH=1` enables a stricter gate and currently fails on the
documented nonmatching rows.

The original [validation plan](arxml_validation_plan.md) describes the separate
payload-decoding and output-adaptation work that remains.

## Reusable Decode Layouts

`ArxmlCodec` compiles immutable payload plans when loading CP/AP message entries.
Aliases, scoped references, primitive kinds, field order, and fixed array lengths
are resolved before repeated decoding. Runtime execution only traverses the plan
and constructs output values. Cached entry errors do not reject unrelated
messages at load time; unsupported nested layouts still fail when visited.
Existing variable-array truncation, vector errors, empty requests, fixed strings,
and zero-count child behavior remain unchanged.

`Decoder::compile` exposes an owned `DecodePlan` for callers with repeated
payloads. Plans do not borrow a mutable registry and remain snapshots if the
source registry changes. `Decoder::decode` compiles from its supplied registry
on each invocation, preserving the public stateless API without a global cache.
Plan compilation adds load-time work and retained memory per configured message;
plans share referenced nodes within each entry, not across entries.

See [decode performance validation](arxml_decode_performance.md) for the local
release comparison and reproduction commands.
