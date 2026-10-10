# Plan-Fixed Output Layout

## Summary

The planner provides every sink consumer with one `OutputLayout`. Each output
column contains its SQL-visible name, datatype, and a fixed address in the
runtime `Tuple`:

```rust
pub enum OutputValueRef {
    Message {
        message_index: usize,
        value_index: usize,
    },
    Affiliate {
        affiliate_index: usize,
    },
    Null,
}
```

Encoders, row-diff, and the template encoder resolve final output values through
this layout. They do not scan messages by source name or look up computed values
by name.

## Layout Derivation

The physical planner derives the tuple layout from every operator boundary.
Decoder projection slots define message value indexes. Pass-through operators
preserve their child layout, collection normalization and materialization remap
it, and fan-in operators require compatible layouts.

Per-sink column filtering narrows only the visible output columns. It preserves
each selected column's existing fixed value reference because the upstream tuple
is not rebuilt. After row-diff, encoders, and direct collection materialization
capture the filtered layout, the planner-only filter node is removed.

Project applies these rules:

- A direct column and its aliases inherit the child's fixed value reference.
- A wildcard preserves expanded planner order and inherits each reference.
- A computed expression is stored in the ordered affiliate row and receives the
  next `Affiliate` index.
- A missing output that is valid by plan semantics uses `Null`.

Duplicate output names remain representable because output columns and values
are addressed by index. Individual output formats may reject duplicates when
their data model requires unique keys.

## Runtime Contract

`OutputValueRef::resolve` performs only indexed access. A structurally valid
missing value is represented by `Null`. An out-of-range message, message value,
or affiliate index is a planner/runtime contract violation: debug and test
builds assert, while release builds return a processing error.

There is no runtime name-scan fallback.

`AffiliateRow` maintains insertion order in parallel key and value arrays.
Overwriting a key updates its existing value slot, so every planned affiliate
index remains stable.

## Value Ownership

A `Message` owns a contiguous `Vec<Value>`. Its constructors accept owned
values, and indexed entry APIs borrow `&Value`. Shared messages and tuple message
lists retain their existing `Arc` ownership, so direct SQL projections and
window buffering share whole messages without copying cells.

Consumers that materialize a new message or retain selected cells independently
clone the selected `Value`s. Numeric values are copied inline. Strings share an
`Arc<String>` payload; bytes share `Arc<Bytes>`. Lists and structs each share
a single Arc containing their items and type metadata. Cloning
these values retains their payload without copying text or nested elements.
Unselected input columns can be released without retaining the original wide
message. Constructing a complex payload adds one Arc allocation, while numeric
cells still require no individual allocation.

`ListValue::new` and `StructValue::new` move their input vectors into shared
ownership, and their read APIs continue borrowing items. `StructValue::into_parts`
moves a uniquely owned vector or shallow-clones a shared item container; changing
the returned vector does not change other structs. Content equality and hashing
are unchanged. String readers borrow `&str`; owning external APIs such as JSON
DOM conversion copy text at that boundary.

On 64-bit targets, dense fixed slots use 16 bytes per `Value`, including Null.
All heap-backed variants use a single pointer, while numeric values remain inline.
This reduces slot storage by 60% from the previous 40-byte representation. Wide,
mostly-null messages can still use more slot memory than the historical 8-byte
shared `Arc<Value::Null>` slots. Sparse slot storage is deferred.

Constructing a bytes value adds one Arc allocation around the existing Bytes
without copying its backing buffer. Readers borrow bytes; boundaries that require
an owned Bytes clone the inner Bytes and retain its backing storage.

## Consumers

- JSON, protobuf, and SDV columnar encoders iterate `OutputLayout.columns` and
  resolve each column's fixed reference.
- Row-diff tracks output indexes from its input layout and materializes a dense
  diff row with `NULL` for unchanged tracked columns plus an output mask for
  changed columns.
- Template transforms receive the dense diff row. The output mask is not
  exposed to the initial template input contract.
- Project preserves input messages and materializes only computed expressions
  into the affiliate row.
- Direct collection sinks materialize a filtered layout at the final sink
  boundary because their connectors do not consume `OutputLayout` directly.

The former `ByIndexProjection`, `late_projection`, and
`passthrough_messages` optimization paths were removed. Fixed value addressing
is a planner contract shared by all consumers rather than an encoder-specific
rewrite.

## Validation

Planner tests cover decoder slot ordering, direct and computed projections,
aliases, row-diff, batching, shared streams, and mixed sink consumers. Pipeline
tests verify full output, delta masks, null transitions, template transforms,
and memory collection materialization.
