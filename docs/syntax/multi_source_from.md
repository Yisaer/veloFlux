# Multi-source FROM

Comma-separated FROM inputs use per-event evaluation in VeloFlux:

```sql
SELECT faults.ts AS fault_ts, messages.ts AS message_ts, payload
FROM faults, messages
```

Each evaluation handles one input event. Its source contributes one row; the other
sources contribute one NULL placeholder row each. This can be understood as the
product of the current row and the placeholder rows. A NULL placeholder row is not
an empty table: a standard relational Cartesian product with an empty input has
no rows. VeloFlux explicitly supplies the placeholder for this streaming dialect.

No historical or latest row from another source is paired with the event. Each
input event contributes one evaluation, including duplicates. Collections are
processed row by row with the same semantics. Every query, including a single-source query, assigns processing
time when it consumes a collection at its common input boundary; its rows share that timestamp.
This time describes query processing, not the source decode time or event time.

This behavior differs from standard SQL comma FROM over relations. Explicit
stream-table lookup JOIN retains its existing matching and lookup semantics.

## Field identity and types

Columns retain the identity `(source, column)`; matching column names are not
merged. Same-name fields from different sources may have different types.

- Qualify any field present in multiple input schemas, such as `faults.ts`.
- Unique names, such as `payload`, may be unqualified.
- Ambiguity is determined against all input schemas, never the current event.
- A valid reference to an absent source returns NULL.
- Unknown sources, unknown columns, and ambiguous references fail planning.
- A runtime layout mismatch remains an error; it is not converted to NULL.
- Cross-source expressions still obey the existing expression type rules.

For example, `faults.ts` can be an integer while `messages.ts` is a string. Select
both using distinct output aliases. Existing sink restrictions on duplicate output
names still apply. JSON encoding can omit NULL columns according to its encoder
configuration; that does not change the internal NULL values.

## Conditional collection

```sql
SELECT messages.ts, payload
FROM faults, messages
WHERE msg_type IN ('X', 'Y', 'Z')
GROUP BY slidingwindow('ss', 0, 120)
    OVER (
        WHEN fault_code = 'A'
        PARTITION BY coalesce(faults.device_id, messages.device_id)
    )
```

The device keys must have compatible types. A fault event creates its device's
window; a message event supplies the message-side device key. WHERE filters the
window output and must not remove fault events before the trigger is evaluated.
Each matching fault creates an independent window using existing sliding-window
boundaries and emission behavior.

## Planning and execution

Each source retains its own sampler and decoder. Column requirements from
projections, predicates, windows, stateful expressions, and partition keys are
collected per source and retain existing decoder whitelist optimizations.

Top-level physical planning prepares one query input layout before recursive plan
construction. Each source is mapped into this fixed message-slot layout through
CollectionLayoutNormalize where needed; an identity mapping reuses its existing
message storage. Actual source messages are reused through Arc; other sources'
NULL messages are built once and shared across events. Alignment consumes owned
tuples and constructs the final Arc slot array through copy-on-write, without a
temporary Vec. Dense alignment still allocates one slot array and copies O(input
count) Arc references per row; payload values remain shared. It does not scan column
names or deep-copy payloads per event. Native memory inputs still use their
existing normalization before source-slot alignment.

A query input Barrier combines one or more data inputs, assigns processing time,
and aligns control signals before downstream scalar or window evaluation. This boundary
reads the clock once per collection and prevents assigned timestamps from moving
backward if the system clock is adjusted. Existing Barrier nodes retain their
original timestamp behavior. Single-input boundaries can fuse with adjacent engine
stages without adding a channel; multi-input and shared edges retain execution region
boundaries. EXPLAIN identifies each alignment branch with `mode=source_alignment`
and its source-slot order. No new stream resource, connector, memory topic,
forwarding pipeline, or MergeStage is needed.

Shared inputs reuse their existing reading, decoding, stable slots, and consumer
projection registration. A row-only shared input retains one source field because
the existing shared runtime interprets an empty requirement as full decoding.
Other direct consumers still contribute to the shared decoding union. The same
row-only field-requirement rule applies to single-source shared queries. Fixed-schema
decoders such as file_line retain their required decoder schema.

## Current scope

- Comma FROM accepts multiple registered streams; relation aliases remain unsupported.
- Each stream's sampler keeps its existing behavior before alignment. Sampling one
  source does not sample events from another source.
- Multi-source event-time pipelines are rejected. Only processing-time evaluation
  is currently supported. Source tuple timestamps are replaced at fan-in; payload
  timestamp fields are unchanged. Queued events therefore use their consumption
  time in this query. No event-time sorting is provided.
- Explicit JOIN is separate and unchanged. Multi-table scans are not added.
- Pipeline chaining uses existing memory source/sink. Automatically deriving those
  intermediate schemas is tracked separately in issue 452.

## Checkpoint scope

The input boundary reuses the data-channel checkpoint alignment protocol: pause an
input after its checkpoint, wait for every input, forward one checkpoint, and only
then resume post-checkpoint data. Single-input boundaries forward the checkpoint
in order and may execute inline. Timestamp assignment does not modify control signals.

Two-file-input coverage verifies graceful final checkpoint, both source cursor
snapshots, append while stopped, and restart without replaying committed file data.
Unfenced source connectors still fail checkpoint shutdown without committing a
manifest. This does not add window, aggregation, or sampler state snapshots; see
[Checkpoint](../runtime/checkpoint.md) for the existing recovery limitations.
