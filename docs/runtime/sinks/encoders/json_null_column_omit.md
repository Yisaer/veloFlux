# JSON Null Column Omission

This document records the planned JSON encoder behavior for omitting top-level `null` columns from
encoded JSON row objects.

## Background

The current JSON encoder emits a JSON object for each SQL output row.

For normal full-output rows, every output column is currently serialized:

- non-`null` values become normal JSON fields
- `null` values become `"field": null`

For some downstream systems, this is not the preferred payload shape.
Those systems treat a missing field as the desired representation for an unset optional value, and
they do not want top-level `null` fields to be emitted by default.

At the same time, row-diff output already has a separate sparse-output contract driven by
`output_mask`.
That contract must continue to distinguish:

- a field that is unchanged and should be omitted
- a field that changed to `null` and must still be emitted as `"field": null`

## Decision

We will add a JSON-encoder-local option:

- `omit_null_columns`

This option controls whether the JSON encoder omits top-level row-object fields whose value is
`null`.

Default:

- `omit_null_columns = true`

This is an encoder-local formatting rule, not a sink output rule and not a connector rule.

## Configuration Shape

The option lives under `encoder.props`.

Example:

```json
{
  "encoder": {
    "type": "json",
    "props": {
      "omit_null_columns": true
    }
  }
}
```

Disable the behavior explicitly:

```json
{
  "encoder": {
    "type": "json",
    "props": {
      "omit_null_columns": false
    }
  }
}
```

Why `encoder.props`:

- the behavior is specific to the JSON encoder
- it changes encoder-owned JSON object formatting
- it should not be modeled under `sink.output`, which is reserved for sink-branch semantics such as
  row diff and empty-result suppression

## Semantics

### Full Output Rows

For normal JSON object encoding without `output_mask`:

- if `omit_null_columns = true`, a top-level column whose runtime value is `null` is omitted
- if `omit_null_columns = false`, the column is emitted as `"field": null`

Example input row:

```json
{"a": 1, "b": null}
```

Encoded result with `omit_null_columns = true`:

```json
{"a": 1}
```

Encoded result with `omit_null_columns = false`:

```json
{"a": 1, "b": null}
```

### Row-Diff Output With `output_mask`

This option must not weaken the existing row-diff contract.

For JSON object encoding on row-diff branches:

- `output_mask = false` for a column means the field is omitted
- `output_mask = true` for a column means the field is emitted
- if `output_mask = true` and the field value is `null`, the encoder must still emit
  `"field": null`

So `omit_null_columns` does **not** suppress a mask-selected `null` field.

This preserves the distinction between:

- unchanged field: omitted because the mask does not select it
- changed-to-`null` field: emitted as `"field": null`

### With `encoder.transform=template`

For the first iteration, `omit_null_columns` only applies to the JSON encoder's native object
encoding path.

It does **not** automatically rewrite the output of `encoder.transform=template`.

Reason:

- template output structure is owned by the template itself
- the rendered JSON item may not even be an object
- row-diff + template currently uses a dense `.row` contract and does not yet expose
  mask-aware field omission semantics

If template-aware null omission is needed later, it should be designed explicitly as part of a
template contract extension, not inferred from the plain object-encoding path.

## Compatibility Notes

This is a visible default behavior change for normal full-output JSON encoding:

- before: top-level `null` columns were emitted as `"field": null`
- after: top-level `null` columns are omitted by default

However, the following behavior remains unchanged:

- row-diff sparse omission continues to be driven by `output_mask`
- changed-to-`null` on row-diff branches continues to emit `"field": null`
- template rendering semantics remain unchanged in the first iteration

## Planned Implementation Shape

The intended implementation direction is:

1. Parse `omit_null_columns` from `SinkEncoderConfig` / `encoder.props`, defaulting to `true`.
2. Apply the omission rule in the JSON encoder's native row-object materialization helpers.
3. Keep row-diff mask-aware helpers explicit so they still emit mask-selected `null` fields.
4. Leave `encoder.transform=template` behavior unchanged in the first iteration.
