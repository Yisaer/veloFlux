# Timestamp Type

Status: supported.

`timestamp` represents an absolute point in time. Values are normalized to UTC internally so
comparison, grouping, hashing, and encoding do not depend on local timezone settings.

## Schema Syntax

The initial schema syntax is intentionally minimal:

```json
{
  "name": "event_time",
  "data_type": "timestamp"
}
```

No column-level `props` are required in the initial version. Future versions may add
`props.decode` for per-column decode options such as Unix epoch units or custom string formats.

## Internal Representation

The recommended internal representation is signed microseconds since Unix epoch:

```rust
TimestampValue {
    epoch_micros: i64
}
```

This representation keeps equality, hashing, comparison, and arithmetic boundaries stable. It also
matches the current value model, where values must behave predictably in hash-based operators.

## JSON Decode Semantics

The initial JSON decoder should accept RFC3339 strings only.

Accepted examples:

- `2026-05-08T10:20:30Z`
- `2026-05-08T18:20:30+08:00`
- `2026-05-08T10:20:30.123456Z`

Decode rules:

- The JSON value must be a string.
- The string must include timezone information through `Z` or an explicit offset.
- The decoded value is normalized to UTC and stored as epoch microseconds.
- Missing fields decode to `NULL`.
- Non-string values decode to `NULL`.
- Invalid RFC3339 strings decode to `NULL`.

Unsupported in the initial version:

- JSON numbers such as Unix epoch seconds, milliseconds, or microseconds.
- Strings without timezone information, such as `2026-05-08 10:20:30`.
- Custom timestamp formats.
- Per-column decode options.

## JSON Encode Semantics

JSON encoding should emit RFC3339 UTC strings. The output precision should not exceed microseconds,
because the planned internal representation stores microseconds.

Example:

```json
{
  "event_time": "2026-05-08T10:20:30.123456Z"
}
```

## Cast Semantics

The initial cast surface should stay narrow:

- `cast(string, 'timestamp')` accepts the same RFC3339 strings as JSON decode.
- `cast(timestamp, 'string')` emits an RFC3339 UTC string.
- Numeric casts to timestamp are not supported until an explicit unit is introduced.

## Comparison Semantics

Timestamp values compare by their normalized epoch microsecond value.

Supported initial comparisons:

- `=`
- `!=`
- `<`
- `<=`
- `>`
- `>=`

Timestamp arithmetic is intentionally out of scope until an `interval` type is introduced.

## Future Extensions

Possible future extensions:

- Column-level decode options through `props.decode`.
- Unix epoch numeric formats with explicit units such as `unix_s`, `unix_ms`, or `unix_us`.
- SQL-like timestamp strings with an explicit default timezone policy.
- `date`, `time`, and `interval` types.
- Timestamp arithmetic with interval values.
