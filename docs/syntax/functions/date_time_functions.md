# Date And Time Functions

This document defines the first timestamp-oriented scalar functions exposed by StreamDialect.

## Scope

The initial date and time function surface is intentionally based on the existing `timestamp`
datatype only. `timestamp` values are stored as UTC Unix epoch microseconds, so all functions in
this document use UTC semantics.

The initial surface does not introduce separate `date`, `time`, or `interval` datatypes. Functions
that return date-only or time-only values return strings with stable formats.

## Current Time Functions

- `now()` returns the current UTC timestamp.
- `current_timestamp()` is an alias of `now()`.
- `cur_date()` returns the current UTC date as `YYYY-MM-DD`.
- `current_date()` is an alias of `cur_date()`.
- `cur_time()` returns the current UTC time as `HH:MM:SS.ffffff`.
- `current_time()` is an alias of `cur_time()`.

`now()` and `current_timestamp()` are evaluated when a row is processed. They are not currently
statement-stable across a full query or batch.

## Formatting

`format_time(timestamp, format)` formats a UTC timestamp with chrono strftime patterns.

Example:

```sql
SELECT format_time(event_time, '%Y-%m-%d %H:%M:%S') FROM s
```

If either argument is `NULL`, the result is `NULL`.

## Timestamp Part Extraction

The following scalar functions extract UTC fields from a timestamp:

- `day_name(timestamp) -> string`: English weekday name.
- `day_of_month(timestamp) -> int64`: day of month, from 1 to 31.
- `day(timestamp) -> int64`: alias of `day_of_month`.
- `day_of_week(timestamp) -> int64`: weekday number, using Sunday=1 through Saturday=7.
- `day_of_year(timestamp) -> int64`: day of year, from 1 to 366.
- `hour(timestamp) -> int64`: hour, from 0 to 23.
- `microsecond(timestamp) -> int64`: microsecond component, from 0 to 999999.
- `minute(timestamp) -> int64`: minute, from 0 to 59.
- `month(timestamp) -> int64`: month number, from 1 to 12.
- `month_name(timestamp) -> string`: English month name.
- `second(timestamp) -> int64`: second, from 0 to 59.

If the timestamp argument is `NULL`, the result is `NULL`.

## Calendar Helpers

`last_day(timestamp)` returns the last day of the timestamp's UTC month as `YYYY-MM-DD`.

Example:

```sql
SELECT last_day(event_time) FROM s
```

## Unix Time Conversion

`from_unix_time(seconds)` converts Unix epoch seconds to a UTC timestamp.

The initial version accepts integer seconds only. Millisecond and microsecond inputs should use a
future explicit-unit function or option rather than overloading this function implicitly.

Example:

```sql
SELECT from_unix_time(1778505255) FROM s
```

## Deferred Semantics

The following date/time areas are intentionally deferred:

- local timezone semantics for `local_time` and `local_timestamp`
- month/year arithmetic for `date_calc`
- calendar-boundary difference semantics for `date_diff`
- MySQL-compatible `from_days`
