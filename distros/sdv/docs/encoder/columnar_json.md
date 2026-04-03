# ColumnarJsonEncoder

Batches rows into columnar JSON with array values per column.

## Overview

Does not depend on schema; uses the columns present in incoming tuples/messages. Supports streaming accumulation with incremental JSON writing.

## Output Format

```json
{"email":["a@b.com","c@d.com"],"id":[1,2],"name":["foo","bar"]}
```

## Example

```rust
use encoder::ColumnarJsonEncoder;

let encoder = ColumnarJsonEncoder::new();
```
