# VeloFlux Planner Performance Optimization

This document tracks the effort and results for optimizing VeloFlux's pipeline initialization and restoration performance, specifically for large industrial schemas (e.g., DBC with thousands of signals).

## Summary

By eliminating **O(N²)** complexity in the SQL logical and physical planners and pathing rule assembly, rule restoration time was reduced by **~58x**.

| Metric | Baseline (v0.4.0) | Optimized (Release) | Speedup |
|--------|-------------------|---------------------|---------------|
| **Total Rule Restore** | **~3,350ms** | **58ms** | **~58x** |
| - SQL Parsing | ~20ms | 16ms | 1.2x |
| - Logical Planning | **~2,300ms** | **4ms** | **~575x** |
| - Physical Planning | **~700ms** | **23ms** | **~30x** |
| - Processor Assembly| **~350ms** | **12ms** | **~30x** |

> [!NOTE]
> Baseline figures are estimated based on logs from a yupik DBC schema snapshot containing ~5,000 signals.

## Optimization Details

### 1. Logical Planner (O(N) -> O(1))
- **Column Lookup**: Replaced linear scans in `resolve_column_datatype` with a pre-indexed `HashMap<&str, ConcreteDatatype>`. This reduces type resolution from O(N) per column to O(1).
- **Alias Validation**: Replaced nested loops in `validate_select_alias_names` with a `HashSet<&str>`, reducing uniqueness checks from O(N^2) to O(N).

### 2. Physical Planner (O(N) -> O(1))
- **Index Resolution**: Leveraged `Schema::column_index` (which uses internal indexing) instead of O(N) linear scans in `find_column_index`.

### 3. Rule Assembly & Cloning (O(N) -> O(1))
- **Arc-wrapped Fields**: Changed `PhysicalProject::fields` from `Vec<PhysicalProjectField>` to `Arc<[PhysicalProjectField]>`.
- **Zero-Copy**: This makes the cloning of projection nodes (extremely frequent during multi-pass optimization and assembly) an O(1) pointer copy instead of an O(N) deep-clone of thousands of `ScalarExpr` objects.

## Benchmark Configuration

- **Environment**: Linux (Ubuntu), Release Build
- **Schema**: Yupik DBC (approx. 5,000 signals)
- **Pipeline**: `SELECT * FROM spiStream`
- **Measurement**: Internal timing logs in `build_pipeline_runtime`.

## Known Constraints & Debug Performance

While release performance is **58ms**, debug builds are significantly slower (**~300ms**) due to:
1. **Explain Log Overhead**: Formatting 5,000-column schema snapshots into strings in `debug_assertions` blocks.
2. **Object Creation**: Baseline cost of creating 5,000 Arc objects and mapping entries in Rust's unoptimized runtime.
