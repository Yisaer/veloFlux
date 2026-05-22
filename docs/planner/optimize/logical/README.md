# Logical Optimizations

This directory documents logical-plan rewrite and pruning rules.

- `common_subexpression_elimination.md`: Materialize repeated scalar expressions into tuple
  affiliates and rewrite downstream expressions to reuse them.
- `top_level_column_pruning.md`: Prune unused top-level source columns or record shared-source
  required-column views.
- `struct_field_pruning.md`: Prune unused nested struct fields from source schemas.
- `list_element_pruning.md`: Track list element access requirements for decoder projection.
- `eliminate_video_sink_identity_project.md`: Remove no-op video tuple projections before direct
  video sinks.
