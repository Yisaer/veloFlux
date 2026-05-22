# Planner Optimization Docs

Optimization documents are split by phase:

- `logical/`: SQL-to-logical-plan rewrite and pruning rules, including video sink identity-project
  elimination.
- `physical/`: Physical-plan rewrites and delayed-materialization rules.
