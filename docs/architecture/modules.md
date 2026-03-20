# Modules

## Governance Module
- Owns policy docs, ownership rules, and gate entrypoints.
- Must remain free of product business logic.

## Process Module
- Owns lifecycle, context routing, and validation orchestration.
- Enforces deterministic local/PR/nightly lanes.

## State Module
- Owns plans/memory/task-outcomes registries and sync scripts.
- Keeps canonical item-per-file plus generated compatibility outputs.

## Skills Module
- Owns local generic skills and routing policy.
- Baseline excludes domain-specialized skills.

## App Plane Module
- Owns isolated application/product-plane code and app-specific tests.
- Must not leak shell-sensitive behavior back into governance surfaces.
