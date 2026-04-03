---
name: data-engineer
description: Design and implement reliable data pipelines with idempotent transforms and quality gates for Spark, Delta, DuckDB, and dbt oriented stacks.
classification: KEEP_CORE
wave: WAVE_2
status: ACTIVE
owner_surface: CTX-DATA
scope: data pipeline design, reliability controls, and quality gating
routing_triggers:
  - "data pipeline"
  - "etl"
  - "elt"
  - "spark"
  - "delta lake"
  - "duckdb"
  - "dbt"
  - "data quality"
---

# Data Engineer

## Purpose
Design and deliver data pipelines that are idempotent, testable, and operationally reliable.

## Fit For This Repository
- Prioritize patterns that match current stack surfaces: Spark, Delta, DuckDB, dbt.
- Keep streaming and message-bus complexity optional, not default.
- Treat schema and contract stability as first-order quality requirements.

## Core Delivery Rules
1. Pipelines must be idempotent.
2. Schema evolution must be explicit and observable.
3. Null handling must be intentional on critical fields.
4. Quality checks must execute before downstream publication.
5. Every dataset change must have lineage and ownership clarity.

## Workflow
1. Define source contract and target contract:
   - grain, keys, freshness expectations, allowed nulls.
2. Choose processing mode:
   - batch, incremental, or CDC based on source behavior.
3. Implement layered data flow:
   - raw capture;
   - cleaned and conformed layer;
   - consumer-ready layer.
4. Add quality gates:
   - uniqueness, null-rate, value-range, freshness, referential checks.
5. Add observability:
   - run status, row counts, failure reason, data drift signals.
6. Define backfill and rollback procedure before rollout.

## Stack Guidance

### Spark and Delta
- Use deterministic merge/upsert semantics for incremental updates.
- Keep partitioning and file management aligned to query patterns.
- Avoid hidden schema drift; fail or alert with clear diagnostics.

### DuckDB
- Use fast local validation and profiling for changed transforms.
- Keep repeatable SQL checks as part of pipeline verification.

### dbt
- Use model contracts and data tests where model ownership exists.
- Enforce key constraints and freshness expectations on serving layers.

### Not Default Unless Requested
- Kafka or other message-bus orchestration.
- New cloud-specific platform lock-in.
- Full real-time architecture when batch or micro-batch meets SLA.

## Output Contract For Handoff
Handoff should include:
1. source-to-target mapping and key assumptions;
2. transform stages and idempotency approach;
3. quality gates and observed results;
4. operational risks, backfill plan, and rollback boundary.

## Skill Traces (Conditional Co-Use)
1. New source onboarding or canonical mapping:
   - `source-onboarding`
2. Contract or schema touched:
   - `registry-first`
3. Cross-layer consistency checks needed:
   - `validate-crosslayer`
4. Test amplification for changed transforms:
   - `testing-suite`
5. Dependency or connector package change:
   - `dependency-and-license-audit`

## Boundaries
This skill should NOT:
- add complex streaming topology without explicit requirement;
- treat docs-only updates as pipeline readiness proof;
- skip quality gates because data "looks fine" on small samples.

