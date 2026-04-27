# Task Note
Updated: 2026-04-27 08:25 UTC

## Goal
- Deliver: Fix memory-safe incremental indicator and derived-indicator materialization after profile additions

## Change Surface
- Surface: product-plane
- Rationale: the change is limited to research/data materialization behavior, Dagster product-plane assets, tests, and operator-facing research docs. Shell control-plane behavior is not part of the product logic change.

## Solution Intent
- Solution Class: target
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: scaffold-only, sample artifact, synthetic upstream
- Closure Evidence: integration test evidence must prove Delta schema extension, partition replacement, downstream research indicator profile extension, derived profile extension, and derived reuse after unrelated base-indicator addition; loop gate must pass on changed files.
- Shortcut Waiver: none
- target: make nightly research materialization memory-safe by passing lightweight Delta table summaries between Dagster assets instead of multi-million-row payloads.
- target: make indicator and derived-indicator additions incremental at the column/partition level: reuse persisted values when source data and existing output columns are still valid, compute only missing/new columns, and rewrite only the affected Delta partitions.
- staged: keep correctness conservative for source-data changes and same-column formula changes; those still refresh the affected partition because prior values may be stale.
- fallback: if Delta schema evolution cannot safely merge new columns in append mode, fail with a clear materialization error instead of silently dropping new columns.

## Task Request Contract
- Objective: remove full-table memory pressure from the research indicator/derived-indicator refresh path and make adding new output columns reuse prior wide-table values where source data is unchanged.
- In Scope: research indicator materialization/store, derived-indicator materialization/store, Delta runtime schema/batch helpers, Dagster research asset payload shape, focused tests, and research operations docs if behavior changes.
- Out of Scope: changing canonical MOEX generation, introducing a separate feature layer, changing trading/domain logic, or running a full production rewrite of `research/gold/current` in this task.
- Constraints: preserve PR-only main policy, keep domain logic inside product-plane surfaces, avoid full real-data rewrites during verification, and prefer bounded real/proxy checks over memory-heavy all-table scans.
- Done Evidence: focused product-plane tests for incremental reuse, Delta helper tests for partition replacement/schema evolution, and a lightweight gate or targeted validator run.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Session started and baseline scope captured.
- Dagster research data-prep assets now pass Delta table summaries/counts instead of row payloads for indicator and derived layers.
- Materializers now use metadata-first freshness, partition delete+append writes, schema merge, and column-extension reuse.
- Derived no-op checks avoid loading base indicator rows unless a partition actually needs refresh.
- Added regression coverage for Delta schema extension, profile extension without recomputing existing columns, derived reuse after unrelated base-indicator additions, and derived profile extension.

## First-Time-Right Report
1. Confirmed coverage: objective and acceptance path are explicit.
2. Missing or risky scenarios: unknown integrations and policy drifts.
3. Resource/time risks and chosen controls: phased patches and deterministic checks.
4. Highest-priority fixes or follow-ups: stabilize contract and validation first.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: same failure repeats after two focused edits.
- Reset Action: pause edits, capture failing check, and reframe the approach.
- New Search Space: validator contract, routing logic, and docs alignment.
- Next Probe: run the smallest failing command before next patch.

## Task Outcome
- Outcome Status: completed
- Decision Quality: correct_after_replan
- Final Contexts: CTX-RESEARCH, CTX-DATA, CTX-ORCHESTRATION, CTX-OPS
- Route Match: matched
- Primary Rework Cause: none
- Incident Signature: none
- Improvement Action: workflow
- Improvement Artifact: research indicator/derived materialization tests and research operations runbook update.

## Blockers
- No blocker.

## Next Step
- Implement focused patch and rerun loop gate.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `py -3.11 -m pytest tests/product-plane/unit/test_delta_runtime.py tests/product-plane/unit/test_research_indicator_layer.py tests/product-plane/unit/test_research_derived_indicator_layer.py tests/product-plane/unit/test_research_dagster_manifests.py tests/product-plane/integration/test_research_indicator_materialization.py tests/product-plane/integration/test_research_vectorbt_backtests.py -q` -> PASS, 31 tests.
- `py -3.11 scripts/validate_task_request_contract.py` -> PASS.
- `py -3.11 scripts/validate_session_handoff.py` -> PASS.
- `py -3.11 scripts/validate_solution_intent.py --from-git --git-ref HEAD` -> PASS.
- `py -3.11 scripts/validate_critical_contour_closure.py --from-git --git-ref HEAD` -> PASS.
- `py -3.11 scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none` -> PASS.
