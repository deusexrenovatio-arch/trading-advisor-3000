# Task Note
Updated: 2026-03-27 00:18 UTC

## Goal
- Deliver: Re-open and continue governed module phase 05 (D3 - Dagster Execution Closure) for stack-conformance-remediation until Dagster materialization is asset-level rather than side-effect-shaped.

## Task Request Contract
- Objective: replace asset-spec-only Dagster evidence with executable definitions and a materialization proof path for the agreed phase2a canonical slice.
- In Scope: `src/trading_advisor_3000/dagster_defs/*`, phase-scoped Dagster proof script/tests, and supporting architecture/runbook/checklist/stack-conformance updates tied to D3.
- Out of Scope: phase-06+ runtime/API/transport/sidecar work, broad orchestration redesign, and non-phase closures.
- Constraints: no silent assumptions/skips/fallbacks/deferrals; keep patch reviewable and phase-scoped; keep shell surfaces free of domain logic.
- Done Evidence: Dagster definitions load in tests, materialization executes for selected assets and emits expected Delta outputs, disprover fails closed, and stack/docs validators stay green.
- Priority Rule: quality and governed-route integrity over speed.

## Current Delta
- Previous phase-05 acceptance is reopened because `raw_market_backfill` still writes the full Delta table set through `run_sample_backfill(...)`.
- Downstream Dagster assets mostly read those side effects instead of materializing their own outputs.
- D2 remains the accepted foundation for Spark execution; the reopen is specific to Dagster closure semantics in D3.
- Phase-05 now needs asset-level closure evidence where each Dagster asset materially owns its own output path and the disprover catches regression back to side-effect-only orchestration.

## Solution Intent
- Solution Class: target
- Critical Contour: none
- Forbidden Shortcuts: metadata-only asset declarations, narrative-only orchestration claims
- Closure Evidence: `raw_market_backfill` may materialize only the raw Delta table, while each canonical Dagster asset must compute from upstream data and materialize its own Delta output so that selection/materialization semantics stay honest.
- Shortcut Waiver: none
- Design Checkpoint: keep closure on the phase2a canonical slice already used in D1/D2, but remove the hidden "one asset writes everything" execution model before asking acceptance to pass again.

## First-Time-Right Report
1. Confirmed coverage: the gap is not importability but asset ownership, so this retry focuses on honest per-asset materialization for the phase2a slice.
2. Missing or risky scenarios: Dagster can still look green while silently collapsing back to a Python pipeline with side effects.
3. Resource/time risks and chosen controls: keep the deterministic fixture/profile, but strengthen proof so partial selection cannot piggyback on hidden writes from `raw_market_backfill`.
4. Highest-priority fixes or follow-ups: refactor D3 so canonical assets own their own Delta writes, then rerun governed phase-05 before reopening R1/R2.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: if asset-level materialization still collapses into hidden side effects after one more worker cycle, stop scope expansion and remediate the Dagster proof model before touching R1/R2.
- Reset Action: capture the exact selection/materialization behavior, then align Dagster asset boundaries with the Delta ownership contract before retry.
- New Search Space: per-asset Delta writes, partial selection behavior, and disprovers that fail when one asset secretly writes the entire table set.
- Next Probe: rerun the governed worker on reopened phase-05 with the asset-level closure gap as the primary blocker.

## Task Outcome
- Outcome Status: in_progress
- Decision Quality: pending
- Final Contexts: CTX-DATA, GOV-RUNTIME, ARCH-DOCS
- Route Match: matched
- Route Signal: re-opened-for-remediation
- Primary Rework Cause: previous D3 acceptance depended on side-effect-shaped Dagster materialization rather than honest asset-level closure.
- Incident Signature: none
- Improvement Action: dagster-asset-materialization-hardening
- Improvement Artifact: reopened phase-05 route with stricter asset-ownership evidence and disprover expectations.

## Blockers
- Reopen reason: current Dagster closure still allows `raw_market_backfill` to write the full phase2a Delta table set through `run_sample_backfill(...)`, which makes downstream assets consumers of side effects instead of owners of their own materializations.
- Acceptance condition for this retry: phase-05 must prove that canonical assets materialize their own outputs and that partial selection cannot pass via hidden writes from another asset.

## Next Step
- Execute the governed worker route on reopened phase-05, repair the Dagster asset graph, and only then hand the phase back to acceptance.

## Executed Evidence
- `python -m pytest tests/app/unit/test_phase2a_manifests.py -q` -> 4 passed
- `python -m pytest tests/app/integration/test_phase2a_dagster_execution.py -q` -> 3 passed
- `python -m pytest tests/app/integration/test_phase2a_dagster_execution.py -k metadata_only -q` -> 1 passed, 2 deselected
- `python scripts/run_phase2a_dagster_proof.py --source tests/app/fixtures/data_plane/raw_backfill_sample.jsonl --output-dir .tmp/phase2a-dagster-proof --contracts BR-6.26,Si-6.26 --output-json artifacts/phase2a-dagster-proof.json` -> proof report generated
- `python scripts/validate_docs_links.py --roots docs/architecture/app docs/runbooks/app docs/checklists/app` -> OK
- `python scripts/validate_stack_conformance.py` -> OK
- `python scripts/run_loop_gate.py --skip-session-check --changed-files pyproject.toml registry/stack_conformance.yaml src/trading_advisor_3000/dagster_defs/phase2a_assets.py src/trading_advisor_3000/dagster_defs/__init__.py scripts/run_phase2a_dagster_proof.py tests/app/unit/test_phase2a_manifests.py tests/app/integration/test_phase2a_dagster_execution.py docs/architecture/app/phase2a-data-plane-mvp.md docs/architecture/app/stack-conformance-baseline.md docs/architecture/app/STATUS.md docs/runbooks/app/README.md docs/runbooks/app/phase2-dagster-execution-runbook.md docs/checklists/app/phase2a-acceptance-checklist.md docs/session_handoff.md docs/tasks/active/index.yaml docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-05-for-stack-conf.md artifacts/phase2a-dagster-proof.json` -> loop gate OK (includes contract/state/architecture/session validators)
- `python scripts/run_pr_gate.py --skip-session-check --changed-files pyproject.toml registry/stack_conformance.yaml src/trading_advisor_3000/dagster_defs/phase2a_assets.py src/trading_advisor_3000/dagster_defs/__init__.py scripts/run_phase2a_dagster_proof.py tests/app/unit/test_phase2a_manifests.py tests/app/integration/test_phase2a_dagster_execution.py docs/architecture/app/phase2a-data-plane-mvp.md docs/architecture/app/stack-conformance-baseline.md docs/architecture/app/STATUS.md docs/runbooks/app/README.md docs/runbooks/app/phase2-dagster-execution-runbook.md docs/checklists/app/phase2a-acceptance-checklist.md docs/session_handoff.md docs/tasks/active/index.yaml docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-05-for-stack-conf.md artifacts/phase2a-dagster-proof.json` -> pr gate OK

## Validation
- `python -m pytest tests/app/unit/test_phase2a_manifests.py -q`
- `python -m pytest tests/app/integration/test_phase2a_dagster_execution.py -q`
- `python -m pytest tests/app/integration/test_phase2a_dagster_execution.py -k metadata_only -q`
- `python scripts/run_phase2a_dagster_proof.py --source tests/app/fixtures/data_plane/raw_backfill_sample.jsonl --output-dir .tmp/phase2a-dagster-proof --contracts BR-6.26,Si-6.26 --output-json artifacts/phase2a-dagster-proof.json`
- `python scripts/validate_docs_links.py --roots docs/architecture/app docs/runbooks/app docs/checklists/app`
- `python scripts/validate_stack_conformance.py`
- `python scripts/run_loop_gate.py --skip-session-check --changed-files pyproject.toml registry/stack_conformance.yaml src/trading_advisor_3000/dagster_defs/phase2a_assets.py src/trading_advisor_3000/dagster_defs/__init__.py scripts/run_phase2a_dagster_proof.py tests/app/unit/test_phase2a_manifests.py tests/app/integration/test_phase2a_dagster_execution.py docs/architecture/app/phase2a-data-plane-mvp.md docs/architecture/app/stack-conformance-baseline.md docs/architecture/app/STATUS.md docs/runbooks/app/README.md docs/runbooks/app/phase2-dagster-execution-runbook.md docs/checklists/app/phase2a-acceptance-checklist.md docs/session_handoff.md docs/tasks/active/index.yaml docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-05-for-stack-conf.md artifacts/phase2a-dagster-proof.json`
- `python scripts/run_pr_gate.py --skip-session-check --changed-files pyproject.toml registry/stack_conformance.yaml src/trading_advisor_3000/dagster_defs/phase2a_assets.py src/trading_advisor_3000/dagster_defs/__init__.py scripts/run_phase2a_dagster_proof.py tests/app/unit/test_phase2a_manifests.py tests/app/integration/test_phase2a_dagster_execution.py docs/architecture/app/phase2a-data-plane-mvp.md docs/architecture/app/stack-conformance-baseline.md docs/architecture/app/STATUS.md docs/runbooks/app/README.md docs/runbooks/app/phase2-dagster-execution-runbook.md docs/checklists/app/phase2a-acceptance-checklist.md docs/session_handoff.md docs/tasks/active/index.yaml docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-05-for-stack-conf.md artifacts/phase2a-dagster-proof.json`
