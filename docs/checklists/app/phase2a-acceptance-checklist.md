# Phase 2A Acceptance Checklist

Date: 2026-03-26

## Route Signal
- `remediation:phase-only` (phase-05 remediation cycle, attempt 2)

## Scope Note
- This checklist is current-cycle evidence for Phase 05 (D3 Dagster Execution Closure).
- Scope stays on the agreed phase2a canonical slice and does not claim phase-06+ closure.

## Acceptance Disposition
- [x] Phase 2A remains accepted as MVP baseline slice (historical baseline decision).
- [x] Full module DoD closure remains not accepted in the current truth source (`docs/architecture/app/STATUS.md`).

## Historical Baseline Snapshot (captured 2026-03-17; reference only)
- Ingestion implements explicit incremental + append-only + idempotent raw flow with watermark and dedup evidence.
- Canonical builder emits `bars`, `instruments`, `contracts`, `session_calendar`, and `roll_map`.
- Data-plane manifest, Dagster proof path, Spark proof path, and scoped tests exist for the baseline slice.

## Deliverables
- [x] Ingestion module added.
- [x] Canonical builder added.
- [x] Data quality gate added.
- [x] Delta schema manifest added.
- [x] Dagster definitions + materialization proof path added.
- [x] Minimal Spark execution proof path added.
- [x] Sample backfill fixture added.
- [x] Integration and unit tests for Phase 2A added.

## Phase-05 Dagster Remediation Criteria (current cycle)
- [x] Dagster definitions load from executable `Definitions` with materializable asset nodes.
- [x] Materialization proof executes and produces the agreed phase2a Delta outputs.
- [x] Partial selection cannot pass via hidden side effects; only selected assets and required upstream dependencies materialize.
- [x] Metadata-only disprover fails closed and blocks static asset-spec-only substitutions.
- [x] Stack conformance and docs-link checks stay green for touched surfaces.
- [x] Loop and PR gates stay green for the corrected phase-05 changed-files scope.

## Current Phase-05 Dagster Remediation Evidence (executed 2026-03-26)
- [x] `python -m pytest tests/app/unit/test_phase2a_manifests.py -q`
- [x] `python -m pytest tests/app/integration/test_phase2a_dagster_execution.py -q`
- [x] `python -m pytest tests/app/integration/test_phase2a_dagster_execution.py -k partial_selection -q`
- [x] `python -m pytest tests/app/integration/test_phase2a_dagster_execution.py -k metadata_only -q`
- [x] `python scripts/run_phase2a_dagster_proof.py --source tests/app/fixtures/data_plane/raw_backfill_sample.jsonl --output-dir .tmp/phase2a-dagster-proof --contracts BR-6.26,Si-6.26 --output-json artifacts/phase2a-dagster-proof.json`
- [x] `python scripts/validate_docs_links.py --roots docs/architecture/app docs/runbooks/app docs/checklists/app`
- [x] `python scripts/validate_stack_conformance.py`
- [x] `python scripts/run_loop_gate.py --skip-session-check --changed-files pyproject.toml registry/stack_conformance.yaml src/trading_advisor_3000/dagster_defs/phase2a_assets.py src/trading_advisor_3000/dagster_defs/__init__.py scripts/run_phase2a_dagster_proof.py tests/app/unit/test_phase2a_manifests.py tests/app/integration/test_phase2a_dagster_execution.py docs/architecture/app/phase2a-data-plane-mvp.md docs/architecture/app/stack-conformance-baseline.md docs/architecture/app/STATUS.md docs/runbooks/app/README.md docs/runbooks/app/phase2-dagster-execution-runbook.md docs/checklists/app/phase2a-acceptance-checklist.md docs/session_handoff.md docs/tasks/active/index.yaml docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-05-for-stack-conf.md artifacts/phase2a-dagster-proof.json`
- [x] `python scripts/run_pr_gate.py --skip-session-check --changed-files pyproject.toml registry/stack_conformance.yaml src/trading_advisor_3000/dagster_defs/phase2a_assets.py src/trading_advisor_3000/dagster_defs/__init__.py scripts/run_phase2a_dagster_proof.py tests/app/unit/test_phase2a_manifests.py tests/app/integration/test_phase2a_dagster_execution.py docs/architecture/app/phase2a-data-plane-mvp.md docs/architecture/app/stack-conformance-baseline.md docs/architecture/app/STATUS.md docs/runbooks/app/README.md docs/runbooks/app/phase2-dagster-execution-runbook.md docs/checklists/app/phase2a-acceptance-checklist.md docs/session_handoff.md docs/tasks/active/index.yaml docs/tasks/active/TASK-2026-03-26-continue-governed-module-phase-05-for-stack-conf.md artifacts/phase2a-dagster-proof.json`

## Historical Reference (not current cycle evidence)
- Phase-04 Spark remediation evidence remains in the phase-04 task note and orchestration artifacts.
- It is preserved as context only and is not used as phase-05 closure proof.

## F1 Re-Acceptance Snapshot (2026-03-27)
- [x] Checklist wording remains aligned with truth-source precedence (`docs/architecture/app/STATUS.md`) and restricted acceptance vocabulary.
- [x] Phase evidence maps to stack-conformance registry entries `delta_lake`, `apache_spark`, and `dagster` with current class `partial`.
- [x] Negative-test evidence remains explicit (`metadata_only`, `partial_selection`, and physical-output disprovers).
- [x] This checklist remains baseline evidence only and does not claim full target-architecture closure.
