# Task Note
Updated: 2026-03-25 15:26 UTC

## Goal
- Deliver: normalize the governed intake of the explicit stack-conformance remediation package into a canonical execution contract and module phase plan.

## Package Intake
- Package Type: single zip source package, not an already-clean specification.
- Manifest Suggested Primary: validation verdict
- Selected Primary: corrective technical assignment
- Selection Rule: the manifest produced a top-score tie across multiple substantive markdown documents, so the primary was resolved explicitly by preferring the directive execution source that defines objective, scope, hard rules, deliverables, and success criteria over verdict, matrix, or appendix documents.
- Supporting Documents: the stack conformance matrix, phase plan, anti-self-deception controls, backlog, registry template, evidence template, red-team checklist, and evidence map from the same extracted package root.
- Mode Hint: `auto`, normalized to module-path continuation after contract creation.

## Task Request Contract
- Objective: convert the phase-driven remediation package into explicit governed contract and phase state without silently implementing multiple remediation phases in one intake patch.
- In Scope: `docs/codex/contracts/stack-conformance-remediation.execution-contract.md`, `docs/codex/modules/stack-conformance-remediation.parent.md`, the phase briefs under `docs/codex/modules/stack-conformance-remediation.phase-*.md`, and this active task note.
- Out of Scope: product-plane runtime implementation, stack-conformance registry delivery, validator implementation, CI lane expansion, ADR decisions, and any Delta/Spark/Dagster/Postgres/sidecar closure work.
- Constraints: do not call the governed launcher again; keep `docs/session_handoff.md` as a lightweight pointer shim; use canonical gate names only; preserve the package's atomic phase order; keep governance and future product patches separated; do not move trading logic into shell control-plane files.
- Done Evidence: the canonical execution contract and all declared phase briefs exist under `docs/codex/`; `python scripts/validate_task_request_contract.py`; `python scripts/validate_session_handoff.py`; `python scripts/run_loop_gate.py --from-git --git-ref HEAD`.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Read the required repo docs, the handoff pointer shim, the package manifest, the suggested primary document, and only the supporting package documents needed to resolve routing and phase shape.
- Classified the package as phase-driven because the corrective assignment and phase plan decompose remediation into atomic merge phases with per-phase acceptance and disprover rules.
- Resolved the manifest tie explicitly instead of silently accepting the suggested verdict document as execution primary.
- This run is limited to module-path normalization and phase planning, so no multi-phase remediation implementation is being folded into the intake patch.

## First-Time-Right Report
1. Confirmed coverage: the package objective, hard rules, acceptance vocabulary, phase order, and disprover model are explicit enough to normalize into the governed module path without extra operator input.
2. Missing or risky scenarios: the manifest suggestion and the best execution source diverged because of a ranking tie, and later phases span large product/runtime surfaces that must stay out of this intake patch.
3. Resource/time risks and chosen controls: treat the bundle as one package, record the tie-break rule explicitly, create one execution contract plus atomic phase briefs, and stop before implementation work begins.
4. Highest-priority fixes or follow-ups: run G0 first to freeze false closure language, then G1 to add machine-verifiable conformance gates before any product closure phase starts.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: same failure repeats after two focused edits.
- Reset Action: pause edits, capture failing check, and reframe the approach.
- New Search Space: validator contract, routing logic, and docs alignment.
- Next Probe: run the smallest failing command before next patch.

## Task Outcome
- Outcome Status: in_progress
- Decision Quality: pending
- Final Contexts: pending
- Route Match: package intake matched and normalized into module path
- Primary Rework Cause: none
- Incident Signature: none
- Improvement Action: make the package's phase rollout explicit before any remediation implementation starts
- Improvement Artifact: docs/codex/contracts/stack-conformance-remediation.execution-contract.md

## Blockers
- No blocker.

## Next Step
- Validate the normalized contract state with the canonical checks, then hand the next governed unit of work to phase 01 (`G0` claim freeze and checklist repair).

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
